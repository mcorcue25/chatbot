import streamlit as st
import pandas as pd
import requests
import time
import os
import datetime
import pytz
import matplotlib.pyplot as plt
import seaborn as sns
from groq import Groq
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import date
from streamlit_gsheets import GSheetsConnection

# --- CONFIGURACIÓN GLOBAL ---
st.set_page_config(page_title="Monitor Energía 360", page_icon="⚡", layout="wide")
st.title("⚡ Monitor de Energía (Spot + Futuros Automáticos)")

# Nombre EXACTO del archivo local para evitar errores
FILE_SPOT = "datos_luz.csv"

# ==========================================
# 1. GESTIÓN DE GOOGLE SHEETS (LECTURA Y ESCRITURA)
# ==========================================
def obtener_conexion():
    return st.connection("gsheets", type=GSheetsConnection)

def cargar_omip_sheets():
    try:
        conn = obtener_conexion()
        # ttl=0 obliga a leer datos frescos, no caché
        df = conn.read(ttl=0)
        
        # Limpieza robusta
        if 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True, errors='coerce')
            df = df.sort_values('Fecha', ascending=False)

        cols_ignorar = ['Fecha']
        for col in df.columns:
            if col not in cols_ignorar and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"❌ Error leyendo Google Sheets: {e}")
        return pd.DataFrame()

def guardar_fila_en_sheets(nuevo_dato_dict):
    """
    Descarga el Sheet actual, añade la fila de hoy y lo sube de nuevo.
    """
    try:
        conn = obtener_conexion()
        df_actual = conn.read(ttl=0)
        
        # Crear DataFrame con la nueva fila
        df_nuevo = pd.DataFrame([nuevo_dato_dict])
        
        # Concatenar (si ya existe datos o si está vacío)
        if not df_actual.empty:
            # Convertimos fechas a string para evitar duplicados de HOY
            hoy_str = str(date.today())
            # Asumimos que la columna fecha puede venir como string o date
            # Hacemos una limpieza temporal para verificar
            df_actual['temp_date'] = pd.to_datetime(df_actual['Fecha'], dayfirst=True, errors='coerce').dt.date.astype(str)
            
            # Si ya existe una fila con la fecha de hoy, la borramos para sobreescribirla
            df_actual = df_actual[df_actual['temp_date'] != hoy_str]
            df_actual = df_actual.drop(columns=['temp_date'])
            
            df_final = pd.concat([df_actual, df_nuevo], ignore_index=True)
        else:
            df_final = df_nuevo
            
        # ACTUALIZAR GOOGLE SHEETS
        conn.update(data=df_final)
        st.toast("✅ Google Sheet actualizado correctamente!", icon="🚀")
        return True
        
    except Exception as e:
        st.error(f"❌ Error escribiendo en Google Sheets: {e}")
        st.info("💡 Nota: Para escribir necesitas configurar 'service_account' en secrets.toml, no solo la URL pública.")
        return False

# ==========================================
# 2. SCRAPING OMIP (SELENIUM) - RECUPERADO
# ==========================================
def ejecutar_robot_omip():
    """
    Escanea la web de OMIP, obtiene los precios y llama a guardar_fila_en_sheets.
    """
    CONTRATOS = ["Q1-26", "Q2-26", "Q3-26", "Q4-26", "Q1-27", "Q2-27", "Q3-27",
                 "YR-26", "YR-27", "YR-28", "YR-29", "YR-30", "YR-31", "YR-32"]
    
    st.info("🤖 Robot iniciando escaneo de OMIP...")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,3000")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get("https://www.omip.pt/es")
        
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3)
        
        # Diccionario con la fecha de hoy en formato dd/mm/yyyy (formato español)
        datos_hoy = {"Fecha": date.today().strftime("%d/%m/%Y")}
        encontrados = 0
        
        for contrato in CONTRATOS:
            try:
                xpath = f"//*[contains(text(), '{contrato}')]"
                elementos = driver.find_elements(By.XPATH, xpath)
                precio = None
                
                for elem in elementos:
                    try:
                        padre = elem.find_element(By.XPATH, "./..")
                        texto = padre.get_attribute("textContent")
                        texto = " ".join(texto.split()) # Quitar espacios extra
                        
                        partes = texto.split()
                        for parte in partes:
                            if any(c.isdigit() for c in parte) and contrato not in parte:
                                # OMIP usa coma para decimales (64,50) -> convertimos a string con coma
                                # OJO: Para Sheets, si tu locale es ES, prefiere coma. Si es US, punto.
                                # Vamos a guardar como string limpio "64.50" o float directamtnte
                                p_clean = parte.replace("€", "").replace(",", ".")
                                try:
                                    precio = float(p_clean)
                                    # Para el Sheet, lo guardamos formateado como string con coma si tu Excel lo requiere,
                                    # o float si streamlit-gsheets lo maneja. Probemos float nativo.
                                    break
                                except: continue
                        if precio: break
                    except: continue
                
                datos_hoy[contrato] = precio
                if precio: encontrados += 1
            except:
                datos_hoy[contrato] = None
        
        driver.quit()
        
        if encontrados > 0:
            st.success(f"🔍 Escaneo completado. {encontrados} contratos encontrados.")
            guardar_fila_en_sheets(datos_hoy)
            return True
        else:
            st.warning("⚠️ El robot no encontró precios. ¿Quizás la web cambió?")
            return False
            
    except Exception as e:
        st.error(f"❌ Error crítico del robot: {e}")
        return False

# ==========================================
# 3. ACTUALIZAR ESIOS (SPOT)
# ==========================================
def actualizar_esios():
    try:
        token = st.secrets["ESIOS_TOKEN"]
    except:
        st.error("❌ Falta ESIOS_TOKEN en secrets.")
        return False

    years = [2024, 2025, 2026] # Añadido 2026 por si acaso
    dfs = []
    bar = st.progress(0)
    
    for i, year in enumerate(years):
        url = "https://api.esios.ree.es/indicators/805"
        headers = {"x-api-key": token}
        params = {"start_date": f"{year}-01-01T00:00", "end_date": f"{year}-12-31T23:59", "time_trunc": "hour"}
        
        try:
            r = requests.get(url, headers=headers, params=params)
            if r.status_code == 200:
                vals = r.json()['indicator']['values']
                if vals:
                    df = pd.DataFrame(vals)
                    if 'geo_id' in df.columns: df = df[df['geo_id'] == 8741]
                    df = df.rename(columns={'value': 'precio', 'datetime': 'fecha_hora'})
                    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'], utc=True).dt.tz_convert('Europe/Madrid').dt.tz_localize(None)
                    dfs.append(df[['fecha_hora', 'precio']])
        except: pass
        bar.progress((i+1)/len(years))
    
    bar.empty()
    if dfs:
        full = pd.concat(dfs).sort_values('fecha_hora')
        full.to_csv(FILE_SPOT, index=False)
        st.success(f"✅ Spot descargado y guardado en {FILE_SPOT}")
        return True
    return False

# ==========================================
# 4. CEREBRO IA (CON PROTECCIÓN DE ARCHIVOS)
# ==========================================
class CerebroGroq:
    def __init__(self, df_spot, df_omip, api_key):
        self.df_spot = df_spot
        self.df_omip = df_omip
        self.client = Groq(api_key=api_key)
        
    def pensar_y_programar(self, pregunta):
        zona_es = pytz.timezone('Europe/Madrid')
        hoy_str = datetime.datetime.now(zona_es).strftime("%Y-%m-%d")
        
        # Info para el prompt
        cols_omip = list(self.df_omip.columns) if self.df_omip is not None else []
        sample_omip = self.df_omip.head(3).to_markdown(index=False) if self.df_omip is not None else "Sin datos"

        prompt = f"""
        ERES UN DATA SCIENTIST EXPERTO EN PYTHON.
        FECHA ACTUAL: {hoy_str}
        
        TIENES ESTAS VARIABLES CARGADAS EN MEMORIA (NO LEAS ARCHIVOS):
        1. df_spot (DataFrame): Histórico horario. Cols: [fecha_hora, precio].
        2. df_omip (DataFrame): Futuros diarios. Cols: {cols_omip}.
           Muestra: {sample_omip}
        
        OBJETIVO: {pregunta}
        
        REGLAS DE SEGURIDAD (MUY IMPORTANTE):
        1. ¡¡¡PROHIBIDO USAR pd.read_csv()!!! Los datos YA ESTÁN en 'df_spot' y 'df_omip'.
        2. Si intentas leer 'df_spot.csv' el programa fallará. USA LA VARIABLE df_spot.
        3. Para Futuros usa 'df_omip'. Para Pasado/Spot usa 'df_spot'.
        4. Genera solo código Python.
        5. Guarda el resultado texto en la variable 'resultado'.
        """
        
        try:
            chat = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama-3.3-70b-versatile",
                temperature=0.0
            )
            code = chat.choices[0].message.content.replace("```python", "").replace("```", "").strip()
            return code
        except Exception as e:
            return f"resultado = 'Error IA: {e}'"

    def ejecutar(self, codigo):
        try:
            # Contexto blindado: Pasamos las variables explícitamente
            ctx = {
                "df_spot": self.df_spot,
                "df_omip": self.df_omip,
                "pd": pd,
                "plt": plt,
                "sns": sns,
                "date": date,
                "resultado": None
            }
            exec(codigo, ctx)
            
            res = ctx.get("resultado")
            fig = plt.gcf()
            
            if len(fig.axes) > 0: return "IMG", fig
            elif res: return "TXT", str(res)
            else: return "ERR", "Código ejecutado pero sin resultado."
            
        except Exception as e:
            return "ERR", f"Error ejecución: {e}"

# ==========================================
# INTERFAZ
# ==========================================

# Carga inicial segura
@st.cache_data
def cargar_spot_seguro():
    if os.path.exists(FILE_SPOT):
        try:
            df = pd.read_csv(FILE_SPOT)
            df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
            return df
        except: return None
    return None

df_spot = cargar_spot_seguro()
df_omip = cargar_omip_sheets()

cerebro = None
if "GROQ_API_KEY" in st.secrets:
    cerebro = CerebroGroq(df_spot, df_omip, st.secrets["GROQ_API_KEY"])

with st.sidebar:
    st.header("⚙️ Operaciones")
    
    # BOTÓN 1: ESIOS
    if st.button("📥 Descargar Histórico (Spot)"):
        if actualizar_esios():
            st.cache_data.clear()
            st.rerun()
            
    # BOTÓN 2: OMIP (ROBOT + SUBIDA)
    if st.button("🤖 Robot OMIP -> Google Sheets"):
        if ejecutar_robot_omip():
            st.cache_data.clear() # Limpiamos caché para ver los datos nuevos
            time.sleep(1)
            st.rerun()
    
    st.divider()
    st.write("Estado de Datos:")
    if df_spot is not None: st.success(f"Spot: {len(df_spot)} regs")
    else: st.error("Falta descargar Spot")
    
    if df_omip is not None and not df_omip.empty: st.success(f"Futuros: {len(df_omip)} días")
    else: st.warning("No hay datos de Futuros")

# Chat App
st.subheader("💬 Asistente Energía")

if "mensajes" not in st.session_state: st.session_state.mensajes = []

for m in st.session_state.mensajes:
    with st.chat_message(m["rol"]):
        if m["tipo"] == "TXT": st.write(m["cont"])
        elif m["tipo"] == "IMG": st.pyplot(m["cont"])
        elif m["tipo"] == "CODE": st.code(m["cont"])

if q := st.chat_input("Pregunta..."):
    st.session_state.mensajes.append({"rol": "user", "tipo": "TXT", "cont": q})
    with st.chat_message("user"): st.write(q)
    
    if cerebro and df_spot is not None:
        with st.chat_message("assistant"):
            with st.spinner("Analizando..."):
                code = cerebro.pensar_y_programar(q)
                tipo, res = cerebro.ejecutar(code)
                
                if tipo == "ERR":
                    st.error(res)
                    with st.expander("Ver código"): st.code(code)
                else:
                    if tipo == "TXT": st.write(res)
                    elif tipo == "IMG": st.pyplot(res)
                    st.session_state.mensajes.append({"rol": "assistant", "tipo": tipo, "cont": res})
    else:
        st.error("Faltan datos (Spot) o API Key.")
