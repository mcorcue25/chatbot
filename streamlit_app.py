import streamlit as st
import pandas as pd
import requests
import time
import os
import datetime
import pytz
import re
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
st.set_page_config(page_title="Monitor Energía 2026", page_icon="⚡", layout="wide")
st.title("⚡ Monitor de Energía (Spot + Futuros)")

FILE_SPOT = "datos_luz.csv"

# ==========================================
# 1. GESTIÓN DE GOOGLE SHEETS
# ==========================================
def obtener_conexion():
    return st.connection("gsheets", type=GSheetsConnection)

def cargar_omip_sheets():
    try:
        conn = obtener_conexion()
        df = conn.read(ttl=0)
        
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
    try:
        conn = obtener_conexion()
        df_actual = conn.read(ttl=0)
        
        df_nuevo = pd.DataFrame([nuevo_dato_dict])
        
        if not df_actual.empty:
            hoy_str = str(date.today())
            df_actual['temp_date'] = pd.to_datetime(df_actual['Fecha'], dayfirst=True, errors='coerce').dt.date.astype(str)
            df_actual = df_actual[df_actual['temp_date'] != hoy_str]
            df_actual = df_actual.drop(columns=['temp_date'])
            df_final = pd.concat([df_actual, df_nuevo], ignore_index=True)
        else:
            df_final = df_nuevo
            
        conn.update(data=df_final)
        st.toast("✅ Google Sheet actualizado correctamente!", icon="🚀")
        return True
    except Exception as e:
        st.error(f"❌ Error escribiendo en Sheets: {e}")
        return False

# ==========================================
# 2. ROBOT OMIP (SELENIUM)
# ==========================================
def ejecutar_robot_omip():
    CONTRATOS = ["Q1-26", "Q2-26", "Q3-26", "Q4-26", "Q1-27", "Q2-27", "Q3-27",
                 "YR-26", "YR-27", "YR-28", "YR-29", "YR-30", "YR-31", "YR-32"]
    
    st.info("🤖 Iniciando escaneo OMIP...")
    
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
                        texto = " ".join(texto.split())
                        partes = texto.split()
                        for parte in partes:
                            if any(c.isdigit() for c in parte) and contrato not in parte:
                                p_clean = parte.replace("€", "").replace(",", ".")
                                try:
                                    precio = float(p_clean)
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
            st.success(f"🔍 {encontrados} contratos encontrados.")
            guardar_fila_en_sheets(datos_hoy)
            return True
        else:
            st.warning("⚠️ No se encontraron precios.")
            return False
            
    except Exception as e:
        st.error(f"❌ Error robot: {e}")
        return False

# ==========================================
# 3. ESIOS (SPOT)
# ==========================================
def actualizar_esios():
    try:
        token = st.secrets["ESIOS_TOKEN"]
    except:
        st.error("❌ Falta ESIOS_TOKEN.")
        return False

    # IMPORTANTE: Aseguramos que 2026 está en la lista para que baje datos de este año
    years = [2024, 2025, 2026]
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
        st.success("✅ Spot actualizado correctamente.")
        return True
    return False

# ==========================================
# 4. CEREBRO IA (CON PROTECCIÓN ANTI-NAN)
# ==========================================
class CerebroGroq:
    def __init__(self, df_spot, df_omip, api_key):
        self.df_spot = df_spot
        self.df_omip = df_omip
        self.client = Groq(api_key=api_key)
        
    def pensar_y_programar(self, pregunta):
        zona_es = pytz.timezone('Europe/Madrid')
        hoy_str = datetime.datetime.now(zona_es).strftime("%Y-%m-%d")
        
        cols_omip = list(self.df_omip.columns) if self.df_omip is not None else []
        sample_omip = self.df_omip.head(3).to_markdown(index=False) if self.df_omip is not None else "Sin datos"

        prompt = f"""
        ERES UN DATA SCIENTIST EXPERTO. FECHA ACTUAL: {hoy_str}
        
        VARIABLES EN MEMORIA:
        1. df_spot (DataFrame): [fecha_hora, precio].
        2. df_omip (DataFrame): [Fecha, ...]. Cols: {cols_omip}. Muestra: {sample_omip}
        
        OBJETIVO: {pregunta}
        
        REGLAS ESTRICTAS (ANTI-ERRORES):
        1. NO uses pd.read_csv. Usa las variables df_spot y df_omip directamente.
        2. IMPORTANTE: Antes de calcular medias o acceder a datos, VERIFICA QUE EL DATAFRAME FILTRADO NO ESTÉ VACÍO.
           Mal: 
             media = df_filtrado['precio'].mean() (Esto da 'nan' si está vacío)
           Bien: 
             if df_filtrado.empty:
                 resultado = "No hay datos disponibles para esa fecha. Por favor actualiza el histórico."
             else:
                 media = df_filtrado['precio'].mean()
                 resultado = f"El precio medio es {{media}}..."
        3. Genera un único bloque de código ```python ... ```.
        4. Guarda la respuesta final en 'resultado'.
        """
        
        try:
            chat = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama-3.3-70b-versatile",
                temperature=0.0
            )
            raw_response = chat.choices[0].message.content
            
            # Limpieza con Regex
            match = re.search(r"```python(.*?)```", raw_response, re.DOTALL)
            if match:
                return match.group(1).strip()
            
            return raw_response.replace("```python", "").replace("```", "").strip()

        except Exception as e:
            return f"resultado = 'Error IA: {e}'"

    def ejecutar(self, codigo):
        try:
            ctx = {
                "df_spot": self.df_spot,
                "df_omip": self.df_omip,
                "pd": pd, "plt": plt, "sns": sns, "date": date,
                "resultado": None
            }
            exec(codigo, ctx)
            
            res = ctx.get("resultado")
            fig = plt.gcf()
            
            if len(fig.axes) > 0: return "IMG", fig
            elif res: return "TXT", str(res)
            else: return "ERR", "El código se ejecutó pero no generó 'resultado'."
            
        except Exception as e:
            return "ERR", f"Error ejecución: {e}"

# ==========================================
# INTERFAZ
# ==========================================

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
    st.header("⚙️ Panel de Control")
    
    # 1. BOTÓN ESIOS
    if st.button("📥 Descargar Histórico (Spot)"):
        if actualizar_esios():
            st.cache_data.clear()
            st.rerun()
            
    # 2. BOTÓN OMIP
    if st.button("🤖 Robot OMIP -> Sheets"):
        if ejecutar_robot_omip():
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()
    
    st.divider()
    
    # --- INDICADOR DE ESTADO DE DATOS (NUEVO) ---
    st.write("📊 **Estado de los Datos**")
    
    if df_spot is not None: 
        ultimo_dato = df_spot['fecha_hora'].max()
        st.success(f"Spot: OK ({len(df_spot)} regs)")
        st.caption(f"📅 Último dato: {ultimo_dato}")
        
        # Alerta visual si los datos son viejos
        if ultimo_dato.year < date.today().year:
            st.warning("⚠️ Tus datos son antiguos. Pulsa 'Descargar Histórico'.")
    else: 
        st.error("Falta descargar Spot")
    
    if df_omip is not None and not df_omip.empty: 
        st.success(f"Futuros: OK ({len(df_omip)} días)")
    else: 
        st.warning("No hay datos de Futuros")

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
            with st.spinner("Pensando..."):
                code = cerebro.pensar_y_programar(q)
                tipo, res = cerebro.ejecutar(code)
                
                if tipo == "ERR":
                    st.error(res)
                    with st.expander("Ver código fallido"): st.code(code)
                else:
                    if tipo == "TXT": st.write(res)
                    elif tipo == "IMG": st.pyplot(res)
                    st.session_state.mensajes.append({"rol": "assistant", "tipo": tipo, "cont": res})
    else:
        st.error("Faltan datos o API Key.")
