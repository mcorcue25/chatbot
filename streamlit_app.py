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

# --- CONFIGURACIÓN GLOBAL ---
st.set_page_config(page_title="Super Analista Energía ⚡", page_icon="🔋", layout="wide")
st.title("⚡ Asistente de Mercado Eléctrico (Spot + Futuros)")
st.caption("Motor: Llama 3.3-70b | Datos: ESIOS (REE) & OMIP")

# Archivos de datos
FILE_SPOT = "datos_luz.csv"
FILE_OMIP = "historico_omip.csv"

# ==========================================
# 1. MÓDULO DE DATOS: ESIOS (SPOT)
# ==========================================
def actualizar_esios():
    INDICATOR_ID = "805" # Precio Mercado Spot
    
    try:
        token = st.secrets["ESIOS_TOKEN"]
    except Exception:
        st.error("❌ Error: No he encontrado 'ESIOS_TOKEN' en los Secrets.")
        return False

    years = [2022, 2023,2024, 2025, 2026] 
    dfs = []
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, year in enumerate(years):
        status_text.text(f"⏳ Descargando ESIOS año {year}...")
        
        url = f"https://api.esios.ree.es/indicators/{INDICATOR_ID}"
        headers = {
            "x-api-key": token,
            "Content-Type": "application/json"
        }
        params = {
            "start_date": f"{year}-01-01T00:00:00",
            "end_date": f"{year}-12-31T23:59:59",
            "time_trunc": "hour"
        }
        
        try:
            r = requests.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            vals = data['indicator']['values']
            
            if vals:
                df = pd.DataFrame(vals)
                if 'geo_id' in df.columns:
                    df = df[df['geo_id'] == 8741] # Península
                
                df = df.rename(columns={'value': 'precio_eur_mwh', 'datetime': 'fecha_hora'})
                # Limpieza de zona horaria
                df['fecha_hora'] = pd.to_datetime(df['fecha_hora'], utc=True).dt.tz_convert('Europe/Madrid').dt.tz_localize(None)
                
                dfs.append(df[['fecha_hora', 'precio_eur_mwh']])
        except Exception as e:
            st.warning(f"⚠️ Error en {year}: {e}")
        
        progress_bar.progress((i + 1) / len(years))
        time.sleep(0.5)

    status_text.empty()
    progress_bar.empty()

    if dfs:
        full_df = pd.concat(dfs)
        full_df = full_df.sort_values('fecha_hora').reset_index(drop=True)
        full_df.to_csv(FILE_SPOT, index=False)
        st.success(f"✅ ESIOS Actualizado: {len(full_df)} registros.")
        return True
    else:
        st.error("❌ No se pudieron descargar datos de ESIOS.")
        return False

# ==========================================
# 2. MÓDULO DE DATOS: OMIP (FUTUROS - SELENIUM)
# ==========================================
def actualizar_omip():
    # Lista de contratos a buscar (Años y Trimestres)
    CONTRATOS_OBJETIVO = [
        "YR-26", "YR-27", "YR-28", "YR-29", 
        "Q2-26", "Q3-26", "Q4-26", "Q1-27", "Q2-27"
    ]
    
    st.info("⏳ Iniciando navegador remoto para leer OMIP...")
    
    # Configuración Headless para servidor
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,3000") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        url = "https://www.omip.pt/es"
        
        driver.get(url)
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3) # Espera de seguridad

        datos_hoy = {"Fecha_Lectura": str(date.today())}
        encontrados = 0

        for contrato in CONTRATOS_OBJETIVO:
            try:
                # Búsqueda visual por texto
                xpath = f"//*[contains(text(), '{contrato}')]"
                elementos = driver.find_elements(By.XPATH, xpath)
                
                precio_detectado = None
                for elem in elementos:
                    try:
                        fila = elem.find_element(By.XPATH, "./..")
                        texto_oculto = fila.get_attribute("textContent")
                        texto_limpio = " ".join(texto_oculto.split())
                        
                        # Buscar números en el texto
                        partes = texto_limpio.split()
                        for parte in partes:
                            # Si es numérico y no es parte del nombre del contrato
                            if any(c.isdigit() for c in parte) and contrato not in parte:
                                # Limpieza básica de moneda
                                p_clean = parte.replace("€", "").replace(",", ".")
                                try:
                                    precio_detectado = float(p_clean)
                                    break
                                except:
                                    continue
                        if precio_detectado:
                            break
                    except:
                        continue
                
                datos_hoy[contrato] = precio_detectado
                if precio_detectado: encontrados += 1
                    
            except Exception:
                datos_hoy[contrato] = None

        driver.quit()
        
        # Guardar en Histórico CSV
        df_nuevo = pd.DataFrame([datos_hoy])
        
        if os.path.exists(FILE_OMIP):
            df_hist = pd.read_csv(FILE_OMIP)
            # Evitar duplicados del mismo día
            df_hist = df_hist[df_hist["Fecha_Lectura"] != str(date.today())]
            df_final = pd.concat([df_hist, df_nuevo], ignore_index=True)
        else:
            df_final = df_nuevo
            
        df_final.to_csv(FILE_OMIP, index=False)
        st.success(f"✅ OMIP Actualizado: {encontrados} contratos encontrados hoy.")
        return True

    except Exception as e:
        st.error(f"❌ Error en Selenium OMIP: {e}")
        return False

# ==========================================
# 3. CEREBRO IA (GROQ)
# ==========================================
class CerebroGroq:
    def __init__(self, df_spot, df_omip, api_key):
        self.df_spot = df_spot
        self.df_omip = df_omip
        self.client = Groq(api_key=api_key)
        
    def pensar_y_programar(self, pregunta):
        # Contexto
        zona_es = pytz.timezone('Europe/Madrid')
        ahora = datetime.datetime.now(zona_es)
        hoy_str = ahora.strftime("%Y-%m-%d")
        
        # Info de los Dataframes
        info_spot = str(self.df_spot.dtypes)
        if self.df_omip is not None:
            info_omip = self.df_omip.tail(5).to_markdown(index=False)
        else:
            info_omip = "No hay datos de OMIP disponibles."

        prompt_sistema = f"""
        Eres un experto analista de mercados energéticos en Python.
        
        --- CONTEXTO ---
        HOY ES: {hoy_str}
        
        TIENES ACCESO A DOS DATAFRAMES:
        1. 'df_spot': Histórico horario ESIOS. Columnas: [fecha_hora (datetime), precio_eur_mwh (float)].
           - Contiene datos de 2024 y 2025.
           
        2. 'df_omip': Histórico futuros OMIP. Columnas: [Fecha_Lectura (str YYYY-MM-DD), YR-26, Q2-26, etc...]
           - Contiene lecturas diarias de cómo cotizan los futuros.
           - Muestra: {info_omip}
        
        --- REGLAS DE ORO ---
        1. SI PIDEN DATOS DE HOY/AYER: Usa 'df_spot'. Filtra por fecha: df_spot[df_spot['fecha_hora'].dt.date == pd.to_datetime('YYYY-MM-DD').date()]
        2. SI PIDEN FUTUROS (AÑOS/TRIMESTRES): Usa 'df_omip'.
        3. SI PIDEN COMPARAR: Puedes usar ambos dataframes en el mismo código.
        4. VARIABLE FINAL: Guarda el resultado explicativo en la variable 'resultado'.
        5. GRÁFICOS: Usa matplotlib (plt). NO definas 'resultado' si haces un gráfico.
        6. IMPORTANTE: Devuelve SOLO CÓDIGO PYTHON puro. Sin explicaciones previas.
        """
        
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": prompt_sistema},
                    {"role": "user", "content": pregunta}
                ],
                model="llama-3.3-70b-versatile",
                temperature=0
            )
            codigo = chat_completion.choices[0].message.content
            codigo = codigo.replace("```python", "").replace("```", "").strip()
            return codigo
        except Exception as e:
            return f"# Error Groq: {e}"

    def ejecutar(self, codigo):
        try:
            # Pasamos ambos DFs al entorno de ejecución
            local_vars = {
                "df_spot": self.df_spot, 
                "df_omip": self.df_omip, 
                "pd": pd, 
                "plt": plt, 
                "sns": sns, 
                "resultado": None
            }
            exec(codigo, {}, local_vars)
            
            resultado = local_vars.get("resultado")
            fig = plt.gcf()
            
            if len(fig.axes) > 0: 
                return "IMG", fig
            elif resultado:
                return "TXT", str(resultado)
            else:
                return "ERR", "El código se ejecutó pero no generó texto en variable 'resultado' ni gráficos."
        except Exception as e:
            return "ERR", f"Error de ejecución Python: {e}"

# ==========================================
# 4. INTERFAZ PRINCIPAL
# ==========================================

# --- CARGAR DATOS AL INICIO ---
@st.cache_data
def cargar_spot():
    if os.path.exists(FILE_SPOT):
        df = pd.read_csv(FILE_SPOT)
        df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
        return df
    return None

def cargar_omip():
    if os.path.exists(FILE_OMIP):
        return pd.read_csv(FILE_OMIP)
    return None

df_spot = cargar_spot()
df_omip = cargar_omip()

# --- SIDEBAR ---
with st.sidebar:
    st.header("🔄 Actualización de Datos")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Descargar ESIOS"):
            if actualizar_esios():
                st.cache_data.clear()
                st.rerun()
    
    with col2:
        if st.button("Leer OMIP"):
            if actualizar_omip():
                st.rerun()
    
    st.divider()
    st.write("Estado de Archivos:")
    if df_spot is not None:
        st.success(f"ESIOS: {len(df_spot)} horas")
    else:
        st.error("ESIOS: No encontrado")
        
    if df_omip is not None:
        st.success(f"OMIP: {len(df_omip)} lecturas")
    else:
        st.warning("OMIP: No encontrado")

# --- CHAT AREA ---
if df_spot is None:
    st.info("👋 Hola. Para empezar, pulsa 'Descargar ESIOS' en el menú lateral.")
else:
    # Historial de Chat
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg.get("type") == "image":
                st.pyplot(msg["content"])
            elif msg.get("type") == "code":
                with st.expander("🛠️ Ver código Python generado"):
                    st.code(msg["content"], language="python")
            else:
                st.markdown(msg["content"])

    # Input Usuario
    if prompt := st.chat_input("Ej: Compara el precio spot de hoy con el futuro YR-26"):
        st.session_state.messages.append({"role": "user", "content": prompt, "type": "text"})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Analizando mercados (Spot + Futuros)..."):
                try:
                    if "GROQ_API_KEY" in st.secrets:
                        api_key = st.secrets["GROQ_API_KEY"]
                        
                        # Instanciamos el cerebro con AMBOS datos
                        bot = CerebroGroq(df_spot, df_omip, api_key)
                        
                        # 1. Pensar (Generar Código)
                        codigo = bot.pensar_y_programar(prompt)
                        
                        with st.expander("🛠️ Ver lógica interna"):
                            st.code(codigo, language="python")
                        st.session_state.messages.append({"role": "assistant", "content": codigo, "type": "code"})
                        
                        # 2. Ejecutar
                        if codigo.startswith("# Error"):
                            st.error(codigo)
                        else:
                            tipo, respuesta = bot.ejecutar(codigo)
                            
                            if tipo == "IMG":
                                st.pyplot(respuesta)
                                st.session_state.messages.append({"role": "assistant", "content": respuesta, "type": "image"})
                                plt.clf()
                            elif tipo == "TXT":
                                st.write(respuesta)
                                st.session_state.messages.append({"role": "assistant", "content": respuesta, "type": "text"})
                            else:
                                st.error(f"❌ {respuesta}")
                    else:
                        st.error("❌ Falta GROQ_API_KEY en secrets.toml")
                        
                except Exception as e:
                    st.error(f"Error crítico: {e}")
