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
st.set_page_config(page_title="Super Analista Energía ⚡", page_icon="🔋", layout="wide")
st.title("⚡ Asistente de Mercado Eléctrico (Spot + Futuros)")
st.caption("Motor: Llama 3.3-70b | Datos: ESIOS (Histórico) & OMIP (Futuros)")

# Archivos de datos locales (Caché)
FILE_SPOT = "datos_luz.csv"

# ==========================================
# 1. MÓDULO DE DATOS: ESIOS (SPOT - PASADO)
# ==========================================
def actualizar_esios():
    INDICATOR_ID = "805" # Precio Mercado Spot
    
    try:
        token = st.secrets["ESIOS_TOKEN"]
    except Exception:
        st.error("❌ Error: No he encontrado 'ESIOS_TOKEN' en los Secrets.")
        return False

    years = [2024, 2025] 
    dfs = []
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, year in enumerate(years):
        status_text.text(f"⏳ Descargando Histórico ESIOS {year}...")
        
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
        st.success(f"✅ ESIOS Actualizado: {len(full_df)} horas de datos históricos.")
        return True
    else:
        st.error("❌ No se pudieron descargar datos de ESIOS.")
        return False

# ==========================================
# 2. MÓDULO DE DATOS: OMIP (FUTUROS - GOOGLE SHEETS)
# ==========================================
def cargar_omip_sheets():
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        df = conn.read(ttl=0)
        
        # --- LIMPIEZA DE DATOS ---
        if 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True, errors='coerce')
            df = df.sort_values('Fecha', ascending=False)

        cols_a_ignorar = ['Fecha']
        for col in df.columns:
            if col not in cols_a_ignorar:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    except Exception as e:
        st.error(f"❌ Error conectando con Google Sheets: {e}")
        return None

# ==========================================
# 3. CEREBRO IA (LÓGICA PASADO VS FUTURO)
# ==========================================
class CerebroGroq:
    def __init__(self, df_spot, df_omip, api_key):
        self.df_spot = df_spot
        self.df_omip = df_omip
        self.client = Groq(api_key=api_key)
        
    def pensar_y_programar(self, pregunta):
        # Contexto temporal
        zona_es = pytz.timezone('Europe/Madrid')
        ahora = datetime.datetime.now(zona_es)
        hoy_str = ahora.strftime("%Y-%m-%d")
        
        # Preparación de muestras para el prompt
        if self.df_omip is not None:
            info_omip = self.df_omip.head(3).to_markdown(index=False)
            cols_omip = list(self.df_omip.columns)
        else:
            info_omip = "No disponible"
            cols_omip = []

        # --- PROMPT REFINADO: LÓGICA PASADO VS FUTURO ---
        prompt_sistema = f"""
        Eres un programador experto en análisis de mercados energéticos (Python/Pandas).
        Hoy es: {hoy_str}
        
        TIENES DOS FUENTES DE DATOS:

        1. 🔙 FUENTE DEL PASADO (df_spot):
           - Contiene: Precios HISTÓRICOS reales hora a hora (2024, 2025 hasta hoy).
           - Columnas: ['fecha_hora', 'precio_eur_mwh']
           - Uso: ÚSALO SIEMPRE que pregunten por "ayer", "semana pasada", "año pasado", "histórico", "tendencia actual".

        2. 🔮 FUENTE DEL FUTURO (df_omip):
           - Contiene: Cotizaciones de FUTUROS (Años 2026, 2027... y Trimestres Q1-26, etc).
           - Columnas Disponibles: {cols_omip}
           - Muestra: {info_omip}
           - Uso: ÚSALO SIEMPRE que pregunten por "futuro", "año que viene", "2026", "2027", "previsión", "precio de cierre".
        
        REGLAS DE DECISIÓN ESTRICTAS:
        A. Si preguntan "¿Cómo estaba el precio ayer?" -> `df_spot`.
        B. Si preguntan "¿A cuánto está el Q2-26?" -> `df_omip`.
        C. Si preguntan "¿Sale rentable comprar futuros?" -> USA AMBOS. Calcula la media actual de `df_spot` y compárala con el valor del futuro en `df_omip`.

        INSTRUCCIONES TÉCNICAS:
        1. Genera SOLO CÓDIGO PYTHON.
        2. Guarda la respuesta en texto en la variable 'resultado'.
        3. Si haces gráficas, usa `plt` pero NO uses `plt.show()`.
        4. OJO FECHAS OMIP: La columna 'Fecha' en `df_omip` es datetime64.
        """
        
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": prompt_sistema},
                    {"role": "user", "content": pregunta}
                ],
                model="llama-3.3-70b-versatile",
                temperature=0.0 # Cero creatividad para seguir reglas estrictas
            )
            codigo = chat_completion.choices[0].message.content
            codigo = codigo.replace("```python", "").replace("```", "").strip()
            return codigo
        except Exception as e:
            return f"# Error Groq: {e}"

    def ejecutar(self, codigo):
        try:
            local_vars = {
                "df_spot": self.df_spot, 
                "df_omip": self.df_omip, 
                "pd": pd, 
                "plt": plt, 
                "sns": sns, 
                "resultado": None,
                "date": date
            }
            exec(codigo, {}, local_vars)
            
            resultado = local_vars.get("resultado")
            fig = plt.gcf()
            
            if len(fig.axes) > 0: 
                return "IMG", fig
            elif resultado:
                return "TXT", str(resultado)
            else:
                return "ERR", "El código se ejecutó pero no generó la variable 'resultado'."
        except Exception as e:
            return "ERR", f"Error de ejecución: {e}"

# ==========================================
# 4. INTERFAZ PRINCIPAL
# ==========================================

# --- CARGAR DATOS ---
@st.cache_data
def cargar_spot():
    if os.path.exists(FILE_SPOT):
        df = pd.read_csv(FILE_SPOT)
        df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
        return df
    return None

df_omip = cargar_omip_sheets()
df_spot = cargar_spot()

# Inicializar IA
api_key = st.secrets.get("GROQ_API_KEY")
cerebro = CerebroGroq(df_spot, df_omip, api_key) if api_key else None

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Panel de Control")
    
    if st.button("🔄 Actualizar Histórico (ESIOS)"):
        if actualizar_esios():
            st.cache_data.clear()
            st.rerun()
    
    if st.button("🔄 Refrescar Futuros (Sheets)"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.write("📊 **Resumen de Datos:**")
    if df_spot is not None:
        st.info(f"🔙 **Pasado (Spot):**\n{len(df_spot)} registros horarios.\n(Fuente: ESIOS)")
    else:
        st.warning("Faltan datos de ESIOS.")

    if df_omip is not None:
        st.info(f"🔮 **Futuro (OMIP):**\n{len(df_omip)} días de cotización.\n(Fuente: Google Sheets)")
    else:
        st.error("Error conectando a Sheets.")

# --- CHAT ---
st.subheader("💬 Analista de Mercado")

if "mensajes" not in st.session_state:
    st.session_state.mensajes = []

for msg in st.session_state.mensajes:
    with st.chat_message(msg["rol"]):
        if msg["tipo"] == "TXT":
            st.write(msg["contenido"])
        elif msg["tipo"] == "IMG":
            st.pyplot(msg["contenido"])
        elif msg["tipo"] == "CODE":
            st.code(msg["contenido"])

pregunta = st.chat_input("Ej: ¿Cómo está el precio hoy? vs ¿A cuánto cotiza el 2026?")

if pregunta:
    st.session_state.mensajes.append({"rol": "user", "tipo": "TXT", "contenido": pregunta})
    with st.chat_message("user"):
        st.write(pregunta)
    
    if not cerebro:
        st.error("⚠️ Configura tu API KEY en .streamlit/secrets.toml")
    else:
        with st.chat_message("assistant"):
            with st.spinner("Consultando bases de datos (Pasado vs Futuro)..."):
                codigo_generado = cerebro.pensar_y_programar(pregunta)
                
                tipo_resp, contenido_resp = cerebro.ejecutar(codigo_generado)
                
                if tipo_resp == "ERR":
                    st.error(contenido_resp)
                    with st.expander("Ver código generado"):
                        st.code(codigo_generado)
                else:
                    if tipo_resp == "TXT":
                        st.write(contenido_resp)
                    elif tipo_resp == "IMG":
                        st.pyplot(contenido_resp)
                    
                    st.session_state.mensajes.append({"rol": "assistant", "tipo": tipo_resp, "contenido": contenido_resp})
