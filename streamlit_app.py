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
# --- NUEVA IMPORTACIÓN ---
from streamlit_gsheets import GSheetsConnection

# --- CONFIGURACIÓN GLOBAL ---
st.set_page_config(page_title="Super Analista Energía ⚡", page_icon="🔋", layout="wide")
st.title("⚡ Asistente de Mercado Eléctrico (Spot + Futuros)")
st.caption("Motor: Llama 3.3-70b | Datos: ESIOS (REE) & OMIP (Google Sheets)")

# Archivos de datos
FILE_SPOT = "datos_luz.csv"
# FILE_OMIP = "historico_omip.csv"  <-- YA NO SE USA PARA LECTURA, USAMOS G-SHEETS

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

    years = [2024, 2025] 
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
# 2. MÓDULO DE DATOS: OMIP (GOOGLE SHEETS)
# ==========================================

# --- NUEVA FUNCIÓN PARA CARGAR Y LIMPIAR DESDE SHEETS ---
def cargar_omip_sheets():
    try:
        # 1. Conexión a Google Sheets
        conn = st.connection("gsheets", type=GSheetsConnection)
        
        # 2. Lectura de datos (ttl=0 para que no use caché y lea siempre lo fresco)
        df = conn.read(ttl=0)
        
        # 3. LIMPIEZA DE FORMATO (CRÍTICO PARA TU EXCEL)
        # El formato viene como "64,5" (string con coma) y fechas "dd/mm/yyyy"
        
        # A. Limpiar Fechas
        if 'Fecha' in df.columns:
            # dayfirst=True es clave para 7/03/2025
            df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True, errors='coerce')
            # Ordenamos por fecha descendente
            df = df.sort_values('Fecha', ascending=False)

        # B. Limpiar Números (Q1-26, YR-26, etc.)
        cols_a_ignorar = ['Fecha']
        for col in df.columns:
            if col not in cols_a_ignorar:
                # Si la columna es tipo objeto (texto), intentamos arreglar la coma
                if df[col].dtype == 'object':
                    # Reemplazar coma por punto y convertir a float
                    df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    except Exception as e:
        st.error(f"❌ Error conectando con Google Sheets: {e}")
        return None

# Mantenemos la función de Scraping por si quieres actualizar datos, 
# PERO AHORA DEBERÍA ESCRIBIR EN EL SHEET O EN LOCAL PARA LUEGO SUBIRLO.
# Para simplificar, dejo tu función de scraping escribiendo en local, 
# pero la lectura principal vendrá del Sheet.
def actualizar_omip_scraping():
    # ... (Tu código de Selenium existente se mantiene igual para scraping local) ...
    # NOTA: Si quieres que esto actualice el Google Sheet automáticamente, 
    # requeriría credenciales de escritura (Service Account), lo cual es más complejo.
    pass 

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
        
        # Formateamos la muestra de OMIP para que la IA entienda bien los datos
        if self.df_omip is not None:
            # Le pasamos las primeras 5 filas y los tipos de datos para que vea que son floats
            info_omip = self.df_omip.head(5).to_markdown(index=False)
            dtypes_omip = str(self.df_omip.dtypes)
        else:
            info_omip = "No hay datos de OMIP disponibles."
            dtypes_omip = ""

        prompt_sistema = f"""
        Eres un experto analista de mercados energéticos en Python.
        
        --- CONTEXTO ---
        HOY ES: {hoy_str}
        
        TIENES ACCESO A DOS DATAFRAMES:
        1. 'df_spot': Histórico horario ESIOS. Columnas: [fecha_hora (datetime), precio_eur_mwh (float)].
           
        2. 'df_omip': Histórico futuros OMIP (Origen: Google Sheets).
           - Columnas: [Fecha (datetime), Q1-26 (float), YR-26 (float), etc...]
           - Muestra de datos: 
           {info_omip}
           - Tipos de datos:
           {dtypes_omip}
        
        --- REGLAS DE ORO ---
        1. SI PIDEN DATOS DE HOY/AYER: Usa 'df_spot'. 
        2. SI PIDEN FUTUROS: Usa 'df_omip'. Recuerda que 'Fecha' es la fecha de cotización.
           Ejemplo: Para ver cómo cerró el Q2-26 ayer, filtra 'df_omip' por la fecha más reciente.
        3. SI PIDEN COMPARAR: Puedes usar ambos.
        4. IMPORTANTE FORMATO NÚMEROS: Los datos de OMIP ya están convertidos a FLOAT (ej: 64.5). No intentes reemplazar comas, ya está hecho.
        5. VARIABLE FINAL: Guarda el resultado explicativo en la variable 'resultado'.
        6. GRÁFICOS: Usa matplotlib (plt).
        7. Devuelve SOLO CÓDIGO PYTHON puro.
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

# Cargamos OMIP desde la nueva función de Sheets
df_omip = cargar_omip_sheets()
df_spot = cargar_spot()

# Inicializar IA
api_key = st.secrets.get("GROQ_API_KEY")
cerebro = CerebroGroq(df_spot, df_omip, api_key) if api_key else None

# --- SIDEBAR ---
with st.sidebar:
    st.header("🔄 Actualización de Datos")
    
    if st.button("Descargar ESIOS (Spot)"):
        if actualizar_esios():
            st.cache_data.clear()
            st.rerun()
    
    # Botón para recargar Sheets manualmente (limpia caché)
    if st.button("Recargar Google Sheets"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.write("📊 Estado de Datos:")
    if df_spot is not None:
        st.success(f"Spot: {len(df_spot)} horas")
    else:
        st.warning("Spot: Sin datos")

    if df_omip is not None:
        st.success(f"Futuros (Sheets): {len(df_omip)} días")
        st.dataframe(df_omip.head(3), hide_index=True) # Previsualización rápida
    else:
        st.error("Futuros: Error al conectar con Sheets")

# --- CHAT INTERFACE ---
st.subheader("💬 Consulta a tu Data Warehouse")

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

pregunta = st.chat_input("Ej: ¿Cómo ha evolucionado el Q2-26 esta semana?")

if pregunta:
    st.session_state.mensajes.append({"rol": "user", "tipo": "TXT", "contenido": pregunta})
    with st.chat_message("user"):
        st.write(pregunta)
    
    if not cerebro:
        st.error("Falta configurar la GROQ_API_KEY en secrets.")
    else:
        with st.chat_message("assistant"):
            with st.spinner("Analizando datos de Sheets y ESIOS..."):
                codigo_generado = cerebro.pensar_y_programar(pregunta)
                
                # Opcional: Mostrar código generado (debug)
                # with st.expander("Ver código generado"):
                #    st.code(codigo_generado)
                
                tipo_resp, contenido_resp = cerebro.ejecutar(codigo_generado)
                
                if tipo_resp == "ERR":
                    st.error(contenido_resp)
                    with st.expander("Ver código fallido"):
                        st.code(codigo_generado)
                else:
                    if tipo_resp == "TXT":
                        st.write(contenido_resp)
                    elif tipo_resp == "IMG":
                        st.pyplot(contenido_resp)
                    
                    st.session_state.mensajes.append({"rol": "assistant", "tipo": tipo_resp, "contenido": contenido_resp})
