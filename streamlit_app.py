import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import date
import matplotlib.pyplot as plt
from groq import Groq
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from streamlit_gsheets import GSheetsConnection

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Monitor Energía Unificado", page_icon="⚡", layout="wide")
st.title("⚡ Monitor de Energía (Spot + Futuros Persistentes)")

# Archivo local para SPOT (Horario)
FILE_SPOT = "datos_luz.csv"

# ==========================================
# 1. GESTIÓN DE GOOGLE SHEETS (OMIP)
# ==========================================
def obtener_conexion_gsheets():
    return st.connection("gsheets", type=GSheetsConnection)

def cargar_historico_omip():
    try:
        conn = obtener_conexion_gsheets()
        # Leemos la hoja. ttl=0 para que no use caché y lea siempre lo fresco
        df = conn.read(ttl=0)
        # Aseguramos que la fecha sea datetime para ordenar bien
        if not df.empty and 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'])
            df = df.sort_values('Fecha', ascending=False)
        return df
    except Exception as e:
        st.error(f"Error conectando a Google Sheets: {e}")
        return pd.DataFrame()

def guardar_nuevo_dato_omip(nuevo_dato_dict):
    """
    Añade una fila a Google Sheets respetando la estructura existente.
    """
    conn = obtener_conexion_gsheets()
    df_actual = conn.read(ttl=0)
    
    # Convertimos el diccionario nuevo a DataFrame
    df_nuevo = pd.DataFrame([nuevo_dato_dict])
    
    # Si la hoja ya tiene datos, concatenamos
    if not df_actual.empty:
        # Filtramos para no duplicar la fecha de hoy si ya existe
        fecha_hoy = str(date.today())
        df_actual['Fecha'] = df_actual['Fecha'].astype(str)
        df_actual = df_actual[df_actual['Fecha'] != fecha_hoy]
        
        # Unimos
        df_final = pd.concat([df_actual, df_nuevo], ignore_index=True)
    else:
        df_final = df_nuevo
        
    # Escribimos de vuelta a Sheets
    conn.update(data=df_final)

# ==========================================
# 2. SCRAPING OMIP (FUTUROS)
# ==========================================
def actualizar_omip():
    st.info("⏳ Iniciando robot para leer OMIP...")
    
    # 1. Averiguar qué contratos necesitamos buscar
    df_ref = cargar_historico_omip()
    if df_ref.empty:
        columnas_objetivo = [
            "Q1-26", "Q2-26", "Q3-26", "Q4-26", "Q1-27", "Q2-27", "Q3-27",
            "YR-26", "YR-27", "YR-28", "YR-29", "YR-30", "YR-31", "YR-32"
        ]
    else:
        columnas_objetivo = [c for c in df_ref.columns if c != 'Fecha']

    # 2. Configurar Selenium
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
        
        datos_hoy = {"Fecha": str(date.today())}
        encontrados = 0
        
        # 3. Buscar cada contrato
        for contrato in columnas_objetivo:
            try:
                xpath = f"//*[contains(text(), '{contrato}')]"
                elementos = driver.find_elements(By.XPATH, xpath)
                
                precio_final = None
                
                for elem in elementos:
                    try:
                        padre = elem.find_element(By.XPATH, "./..")
                        texto_linea = padre.get_attribute("textContent")
                        texto_linea = " ".join(texto_linea.split())
                        
                        partes = texto_linea.split()
                        for parte in partes:
                            if any(c.isdigit() for c in parte) and contrato not in parte:
                                p_clean = parte.replace("€", "").replace(",", ".")
                                try:
                                    precio_final = float(p_clean)
                                    break 
                                except: continue
                        
                        if precio_final: break
                    except: continue
                
                datos_hoy[contrato] = precio_final
                if precio_final: encontrados += 1
                
            except:
                datos_hoy[contrato] = None
        
        driver.quit()
        
        # 4. Guardar en la Nube
        guardar_nuevo_dato_omip(datos_hoy)
        st.success(f"✅ Datos guardados en Google Sheets. ({encontrados} contratos actualizados)")
        time.sleep(1)
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Error scraping: {e}")

# ==========================================
# 3. ESIOS (SPOT)
# ==========================================
def actualizar_esios():
    try:
        token = st.secrets["ESIOS_TOKEN"]
    except:
        st.error("❌ Falta ESIOS_TOKEN en secrets.")
        return

    years = [2022, 2023, 2024, 2025, 2026]
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
        st.success(f"✅ Spot Actualizado: {len(full)} horas.")
        st.rerun()

# ==========================================
# 4. INTELIGENCIA ARTIFICIAL (MODIFICADA)
# ==========================================
def consultar_ia(pregunta, df_spot, df_omip):
    try:
        client = Groq(api_key=st.secrets["GROQ_API_KEY"])
        # ==========================================
# 4. INTELIGENCIA ARTIFICIAL (MEJORADA - ANTI-ERRORES)
# ==========================================
def consultar_ia(pregunta, df_spot, df_omip):
    try:
        client = Groq(api_key=st.secrets["GROQ_API_KEY"])
        
        # Preparamos muestras de datos
        txt_spot = df_spot.tail(48).to_string(index=False) if df_spot is not None else "Sin datos"
        # Mostramos las columnas disponibles para que la IA no invente nombres
        cols_omip = list(df_omip.columns) if not df_omip.empty else "Sin columnas"
        txt_omip = df_omip.head(5).to_string(index=False) if not df_omip.empty else "Sin datos"
        
        prompt = f"""
        ACTÚA COMO UN GENERADOR DE CÓDIGO PYTHON EXPERTO Y ROBUSTO.
        
        DATOS DISPONIBLES:
        1. df_spot (DataFrame): [fecha_hora, precio].
        2. df_omip (DataFrame): [Fecha, ...].
           COLUMNAS EXACTAS DISPONIBLES EN DF_OMIP: {cols_omip}
           Muestra datos: {txt_omip}

        OBJETIVO:
        {pregunta}

        REGLAS DE SEGURIDAD (CRÍTICO PARA EVITAR CRASH):
        1. NO asumas que los datos siempre existen.
        2. ANTES de extraer un valor único (ej: `valor = df_filt.values[0]`), DEBES verificar si el dataframe filtrado tiene filas.
           
           MALO:
           precio = df_omip.loc[...].values[0] # Esto crashea si no hay datos
           
           BUENO:
           filtro = df_omip.loc[...]
           if not filtro.empty:
               precio = filtro.values[0]
           else:
               resultado = "No se encontraron datos para esa fecha/contrato."
               return # O detener lógica

        3. La variable final a mostrar debe llamarse 'resultado' (string).
        4. RESPONDE SOLO CÓDIGO PYTHON PURO (sin markdown, sin explicaciones).
        5. Si usas fechas, recuerda que en df_omip la columna 'Fecha' es datetime.
        """
        
        chat = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile",
            temperature=0.0
        )
        return chat.choices[0].message.content
    except Exception as e:
        return f"resultado = 'Error conectando con la IA: {e}'"

# ==========================================
# INTERFAZ PRINCIPAL
# ==========================================

# Carga de datos
df_omip = cargar_historico_omip()
df_spot = pd.read_csv(FILE_SPOT) if os.path.exists(FILE_SPOT) else None

with st.sidebar:
    st.header("🔄 Actualizar Datos")
    col1, col2 = st.columns(2)
    if col1.button("Spot (ESIOS)"): actualizar_esios()
    if col2.button("Futuros (OMIP)"): actualizar_omip()
    
    st.divider()
    if not df_omip.empty:
        st.write("### Últimos Futuros")
        st.dataframe(df_omip.head(3), use_container_width=True, hide_index=True)

# Chat
if "mensajes" not in st.session_state: st.session_state.mensajes = []

for m in st.session_state.mensajes:
    with st.chat_message(m["rol"]):
        if m["tipo"] == "texto": st.write(m["cont"])
        elif m["tipo"] == "codigo": st.code(m["cont"])
        elif m["tipo"] == "img": st.pyplot(m["cont"])

if q := st.chat_input("Pregunta a tu Data Warehouse..."):
    # Guardamos pregunta usuario
    st.session_state.mensajes.append({"rol": "user", "tipo": "texto", "cont": q})
    with st.chat_message("user"): st.write(q)
    
    with st.chat_message("assistant"):
        with st.spinner("Analizando y generando código..."):
            # 1. Obtener respuesta cruda de la IA
            resp_raw = consultar_ia(q, df_spot, df_omip)
            
            # 2. Limpieza de seguridad por si la IA pone markdown
            code_clean = resp_raw.replace("```python", "").replace("```", "").strip()
            
            # Mostramos el código que se va a ejecutar
            st.code(code_clean)
            st.session_state.mensajes.append({"rol": "assistant", "tipo": "codigo", "cont": code_clean})
            
            # 3. Ejecución dinámica
            try:
                # Diccionario local donde ocurrirá la magia
                local_vars = {
                    "pd": pd, 
                    "plt": plt, 
                    "df_spot": df_spot, 
                    "df_omip": df_omip, 
                    "resultado": "La IA no generó la variable 'resultado'."
                }
                
                exec(code_clean, {}, local_vars)
                
                # 4. Procesar resultados
                resultado_texto = local_vars.get("resultado", "")
                
                # Si hay texto, lo mostramos
                if resultado_texto:
                    st.write(resultado_texto)
                    st.session_state.mensajes.append({"rol": "assistant", "tipo": "texto", "cont": resultado_texto})
                
                # Si hay gráficos, los mostramos
                if plt.get_fignums():
                    fig = plt.gcf()
                    st.pyplot(fig)
                    st.session_state.mensajes.append({"rol": "assistant", "tipo": "img", "cont": fig})
                    plt.clf() # Limpiamos figura para la próxima
                    
            except Exception as e:
                st.error(f"Error ejecutando código generado: {e}")
