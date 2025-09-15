#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 13 03:00:22 2025

@author: guillermofernandez
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import time
import requests

from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

# Configuración de rutas
GECKO_PATH = "C:/Users/guillermo.fernandez/Downloads/geckodriver-v0.34.0-win64/geckodriver.exe"
FIREFOX_PATH = "C:/Program Files/Mozilla Firefox/firefox.exe"
CARPETA_DESTINO = "C:/Users/guillermo.fernandez/Desktop/MSDS/celec/descargas_celec"
os.makedirs(CARPETA_DESTINO, exist_ok=True)

# Traducción de mes corto
def obtener_nombre_mes_corto(mes_num):
    return {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }.get(mes_num)

def descargar_csv_celec(**context):
    # Usar la fecha de ejecución del DAG o fecha actual
    fecha = context.get("execution_date", datetime.today())
    fecha_str = fecha.strftime("%Y-%m-%d")
    fecha_str_nombre = fecha.strftime("%d-%m-%Y")
    dia = str(int(fecha.strftime("%d")))
    mes = obtener_nombre_mes_corto(fecha.month)
    anio = str(fecha.year)

    print(f"📅 Descargando datos para: {fecha_str}")

    # Configurar navegador
    options = Options()
    options.headless = True  # O False si quieres ver el navegador
    options.binary_location = FIREFOX_PATH
    service = Service(GECKO_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get("https://generacioncsr.celec.gob.ec/graficasproduccion/")
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)

        # Calendario
        wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "calendar"))).click()
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ngb-dp-day")))
        time.sleep(1)

        Select(wait.until(EC.presence_of_element_located((By.XPATH, '//select[@title="Select year"]')))).select_by_visible_text(anio)
        Select(wait.until(EC.presence_of_element_located((By.XPATH, '//select[@title="Select month"]')))).select_by_visible_text(mes)

        WebDriverWait(driver, 10).until(
            lambda d: mes.lower() in d.find_element(By.CLASS_NAME, "ngb-dp-navigation-select-month").text.lower()
                      and anio in d.find_element(By.CLASS_NAME, "ngb-dp-navigation-select-year").text
        )

        dias = driver.find_elements(By.CLASS_NAME, "ngb-dp-day")
        for d in dias:
            clases = d.get_attribute("class")
            if (
                d.text == dia and
                "disabled" not in clases and
                "hidden" not in clases and
                "outside" not in clases
            ):
                d.click()
                break

        time.sleep(5)
        boton_csv = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[contains(text(),"Descargar CSV")]')))
        boton_csv.click()

        time.sleep(3)
        enlace_csv = driver.execute_script("""
            const links = document.querySelectorAll('a');
            for (const link of links) {
                if (link.href.includes('.csv')) return link.href;
            }
            return null;
        """)

        if enlace_csv:
            nombre_archivo = f"Produccion_CSR_{fecha_str_nombre}.csv"
            ruta_local = os.path.join(CARPETA_DESTINO, nombre_archivo)
            if os.path.exists(ruta_local):
                print(f"🟡 Ya existe: {ruta_local}")
            else:
                respuesta = requests.get(enlace_csv)
                if respuesta.status_code == 200:
                    with open(ruta_local, "wb") as f:
                        f.write(respuesta.content)
                    print(f"✅ Guardado: {ruta_local}")
                else:
                    print(f"⚠️ Error HTTP: {respuesta.status_code}")
        else:
            print("⚠️ No se generó enlace al CSV.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        driver.quit()


# Definición del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_descarga_celec',
    default_args=default_args,
    description='Descarga diaria de CSV desde portal CELEC',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['celec', 'webscraping'],
) as dag:

    tarea_descarga_csv = PythonOperator(
        task_id='descargar_csv_diario',
        python_callable=descargar_csv_celec,
        provide_context=True
    )

    tarea_descarga_csv