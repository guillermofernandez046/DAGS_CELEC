#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 13 04:02:24 2025

@author: guillermofernandez
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

from tqdm import tqdm  # Asegúrate de tener instalado tqdm en el entorno Airflow

# 🔧 Ruta base donde están los archivos diarios
CARPETA_CSV_BASE = "C:/Users/guillermo.fernandez/Desktop/MSDS/celec/descargas_celec"

def compilar_csv_por_anio():
    carpeta_salida = os.path.join(CARPETA_CSV_BASE, "combinados_por_anio")
    os.makedirs(carpeta_salida, exist_ok=True)

    # Buscar todos los archivos .csv dentro de subcarpetas
    archivos_csv = []
    for root, dirs, files in os.walk(CARPETA_CSV_BASE):
        for file in files:
            if (file.startswith("Produccion_CSR_") or file.startswith("Producción_CSR_")) and file.endswith(".csv"):
                archivos_csv.append(os.path.join(root, file))

    print(f"\n🔍 Archivos CSV encontrados: {len(archivos_csv)}")

    archivos_por_anio = {}
    for ruta_completa in archivos_csv:
        archivo = os.path.basename(ruta_completa)
        partes = archivo.replace(".csv", "").split("_")[-1].split("-")
        if len(partes) == 3:
            anio = partes[2]
            archivos_por_anio.setdefault(anio, []).append(ruta_completa)
        else:
            print(f"⚠️ Nombre no válido: {archivo}")

    for anio, rutas in archivos_por_anio.items():
        print(f"\n📦 Procesando año {anio} con {len(rutas)} archivos...")
        df_total = pd.DataFrame()

        for i, ruta in enumerate(sorted(rutas)):
            try:
                df = pd.read_csv(ruta)
                df['Archivo_Origen'] = os.path.basename(ruta)
                df_total = pd.concat([df_total, df], ignore_index=True)

                pct = int((i + 1)/len(rutas) * 100)
                print(f"✅ {os.path.basename(ruta)} ({pct}%)")
            except Exception as e:
                print(f"❌ Error al leer {ruta}: {e}")

        if not df_total.empty:
            salida_csv = os.path.join(carpeta_salida, f"Produccion_CSR_{anio}.csv")
            df_total.to_csv(salida_csv, index=False)
            print(f"✅ Guardado: {salida_csv}")
            print(f"📊 Total registros: {len(df_total):,}")
        else:
            print(f"⚠️ No se generó archivo para {anio}")

# ======================
# DAG DEFINITION
# ======================
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_compilar_celec',
    default_args=default_args,
    description='DAG para compilar archivos CELEC por año',
    schedule_interval='@monthly',  # Puedes cambiarlo a @once o '0 1 1 1 *' si lo deseas anual
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['celec', 'compilacion', 'csv'],
) as dag:

    tarea_compilar = PythonOperator(
        task_id='compilar_csv_por_anio',
        python_callable=compilar_csv_por_anio
    )

    tarea_compilar