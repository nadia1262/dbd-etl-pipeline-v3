"""
ingest_sources.py — Pipeline V3
=================================
Script one-off: load raw CSV ke source databases.

DATA FLOW:
  CSV files → MySQL (source_os_dbd): BPS health + demografi + luas
  CSV file  → PostgreSQL (dwh_dbd_v3.public): GEE environmental

Jalankan SEKALI sebelum trigger DAG Airflow.
"""
import sys
from pathlib import Path

_DOCS_V3_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_DOCS_V3_DIR))

import pandas as pd
from config.db_config import get_mysql_source_engine, get_pg_engine, get_data_path


def ingest():
    print("=" * 60)
    print("INGESTING RAW DATA TO SOURCE DATABASES")
    print("=" * 60)

    mysql_eng = get_mysql_source_engine()
    pg_eng = get_pg_engine()

    # --- MySQL: BPS Health ---
    df = pd.read_csv(get_data_path('dbd_jabar.csv'))
    df.to_sql('source_bps_health', mysql_eng, if_exists='replace', index=False)
    print(f"[1/4] MySQL source_bps_health: {len(df)} rows")

    # --- MySQL: BPS Kepadatan ---
    df = pd.read_csv(get_data_path('kepadatan_penduduk_jabar.csv'))
    df.to_sql('source_bps_kepadatan', mysql_eng, if_exists='replace', index=False)
    print(f"[2/4] MySQL source_bps_kepadatan: {len(df)} rows")

    # --- MySQL: BPS Luas ---
    df = pd.read_excel(get_data_path('LuasDaerah_KabKot_Jabar.xlsx'))
    df.to_sql('source_bps_luas', mysql_eng, if_exists='replace', index=False)
    print(f"[3/4] MySQL source_bps_luas: {len(df)} rows")

    # --- PostgreSQL: GEE Environment ---
    df = pd.read_csv(get_data_path('Data_Tahunan_DBD_Jabar_2019_2024.csv'))
    df.columns = [c.lower() for c in df.columns]
    df.to_sql('source_gee_env', pg_eng, if_exists='replace', index=False)
    print(f"[4/4] PostgreSQL source_gee_env: {len(df)} rows")

    print("=" * 60)
    print("INGESTION COMPLETE")


if __name__ == "__main__":
    ingest()
