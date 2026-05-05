"""
DAG: dbd_v3_final_pipeline
============================
Pipeline V3 FINAL — Triple-Source ETL dengan Proper Staging Layer.

ARSITEKTUR:
  MySQL (BPS) + PostgreSQL (GEE) + GeoJSON (Spatial)
      ↓ 3x PARALLEL EXTRACT
  4x STAGING TABLES (staging_dbd_v3.*)
      ↓ VALIDATION
  MERGE + TRANSFORM (feature engineering)
      ↓
  STAR SCHEMA (warehouse_dbd_v3.*)
      ↓
  Metabase (OLAP + Map)

GRAIN FACT TABLE:
  "Satu row fact_dbd_env merepresentasikan satu kabupaten/kota
   pada satu tahun observasi (2019-2024)."
  Total expected: 27 kab/kota × 6 tahun = 162 rows.

ISOLASI: AIRFLOW_HOME = docs_v3/airflow_home, port 8095
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine, text
import sys
import os
from pathlib import Path

_DOCS_V3_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_DOCS_V3_DIR))

from config.db_config import (
    get_mysql_source_engine, get_pg_engine,
    get_data_path, get_geojson_path, get_docs_v3_dir,
    STAGING_SCHEMA, WAREHOUSE_SCHEMA,
)

TEMP_DIR = get_docs_v3_dir() / 'temp'
TEMP_DIR.mkdir(exist_ok=True)

STG = STAGING_SCHEMA      # "staging_dbd_v3"
WH = WAREHOUSE_SCHEMA     # "warehouse_dbd_v3"

# ── Dynamic year range ─────────────────────────────────────────
# To add 2025: set YEAR_END=2025 in .env, re-run ingest_sources.py, trigger DAG.
# No Python code change required.
YEAR_START     = int(os.getenv('YEAR_START', 2019))
YEAR_END       = int(os.getenv('YEAR_END',   2024))
PANDEMIC_YEARS = set(
    int(y) for y in os.getenv('PANDEMIC_YEARS', '2020,2021').split(',')
)

default_args = {
    'owner': 'pipeline_v3',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


@dag(
    'dbd_v3_final_pipeline',
    default_args=default_args,
    description='Pipeline V3 FINAL — Triple-Source ETL + Staging + Star Schema',
    schedule_interval='@monthly',
    catchup=False,
    tags=['dbd', 'v3', 'final', 'spatial'],
)
def dbd_v3_pipeline():

    # ================================================================
    # STEP 4 — EXTRACT (3 paralel)
    # ================================================================

    @task
    def extract_from_mysql():
        """Extract data BPS dari MySQL source_os_dbd.
        INPUT:  MySQL tables (source_bps_health, source_bps_kepadatan, source_bps_luas)
        OUTPUT: temp CSV files
        """
        eng = get_mysql_source_engine()
        df_h = pd.read_sql("SELECT kode_bps, nama_kabupaten_kota, tahun, jumlah_kasusDBD FROM source_bps_health", eng)
        df_k = pd.read_sql("SELECT kode_bps, tahun, kepadatan_penduduk FROM source_bps_kepadatan", eng)
        df_l = pd.read_sql("SELECT kode_bps, tahun, luas_km2 FROM source_bps_luas", eng)
        df_h.to_csv(str(TEMP_DIR / 'raw_health.csv'), index=False)
        df_k.to_csv(str(TEMP_DIR / 'raw_kepadatan.csv'), index=False)
        df_l.to_csv(str(TEMP_DIR / 'raw_luas.csv'), index=False)
        return "mysql_ok"

    @task
    def extract_from_postgres():
        """Extract data GEE dari PostgreSQL source_gee_env.
        INPUT:  PostgreSQL dwh_dbd_v3.public.source_gee_env
        OUTPUT: temp CSV file
        """
        eng = get_pg_engine()
        df = pd.read_sql("SELECT * FROM source_gee_env", eng)
        df.to_csv(str(TEMP_DIR / 'raw_gee.csv'), index=False)
        return "pg_ok"

    @task
    def extract_geojson():
        """Extract spatial data dari GeoJSON.
        INPUT:  data/jabar_kabkot (27 MultiPolygon features)
        PROSES: geopandas read → centroid → lat/lon
        OUTPUT: temp CSV (27 rows)
        """
        import geopandas as gpd
        gdf = gpd.read_file(str(get_geojson_path()))
        gdf['centroid'] = gdf.geometry.centroid
        gdf['latitude'] = gdf['centroid'].y
        gdf['longitude'] = gdf['centroid'].x
        gdf['kode_bps'] = gdf['ID_KAB'].astype(int).astype(str)
        result = gdf[['kode_bps', 'KABKOT', 'latitude', 'longitude']].copy()
        result = result.rename(columns={'KABKOT': 'nama_geo'})
        result.to_csv(str(TEMP_DIR / 'raw_geojson.csv'), index=False)
        return "geojson_ok"

    # ================================================================
    # STEP 5 — STAGING LOAD (clean + standardize + load)
    # ================================================================

    @task
    def load_staging(mysql_status, pg_status, geo_status):
        """Load ke 4 staging tables dengan cleaning & standardisasi.

        STAGING RULES:
          ✅ rename kolom, normalize kode_bps, cast type, handle null, dedup
          ❌ TIDAK boleh: CVI, incidence_rate, lag features
        """
        eng = get_pg_engine()
        region_map = pd.read_csv(get_data_path('region_mapping.csv'))
        region_map['kode_bps'] = region_map['kode_bps'].astype(str)

        # --- stg_health_dbd ---
        df_h = pd.read_csv(str(TEMP_DIR / 'raw_health.csv'))
        df_h['kode_bps'] = df_h['kode_bps'].astype(str)
        df_h = df_h[df_h['tahun'].between(YEAR_START, YEAR_END)]  # dynamic
        df_h = df_h.rename(columns={'jumlah_kasusDBD': 'jumlah_kasus_dbd'})
        df_h = df_h[['kode_bps', 'nama_kabupaten_kota', 'tahun', 'jumlah_kasus_dbd']]
        df_h = df_h.drop_duplicates(subset=['kode_bps', 'tahun'])
        df_h.to_sql('stg_health_dbd', eng, schema=STG, if_exists='replace', index=False)

        # --- stg_demografi ---
        df_k = pd.read_csv(str(TEMP_DIR / 'raw_kepadatan.csv'))
        df_l = pd.read_csv(str(TEMP_DIR / 'raw_luas.csv'))
        df_k['kode_bps'] = df_k['kode_bps'].astype(str)
        df_l['kode_bps'] = df_l['kode_bps'].astype(str)
        df_k = df_k[df_k['tahun'].between(YEAR_START, YEAR_END)]  # dynamic
        df_l = df_l[df_l['tahun'].between(YEAR_START, YEAR_END)]  # dynamic
        df_demo = df_k.merge(df_l[['kode_bps', 'tahun', 'luas_km2']],
                             on=['kode_bps', 'tahun'], how='left')
        df_demo = df_demo[['kode_bps', 'tahun', 'kepadatan_penduduk', 'luas_km2']]
        df_demo = df_demo.drop_duplicates(subset=['kode_bps', 'tahun'])
        df_demo.to_sql('stg_demografi', eng, schema=STG, if_exists='replace', index=False)

        # --- stg_environment ---
        df_gee = pd.read_csv(str(TEMP_DIR / 'raw_gee.csv'))
        df_gee.columns = [c.lower() for c in df_gee.columns]
        df_gee = df_gee.rename(columns={'adm2_name': 'nama_env', 'year': 'tahun'})
        df_gee['tahun'] = df_gee['tahun'].astype(int)
        # Map nama_env → kode_bps via region_mapping
        df_gee = df_gee.merge(region_map[['kode_bps', 'nama_env']],
                              on='nama_env', how='left')
        # [PATCH-4] Warn on unmatched GEE regions (silent drop prevention)
        unmatched_env = df_gee[df_gee['kode_bps'].isna()]['nama_env'].unique()
        if len(unmatched_env) > 0:
            print(f"[WARNING] GEE mapping: {len(unmatched_env)} unmatched nama_env (rows will be dropped):")
            for _name in unmatched_env:
                print(f"  → '{_name}'  — fix in region_mapping.csv")
        df_gee = df_gee[df_gee['kode_bps'].notna()]
        df_gee = df_gee[['kode_bps', 'tahun', 'rainfall_annual', 'lst_celsius', 'ndvi', 'ndmi']]
        df_gee = df_gee.drop_duplicates(subset=['kode_bps', 'tahun'])
        df_gee.to_sql('stg_environment', eng, schema=STG, if_exists='replace', index=False)

        # --- stg_geojson ---
        df_geo = pd.read_csv(str(TEMP_DIR / 'raw_geojson.csv'))
        df_geo['kode_bps'] = df_geo['kode_bps'].astype(str)
        df_geo = df_geo.drop_duplicates(subset=['kode_bps'])
        df_geo.to_sql('stg_geojson', eng, schema=STG, if_exists='replace', index=False)

        return "staging_ok"

    # ================================================================
    # STEP 6 — VALIDATION
    # ================================================================

    @task
    def validate_staging(staging_status):
        """Validate staging data sebelum transform.

        CHECKS:
          1. Duplicate kode_bps+tahun
          2. Null kode_bps
          3. Row count minimum
          4. Referential: semua kode_bps di health ada di geojson
        """
        eng = get_pg_engine()
        errors = []

        # Duplicate check
        for tbl, cols in [('stg_health_dbd', 'kode_bps, tahun'),
                          ('stg_demografi', 'kode_bps, tahun'),
                          ('stg_environment', 'kode_bps, tahun')]:
            q = f"""SELECT {cols}, COUNT(*) AS cnt
                    FROM {STG}.{tbl} GROUP BY {cols} HAVING COUNT(*) > 1"""
            dups = pd.read_sql(q, eng)
            if len(dups) > 0:
                errors.append(f"DUPLICATE in {tbl}: {len(dups)} groups")

        # Null check
        for tbl in ['stg_health_dbd', 'stg_demografi', 'stg_environment']:
            q = f"SELECT COUNT(*) AS n FROM {STG}.{tbl} WHERE kode_bps IS NULL"
            n = pd.read_sql(q, eng).iloc[0]['n']
            if n > 0:
                errors.append(f"NULL kode_bps in {tbl}: {n} rows")

        # Row count
        counts = {}
        for tbl in ['stg_health_dbd', 'stg_demografi', 'stg_environment', 'stg_geojson']:
            q = f"SELECT COUNT(*) AS n FROM {STG}.{tbl}"
            counts[tbl] = pd.read_sql(q, eng).iloc[0]['n']

        # [PATCH-3] Dynamic threshold: 80% of expected grain (n_regions × n_years)
        n_years_range = YEAR_END - YEAR_START + 1
        n_regions     = counts['stg_geojson']          # validated below
        expected_min  = int(n_regions * n_years_range * 0.80)
        if counts['stg_health_dbd'] < expected_min:
            errors.append(
                f"stg_health_dbd too few rows: {counts['stg_health_dbd']} "
                f"(expected >= {expected_min} = {n_regions}×{n_years_range}×80%)"
            )
        if counts['stg_geojson'] != 27:
            errors.append(f"stg_geojson expected 27, got: {counts['stg_geojson']}")

        # Referential: health kode_bps ⊂ geojson kode_bps
        q = f"""SELECT DISTINCT h.kode_bps FROM {STG}.stg_health_dbd h
                LEFT JOIN {STG}.stg_geojson g ON h.kode_bps = g.kode_bps
                WHERE g.kode_bps IS NULL"""
        orphans = pd.read_sql(q, eng)
        if len(orphans) > 0:
            errors.append(f"Orphan kode_bps (no geo): {list(orphans['kode_bps'])}")

        if errors:
            raise ValueError(f"VALIDATION FAILED:\n" + "\n".join(errors))

        return f"valid|health={counts['stg_health_dbd']}|demo={counts['stg_demografi']}|env={counts['stg_environment']}|geo={counts['stg_geojson']}"

    # ================================================================
    # STEP 7 — TRANSFORM (feature engineering)
    # ================================================================

    @task
    def transform(validation_status):
        """Merge staging tables + feature engineering.

        INPUT:  4 staging tables
        OUTPUT: temp/panel_final.csv

        FEATURES BUILT:
          incidence_rate, rainfall_mm, rainfall_zscore,
          lst_quartile_rank, ndvi_class, water_body_density_tier,
          lag1-3_rainfall, CVI, yoy_growth_rate,
          kepadatan_quartile, density_tier, is_imputed,
          ir_q, endemic_persistence_score
        """
        eng = get_pg_engine()

        # Read staging
        df_h = pd.read_sql(f"SELECT * FROM {STG}.stg_health_dbd", eng)
        df_d = pd.read_sql(f"SELECT * FROM {STG}.stg_demografi", eng)
        df_e = pd.read_sql(f"SELECT * FROM {STG}.stg_environment", eng)
        df_g = pd.read_sql(f"SELECT * FROM {STG}.stg_geojson", eng)

        for df in [df_h, df_d, df_e, df_g]:
            df['kode_bps'] = df['kode_bps'].astype(str)

        # MERGE: health + demografi + environment + geojson
        panel = df_h.merge(df_d, on=['kode_bps', 'tahun'], how='left')
        panel = panel.merge(df_e, on=['kode_bps', 'tahun'], how='left')
        panel = panel.merge(df_g[['kode_bps', 'latitude', 'longitude']],
                            on='kode_bps', how='left')

        # Mark imputed rows (kode_bps without GEE data)
        panel['is_imputed'] = panel['rainfall_annual'].isna()

        # TOBLER IMPUTATION (3217=KBB←3204=Bandung, 3218=Pangandaran←3207=Ciamis)
        env_vars = ['rainfall_annual', 'lst_celsius', 'ndvi', 'ndmi']
        for var in env_vars:
            d3204 = panel[panel['kode_bps'] == '3204'].set_index('tahun')[var].to_dict()
            d3207 = panel[panel['kode_bps'] == '3207'].set_index('tahun')[var].to_dict()
            panel.loc[(panel['kode_bps'] == '3217') & panel[var].isna(), var] = panel['tahun'].map(d3204)
            panel.loc[(panel['kode_bps'] == '3218') & panel[var].isna(), var] = panel['tahun'].map(d3207)

        # [PATCH-2] Post-imputation NaN integrity check
        # Detects rows with missing env data that slipped through imputation.
        env_cols = ['rainfall_annual', 'lst_celsius', 'ndvi', 'ndmi']
        leaking = panel[
            (panel['is_imputed'] == False) & panel[env_cols].isnull().any(axis=1)
        ][['kode_bps', 'tahun'] + env_cols]
        if not leaking.empty:
            print(f"[WARNING] {len(leaking)} rows have NaN env values but is_imputed=False — correcting flag:")
            print(leaking.to_string(index=False))
            panel.loc[leaking.index, 'is_imputed'] = True  # correct the flag

        # --- FEATURE ENGINEERING ---
        panel['rainfall_mm'] = panel['rainfall_annual']

        # Incidence Rate = (kasus / populasi) × 100.000
        panel['incidence_rate'] = (
            panel['jumlah_kasus_dbd']
            / (panel['kepadatan_penduduk'] * panel['luas_km2'])
        ) * 100000

        # YoY Growth Rate
        panel = panel.sort_values(['kode_bps', 'tahun'])
        panel['yoy_growth_rate'] = (
            panel.groupby('kode_bps')['incidence_rate'].pct_change() * 100
        )

        # Rainfall Z-Score (per tahun)
        panel['rainfall_zscore'] = panel.groupby('tahun')['rainfall_mm'].transform(
            lambda x: stats.zscore(x, nan_policy='omit')
        )

        # LST Quartile Rank
        panel['lst_quartile_rank'] = panel.groupby('tahun')['lst_celsius'].transform(
            lambda x: pd.qcut(x, 4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
        )

        # NDVI Class
        panel['ndvi_class'] = panel['ndvi'].apply(
            lambda v: 'Sparse' if pd.notna(v) and v < 0.4
            else ('Moderate' if pd.notna(v) and v <= 0.6 else ('Dense' if pd.notna(v) else None))
        )

        # Water Body Density Tier
        panel['water_body_density_tier'] = panel.groupby('tahun')['ndmi'].transform(
            lambda x: pd.qcut(x, 3, labels=['Low', 'Med', 'High'])
            if x.notna().nunique() >= 3 else np.nan
        )

        # Lag 1-3 Rainfall
        panel = panel.sort_values(['kode_bps', 'tahun'])
        panel['lag1_rainfall'] = panel.groupby('kode_bps')['rainfall_mm'].shift(1).fillna(panel['rainfall_mm'])
        panel['lag2_rainfall'] = panel.groupby('kode_bps')['rainfall_mm'].shift(2).fillna(panel['lag1_rainfall'])
        panel['lag3_rainfall'] = panel.groupby('kode_bps')['rainfall_mm'].shift(3).fillna(panel['lag2_rainfall'])

        # CVI (Composite Vulnerability Index)
        z_col = lambda col: panel.groupby('tahun')[col].transform(lambda x: (x - x.mean()) / (x.std() + 1e-9))
        panel['composite_vulnerability_index'] = (
            0.3 * panel['rainfall_zscore']
            + 0.25 * z_col('lst_celsius')
            + 0.25 * z_col('kepadatan_penduduk')
            + 0.2 * (1 - z_col('ndvi'))
        )

        # Kepadatan Quartile + Tier
        panel['kepadatan_quartile'] = pd.qcut(
            panel['kepadatan_penduduk'], 4, labels=['Q1', 'Q2', 'Q3', 'Q4']
        )
        panel['density_tier'] = panel['kepadatan_quartile'].map(
            {'Q1': 'Rendah', 'Q2': 'Sedang', 'Q3': 'Tinggi', 'Q4': 'Sangat Tinggi'}
        )

        # IR Quartile + Endemic Persistence
        panel['ir_q'] = panel.groupby('tahun')['incidence_rate'].transform(
            lambda x: pd.qcut(x, 4, labels=[1, 2, 3, 4])
        )
        panel['endemic_persistence_score'] = panel.groupby('kode_bps')['ir_q'].transform(
            lambda x: (x == 4).sum()
        )

        panel['is_imputed'] = panel['is_imputed'].astype(bool)

        out = str(TEMP_DIR / 'panel_final.csv')
        panel.to_csv(out, index=False)
        return out

    # ================================================================
    # STEP 8 — LOAD DWH (Star Schema)
    # ================================================================

    @task
    def load_dwh(transform_path):
        """Load Star Schema ke warehouse_dbd_v3.

        ORDER: truncate → dim_wilayah → dim_waktu → fact_dbd_env
        FK: fact.kode_bps → dim_wilayah, fact.tahun → dim_waktu
        """
        eng = get_pg_engine()
        panel = pd.read_csv(transform_path)
        panel['kode_bps'] = panel['kode_bps'].astype(str)

        # TRUNCATE
        with eng.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {WH}.fact_dbd_env CASCADE;"))
            conn.execute(text(f"TRUNCATE TABLE {WH}.dim_wilayah CASCADE;"))
            conn.execute(text(f"TRUNCATE TABLE {WH}.dim_waktu CASCADE;"))

        # dim_wilayah (with lat/lon from GeoJSON)
        dim_wil = panel[['kode_bps', 'nama_kabupaten_kota',
                          'latitude', 'longitude']].drop_duplicates(subset='kode_bps')
        dim_wil['kode_provinsi'] = '32'
        dim_wil['nama_provinsi'] = 'JAWA BARAT'
        dim_wil.to_sql('dim_wilayah', eng, schema=WH, if_exists='append', index=False)

        # dim_waktu
        years = sorted(panel['tahun'].unique())
        dim_wkt = pd.DataFrame({
            'tahun': years,
            'covid_dummy':         [1 if t in PANDEMIC_YEARS else 0 for t in years],      # dynamic
            'keterangan_periode':  ['Pandemi' if t in PANDEMIC_YEARS else 'Normal' for t in years],  # dynamic
        })
        dim_wkt.to_sql('dim_waktu', eng, schema=WH, if_exists='append', index=False)

        # fact_dbd_env
        fact_cols = [
            'kode_bps', 'tahun', 'jumlah_kasus_dbd', 'incidence_rate',
            'kepadatan_penduduk', 'luas_km2', 'rainfall_mm', 'rainfall_zscore',
            'lst_celsius', 'lst_quartile_rank', 'ndvi', 'ndvi_class',
            'ndmi', 'water_body_density_tier',
            'lag1_rainfall', 'lag2_rainfall', 'lag3_rainfall',
            'composite_vulnerability_index', 'yoy_growth_rate',
            'kepadatan_quartile', 'density_tier',
            'is_imputed', 'ir_q', 'endemic_persistence_score',
        ]
        fact = panel[fact_cols].copy()
        fact['is_imputed'] = fact['is_imputed'].fillna(False).astype(bool)
        fact.to_sql('fact_dbd_env', eng, schema=WH, if_exists='append', index=False)

        # [PATCH-5] Write pipeline run metadata for observability
        imputed_count = int(fact['is_imputed'].sum())
        null_ir_count = int(fact['incidence_rate'].isna().sum())
        run_meta = pd.DataFrame([{
            'run_at':       pd.Timestamp.now(),
            'year_start':   YEAR_START,
            'year_end':     YEAR_END,
            'n_regions':    len(dim_wil),
            'n_years':      len(dim_wkt),
            'fact_rows':    len(fact),
            'imputed_rows': imputed_count,
            'null_ir_rows': null_ir_count,
            'status':       'SUCCESS',
        }])
        run_meta.to_sql('pipeline_runs', eng, schema=WH, if_exists='append', index=False)

        return (
            f"dwh_ok | wil={len(dim_wil)} | wkt={len(dim_wkt)} | fact={len(fact)}"
            f" | imputed={imputed_count} | null_ir={null_ir_count}"
        )

    @task
    def cleanup(dwh_status):
        for f in TEMP_DIR.glob('*.csv'):
            f.unlink()

    # ================================================================
    # WIRING — DEPENDENCY GRAPH
    # ================================================================
    t_mysql = extract_from_mysql()
    t_pg = extract_from_postgres()
    t_geo = extract_geojson()

    t_staging = load_staging(t_mysql, t_pg, t_geo)
    t_valid = validate_staging(t_staging)
    t_transform = transform(t_valid)
    t_dwh = load_dwh(t_transform)
    cleanup(t_dwh)


dbd_v3_pipeline()
