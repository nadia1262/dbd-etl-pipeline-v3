-- ============================================================
-- DDL: Pipeline V3 — Database dwh_dbd_v3
-- 2 Schema: staging_dbd_v3 + warehouse_dbd_v3
-- ============================================================

-- Schema 1: STAGING
CREATE SCHEMA IF NOT EXISTS staging_dbd_v3;

CREATE TABLE IF NOT EXISTS staging_dbd_v3.stg_health_dbd (
    kode_bps            VARCHAR(10),
    nama_kabupaten_kota VARCHAR(100),
    tahun               INT,
    jumlah_kasus_dbd    INT
);

CREATE TABLE IF NOT EXISTS staging_dbd_v3.stg_demografi (
    kode_bps            VARCHAR(10),
    tahun               INT,
    kepadatan_penduduk  FLOAT,
    luas_km2            FLOAT
);

CREATE TABLE IF NOT EXISTS staging_dbd_v3.stg_environment (
    kode_bps            VARCHAR(10),
    tahun               INT,
    rainfall_annual     FLOAT,
    lst_celsius         FLOAT,
    ndvi                FLOAT,
    ndmi                FLOAT
);

CREATE TABLE IF NOT EXISTS staging_dbd_v3.stg_geojson (
    kode_bps            VARCHAR(10),
    nama_geo            VARCHAR(100),
    latitude            FLOAT,
    longitude           FLOAT
);

-- Schema 2: WAREHOUSE (Star Schema)
CREATE SCHEMA IF NOT EXISTS warehouse_dbd_v3;

CREATE TABLE IF NOT EXISTS warehouse_dbd_v3.dim_wilayah (
    kode_bps            VARCHAR(10) PRIMARY KEY,
    nama_kabupaten_kota VARCHAR(100),
    kode_provinsi       VARCHAR(10),
    nama_provinsi       VARCHAR(50),
    latitude            FLOAT,
    longitude           FLOAT
);

CREATE TABLE IF NOT EXISTS warehouse_dbd_v3.dim_waktu (
    tahun               INT PRIMARY KEY,
    covid_dummy         INT,
    keterangan_periode  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS warehouse_dbd_v3.fact_dbd_env (
    fact_id                       SERIAL PRIMARY KEY,
    kode_bps                      VARCHAR(10) REFERENCES warehouse_dbd_v3.dim_wilayah(kode_bps),
    tahun                         INT REFERENCES warehouse_dbd_v3.dim_waktu(tahun),
    jumlah_kasus_dbd              INT,
    incidence_rate                FLOAT,
    kepadatan_penduduk            FLOAT,
    luas_km2                      FLOAT,
    rainfall_mm                   FLOAT,
    rainfall_zscore               FLOAT,
    lst_celsius                   FLOAT,
    lst_quartile_rank             VARCHAR(5),
    ndvi                          FLOAT,
    ndvi_class                    VARCHAR(20),
    ndmi                          FLOAT,
    water_body_density_tier       VARCHAR(10),
    lag1_rainfall                 FLOAT,
    lag2_rainfall                 FLOAT,
    lag3_rainfall                 FLOAT,
    composite_vulnerability_index FLOAT,
    yoy_growth_rate               FLOAT,
    kepadatan_quartile            VARCHAR(5),
    density_tier                  VARCHAR(20),
    is_imputed                    BOOLEAN,
    ir_q                          INT,
    endemic_persistence_score     INT
);

-- [PATCH-5] Pipeline run metadata — lightweight observability table
-- Populated automatically at the end of each load_dwh() execution.
CREATE TABLE IF NOT EXISTS warehouse_dbd_v3.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    run_at          TIMESTAMP DEFAULT NOW(),
    year_start      INT,
    year_end        INT,
    n_regions       INT,
    n_years         INT,
    fact_rows       INT,
    imputed_rows    INT,
    null_ir_rows    INT,
    status          VARCHAR(20)
);

-- DQ Report query (run in pgAdmin / Metabase after each pipeline run):
-- SELECT
--     run_at::DATE                                        AS run_date,
--     year_start || '–' || year_end                       AS year_range,
--     fact_rows,
--     n_regions * n_years                                 AS expected_rows,
--     fact_rows - (n_regions * n_years)                   AS row_gap,
--     ROUND(imputed_rows * 100.0 / NULLIF(fact_rows,0),1) AS pct_imputed,
--     ROUND(null_ir_rows * 100.0 / NULLIF(fact_rows,0),1) AS pct_null_ir,
--     status
-- FROM warehouse_dbd_v3.pipeline_runs
-- ORDER BY run_at DESC LIMIT 10;
