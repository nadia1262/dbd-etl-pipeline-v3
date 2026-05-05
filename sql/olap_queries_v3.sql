-- ============================================================
-- VALIDATION & OLAP QUERIES — Pipeline V3
-- Target: PostgreSQL → dwh_dbd_v3
-- ============================================================

-- ========== DATA INTEGRITY ==========

-- Row counts
SELECT 'stg_health_dbd' AS tabel, COUNT(*) AS rows FROM staging_dbd_v3.stg_health_dbd
UNION ALL SELECT 'stg_demografi', COUNT(*) FROM staging_dbd_v3.stg_demografi
UNION ALL SELECT 'stg_environment', COUNT(*) FROM staging_dbd_v3.stg_environment
UNION ALL SELECT 'stg_geojson', COUNT(*) FROM staging_dbd_v3.stg_geojson
UNION ALL SELECT 'dim_wilayah', COUNT(*) FROM warehouse_dbd_v3.dim_wilayah
UNION ALL SELECT 'dim_waktu', COUNT(*) FROM warehouse_dbd_v3.dim_waktu
UNION ALL SELECT 'fact_dbd_env', COUNT(*) FROM warehouse_dbd_v3.fact_dbd_env;

-- Spatial check: lat/lon populated
SELECT kode_bps, nama_kabupaten_kota, latitude, longitude
FROM warehouse_dbd_v3.dim_wilayah
ORDER BY kode_bps LIMIT 10;

-- FK integrity
SELECT COUNT(*) AS orphan_facts FROM warehouse_dbd_v3.fact_dbd_env f
LEFT JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
WHERE w.kode_bps IS NULL;

-- ========== OLAP QUERIES (sesuai olap.html) ==========

-- UC1: Tren IR + Rainfall Anomaly
SELECT w.nama_kabupaten_kota, f.tahun, f.incidence_rate,
       f.yoy_growth_rate, f.rainfall_zscore,
       CASE WHEN f.yoy_growth_rate > 50 AND f.rainfall_zscore > 1.5
            THEN 'HIGH RISK' ELSE 'Normal' END AS surge_flag
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
ORDER BY f.yoy_growth_rate DESC NULLS LAST;

-- UC7: CVI + Map
SELECT w.nama_kabupaten_kota, w.latitude, w.longitude,
       AVG(f.composite_vulnerability_index) AS avg_cvi,
       SUM(f.jumlah_kasus_dbd) AS total_kasus
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
GROUP BY w.nama_kabupaten_kota, w.latitude, w.longitude
ORDER BY avg_cvi DESC;

-- COVID analysis
SELECT t.keterangan_periode, AVG(f.incidence_rate) AS avg_ir,
       SUM(f.jumlah_kasus_dbd) AS total_kasus
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_waktu t ON f.tahun = t.tahun
GROUP BY t.keterangan_periode;

-- Metabase PIN MAP query
-- [PATCH-6] Dynamic year anchor — always shows the latest available year.
-- When 2025 data is loaded, this query auto-updates with zero manual change.
SELECT w.nama_kabupaten_kota, w.latitude, w.longitude,
       f.tahun, f.jumlah_kasus_dbd, f.incidence_rate,
       f.composite_vulnerability_index
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
WHERE f.tahun = (SELECT MAX(tahun) FROM warehouse_dbd_v3.fact_dbd_env)
ORDER BY f.incidence_rate DESC;
