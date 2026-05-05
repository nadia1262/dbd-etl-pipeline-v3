# Setup Guide — Pipeline V3 FINAL (ISOLATED)

---

## Status Database

| Database | Status |
|----------|--------|
| `dwh_dbd_v3` (PostgreSQL) | ✅ Created |
| Schema `staging_dbd_v3` | ✅ 4 tables |
| Schema `warehouse_dbd_v3` | ✅ 3 tables (dim_wilayah, dim_waktu, fact_dbd_env) |
| `source_os_dbd` (MySQL) | ✅ Created |
| V1 `dwh_dbd` | ✅ UNTOUCHED |
| V2 `dwh_dbd_v2` | ✅ UNTOUCHED |

---

## STEP 1 — Buat WSL venv + Install Dependencies

```bash
wsl
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
python3 -m venv venv_wsl
source venv_wsl/bin/activate
pip install -r requirements.txt
```

---

## STEP 2 — Ingest Raw Data ke Source Databases

```bash
# Masih di WSL, venv aktif
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
python scripts/ingest_sources.py
```

Expected output:
```
[1/4] MySQL source_bps_health: 243 rows
[2/4] MySQL source_bps_kepadatan: 216 rows
[3/4] MySQL source_bps_luas: ...
[4/4] PostgreSQL source_gee_env: 150 rows
INGESTION COMPLETE
```

---

## STEP 3 — Setup Airflow V3 (port 8095)

### Terminal 1 — Init

```bash
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
source venv_wsl/bin/activate
export AIRFLOW_HOME="/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/airflow_home"

airflow db init

sed -i "s|dags_folder = .*|dags_folder = /mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags|" "$AIRFLOW_HOME/airflow.cfg"

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname V3 \
    --role Admin \
    --email admin@v3.local
```

### Terminal 1 — Scheduler

```bash
airflow scheduler
```

### Terminal 2 — Webserver

```bash
wsl
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
source venv_wsl/bin/activate
export AIRFLOW_HOME="/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/airflow_home"
airflow webserver --port 8095
```

---

## STEP 4 — Trigger DAG

1. Browser → **http://localhost:8095**
2. Login: `admin / admin`
3. Find: `dbd_v3_final_pipeline`
4. Unpause → Trigger
5. Monitor Graph View — semua task harus hijau

---

## STEP 5 — Validasi

```bash
psql -U postgres -d dwh_dbd_v3 -c "
SELECT 'dim_wilayah' AS t, COUNT(*) FROM warehouse_dbd_v3.dim_wilayah
UNION ALL SELECT 'dim_waktu', COUNT(*) FROM warehouse_dbd_v3.dim_waktu
UNION ALL SELECT 'fact_dbd_env', COUNT(*) FROM warehouse_dbd_v3.fact_dbd_env;"
```

Expected: wilayah=27, waktu=6, fact=162

```bash
# Verify lat/lon
psql -U postgres -d dwh_dbd_v3 -c "
SELECT nama_kabupaten_kota, latitude, longitude
FROM warehouse_dbd_v3.dim_wilayah LIMIT 5;"

# Verify V1 UNTOUCHED
psql -U postgres -d dwh_dbd -c "SELECT COUNT(*) FROM fact_dbd_env;"
```

---

## STEP 6 — Metabase

1. Buka `http://localhost:3000`
2. Admin > Databases > Add: PostgreSQL, Host: localhost, Port: 5432, DB: `dwh_dbd_v3`
3. SQL Query → copy dari `sql/olap_queries_v3.sql`
4. Pin Map → gunakan `latitude` + `longitude` dari dim_wilayah
