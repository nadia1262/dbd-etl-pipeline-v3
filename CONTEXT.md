# CONTEXT PROMPT — Pipeline V3 DBD Jabar

Copy-paste seluruh isi file ini ke percakapan baru agar konteks terjaga.

---

## IDENTITAS PROJECT

Saya mengerjakan project ETL Pipeline untuk analisis epidemiologi Demam Berdarah Dengue (DBD) di Jawa Barat. Project ini untuk tugas akademik (UTS) di POLSTAT STIS, Semester 6.

**Fokus saya HANYA pada folder `docs_v3`.**

---

## STRUKTUR FOLDER PROJECT

```
D:\POLSTAT STIS\Tingkat 3\Semester 6\TPD\Project_UTS_5\
├── data/                        ← CSV sumber data (BPS health, kepadatan, luas, GEE env, GeoJSON)
├── docs/                        ← V1 (JANGAN SENTUH)
├── docs_v2/                     ← V2 (JANGAN SENTUH)
├── docs_v3/                     ← V3 FINAL ← FOKUS DI SINI
│   ├── .env                     ← Konfigurasi environment (DB, schema, year range)
│   ├── requirements.txt         ← Dependencies (apache-airflow==2.8.1, dll)
│   ├── DEEP_DIVE_PIPELINE_V3.md ← Dokumentasi teknis detail
│   ├── PANDUAN_EKSEKUSI_PIPELINE_V3.md ← Panduan step-by-step
│   ├── SETUP_GUIDE.md           ← Setup guide awal
│   ├── config/
│   │   ├── __init__.py
│   │   └── db_config.py         ← Sentralisasi koneksi DB + load_dotenv()
│   ├── dags/
│   │   └── dag_dbd_v3.py        ← DAG Airflow utama (479 baris)
│   ├── scripts/
│   │   └── ingest_sources.py    ← Script ingest CSV → MySQL/PostgreSQL
│   ├── sql/
│   │   ├── ddl_v3.sql           ← Schema DDL (staging + warehouse)
│   │   └── olap_queries_v3.sql  ← Query OLAP untuk Metabase
│   ├── temp/                    ← File temporary saat pipeline jalan
│   ├── airflow_home/            ← (TIDAK DIPAKAI — disk I/O error di /mnt/d/)
│   ├── venv_wsl/                ← (TIDAK DIPAKAI — berisi Airflow 3.2.1, salah)
│   └── venv_v3/                 ← (TIDAK DIPAKAI — gagal install, sudah dihapus)
├── etl_rani_v2/                 ← Folder lain (JANGAN SENTUH)
└── dwh_backup.sql               ← Backup DWH
```

---

## ENVIRONMENT YANG BENAR (SUDAH TERBUKTI WORKING)

### Python Environment
- **OS:** Windows 11 + WSL2 (Ubuntu)
- **Python:** 3.10.13 via pyenv di WSL
- **Virtual Environment:** `pyenv activate dbd_pipeline_env`
- **Airflow Version:** 2.8.1 (BUKAN 3.x)

### AIRFLOW_HOME
```bash
export AIRFLOW_HOME=~/airflow_home_v3
```
- **HARUS** di filesystem Linux (`~/`), **BUKAN** di `/mnt/d/` (SQLite disk I/O error)
- `dags_folder` di `airflow.cfg` mengarah ke: `/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags`
- `load_examples = False`

### Database
- **MySQL** (via Laragon di Windows): `root:rootpassword123@localhost:3306/source_os_dbd`
- **PostgreSQL** (di Windows): `postgres:Bismillah_22@localhost:5432/dwh_dbd_v3`
  - Schema `staging_dbd_v3`: 4 tabel staging
  - Schema `warehouse_dbd_v3`: dim_wilayah, dim_waktu, fact_dbd_env, pipeline_runs

### Airflow Services
- **Scheduler:** `airflow scheduler` (Terminal WSL 1, biarkan jalan)
- **Webserver:** `airflow webserver --port 8095` (Terminal WSL 2, biarkan jalan)
- **UI:** http://localhost:8095 (login: admin / admin)

---

## PERINTAH STARTUP (SETIAP KALI BUKA WSL BARU)

### Terminal 1 — Scheduler
```bash
pyenv activate dbd_pipeline_env
export AIRFLOW_HOME=~/airflow_home_v3
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
airflow scheduler
```

### Terminal 2 — Webserver
```bash
pyenv activate dbd_pipeline_env
export AIRFLOW_HOME=~/airflow_home_v3
airflow webserver --port 8095
```

---

## ARSITEKTUR PIPELINE

```
MySQL (BPS: health, kepadatan, luas) + PostgreSQL (GEE env) + GeoJSON (spatial)
    ↓ EXTRACT (3x parallel)
temp/ (CSV temporary files)
    ↓ LOAD STAGING
staging_dbd_v3.stg_health_dbd
staging_dbd_v3.stg_demografi
staging_dbd_v3.stg_environment
staging_dbd_v3.stg_geojson
    ↓ VALIDATE
threshold check, null check, duplicate check
    ↓ TRANSFORM
merge + feature engineering (incidence_rate, composite_vulnerability_index, dll)
    ↓ LOAD DWH
warehouse_dbd_v3.dim_wilayah (27 rows)
warehouse_dbd_v3.dim_waktu (6 rows: 2019-2024)
warehouse_dbd_v3.fact_dbd_env (162 rows: 27 kab × 6 tahun)
warehouse_dbd_v3.pipeline_runs (1 row per run: metadata/observability)
    ↓
Metabase (OLAP + Pin Map)
```

### DAG Task Order
```
extract_geojson ──┐
extract_from_postgres ──┤→ load_staging → validate_staging → transform → load_dwh → cleanup
extract_from_mysql ─────┘
```

---

## KONFIGURASI .env (docs_v3/.env)

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASS=rootpassword123
MYSQL_SOURCE_DB=source_os_dbd

PG_SOURCE_CONN="postgresql://postgres:Bismillah_22@localhost:5432/dwh_dbd_v3"
PG_DWH_CONN="postgresql://postgres:Bismillah_22@localhost:5432/dwh_dbd_v3"

STAGING_SCHEMA=staging_dbd_v3
WAREHOUSE_SCHEMA=warehouse_dbd_v3

YEAR_START=2019
YEAR_END=2024
PANDEMIC_YEARS=2020,2021
```

---

## PERBAIKAN YANG SUDAH DITERAPKAN (6 Patch)

1. **Dynamic Year Range:** YEAR_START/YEAR_END dari .env (bukan hardcoded)
2. **NaN Guard:** Validasi post-imputasi untuk incidence_rate
3. **Dynamic Validation Threshold:** n_regions × n_years × 80%
4. **Observability:** Tabel `pipeline_runs` mencatat metrik setiap run
5. **GEE Mismatch Warning:** Log warning jika mapping GEE ↔ kabupaten tidak cocok
6. **Dynamic OLAP:** Query Metabase menggunakan MAX(tahun)

---

## EXPECTED RESULTS (SUDAH TERVERIFIKASI)

- `fact_dbd_env`: **162 rows** (27 kab × 6 tahun)
- `dim_wilayah`: **27 rows**
- `dim_waktu`: **6 rows** (2019-2024)
- `pipeline_runs`: 1 row per successful run, `status = success`
- Semua task DAG: **HIJAU** (success)

---

## JEBAKAN YANG SUDAH DITEMUKAN (JANGAN ULANGI)

| Jebakan | Penjelasan |
|---------|------------|
| `venv_wsl` = Airflow 3.2.1 | JANGAN PAKAI. Tidak kompatibel dengan DAG ini. |
| AIRFLOW_HOME di `/mnt/d/` | SQLite akan error `disk I/O error`. Harus di `~/` (Linux filesystem). |
| `airflow webserver` di Airflow 3 | Tidak ada. Airflow 3 pakai `api-server`. Tapi kita pakai Airflow 2. |
| Provider terlalu baru | pyenv global punya provider Airflow 3. Solusi: pakai `dbd_pipeline_env`. |
| `.env` path Windows (`D:\...`) | Di WSL, `load_dotenv()` dari Python tetap bisa baca ini dengan benar. |
| `source .env` di bash | Path Windows dengan spasi akan error. Gunakan `load_dotenv()` di Python saja. |
| Install pip di `/mnt/d/` | Sangat lambat (20+ menit). Selalu install di filesystem Linux. |

---

## FILE-FILE PENTING (BACA KALAU PERLU KONTEKS)

- `docs_v3/dags/dag_dbd_v3.py` — DAG utama (479 baris), berisi semua task ETL
- `docs_v3/config/db_config.py` — Koneksi DB, load_dotenv(), path management
- `docs_v3/scripts/ingest_sources.py` — Ingest CSV ke MySQL/PostgreSQL
- `docs_v3/sql/ddl_v3.sql` — DDL schema staging + warehouse
- `docs_v3/sql/olap_queries_v3.sql` — Query OLAP untuk Metabase
- `docs_v3/.env` — Konfigurasi environment

---

## STATUS TERAKHIR (1 Mei 2026, 20:04 WIB)

✅ Pipeline `dbd_v3_final_pipeline` sudah **berhasil dijalankan** — semua 7 task HIJAU (extract_geojson, extract_from_postgres, extract_from_mysql, load_staging, validate_staging, transform, load_dwh, cleanup).

Belum dilakukan:
- Validasi SQL (query COUNT, range tahun, pipeline_runs)
- Koneksi Metabase ke dwh_dbd_v3
