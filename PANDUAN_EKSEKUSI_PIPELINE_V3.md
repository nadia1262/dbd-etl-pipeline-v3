# Panduan Eksekusi Pipeline V3 — DETAIL

---

## 1. PRE-CHECK

Buka terminal WSL. Jalankan satu per satu.

---

### Pre-Check A: Python aktif

```bash
which python
```

**Tujuan:** Pastikan Python berasal dari pyenv global, bukan dari venv_wsl.

**Output sukses:**
```
/home/csvcompt/.pyenv/shims/python
```

**Output error:**
```
/mnt/d/POLSTAT STIS/.../docs_v3/venv_wsl/bin/python
```
→ Artinya kamu masih di dalam venv_wsl. Solusi:
```bash
deactivate
```

---

### Pre-Check B: Airflow versi 2

```bash
airflow version 2>/dev/null | tail -1
```

**Tujuan:** Pastikan Airflow yang aktif adalah versi 2.8.1.

**Output sukses:**
```
2.8.1
```

**Output error:**
```
3.2.1
```
→ Artinya kamu masih pakai venv_wsl. Solusi:
```bash
deactivate
airflow version 2>/dev/null | tail -1
```

---

### Pre-Check C: PostgreSQL hidup

```bash
pg_isready -h localhost -p 5432 2>/dev/null || python -c "import psycopg2; c=psycopg2.connect('host=localhost port=5432 dbname=dwh_dbd_v3 user=postgres password=Bismillah_22'); print('PostgreSQL OK'); c.close()"
```

**Tujuan:** Pastikan PostgreSQL di Windows bisa diakses dari WSL.

**Output sukses:**
```
localhost:5432 - accepting connections
```
atau:
```
PostgreSQL OK
```

**Output error:**
```
localhost:5432 - no response
```
atau:
```
psycopg2.OperationalError: could not connect to server: Connection refused
```
→ Artinya PostgreSQL belum jalan. Solusi: Buka Windows → cari "Services" → cari "postgresql" → klik **Start**.

---

### Pre-Check D: MySQL hidup

```bash
python -c "import pymysql; c=pymysql.connect(host='localhost',port=3306,user='root',password='rootpassword123'); print('MySQL OK'); c.close()"
```

**Tujuan:** Pastikan MySQL/Laragon di Windows bisa diakses dari WSL.

**Output sukses:**
```
MySQL OK
```

**Output error:**
```
pymysql.err.OperationalError: (2003, "Can't connect to MySQL server on 'localhost'")
```
→ Artinya MySQL belum jalan. Solusi: Buka Windows → buka Laragon → klik **Start All**. Atau di PowerShell Windows:
```powershell
net start mysql
```

---

## 2. EKSEKUSI DETAIL

Setelah 4 pre-check di atas SEMUA sukses, lanjut ke step eksekusi.

---

### STEP 1 — Masuk folder docs_v3

```bash
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
```

**Tujuan:** Semua command selanjutnya dijalankan dari folder ini.

**Output sukses:**
```
(tidak ada output, prompt berubah ke)
csvcompt@nadiana:/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3$
```

**Output error:**
```
-bash: cd: /mnt/d/POLSTAT STIS/...: No such file or directory
```
→ Artinya drive D belum ter-mount. Solusi:
```bash
sudo mount -t drvfs D: /mnt/d
```

---

### STEP 2 — Pastikan tidak pakai venv_wsl

```bash
deactivate 2>/dev/null; echo "venv cleared"
```

**Tujuan:** Matikan venv_wsl kalau masih aktif. Command ini aman dijalankan bahkan kalau tidak ada venv aktif.

**Output sukses:**
```
venv cleared
```

Verifikasi:
```bash
echo $VIRTUAL_ENV
```

**Output sukses:**
```
(baris kosong — tidak ada output)
```

**Output error:**
```
/mnt/d/.../venv_wsl
```
→ Artinya deactivate gagal. Solusi: tutup terminal WSL, buka terminal baru, ulangi dari STEP 1.

---

### STEP 3 — Set AIRFLOW_HOME

```bash
export AIRFLOW_HOME="/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/airflow_home"
echo "AIRFLOW_HOME = $AIRFLOW_HOME"
```

**Tujuan:** Kasih tahu Airflow di mana config dan database-nya disimpan.

**Output sukses:**
```
AIRFLOW_HOME = /mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/airflow_home
```

---

### STEP 4 — Init Airflow

```bash
airflow db init
```

**Tujuan:** Buat database Airflow (airflow.db) dan file konfigurasi (airflow.cfg) di folder airflow_home.

**Output sukses (kalau pertama kali):**
```
DB: sqlite:////mnt/d/.../airflow_home/airflow.db
...
Initialization done
```

**Output sukses (kalau sudah pernah):**
```
DB: sqlite:////mnt/d/.../airflow_home/airflow.db
...
No migrations to run.
```

**Setelah init berhasil, set dags_folder:**

```bash
sed -i "s|dags_folder = .*|dags_folder = /mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags|" "$AIRFLOW_HOME/airflow.cfg"
```

**Verifikasi dags_folder sudah benar:**

```bash
grep "^dags_folder" "$AIRFLOW_HOME/airflow.cfg"
```

**Output sukses:**
```
dags_folder = /mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags
```

**Buat user admin (kalau pertama kali):**

```bash
airflow users create --username admin --password admin --firstname Admin --lastname V3 --role Admin --email admin@v3.local
```

**Output sukses:**
```
[2026-05-01 ...] Admin user admin created
```

**Output kalau sudah ada:**
```
admin already exists
```
→ Aman, lanjut ke step berikutnya.

---

### STEP 5 — Jalankan ingest_sources.py

```bash
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
python scripts/ingest_sources.py
```

**Tujuan:** Isi ulang data mentah dari CSV ke MySQL dan PostgreSQL (source databases).

**Output sukses:**
```
[1/4] MySQL source_bps_health: 243 rows
[2/4] MySQL source_bps_kepadatan: 216 rows
[3/4] MySQL source_bps_luas: 162 rows
[4/4] PostgreSQL source_gee_env: 150 rows
INGESTION COMPLETE
```

**Output error 1:**
```
ModuleNotFoundError: No module named 'dotenv'
```
→ Solusi:
```bash
pip install python-dotenv
```

**Output error 2:**
```
ModuleNotFoundError: No module named 'pymysql'
```
→ Solusi:
```bash
pip install pymysql
```

**Output error 3:**
```
pymysql.err.OperationalError: (2003, "Can't connect to MySQL server")
```
→ Solusi: MySQL belum jalan. Kembali ke Pre-Check D.

---

### STEP 6 — Create tabel pipeline_runs

```bash
PGPASSWORD=Bismillah_22 psql -h localhost -U postgres -d dwh_dbd_v3 -c "
CREATE TABLE IF NOT EXISTS warehouse_dbd_v3.pipeline_runs (
    run_id       SERIAL PRIMARY KEY,
    run_at       TIMESTAMP DEFAULT NOW(),
    year_start   INT,
    year_end     INT,
    n_regions    INT,
    n_years      INT,
    fact_rows    INT,
    imputed_rows INT,
    null_ir_rows INT,
    status       VARCHAR(20)
);
"
```

**Tujuan:** Buat tabel observability baru (dari patch kemarin).

**Output sukses:**
```
CREATE TABLE
```

**Kalau psql tidak tersedia di WSL:**
→ Buka **pgAdmin** di Windows → connect ke database `dwh_dbd_v3` → Query Tool → copy-paste SQL di atas → klik Run (F5).

---

### STEP 7 — Jalankan Airflow Scheduler

**Buka Terminal WSL pertama (atau tetap di terminal yang sama):**

```bash
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
export AIRFLOW_HOME="/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/airflow_home"
airflow scheduler
```

**Tujuan:** Jalankan Airflow scheduler yang memantau dan menjalankan DAG.

**Output sukses (log terus berjalan, JANGAN DITUTUP):**
```
[2026-05-01 ...] [INFO] Starting the scheduler
[2026-05-01 ...] [INFO] Processing file /mnt/d/.../docs_v3/dags/dag_dbd_v3.py
...
```
→ Biarkan terminal ini tetap terbuka dan jalan.

**Output error:**
```
airflow.exceptions.AirflowConfigException: error: cannot use sqlite with the LocalExecutor
```
→ Solusi (pakai SequentialExecutor untuk SQLite):
```bash
sed -i "s|executor = LocalExecutor|executor = SequentialExecutor|" "$AIRFLOW_HOME/airflow.cfg"
airflow scheduler
```

---

### STEP 8 — Jalankan Airflow Webserver

**Buka Terminal WSL BARU (terminal kedua):**

```bash
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
export AIRFLOW_HOME="/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/airflow_home"
airflow webserver --port 8095
```

**Tujuan:** Jalankan Airflow UI yang bisa diakses lewat browser.

**Output sukses (log terus berjalan, JANGAN DITUTUP):**
```
[2026-05-01 ...] [INFO] Running on http://0.0.0.0:8095
```

**Output error:**
```
OSError: [Errno 98] Address already in use
```
→ Artinya port 8095 sudah dipakai. Solusi:
```bash
# Kill proses lama yang pakai port 8095
fuser -k 8095/tcp
# Jalankan ulang
airflow webserver --port 8095
```

---

### STEP 9 — Trigger DAG di Browser

1. Buka browser di Windows
2. Pergi ke: **http://localhost:8095**
3. Login:
   - Username: `admin`
   - Password: `admin`
4. Di halaman utama, cari DAG bernama: **`dbd_v3_final_pipeline`**
5. Klik tombol toggle di sebelah kiri nama DAG → ubah dari **OFF** ke **ON** (unpause)
6. Klik ikon ▶ (tombol play) di sisi kanan baris DAG → **Trigger DAG**
7. Klik **Trigger** di popup konfirmasi

**Sukses:** Status DAG berubah menjadi "running" (lingkaran hijau muda berputar).

**Error — DAG tidak muncul di list:**
→ Cek terminal scheduler (Terminal 1). Cari baris error:
```
ERROR - Failed to import: /mnt/d/.../dags/dag_dbd_v3.py
```
→ Kalau ada `ModuleNotFoundError: No module named 'config'`:
```bash
# Pastikan sys.path di DAG sudah benar. Cek:
grep "sys.path" "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags/dag_dbd_v3.py"
```
→ Harusnya ada: `sys.path.insert(0, str(_DOCS_V3_DIR))` — ini sudah ada di kode kamu, jadi harusnya aman.

---

## 3. MONITORING

Setelah DAG di-trigger, klik nama DAG → pilih **Graph View**.

**Urutan task (dari atas ke bawah):**

| Urutan | Nama Task | Apa yang terjadi |
|--------|-----------|------------------|
| 1 | `extract_from_mysql` | Ambil data BPS dari MySQL → simpan ke temp/ |
| 2 | `extract_from_gee` | Ambil data GEE dari PostgreSQL → simpan ke temp/ |
| 3 | `load_staging` | Baca temp/ → isi 4 tabel staging di PostgreSQL |
| 4 | `validate_staging` | Cek jumlah baris, duplikasi, null |
| 5 | `transform` | Merge staging → feature engineering → hasilkan fact table |
| 6 | `load_dwh` | TRUNCATE fact + dim → isi ulang → tulis pipeline_runs |

**Kapan dianggap SUKSES:**
- Semua 6 kotak berwarna **HIJAU TUA** (dark green = success)
- Tidak ada kotak **MERAH** (merah = failed)
- Tidak ada kotak **KUNING** (kuning = masih running, tunggu)

**Kalau ada task MERAH:**
1. Klik kotak merah tersebut
2. Klik **Log**
3. Scroll ke bawah, cari baris yang ada kata `ERROR` atau `Traceback`
4. Copy-paste error tersebut ke sini — aku bantu debug

---

## 4. VALIDASI HASIL

Setelah semua 6 task HIJAU, jalankan query berikut.

**Bisa di terminal WSL:**
```bash
PGPASSWORD=Bismillah_22 psql -h localhost -U postgres -d dwh_dbd_v3
```
→ Lalu ketik query di bawah satu per satu.

**Atau bisa di pgAdmin** → connect ke `dwh_dbd_v3` → Query Tool.

---

### Validasi 1: Jumlah baris fact table

```sql
SELECT COUNT(*) AS total_rows FROM warehouse_dbd_v3.fact_dbd_env;
```

**Hasil yang HARUS muncul:**
```
 total_rows
------------
        162
```

---

### Validasi 2: Range tahun + jumlah kabupaten per tahun

```sql
SELECT tahun, COUNT(*) AS n_kabupaten
FROM warehouse_dbd_v3.fact_dbd_env
GROUP BY tahun ORDER BY tahun;
```

**Hasil yang HARUS muncul:**
```
 tahun | n_kabupaten
-------+-------------
  2019 |          27
  2020 |          27
  2021 |          27
  2022 |          27
  2023 |          27
  2024 |          27
```

---

### Validasi 3: Tidak ada NaN di kolom kritis

```sql
SELECT
    COUNT(*) FILTER (WHERE incidence_rate IS NULL) AS null_ir,
    COUNT(*) FILTER (WHERE composite_vulnerability_index IS NULL) AS null_cvi,
    COUNT(*) FILTER (WHERE is_imputed = TRUE) AS imputed_rows
FROM warehouse_dbd_v3.fact_dbd_env;
```

**Hasil yang HARUS muncul:**
```
 null_ir | null_cvi | imputed_rows
---------+----------+--------------
       0 |        0 |            0
```
(imputed_rows bisa > 0 kalau memang ada data yang diimputasi, tapi null_ir HARUS 0)

---

### Validasi 4: Pipeline runs (patch baru)

```sql
SELECT run_at, year_start, year_end, fact_rows, status
FROM warehouse_dbd_v3.pipeline_runs
ORDER BY run_at DESC LIMIT 3;
```

**Hasil yang HARUS muncul:**
```
       run_at        | year_start | year_end | fact_rows |  status
---------------------+------------+----------+-----------+---------
 2026-05-01 18:xx:xx |       2019 |     2024 |       162 | success
```

---

## 5. ERROR HANDLING

### Error 1: ModuleNotFoundError

```
ModuleNotFoundError: No module named 'scipy'
```

**Artinya:** Library belum terinstall di Python global.

**Solusi:**
```bash
pip install scipy pandas numpy sqlalchemy pymysql psycopg2-binary python-dotenv geopandas shapely openpyxl
```

---

### Error 2: Connection refused

```
sqlalchemy.exc.OperationalError: (pymysql.err.OperationalError) (2003, "Can't connect to MySQL server on 'localhost' ([Errno 111] Connection refused)")
```

**Artinya:** MySQL di Windows tidak jalan.

**Solusi:** Buka Windows → buka Laragon → klik Start All. Tunggu 10 detik. Ulangi trigger DAG.

---

### Error 3: DAG tidak muncul di Airflow UI

**Artinya:** `dags_folder` salah atau DAG file ada error syntax.

**Solusi — cek dags_folder:**
```bash
grep "^dags_folder" "$AIRFLOW_HOME/airflow.cfg"
```
Harus keluar:
```
dags_folder = /mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags
```

**Solusi — cek error import DAG:**
```bash
python "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags/dag_dbd_v3.py"
```
Kalau ada error, dia akan muncul di sini.

---

### Error 4: Port already in use

```
OSError: [Errno 98] Address already in use
```

**Artinya:** Port 8095 masih dipakai oleh proses lama.

**Solusi:**
```bash
fuser -k 8095/tcp
airflow webserver --port 8095
```

---

### Error 5: Permission denied pada folder temp

```
PermissionError: [Errno 13] Permission denied: '/mnt/d/.../docs_v3/temp/health.parquet'
```

**Artinya:** WSL tidak punya izin menulis ke drive Windows.

**Solusi:**
```bash
chmod -R 777 "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/temp"
```
