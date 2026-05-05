# 🔄 RECOVERY GUIDE — Dari Laptop Mati Sampai Dashboard Jalan

> **Skenario:** Laptop habis di-shutdown/restart. Semua service mati. Ikuti langkah berikut SATU PER SATU.
> **Estimasi waktu:** ~15 menit sampai DAG hijau, +15 menit untuk Metabase.

---

## FASE 1: START SERVICES DI WINDOWS (5 menit)

### Step 1.1 — Start Laragon (MySQL)
1. Buka **Laragon** dari Start Menu / Desktop
2. Klik **"Start All"**
3. Tunggu sampai status MySQL **hijau**

**Verifikasi:**
Buka terminal (cmd/PowerShell):
```
mysql -u root -prootpassword123 -e "SHOW DATABASES;"
```
✅ Harus muncul list database, termasuk `source_os_dbd`

### Step 1.2 — Start PostgreSQL
PostgreSQL biasanya auto-start. Kalau tidak:
1. Buka **Services** (tekan `Win+R` → ketik `services.msc`)
2. Cari **postgresql-x64-15** (atau versi kamu)
3. Klik **Start**

**Verifikasi:**
```
psql -U postgres -h localhost -c "SELECT 1;"
```
✅ Password: `Bismillah_22`. Harus return 1 row.

### Step 1.3 — Pastikan Database dwh_dbd_v3 Ada
```
psql -U postgres -h localhost -c "\l" | findstr dwh_dbd_v3
```
✅ Harus muncul `dwh_dbd_v3`

**Kalau TIDAK ada** (misal habis install ulang):
```
psql -U postgres -h localhost -c "CREATE DATABASE dwh_dbd_v3;"
psql -U postgres -h localhost -d dwh_dbd_v3 -f "D:\POLSTAT STIS\Tingkat 3\Semester 6\TPD\Project_UTS_5\docs_v3\sql\ddl_v3.sql"
```

---

## FASE 2: SETUP WSL & AIRFLOW (5 menit)

### Step 2.1 — Buka Terminal WSL
- Buka **Windows Terminal** → Tab **Ubuntu** (atau ketik `wsl` di cmd)

### Step 2.2 — Aktivasi Environment
```bash
pyenv activate dbd_pipeline_env
export AIRFLOW_HOME=~/airflow_home_v3
```

**Verifikasi:**
```bash
airflow version
```
✅ Harus output: `2.8.1`

### Step 2.3 — Cek Konfigurasi Airflow
```bash
grep "^dags_folder" "$AIRFLOW_HOME/airflow.cfg"
grep "^load_examples" "$AIRFLOW_HOME/airflow.cfg"
```
✅ Expected:
```
dags_folder = /mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags
load_examples = False
```

**Kalau salah:**
```bash
sed -i "s|dags_folder = .*|dags_folder = /mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/dags|" "$AIRFLOW_HOME/airflow.cfg"
sed -i "s|load_examples = True|load_examples = False|" "$AIRFLOW_HOME/airflow.cfg"
```

---

## FASE 3: INGEST DATA (2 menit)

### Step 3.1 — Jalankan Ingest Script
```bash
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3"
python scripts/ingest_sources.py
```
✅ Expected: `INGESTION COMPLETE`

**Kalau error koneksi MySQL:**
- Cek Laragon sudah Start
- Cek password di `.env`: `MYSQL_PASS=rootpassword123`

**Kalau error koneksi PostgreSQL:**
- Cek service PostgreSQL sudah Start
- Cek password di `.env`: `Bismillah_22`

---

## FASE 4: JALANKAN AIRFLOW (3 menit)

> ⚠️ Butuh **2 terminal WSL terpisah**. Jangan tutup salah satunya.

### Step 4.1 — Terminal 1: Scheduler
```bash
pyenv activate dbd_pipeline_env
export AIRFLOW_HOME=~/airflow_home_v3
airflow scheduler
```
→ Biarkan jalan. **JANGAN DITUTUP.**

### Step 4.2 — Terminal 2: Webserver
Buka terminal WSL **baru** (Tab baru di Windows Terminal):
```bash
pyenv activate dbd_pipeline_env
export AIRFLOW_HOME=~/airflow_home_v3
airflow webserver --port 8095
```
→ Biarkan jalan. **JANGAN DITUTUP.**

**Kalau error "Address already in use":**
```bash
lsof -i :8095 | grep LISTEN | awk '{print $2}' | xargs kill -9
airflow webserver --port 8095
```

---

## FASE 5: TRIGGER DAG (2 menit)

### Step 5.1 — Buka Airflow UI
1. Buka browser: **http://localhost:8095**
2. Login: `admin` / `admin`

### Step 5.2 — Aktifkan & Trigger DAG
1. Cari DAG: **`dbd_v3_final_pipeline`**
2. Toggle switch ke **ON** (kalau masih OFF)
3. Klik tombol ▶ **Trigger DAG** (tombol play di kanan)
4. Klik **Trigger** di popup konfirmasi

### Step 5.3 — Pantau Progress
1. Klik nama DAG → masuk ke **Graph View**
2. Tunggu semua task berubah **hijau** (SUCCESS)
3. Urutan: extract (3 paralel) → load_staging → validate_staging → transform → load_dwh → cleanup

**Estimasi waktu:** ~30 detik untuk semua task selesai.

### Step 5.4 — Verifikasi Data (Opsional, Terminal 3)
```bash
pyenv activate dbd_pipeline_env
PGPASSWORD=Bismillah_22 psql -h localhost -U postgres -d dwh_dbd_v3 -c \
  "SELECT COUNT(*) FROM warehouse_dbd_v3.fact_dbd_env;"
```
✅ Expected: `162`

```bash
PGPASSWORD=Bismillah_22 psql -h localhost -U postgres -d dwh_dbd_v3 -c \
  "SELECT run_at, fact_rows, status FROM warehouse_dbd_v3.pipeline_runs ORDER BY run_at DESC LIMIT 1;"
```
✅ Expected: `fact_rows = 162`, `status = SUCCESS`

---

## FASE 6: SETUP METABASE & BUAT DASHBOARD (15 menit)

### Step 6.1 — Start Metabase

**Jalankan Metabase (pilih SALAH SATU):**

**Opsi A — Dari Windows (cmd/PowerShell):**
```
cd "D:\POLSTAT STIS\Tingkat 3\Semester 6\TPD\Project_UTS_5\docs_v3\metabase"
java -jar metabase.jar
```

**Opsi B — Dari WSL (terminal baru):**
```bash
cd "/mnt/d/POLSTAT STIS/Tingkat 3/Semester 6/TPD/Project_UTS_5/docs_v3/metabase"
java -jar metabase.jar
```
→ Tunggu sampai muncul: `Metabase Initialization COMPLETE`
→ Biarkan jalan. **JANGAN DITUTUP.**

### Step 6.2 — Setup Awal Metabase (Hanya Pertama Kali)
1. Buka browser: **http://localhost:3000**
2. Ikuti setup wizard:
   - Bahasa: English
   - Nama: (isi sembarang)
   - Email: admin@v3.local
   - Password: (pilih sendiri, ingat!)
3. **Add Database:**
   - Database type: **PostgreSQL**
   - Host: (lihat tabel di bawah)
   - Port: `5432`
   - Database name: `dwh_dbd_v3`
   - Username: `postgres`
   - Password: `Bismillah_22`
   - Klik **Save**

   **Host mana yang dipakai?**
   | Metabase jalan dari | Host yang diisi |
   |---------------------|----------------|
   | Windows (Opsi A) | `localhost` |
   | WSL (Opsi B) — coba dulu | `localhost` |
   | WSL (Opsi B) — kalau localhost gagal | Jalankan `hostname -I` di WSL, atau `ipconfig` di Windows → ambil IP adapter `vEthernet (WSL)` |
4. Skip bagian lainnya → klik **Take me to Metabase**

### Step 6.3 — Buat Question/Chart OLAP

Klik **+ New** → **SQL Query** → Pilih database `dwh_dbd_v3`

#### Chart 1: Tren Incidence Rate per Tahun (Line Chart)
```sql
SELECT f.tahun,
       ROUND(AVG(f.incidence_rate)::numeric, 2) AS avg_ir,
       SUM(f.jumlah_kasus_dbd) AS total_kasus
FROM warehouse_dbd_v3.fact_dbd_env f
GROUP BY f.tahun
ORDER BY f.tahun;
```
→ Klik **Visualize** → Pilih **Line Chart**
→ X-axis: `tahun`, Y-axis: `avg_ir`
→ Klik **Save** → Nama: "Tren IR per Tahun" → Save ke koleksi baru "Dashboard DBD V3"

#### Chart 2: Top 10 Kabupaten IR Tertinggi (Bar Chart)
```sql
SELECT w.nama_kabupaten_kota,
       ROUND(AVG(f.incidence_rate)::numeric, 2) AS avg_ir
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
GROUP BY w.nama_kabupaten_kota
ORDER BY avg_ir DESC
LIMIT 10;
```
→ Visualize → **Bar Chart** (horizontal)
→ Save: "Top 10 IR Tertinggi"

#### Chart 3: Pin Map — CVI per Kabupaten (Tahun Terbaru)
```sql
SELECT w.nama_kabupaten_kota, w.latitude, w.longitude,
       f.composite_vulnerability_index AS cvi,
       f.incidence_rate
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
WHERE f.tahun = (SELECT MAX(tahun) FROM warehouse_dbd_v3.fact_dbd_env);
```
→ Visualize → **Map** → Pin Map
→ Latitude: `latitude`, Longitude: `longitude`
→ Color: `cvi`
→ Save: "Peta CVI Jabar"

#### Chart 4: Dampak COVID terhadap DBD
```sql
SELECT t.keterangan_periode,
       ROUND(AVG(f.incidence_rate)::numeric, 2) AS avg_ir,
       SUM(f.jumlah_kasus_dbd) AS total_kasus,
       COUNT(*) AS n_observasi
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_waktu t ON f.tahun = t.tahun
GROUP BY t.keterangan_periode;
```
→ Visualize → **Bar Chart**
→ Save: "Dampak COVID vs Normal"

#### Chart 5: Surge Detection (High Risk)
```sql
SELECT w.nama_kabupaten_kota, f.tahun, f.incidence_rate,
       f.yoy_growth_rate, f.rainfall_zscore,
       CASE WHEN f.yoy_growth_rate > 50 AND f.rainfall_zscore > 1.5
            THEN '🔴 HIGH RISK' ELSE '✅ Normal' END AS surge_flag
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
WHERE f.yoy_growth_rate IS NOT NULL
ORDER BY f.yoy_growth_rate DESC NULLS LAST;
```
→ Visualize → **Table**
→ Save: "Surge Detection"

#### Chart 6: Pipeline Health (dari pipeline_runs)
```sql
SELECT run_at::DATE AS run_date,
       year_start || '–' || year_end AS period,
       fact_rows,
       n_regions * n_years AS expected,
       imputed_rows,
       null_ir_rows,
       status
FROM warehouse_dbd_v3.pipeline_runs
ORDER BY run_at DESC;
```
→ Visualize → **Table**
→ Save: "Pipeline Health Monitor"

### Step 6.4 — Susun Dashboard
1. Klik **+ New** → **Dashboard**
2. Nama: **"Dashboard Analisis DBD Jawa Barat V3"**
3. Klik **pencil icon** (edit mode)
4. Klik **+** → tambahkan 6 question yang sudah dibuat
5. Susun layout:
   ```
   ┌─────────────────────┬─────────────────────┐
   │ Tren IR per Tahun   │ Top 10 IR Tertinggi │
   ├─────────────────────┴─────────────────────┤
   │           Peta CVI Jabar (lebar penuh)    │
   ├─────────────────────┬─────────────────────┤
   │ Dampak COVID        │ Surge Detection     │
   ├─────────────────────┴─────────────────────┤
   │        Pipeline Health Monitor            │
   └───────────────────────────────────────────┘
   ```
6. Klik **Save**

---

## ✅ CHECKLIST FINAL

```
□ Laragon MySQL     → HIJAU
□ PostgreSQL        → RUNNING
□ WSL scheduler     → JALAN (terminal 1)
□ WSL webserver     → JALAN (terminal 2)
□ Airflow UI        → http://localhost:8095 bisa dibuka
□ DAG triggered     → Semua task HIJAU
□ fact_dbd_env      → 162 rows
□ pipeline_runs     → Ada data, status SUCCESS
□ Metabase          → http://localhost:3000 bisa dibuka
□ Dashboard         → 6 chart tampil dengan data
```

---

## 🚨 TROUBLESHOOTING CEPAT

| Masalah | Solusi |
|---------|--------|
| `ModuleNotFoundError: dotenv` | `pip install python-dotenv` di WSL |
| `disk I/O error` (Airflow) | Pastikan `AIRFLOW_HOME=~/airflow_home_v3` (BUKAN /mnt/d/) |
| `Address already in use: 8095` | `lsof -i :8095` → `kill -9 <PID>` |
| DAG tidak muncul | Cek `dags_folder` di airflow.cfg, cek `load_examples = False` |
| extract gagal (MySQL) | Start Laragon dulu |
| extract gagal (PostgreSQL) | Start PostgreSQL service dulu |
| Metabase error koneksi | Cek host=localhost, port=5432, password=Bismillah_22 |
| `pyenv: command not found` | Jalankan: `export PATH="$HOME/.pyenv/bin:$PATH" && eval "$(pyenv init -)"` |

---

> **Dokumen ini adalah panduan darurat untuk recovery dari nol.**
> **Terakhir diperbarui: 1 Mei 2026**
