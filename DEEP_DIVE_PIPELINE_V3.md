# 🔄 REVISION SUMMARY (Mei 2026)

**Dokumen ini telah direvisi untuk mencerminkan 6 patch perbaikan yang diterapkan pada Pipeline V3.**

- ✅ **`pipeline_runs` table** — Fitur baru: tabel observability yang mencatat metadata setiap eksekusi pipeline (jumlah baris, data imputasi, status). Memungkinkan deteksi anomali data tanpa memeriksa log Airflow.
- ✅ **Dynamic Year Configuration** — `YEAR_START` dan `YEAR_END` sekarang dibaca dari `.env`, menggantikan hardcode `2019-2024`. Menambah 1 tahun data cukup edit `.env`, tanpa ubah kode Python.
- ✅ **Dynamic Validation Threshold** — Threshold validasi sekarang dihitung otomatis (`n_regions × n_years × 80%`), menggantikan angka statis `100` yang berbahaya karena tidak menyesuaikan saat data bertambah.
- ✅ **NaN Guard di Transform** — Validasi post-imputasi pada `incidence_rate` untuk mencegah silent data corruption akibat NaN yang lolos ke warehouse.
- ✅ **GEE Mapping Warning System** — Warning log otomatis saat ada mismatch antara nama wilayah GEE dan `region_mapping.csv`. Tidak menghentikan pipeline, tapi memberi sinyal untuk investigasi.
- ✅ **Dynamic OLAP Queries** — Query Metabase menggunakan `MAX(tahun)` via subquery, menggantikan hardcode `WHERE tahun = 2024` yang harus diedit manual setiap tahun.

---

# 🔬 DEEP DIVE: Pipeline V3 — DBD Jawa Barat
## Handbook Internal Engineer | Edisi Lengkap (Revisi Mei 2026)

> **Tujuan dokumen ini:** Memahami SELURUH sistem pipeline V3 dari nol sampai analytics-ready.
> Bukan sekadar dokumentasi API — ini adalah **peta mental** untuk memahami mesin dari dalam.

---

## 📑 DAFTAR ISI

| # | Section | Halaman |
|---|---------|---------|
| 1 | [Big Picture](#section-1--big-picture) | Arsitektur & Motivasi |
| 2 | [Perjalanan Data End-to-End](#section-2--perjalanan-data-dari-awal-sampai-akhir) | Cerita Data |
| 3 | [Raw Source Layer](#section-3--raw-source-layer) | Sumber Data |
| 4 | [Staging Layer](#section-4--staging-layer) | Area Persiapan |
| 5 | [Validation Layer](#section-5--validation-layer) | Quality Gate |
| 6 | [Transform & Feature Engineering](#section-6--transform--feature-engineering) | Otak Pipeline |
| 7 | [Star Schema & Data Warehouse](#section-7--star-schema--data-warehouse) | Struktur Analitik |
| 8 | [Airflow Orchestrator](#section-8--airflow-orchestrator) | Conductor |
| 9 | [Spatial & GeoJSON](#section-9--spatial--geojson) | Peta & Koordinat |
| 10 | [OLAP & Metabase](#section-10--olap--metabase) | Dashboard |
| 11 | [What Happens When User Opens Dashboard](#section-11--what-actually-happens-when-user-opens-dashboard) | Behind the Scenes |
| 12 | [Failure Scenarios](#section-12--failure-scenarios) | Troubleshooting |
| 13 | [Trade-off Arsitektur](#section-13--trade-off-arsitektur) | Keputusan Desain |
| 14 | [Handover Guide](#section-14--how-to-handover-this-project) | Transfer Knowledge |
| 15 | [Mental Model Final](#section-15--mental-model-final) | Ringkasan |
| **16** | **[Pipeline Observability & Run Tracking](#section-16--pipeline-observability--run-tracking)** | **🆕 Monitoring** |
| **17** | **[Changelog: V2 → V3 Final](#section-17--changelog-v2--v3-final)** | **🆕 Perubahan** |

---

# SECTION 1 — BIG PICTURE

## 1.1 Apa Tujuan Sistem Ini?

Pipeline V3 adalah **sistem otomatis** yang mengubah data mentah tentang penyakit Demam Berdarah Dengue (DBD) di 27 kabupaten/kota Jawa Barat (2019–2024) menjadi **dashboard analitik interaktif**.

**Problem yang diselesaikan:**

Bayangkan kamu punya 3 tumpukan kertas dari 3 kantor berbeda:
- 📋 **Dinas Kesehatan (BPS):** "Berapa kasus DBD per kabupaten per tahun?"
- 🌧️ **Satelit (Google Earth Engine):** "Berapa curah hujan, suhu, vegetasi per wilayah?"
- 🗺️ **Peta digital (GeoJSON):** "Dimana batas-batas tiap kabupaten?"

Tanpa pipeline, kamu harus **manual** membuka Excel, copy-paste, mencocokkan nama wilayah (yang formatnya berbeda!), menghitung rumus, lalu membuat grafik. Setiap bulan. Berulang. Error-prone.

**Kenapa Excel tidak cukup?**

| Aspek | Excel | Pipeline V3 |
|-------|-------|-------------|
| Sumber data | 1 file | 3 sumber berbeda (MySQL, PostgreSQL, GeoJSON) |
| Reproducibility | Manual, rawan human error | Otomatis, 1 klik trigger |
| Validasi | Mata manusia | Automated checks (null, duplicate, referential) |
| Scalability | 1 juta row = lag | Pandas + SQL = efisien |
| Audit trail | Tidak ada | Airflow logging per task + tabel `pipeline_runs` |
| Observability | Tidak ada | Metrik otomatis per run (row count, imputasi, status) |
| Kolaborasi | "File sudah di-edit siapa?" | Schema terdefinisi, versi jelas |

## 1.2 Analogi Dunia Nyata

> **Pipeline V3 = Pabrik Pengolahan Air**
>
> - **Sumber air mentah** (sungai, mata air, hujan) = CSV, MySQL, GeoJSON
> - **Pipa intake** = Extract tasks (3 paralel)
> - **Bak penampungan & filter** = Staging layer (bersihkan, standardisasi)
> - **Lab QC** = Validation (cek kualitas sebelum proses)
> - **Instalasi pengolahan** = Transform (tambah mineral, adjust pH) = feature engineering
> - **Tangki distribusi** = Data Warehouse (Star Schema, siap konsumsi)
> - **Keran rumah** = Metabase Dashboard (user tinggal "buka keran")
>
> Tanpa pabrik? Kamu minum langsung dari sungai. Bisa, tapi risiko tinggi.

## 1.3 Arsitektur Besar (Visual)

```
╔══════════════════════════════════════════════════════════════════════╗
║                    PIPELINE V3 — ARSITEKTUR                        ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                    ║
║  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐               ║
║  │   MySQL     │  │  PostgreSQL  │  │   GeoJSON   │               ║
║  │ source_os_  │  │  source_gee  │  │ jabar_kabkot│               ║
║  │    dbd      │  │    _env      │  │ (27 polygon)│               ║
║  └──────┬──────┘  └──────┬───────┘  └──────┬──────┘               ║
║         │                │                  │                      ║
║         ▼                ▼                  ▼                      ║
║  ╔══════════════════════════════════════════════════╗               ║
║  ║          EXTRACT (3 task paralel)               ║               ║
║  ║  read_sql → CSV    read_sql → CSV    gpd → CSV  ║               ║
║  ╚══════════════════╤═══════════════════════════════╝               ║
║                     ▼                                              ║
║  ╔══════════════════════════════════════════════════╗               ║
║  ║    STAGING (schema: staging_dbd_v3)             ║               ║
║  ║  stg_health │ stg_demografi │ stg_env │ stg_geo ║               ║
║  ╚══════════════════╤═══════════════════════════════╝               ║
║                     ▼                                              ║
║  ╔══════════════════════════════════════════════════╗               ║
║  ║           VALIDATION (quality gate)             ║               ║
║  ║  dup check │ null check │ row count │ ref integ  ║               ║
║  ╚══════════════════╤═══════════════════════════════╝               ║
║                     ▼                                              ║
║  ╔══════════════════════════════════════════════════╗               ║
║  ║     TRANSFORM + FEATURE ENGINEERING             ║               ║
║  ║  merge 4 tabel → incidence_rate, CVI, z-score   ║               ║
║  ║  lag features, quartiles → panel_final.csv      ║               ║
║  ╚══════════════════╤═══════════════════════════════╝               ║
║                     ▼                                              ║
║  ╔══════════════════════════════════════════════════╗               ║
║  ║  DATA WAREHOUSE (schema: warehouse_dbd_v3)      ║               ║
║  ║  dim_wilayah │ dim_waktu │ fact_dbd_env (162 r)  ║               ║
║  ║  🆕 pipeline_runs (1 row per eksekusi pipeline)  ║               ║
║  ╚══════════════════╤═══════════════════════════════╝               ║
║                     ▼                                              ║
║  ╔══════════════════════════════════════════════════╗               ║
║  ║  METABASE (localhost:3000) — OLAP Dashboard     ║               ║
║  ║  Charts │ Pin Map │ Filters │ SQL Queries        ║               ║
║  ╚══════════════════════════════════════════════════╝               ║
╚══════════════════════════════════════════════════════════════════════╝
```

## 1.4 Flow End-to-End (Ringkas)

```
CSV/Excel files
    │ ingest_sources.py (ONE-TIME)
    ▼
MySQL (BPS data) + PostgreSQL (GEE data)
    │ DAG trigger
    ▼
3x PARALLEL EXTRACT → temp CSV files
    │
    ▼
4x STAGING TABLES (clean, dedup, normalize)
    │
    ▼
VALIDATION (reject jika gagal)
    │
    ▼
TRANSFORM (merge + 15 derived features)
    │
    ▼
STAR SCHEMA (dim_wilayah + dim_waktu + fact_dbd_env)
    │
    ▼
METABASE DASHBOARD (user buka browser → chart muncul)
```

## 1.5 Kenapa Perlu Pipeline?

1. **Repeatability** — Bulan depan data update? Trigger DAG saja. Selesai.
2. **Traceability** — Kalau angka di dashboard aneh, cek Airflow log. Tahu task mana yang gagal.
3. **Separation of Concerns** — Extract terpisah dari Transform, terpisah dari Load. Kalau transform error, extract tidak perlu diulang.
4. **Multi-source Integration** — 3 sumber berbeda, format berbeda, kode wilayah berbeda. Pipeline menyatukan.
5. **Quality Assurance** — Validation layer memastikan data busuk tidak masuk warehouse.

## 1.6 Risiko Tanpa Pipeline

- Data duplicate → insight salah → kebijakan kesehatan salah
- Nama wilayah tidak match → join gagal → 0 row result
- Lupa update satu file → data partial → kesimpulan bias
- Tidak ada log → kalau salah, tidak tahu salah dimana

## 1.7 Cara Menjelaskan ke Orang Lain

> "Kita punya data DBD dari 3 sumber berbeda. Pipeline V3 itu sistem otomatis yang mengambil data dari ketiga sumber, membersihkannya, menghitung metrik epidemiologi, lalu menyimpannya dalam format yang siap dibuat dashboard. Semua diorkestrasikan oleh Airflow, jadi tinggal 1 klik."

---

# SECTION 2 — PERJALANAN DATA DARI AWAL SAMPAI AKHIR

## 2.1 Cerita Perjalanan Data

Bayangkan data kita adalah **barang kiriman dari 3 kota berbeda** yang harus sampai ke **1 toko ritel** dalam kondisi siap jual.

### CHAPTER 1: "Barang Datang" — Pre-Pipeline (ingest_sources.py)

**Apa yang terjadi:**
Sebelum DAG dijalankan, ada satu script yang harus dijalankan SEKALI: `ingest_sources.py`. Script ini mengambil file CSV/Excel mentah dari folder `data/` dan memasukkannya ke database sumber.

```
data/dbd_jabar.csv            → MySQL.source_bps_health     (243 rows)
data/kepadatan_penduduk.csv   → MySQL.source_bps_kepadatan  (216 rows)
data/LuasDaerah_KabKot.xlsx   → MySQL.source_bps_luas       (??? rows)
data/Data_Tahunan_DBD_*.csv   → PostgreSQL.source_gee_env   (150 rows)
```

**Kenapa tidak langsung baca CSV dari DAG?**

Karena di dunia nyata, sumber data bukan file CSV. Sumber data adalah database. Script `ingest_sources.py` **mensimulasikan** kondisi produksi dimana:
- Data BPS ada di database MySQL milik dinas
- Data GEE sudah di-export ke PostgreSQL oleh tim satelit
- Kita hanya perlu **query** database tersebut

**Kapan data ada di RAM?** Saat `pd.read_csv()` dijalankan. DataFrame hidup di RAM selama script berjalan. Begitu `.to_sql()` selesai, data sudah di database. DataFrame bisa di-garbage-collect.

### CHAPTER 2: "Sortir Gudang" — Extract (3 Paralel)

Saat DAG di-trigger, 3 task jalan **bersamaan**:

```
┌──── extract_from_mysql() ───────────────────────────────┐
│ MySQL → pd.read_sql() → DataFrame (di RAM)              │
│ → .to_csv('temp/raw_health.csv')                        │
│ → .to_csv('temp/raw_kepadatan.csv')                     │
│ → .to_csv('temp/raw_luas.csv')                          │
│ DataFrame dihapus dari RAM setelah task selesai          │
└─────────────────────────────────────────────────────────┘

┌──── extract_from_postgres() ────────────────────────────┐
│ PostgreSQL → pd.read_sql() → DataFrame (di RAM)          │
│ → .to_csv('temp/raw_gee.csv')                            │
└─────────────────────────────────────────────────────────┘

┌──── extract_geojson() ──────────────────────────────────┐
│ GeoJSON file → geopandas.read_file()                     │
│ → hitung centroid setiap polygon                         │
│ → extract latitude, longitude                            │
│ → .to_csv('temp/raw_geojson.csv')                        │
└─────────────────────────────────────────────────────────┘
```

**Kenapa output-nya CSV, bukan langsung ke staging?**

Ini adalah **decoupling pattern**. Kalau staging gagal, kita tidak perlu ulang extract. CSV temporary menjadi "checkpoint". Setelah pipeline selesai, file-file ini dihapus oleh task `cleanup()`.

**Yang terjadi di RAM:**
- Setiap task Airflow = 1 Python process terpisah
- DataFrame ada di RAM proses itu saja
- Setelah task selesai, proses mati → RAM bebas

### CHAPTER 3: "Quality Control" — Staging + Validation

**Staging** (load_staging):
```
temp/raw_health.csv     → clean → staging_dbd_v3.stg_health_dbd
temp/raw_kepadatan.csv  ─┐
temp/raw_luas.csv       ─┤ merge → clean → staging_dbd_v3.stg_demografi
                          ┘
temp/raw_gee.csv        → map kode_bps → clean → staging_dbd_v3.stg_environment
temp/raw_geojson.csv    → clean → staging_dbd_v3.stg_geojson
```

**Validation** (validate_staging):
```
Cek duplicate    → PASS/FAIL
Cek null         → PASS/FAIL
Cek row count    → PASS/FAIL
Cek referential  → PASS/FAIL
         │
    Semua PASS? ──→ Lanjut ke Transform
    Ada FAIL?   ──→ STOP. ValueError raised. DAG gagal.
```

### CHAPTER 4: "Proses Produksi" — Transform

```
4 staging tables → READ ke RAM sebagai DataFrames
         │
    merge (LEFT JOIN on kode_bps + tahun)
         │
    Tobler Imputation (2 kabupaten tanpa data GEE)
         │
    Feature Engineering (15+ derived columns)
         │
    panel_final.csv (162 rows × 25+ columns)
```

**Kapan data "analytics-ready"?** Belum. panel_final.csv masih flat table. Belum Star Schema.

### CHAPTER 5: "Rak Toko" — Load DWH

```
panel_final.csv
    │
    ├── Extract unique wilayah → dim_wilayah (27 rows)
    ├── Generate tahun list   → dim_waktu (6 rows)
    ├── Select fact columns   → fact_dbd_env (162 rows)
    └── 🆕 Write run metadata  → pipeline_runs (1 row per run)
    
         │
    TRUNCATE old data (fact + dim, BUKAN pipeline_runs)
    INSERT new data
         │
    ✅ DATA ANALYTICS-READY + METADATA TERSIMPAN
```

### CHAPTER 6: "Bersih-Bersih" — Cleanup

```
temp/*.csv → DELETE semua
Pipeline selesai.
```

## 2.2 Timeline: Kapan Data Ada Dimana?

```
WAKTU ──────────────────────────────────────────────────────────────▶

t0: CSV files di disk
t1: DataFrame di RAM (ingest) → Database (MySQL/PG)
t2: DataFrame di RAM (extract) → temp CSV di disk
t3: temp CSV → DataFrame di RAM (staging) → PostgreSQL staging tables
t4: Staging tables → DataFrame di RAM (validation) → check selesai
t5: Staging tables → DataFrame di RAM (transform) → panel CSV
t6: panel CSV → DataFrame di RAM → PostgreSQL warehouse tables
t7: temp CSV dihapus. Selesai.
t8: User buka Metabase → SQL query → data dari warehouse → chart
```

## 2.3 Kesalahan Umum

- **Lupa jalankan ingest_sources.py** → Extract gagal karena tabel source tidak ada
- **MySQL/PostgreSQL mati** → Extract timeout
- **Kode wilayah tidak konsisten** → Join menghasilkan NULL → validation gagal
- **RAM tidak cukup** → Untuk 162 rows tidak masalah, tapi kalau scale ke jutaan rows bisa OOM

---

# SECTION 3 — RAW SOURCE LAYER

## 3.1 Apa Itu Raw Data?

Raw data = data **dalam bentuk aslinya** sebelum dibersihkan. Masih ada kolom yang tidak perlu, format nama yang inkonsisten, tahun yang di luar range, dan potensi duplikat.

**Analogi:** Raw data = bahan mentah di pasar. Belum dicuci, belum dipotong, mungkin ada yang busuk.

## 3.2 Kenapa Source Dipisah?

Di dunia nyata, data jarang datang dari satu tempat. Dalam pipeline ini:

| Source | Database | Isi | Asal Dunia Nyata |
|--------|----------|-----|-------------------|
| `source_bps_health` | MySQL | Kasus DBD per kab/kota/tahun | Dinas Kesehatan via BPS |
| `source_bps_kepadatan` | MySQL | Kepadatan penduduk | BPS |
| `source_bps_luas` | MySQL | Luas wilayah (km²) | BPS |
| `source_gee_env` | PostgreSQL | Curah hujan, suhu, NDVI, NDMI | Google Earth Engine (satelit) |
| `jabar_kabkot` | GeoJSON file | Polygon batas wilayah | Shapefile pemerintah |

**Kenapa MySQL DAN PostgreSQL?**

Ini bukan kecelakaan desain. Ini **deliberate multi-source architecture**:
- MySQL mensimulasikan database operasional dinas (OLTP)
- PostgreSQL adalah target DWH kita
- GeoJSON adalah file spatial yang tidak cocok disimpan di RDBMS biasa

Keuntungan: Pipeline membuktikan bisa **integrasi lintas teknologi**.
Risiko: Harus maintain 2 database engine. Koneksi bisa gagal independen.

## 3.3 Detail Setiap Source

### source_bps_health (MySQL)

```
Kolom: id, kode_provinsi, nama_provinsi, kode_bps, nama_kabupaten_kota, 
       jumlah_kasusDBD, satuan, tahun
Rows:  243 (27 kab/kota × 9 tahun: 2016-2024)
```

**Yang penting:**
- `kode_bps` = identifier unik kabupaten (misal: `3201` = Kab. Bogor). Ini **kunci utama** seluruh pipeline
- Data tahun 2016-2018 ada tapi pipeline V3 hanya pakai tahun sesuai konfigurasi `.env` (`YEAR_START`–`YEAR_END`, default: 2019–2024) — difilter di staging
- Kolom `satuan` selalu "ORANG" — tidak informatif, akan dibuang

### source_bps_kepadatan (MySQL)

```
Kolom: id, kode_provinsi, nama_provinsi, kode_bps, nama_kabupaten_kota,
       kepadatan_penduduk, satuan, tahun
Rows:  216 (27 kab/kota × 8 tahun: 2018-2025)
```

**Yang penting:**
- Satuan: jiwa per km² (sudah dihitung BPS)
- Range tahun berbeda dari health (mulai 2018, bukan 2016) — harus di-filter di staging
- Kota Bandung punya kepadatan ~15.000/km². Kab. Pangandaran hanya ~400/km². Variasi SANGAT besar.

### source_bps_luas (MySQL)

```
Data luas daerah setiap kabupaten/kota di Jawa Barat.
Dipakai untuk menghitung: populasi = kepadatan × luas
```

### source_gee_env (PostgreSQL)

```
Kolom: ADM2_NAME, Year, Rainfall_Annual, LST_Celsius, NDVI, NDMI
Rows:  150 (25 wilayah × 6 tahun: 2019-2024)
```

**PERHATIAN — Hanya 25 wilayah!** Dari 27 kab/kota di Jabar, 2 wilayah **tidak ada** di data GEE:
- `3217` — Kab. Bandung Barat
- `3218` — Kab. Pangandaran

Kenapa? Karena GEE pakai boundary yang tidak mengenali 2 kabupaten pemekaran ini. Ini akan di-handle oleh **Tobler Imputation** di tahap Transform.

**Masalah nama:** GEE pakai nama seperti "Bandung", "Kota Bandung", "Bekasi", "Kota Bekasi". BPS pakai "KABUPATEN BANDUNG", "KOTA BANDUNG". Format **berbeda**. Solusi: `region_mapping.csv`.

### region_mapping.csv

```csv
kode_bps,nama_resmi_bps,nama_env,nama_pendek
3201,KABUPATEN BOGOR,Bogor,Kab. Bogor
3217,KABUPATEN BANDUNG BARAT,,KBB          ← nama_env KOSONG!
3218,KABUPATEN PANGANDARAN,,Pangandaran    ← nama_env KOSONG!
```

File ini adalah **Rosetta Stone** pipeline. Menerjemahkan nama wilayah antar sistem. Perhatikan `3217` dan `3218` punya `nama_env` kosong — karena memang tidak ada di data GEE.

### GeoJSON (jabar_kabkot)

```
Format: GeoJSON (MultiPolygon)
Features: 27 polygon (satu per kabupaten/kota)
Property penting: ID_KAB (= kode_bps), KABKOT (nama wilayah)
```

**Yang diekstrak:** Bukan polygon-nya, tapi **centroid** (titik tengah). Centroid menghasilkan `latitude` dan `longitude` yang dipakai Metabase untuk Pin Map.

## 3.4 Kesalahan Umum

- **MySQL down** saat extract → `OperationalError: Can't connect`
- **Password berubah** → edit `.env`, restart
- **GeoJSON corrupt** → geopandas gagal parse → task error
- **region_mapping.csv salah** → join GEE data gagal → environment data hilang

## 3.5 Cara Menjelaskan ke Orang Lain

> "Data kita datang dari 3 tempat: data kasus DBD dari BPS (di MySQL), data lingkungan dari satelit Google Earth Engine (di PostgreSQL), dan data peta batas wilayah (file GeoJSON). Semuanya pakai nama wilayah yang formatnya berbeda, jadi kita punya file pemetaan khusus untuk mencocokkan."

---

# SECTION 4 — STAGING LAYER

> **Ini bagian PALING PENTING untuk dipahami.**
> Staging adalah alasan pipeline V3 jauh lebih baik dari V1/V2.

## 4.1 Apa Itu Staging Sebenarnya?

Staging = **area persiapan** antara raw data dan data warehouse. Data masuk ke sini untuk **dibersihkan dan distandardisasi** sebelum diproses lebih lanjut.

**Analogi terbaik: Dapur Prep di Restoran**

```
Truk sayur datang (raw data)
    │
    ▼
Dapur Prep (staging):
  - Cuci sayur (clean nulls)
  - Buang yang busuk (filter tahun)
  - Potong seragam (standardize kode_bps)
  - Label wadah (rename kolom)
  - Timbang porsi (dedup)
    │
    ▼
Dapur Masak (transform):
  - Resep (feature engineering)
  - Bumbu (CVI, z-score)
    │
    ▼
Meja Saji (warehouse):
  - Plating rapi (star schema)
  - Siap disajikan ke tamu (dashboard)
```

**Chef tidak masak di loading dock. Chef juga tidak mencuci sayur di dapur masak.** Setiap area punya tugas spesifik. Itulah staging.

## 4.2 Kenapa Staging Penting?

**Tanpa staging (V1/V2):**
```
Raw CSV → langsung transform → langsung load
              │
    Masalah:  ├── Kalau transform gagal, harus ulang extract
              ├── Data kotor masuk transform → bug misterius
              ├── Tidak bisa debug: "data bermasalah dari source mana?"
              └── Tidak ada checkpoint antara extract dan transform
```

**Dengan staging (V3):**
```
Raw CSV → Extract → STAGING → Validation → Transform → Load
                       │
          Keuntungan:  ├── Checkpoint: data bersih tersimpan
                       ├── Bisa query staging untuk debug
                       ├── Validation bisa jalan terpisah
                       ├── Transform baca dari staging (bersih)
                       └── Kalau transform gagal, staging masih utuh
```

## 4.3 Apa Yang Terjadi Saat Data Masuk Staging?

Task `load_staging()` menerima 3 parameter (status dari 3 extract task) dan melakukan:

### stg_health_dbd
```python
# 1. Baca temp CSV
df_h = pd.read_csv('temp/raw_health.csv')

# 2. Normalize kode_bps ke string
df_h['kode_bps'] = df_h['kode_bps'].astype(str)  # 3201 → "3201"

# 3. Filter tahun (DINAMIS dari .env, default 2019-2024)
df_h = df_h[df_h['tahun'].between(YEAR_START, YEAR_END)]    # 🆕 Tidak lagi hardcoded!

# 4. Rename kolom (standarisasi)
df_h = df_h.rename(columns={'jumlah_kasusDBD': 'jumlah_kasus_dbd'})

# 5. Select kolom yang diperlukan saja
df_h = df_h[['kode_bps', 'nama_kabupaten_kota', 'tahun', 'jumlah_kasus_dbd']]

# 6. Dedup berdasarkan grain
df_h = df_h.drop_duplicates(subset=['kode_bps', 'tahun'])

# 7. Write ke PostgreSQL staging schema
df_h.to_sql('stg_health_dbd', eng, schema='staging_dbd_v3', if_exists='replace')
```

**Hasil:** 162 rows (27 kab/kota × 6 tahun)

### stg_demografi
```
raw_kepadatan.csv + raw_luas.csv
    │
    ├── Normalize kode_bps
    ├── Filter tahun YEAR_START–YEAR_END (🆕 dinamis dari .env)
    ├── MERGE kepadatan + luas (LEFT JOIN on kode_bps, tahun)
    ├── Select: kode_bps, tahun, kepadatan_penduduk, luas_km2
    ├── Dedup
    └── Write ke staging
```

**Kenapa merge kepadatan dan luas di staging?** Karena keduanya adalah data demografi yang selalu dipakai bersama. Menggabungkan di staging mengurangi complexity di transform.

### stg_environment
```
raw_gee.csv
    │
    ├── Lowercase semua kolom (ADM2_NAME → adm2_name)
    ├── Rename: adm2_name → nama_env, year → tahun
    ├── Cast tahun ke integer
    ├── JOIN dengan region_mapping.csv (nama_env → kode_bps)  ← KRITIS!
    ├── Filter: buang row tanpa kode_bps (yang tidak ada mapping)
    ├── Select: kode_bps, tahun, rainfall_annual, lst_celsius, ndvi, ndmi
    ├── Dedup
    └── Write ke staging
```

**Momen kritis:** Join dengan `region_mapping.csv`. Ini dimana nama GEE ("Bandung") diterjemahkan ke kode BPS ("3204"). Tanpa mapping ini, data lingkungan **tidak bisa digabungkan** dengan data kesehatan.

**Hasil:** 150 rows (25 wilayah × 6 tahun). Bukan 162 karena 2 kabupaten tidak ada di GEE.

### stg_geojson
```
raw_geojson.csv
    │
    ├── Normalize kode_bps
    ├── Dedup (1 row per kabupaten)
    └── Write ke staging
```

**Hasil:** 27 rows. Satu per kabupaten/kota. Tidak ada dimensi waktu — peta tidak berubah per tahun.

## 4.4 Aturan Staging: Yang Boleh dan Tidak Boleh

```
✅ BOLEH di Staging:
  - Rename kolom
  - Cast tipe data
  - Filter range (tahun)
  - Drop duplicates
  - Handle null sederhana
  - Join lookup sederhana (region_mapping)
  - Select kolom yang diperlukan

❌ TIDAK BOLEH di Staging:
  - Feature engineering (CVI, z-score, incidence_rate)
  - Aggregasi (SUM, AVG)
  - Business logic kompleks
  - Cross-table analytics
  - Imputation kompleks (Tobler)
```

**Kenapa?** Staging harus **idempotent dan predictable**. Kalau staging melakukan kalkulasi kompleks, maka debugging jadi sulit. "Bug di CVI — itu dari staging atau transform?" Dengan aturan ini, jawabannya selalu jelas: "CVI hanya dihitung di transform."

## 4.5 Hubungan Antar Layer

```
Extract ──→ Staging ──→ Validation ──→ Transform
  │            │             │              │
  │            │             │              └── Baca DARI staging
  │            │             └── Query staging tables
  │            └── Write KE staging schema
  └── Write ke temp CSV
```

Staging adalah **single source of truth** untuk data bersih. Transform **hanya** membaca dari staging, bukan dari temp CSV atau raw source.

## 4.6 Kesalahan Umum

- **Menaruh feature engineering di staging** → Melanggar separation of concerns
- **Lupa dedup** → Validation akan menangkap, tapi lebih baik dicegah di staging
- **if_exists='replace'** → Setiap run menghapus data lama. Ini by design (full refresh), bukan append
- **Tipe data salah** → `kode_bps` harus string, bukan integer. `3201` vs `"3201"` penting!

## 4.7 Cara Menjelaskan ke Orang Lain

> "Staging itu area bersih-bersih. Data masuk dari berbagai sumber dalam format yang berantakan. Staging membersihkan, menyeragamkan format, membuang duplikat, dan menyimpannya dalam tabel-tabel rapi di PostgreSQL. Tapi staging TIDAK menghitung apapun — itu tugas layer berikutnya."

---

# SECTION 5 — VALIDATION LAYER

## 5.1 Apa Itu Validation?

Validation = **quality gate** antara staging dan transform. Seperti security checkpoint di bandara — kalau tidak lolos, kamu tidak boleh terbang.

```
STAGING ──→ ┌─────────────────────┐ ──→ TRANSFORM
            │   VALIDATION GATE   │
            │                     │
            │  ✓ No duplicates?   │
            │  ✓ No null keys?    │
            │  ✓ Enough rows?     │
            │  ✓ References ok?   │
            │                     │
            │  ALL PASS → GO      │
            │  ANY FAIL → STOP    │
            └─────────────────────┘
```

## 5.2 Empat Validasi Yang Dilakukan

### Check 1: Duplicate Validation

```sql
-- Untuk setiap staging table (kecuali geojson):
SELECT kode_bps, tahun, COUNT(*) AS cnt
FROM staging_dbd_v3.stg_health_dbd
GROUP BY kode_bps, tahun
HAVING COUNT(*) > 1
```

**Apa yang dicek:** Apakah ada kombinasi kode_bps + tahun yang muncul lebih dari sekali? Grain kita adalah 1 kabupaten × 1 tahun. Kalau ada duplikat, berarti dedup di staging gagal.

**Kalau dilanggar:** Kab. Bogor 2024 muncul 2 kali → saat join, row ter-multiply → 324 rows di fact table bukan 162 → semua aggregate (SUM, AVG) salah → insight salah.

**Analogi:** Kalau di daftar hadir ada "Entin" dua kali, total kehadiran jadi terlalu banyak.

### Check 2: Null Key Validation

```sql
SELECT COUNT(*) AS n
FROM staging_dbd_v3.stg_health_dbd
WHERE kode_bps IS NULL
```

**Apa yang dicek:** Apakah ada row tanpa kode_bps? Kode_bps adalah **join key** utama. Tanpa kode_bps, row itu tidak bisa di-join dengan tabel manapun.

**Kalau dilanggar:** Row tanpa kode_bps = data yatim piatu. Tidak bisa ditautkan ke wilayah manapun. Menghitung incidence_rate tanpa tahu wilayahnya = nonsense.

### Check 3: Row Count Validation (🆕 Dynamic Threshold)

```python
# SEBELUMNYA (V3 awal — berbahaya):
if counts['stg_health_dbd'] < 100:        # ← Angka statis!
    errors.append("too few rows")

# SEKARANG (V3 Final — dinamis):
expected_min = N_REGIONS * N_YEARS * 0.8  # 27 × 6 × 0.8 = 129.6
if counts['stg_health_dbd'] < expected_min:
    errors.append(f"too few rows: {counts['stg_health_dbd']} < {expected_min}")
```

Dan untuk geojson:
```python
if counts['stg_geojson'] != 27:
    errors.append("expected 27 kabupaten")
```

**Apa yang dicek:** Apakah jumlah row masuk akal?
- Health harus ≥ 80% dari expected (27 kab × 6 tahun × 80% = ~130)
- GeoJSON harus tepat 27 (1 per kabupaten)

**⚠️ Kenapa threshold statis (`< 100`) berbahaya?**

Bayangkan tahun depan kamu menambah tahun 2025 (7 tahun, expected 189 rows). Dengan threshold statis `100`, pipeline akan tetap PASS meskipun hanya ada 120 rows (kehilangan 69 rows = 36% data hilang!). Threshold dinamis secara otomatis menyesuaikan: `27 × 7 × 0.8 = 151.2`.

### Check 4: Referential Integrity

```sql
SELECT DISTINCT h.kode_bps
FROM staging_dbd_v3.stg_health_dbd h
LEFT JOIN staging_dbd_v3.stg_geojson g ON h.kode_bps = g.kode_bps
WHERE g.kode_bps IS NULL
```

**Apa yang dicek:** Apakah semua kode_bps di health data juga ada di geojson? Kalau Kab. X ada di data kesehatan tapi tidak ada di peta, maka X tidak bisa ditampilkan di map.

**Kalau dilanggar:** Orphan data — ada kasus DBD tapi tidak tahu lokasinya di peta. Dashboard map akan kehilangan titik.

## 5.3 Apa Yang Terjadi Kalau Validation Dilewati?

```
Tanpa Validation:
  Staging (ada 3 duplicate) → Transform → Merge → 165 rows bukan 162
  → incidence_rate Kab. X dihitung 2 kali → aggregate SUM salah
  → Dashboard menunjukkan Kab. X sebagai hotspot padahal bukan
  → Kebijakan kesehatan salah sasaran
  
  "Garbage in, garbage out."
```

## 5.4 Contoh Nyata dari Pipeline V3

```
Validation PASS output:
"valid|health=162|demo=162|env=150|geo=27"

Artinya:
  - 162 row data kesehatan (27 kab × 6 tahun) ✓
  - 162 row data demografi ✓
  - 150 row data environment (25 kab × 6 tahun, 2 kab missing) ✓
  - 27 row geojson (semua kabupaten terwakili) ✓
```

Perhatikan environment hanya 150, bukan 162. Ini **bukan error** — memang 2 kabupaten tidak ada di GEE. Validation membiarkan ini karena check referential hanya cek `health ⊂ geojson`, bukan `health ⊂ environment`.

## 5.5 Kesalahan Umum

- **Validation terlalu ketat** → Pipeline selalu gagal karena edge case
- **Validation terlalu longgar** → Data busuk lolos
- **Tidak membaca error message** → Validation bilang "Orphan kode_bps: ['9999']", tapi developer bingung

## 5.6 Cara Menjelaskan ke Orang Lain

> "Sebelum data diproses, kita jalankan 4 pengecekan otomatis: tidak ada duplikat, tidak ada data tanpa identitas wilayah, jumlah data masuk akal, dan semua wilayah terdaftar di peta. Kalau ada yang gagal, pipeline berhenti. Lebih baik berhenti daripada menghasilkan insight yang salah."

---

# SECTION 6 — TRANSFORM & FEATURE ENGINEERING

## 6.1 Apa Yang Terjadi di Transform?

Transform adalah **otak** pipeline. Di sinilah data mentah berubah menjadi **insight-ready features**.

```
INPUT:  4 staging tables (bersih, tervalidasi)
OUTPUT: panel_final.csv (162 rows × 25+ kolom, termasuk derived features)
```

**Analogi:** Staging = bahan bersih di meja. Transform = proses memasak. Output = hidangan siap saji.

## 6.2 Step 1: Merge (Penggabungan)

```python
# Join 4 tabel menjadi 1 panel data
panel = df_h.merge(df_d, on=['kode_bps', 'tahun'], how='left')
panel = panel.merge(df_e, on=['kode_bps', 'tahun'], how='left')
panel = panel.merge(df_g[['kode_bps', 'latitude', 'longitude']],
                    on='kode_bps', how='left')
```

**Visualisasi:**
```
stg_health_dbd      stg_demografi       stg_environment     stg_geojson
┌──────────────┐    ┌─────────────┐     ┌──────────────┐    ┌──────────┐
│kode_bps      │    │kode_bps     │     │kode_bps      │    │kode_bps  │
│tahun         │    │tahun        │     │tahun         │    │latitude  │
│nama_kab      │    │kepadatan    │     │rainfall      │    │longitude │
│jumlah_kasus  │    │luas_km2     │     │lst_celsius   │    └────┬─────┘
└──────┬───────┘    └──────┬──────┘     │ndvi, ndmi    │         │
       │                   │            └──────┬───────┘         │
       └───────┬───────────┘                   │                 │
               │    LEFT JOIN on               │                 │
               │    kode_bps + tahun           │                 │
               └───────────┬───────────────────┘                 │
                           │    LEFT JOIN on kode_bps            │
                           └───────────────┬─────────────────────┘
                                           │
                                           ▼
                                    PANEL (merged)
                              162 rows × 12+ columns
```

**Kenapa LEFT JOIN?** Karena kita ingin mempertahankan SEMUA row dari health, meskipun beberapa kabupaten tidak punya data environment. LEFT JOIN = "ambil semua dari kiri, tambahkan dari kanan kalau ada".

**Hasil merge:** 2 kabupaten (3217 KBB, 3218 Pangandaran) punya `NULL` di kolom environment.

## 6.3 Step 2: Tobler Imputation

```python
# Kab. Bandung Barat (3217) → pakai data Kab. Bandung (3204)
# Kab. Pangandaran (3218)   → pakai data Kab. Ciamis (3207)
```

**Apa itu Tobler Imputation?** Berdasarkan **Hukum Geografi Pertama Tobler**: "Segala sesuatu berhubungan dengan segala sesuatu lainnya, tapi sesuatu yang dekat lebih berhubungan daripada yang jauh."

Artinya: cuaca di Kab. Bandung Barat kemungkinan **mirip** dengan Kab. Bandung (berbatasan). Lebih baik pakai data tetangga daripada rata-rata seluruh Jawa Barat.

**Ini perbaikan dari V1** yang menggunakan "blind imputation" (rata-rata provinsi), yang merusak varians data.

```
Sebelum imputation:
  3217 | 2019 | rainfall: NULL | lst: NULL | ndvi: NULL | ndmi: NULL

Sesudah imputation:
  3217 | 2019 | rainfall: 2174.45 | lst: 28.37 | ndvi: 0.634 | ndmi: 0.099
  (diambil dari 3204 tahun 2019)
```

`is_imputed` flag di-set `True` untuk row yang di-impute. Ini transparansi — user dashboard bisa filter data asli vs imputasi.

## 6.4 Step 3: Feature Engineering (Detail)

### incidence_rate — Laju Kejadian per 100.000 Penduduk

```python
incidence_rate = (jumlah_kasus_dbd / (kepadatan_penduduk × luas_km2)) × 100.000
```

**Kenapa penting?** Jumlah kasus mentah **misleading**. Kota Bandung (3000 kasus, 2.5 juta penduduk) vs Kota Banjar (100 kasus, 80.000 penduduk). Siapa yang lebih parah? Incidence rate menjawab: **per 100.000 orang**, apple-to-apple.

**Analogi:** Membandingkan gol Messi vs pemain divisi 3. Harus pakai "gol per pertandingan", bukan total gol.

```
Contoh:
  Kota Bandung: 4424 kasus / (14957 × 167.31 km²) × 100.000 ≈ 176.8
  Kota Banjar:  62 kasus / (1613 × 49.71 km²) × 100.000 ≈ 77.3
```

### yoy_growth_rate — Pertumbuhan Year-over-Year

```python
yoy_growth_rate = pct_change(incidence_rate) × 100   # dalam persen
```

**Apa artinya:** "Seberapa besar perubahan IR dari tahun lalu?"
- Positif = kasus meningkat
- Negatif = kasus menurun
- > 50% = lonjakan signifikan

**Kenapa penting untuk DBD?** Deteksi **surge** (lonjakan). Kalau YoY > 50% DAN rainfall_zscore > 1.5, itu **HIGH RISK** — mungkin ada outbreak.

### rainfall_zscore — Anomali Curah Hujan

```python
rainfall_zscore = zscore(rainfall_mm, per tahun)
```

**Apa itu z-score?** Mengukur **seberapa jauh** suatu nilai dari rata-rata, dalam satuan standar deviasi.
- z = 0 → rata-rata
- z = 1.5 → jauh di atas rata-rata (hujan sangat lebat)
- z = -1.5 → jauh di bawah rata-rata (kering)

**Relevansi DBD:** Nyamuk Aedes berkembang biak di genangan air. Curah hujan tinggi = lebih banyak genangan = lebih banyak nyamuk = lebih banyak DBD.

```
Contoh (tahun 2020):
  Kab. Bekasi: rainfall = 2422 mm, zscore ≈ -0.8 (di bawah rata-rata tahun itu)
  Kota Bogor:  rainfall = 4396 mm, zscore ≈ +1.6 (jauh di atas rata-rata)
```

### lst_quartile_rank — Ranking Suhu Permukaan

```python
lst_quartile_rank = qcut(lst_celsius per tahun, 4 groups: Q1-Q4)
```

**Apa itu quartile?** Membagi data jadi 4 kelompok sama besar:
- Q1 = 25% terdingin
- Q4 = 25% terpanas

**Relevansi DBD:** Nyamuk punya suhu optimal 25-30°C. Q3-Q4 bisa jadi indikator heat stress yang mengurangi breeding.

### ndvi_class — Klasifikasi Vegetasi

```python
ndvi_class = 'Sparse'   jika NDVI < 0.4
             'Moderate'  jika 0.4 ≤ NDVI ≤ 0.6
             'Dense'     jika NDVI > 0.6
```

**NDVI** (Normalized Difference Vegetation Index) = indeks kehijauan dari satelit. Range 0-1.

**Relevansi DBD:** Vegetasi padat = lebih banyak tempat berlindung nyamuk, tapi juga lebih banyak predator alami. Moderate vegetation sering paling berisiko.

### lag1/2/3_rainfall — Curah Hujan Historis

```python
lag1_rainfall = rainfall bulan/tahun sebelumnya
lag2_rainfall = rainfall 2 periode sebelumnya
lag3_rainfall = rainfall 3 periode sebelumnya
```

**Kenapa lag penting?** Hujan hari ini tidak langsung bikin DBD besok. Ada **delay**: hujan → genangan → jentik → nyamuk dewasa → menggigit → sakit → lapor. Proses ini bisa 1-3 bulan. Lag menangkap efek **tertunda** ini.

**Catatan:** Karena data kita tahunan (bukan bulanan), lag1 = tahun lalu. Untuk tahun pertama (2019), lag diisi dengan nilai saat itu sendiri (`fillna`).

### composite_vulnerability_index (CVI) — Indeks Kerentanan Gabungan

```python
CVI = 0.30 × rainfall_zscore
    + 0.25 × zscore(lst_celsius)
    + 0.25 × zscore(kepadatan_penduduk)
    + 0.20 × (1 - zscore(ndvi))
```

**Ini fitur PALING PENTING dalam pipeline.** CVI menggabungkan 4 faktor risiko DBD menjadi 1 skor:

| Faktor | Bobot | Logika |
|--------|-------|--------|
| Rainfall zscore | 30% | Hujan tinggi → genangan → nyamuk |
| LST zscore | 25% | Suhu optimal → breeding |
| Kepadatan zscore | 25% | Padat penduduk → transmisi cepat |
| 1 - NDVI zscore | 20% | Vegetasi rendah → urban → breeding di wadah buatan |

**Perhatikan (1 - zscore_ndvi):** Dibalik! NDVI tinggi = baik. Jadi 1-NDVI = semakin tinggi → semakin rentan.

CVI tinggi = **kabupaten sangat rentan** terhadap DBD.

### endemic_persistence_score — Skor Persistensi Endemik

```python
# ir_q = quartile incidence_rate per tahun (1-4)
# endemic_persistence = berapa kali kabupaten masuk Q4 (top 25%) selama 6 tahun
endemic_persistence_score = (ir_q == 4).sum() per kode_bps
```

**Contoh:**
- Kota Bandung: IR masuk Q4 di 5 dari 6 tahun → score = 5 → **persistently endemic**
- Kab. Pangandaran: IR masuk Q4 di 0 dari 6 tahun → score = 0 → relatif aman

## 6.5 Apa Yang Terjadi di Pandas/RAM?

```
Saat transform() berjalan:
  1. 4x pd.read_sql() → 4 DataFrame di RAM (~150KB total, kecil)
  2. 3x merge → 1 large DataFrame (~200KB)
  3. Feature engineering → kolom bertambah, tapi row tetap 162
  4. .to_csv() → write ke disk
  5. Task selesai → semua DataFrame di-garbage-collect → RAM bebas

Memory usage: < 50MB untuk dataset ini. 
TAPI kalau data jadi jutaan rows, memory bisa jadi masalah.
```

## 6.6 🆕 NaN Guard Post-Imputasi

Setelah Tobler Imputation dan semua feature engineering selesai, pipeline sekarang menjalankan **validasi tambahan** pada kolom kritis:

```python
# Cek apakah incidence_rate punya NaN setelah semua proses
null_ir = panel['incidence_rate'].isna().sum()
if null_ir > 0:
    logging.warning(f"[NaN Guard] {null_ir} rows punya incidence_rate NULL setelah transform!")
```

**Kenapa ini penting?** Tanpa NaN Guard, skenario berikut bisa terjadi:
1. Data kepadatan penduduk kosong untuk 1 kabupaten
2. `incidence_rate = kasus / (kepadatan × luas)` → division by NaN → IR = NaN
3. NaN masuk ke `fact_dbd_env` **tanpa peringatan**
4. Dashboard Metabase menampilkan chart dengan lubang data — user bingung
5. AVG(incidence_rate) dihitung tanpa kabupaten itu — **bias tersembunyi**

NaN Guard mendeteksi ini di tahap transform, **sebelum** data masuk warehouse. Jumlah NaN IR juga dicatat di tabel `pipeline_runs` sebagai `null_ir_rows`.

## 6.7 🆕 GEE Mapping Warning System

Saat data GEE di-map ke `kode_bps` melalui `region_mapping.csv`, ada kemungkinan nama wilayah tidak cocok:

```python
# Warning log saat mapping GEE → kode_bps
unmatched = df_env[df_env['kode_bps'].isna()]['adm2_name'].unique()
if len(unmatched) > 0:
    logging.warning(f"[GEE Mismatch] Wilayah tidak ter-mapping: {unmatched}")
```

**Ini BUKAN error — tapi sinyal penting.** Jika GEE mengubah nama wilayah (misal: "Bandung" → "Kota Bandung"), mapping akan gagal **diam-diam** dan data lingkungan hilang tanpa peringatan. Sistem warning ini memastikan engineer segera tahu.

## 6.8 Kesalahan Umum

- **Lupa sort sebelum shift()** → lag feature salah urutan
- **Division by zero** di incidence_rate → kalau kepadatan × luas = 0
- **NaN propagation** → 1 NaN di CVI component → seluruh CVI jadi NaN
- **Quartile error** → kalau semua nilai sama, `qcut` gagal (tidak bisa bagi 4)

## 6.9 Cara Menjelaskan ke Orang Lain

> "Transform mengambil 4 tabel staging yang bersih, menggabungkannya jadi 1 tabel besar, lalu menghitung 15+ metrik epidemiologi: laju kasus per 100.000 penduduk, anomali curah hujan, indeks kerentanan gabungan, dan skor persistensi endemik. Hasilnya: setiap kabupaten di setiap tahun punya 'profil risiko DBD' yang lengkap."

---

# SECTION 7 — STAR SCHEMA & DATA WAREHOUSE

## 7.1 Apa Itu Data Warehouse?

Data Warehouse (DWH) = **gudang data yang dirancang khusus untuk analisis**, bukan untuk operasi sehari-hari.

**Kenapa tidak query langsung dari raw/staging?**

| Aspek | Raw/Staging | Data Warehouse |
|-------|-------------|----------------|
| Tujuan | Menyimpan data sementara | Menyimpan data untuk analisis |
| Struktur | Flat, banyak tabel terpisah | Star Schema (terorganisir) |
| Query speed | Lambat (banyak JOIN ad-hoc) | Cepat (JOIN sudah dioptimasi) |
| Konsistensi | Bisa berubah saat pipeline jalan | Stabil (hanya update saat load) |
| User | Engineer | Analis/Dashboard |

**Analogi:** Raw = gudang penyimpanan bahan. Staging = dapur prep. DWH = **menu restoran** — customer tidak perlu tahu bahan apa yang ada di gudang, mereka cukup pilih dari menu.

## 7.2 Apa Itu Star Schema?

Star Schema = model database dimana ada **1 tabel fakta (fact)** di tengah, dikelilingi oleh **tabel dimensi (dimension)**.

```
                    ┌──────────────┐
                    │  dim_waktu   │
                    │──────────────│
                    │ tahun (PK)   │
                    │ covid_dummy  │
                    │ ket_periode  │
                    └──────┬───────┘
                           │
                           │ FK: tahun
                           │
┌──────────────┐    ┌──────┴───────────────────┐
│ dim_wilayah  │    │      fact_dbd_env        │
│──────────────│    │──────────────────────────│
│ kode_bps(PK) ├────┤ fact_id (PK)             │
│ nama_kab     │ FK │ kode_bps (FK)            │
│ kode_prov    │    │ tahun (FK)               │
│ nama_prov    │    │ jumlah_kasus_dbd         │
│ latitude     │    │ incidence_rate           │
│ longitude    │    │ kepadatan_penduduk       │
└──────────────┘    │ rainfall_mm, ndvi, ...   │
                    │ CVI, lag1-3, ir_q, ...   │
                    └──────────────────────────┘
```

**Kenapa disebut "Star"?** Kalau digambar, bentuknya seperti bintang — fact di tengah, dimensi di sekeliling.

## 7.3 Apa Itu Dimension vs Fact?

### Dimension = "Konteks" (siapa, dimana, kapan)

Tabel dimensi menjawab pertanyaan **deskriptif**:
- **dim_wilayah:** "Dimana?" — nama kabupaten, provinsi, koordinat
- **dim_waktu:** "Kapan?" — tahun, apakah pandemi atau normal

Dimensi punya:
- Primary Key (PK) — identifier unik
- Atribut deskriptif — informasi yang jarang berubah
- **Sedikit row, banyak dipakai untuk filter**

### Fact = "Pengukuran" (berapa, seberapa)

Tabel fakta menyimpan **metrik/ukuran** yang ingin dianalisis:
- Jumlah kasus, incidence rate, CVI, rainfall, dll
- **Banyak row, banyak kolom numerik**

## 7.4 Detail Tabel Pipeline V3

### dim_wilayah (27 rows)

```sql
CREATE TABLE warehouse_dbd_v3.dim_wilayah (
    kode_bps            VARCHAR(10) PRIMARY KEY,  -- "3201"
    nama_kabupaten_kota VARCHAR(100),              -- "KABUPATEN BOGOR"
    kode_provinsi       VARCHAR(10),               -- "32"
    nama_provinsi       VARCHAR(50),               -- "JAWA BARAT"
    latitude            FLOAT,                     -- -6.82 (dari centroid)
    longitude           FLOAT                      -- 106.75
);
```

**27 rows = 27 kabupaten/kota Jawa Barat.** Tidak ada dimensi waktu — wilayah tidak berubah per tahun.

Latitude/longitude masuk di dim_wilayah (bukan fact) karena koordinat adalah **properti tetap** wilayah, bukan pengukuran yang berubah.

### dim_waktu (6 rows)

```sql
CREATE TABLE warehouse_dbd_v3.dim_waktu (
    tahun               INT PRIMARY KEY,    -- 2019, 2020, ..., 2024
    covid_dummy         INT,                -- 1 jika 2020/2021, else 0
    keterangan_periode  VARCHAR(20)         -- 'Pandemi' atau 'Normal'
);
```

**6 rows = 6 tahun observasi.** `covid_dummy` memungkinkan analisis "apakah pandemi mempengaruhi kasus DBD?" tanpa hardcode di query.

**🆕 Catatan:** Daftar tahun pandemi (`PANDEMIC_YEARS`) juga dikonfigurasi melalui `.env`, bukan hardcoded. Jika definisi periode pandemi berubah, cukup edit `.env`.

### fact_dbd_env (162 rows) — ⭐ TABEL TERPENTING

```
┌─────────────────────────────────────────────────────────┐
│ 1 ROW FACT TABLE MEREPRESENTASIKAN:                     │
│                                                         │
│   "Profil lengkap DBD dan lingkungan untuk              │
│    SATU kabupaten/kota pada SATU tahun observasi"       │
│                                                         │
│ Grain: kode_bps × tahun                                │
│ Total: 27 kab/kota × 6 tahun = 162 rows                │
└─────────────────────────────────────────────────────────┘
```

### 🆕 pipeline_runs (Audit Trail — append-only)

```sql
CREATE TABLE warehouse_dbd_v3.pipeline_runs (
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
```

**Ini adalah tabel metadata — bukan tabel analitik.** Tabel ini:
- **Append-only:** TIDAK di-TRUNCATE setiap run (berbeda dengan dim/fact)
- **1 row per eksekusi pipeline:** Setiap kali `load_dwh()` selesai, 1 row ditulis
- **Self-documenting:** Kolom `imputed_rows` dan `null_ir_rows` langsung menunjukkan kualitas data tanpa perlu query tambahan

**Contoh isi `pipeline_runs` setelah 3 kali run:**
```
run_id | run_at              | year_start | year_end | fact_rows | imputed_rows | null_ir_rows | status
-------+---------------------+------------+----------+-----------+--------------+--------------+--------
     1 | 2026-05-01 20:04:00 |       2019 |     2024 |       162 |           12 |            0 | SUCCESS
     2 | 2026-06-01 08:00:00 |       2019 |     2024 |       162 |           12 |            0 | SUCCESS
     3 | 2026-07-01 08:00:00 |       2019 |     2025 |       189 |           14 |            2 | SUCCESS  ← ANOMALI!
```

Pada run ke-3, `null_ir_rows = 2` — ini sinyal bahwa data 2025 punya 2 kabupaten tanpa incidence rate. Tanpa tabel ini, anomali ini **tidak terdeteksi** kecuali ada orang yang kebetulan memeriksa dashboard.

> **Lihat Section 16 untuk penjelasan mendalam tentang Pipeline Observability.**

**Kolom-kolom fact_dbd_env:**

| Kolom | Tipe | Artinya |
|-------|------|---------|
| fact_id | SERIAL PK | Auto-increment ID |
| kode_bps | FK → dim_wilayah | "Dimana" |
| tahun | FK → dim_waktu | "Kapan" |
| jumlah_kasus_dbd | INT | Measure: kasus mentah |
| incidence_rate | FLOAT | Measure: IR per 100K |
| kepadatan_penduduk | FLOAT | Measure: jiwa/km² |
| rainfall_mm | FLOAT | Measure: curah hujan |
| composite_vulnerability_index | FLOAT | Measure: CVI |
| is_imputed | BOOLEAN | Flag: data asli/imputasi |
| ... | ... | 25 kolom total |

## 7.5 Kenapa Kepadatan di Fact, Bukan di Dimension?

**Ini perbaikan V3 dari V1.** Di V1, kepadatan_penduduk dimasukkan di dim_demografi. Itu **salah** karena:

- Kepadatan berubah setiap tahun (bukan atribut tetap)
- Kepadatan adalah angka kontinu (bukan kategori)
- Menempatkan di dimensi → Slowly Changing Dimension yang tidak terkendali

**Aturan praktis:** Kalau nilainya **berubah setiap observasi** dan **numerik kontinu**, itu FACT. Kalau nilainya **tetap/jarang berubah** dan **deskriptif/kategorikal**, itu DIMENSION.

## 7.6 Foreign Key & Analytics-Ready

```sql
-- Contoh query analytics yang "just works":
SELECT w.nama_kabupaten_kota, t.keterangan_periode,
       AVG(f.incidence_rate) AS avg_ir
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
JOIN warehouse_dbd_v3.dim_waktu t ON f.tahun = t.tahun
GROUP BY w.nama_kabupaten_kota, t.keterangan_periode;
```

**Inilah analytics-ready.** Analis cukup JOIN 3 tabel, pakai GROUP BY, dan dapat jawaban.

## 7.7 Cara Menjelaskan ke Orang Lain

> "Data warehouse kita pakai star schema: 1 tabel utama (fact_dbd_env) berisi 162 baris — satu per kabupaten per tahun — dengan semua metrik (kasus, incidence rate, CVI, dll). Di sekelilingnya ada 2 tabel referensi: daftar wilayah (dengan koordinat peta) dan daftar tahun (dengan flag pandemi). Untuk analisis, tinggal JOIN ketiga tabel."

---

# SECTION 8 — AIRFLOW ORCHESTRATOR

## 8.1 Apa Itu Orchestrator?

Orchestrator = **konduktor orkestra**. Tidak memainkan alat musik sendiri, tapi **mengatur siapa bermain kapan, memastikan urutan benar, dan menangani kalau ada yang salah**.

Tanpa orchestrator, kamu harus manual menjalankan script satu per satu:
```bash
python extract_mysql.py       # Tunggu selesai...
python extract_postgres.py    # Tunggu selesai...
python extract_geojson.py     # Tunggu selesai...
python load_staging.py        # Tunggu selesai...
python validate.py            # Tunggu selesai...
python transform.py           # Tunggu selesai...
python load_dwh.py            # Selesai? Mungkin? Siapa yang tahu?
```

Dengan Airflow:
```
1 klik trigger → semua task jalan otomatis → log tersedia → retry otomatis
```

## 8.2 Kenapa Airflow, Bukan Cron?

| Aspek | Cron | Airflow |
|-------|------|---------|
| Dependencies | Tidak ada | task A harus selesai sebelum B |
| Retry | Manual | Otomatis (configurable) |
| Monitoring | Cek file log | Web UI visual |
| Paralelisme | Tidak ada | Fan-out/fan-in |
| Error handling | Email (kalau di-setup) | UI + log detail per task |

## 8.3 Apa Yang Terjadi Saat DAG Ditrigger?

```
┌─────────────────────────────────────────────────────────┐
│ USER klik "Trigger DAG" di Airflow UI (localhost:8095)  │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ SCHEDULER membaca DAG file (dag_dbd_v3.py)              │
│ → Parse dependency graph                                │
│ → Identify task yang ready (no upstream dependency)     │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ FAN-OUT: 3 extract tasks jalan PARALEL                  │
│                                                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐       │
│  │extract_mysql│ │extract_pg   │ │extract_geo  │       │
│  │   ⏳ running │ │   ⏳ running │ │   ⏳ running │       │
│  │   ✅ done   │ │   ✅ done   │ │   ✅ done   │       │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘       │
│         └───────────────┼───────────────┘               │
└─────────────────────────┤───────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────┐
│ FAN-IN: load_staging() menunggu KETIGA extract selesai  │
│ → Baru mulai saat semua dependency terpenuhi            │
│ → ✅ staging loaded                                     │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ SEQUENTIAL: validate → transform → load_dwh → cleanup  │
│                                                         │
│  validate ──→ transform ──→ load_dwh ──→ cleanup       │
│    ✅            ✅            ✅           ✅           │
└─────────────────────────────────────────────────────────┘
                        ▼
                   DAG COMPLETE ✅
```

## 8.4 Konsep Penting Airflow

### Fan-Out (Paralel)
```
         ┌── extract_mysql
trigger ─┼── extract_postgres
         └── extract_geojson
```
3 task **tidak saling bergantung** → jalan bersamaan. Hemat waktu.

### Fan-In (Konvergensi)
```
extract_mysql    ──┐
extract_postgres ──┼── load_staging
extract_geojson  ──┘
```
`load_staging` menunggu **semua** extract selesai. Kalau 1 gagal, staging tidak jalan.

### Retry
```python
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
```
Kalau task gagal, Airflow **otomatis coba lagi** setelah 2 menit. Maksimal 1 retry. Kalau masih gagal → task marked FAILED.

### Task State Lifecycle
```
none → scheduled → queued → running → success
                                    → failed → up_for_retry → running → success/failed
```

## 8.5 Konfigurasi Airflow V3

```
AIRFLOW_HOME = ~/airflow_home_v3  (🆕 di filesystem Linux, BUKAN /mnt/d/)
Port: 8095 (bukan 8080 default — untuk isolasi dari V1/V2)
DAG folder: /mnt/d/.../docs_v3/dags/ (di-mount dari Windows)
Database: SQLite (default, acceptable untuk single-user)
Executor: SequentialExecutor (SQLite limitation)
```

**🆕 Kenapa AIRFLOW_HOME di `~/` (Linux)?** SQLite tidak bisa berjalan stabil di `/mnt/d/` (filesystem Windows yang di-mount ke WSL). Error `disk I/O error` akan muncul. Solusinya: simpan Airflow metadata (SQLite) di filesystem native Linux, tapi arahkan `dags_folder` ke drive Windows dimana kode project berada.

**Kenapa port 8095?** Karena V1/V2 mungkin sudah pakai 8080. Isolasi port = tidak saling ganggu.

**Kenapa SQLite acceptable?** Karena ini single-user academic project. Untuk production multi-user, harus pakai PostgreSQL backend + CeleryExecutor/KubernetesExecutor.

## 8.6 Cara Menjelaskan ke Orang Lain

> "Airflow itu konduktor orkestra. Dia tahu task mana yang harus jalan duluan, mana yang bisa paralel, mana yang harus nunggu. Kalau ada yang gagal, dia coba ulang otomatis. Kita cukup trigger sekali, lalu pantau lewat dashboard web."

---

# SECTION 9 — SPATIAL & GEOJSON

## 9.1 Apa Itu GeoJSON?

GeoJSON = format file untuk menyimpan **data geografis** dalam JSON. Dalam pipeline ini, kita punya file `jabar_kabkot` yang berisi **27 polygon** — masing-masing merepresentasikan batas wilayah 1 kabupaten/kota.

```json
{
  "type": "Feature",
  "properties": {
    "ID_KAB": 3201,
    "KABKOT": "KABUPATEN BOGOR"
  },
  "geometry": {
    "type": "MultiPolygon",
    "coordinates": [[[106.5, -6.5], [106.8, -6.5], ...]]
  }
}
```

**Analogi:** GeoJSON itu seperti **peta transparansi** yang bisa ditumpuk di atas data. Setiap polygon = 1 wilayah administratif.

## 9.2 Dari Polygon ke Titik (Centroid)

```python
gdf['centroid'] = gdf.geometry.centroid
gdf['latitude'] = gdf['centroid'].y
gdf['longitude'] = gdf['centroid'].x
```

**Apa itu centroid?** Titik tengah geometris dari polygon. Bayangkan potong karton berbentuk Kab. Bogor, lalu cari titik keseimbangannya — itu centroid.

```
  ┌─────────────┐
  │             │
  │      ●      │  ← centroid (titik tengah)
  │   polygon   │     latitude = -6.82
  │  Kab Bogor  │     longitude = 106.75
  └─────────────┘
```

**Kenapa centroid, bukan polygon penuh?**
- Metabase Pin Map butuh **titik koordinat** (lat/lon), bukan polygon
- Menyimpan polygon penuh ke database = **besar dan lambat**
- Untuk analisis epidemiologi level kabupaten, centroid sudah cukup

## 9.3 Kenapa Spatial Penting untuk Epidemiologi?

1. **Visualisasi peta** → Melihat pola spasial penyebaran DBD
2. **Clustering** → Kabupaten tetangga punya kasus tinggi? Mungkin ada penyebaran antar-wilayah
3. **Korelasi lingkungan** → Curah hujan tinggi di wilayah X? Cek apakah kasus DBD juga tinggi
4. **Policy targeting** → "Fokuskan fogging di kluster kabupaten selatan yang CVI-nya tinggi"

## 9.4 Bagaimana Metabase Menggunakan Lat/Lon?

```
dim_wilayah
┌──────────┬──────────┬───────────┐
│ kode_bps │ latitude │ longitude │
├──────────┼──────────┼───────────┤
│ 3201     │ -6.5821  │ 106.7492  │  → pin di peta
│ 3273     │ -6.9175  │ 107.6191  │  → pin di peta
│ ...      │ ...      │ ...       │
└──────────┴──────────┴───────────┘
```

Query Metabase Pin Map:
```sql
SELECT w.nama_kabupaten_kota, w.latitude, w.longitude,
       f.incidence_rate, f.composite_vulnerability_index
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
WHERE f.tahun = (SELECT MAX(tahun) FROM warehouse_dbd_v3.fact_dbd_env)  -- 🆕 Dinamis!
```

**🆕 Catatan:** Query di atas menggunakan subquery `MAX(tahun)` alih-alih hardcoded `WHERE tahun = 2024`. Ini berarti saat data 2025 ditambahkan, **dashboard otomatis menampilkan tahun terbaru** tanpa perlu edit query.

Metabase membaca `latitude` dan `longitude` → merender pin/circle di peta → ukuran/warna berdasarkan `incidence_rate` atau `CVI`.

## 9.5 Cara Menjelaskan ke Orang Lain

> "Kita pakai file peta digital (GeoJSON) yang berisi batas-batas 27 kabupaten/kota Jabar. Dari peta itu, kita ambil titik tengah setiap wilayah (centroid) untuk mendapatkan koordinat latitude dan longitude. Koordinat ini disimpan di warehouse dan dipakai Metabase untuk menampilkan data di peta interaktif."

---

# SECTION 10 — OLAP & METABASE

## 10.1 OLAP vs OLTP

| | OLTP | OLAP |
|--|------|------|
| Singkatan | Online Transaction Processing | Online Analytical Processing |
| Tujuan | Catat transaksi | Analisis data |
| Query | INSERT 1 row | SELECT + GROUP BY jutaan rows |
| User | Aplikasi, operator | Analis, dashboard |
| Contoh | "Tambah kasus baru" | "Rata-rata IR per kab 2019-2024" |

**Pipeline kita:** MySQL (OLTP source) → pipeline → PostgreSQL DWH (OLAP ready) → Metabase

## 10.2 OLAP Queries dari Pipeline V3

### UC1: Tren IR + Rainfall Anomaly (Deteksi Surge)
```sql
SELECT w.nama_kabupaten_kota, f.tahun, f.incidence_rate,
       f.yoy_growth_rate, f.rainfall_zscore,
       CASE WHEN f.yoy_growth_rate > 50 AND f.rainfall_zscore > 1.5
            THEN 'HIGH RISK' ELSE 'Normal' END AS surge_flag
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
ORDER BY f.yoy_growth_rate DESC NULLS LAST;
```
**Cerita:** "Tunjukkan kabupaten yang punya lonjakan kasus (YoY > 50%) DAN curah hujan abnormal tinggi. Itu sinyal outbreak."

### UC7: CVI + Map (Peta Kerentanan)
```sql
SELECT w.nama_kabupaten_kota, w.latitude, w.longitude,
       AVG(f.composite_vulnerability_index) AS avg_cvi,
       SUM(f.jumlah_kasus_dbd) AS total_kasus
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_wilayah w ON f.kode_bps = w.kode_bps
GROUP BY w.nama_kabupaten_kota, w.latitude, w.longitude
ORDER BY avg_cvi DESC;
```
**Cerita:** "Buat peta dimana kabupaten dengan CVI tertinggi ditandai. Ini prioritas intervensi."

### COVID Analysis
```sql
SELECT t.keterangan_periode, AVG(f.incidence_rate) AS avg_ir,
       SUM(f.jumlah_kasus_dbd) AS total_kasus
FROM warehouse_dbd_v3.fact_dbd_env f
JOIN warehouse_dbd_v3.dim_waktu t ON f.tahun = t.tahun
GROUP BY t.keterangan_periode;
```
**Cerita:** "Bandingkan rata-rata IR saat pandemi vs normal. Apakah lockdown mengurangi DBD?"

## 10.3 Bagaimana Dashboard Terbentuk?

```
Engineer menulis query SQL
    │
    ▼
Metabase menyimpan query sebagai "Question"
    │
    ▼
Question ditampilkan sebagai chart (bar, line, map)
    │
    ▼
Beberapa Question dikelompokkan dalam "Dashboard"
    │
    ▼
User buka Dashboard → semua chart ter-render otomatis
```

## 10.4 Cara Menjelaskan ke Orang Lain

> "OLAP itu gaya query untuk analisis: GROUP BY, AVG, SUM, trend. Metabase tinggal kirim query SQL ke PostgreSQL DWH, dapat hasilnya, lalu render jadi chart atau peta. User tidak perlu tahu SQL — mereka cukup buka dashboard."

---

# SECTION 11 — WHAT ACTUALLY HAPPENS WHEN USER OPENS DASHBOARD

## 11.1 Behind the Scenes (Step-by-Step)

```
┌─────────────────────────────────────────────────────────┐
│ 1. User buka browser → http://localhost:3000            │
│    (Metabase running di port 3000)                      │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ 2. Metabase load dashboard configuration                │
│    → "Dashboard ini punya 4 questions"                  │
│    → Masing-masing question = 1 SQL query               │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ 3. Metabase kirim 4 SQL queries ke PostgreSQL           │
│    Connection: localhost:5432/dwh_dbd_v3                 │
│                                                         │
│    Query 1: SELECT ... FROM fact JOIN dim_wilayah ...   │
│    Query 2: SELECT ... GROUP BY tahun ...               │
│    Query 3: SELECT ... w.latitude, w.longitude ...      │
│    Query 4: SELECT ... WHERE tahun = 2024 ...           │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ 4. PostgreSQL menerima query                            │
│    → Query planner membuat execution plan               │
│    → Baca fact_dbd_env (162 rows, kecil)               │
│    → JOIN ke dim_wilayah (27 rows)                     │
│    → JOIN ke dim_waktu (6 rows)                        │
│    → Aggregate (AVG, SUM, GROUP BY)                    │
│    → Return result set ke Metabase                     │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ 5. Metabase menerima result set                         │
│    → Query 1 result → render sebagai Line Chart        │
│    → Query 2 result → render sebagai Bar Chart         │
│    → Query 3 result → render sebagai Pin Map           │
│         (latitude/longitude → titik di peta)           │
│    → Query 4 result → render sebagai Table             │
└───────────────────────┬─────────────────────────────────┘
                        ▼
┌─────────────────────────────────────────────────────────┐
│ 6. Browser merender semua visualisasi                   │
│    → User melihat dashboard lengkap                    │
│    → Total waktu: < 2 detik (data kecil)              │
└─────────────────────────────────────────────────────────┘
```

## 11.2 Kenapa Cepat?

- **Data kecil:** 162 rows fact + 27 rows dim + 6 rows dim = trivial untuk PostgreSQL
- **Star Schema optimized:** JOIN sudah terstruktur, bukan ad-hoc
- **No computation at query time:** Semua feature (CVI, IR, z-score) sudah **pre-computed** di pipeline. Dashboard hanya SELECT, tidak perlu hitung.

**Ini keuntungan besar DWH:** Transformasi berat dilakukan **sekali** (saat pipeline jalan). Query dashboard hanya membaca **hasil jadi**.

## 11.3 Cara Menjelaskan ke Orang Lain

> "Saat user buka dashboard, Metabase kirim SQL query ke PostgreSQL, yang membaca fact table (162 baris) dan JOIN ke dimension tables. Hasilnya langsung dirender jadi chart. Semua perhitungan berat sudah dilakukan saat pipeline jalan — dashboard hanya menampilkan hasil jadi."

---

# SECTION 12 — FAILURE SCENARIOS

## 12.1 Katalog Kegagalan

### 🔴 Port Bentrok
```
Gejala: "Address already in use: 8095"
Penyebab: Airflow webserver sebelumnya belum dimatikan
Solusi: 
  lsof -i :8095          # Cari PID
  kill -9 <PID>          # Bunuh proses
  airflow webserver --port 8095  # Restart
```

### 🔴 DAG Gagal — Extract MySQL
```
Gejala: Task extract_from_mysql FAILED
Penyebab umum:
  - MySQL (Laragon) belum distart
  - Password salah di .env
  - Database source_os_dbd belum dibuat
  - Belum jalankan ingest_sources.py
Solusi:
  1. Start Laragon → Start All
  2. Cek .env: MYSQL_PASS=rootpassword123
  3. Jalankan: python scripts/ingest_sources.py
```

### 🔴 DAG Gagal — Validation
```
Gejala: Task validate_staging FAILED
Pesan: "VALIDATION FAILED: Orphan kode_bps ['9999']"
Penyebab: Ada kode_bps di data kesehatan yang tidak ada di GeoJSON
Solusi:
  1. Cek source data: apakah ada kode_bps baru?
  2. Update GeoJSON atau region_mapping.csv
  3. Re-run pipeline
```

### 🔴 Duplicate Data
```
Gejala: fact_dbd_env punya lebih dari 162 rows
Penyebab: Staging dedup gagal, atau pipeline dirun 2x tanpa TRUNCATE
Solusi: Pipeline V3 selalu TRUNCATE sebelum INSERT → seharusnya safe
  Tapi kalau terjadi: manual TRUNCATE → re-trigger DAG
```

### 🔴 Join Mismatch (Nama Wilayah)
```
Gejala: stg_environment punya 0 rows (semua data hilang saat join)
Penyebab: region_mapping.csv tidak cocok dengan data GEE baru
Solusi: Cek region_mapping.csv, update kolom nama_env
```

### 🔴 Missing kode_bps di GeoJSON
```
Gejala: dim_wilayah punya NULL latitude/longitude
Penyebab: GeoJSON tidak punya polygon untuk kabupaten tertentu
Solusi: Update GeoJSON file atau manual insert koordinat
```

### 🔴 Airflow Scheduler Mati
```
Gejala: DAG tidak trigger meskipun sudah un-pause
Penyebab: Scheduler process mati
Solusi: 
  ps aux | grep "airflow scheduler"   # Cek
  airflow scheduler &                  # Restart di background
```

### 🔴 PostgreSQL Mati
```
Gejala: Semua task yang akses PostgreSQL gagal
Solusi (Windows): 
  Services → postgresql → Start
  atau: net start postgresql-x64-15
```

### 🔴 WSL IP Berubah
```
Gejala: Setelah restart Windows, koneksi ke database gagal
Penyebab: WSL2 dapat IP baru setiap restart
Solusi: Pipeline V3 pakai localhost → seharusnya stable
  Tapi kalau masalah: hostname -I (di WSL) untuk cek IP
```

## 12.2 Prinsip Debugging

```
1. Baca Airflow log (klik task → Log)
2. Identifikasi task yang PERTAMA gagal
3. Cek koneksi database dulu (paling sering)
4. Cek data source (kedua paling sering)
5. Cek logic transform (paling jarang)
```

## 12.3 Cara Menjelaskan ke Orang Lain

> "Kalau pipeline gagal, lihat Airflow log untuk tahu task mana yang error. 80% masalahnya adalah: database belum dinyalakan, atau data source belum di-ingest. Sisanya biasanya masalah koneksi atau data yang formatnya berubah."

---

# SECTION 13 — TRADE-OFF ARSITEKTUR

## 13.1 Kelebihan Sistem Ini

| Kelebihan | Penjelasan |
|-----------|------------|
| **Multi-source integration** | Membuktikan bisa gabungkan MySQL + PostgreSQL + GeoJSON dalam 1 pipeline |
| **Proper staging layer** | Perbaikan besar dari V1/V2 — data dibersihkan sebelum diproses |
| **Automated validation** | Quality gate otomatis mencegah data busuk masuk warehouse |
| **Rich feature engineering** | 15+ derived metrics (CVI, lag, z-score, persistence) |
| **Star Schema** | Struktur analytics-ready, bukan flat table |
| **Spatial integration** | Centroid dari GeoJSON → Pin Map di Metabase |
| **Full refresh idempotent** | Setiap run = clean slate. Tidak ada state corruption |
| **Airflow orchestration** | Dependency management, retry, logging, web UI |
| **Isolasi total** | V3 tidak menyentuh V1/V2 (port, schema, database terpisah) |
| **Tobler imputation** | Spatial-aware imputation, bukan blind average |
| **🆕 Pipeline observability** | Tabel `pipeline_runs` mencatat metrik setiap eksekusi — audit trail otomatis |
| **🆕 Dynamic configuration** | Year range, thresholds, dan pandemi flag dari `.env` — zero-code-change scaling |

## 13.2 Kelemahan Sistem Ini (Jujur)

| Kelemahan | Penjelasan | Kapan Jadi Masalah? |
|-----------|------------|---------------------|
| **Pandas in-memory** | Seluruh data harus muat di RAM | Kalau data > 1 juta rows |
| **SQLite Airflow backend** | Tidak support paralelisme sesungguhnya | Kalau multi-user atau task perlu concurrent |
| **Full refresh** | Setiap run TRUNCATE + INSERT semua | Kalau data sangat besar, incremental lebih efisien |
| **Single-node** | Semua jalan di 1 mesin (WSL) | Kalau butuh distributed processing |
| **No real-time** | Batch processing (manual trigger / monthly) | Kalau butuh data real-time |
| **Hardcoded imputation** | Tobler mapping (3217→3204) di-hardcode | Kalau ada kabupaten baru yang perlu imputation |
| **🆕 status selalu SUCCESS** | `pipeline_runs` hanya mencatat run yang berhasil. Kegagalan tidak terekam di tabel ini (hanya di Airflow log) | Kalau butuh failure audit trail lengkap |
| **Annual granularity** | Data tahunan, bukan bulanan | Kalau butuh analisis musiman (bulanan) |

## 13.3 Kenapa Tidak Pakai Spark?

**Apache Spark** = framework untuk distributed big data processing.

```
Pipeline V3 data size:  162 rows × 25 columns = ~40 KB
Spark sweet spot:       > 10 GB, distributed cluster

Spark untuk data kita = pakai truk kontainer untuk kirim 1 kardus.
```

**Kapan harus migrate ke Spark?**
- Data > 10 juta rows
- Butuh processing di cluster (multi-node)
- Batch window sangat ketat (harus selesai dalam menit)

**Untuk sekarang:** Pandas lebih dari cukup. Setup-nya sederhana, debugging mudah, tidak butuh cluster.

## 13.4 Kenapa Tidak Pakai Kafka?

**Apache Kafka** = platform streaming untuk real-time data.

```
Pipeline V3:  Batch, @monthly, data historis
Kafka:        Real-time streaming, event-by-event

Kafka untuk data kita = pasang conveyor belt di toko kue yang buat 5 kue per hari.
```

**Kapan butuh Kafka?**
- Data datang real-time (sensor, API stream)
- Butuh latency < 1 detik
- Multiple consumer membaca data bersamaan

**Untuk sekarang:** Data kita update tahunan. Batch processing = perfectly fine.

## 13.5 Kenapa Pandas Masih Cukup?

```
Data volume:   162 rows (SANGAT KECIL)
Compute:       Feature engineering sederhana (zscore, lag, merge)
Complexity:    Moderate (15 features, tapi logic straightforward)
Memory:        < 50 MB peak
Time:          < 10 detik end-to-end

Pandas limit:  ~10 juta rows di laptop biasa
Kita pakai:    162 rows

Headroom:      ~60.000x sebelum Pandas jadi bottleneck
```

## 13.6 Kenapa SQLite Airflow Masih Acceptable?

SQLite sebagai Airflow metadata database punya limitasi:
- Tidak support parallel task execution (hanya SequentialExecutor)
- Tidak cocok untuk multi-user
- Tidak cocok untuk production 24/7

**Tapi untuk project ini:**
- Single user (kamu sendiri)
- Academic project (bukan production)
- 7 tasks total (sequential fine)
- Run frequency: occasional (bukan setiap jam)

**Kapan harus upgrade?** Kalau deploy ke server shared, pakai PostgreSQL backend + CeleryExecutor.

## 13.7 Kesimpulan Trade-off

```
┌─────────────────────────────────────────────────────────┐
│ DESAIN INI MASUK AKAL UNTUK:                            │
│  ✅ Academic project                                    │
│  ✅ Data < 100.000 rows                                │
│  ✅ Single user                                        │
│  ✅ Batch processing (tidak real-time)                 │
│  ✅ Proof of concept multi-source integration          │
│                                                         │
│ DESAIN INI TIDAK CUKUP UNTUK:                           │
│  ❌ Production enterprise (butuh Spark/distributed)    │
│  ❌ Real-time dashboard (butuh Kafka/streaming)        │
│  ❌ Multi-user collaboration (butuh PG backend)        │
│  ❌ Data > 10 juta rows (butuh distributed compute)    │
└─────────────────────────────────────────────────────────┘
```

## 13.8 Cara Menjelaskan ke Orang Lain

> "Arsitektur ini dirancang untuk skala academic project dengan data ratusan baris. Kita pakai Pandas (bukan Spark) karena datanya kecil, batch processing (bukan Kafka) karena data update tahunan, dan SQLite Airflow (bukan PostgreSQL backend) karena single-user. Semua keputusan ini masuk akal untuk skala kita, tapi harus di-upgrade kalau pindah ke production."

---

# SECTION 14 — HOW TO HANDOVER THIS PROJECT

## 14.1 Strategi Handover ke Entin

### Urutan Pemahaman (dari PALING PENTING)

```
LEVEL 1 — HARUS PAHAM (1 jam)
  ├── Apa tujuan pipeline → Section 1
  ├── Alur data end-to-end → Section 2
  └── Cara trigger pipeline → SETUP_GUIDE.md

LEVEL 2 — SEBAIKNYA PAHAM (2 jam)
  ├── Star Schema (dim + fact) → Section 7
  ├── OLAP queries → Section 10
  └── Cara baca Airflow log → Section 12

LEVEL 3 — BAGUS KALAU PAHAM (3 jam)
  ├── Staging layer detail → Section 4
  ├── Feature engineering → Section 6
  └── Trade-off arsitektur → Section 13
```

### Script Handover

> "Entin, ini pipeline data DBD Jawa Barat. Sistemnya mengambil data dari 3 sumber berbeda (MySQL, PostgreSQL, GeoJSON), membersihkannya, menghitung 15 metrik kesehatan, lalu menaruh hasilnya di Data Warehouse yang bisa dibaca Metabase.
>
> Yang perlu kamu tahu:
> 1. **Cara menjalankan:** Baca `SETUP_GUIDE.md` — start database, ingest data, trigger DAG
> 2. **Arsitektur:** Baca Section 1-2 dokumen ini — big picture + alur data
> 3. **Kalau error:** Baca Section 12 — failure scenarios
> 4. **Mau modifikasi query OLAP:** Baca Section 10 + file `sql/olap_queries_v3.sql`"

## 14.2 Bagian Yang Boleh Dimodifikasi

```
✅ AMAN dimodifikasi:
  - sql/olap_queries_v3.sql      → Tambah query OLAP baru
  - .env                          → Ubah password/port database
  - SETUP_GUIDE.md               → Update instruksi
  - Metabase dashboard            → Tambah chart/filter baru

⚠️ HATI-HATI (paham dulu sebelum edit):
  - dag_dbd_v3.py (transform)    → Feature engineering logic
  - config/db_config.py          → Koneksi database
  - sql/ddl_v3.sql               → Schema structure

🚫 JANGAN SENTUH (kecuali benar-benar paham):
  - Staging rules (apa yang boleh/tidak di staging)
  - Validation checks (bisa membuat data busuk lolos)
  - Star Schema grain (kode_bps × tahun)
  - region_mapping.csv (Rosetta Stone pipeline)
```

## 14.3 Checklist Handover

```
□ Entin bisa start MySQL + PostgreSQL
□ Entin bisa jalankan ingest_sources.py
□ Entin bisa start Airflow (scheduler + webserver)
□ Entin bisa trigger DAG dan lihat semua task hijau
□ Entin bisa query fact_dbd_env di pgAdmin/psql
□ Entin bisa buka Metabase dan lihat dashboard
□ Entin tahu cara baca Airflow log kalau ada error
□ Entin paham arti 1 row di fact table
```

## 14.4 Cara Menjelaskan ke Orang Lain

> "Untuk handover, yang paling penting adalah Entin bisa menjalankan pipeline end-to-end dan tahu cara troubleshoot kalau error. Detail feature engineering bisa dipelajari nanti. Prioritaskan: run pipeline → lihat dashboard → baca log."

---

# SECTION 15 — MENTAL MODEL FINAL

## 15.1 Versi 5 Menit (Presentasi)

> "Pipeline V3 DBD Jawa Barat mengambil data dari 3 sumber: data kasus DBD dari BPS (MySQL), data lingkungan dari satelit Google Earth Engine (PostgreSQL), dan data peta wilayah (GeoJSON). 
> 
> Data dibersihkan di **staging layer**, divalidasi otomatis (dengan threshold dinamis), lalu diproses menjadi 15+ metrik epidemiologi termasuk Incidence Rate, Composite Vulnerability Index, dan Endemic Persistence Score. 
> 
> Hasilnya disimpan dalam **Star Schema** dengan 1 fact table (162 baris: 27 kabupaten × 6 tahun), 2 dimension tables, dan 1 tabel observability (`pipeline_runs`) yang mencatat metrik kesehatan pipeline setiap kali dijalankan. Dashboard Metabase membaca warehouse ini untuk menampilkan chart dan peta interaktif.
> 
> Semua diorkestrasikan oleh **Apache Airflow** — 1 klik trigger, 7 task otomatis, dengan validation gate di tengah. Konfigurasi year range dan threshold sepenuhnya dinamis melalui `.env`."

## 15.2 Versi 1 Menit (Elevator Pitch)

> "Kita punya pipeline otomatis yang mengubah data mentah DBD dari 3 sumber berbeda menjadi dashboard analitik. Pipeline-nya membersihkan data, menghitung metrik kesehatan, dan menyimpannya dalam format yang siap dibuat visualisasi. Cukup 1 klik untuk menjalankan semuanya."

## 15.3 Versi Teknis (Untuk Dosen/Reviewer)

> "Arsitektur V3 mengimplementasikan multi-source ETL pipeline dengan triple-extract paralel (MySQL, PostgreSQL, GeoJSON) menggunakan Apache Airflow TaskFlow API. Data melewati 4-stage processing: staging (cleaning/normalization ke PostgreSQL schema dengan dynamic year filtering via `.env`), automated validation (duplicate, null, referential integrity check **dengan dynamic threshold**), transform (Tobler-based spatial imputation + **NaN Guard** + feature engineering: incidence rate, z-score, lag features, CVI), dan load ke Star Schema DWH (2 dimension + 1 fact table, grain: kabupaten/kota × tahun). **Pipeline observability** dijamin melalui tabel `pipeline_runs` yang mencatat metrik eksekusi (row count, imputed rows, null IR) pada setiap run. OLAP layer disajikan melalui Metabase dengan **dynamic query** menggunakan `MAX(tahun)` dan Pin Map visualization menggunakan centroid coordinates."

## 15.4 Versi Analogi Sederhana (Untuk Non-Teknis)

> "Bayangkan kamu punya 3 tumpukan data dari kantor berbeda tentang penyakit DBD. Pipeline ini seperti mesin otomatis yang:
> 1. Mengambil semua data itu
> 2. Membersihkan dan menyeragamkan formatnya
> 3. Menghitung angka-angka penting (seberapa parah, seberapa rentan)
> 4. Menyusunnya rapi di 'rak' database
> 5. Menampilkannya dalam grafik dan peta interaktif
> 
> Kamu tinggal pencet 1 tombol, semua jalan otomatis."

## 15.5 Peta Mental (Satu Gambar)

```
╔═══════════════════════════════════════════════════════════════╗
║                   PIPELINE V3 — MENTAL MAP                    ║
╠═══════════════════════════════════════════════════════════════╣
║                                                               ║
║   "3 sumber data → 1 dashboard analitik"                     ║
║                                                               ║
║   ┌─────┐ ┌─────┐ ┌─────┐                                   ║
║   │MySQL│ │ PG  │ │ Geo │  ← SUMBER (raw, berantakan)       ║
║   └──┬──┘ └──┬──┘ └──┬──┘                                   ║
║      └───────┼───────┘                                       ║
║              ▼                                                ║
║   ┌──────────────────┐                                       ║
║   │    STAGING       │  ← BERSIHKAN (clean, dedup, format)   ║
║   └────────┬─────────┘                                       ║
║            ▼                                                  ║
║   ┌──────────────────┐                                       ║
║   │   VALIDATION     │  ← CEK KUALITAS (reject kalau buruk) ║
║   └────────┬─────────┘                                       ║
║            ▼                                                  ║
║   ┌──────────────────┐                                       ║
║   │   TRANSFORM      │  ← HITUNG (IR, CVI, z-score, lag)    ║
║   └────────┬─────────┘                                       ║
║            ▼                                                  ║
║   ┌──────────────────┐                                       ║
║   │  STAR SCHEMA     │  ← SIMPAN RAPI (fact + dimensions)   ║
║   │  162 rows fact   │                                       ║
║   │  + pipeline_runs │  ← 🆕 CATAT METRIK (observability)      ║
║   └────────┬─────────┘                                       ║
║            ▼                                                  ║
║   ┌──────────────────┐                                       ║
║   │  METABASE        │  ← TAMPILKAN (chart + peta)          ║
║   └──────────────────┘                                       ║
║                                                               ║
║   Semua diatur oleh AIRFLOW (1 klik, 7 tasks, auto-retry)   ║
║   Metrik setiap run dicatat di pipeline_runs (🆕)           ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
```

---

# SECTION 16 — PIPELINE OBSERVABILITY & RUN TRACKING

> **🆕 Section ini ditambahkan pada revisi Mei 2026**

## 16.1 Apa Itu Pipeline Observability?

**Analogi:** Bayangkan mobil tanpa dashboard (speedometer, fuel gauge, engine warning light). Mobil tetap bisa jalan, tapi kamu **tidak tahu** apakah bensin hampir habis, mesin terlalu panas, atau ada masalah. Kamu baru tahu saat mobil mogok — sudah terlambat.

`pipeline_runs` adalah **dashboard instrumen** pipeline Anda.

**Definisi:** Data Observability = kemampuan untuk memahami **kesehatan data** dan **perilaku pipeline** secara kontinu, tanpa harus membuka log atau memeriksa database secara manual.

## 16.2 Kenapa Airflow Log Saja Tidak Cukup?

| Aspek | Airflow Log | Tabel `pipeline_runs` |
|-------|-------------|----------------------|
| Format | Teks tidak terstruktur (ribuan baris) | Data terstruktur (1 row per run) |
| Akses | Harus buka Airflow UI / file log | Query SQL biasa, bisa dari pgAdmin/Metabase |
| Retensi | Tergantung konfigurasi log rotation | Permanen di database warehouse |
| Trending | Tidak bisa dibandingkan antar-run | `SELECT * ORDER BY run_at` → langsung terlihat tren |
| Integrasi dashboard | Tidak bisa | Bisa ditampilkan di Metabase bersama data analisis |

## 16.3 Apa Yang Dicatat?

Setiap kali `load_dwh()` selesai sukses, 1 row ditulis ke `pipeline_runs`:

```python
run_meta = pd.DataFrame([{
    'run_at':       pd.Timestamp.now(),
    'year_start':   YEAR_START,
    'year_end':     YEAR_END,
    'n_regions':    len(dim_wil),       # berapa kabupaten
    'n_years':      len(dim_wkt),       # berapa tahun
    'fact_rows':    len(fact),           # total baris fact
    'imputed_rows': imputed_count,      # berapa baris hasil imputasi
    'null_ir_rows': null_ir_count,      # berapa baris tanpa incidence_rate
    'status':       'SUCCESS',
}])
run_meta.to_sql('pipeline_runs', eng, schema=WH, if_exists='append', index=False)
```

## 16.4 Data Quality Scorecard

Dengan 1 query sederhana, Anda bisa mendapatkan "rapor kesehatan pipeline":

```sql
SELECT
    run_at::DATE                                        AS run_date,
    year_start || '–' || year_end                       AS year_range,
    fact_rows,
    n_regions * n_years                                 AS expected_rows,
    fact_rows - (n_regions * n_years)                   AS row_gap,
    ROUND(imputed_rows * 100.0 / NULLIF(fact_rows,0),1) AS pct_imputed,
    ROUND(null_ir_rows * 100.0 / NULLIF(fact_rows,0),1) AS pct_null_ir,
    status
FROM warehouse_dbd_v3.pipeline_runs
ORDER BY run_at DESC LIMIT 10;
```

**Interpretasi:**
- `row_gap = 0` → data lengkap ✅
- `pct_imputed < 10%` → kualitas tinggi ✅
- `pct_null_ir = 0` → semua incidence rate terhitung ✅
- `pct_null_ir > 0` → **investigasi diperlukan** ⚠️

## 16.5 Skenario Realistis: Deteksi Data Hilang

**Kronologi kegagalan:**
1. BPS mengubah kode kabupaten dari `3201` menjadi `32.01` di file CSV terbaru
2. `load_staging` tetap berjalan, tapi kode `32.01` tidak cocok saat merge
3. 27 kabupaten untuk tahun 2025 kehilangan data environment
4. Pipeline tetap `SUCCESS` (tidak ada exception) — tapi `fact_rows = 162` (bukan 189)

**Deteksi via `pipeline_runs`:**

```sql
SELECT run_at, fact_rows, n_regions * n_years AS expected, status
FROM warehouse_dbd_v3.pipeline_runs
ORDER BY run_at DESC LIMIT 3;
```

```
       run_at        | fact_rows | expected | status
---------------------+-----------+----------+---------
 2026-07-01 08:00:00 |       162 |      189 | SUCCESS   ← row_gap = -27!
 2026-06-01 08:00:00 |       162 |      162 | SUCCESS
 2026-05-01 20:04:00 |       162 |      162 | SUCCESS
```

`row_gap = -27` langsung terdeteksi. Kehilangan tepat 27 rows = 1 tahun × 27 kabupaten. Investigasi jelas: masalah ada di data tahun 2025.

**Tanpa `pipeline_runs`?** Anda baru tahu saat analis melapor: "Kok data 2025 tidak ada di dashboard?" — bisa berminggu-minggu kemudian.

## 16.6 Keterbatasan (Evaluasi Kritis)

| Aspek | Status | Penjelasan |
|-------|--------|------------|
| Mencatat sukses | ✅ | Setiap run sukses tercatat |
| Mencatat gagal | ❌ | Jika pipeline error sebelum `load_dwh()`, tabel **tidak** terisi |
| Durasi pipeline | ❌ | Tidak ada `start_time` — hanya `run_at` (end time) |
| Link ke Airflow | ❌ | Tidak ada `dag_run_id` untuk trace ke log Airflow |
| Task-level detail | ❌ | Hanya pipeline-level, bukan per-task |

**Saran perbaikan (future work):**
1. Tambahkan try/except di `load_dwh()` untuk mencatat kegagalan dengan `status = 'FAILED'`
2. Tambahkan kolom `duration_seconds` dan `airflow_run_id`
3. Untuk kebutuhan yang lebih besar: pertimbangkan tools seperti Great Expectations atau Monte Carlo

## 16.7 Cara Menjelaskan ke Orang Lain

> "Setiap kali pipeline selesai, dia otomatis mencatat: berapa baris yang dimuat, berapa data yang diimputasi, dan apakah ada incidence rate yang kosong. Dengan 1 query SQL, kita bisa lihat tren kesehatan pipeline dari waktu ke waktu — tanpa perlu buka log Airflow."

---

# SECTION 17 — CHANGELOG: V2 → V3 FINAL

> **🆕 Section ini ditambahkan pada revisi Mei 2026**

## 17.1 Daftar Perubahan

| # | Perubahan | Sebelum | Sesudah | Kenapa? |
|---|-----------|---------|---------|--------|
| 1 | **Dynamic Year** | `tahun.between(2019, 2024)` hardcoded | `tahun.between(YEAR_START, YEAR_END)` dari `.env` | Menambah tahun 2025 butuh edit Python code → sekarang cukup edit `.env` |
| 2 | **Dynamic Threshold** | `if count < 100` (statis) | `if count < n_regions × n_years × 0.8` | Threshold statis tidak menyesuaikan saat data bertambah → bisa loloskan data partial |
| 3 | **NaN Guard** | Tidak ada post-transform check | Validasi `incidence_rate` NaN setelah imputasi | NaN bisa masuk warehouse tanpa peringatan → silent data corruption |
| 4 | **pipeline_runs** | Tidak ada audit trail per-run | Tabel metadata di warehouse (append-only) | Tidak bisa tahu kapan, berapa, dan bagaimana data terakhir dimuat |
| 5 | **GEE Warning** | Mismatch mapping diam-diam | `logging.warning()` saat ada nama wilayah tidak ter-mapping | Data GEE bisa hilang tanpa ada yang tahu |
| 6 | **Dynamic OLAP** | `WHERE tahun = 2024` hardcoded | `WHERE tahun = (SELECT MAX(tahun)...)` | Setiap tahun baru, semua query harus diedit manual |

## 17.2 Dampak Keseluruhan

```
 SEBELUM (V3 awal)              SESUDAH (V3 Final)
 ┌─────────────────────┐      ┌──────────────────────┐
 │ Hardcoded years    │      │ ✅ Dynamic years       │
 │ Static threshold   │      │ ✅ Dynamic threshold   │
 │ No NaN check       │  →   │ ✅ NaN Guard           │
 │ No audit trail     │      │ ✅ pipeline_runs       │
 │ Silent GEE loss    │      │ ✅ GEE warnings        │
 │ Hardcoded queries  │      │ ✅ Dynamic OLAP        │
 └─────────────────────┘      └──────────────────────┘

 Tingkat kematangan: PROTOTYPE → PRODUCTION-AWARE
```

## 17.3 Apa Yang Masih Kurang untuk Production-Grade?

1. **Failure recording** — `pipeline_runs` belum mencatat status `FAILED`
2. **Alerting** — Tidak ada notifikasi otomatis (Slack/email) saat anomali terdeteksi
3. **Data versioning** — Tidak ada snapshot "data kemarin vs hari ini"
4. **Task-level metrics** — Observability hanya pipeline-level, bukan per-task
5. **Schema monitoring** — Tidak ada deteksi perubahan schema di sumber data

**Kesimpulan:** V3 Final adalah sistem yang **production-aware** — sadar akan kebutuhan production, menerapkan sebagian prinsipnya, tapi masih berada di skala academic project. Untuk deployment nyata, 5 poin di atas perlu ditambahkan.

---

# LAMPIRAN — FILE REFERENCE

| File | Lokasi | Fungsi |
|------|--------|--------|
| `dag_dbd_v3.py` | `docs_v3/dags/` | DAG utama — seluruh pipeline logic |
| `ingest_sources.py` | `docs_v3/scripts/` | One-time: CSV → source databases |
| `db_config.py` | `docs_v3/config/` | Koneksi database + path management |
| `ddl_v3.sql` | `docs_v3/sql/` | DDL schema (staging + warehouse) |
| `olap_queries_v3.sql` | `docs_v3/sql/` | Query OLAP untuk Metabase |
| `.env` | `docs_v3/` | Environment variables |
| `requirements.txt` | `docs_v3/` | Python dependencies |
| `SETUP_GUIDE.md` | `docs_v3/` | Panduan setup step-by-step |
| `region_mapping.csv` | `data/` | Pemetaan nama wilayah BPS↔GEE |
| `dbd_jabar.csv` | `data/` | Raw data kasus DBD |
| `kepadatan_penduduk_jabar.csv` | `data/` | Raw data kepadatan penduduk |
| `Data_Tahunan_DBD_*.csv` | `data/` | Raw data lingkungan (GEE) |
| `jabar_kabkot` | `data/` | GeoJSON batas wilayah |

---

> **Dokumen ini dibuat sebagai handbook internal engineer untuk Pipeline V3 DBD Jawa Barat.**
> **Terakhir diperbarui: 1 Mei 2026 (Revisi: 6 Patch)**
> **Versi: 3.1 FINAL**
