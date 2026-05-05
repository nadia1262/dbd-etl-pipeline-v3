# LAPORAN UJIAN TENGAH SEMESTER

## End-to-End Data Warehouse untuk Deteksi Dini Lonjakan Kasus DBD Berbasis Faktor Lingkungan di Jawa Barat

Disusun sebagai Project Ujian Tengah Semester Mata Kuliah Teknologi Perekayasaan Data

---

**Disusun oleh: Tim 9 3SI1**

| Nama | NIM |
|---|---|
| Cristiano Teddy Anta | 222313031 |
| Nadia Nur Nisrina | 222313276 |
| Rani Kusumawati | 222313336 |
| Valentina Lasma Situmorang | 222313413 |

---

**D-IV Komputasi Statistik**
**Politeknik Statistika STIS**
**2026**

---

## Latar Belakang

Demam Berdarah Dengue (DBD) merupakan penyakit tropis yang ditularkan melalui nyamuk *Aedes aegypti* dan telah menjadi ancaman kesehatan masyarakat di kawasan Asia Tenggara, termasuk Indonesia. Berdasarkan kajian Bhatt et al. (2013), sekitar 390 juta infeksi dengue terjadi setiap tahun secara global, dengan proporsi signifikan terkonsentrasi di wilayah beriklim tropis. Indonesia, sebagai negara kepulauan dengan curah hujan tinggi dan suhu hangat sepanjang tahun, menghadapi tantangan endemisitas DBD yang persisten di berbagai provinsi.

Jawa Barat, sebagai provinsi dengan jumlah penduduk terbesar di Indonesia, secara konsisten mencatat angka kasus DBD tertinggi di tingkat nasional. Selama periode 2019 hingga 2024, fluktuasi kasus menunjukkan pola yang dipengaruhi oleh dinamika iklim, kepadatan penduduk, serta perubahan tutupan lahan. Tahun 2020–2021 yang bertepatan dengan pandemi COVID-19 memperlihatkan anomali dalam pelaporan kasus, sementara tahun 2024 mencatat lonjakan signifikan yang mengindikasikan perlunya sistem deteksi dini berbasis data.

Dalam konteks tersebut, pendekatan berbasis data warehouse menjadi relevan untuk mengintegrasikan data kesehatan, data lingkungan, dan data spasial ke dalam satu repositori analitik terpadu. Integrasi ini memungkinkan analisis multidimensi yang tidak dapat dilakukan apabila data tersimpan secara terfragmentasi di berbagai sumber. Dengan memanfaatkan teknologi Extract-Transform-Load (ETL) yang diorkestrasi secara otomatis, proses pengumpulan dan transformasi data dapat dijalankan secara konsisten dan reproducible tanpa intervensi manual yang rentan terhadap human error.

Project ini membangun sebuah pipeline ETL end-to-end yang mengintegrasikan tiga sumber data heterogen — data kasus DBD dari Badan Pusat Statistik (BPS), data lingkungan dari Google Earth Engine (GEE), serta data spasial berformat GeoJSON — ke dalam sebuah data warehouse berarsitektur star schema. Seluruh proses diorkestrasi menggunakan Apache Airflow dan divisualisasikan melalui dashboard Metabase, sehingga stakeholder dapat mengidentifikasi wilayah berisiko tinggi serta memahami korelasi antara faktor lingkungan dan dinamika kasus DBD di 27 kabupaten/kota se-Jawa Barat.

---

## Tujuan Project

Project ini bertujuan membangun sistem data warehouse yang mampu mendukung deteksi dini lonjakan kasus DBD di Jawa Barat melalui pendekatan rekayasa data end-to-end. Secara spesifik, tujuan tersebut mencakup empat aspek utama.

Pertama, project ini mengembangkan pipeline ETL otomatis yang mampu mengekstrak data dari tiga sumber heterogen — MySQL, PostgreSQL, dan file GeoJSON — secara paralel dan terjadwal. Otomatisasi ini menghilangkan kebutuhan intervensi manual pada setiap siklus pembaruan data, sehingga menjamin konsistensi dan reproducibility proses.

Kedua, project ini mengimplementasikan integrasi data multisumber dengan mekanisme staging layer yang melakukan standardisasi format, normalisasi kode wilayah, serta validasi kualitas data sebelum data memasuki tahap transformasi. Pendekatan ini memastikan bahwa data dari sumber yang berbeda dapat digabungkan dengan integritas referensial yang terjaga.

Ketiga, project ini merancang dan membangun data warehouse berarsitektur star schema yang mengakomodasi analisis multidimensi — baik berdasarkan dimensi wilayah maupun dimensi waktu. Fact table diperkaya dengan fitur-fitur turunan hasil feature engineering, seperti incidence rate, rainfall z-score, composite vulnerability index, dan endemic persistence score, yang memberikan nilai analitik lebih tinggi dibandingkan data mentah.

Keempat, project ini menyajikan hasil analisis melalui dashboard interaktif Metabase yang memungkinkan stakeholder melakukan eksplorasi data secara mandiri, mengidentifikasi wilayah dengan kerentanan tinggi, serta memahami pola hubungan antara faktor lingkungan dan dinamika kasus DBD tanpa memerlukan keahlian teknis dalam penulisan query SQL.

---

## Arsitektur Pipeline

Pipeline ini dibangun dengan arsitektur multi-layer yang memisahkan secara tegas antara data sumber, area staging, zona transformasi, dan data warehouse. Pemisahan ini mengikuti prinsip separation of concerns yang memudahkan debugging, auditing, dan pemeliharaan sistem secara independen pada setiap layer.

Pada layer pertama, terdapat tiga sumber data yang bersifat heterogen. Data kesehatan DBD beserta data demografi (kepadatan penduduk dan luas wilayah) tersimpan di database MySQL bernama `source_os_dbd`. Data lingkungan hasil observasi Google Earth Engine tersimpan di tabel `source_gee_env` pada database PostgreSQL. Sementara data spasial berupa koordinat dan batas administrasi 27 kabupaten/kota tersimpan dalam format GeoJSON.

Seluruh orkestrasi pipeline dikelola oleh Apache Airflow melalui sebuah Directed Acyclic Graph (DAG) bernama `dbd_v3_final_pipeline` yang dijadwalkan berjalan secara bulanan (`@monthly`). DAG ini mengoordinasikan enam tahapan utama: tiga task extract yang berjalan secara paralel, satu task staging load, satu task validasi, satu task transformasi, satu task load ke warehouse, dan satu task cleanup. Dependency antar-task didefinisikan secara eksplisit sehingga Airflow menjamin urutan eksekusi yang benar.

Konfigurasi pipeline bersifat dinamis melalui file `.env` yang memuat parameter seperti `YEAR_START`, `YEAR_END`, koneksi database, dan tahun pandemi. Pendekatan ini memungkinkan perubahan rentang tahun data tanpa modifikasi kode Python.

```text
   [MySQL/BPS]     [PostgreSQL/GEE]     [GeoJSON/Spatial]
       ↓                  ↓                    ↓
       └──────── Extract (3× Parallel) ────────┘
                          ↓
                  [Staging Layer]
                  4 tabel staging
                          ↓
                    [Validation]
                  Quality checks
                          ↓
                    [Transform]
               Feature engineering
                          ↓
                 [Data Warehouse]
                   Star Schema
                          ↓
                    [Metabase]
                Dashboard & OLAP
```

Database PostgreSQL `dwh_dbd_v3` menampung dua schema yang terisolasi: `staging_dbd_v3` untuk empat tabel staging, dan `warehouse_dbd_v3` untuk tabel dimensi, tabel fakta, serta tabel observability. Penggunaan schema tunggal dalam satu database memanfaatkan kemampuan native PostgreSQL untuk cross-schema query, yang tidak memerlukan extension tambahan seperti `dblink` atau Foreign Data Wrapper.

---

## Tahapan ETL

### Extract

Tahap ekstraksi mengambil data dari tiga sumber secara paralel menggunakan tiga Airflow task yang independen satu sama lain. Paralelisasi ini mempercepat proses pengambilan data karena ketiga sumber tidak memiliki ketergantungan sekuensial.

Task pertama, `extract_from_mysql`, mengambil tiga tabel dari database MySQL `source_os_dbd`: tabel `source_bps_health` yang memuat data kasus DBD per kabupaten/kota per tahun, tabel `source_bps_kepadatan` yang memuat kepadatan penduduk, dan tabel `source_bps_luas` yang memuat luas wilayah dalam kilometer persegi. Data dibaca menggunakan pandas `read_sql` dan disimpan sementara sebagai file CSV di direktori `temp/`.

Task kedua, `extract_from_postgres`, mengambil tabel `source_gee_env` dari schema public di PostgreSQL `dwh_dbd_v3`. Tabel ini memuat empat variabel lingkungan hasil observasi satelit: curah hujan tahunan (*rainfall_annual*), suhu permukaan tanah (*lst_celsius*), indeks vegetasi (*ndvi*), dan indeks kelembaban (*ndmi*) untuk 25 wilayah yang ter-cover oleh GEE.

Task ketiga, `extract_geojson`, memproses file GeoJSON yang memuat 27 fitur MultiPolygon kabupaten/kota di Jawa Barat. Menggunakan library `geopandas`, setiap polygon dihitung centroid-nya untuk mengekstrak koordinat latitude dan longitude yang selanjutnya digunakan sebagai atribut spasial pada dimensi wilayah.

### Transform

Tahap transformasi merupakan inti dari pipeline yang terdiri dari tiga sub-proses: staging load, validasi, dan feature engineering.

**Staging Load.** Empat tabel staging diisi dengan data yang telah melalui proses cleaning dan standardisasi. Pada tahap ini dilakukan normalisasi kode BPS ke format string, filtering data berdasarkan rentang tahun yang dikonfigurasi secara dinamis (`YEAR_START` hingga `YEAR_END`), penghapusan duplikat berdasarkan composite key `(kode_bps, tahun)`, serta pemetaan nama wilayah GEE ke kode BPS menggunakan file `region_mapping.csv`. Sistem GEE Mapping Warning secara otomatis mencatat peringatan apabila terdapat nama wilayah GEE yang tidak dapat dipetakan, sehingga mencegah hilangnya data secara tidak terdeteksi.

Keempat tabel staging yang dihasilkan adalah:

| Tabel Staging | Konten | Sumber |
|---|---|---|
| `stg_health_dbd` | Kasus DBD per wilayah per tahun | MySQL (BPS) |
| `stg_demografi` | Kepadatan penduduk dan luas wilayah | MySQL (BPS) |
| `stg_environment` | Curah hujan, suhu, NDVI, NDMI | PostgreSQL (GEE) |
| `stg_geojson` | Koordinat centroid 27 kab/kota | File GeoJSON |

**Validasi.** Sebelum memasuki feature engineering, data staging melewati empat pemeriksaan kualitas: (1) deteksi duplikat pada composite key `(kode_bps, tahun)`, (2) pemeriksaan null pada kolom `kode_bps`, (3) validasi jumlah baris minimum menggunakan threshold dinamis yang dihitung sebagai `n_regions × n_years × 80%`, dan (4) validasi integritas referensial antara `kode_bps` di tabel health terhadap tabel geojson. Apabila salah satu pemeriksaan gagal, pipeline akan berhenti dengan pesan error yang deskriptif.

**Feature Engineering.** Empat tabel staging digabungkan melalui operasi merge menggunakan key `(kode_bps, tahun)` untuk menghasilkan panel data terintegrasi. Selanjutnya, dilakukan perhitungan fitur-fitur turunan:

*Incidence Rate* dihitung sebagai rasio jumlah kasus terhadap populasi (kepadatan × luas) dikalikan 100.000, menghasilkan metrik yang comparable antar-wilayah dengan ukuran populasi berbeda.

*Year-over-Year (YoY) Growth Rate* menghitung persentase perubahan incidence rate terhadap tahun sebelumnya dalam satu wilayah. Fitur ini menangkap dinamika tren kasus yang tidak terlihat dari angka absolut.

*Rainfall Z-Score* menstandarisasi curah hujan setiap wilayah terhadap distribusi curah hujan pada tahun yang sama menggunakan z-score. Nilai positif mengindikasikan curah hujan di atas rata-rata yang berpotensi meningkatkan breeding site nyamuk.

*Lag Features (Lag-1, Lag-2, Lag-3)* menangkap efek temporal curah hujan terhadap kasus DBD dengan menyimpan nilai curah hujan satu, dua, dan tiga tahun sebelumnya. Untuk tahun pertama yang tidak memiliki data historis, digunakan teknik backfill dengan nilai curah hujan tahun berjalan.

*Composite Vulnerability Index (CVI)* merupakan indeks komposit yang mengkombinasikan empat komponen melalui weighted z-score: curah hujan (30%), suhu permukaan (25%), kepadatan penduduk (25%), dan inverse vegetasi (20%). Semakin tinggi CVI, semakin rentan suatu wilayah terhadap wabah DBD.

*Endemic Persistence Score* menghitung berapa kali suatu wilayah masuk ke kuartil tertinggi (Q4) incidence rate sepanjang seluruh periode observasi. Skor ini mengidentifikasi wilayah yang secara konsisten menjadi hotspot DBD.

*Tobler Imputation* menangani dua wilayah yang tidak ter-cover oleh data GEE: Kabupaten Bandung Barat (3217) diimputasi menggunakan data Kabupaten Bandung (3204), dan Kabupaten Pangandaran (3218) diimputasi menggunakan data Kabupaten Ciamis (3207). Pemilihan donor didasarkan pada prinsip Tobler's First Law of Geography bahwa wilayah yang berdekatan cenderung memiliki karakteristik lingkungan yang serupa. Kolom `is_imputed` secara eksplisit menandai baris hasil imputasi untuk transparansi analitik.

Setelah imputasi, NaN Guard melakukan pemeriksaan integritas post-imputasi pada kolom `incidence_rate` untuk mencegah silent data corruption akibat nilai NaN yang lolos ke warehouse.

### Load

Tahap load menerapkan strategi full-refresh: seluruh tabel warehouse di-truncate sebelum data baru dimasukkan. Strategi ini dipilih karena data sumber (BPS dan GEE) bersifat historis dan dapat mengalami revisi retroaktif. Full refresh menjamin bahwa warehouse selalu mencerminkan versi terbaru dari seluruh data sumber tanpa risiko inkonsistensi akibat update parsial.

Urutan load mengikuti constraint foreign key: `dim_wilayah` diisi terlebih dahulu (27 baris, satu per kabupaten/kota), kemudian `dim_waktu` (6 baris, satu per tahun 2019–2024), dan terakhir `fact_dbd_env` (162 baris = 27 wilayah × 6 tahun). Setelah load selesai, metadata eksekusi dicatat ke tabel `pipeline_runs` untuk keperluan observability.

---

## Desain Database dan Data Warehouse

Data warehouse dirancang menggunakan arsitektur star schema yang terdiri dari satu tabel fakta sentral dan dua tabel dimensi. Arsitektur ini dipilih karena kesederhanaannya dalam mendukung query analitik multidimensi serta kompatibilitasnya dengan tools Business Intelligence seperti Metabase.

**Dimensi Wilayah (`dim_wilayah`)** menyimpan atribut statis 27 kabupaten/kota di Jawa Barat dengan primary key `kode_bps`. Tabel ini memuat nama kabupaten/kota, kode dan nama provinsi, serta koordinat latitude dan longitude yang diekstrak dari centroid GeoJSON. Koordinat spasial ini memungkinkan visualisasi peta langsung dari query SQL tanpa memerlukan join ke sumber data eksternal.

**Dimensi Waktu (`dim_waktu`)** menyimpan atribut temporal dengan primary key `tahun`. Selain nilai tahun, tabel ini memuat kolom `covid_dummy` (bernilai 1 untuk tahun 2020–2021) dan `keterangan_periode` yang mengkategorikan tahun sebagai "Pandemi" atau "Normal". Fitur ini memungkinkan analisis dampak pandemi terhadap pelaporan kasus DBD secara langsung melalui filter dimensi.

**Tabel Fakta (`fact_dbd_env`)** merupakan tabel sentral yang menyimpan 162 baris observasi pada grain "satu kabupaten/kota pada satu tahun." Tabel ini memiliki surrogate key `fact_id` (auto-increment) serta dua foreign key: `kode_bps` yang mereferensi `dim_wilayah` dan `tahun` yang mereferensi `dim_waktu`. Selain data mentah (jumlah kasus, curah hujan, suhu, NDVI, NDMI), tabel fakta juga menyimpan seluruh fitur turunan hasil transformasi, sehingga query analitik tidak memerlukan perhitungan ulang pada saat runtime.

```text
                    ┌────────────────┐
                    │  dim_wilayah   │
                    │  PK: kode_bps  │
                    └───────┬────────┘
                            │ FK
                            ▼
┌────────────┐     ┌────────────────┐     ┌──────────────────┐
│ dim_waktu  │────▶│  fact_dbd_env  │     │  pipeline_runs   │
│ PK: tahun  │ FK  │  PK: fact_id   │     │  PK: run_id      │
└────────────┘     │  25 measures   │     │  observability   │
                   └────────────────┘     └──────────────────┘
```

**Tabel Observability (`pipeline_runs`)** mencatat metadata setiap eksekusi pipeline secara otomatis, meliputi: rentang tahun yang diproses, jumlah wilayah, jumlah baris fakta, jumlah baris yang diimputasi, jumlah baris dengan null incidence rate, serta status eksekusi. Tabel ini memungkinkan tim melakukan monitoring kualitas data lintas waktu tanpa memeriksa log Airflow.

---

## Hasil Akhir dan Insight

Pipeline yang telah berhasil dijalankan menghasilkan data warehouse dengan 162 baris fakta yang mencakup seluruh kombinasi 27 kabupaten/kota dan 6 tahun observasi (2019–2024). Dari total tersebut, 12 baris (7,4%) merupakan hasil imputasi Tobler untuk dua wilayah tanpa coverage GEE, dan tidak terdapat baris dengan null incidence rate — mengindikasikan integritas data yang terjaga.

Dashboard Metabase yang terhubung langsung ke warehouse menyajikan visualisasi interaktif yang memungkinkan analisis dari berbagai perspektif. Query OLAP dirancang secara dinamis menggunakan subquery `MAX(tahun)` sehingga dashboard secara otomatis menampilkan data tahun terbaru tanpa perlu edit manual.

Dari hasil analisis, beberapa insight utama dapat diidentifikasi. Pertama, Kota Bandung secara konsisten mencatat incidence rate tertinggi dengan endemic persistence score mencapai 6 (masuk Q4 di seluruh periode observasi), menjadikannya hotspot DBD paling persisten di Jawa Barat. Kedua, wilayah dengan kepadatan penduduk sangat tinggi (Q4) seperti Kota Bekasi, Kota Depok, dan Kota Cimahi juga menunjukkan CVI yang tinggi, mengkonfirmasi hubungan antara urbanisasi dan kerentanan terhadap DBD. Ketiga, tahun 2024 menunjukkan lonjakan kasus signifikan di hampir seluruh wilayah — Kabupaten Garut misalnya mencatat YoY growth rate sebesar 342% — yang berkorelasi dengan pola curah hujan di atas rata-rata pada periode tersebut. Keempat, lag features curah hujan mengindikasikan bahwa dampak anomali curah hujan terhadap kasus DBD tidak bersifat instan melainkan memiliki efek tertunda 1–3 tahun.

Sistem ini memberikan nilai tambah bagi pengambilan keputusan di bidang kesehatan masyarakat dengan menyediakan early warning berbasis data untuk alokasi sumber daya pengendalian vektor, identifikasi wilayah prioritas intervensi berdasarkan CVI dan endemic persistence, serta kemampuan membandingkan pola kasus pada periode pandemi versus normal melalui dimensi waktu.

---

## Kesimpulan

Project ini berhasil membangun sebuah data warehouse end-to-end yang mengintegrasikan data kesehatan, lingkungan, dan spasial ke dalam arsitektur star schema melalui pipeline ETL yang sepenuhnya terotomatisasi. Dengan enam fitur perbaikan yang diterapkan — meliputi tabel observability, konfigurasi tahun dinamis, threshold validasi adaptif, NaN guard, sistem peringatan pemetaan GEE, dan query OLAP dinamis — pipeline ini telah mencapai tingkat kematangan yang memadai untuk lingkungan produksi.

Arsitektur yang dibangun bersifat extensible: penambahan data tahun baru cukup dilakukan melalui konfigurasi `.env` tanpa modifikasi kode, dan dashboard akan secara otomatis menyesuaikan. Ke depan, pipeline ini dapat diperkaya dengan granularitas data bulanan, integrasi data real-time dari API kesehatan, serta penerapan model prediktif untuk forecasting kasus DBD berdasarkan fitur-fitur lingkungan yang telah tersedia di warehouse.

---

## Lampiran

### A. Teknologi yang Digunakan

| Komponen | Teknologi | Fungsi |
|---|---|---|
| Orchestrator | Apache Airflow 2.x | Penjadwalan dan orkestrasi DAG |
| Source DB 1 | MySQL (Laragon) | Penyimpanan data BPS |
| Source DB 2 | PostgreSQL 16 | Penyimpanan data GEE + DWH |
| Staging + DWH | PostgreSQL (2 schema) | `staging_dbd_v3` + `warehouse_dbd_v3` |
| ETL Runtime | Python 3.x, pandas, scipy | Transformasi dan feature engineering |
| Spatial | geopandas, GeoJSON | Ekstraksi koordinat spasial |
| Dashboard | Metabase | Visualisasi OLAP dan peta |
| Environment | WSL2 (Ubuntu) | Runtime Airflow |
| Config | python-dotenv, `.env` | Manajemen konfigurasi dinamis |

### B. Struktur Tabel Data Warehouse

**fact_dbd_env** (162 rows, 25 kolom)

| Kolom | Tipe | Keterangan |
|---|---|---|
| fact_id | SERIAL (PK) | Surrogate key |
| kode_bps | VARCHAR(10) (FK) | Referensi ke dim_wilayah |
| tahun | INT (FK) | Referensi ke dim_waktu |
| jumlah_kasus_dbd | INT | Kasus DBD absolut |
| incidence_rate | FLOAT | Kasus per 100.000 penduduk |
| kepadatan_penduduk | FLOAT | Jiwa per km² |
| luas_km2 | FLOAT | Luas wilayah |
| rainfall_mm | FLOAT | Curah hujan tahunan (mm) |
| rainfall_zscore | FLOAT | Standarisasi curah hujan |
| lst_celsius | FLOAT | Suhu permukaan tanah |
| lst_quartile_rank | VARCHAR(5) | Kuartil suhu (Q1–Q4) |
| ndvi | FLOAT | Indeks vegetasi |
| ndvi_class | VARCHAR(20) | Sparse / Moderate / Dense |
| ndmi | FLOAT | Indeks kelembaban |
| water_body_density_tier | VARCHAR(10) | Low / Med / High |
| lag1_rainfall | FLOAT | Curah hujan t-1 |
| lag2_rainfall | FLOAT | Curah hujan t-2 |
| lag3_rainfall | FLOAT | Curah hujan t-3 |
| composite_vulnerability_index | FLOAT | Indeks kerentanan komposit |
| yoy_growth_rate | FLOAT | Pertumbuhan IR tahunan (%) |
| kepadatan_quartile | VARCHAR(5) | Kuartil kepadatan |
| density_tier | VARCHAR(20) | Rendah / Sedang / Tinggi / Sangat Tinggi |
| is_imputed | BOOLEAN | Flag data hasil imputasi |
| ir_q | INT | Kuartil incidence rate |
| endemic_persistence_score | INT | Skor persistensi endemis |
