# End-to-End Data Warehouse untuk Deteksi Dini Lonjakan Kasus DBD Berbasis Faktor Lingkungan di Jawa Barat

## Overview

ETL pipeline and data warehouse for epidemiological analysis of Dengue Hemorrhagic Fever (DBD) in West Java, integrating health, demographic, environmental, and spatial data. Built as a midterm project for the Data Engineering course at Politeknik Statistika STIS. Covers 27 regions across 6 years (2019–2024).

---

## Pipeline Architecture

```
[MySQL]        ─┐
[PostgreSQL]   ─┼──► Extract ──► Staging ──► Validation ──► Transform ──► Data Warehouse (PostgreSQL)
[GeoJSON]      ─┘                                                                    │
                                                                                     ▼
                                                                               Metabase (Dashboard)

Orchestration: Apache Airflow
```

---

## Project Structure

```
docs_v3/
├── dags/        → Airflow DAG definitions
├── config/      → Database connection configurations
├── scripts/     → Data ingestion and transformation scripts
├── sql/         → DDL statements and analytical queries
├── data/        → Raw source data files
└── *.md         → Project documentation
```

---

## Data Warehouse

Star schema with the following tables:

| Table | Description |
|---|---|
| `dim_wilayah` | Regional dimension (27 kabupaten/kota) |
| `dim_waktu` | Time dimension (year/month) |
| `fact_dbd_env` | Core fact table: DBD cases + environmental factors |
| `pipeline_runs` | Pipeline observability & run tracking |

---

## Features

- `incidence_rate` — DBD cases per 100,000 population
- `rainfall_zscore` — Standardized rainfall anomaly score
- `lag_rainfall` — Lagged rainfall variables (1–3 months)
- `CVI` — Composite Vulnerability Index
- `endemic_persistence_score` — Multi-year endemicity measure
- Data validation & imputation tracking
- Pipeline run logging and observability

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Processing | Python (Pandas) |
| Data Warehouse | PostgreSQL |
| Source (Health & Demography) | MySQL |
| Source (Environment / GEE) | PostgreSQL + GeoJSON |
| Visualization | Metabase |

---

## Output

- **Coverage:** 27 regions × 6 years (2019–2024)
- **Format:** Star schema DW, query-ready
- **Dashboard:** Interactive Metabase visualizations for early warning analysis

---

## Team

**Tim 9 – 3SI1**

| Name | NIM |
|---|---|
| Cristiano Teddy Anta | 222313031 |
| Nadia Nur Nisrina | 222313276 |
| Rani Kusumawati | 222313336 |
| Valentina Lasma Situmorang | 222313413 |

D-IV Komputasi Statistik — Politeknik Statistika STIS, 2026