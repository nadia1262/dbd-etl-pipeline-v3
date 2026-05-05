"""
db_config.py — Pipeline V3 (FINAL)
====================================
Sentralisasi koneksi database dan path management.

ARSITEKTUR DATABASE V3:
  MySQL  (source_os_dbd)     → sumber data BPS: health, demografi, luas
  PostgreSQL (dwh_dbd_v3)    → staging + DWH dalam 1 database, 2 schema:
    - staging_dbd_v3.*       → 4 tabel staging
    - warehouse_dbd_v3.*     → dim + fact (Star Schema)

Kenapa schema, bukan database terpisah?
  → PostgreSQL cross-schema query = native (SELECT staging_dbd_v3.x JOIN warehouse_dbd_v3.y)
  → Cross-database query di PostgreSQL TIDAK bisa tanpa extension (dblink/FDW)
  → Satu database + 2 schema = best practice untuk staging+DWH di PG
"""
import os
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

_DOCS_V3_DIR = Path(__file__).resolve().parent.parent
load_dotenv(_DOCS_V3_DIR / '.env')

# ============================================================
# CONNECTION STRINGS
# ============================================================
MYSQL_SOURCE_URI = (
    f"mysql+pymysql://{os.getenv('MYSQL_USER','root')}:{os.getenv('MYSQL_PASS','rootpassword123')}"
    f"@{os.getenv('MYSQL_HOST','localhost')}:{os.getenv('MYSQL_PORT','3306')}"
    f"/{os.getenv('MYSQL_SOURCE_DB','source_os_dbd')}"
)

PG_DWH_URI = os.getenv(
    'PG_DWH_CONN',
    'postgresql://postgres:Bismillah_22@localhost:5432/dwh_dbd_v3'
)

# Schema names
STAGING_SCHEMA = os.getenv('STAGING_SCHEMA', 'staging_dbd_v3')
WAREHOUSE_SCHEMA = os.getenv('WAREHOUSE_SCHEMA', 'warehouse_dbd_v3')


def get_mysql_source_engine():
    """Engine ke MySQL source_os_dbd (data BPS: health + demografi).
    Kita WRITE raw CSV ke sini, lalu DAG akan READ dari sini.
    """
    return create_engine(MYSQL_SOURCE_URI)


def get_pg_engine():
    """Engine ke PostgreSQL dwh_dbd_v3.
    Satu engine untuk staging + DWH (beda schema).
    """
    return create_engine(PG_DWH_URI)


# ============================================================
# PATH MANAGEMENT
# ============================================================
PROJECT_ROOT = _DOCS_V3_DIR.parent
DATA_DIR = 'data'


def get_data_path(filename: str) -> Path:
    return PROJECT_ROOT / DATA_DIR / filename


def get_geojson_path() -> Path:
    return PROJECT_ROOT / DATA_DIR / 'jabar_kabkot'


def get_docs_v3_dir() -> Path:
    return _DOCS_V3_DIR
