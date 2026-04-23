"""
共享配置 — LA Water Quality 项目
所有 build_*.py 脚本从此导入路径和常量，保持一致性。
"""

import os

# ── 基础路径 ────────────────────────────────────────────────────────────────
ROOT_DIR  = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BUILD_DIR = os.path.join(ROOT_DIR, "src", "build")
SRC_DIR   = os.path.join(ROOT_DIR, "src")
DATA_DIR  = os.path.join(ROOT_DIR, "data", "raw_data")
OUT_DIR   = os.path.join(ROOT_DIR, "output")
OUT_DATA  = os.path.join(OUT_DIR, "data")

# ── 子数据目录 ──────────────────────────────────────────────────────────────
WQP_DIR      = os.path.join(DATA_DIR, "wqp")
AQS_DIR      = os.path.join(DATA_DIR, "aqs")
CENSUS_DIR   = os.path.join(DATA_DIR, "census")
FIRE_DIR     = os.path.join(DATA_DIR, "fire_perimeters")
TRI_DIR      = os.path.join(DATA_DIR, "epa_tri")
EWG_DIR      = os.path.join(DATA_DIR, "ewg")
GEOTRACKER_DIR = os.path.join(DATA_DIR, "geotracker")
USGS_DIR     = os.path.join(DATA_DIR, "usgs")

# ── 中间产物路径 ────────────────────────────────────────────────────────────
# build_aqs_zipcode.py → build_wqp.py 依赖
AQS_ZCTA_GEO  = os.path.join(OUT_DATA, "aqs_zcta_geo.geojson")
AQS_ZCTA_DATA = os.path.join(OUT_DATA, "aqs_zcta_data.json")

# build_wqp.py 输出
WQP_ZCTA_DATA  = os.path.join(OUT_DATA, "wqp_zcta_data.json")
WQP_STATIONS   = os.path.join(OUT_DATA, "wqp_stations.json")

# ── 地理常量 ────────────────────────────────────────────────────────────────
LA_CENTER  = (34.0522, -118.2437)   # (lat, lon)
LA_BOUNDS  = {                       # LA County 大致边界
    "lat_min": 33.5, "lat_max": 34.9,
    "lon_min": -119.0, "lon_max": -117.5,
}

# ── 时间常量 ────────────────────────────────────────────────────────────────
FIRE_DATE = "2025-01-07"   # Palisades/Eaton 野火爆发日期
