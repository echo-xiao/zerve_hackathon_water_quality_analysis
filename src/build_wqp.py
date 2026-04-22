"""
build_wqp.py — 从原始 WQP 数据生成地图所需 JSON

输出：
  output/data/wqp_zcta_data.json   — 按 ZCTA + 月份 + 污染物聚合
  output/data/wqp_stations.json    — 站点坐标、site_type、按月份数值

用法：
  python src/build_wqp.py
"""

import os, json
import pandas as pd
import numpy as np
from shapely.geometry import Point, shape
from collections import defaultdict

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR  = os.path.join(BASE_DIR, "data", "raw_data", "wqp")
ZCTA_GEO  = os.path.join(BASE_DIR, "output", "data", "aqs_zcta_geo.geojson")
OUT_DIR   = os.path.join(BASE_DIR, "output", "data")

# ── 污染物分类 ──────────────────────────────────────────────────────────────
CATEGORIES = {
    "微生物":   ["大肠杆菌 E. coli", "总大肠菌 Coliform", "肠球菌 Enterococcus", "粪大肠菌 Fecal Coliform"],
    "重金属":   ["砷 Arsenic", "硒 Selenium", "钡 Barium", "铁 Iron", "铅 Lead", "铜 Copper", "铬 Chromium", "锌 Zinc", "锰 Manganese"],
    "营养盐":   ["氨氮 Ammonia", "硝酸盐 Nitrate", "磷 Phosphorus", "磷酸盐 Phosphate"],
    "理化指标": ["pH", "溶解固体 TDS", "电导率 Conductance", "浊度 Turbidity", "水温 Temp", "溶解氧 DO", "碱度 Alkalinity", "氯化物 Chloride"],
}

# ── 污染物英文 → 显示名映射 ──────────────────────────────────────────────────
CONTAMINANT_MAP = {
    "pH":                            "pH",
    "Ammonia":                       "氨氮 Ammonia",
    "Ammonia and ammonium":          "氨氮 Ammonia",
    "Chloride":                      "氯化物 Chloride",
    "Turbidity":                     "浊度 Turbidity",
    "Total dissolved solids":        "溶解固体 TDS",
    "Arsenic":                       "砷 Arsenic",
    "Selenium":                      "硒 Selenium",
    "Nitrate":                       "硝酸盐 Nitrate",
    "Nitrate as N":                  "硝酸盐 Nitrate",
    "Orthophosphate":                "磷酸盐 Phosphate",
    "Phosphorus":                    "磷 Phosphorus",
    "Barium":                        "钡 Barium",
    "Iron":                          "铁 Iron",
    "Lead":                          "铅 Lead",
    "Copper":                        "铜 Copper",
    "Chromium":                      "铬 Chromium",
    "Zinc":                          "锌 Zinc",
    "Manganese":                     "锰 Manganese",
    "Escherichia coli":              "大肠杆菌 E. coli",
    "Total Coliform":                "总大肠菌 Coliform",
    "Enterococcus":                  "肠球菌 Enterococcus",
    "Fecal Coliform":                "粪大肠菌 Fecal Coliform",
    "Oxygen":                        "溶解氧 DO",
    "Temperature, water":            "水温 Temp",
    "Specific conductance":          "电导率 Conductance",
    "Alkalinity":                    "碱度 Alkalinity",
}

# ── 1. 加载站点 ──────────────────────────────────────────────────────────────
print("加载站点数据...")
stations = pd.read_csv(os.path.join(DATA_DIR, "stations.csv"), low_memory=False, usecols=[
    "MonitoringLocationIdentifier",
    "LatitudeMeasure", "LongitudeMeasure",
    "MonitoringLocationTypeName",
])
stations = stations.rename(columns={
    "MonitoringLocationIdentifier": "station_id",
    "LatitudeMeasure":              "lat",
    "LongitudeMeasure":             "lon",
    "MonitoringLocationTypeName":   "site_type",
})
stations = stations.dropna(subset=["lat", "lon"])
stations["lat"] = pd.to_numeric(stations["lat"], errors="coerce")
stations["lon"] = pd.to_numeric(stations["lon"], errors="coerce")
stations = stations.dropna(subset=["lat", "lon"])
print(f"  {len(stations)} 个站点，site types: {stations['site_type'].value_counts().to_dict()}")

# ── 2. 加载检测结果 ──────────────────────────────────────────────────────────
print("加载检测结果（可能需要 30 秒）...")
results = pd.read_csv(os.path.join(DATA_DIR, "results.csv"), low_memory=False, usecols=[
    "MonitoringLocationIdentifier",
    "ActivityStartDate",
    "CharacteristicName",
    "ResultMeasureValue",
    "ResultStatusIdentifier",
])
results = results.rename(columns={
    "MonitoringLocationIdentifier": "station_id",
    "ActivityStartDate":            "date",
    "CharacteristicName":           "raw_contaminant",
    "ResultMeasureValue":           "value",
})
results["value"] = pd.to_numeric(results["value"], errors="coerce")
results["date"]  = pd.to_datetime(results["date"], errors="coerce")
results = results.dropna(subset=["value", "date"])
results = results[results["value"] >= 0]

# 只保留映射内的污染物，并统一显示名
results["contaminant"] = results["raw_contaminant"].map(CONTAMINANT_MAP)
results = results.dropna(subset=["contaminant"])
results["month"] = results["date"].dt.to_period("M").astype(str)
print(f"  {len(results)} 条有效记录，{results['contaminant'].nunique()} 种污染物")

# ── 3. 合并站点坐标 ──────────────────────────────────────────────────────────
merged = results.merge(stations, on="station_id", how="inner")
print(f"  合并后：{len(merged)} 条记录")

# ── 4. 加载 ZCTA 多边形，将站点分配到 ZCTA ──────────────────────────────────
print("加载 ZCTA 边界，分配站点...")
with open(ZCTA_GEO) as f:
    geo = json.load(f)

zcta_shapes = [(feat["properties"]["zcta"], shape(feat["geometry"]))
               for feat in geo["features"]]

# 计算 ZCTA 中心点
zcta_centroids = {}
for zcta, poly in zcta_shapes:
    c = poly.centroid
    zcta_centroids[zcta] = [round(c.x, 6), round(c.y, 6)]

def assign_zcta(lat, lon):
    pt = Point(lon, lat)
    for zcta, poly in zcta_shapes:
        if poly.contains(pt):
            return zcta
    # 找最近中心点
    best, best_d = None, float("inf")
    for zcta, (cx, cy) in zcta_centroids.items():
        d = (cx - lon)**2 + (cy - lat)**2
        if d < best_d:
            best_d, best = d, zcta
    return best

# 缓存：每个 station_id → zcta
unique_stations = merged[["station_id","lat","lon","site_type"]].drop_duplicates("station_id")
print(f"  分配 {len(unique_stations)} 个站点到 ZCTA...")
unique_stations = unique_stations.copy()
unique_stations["zcta"] = unique_stations.apply(
    lambda r: assign_zcta(r.lat, r.lon), axis=1)
station_zcta   = dict(zip(unique_stations["station_id"], unique_stations["zcta"]))
station_type   = dict(zip(unique_stations["station_id"], unique_stations["site_type"]))
station_coords = {r.station_id: [round(r.lon, 6), round(r.lat, 6)]
                  for r in unique_stations.itertuples()}
print("  站点分配完成")

# ── 5. 按站点 + 月份 + 污染物聚合 ───────────────────────────────────────────
print("聚合数据...")
merged["zcta"] = merged["station_id"].map(station_zcta)
merged = merged.dropna(subset=["zcta"])

# -- wqp_stations.json: {contaminant: {month: {station_id: avg_value}}} --
st_agg = (merged.groupby(["station_id","contaminant","month"])["value"]
          .mean().reset_index())

st_data = defaultdict(lambda: defaultdict(dict))
for _, r in st_agg.iterrows():
    st_data[r.contaminant][r.month][r.station_id] = round(float(r.value), 4)

# -- wqp_zcta_data.json: {contaminant: {month: {zcta: avg_value}}} --
zcta_agg = (merged.groupby(["zcta","contaminant","month"])["value"]
            .mean().reset_index())

contaminants = sorted(zcta_agg["contaminant"].unique().tolist())
months       = sorted(zcta_agg["month"].unique().tolist())

zcta_data = defaultdict(lambda: defaultdict(dict))
for _, r in zcta_agg.iterrows():
    zcta_data[r.contaminant][r.month][r.zcta] = round(float(r.value), 4)

# zcta_id mapping
all_zctas = sorted(set(station_zcta.values()))
zcta_id = {z: i for i, z in enumerate(all_zctas)}

# max_values (95th percentile per contaminant)
max_values = {}
for cont in contaminants:
    vals = [v for month_d in zcta_data[cont].values() for v in month_d.values()]
    if vals:
        max_values[cont] = round(float(np.percentile(vals, 95)), 4)

# ── 6. 写出文件 ──────────────────────────────────────────────────────────────
print("写出 wqp_zcta_data.json...")
# 按分类排列 contaminants
def categorize(conts):
    result, seen = [], set()
    for cat, members in CATEGORIES.items():
        for m in members:
            if m in conts and m not in seen:
                result.append(m); seen.add(m)
    for c in conts:
        if c not in seen:
            result.append(c)
    return result

contaminants_ordered = categorize(set(contaminants))
# 构建 contaminant → category 映射
cont_category = {}
for cat, members in CATEGORIES.items():
    for m in members:
        cont_category[m] = cat

with open(os.path.join(OUT_DIR, "wqp_zcta_data.json"), "w") as f:
    json.dump({
        "months":       months,
        "zcta_id":      zcta_id,
        "contaminants": contaminants_ordered,
        "categories":   CATEGORIES,
        "cont_category": cont_category,
        "data":         {c: dict(m) for c, m in zcta_data.items()},
        "max_values":   max_values,
    }, f, ensure_ascii=False)
print(f"  ✓ {len(contaminants)} 种污染物，{len(months)} 个月份，{len(zcta_id)} 个 ZCTA")

print("写出 wqp_stations.json...")
with open(os.path.join(OUT_DIR, "wqp_stations.json"), "w") as f:
    json.dump({
        "station_coords":  station_coords,
        "station_zcta":    station_zcta,
        "station_type":    station_type,
        "zcta_centroids":  zcta_centroids,
        "data":            {c: dict(m) for c, m in st_data.items()},
    }, f, ensure_ascii=False)
print(f"  ✓ {len(station_coords)} 个站点")

print("\n✅ 完成！")
