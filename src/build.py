#!/usr/bin/env python3
"""
build.py — 构建 MapLibre 地图所需的所有数据文件

依赖：先运行 build_aqs_zipcode.py（输出 aqs_zcta_geo.geojson）

输出（output/data/）：
  wqp_zcta_data.json   — WQP 污染物按 ZCTA + 月份聚合
  wqp_stations.json    — WQP 站点坐标、类型、月度数据
  wqp_heatmap.geojson  — WQP 热力层
  wqp_monthly.json     — WQP 月度时间轴
  tri.geojson          — TRI 工业排放设施
  geotracker.geojson   — GeoTracker 污染地块
  schools.geojson      — 学校铅采样
  groundwater.geojson  — 地下水井（抽样）
  aqs.geojson          — AQS 空气质量站
  ejscreen.geojson     — CalEnviroScreen 环境正义
  cdc_places.geojson   — CDC PLACES 健康数据
  superfund.geojson    — EPA Superfund 场地
  usgs.geojson         — USGS 水文监测站
  fires.geojson        — 野火边界（拷贝）
  water_systems.geojson— 供水系统边界（拷贝）
  system_panel.json    — 供水系统面板数据（拷贝）

用法：
  python src/build_aqs_zipcode.py   # 首次运行或 ZCTA 边界变化时
  python src/build.py
"""

import os, json, shutil, random
import pandas as pd
import numpy as np
from shapely.geometry import Point, shape
from collections import defaultdict
from config import (
    DATA_DIR, OUT_DATA,
    WQP_DIR, AQS_DIR, CENSUS_DIR, FIRE_DIR, TRI_DIR, EWG_DIR,
    GEOTRACKER_DIR, USGS_DIR,
    AQS_ZCTA_GEO,
    LA_BOUNDS, FIRE_DATE,
)

os.makedirs(OUT_DATA, exist_ok=True)

# ── 污染物映射（WQP 原始名 → 显示名）─────────────────────────────────────────
CONTAMINANT_MAP = {
    "pH":                       "pH",
    "Ammonia":                  "氨氮 Ammonia",
    "Ammonia and ammonium":     "氨氮 Ammonia",
    "Chloride":                 "氯化物 Chloride",
    "Turbidity":                "浊度 Turbidity",
    "Total dissolved solids":   "溶解固体 TDS",
    "Arsenic":                  "砷 Arsenic",
    "Selenium":                 "硒 Selenium",
    "Nitrate":                  "硝酸盐 Nitrate",
    "Nitrate as N":             "硝酸盐 Nitrate",
    "Orthophosphate":           "磷酸盐 Phosphate",
    "Phosphorus":               "磷 Phosphorus",
    "Barium":                   "钡 Barium",
    "Iron":                     "铁 Iron",
    "Lead":                     "铅 Lead",
    "Copper":                   "铜 Copper",
    "Chromium":                 "铬 Chromium",
    "Zinc":                     "锌 Zinc",
    "Manganese":                "锰 Manganese",
    "Escherichia coli":         "大肠杆菌 E. coli",
    "Total Coliform":           "总大肠菌 Coliform",
    "Enterococcus":             "肠球菌 Enterococcus",
    "Fecal Coliform":           "粪大肠菌 Fecal Coliform",
    "Oxygen":                   "溶解氧 DO",
    "Temperature, water":       "水温 Temp",
    "Specific conductance":     "电导率 Conductance",
    "Alkalinity":               "碱度 Alkalinity",
}

CATEGORIES = {
    "微生物":   ["大肠杆菌 E. coli", "总大肠菌 Coliform", "肠球菌 Enterococcus", "粪大肠菌 Fecal Coliform"],
    "重金属":   ["砷 Arsenic", "硒 Selenium", "钡 Barium", "铁 Iron", "铅 Lead", "铜 Copper", "铬 Chromium", "锌 Zinc", "锰 Manganese"],
    "营养盐":   ["氨氮 Ammonia", "硝酸盐 Nitrate", "磷 Phosphorus", "磷酸盐 Phosphate"],
    "理化指标": ["pH", "溶解固体 TDS", "电导率 Conductance", "浊度 Turbidity", "水温 Temp", "溶解氧 DO", "碱度 Alkalinity", "氯化物 Chloride"],
}


# ═══════════════════════════════════════════════════════════════════════════════
# 数据加载
# ═══════════════════════════════════════════════════════════════════════════════

def load_stations():
    df = pd.read_csv(os.path.join(WQP_DIR, "stations.csv"), low_memory=False, usecols=[
        "MonitoringLocationIdentifier", "LatitudeMeasure", "LongitudeMeasure",
        "MonitoringLocationName", "MonitoringLocationTypeName",
    ]).rename(columns={
        "MonitoringLocationIdentifier": "station_id",
        "LatitudeMeasure":              "lat",
        "LongitudeMeasure":             "lon",
        "MonitoringLocationName":       "station_name",
        "MonitoringLocationTypeName":   "station_type",
    })
    df = df.dropna(subset=["lat", "lon"])
    lat_min, lat_max = LA_BOUNDS["lat_min"], LA_BOUNDS["lat_max"]
    lon_min, lon_max = LA_BOUNDS["lon_min"], LA_BOUNDS["lon_max"]
    df = df[(df["lat"].between(lat_min, lat_max)) & (df["lon"].between(lon_min, lon_max))]
    print(f"  WQP 站点：{len(df)} 个")
    return df


def load_results():
    print("  加载 WQP 检测结果（可能需要 30 秒）...")
    df = pd.read_csv(os.path.join(WQP_DIR, "results.csv"), low_memory=False, usecols=[
        "MonitoringLocationIdentifier", "ActivityStartDate",
        "CharacteristicName", "ResultMeasureValue", "ResultStatusIdentifier",
    ]).rename(columns={
        "MonitoringLocationIdentifier": "station_id",
        "ActivityStartDate":            "date",
        "CharacteristicName":           "raw_contaminant",
        "ResultMeasureValue":           "value",
    })
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"]  = pd.to_datetime(df["date"],  errors="coerce")
    df = df.dropna(subset=["value", "date"])
    df = df[df["value"] >= 0]
    df["contaminant"] = df["raw_contaminant"].map(CONTAMINANT_MAP)
    df = df.dropna(subset=["contaminant"])
    df["month"] = df["date"].dt.to_period("M").astype(str)
    print(f"  WQP 检测结果：{len(df)} 条，{df['contaminant'].nunique()} 种污染物")
    return df


def load_tri():
    path = os.path.join(DATA_DIR, "epa_tri", "tri_facilities.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)

    def dms_to_dd(val, is_lon=False):
        s = str(int(abs(val))).zfill(7 if is_lon else 6)
        if is_lon:
            deg, m, s_ = int(s[:3]), int(s[3:5]), int(s[5:])
        else:
            deg, m, s_ = int(s[:2]), int(s[2:4]), int(s[4:])
        dd = deg + m / 60 + s_ / 3600
        return -dd if is_lon else dd

    valid = []
    for d in data:
        try:
            lat = dms_to_dd(d["fac_latitude"])
            lon = dms_to_dd(d["fac_longitude"], is_lon=True)
            if 33.5 < lat < 34.9 and -119.0 < lon < -117.5:
                valid.append({"lat": lat, "lon": lon,
                              "name": (d.get("facility_name") or "")[:50],
                              "naics": str(d.get("primary_naics_code", ""))})
        except (ValueError, TypeError, KeyError):
            pass
    print(f"  TRI 设施：{len(valid)} 个")
    return valid


def load_geotracker():
    path = os.path.join(DATA_DIR, "geotracker", "geotracker_sites.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    valid = []
    for d in data:
        try:
            lat = float(d.get("LATITUDE") or 0)
            lon = float(d.get("LONGITUDE") or 0)
            if lat < 0:
                lat, lon = lon, lat
            if 33.5 < lat < 34.9 and -119.0 < lon < -117.5:
                valid.append({"lat": lat, "lon": lon,
                              "name": (d.get("BUSINESS_NAME") or "")[:50],
                              "case_type": d.get("CASE_TYPE", "")})
        except (ValueError, TypeError):
            pass
    print(f"  GeoTracker 场地：{len(valid)} 个")
    return valid


def load_school_lead():
    path = os.path.join(DATA_DIR, "school_lead", "la_school_lead_geocoded.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    print(f"  学校铅采样：{len(data)} 所")
    return data


def load_groundwater():
    path = os.path.join(DATA_DIR, "ca_open_data", "groundwater_stations.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        raw = json.load(f)
    records = raw.get("result", {}).get("records", [])
    valid = [{"lat": float(d["gm_latitude"]), "lon": float(d["gm_longitude"]),
              "category": d.get("gm_well_category", "")}
             for d in records
             if 33.5 < float(d.get("gm_latitude") or 0) < 34.9
             and -119.0 < float(d.get("gm_longitude") or 0) < -117.5]
    print(f"  地下水井：{len(valid)} 口")
    return valid


def load_aqs():
    aqi_path = os.path.join(AQS_DIR, "wildfire_period_aqi.json")
    if not os.path.exists(aqi_path):
        return []
    with open(aqi_path) as f:
        aqi_data = json.load(f)
    aqi_by_site = {}
    for param, records in aqi_data.items():
        for r in records:
            key = (r.get("state_code"), r.get("county_code"), r.get("site_number"))
            aqi_by_site.setdefault(key, []).append(r)
    result, seen = [], set()
    for rec_list in aqi_by_site.values():
        r0 = rec_list[0]
        try:
            lat, lon = float(r0["latitude"]), float(r0["longitude"])
        except (TypeError, ValueError):
            continue
        if not (33.5 < lat < 34.9 and -119.0 < lon < -117.5):
            continue
        key = (round(lat, 3), round(lon, 3))
        if key in seen:
            continue
        seen.add(key)
        result.append({
            "lat": lat, "lon": lon,
            "site_name": r0.get("local_site_name", ""),
            "max_aqi": max((x.get("aqi") or 0) for x in rec_list),
        })
    print(f"  AQS 空气质量站：{len(result)} 个")
    return result


def load_ejscreen():
    path = os.path.join(DATA_DIR, "ejscreen", "la_ejscreen_tracts.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    valid = []
    for d in data:
        try:
            valid.append({
                "lat": float(d["latitude"]), "lon": float(d["longitude"]),
                "ces_score": float(d.get("ces_40_score") or 0),
                "ces_pct":   d.get("ces_40_percentile", ""),
                "poverty":   d.get("poverty", ""),
                "tract":     d.get("census_tract", ""),
            })
        except (ValueError, TypeError, KeyError):
            pass
    print(f"  CalEnviroScreen：{len(valid)} 个 tracts")
    return valid


def load_cdc_places():
    path   = os.path.join(DATA_DIR, "cdc_places", "la_health_wide.json")
    ej_path = os.path.join(DATA_DIR, "ejscreen", "la_ejscreen_tracts.json")
    if not os.path.exists(path) or not os.path.exists(ej_path):
        return []
    with open(path) as f:
        health = json.load(f)
    with open(ej_path) as f:
        ej = json.load(f)
    coord_map = {}
    for d in ej:
        tract = str(int(float(d.get("census_tract", 0)))).zfill(11)
        try:
            coord_map[tract] = (float(d["latitude"]), float(d["longitude"]))
        except (ValueError, TypeError):
            pass
    result = []
    for h in health:
        fips = str(h.get("tractfips", "")).zfill(11)
        coords = coord_map.get(fips)
        if not coords:
            continue
        try:
            result.append({
                "lat": coords[0], "lon": coords[1],
                "asthma_pct":   float(h.get("casthma") or 0),
                "cancer_pct":   float(h.get("cancer")  or 0),
                "diabetes_pct": float(h.get("diabetes") or 0),
                "copd_pct":     float(h.get("copd")    or 0),
            })
        except (ValueError, TypeError):
            pass
    print(f"  CDC PLACES：{len(result)} 个 tracts")
    return result


def load_superfund():
    path = os.path.join(DATA_DIR, "epa_tri", "superfund_npl.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    print(f"  Superfund：{len(data)} 个场地")
    return data


def load_usgs():
    path = os.path.join(DATA_DIR, "usgs", "la_water_gauges.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    print(f"  USGS 水文站：{len(data)} 个")
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# WQP → ZCTA 聚合（生成 wqp_zcta_data.json + wqp_stations.json）
# ═══════════════════════════════════════════════════════════════════════════════

def build_wqp_zcta(stations_df, results_df):
    print("\n── WQP ZCTA 聚合 ──")
    with open(AQS_ZCTA_GEO) as f:
        geo = json.load(f)

    zcta_shapes = [(feat["properties"]["zcta"], shape(feat["geometry"]))
                   for feat in geo["features"]]
    zcta_centroids = {zcta: [round(poly.centroid.x, 6), round(poly.centroid.y, 6)]
                      for zcta, poly in zcta_shapes}

    def assign_zcta(lat, lon):
        pt = Point(lon, lat)
        for zcta, poly in zcta_shapes:
            if poly.contains(pt):
                return zcta
        best, best_d = None, float("inf")
        for zcta, (cx, cy) in zcta_centroids.items():
            d = (cx - lon) ** 2 + (cy - lat) ** 2
            if d < best_d:
                best_d, best = d, zcta
        return best

    # 合并坐标
    merged = results_df.merge(
        stations_df[["station_id", "lat", "lon", "station_type"]], on="station_id", how="inner"
    )

    # 分配 ZCTA（缓存到站点）
    unique_st = merged[["station_id", "lat", "lon", "station_type"]].drop_duplicates("station_id").copy()
    print(f"  分配 {len(unique_st)} 个站点到 ZCTA...")
    unique_st["zcta"] = unique_st.apply(lambda r: assign_zcta(r.lat, r.lon), axis=1)

    station_zcta   = dict(zip(unique_st["station_id"], unique_st["zcta"]))
    station_type   = dict(zip(unique_st["station_id"], unique_st["station_type"]))
    station_coords = {r.station_id: [round(r.lon, 6), round(r.lat, 6)]
                      for r in unique_st.itertuples()}

    merged["zcta"] = merged["station_id"].map(station_zcta)
    merged = merged.dropna(subset=["zcta"])

    # 站点粒度聚合 → wqp_stations.json
    st_agg = merged.groupby(["station_id", "contaminant", "month"])["value"].mean().reset_index()
    st_data: dict = defaultdict(lambda: defaultdict(dict))
    for _, r in st_agg.iterrows():
        st_data[r.contaminant][r.month][r.station_id] = round(float(r.value), 4)

    # ZCTA 粒度聚合 → wqp_zcta_data.json
    zcta_agg = merged.groupby(["zcta", "contaminant", "month"])["value"].mean().reset_index()
    contaminants = sorted(zcta_agg["contaminant"].unique().tolist())
    months       = sorted(zcta_agg["month"].unique().tolist())

    zcta_data: dict = defaultdict(lambda: defaultdict(dict))
    for _, r in zcta_agg.iterrows():
        zcta_data[r.contaminant][r.month][r.zcta] = round(float(r.value), 4)

    all_zctas = sorted(set(station_zcta.values()))
    zcta_id   = {z: i for i, z in enumerate(all_zctas)}

    max_values = {}
    for cont in contaminants:
        vals = [v for md in zcta_data[cont].values() for v in md.values()]
        if vals:
            max_values[cont] = round(float(np.percentile(vals, 95)), 4)

    # 按分类排序污染物
    ordered, seen = [], set()
    for members in CATEGORIES.values():
        for m in members:
            if m in contaminants and m not in seen:
                ordered.append(m); seen.add(m)
    for c in contaminants:
        if c not in seen:
            ordered.append(c)

    cont_category = {m: cat for cat, members in CATEGORIES.items() for m in members}

    with open(os.path.join(OUT_DATA, "wqp_zcta_data.json"), "w") as f:
        json.dump({
            "months": months, "zcta_id": zcta_id,
            "contaminants": ordered, "categories": CATEGORIES,
            "cont_category": cont_category,
            "data": {c: dict(m) for c, m in zcta_data.items()},
            "max_values": max_values,
        }, f, ensure_ascii=False)
    print(f"  ✓ wqp_zcta_data.json  ({len(contaminants)} 污染物, {len(months)} 月, {len(zcta_id)} ZCTA)")

    with open(os.path.join(OUT_DATA, "wqp_stations.json"), "w") as f:
        json.dump({
            "station_coords":  station_coords,
            "station_zcta":    station_zcta,
            "station_type":    station_type,
            "zcta_centroids":  zcta_centroids,
            "data": {c: dict(m) for c, m in st_data.items()},
        }, f, ensure_ascii=False)
    print(f"  ✓ wqp_stations.json   ({len(station_coords)} 站点)")

    return merged


# ═══════════════════════════════════════════════════════════════════════════════
# GeoJSON 导出（供 MapLibre 加载）
# ═══════════════════════════════════════════════════════════════════════════════

def export_geojson(stations_df, results_df, merged_df,
                   tri, geotracker, schools, groundwater,
                   aqs, ejscreen, cdc, superfund, usgs):
    print("\n── 导出 GeoJSON 图层 ──")

    def pt(lon, lat, props):
        return {"type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props}

    def save(name, features):
        path = os.path.join(OUT_DATA, f"{name}.geojson")
        with open(path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        print(f"  ✓ {name}.geojson  ({len(features)} features)")

    # 1. WQP 热力图（最新一条记录）
    latest = results_df.merge(stations_df[["station_id", "lat", "lon"]], on="station_id", how="inner")
    latest = latest.sort_values("date").groupby("station_id").last().reset_index()
    if not latest.empty and latest["value"].max() > 0:
        mn, mx = latest["value"].min(), latest["value"].max()
        save("wqp_heatmap", [
            pt(float(r.lon), float(r.lat),
               {"w": round((float(r.value) - mn) / (mx - mn + 1e-9), 3),
                "c": str(r.contaminant)})
            for _, r in latest.iterrows() if pd.notna(r.lat)
        ])

    # 2. WQP 月度时间轴
    m2 = merged_df.merge(stations_df[["station_id", "lat", "lon"]], on="station_id", how="inner")
    mo = (m2.groupby(["station_id", "month"])
            .agg(avg=("value", "mean"), n=("value", "count"))
            .reset_index()
            .merge(stations_df[["station_id", "lat", "lon"]], on="station_id", how="inner"))
    p95 = mo["avg"].quantile(0.95)
    mo["norm"] = (mo["avg"] / (p95 + 1e-9)).clip(0, 1)
    months_list = sorted(mo["month"].unique().tolist())
    feats = [{"type": "Feature",
               "geometry": {"type": "Point", "coordinates": [float(r.lon), float(r.lat)]},
               "properties": {"month": r.month, "norm": round(float(r.norm), 3),
                              "avg": round(float(r.avg), 4), "n": int(r.n)}}
              for _, r in mo.iterrows()]
    with open(os.path.join(OUT_DATA, "wqp_monthly.json"), "w") as f:
        json.dump({"months": months_list, "features": feats}, f)
    print(f"  ✓ wqp_monthly.json   ({len(feats)} records, {len(months_list)} months)")

    # 3. 点位图层
    save("tri",        [pt(d["lon"], d["lat"], {"name": d["name"], "naics": d["naics"]})      for d in tri])
    save("geotracker", [pt(d["lon"], d["lat"], {"name": d["name"], "case_type": d["case_type"]}) for d in geotracker])
    save("schools",    [pt(float(d["lon"]), float(d["lat"]),
                           {"name": str(d.get("school_name", ""))[:50],
                            "max_ppb": float(d.get("max_lead_ppb") or 0),
                            "exceed": bool(d.get("has_exceedance", False))})
                        for d in schools if d.get("lat") and d.get("lon")])
    save("groundwater",[pt(d["lon"], d["lat"], {"cat": d["category"]})
                        for d in random.sample(groundwater, min(3000, len(groundwater)))])
    save("aqs",        [pt(d["lon"], d["lat"], {"name": d["site_name"], "aqi": int(d["max_aqi"] or 0)}) for d in aqs])
    save("ejscreen",   [pt(d["lon"], d["lat"], {"ces": float(d["ces_score"] or 0),
                                                 "pov": str(d["poverty"]),
                                                 "tract": str(d["tract"])})    for d in ejscreen])
    save("cdc_places", [pt(d["lon"], d["lat"], {"asthma": float(d["asthma_pct"] or 0),
                                                  "cancer": float(d["cancer_pct"] or 0)}) for d in cdc])
    save("superfund",  [pt(float(d["lon"]), float(d["lat"]),
                           {"name": str(d.get("name", ""))[:50], "city": str(d.get("city", ""))})
                        for d in superfund if d.get("lat") and d.get("lon")])
    save("usgs",       [pt(float(d["lon"]), float(d["lat"]),
                           {"name": str(d.get("station_nm", ""))[:50], "type": str(d.get("site_type", ""))})
                        for d in random.sample(usgs, min(2000, len(usgs)))])

    # 4. 拷贝静态文件
    for src_rel, dst_name in [
        (os.path.join(DATA_DIR, "fire_perimeters", "la_2025_fires_calfire.geojson"), "fires.geojson"),
        (os.path.join(DATA_DIR, "water_system_boundaries", "la_water_system_boundaries.geojson"), "water_systems.geojson"),
        (os.path.join(DATA_DIR, "water_system_boundaries", "system_panel_data.json"), "system_panel.json"),
    ]:
        if os.path.exists(src_rel):
            shutil.copy(src_rel, os.path.join(OUT_DATA, dst_name))
            print(f"  ✓ {dst_name} (拷贝)")

    print(f"\n  所有图层已写入 → {OUT_DATA}/")


# ═══════════════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=== build.py — LA Water Quality Data Pipeline ===\n")

    print("1. 加载原始数据...")
    stations_df = load_stations()
    results_df  = load_results()
    tri         = load_tri()
    geotracker  = load_geotracker()
    schools     = load_school_lead()
    groundwater = load_groundwater()
    aqs         = load_aqs()
    ejscreen    = load_ejscreen()
    cdc         = load_cdc_places()
    superfund   = load_superfund()
    usgs        = load_usgs()

    print("\n2. WQP → ZCTA 聚合...")
    merged_df = build_wqp_zcta(stations_df, results_df)

    print("\n3. 导出 GeoJSON...")
    export_geojson(stations_df, results_df, merged_df,
                   tri, geotracker, schools, groundwater,
                   aqs, ejscreen, cdc, superfund, usgs)

    print("\n✅ 完成！")


if __name__ == "__main__":
    main()
