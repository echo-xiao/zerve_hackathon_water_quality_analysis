"""
LA Water Quality 交互式地图构建
输出：output/water_quality_map.html（可直接在浏览器打开 / 嵌入 Zerve）

地图图层：
  1. 野火边界         — Palisades + Eaton 火灾范围（红色多边形）
  2. 污染物热力图     — WQP 监测站位置密度
  3. 监测站点         — 可点击查看详情
  4. TRI 工业排放设施 — 工业污染源标记
  5. 人口收入分布     — Census Tract 级别（等值区域图）
  6. 供水系统标记     — EWG 主要供水系统

用法：
  python src/build_map.py
  python src/build_map.py --contaminant "Lead"   # 只显示铅污染
  python src/build_map.py --after-fire           # 只显示野火后数据
"""

import os
import sys
import json
import argparse
import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster, MiniMap, Fullscreen, TimestampedGeoJson
from folium import plugins

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(BASE_DIR, "data", "raw_data")
OUT_DIR  = os.path.join(BASE_DIR, "output")
os.makedirs(OUT_DIR, exist_ok=True)

# LA 中心坐标
LA_CENTER = [34.0522, -118.2437]

# 野火关键日期
FIRE_DATE = "2025-01-07"

# 重点关注污染物（野火相关）
WILDFIRE_CONTAMINANTS = [
    "Benzene", "Toluene", "Ethylbenzene", "Xylene",
    "Total Trihalomethanes", "Chloroform",
    "Lead", "Arsenic", "Turbidity",
]


# ─────────────────────────────────────────────
# 数据加载
# ─────────────────────────────────────────────

def load_stations():
    """加载 WQP 监测站（含经纬度）"""
    path = os.path.join(DATA_DIR, "wqp", "stations.csv")
    df = pd.read_csv(path, low_memory=False)
    df = df.rename(columns={
        "LatitudeMeasure": "lat",
        "LongitudeMeasure": "lon",
        "MonitoringLocationIdentifier": "station_id",
        "MonitoringLocationName": "station_name",
        "MonitoringLocationTypeName": "station_type",
    })
    # 过滤有效坐标（LA County 范围）
    df = df.dropna(subset=["lat", "lon"])
    df = df[(df["lat"].between(33.5, 34.9)) & (df["lon"].between(-119.0, -117.5))]
    print(f"  监测站：{len(df)} 个")
    return df


def load_results(contaminant_filter=None, after_fire=False):
    """加载 WQP 检测结果，可按污染物/时间过滤"""
    path = os.path.join(DATA_DIR, "wqp", "results.csv")
    print("  加载检测结果（可能需要 30 秒）...")
    df = pd.read_csv(path, low_memory=False, usecols=[
        "MonitoringLocationIdentifier",
        "ActivityStartDate",
        "CharacteristicName",
        "ResultMeasureValue",
        "ResultMeasure/MeasureUnitCode",
        "ResultStatusIdentifier",
    ])
    df = df.rename(columns={
        "MonitoringLocationIdentifier": "station_id",
        "ActivityStartDate": "date",
        "CharacteristicName": "contaminant",
        "ResultMeasureValue": "value",
        "ResultMeasure/MeasureUnitCode": "unit",
    })
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["value", "date"])

    if contaminant_filter:
        df = df[df["contaminant"].str.contains(contaminant_filter, case=False, na=False)]
    if after_fire:
        df = df[df["date"] >= FIRE_DATE]

    print(f"  检测结果：{len(df)} 条（污染物：{df['contaminant'].nunique()} 种）")
    return df


def load_fire_perimeters():
    """加载野火边界 GeoJSON"""
    path = os.path.join(DATA_DIR, "fire_perimeters", "la_2025_fires_calfire.geojson")
    if not os.path.exists(path):
        print("  ⚠ 未找到野火边界文件")
        return None
    with open(path) as f:
        return json.load(f)


def load_census_tracts():
    """加载 Census Tract 属性（无几何，用质心代替）"""
    path = os.path.join(DATA_DIR, "census", "la_census_tracts.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def _dms_int_to_dd(val, is_lon=False):
    """DDMMSS 整数 → 十进制度数（LA 经度取负）"""
    s = str(int(abs(val))).zfill(7 if is_lon else 6)
    if is_lon:
        deg, mins, secs = int(s[:3]), int(s[3:5]), int(s[5:])
    else:
        deg, mins, secs = int(s[:2]), int(s[2:4]), int(s[4:])
    dd = deg + mins / 60 + secs / 3600
    return -dd if is_lon else dd  # 西经取负


def load_tri_facilities():
    """加载 TRI 工业排放设施（坐标为 DDMMSS 整数格式，转为十进制）"""
    path = os.path.join(DATA_DIR, "epa_tri", "tri_facilities.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    valid = []
    for d in data:
        try:
            lat_raw = d.get("fac_latitude")
            lon_raw = d.get("fac_longitude")
            if not lat_raw or not lon_raw:
                continue
            lat = _dms_int_to_dd(lat_raw, is_lon=False)
            lon = _dms_int_to_dd(lon_raw, is_lon=True)
            if not (33.5 < lat < 34.9 and -119.0 < lon < -117.5):
                continue
            valid.append({
                "lat": lat, "lon": lon,
                "name": (d.get("facility_name") or "Unknown")[:50],
                "naics": d.get("primary_naics_code", ""),
            })
        except (ValueError, TypeError):
            pass
    print(f"  TRI 工业设施：{len(valid)} 个（有坐标）")
    return valid


def load_geotracker():
    """加载 GeoTracker 地下储罐/污染地块"""
    path = os.path.join(DATA_DIR, "geotracker", "geotracker_sites.json")
    if not os.path.exists(path):
        print("  ⚠ 未找到 GeoTracker 文件")
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
            if not (33.5 < lat < 34.9 and -119.0 < lon < -117.5):
                continue
            valid.append({
                "lat": lat, "lon": lon,
                "name": (d.get("BUSINESS_NAME") or "Unknown")[:50],
                "case_type": (d.get("CASE_TYPE") or ""),
                "status": (d.get("STATUS") or ""),
                "contaminants": (d.get("POTENTIAL_CONTAMINANTS_OF_CONCERN") or ""),
            })
        except (ValueError, TypeError):
            pass
    print(f"  GeoTracker 污染地块：{len(valid)} 个")
    return valid


def load_school_lead():
    """加载学校铅采样数据（已 geocoded）"""
    path = os.path.join(DATA_DIR, "school_lead", "la_school_lead_geocoded.json")
    if not os.path.exists(path):
        print("  ⚠ 未找到学校铅数据（请先运行 geocoding）")
        return []
    with open(path) as f:
        data = json.load(f)
    print(f"  学校铅采样：{len(data)} 所学校（已 geocoded）")
    return data


def load_water_boundaries():
    """加载供水系统服务区边界"""
    path = os.path.join(DATA_DIR, "water_system_boundaries", "la_water_system_boundaries.geojson")
    if not os.path.exists(path):
        print("  ⚠ 未找到供水系统边界文件")
        return None
    with open(path) as f:
        data = json.load(f)
    print(f"  供水系统边界：{len(data.get('features', []))} 个")
    return data


def load_groundwater():
    """加载地下水井（37,103 口）"""
    path = os.path.join(DATA_DIR, "ca_open_data", "groundwater_stations.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        raw = json.load(f)
    records = raw.get("result", {}).get("records", [])
    valid = []
    for d in records:
        try:
            lat = float(d["gm_latitude"])
            lon = float(d["gm_longitude"])
            if 33.5 < lat < 34.9 and -119.0 < lon < -117.5:
                valid.append({"lat": lat, "lon": lon,
                              "category": d.get("gm_well_category", "")})
        except (ValueError, TypeError, KeyError):
            pass
    print(f"  地下水井：{len(valid)} 口")
    return valid


def load_aqs_stations():
    """加载 EPA AQS 空气质量监测站及 AQI 数据"""
    station_path = os.path.join(DATA_DIR, "aqs", "stations.json")
    aqi_path     = os.path.join(DATA_DIR, "aqs", "wildfire_period_aqi.json")
    if not os.path.exists(station_path):
        return []
    with open(station_path) as f:
        stations = json.load(f)
    # AQI records: dict keyed by param_name → list of records
    aqi_by_site = {}
    if os.path.exists(aqi_path):
        with open(aqi_path) as f:
            aqi_data = json.load(f)
        for param, records in aqi_data.items():
            for r in records:
                key = (r.get("state_code"), r.get("county_code"), r.get("site_number"))
                aqi_by_site.setdefault(key, []).append({
                    "param": param,
                    "date": r.get("date_local"),
                    "aqi": r.get("aqi", 0),
                    "mean": r.get("arithmetic_mean", 0),
                    "lat": r.get("latitude"),
                    "lon": r.get("longitude"),
                    "site_name": r.get("local_site_name", ""),
                })
    # Merge stations with peak AQI
    result = []
    seen = set()
    for rec_list in aqi_by_site.values():
        if not rec_list:
            continue
        r0 = rec_list[0]
        try:
            lat, lon = float(r0["lat"]), float(r0["lon"])
        except (TypeError, ValueError):
            continue
        if not (33.5 < lat < 34.9 and -119.0 < lon < -117.5):
            continue
        key = (round(lat, 3), round(lon, 3))
        if key in seen:
            continue
        seen.add(key)
        max_aqi = max((x["aqi"] or 0) for x in rec_list)
        params = list({x["param"] for x in rec_list})
        result.append({
            "lat": lat, "lon": lon,
            "site_name": r0["site_name"],
            "max_aqi": max_aqi,
            "params": ", ".join(params),
            "n_days": len(rec_list),
        })
    print(f"  AQS 空气质量站：{len(result)} 个")
    return result


def load_ejscreen():
    """加载 CalEnviroScreen 4.0 环境正义评分（Census Tract 质心）"""
    path = os.path.join(DATA_DIR, "ejscreen", "la_ejscreen_tracts.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    valid = []
    for d in data:
        try:
            lat = float(d["latitude"])
            lon = float(d["longitude"])
            ces = float(d.get("ces_40_score") or 0)
            valid.append({
                "lat": lat, "lon": lon,
                "ces_score": ces,
                "ces_pct": d.get("ces_40_percentile", ""),
                "pm25": d.get("pm2_5", ""),
                "poverty": d.get("poverty", ""),
                "tract": d.get("census_tract", ""),
                "pop": d.get("total_population", ""),
            })
        except (ValueError, TypeError, KeyError):
            pass
    print(f"  CalEnviroScreen tracts：{len(valid)} 个")
    return valid


def load_cdc_places():
    """加载 CDC PLACES 健康数据（Census Tract 质心需与 EJScreen 对齐）"""
    path = os.path.join(DATA_DIR, "cdc_places", "la_health_wide.json")
    ej_path = os.path.join(DATA_DIR, "ejscreen", "la_ejscreen_tracts.json")
    if not os.path.exists(path) or not os.path.exists(ej_path):
        return []
    with open(path) as f:
        health = json.load(f)
    with open(ej_path) as f:
        ej = json.load(f)
    # Build lat/lon lookup by census tract FIPS
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
            asthma = float(h.get("casthma") or 0)
            cancer = float(h.get("cancer") or 0)
            result.append({
                "lat": coords[0], "lon": coords[1],
                "asthma_pct": asthma,
                "cancer_pct": cancer,
                "diabetes_pct": float(h.get("diabetes") or 0),
                "copd_pct": float(h.get("copd") or 0),
                "tract": fips,
            })
        except (ValueError, TypeError):
            pass
    print(f"  CDC PLACES 健康数据：{len(result)} 个 tracts")
    return result


def load_violations():
    """加载 CA 饮用水违规记录"""
    path = os.path.join(DATA_DIR, "ca_open_data", "drinking_water_violations.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    print(f"  饮用水违规水系统：{len(data)} 个")
    return data


def load_superfund():
    """加载 EPA Superfund/SEMS 场地"""
    path = os.path.join(DATA_DIR, "epa_tri", "superfund_npl.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    print(f"  Superfund 场地：{len(data)} 个")
    return data


def load_usgs_gauges():
    """加载 USGS 水文监测站"""
    path = os.path.join(DATA_DIR, "usgs", "la_water_gauges.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    print(f"  USGS 水文站：{len(data)} 个")
    return data


def build_time_slider(results_df, stations_df):
    """构建 WQP 污染物月度时间滑块（TimestampedGeoJson）"""
    results_df = results_df.copy()
    results_df["month"] = results_df["date"].dt.to_period("M")
    monthly = (
        results_df.groupby(["station_id", "month"])
        .agg(avg_value=("value", "mean"), n_records=("value", "count"))
        .reset_index()
    )
    monthly["month_str"] = monthly["month"].astype(str)
    monthly = monthly.merge(
        stations_df[["station_id", "lat", "lon"]], on="station_id", how="inner"
    )
    p95 = monthly["avg_value"].quantile(0.95)
    monthly["norm"] = (monthly["avg_value"] / (p95 + 1e-9)).clip(0, 1)

    features = []
    for _, row in monthly.iterrows():
        n = float(row["norm"])
        # 蓝→黄→红渐变
        r = min(255, int(n * 2 * 255)) if n > 0.5 else 0
        g = min(255, int((1 - abs(n - 0.5) * 2) * 200))
        b = max(0, int((1 - n * 2) * 255)) if n < 0.5 else 0
        color = f"#{r:02x}{g:02x}{b:02x}"
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [row["lon"], row["lat"]]},
            "properties": {
                "time": row["month_str"] + "-01",
                "popup": (
                    f"<b>{row['station_id']}</b><br>"
                    f"月均值：{row['avg_value']:.4f}<br>"
                    f"检测数：{int(row['n_records'])}"
                ),
                "icon": "circle",
                "iconstyle": {
                    "fillColor": color,
                    "fillOpacity": 0.75,
                    "stroke": True,
                    "color": "#ffffff",
                    "weight": 1,
                    "radius": max(4, n * 16),
                },
            },
        })

    return TimestampedGeoJson(
        data={"type": "FeatureCollection", "features": features},
        period="P1M",
        duration="P1M",
        auto_play=False,
        loop=False,
        max_speed=3,
        loop_button=True,
        date_options="YYYY-MM",
        time_slider_drag_update=True,
        add_last_point=True,
    )


def export_geojson_layers(stations_df, results_df, fire_data, tri_data,
                          geo_data, school_data, gw_data, aqs_data,
                          ej_data, cdc_data, superfund, usgs_data):
    """把所有数据层导出为 GeoJSON，供 MapLibre HTML 加载"""
    import shutil, random
    out = os.path.join(OUT_DIR, "data")
    os.makedirs(out, exist_ok=True)

    def pt(lon, lat, props):
        return {"type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props}

    def save(name, features):
        path = os.path.join(out, f"{name}.geojson")
        with open(path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)
        print(f"    ✓ {name}.geojson  ({len(features)} features)")

    # 1. WQP 热力权重
    merged = results_df.merge(stations_df[["station_id","lat","lon"]], on="station_id", how="inner")
    latest = merged.sort_values("date").groupby("station_id").last().reset_index()
    if not latest.empty and latest["value"].max() > 0:
        mx = latest["value"].max(); mn = latest["value"].min()
        feats = [pt(float(r.lon), float(r.lat),
                    {"w": round((float(r.value)-mn)/(mx-mn+1e-9), 3),
                     "c": str(r.contaminant)})
                 for _, r in latest.iterrows() if pd.notna(r.lat)]
        save("wqp_heatmap", feats)

    # 2. TRI 工业设施
    save("tri", [pt(d["lon"],d["lat"],{"name":d["name"],"naics":str(d.get("naics",""))})
                 for d in tri_data])

    # 3. GeoTracker 污染地块
    save("geotracker", [pt(d["lon"],d["lat"],
                           {"name":d["name"],"case_type":d.get("case_type","")})
                        for d in geo_data])

    # 4. 学校铅采样
    save("schools", [pt(float(d["lon"]),float(d["lat"]),
                        {"name":str(d.get("school_name",""))[:50],
                         "max_ppb":float(d.get("max_lead_ppb",0) or 0),
                         "avg_ppb":float(d.get("avg_lead_ppb",0) or 0),
                         "exceed":bool(d.get("has_exceedance",False)),
                         "n":int(d.get("n_samples",0) or 0)})
                     for d in school_data if d.get("lat") and d.get("lon")])

    # 5. 地下水井（抽样 3000）
    gw_s = random.sample(gw_data, min(3000, len(gw_data)))
    save("groundwater", [pt(d["lon"],d["lat"],{"cat":d.get("category","")}) for d in gw_s])

    # 6. AQS 空气质量站
    save("aqs", [pt(d["lon"],d["lat"],
                    {"name":str(d.get("site_name","")),"aqi":int(d.get("max_aqi",0) or 0)})
                 for d in aqs_data])

    # 7. CalEnviroScreen
    save("ejscreen", [pt(d["lon"],d["lat"],
                         {"ces":float(d.get("ces_score",0) or 0),
                          "pov":str(d.get("poverty","")),
                          "tract":str(d.get("tract",""))})
                      for d in ej_data])

    # 8. CDC PLACES 健康数据
    save("cdc_places", [pt(d["lon"],d["lat"],
                           {"asthma":float(d.get("asthma_pct",0) or 0),
                            "cancer":float(d.get("cancer_pct",0) or 0)})
                        for d in cdc_data])

    # 9. Superfund
    save("superfund", [pt(float(d["lon"]),float(d["lat"]),
                          {"name":str(d.get("name",""))[:50],"city":str(d.get("city",""))})
                       for d in superfund
                       if d.get("lat") and d.get("lon")])

    # 10. USGS 水文站（抽样 2000）
    usgs_s = random.sample(usgs_data, min(2000, len(usgs_data)))
    save("usgs", [pt(float(d["lon"]),float(d["lat"]),
                     {"name":str(d.get("station_nm",""))[:50],
                      "type":str(d.get("site_type",""))})
                  for d in usgs_s])

    # 11. WQP 月度时间序列（给滑块用）
    m2 = results_df.merge(stations_df[["station_id","lat","lon"]], on="station_id", how="inner").copy()
    m2["month"] = m2["date"].dt.to_period("M")
    mo = (m2.groupby(["station_id","month"])
            .agg(avg=("value","mean"), n=("value","count"))
            .reset_index())
    mo["month_str"] = mo["month"].astype(str)
    mo = mo.merge(stations_df[["station_id","lat","lon"]], on="station_id", how="inner")
    p95 = mo["avg"].quantile(0.95)
    mo["norm"] = (mo["avg"] / (p95+1e-9)).clip(0,1)
    months = sorted(mo["month_str"].unique().tolist())
    feats = [{"type":"Feature",
              "geometry":{"type":"Point","coordinates":[float(r.lon),float(r.lat)]},
              "properties":{"month":r.month_str,"norm":round(float(r.norm),3),
                            "avg":round(float(r.avg),4),"n":int(r.n)}}
             for _, r in mo.iterrows()]
    with open(os.path.join(out,"wqp_monthly.json"),"w") as f:
        json.dump({"months":months,"features":feats}, f)
    print(f"    ✓ wqp_monthly.json  ({len(feats)} records, {len(months)} months)")

    # 12. 供水系统面板数据（拷贝）
    src = os.path.join(DATA_DIR,"water_system_boundaries","system_panel_data.json")
    if os.path.exists(src):
        shutil.copy(src, os.path.join(out,"system_panel.json"))
        print("    ✓ system_panel.json")

    # 13. 野火边界 + 供水系统边界（拷贝到 output/data，供 MapLibre 同域加载）
    for src_rel, dst_name in [
        (os.path.join(DATA_DIR,"fire_perimeters","la_2025_fires_calfire.geojson"), "fires.geojson"),
        (os.path.join(DATA_DIR,"water_system_boundaries","la_water_system_boundaries.geojson"), "water_systems.geojson"),
    ]:
        if os.path.exists(src_rel):
            shutil.copy(src_rel, os.path.join(out, dst_name))
            print(f"    ✓ {dst_name}")

    print(f"  所有图层已导出 → {out}/")


def load_ewg_systems():
    """加载 EWG 主要供水系统数据"""
    ewg_dir = os.path.join(DATA_DIR, "ewg")
    main_systems = ["ladwp", "burbank", "glendale", "pasadena", "long_beach", "santa_monica"]
    # 主要供水系统的大致坐标
    coords = {
        "ladwp":        (34.052, -118.243, "LA Department of Water & Power"),
        "burbank":      (34.181, -118.309, "City of Burbank"),
        "glendale":     (34.143, -118.255, "City of Glendale"),
        "pasadena":     (34.148, -118.144, "City of Pasadena"),
        "long_beach":   (33.770, -118.193, "City of Long Beach"),
        "santa_monica": (34.019, -118.491, "City of Santa Monica"),
    }
    systems = []
    for name in main_systems:
        path = os.path.join(ewg_dir, f"{name}.json")
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
            if name in coords:
                lat, lon, label = coords[name]
                systems.append({
                    "name": label,
                    "lat": lat, "lon": lon,
                    "contaminants": len(data.get("contaminants", [])),
                    "data": data,
                })
    return systems


# ─────────────────────────────────────────────
# 地图构建
# ─────────────────────────────────────────────

def build_map(contaminant_filter=None, after_fire=False):
    print("\n=== 构建交互式水质地图 ===\n")

    # 加载数据
    stations_df    = load_stations()
    results_df     = load_results(contaminant_filter, after_fire)
    fire_data      = load_fire_perimeters()
    tri_data       = load_tri_facilities()
    ewg_systems    = load_ewg_systems()
    geo_data       = load_geotracker()
    water_geojson  = load_water_boundaries()
    school_data    = load_school_lead()
    gw_data        = load_groundwater()
    aqs_data       = load_aqs_stations()
    ej_data        = load_ejscreen()
    cdc_data       = load_cdc_places()
    violations     = load_violations()
    superfund      = load_superfund()
    usgs_data      = load_usgs_gauges()

    # 加载供水系统面板数据（用于点击弹出）
    panel_data_path = os.path.join(DATA_DIR, "water_system_boundaries", "system_panel_data.json")
    try:
        with open(panel_data_path, "r") as _f:
            system_panel_text = _f.read()
    except Exception:
        system_panel_text = "{}"

    # ── 初始化地图 ──────────────────────────────────────────────────
    m = folium.Map(
        location=LA_CENTER,
        zoom_start=10,
        tiles=None,  # 手动添加底图
    )

    STADIA = "https://tiles.stadiamaps.com/tiles"
    STADIA_ATTR = ('Map tiles by <a href="http://stamen.com">Stamen Design</a> / '
                   '<a href="https://stadiamaps.com/">Stadia Maps</a>; '
                   'Data &copy; <a href="https://openstreetmap.org">OpenStreetMap</a>')

    # ── 默认：Stamen Watercolor（水彩手绘）
    folium.TileLayer(
        f"{STADIA}/stamen_watercolor/{{z}}/{{x}}/{{y}}.jpg",
        attr=STADIA_ATTR, name="🎨 Watercolor（水彩手绘）", control=True,
    ).add_to(m)

    # ── 其他 Stadia 风格
    folium.TileLayer(
        f"{STADIA}/alidade_smooth/{{z}}/{{x}}/{{y}}.png",
        attr=STADIA_ATTR, name="⬜ Alidade Smooth（冷灰极简）", control=True,
    ).add_to(m)

    folium.TileLayer(
        f"{STADIA}/alidade_smooth_dark/{{z}}/{{x}}/{{y}}.png",
        attr=STADIA_ATTR, name="⬛ Alidade Dark（深色极简）", control=True,
    ).add_to(m)

    folium.TileLayer(
        f"{STADIA}/stamen_toner_lite/{{z}}/{{x}}/{{y}}.png",
        attr=STADIA_ATTR, name="📰 Toner Lite（报纸黑白）", control=True,
    ).add_to(m)

    folium.TileLayer(
        f"{STADIA}/stamen_terrain/{{z}}/{{x}}/{{y}}.png",
        attr=STADIA_ATTR, name="🏔 Terrain（地形等高线）", control=True,
    ).add_to(m)

    folium.TileLayer(
        f"{STADIA}/outdoors/{{z}}/{{x}}/{{y}}.png",
        attr=STADIA_ATTR, name="🌿 Outdoors（户外绿色）", control=True,
    ).add_to(m)

    folium.TileLayer(
        f"{STADIA}/osm_bright/{{z}}/{{x}}/{{y}}.png",
        attr=STADIA_ATTR, name="🗺 OSM Bright（标准明亮）", control=True,
    ).add_to(m)

    # ── 非 Stadia 备用（国内访问）
    folium.TileLayer(
        "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        attr='&copy; CARTO', name="🟤 CartoDB Voyager（暖色）", control=True,
    ).add_to(m)

    folium.TileLayer(
        "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        attr='&copy; CARTO', name="⬜ CartoDB Positron（白底）", control=True,
    ).add_to(m)

    # ── 图层 0：供水系统服务区边界 ────────────
    if water_geojson:
        water_layer = folium.FeatureGroup(name="🔵 供水系统服务区边界", show=True)
        folium.GeoJson(
            water_geojson,
            style_function=lambda x: {
                "fillColor": "#00AA44",
                "color": "#007730",
                "weight": 2,
                "fillOpacity": 0.12,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["WATER_SYSTEM_NAME", "SABL_PWSID", "POPULATION"],
                aliases=["供水系统", "PWSID", "人口"],
            ),
        ).add_to(water_layer)
        water_layer.add_to(m)

    # ── 图层 1：野火边界 ──────────────────────
    if fire_data:
        fire_layer = folium.FeatureGroup(name="🔥 2025 野火边界（Palisades + Eaton）", show=True)
        folium.GeoJson(
            fire_data,
            style_function=lambda x: {
                "fillColor": "#FF4500",
                "color": "#CC2200",
                "weight": 3,
                "fillOpacity": 0.4,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["FIRE_NAME", "GIS_ACRES"],
                aliases=["火灾名称", "面积（英亩）"],
            ),
            popup=folium.GeoJsonPopup(
                fields=["FIRE_NAME", "GIS_ACRES"],
                aliases=["火灾名称", "面积（英亩）"],
            ),
        ).add_to(fire_layer)
        fire_layer.add_to(m)

    # ── 图层 2：污染物热力图 ──────────────────
    # 将检测结果与监测站坐标合并
    merged = results_df.merge(
        stations_df[["station_id", "lat", "lon"]],
        on="station_id", how="inner"
    )

    # 按站点取最新检测值（归一化后作热力权重）
    latest = merged.sort_values("date").groupby("station_id").last().reset_index()
    if not latest.empty and latest["value"].max() > 0:
        latest["weight"] = (latest["value"] - latest["value"].min()) / \
                           (latest["value"].max() - latest["value"].min() + 1e-9)
        heat_data = latest[["lat", "lon", "weight"]].dropna().values.tolist()

        heat_layer = folium.FeatureGroup(name="🌡 污染物热力图", show=True)
        HeatMap(
            heat_data,
            radius=15,
            blur=20,
            min_opacity=0.3,
            gradient={"0.2": "blue", "0.5": "yellow", "0.8": "orange", "1.0": "red"},
        ).add_to(heat_layer)
        heat_layer.add_to(m)

    # ── 图层 3：监测站（可点击）──────────────
    station_layer = folium.FeatureGroup(name="📍 WQP 监测站", show=False)
    cluster = MarkerCluster(
        options={"maxClusterRadius": 40, "disableClusteringAtZoom": 13}
    )

    # 为每个站点计算最新读数摘要
    station_summary = merged.groupby("station_id").agg(
        latest_date=("date", "max"),
        n_measurements=("value", "count"),
        n_contaminants=("contaminant", "nunique"),
        top_contaminant=("contaminant", lambda x: x.value_counts().index[0]),
    ).reset_index()

    stations_with_data = stations_df.merge(station_summary, on="station_id", how="left")

    for _, row in stations_with_data.iterrows():
        has_data = pd.notna(row.get("n_measurements"))
        color = "red" if has_data else "gray"
        popup_html = f"""
        <div style='font-family:sans-serif;min-width:180px'>
          <b>{str(row.get('station_name','未知站点'))[:40]}</b><br>
          <small>类型：{str(row.get('station_type',''))}</small><br>
          {'<hr>检测记录：' + str(int(row.get('n_measurements',0))) + ' 条<br>' +
           '污染物：' + str(int(row.get('n_contaminants',0))) + ' 种<br>' +
           '主要：' + str(row.get('top_contaminant',''))[:30] + '<br>' +
           '最新：' + str(row.get('latest_date',''))[:10]
           if has_data else '<i>暂无检测数据</i>'}
        </div>
        """
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=5 if has_data else 3,
            color=color,
            fill=True,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=str(row.get("station_name", ""))[:40],
        ).add_to(cluster)

    cluster.add_to(station_layer)
    station_layer.add_to(m)

    # ── 图层 4：TRI 工业排放设施 ─────────────
    if tri_data:
        tri_layer = folium.FeatureGroup(name="🏭 TRI 工业排放设施", show=False)
        tri_cluster = MarkerCluster(options={"maxClusterRadius": 40, "disableClusteringAtZoom": 14})
        for fac in tri_data:
            folium.CircleMarker(
                location=[fac["lat"], fac["lon"]],
                radius=4,
                color="#8B4513",
                fill=True,
                fill_color="#D2691E",
                fill_opacity=0.65,
                popup=folium.Popup(
                    f"<b>{fac['name']}</b><br>NAICS: {fac['naics']}<br><small>TRI 工业排放设施</small>",
                    max_width=200
                ),
                tooltip=fac["name"],
            ).add_to(tri_cluster)
        tri_cluster.add_to(tri_layer)
        tri_layer.add_to(m)

    # ── 图层 5：EWG 主要供水系统 ─────────────
    if ewg_systems:
        ewg_layer = folium.FeatureGroup(name="💧 主要供水系统（EWG）", show=True)
        for sys in ewg_systems:
            contaminants = sys["data"].get("contaminants", [])
            # 找出超标最严重的污染物
            above_guideline = [c for c in contaminants if c.get("times_above_guideline")]
            popup_html = f"""
            <div style='font-family:sans-serif;min-width:200px'>
              <b>{sys['name']}</b><br>
              <hr>
              检测污染物：{sys['contaminants']} 种<br>
              超 EWG 健康标准：{len(above_guideline)} 种<br>
              {'<br>'.join([
                  f"⚠ {c.get('name','')[:25]}: {c.get('times_above_guideline','')}"
                  for c in above_guideline[:5]
              ])}
            </div>
            """
            risk_color = "red" if len(above_guideline) > 5 else \
                         "orange" if len(above_guideline) > 2 else "blue"
            folium.Marker(
                location=[sys["lat"], sys["lon"]],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"💧 {sys['name']}",
                icon=folium.Icon(color=risk_color, icon="tint", prefix="fa"),
            ).add_to(ewg_layer)
        ewg_layer.add_to(m)

    # ── 图层 6：GeoTracker 地下污染地块（HeatMap 避免卡顿）────
    if geo_data:
        geo_layer = folium.FeatureGroup(name="☣ GeoTracker 污染地块（UST/LUST）", show=False)
        geo_heat = [[s["lat"], s["lon"], 0.6] for s in geo_data]
        HeatMap(
            geo_heat,
            radius=10,
            blur=12,
            min_opacity=0.25,
            gradient={"0.3": "#4B0082", "0.6": "#8B008B", "1.0": "#FF00FF"},
        ).add_to(geo_layer)
        geo_layer.add_to(m)

    # ── 图层 7：学校铅采样 ────────────────────
    if school_data:
        school_layer = folium.FeatureGroup(name="🏫 学校铅采样（EPA 行动水平 15 ppb）", show=False)
        school_cluster = MarkerCluster(options={"maxClusterRadius": 45, "disableClusteringAtZoom": 14})
        for s in school_data:
            exceed = s.get("has_exceedance", False)
            max_pb = s.get("max_lead_ppb", 0)
            color = "red" if exceed else ("orange" if max_pb > 5 else "green")
            popup_html = f"""
            <div style='font-family:sans-serif;min-width:200px'>
              <b>{s['school_name'][:45]}</b><br>
              <small>{s.get('district','')}</small><br><hr>
              采样数：{s['n_samples']} 次<br>
              最高铅浓度：<b>{max_pb} ppb</b><br>
              平均铅浓度：{s['avg_lead_ppb']} ppb<br>
              {'<b style="color:red">⚠ 超过 EPA 行动水平（15 ppb）</b>' if exceed else '✓ 未超标'}
            </div>
            """
            folium.CircleMarker(
                location=[s["lat"], s["lon"]],
                radius=6 if exceed else 4,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.75,
                popup=folium.Popup(popup_html, max_width=230),
                tooltip=f"🏫 {s['school_name'][:35]} | 最高 {max_pb} ppb",
            ).add_to(school_cluster)
        school_cluster.add_to(school_layer)
        school_layer.add_to(m)

    # ── 图层 8：地下水井（HeatMap）────────────
    if gw_data:
        gw_layer = folium.FeatureGroup(name="💧 地下水井分布", show=False)
        HeatMap(
            [[d["lat"], d["lon"], 0.5] for d in gw_data],
            radius=8, blur=10, min_opacity=0.2,
            gradient={"0.3": "#0000ff", "0.7": "#00aaff", "1.0": "#00ffff"},
        ).add_to(gw_layer)
        gw_layer.add_to(m)

    # ── 图层 9：AQS 空气质量监测站 ────────────
    if aqs_data:
        aqs_layer = folium.FeatureGroup(name="💨 AQS 空气质量监测站", show=False)
        for s in aqs_data:
            aqi = s["max_aqi"]
            color = "red" if aqi > 150 else "orange" if aqi > 100 else "green"
            folium.CircleMarker(
                location=[s["lat"], s["lon"]],
                radius=8,
                color=color, fill=True, fill_color=color, fill_opacity=0.8,
                popup=folium.Popup(
                    f"<b>{s['site_name']}</b><br>"
                    f"峰值 AQI：<b>{aqi}</b><br>"
                    f"参数：{s['params']}<br>"
                    f"观测天数：{s['n_days']} 天",
                    max_width=220
                ),
                tooltip=f"💨 {s['site_name']} | AQI峰值 {aqi}",
            ).add_to(aqs_layer)
        aqs_layer.add_to(m)

    # ── 图层 10：CalEnviroScreen 环境正义评分 ──
    if ej_data:
        ej_layer = folium.FeatureGroup(name="⚖ CalEnviroScreen 环境正义评分", show=False)
        max_ces = max(d["ces_score"] for d in ej_data if d["ces_score"]) or 1
        for d in ej_data:
            n = d["ces_score"] / max_ces
            r = min(255, int(n * 255))
            g = max(0, int((1 - n) * 180))
            folium.CircleMarker(
                location=[d["lat"], d["lon"]],
                radius=4,
                color=f"#{r:02x}{g:02x}00",
                fill=True,
                fill_color=f"#{r:02x}{g:02x}00",
                fill_opacity=0.55,
                popup=folium.Popup(
                    f"<b>Tract {d['tract']}</b><br>"
                    f"CES 4.0 评分：<b>{d['ces_score']:.1f}</b>（第 {d['ces_pct']} 百分位）<br>"
                    f"PM2.5：{d['pm25']}<br>贫困率：{d['poverty']}%<br>人口：{d['pop']}",
                    max_width=220
                ),
                tooltip=f"CES {d['ces_score']:.1f}",
            ).add_to(ej_layer)
        ej_layer.add_to(m)

    # ── 图层 11：CDC PLACES 健康数据（哮喘率）──
    if cdc_data:
        cdc_layer = folium.FeatureGroup(name="🏥 CDC PLACES 哮喘率（Census Tract）", show=False)
        max_asthma = max(d["asthma_pct"] for d in cdc_data if d["asthma_pct"]) or 1
        for d in cdc_data:
            n = d["asthma_pct"] / max_asthma
            folium.CircleMarker(
                location=[d["lat"], d["lon"]],
                radius=3,
                color=f"#{min(255,int(n*255)):02x}{max(0,int((1-n)*150)):02x}80",
                fill=True,
                fill_color=f"#{min(255,int(n*255)):02x}{max(0,int((1-n)*150)):02x}80",
                fill_opacity=0.5,
                popup=folium.Popup(
                    f"<b>Tract {d['tract']}</b><br>"
                    f"哮喘率：<b>{d['asthma_pct']}%</b><br>"
                    f"癌症率：{d['cancer_pct']}%<br>"
                    f"糖尿病：{d['diabetes_pct']}%<br>"
                    f"COPD：{d['copd_pct']}%",
                    max_width=200
                ),
                tooltip=f"哮喘 {d['asthma_pct']}%",
            ).add_to(cdc_layer)
        cdc_layer.add_to(m)

    # ── 图层 12：Superfund 污染场地 ────────────
    if superfund:
        sf_layer = folium.FeatureGroup(name="☢ EPA Superfund 污染场地", show=False)
        sf_cluster = MarkerCluster(options={"maxClusterRadius": 50, "disableClusteringAtZoom": 13})
        for s in superfund:
            folium.Marker(
                location=[s["lat"], s["lon"]],
                popup=folium.Popup(
                    f"<b>{s['name']}</b><br>{s['address']}, {s['city']}",
                    max_width=220
                ),
                tooltip=f"☢ {s['name'][:40]}",
                icon=folium.Icon(color="black", icon="warning-sign", prefix="glyphicon"),
            ).add_to(sf_cluster)
        sf_cluster.add_to(sf_layer)
        sf_layer.add_to(m)

    # ── 图层 13：USGS 水文站 ──────────────────
    if usgs_data:
        usgs_layer = folium.FeatureGroup(name="📊 USGS 水文监测站", show=False)
        usgs_cluster = MarkerCluster(options={"maxClusterRadius": 40, "disableClusteringAtZoom": 13})
        for s in usgs_data:
            folium.CircleMarker(
                location=[s["lat"], s["lon"]],
                radius=4,
                color="#1565C0", fill=True, fill_color="#42A5F5", fill_opacity=0.75,
                popup=folium.Popup(
                    f"<b>{s['station_nm']}</b><br>"
                    f"类型：{s['site_type']} | ID：{s['site_no']}",
                    max_width=220
                ),
                tooltip=f"📊 {s['station_nm'][:40]}",
            ).add_to(usgs_cluster)
        usgs_cluster.add_to(usgs_layer)
        usgs_layer.add_to(m)

    # ── 图层 14：WQP 污染物时间滑块（⏱ 月度动画）──
    print("  构建时间滑块（月度聚合）...")
    merged_for_time = results_df.merge(
        stations_df[["station_id", "lat", "lon"]], on="station_id", how="inner"
    )
    if not merged_for_time.empty:
        time_layer = build_time_slider(merged_for_time, stations_df)
        time_layer.add_to(m)

    # ── UI 组件 ────────────────────────────
    # 图层控制
    folium.LayerControl(collapsed=False, position="topright").add_to(m)

    # 全屏按钮
    Fullscreen(position="topleft").add_to(m)

    # 小地图
    MiniMap(toggle_display=True, position="bottomleft").add_to(m)

    # 标题
    title_html = f"""
    <div style="
        position: fixed; top: 10px; left: 50%; transform: translateX(-50%);
        z-index: 1000; background: rgba(255,255,255,0.92);
        padding: 8px 20px; border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        font-family: sans-serif; text-align: center;
    ">
        <b style="font-size:15px">🌊 LA Water Quality Root Cause Intelligence</b><br>
        <small style="color:#666">
            {'⚠ 显示野火后数据（2025-01-07+）' if after_fire else '数据覆盖：2020至今'} ·
            {'过滤：' + contaminant_filter if contaminant_filter else '全部污染物'} ·
            {len(stations_df)} 个监测站
        </small>
    </div>
    """
    m.get_root().html.add_child(folium.Element(title_html))

    # 图例
    legend_html = """
    <div style="
        position: fixed; bottom: 40px; right: 10px; z-index: 1000;
        background: rgba(255,255,255,0.92); padding: 10px 14px;
        border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        font-family: sans-serif; font-size: 12px;
    ">
        <b>图例</b><br>
        <span style="color:#007730">▬</span> 供水系统边界<br>
        <span style="color:#FF4500">▬</span> 野火边界<br>
        <span style="color:red">●</span> 有检测数据的监测站<br>
        <span style="color:gray">●</span> 无数据监测站<br>
        <span style="color:#8B4513">●</span> TRI 工业排放设施<br>
        <span style="color:#8B008B">▓</span> GeoTracker 污染热力（紫）<br>
        <span style="color:green">●</span> 学校铅采样（达标）<br>
        <span style="color:red">●</span> 学校铅采样（超标）<br>
        <span style="color:#00aaff">▓</span> 地下水井热力<br>
        <span style="color:green">●</span> AQS 空气站（AQI 良好）<br>
        <span style="color:red">●</span> AQS 空气站（AQI 危险）<br>
        <span style="color:#aa6600">●</span> CalEnviroScreen（高→低）<br>
        <span style="color:black">⚠</span> Superfund 场地<br>
        <span style="color:#1565C0">●</span> USGS 水文站<br>
        <span style="color:#555">⏱</span> 底部滑块 = WQP 月度动画<br>
        <span style="color:blue">📍</span> 供水系统（低风险）<br>
        <span style="color:orange">📍</span> 供水系统（中风险）<br>
        <span style="color:red">📍</span> 供水系统（高风险）
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # ── NYT 风格点击面板 ────────────────────────────────────────────
    map_name = m.get_name()

    panel_css = """
    <style>
    #sys-panel {
        position: fixed; top: 62px; right: 375px;
        width: 290px; z-index: 1002;
        background: #fff; display: none;
        font-family: Georgia, 'Times New Roman', serif;
        border: 1px solid #ccc; border-radius: 3px;
        box-shadow: 0 6px 24px rgba(0,0,0,0.18);
        overflow: hidden;
    }
    #sys-panel-header {
        background: #1a1a1a; color: #fff;
        padding: 10px 32px 10px 14px;
        font-size: 10px; letter-spacing: 2px;
        text-transform: uppercase;
        font-family: Arial, sans-serif;
        position: relative;
    }
    #sys-panel-close {
        position: absolute; top: 8px; right: 12px;
        color: #aaa; cursor: pointer;
        font-size: 17px; line-height: 1;
        font-family: Arial, sans-serif;
    }
    #sys-panel-close:hover { color: #fff; }
    #sys-panel-body { padding: 14px 16px 16px; }
    #sys-panel-name {
        font-size: 15px; font-weight: bold;
        color: #111; margin-bottom: 3px; line-height: 1.35;
    }
    #sys-panel-city {
        font-size: 11px; color: #888; margin-bottom: 12px;
        font-family: Arial, sans-serif;
    }
    .sys-stat {
        display: flex; justify-content: space-between;
        border-bottom: 1px solid #f0f0f0; padding: 5px 0;
        font-family: Arial, sans-serif; font-size: 12px;
    }
    .sys-stat-label { color: #666; }
    .sys-stat-value { font-weight: bold; color: #111; }
    #sys-panel-risk {
        margin-top: 11px; padding: 7px 10px;
        text-align: center; font-family: Arial, sans-serif;
        font-size: 11px; font-weight: bold;
        letter-spacing: 1px; text-transform: uppercase;
        border-radius: 3px;
    }
    #sys-panel-narrative {
        margin-top: 10px; font-size: 12.5px;
        color: #444; line-height: 1.65;
        font-family: Georgia, serif; font-style: italic;
    }
    </style>
    """
    m.get_root().header.add_child(folium.Element(panel_css))

    panel_div = """
    <div id="sys-panel">
        <div id="sys-panel-header">
            Water System Profile
            <span id="sys-panel-close" onclick="document.getElementById('sys-panel').style.display='none'">&times;</span>
        </div>
        <div id="sys-panel-body">
            <div id="sys-panel-name">—</div>
            <div id="sys-panel-city">—</div>
            <div id="sys-stats"></div>
            <div id="sys-panel-risk"></div>
            <div id="sys-panel-narrative"></div>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(panel_div))

    panel_script = """
    <script>
    var systemPanelData = {panel_data};
    var riskColors = {{"Low":"#27ae60","Moderate":"#f39c12","High":"#e67e22","Critical":"#c0392b"}};

    function _fmt(v) {{
        if (v === null || v === undefined || (typeof v === 'number' && isNaN(v))) return '\u2014';
        if (typeof v === 'number') {{
            if (v >= 1000000) return (v/1000000).toFixed(1)+'M';
            if (v >= 1000)    return v.toLocaleString();
            return v % 1 === 0 ? v : v.toFixed(1);
        }}
        return v;
    }}

    function showSystemPanel(props) {{
        var pwsid = props.SABL_PWSID;
        var d = systemPanelData[pwsid] || {{}};
        var name  = d.name  || props.WATER_SYSTEM_NAME || pwsid || '\u2014';
        var city  = d.city  || '\u2014';
        var ot    = (d.owner_type||'').trim();
        var ownerLabel = ot==='L'?'Public utility': ot==='P'?'Private utility': ot==='S'?'State/Federal':'Other';
        var pop   = _fmt(d.population);
        var conn  = _fmt(d.connections);
        var viols = (d.violations !== undefined && d.violations !== null) ? d.violations : '\u2014';
        var violColor = (typeof viols === 'number' && viols > 0) ? '#c0392b' : '#27ae60';
        var ces   = (typeof d.ces_score==='number' && !isNaN(d.ces_score)) ? d.ces_score.toFixed(1) : '\u2014';
        var cesPct= (typeof d.ces_pct  ==='number' && !isNaN(d.ces_pct))   ? d.ces_pct.toFixed(0)  : '\u2014';
        var pov   = (typeof d.poverty  ==='number' && !isNaN(d.poverty))   ? d.poverty.toFixed(1)+'%' : '\u2014';
        var asth  = (typeof d.asthma   ==='number' && !isNaN(d.asthma))    ? d.asthma.toFixed(1)   : '\u2014';
        var risk  = d.risk || 'Unknown';
        var col   = riskColors[risk] || '#888';

        document.getElementById('sys-panel-name').textContent = name;
        document.getElementById('sys-panel-city').textContent = city + '  \u00b7  ' + ownerLabel;

        document.getElementById('sys-stats').innerHTML =
            '<div class="sys-stat"><span class="sys-stat-label">Population served</span><span class="sys-stat-value">'+pop+'</span></div>'+
            '<div class="sys-stat"><span class="sys-stat-label">Service connections</span><span class="sys-stat-value">'+conn+'</span></div>'+
            '<div class="sys-stat"><span class="sys-stat-label">Violations on record</span><span class="sys-stat-value" style="color:'+violColor+'">'+viols+'</span></div>'+
            '<div class="sys-stat"><span class="sys-stat-label">CalEnviroScreen score</span><span class="sys-stat-value">'+ces+' ('+cesPct+'th pct.)</span></div>'+
            '<div class="sys-stat"><span class="sys-stat-label">Poverty rate</span><span class="sys-stat-value">'+pov+'</span></div>'+
            '<div class="sys-stat"><span class="sys-stat-label">Asthma rate (per 10k)</span><span class="sys-stat-value">'+asth+'</span></div>';

        var riskEl = document.getElementById('sys-panel-risk');
        riskEl.textContent = '\u26a0 Risk Level: '+risk;
        riskEl.style.cssText = 'background:'+col+'22;color:'+col+';border:1px solid '+col+'55;'+
            'margin-top:11px;padding:7px 10px;text-align:center;font-family:Arial,sans-serif;'+
            'font-size:11px;font-weight:bold;letter-spacing:1px;text-transform:uppercase;border-radius:3px;';

        var narrative = name+' serves '+pop+' people in '+city+'. ';
        if (typeof viols==='number' && viols>0) narrative += viols+' regulatory violation'+(viols>1?'s':'')+' on record. ';
        if (cesPct!=='\u2014') narrative += 'CalEnviroScreen ranks this area at the '+cesPct+'th percentile statewide. ';
        if (pov!=='\u2014') narrative += 'About '+pov+' of residents live below the poverty line.';
        document.getElementById('sys-panel-narrative').textContent = narrative;

        document.getElementById('sys-panel').style.display = 'block';
    }}

    // Attach click handlers to water boundary polygons after Leaflet initializes
    setTimeout(function() {{
        {map_name}.eachLayer(function(layer) {{
            if (!layer._layers) return;
            Object.values(layer._layers).forEach(function(sub) {{
                var targets = sub._layers ? Object.values(sub._layers) : [sub];
                targets.forEach(function(l) {{
                    if (l.feature && l.feature.properties && l.feature.properties.SABL_PWSID) {{
                        l.on('click', function(e) {{
                            L.DomEvent.stopPropagation(e);
                            showSystemPanel(e.target.feature.properties);
                        }});
                        l.on('mouseover', function(e) {{
                            e.target.setStyle({{weight:3, color:'#005522', fillOpacity:0.28}});
                        }});
                        l.on('mouseout', function(e) {{
                            e.target.setStyle({{weight:2, color:'#007730', fillOpacity:0.12}});
                        }});
                    }}
                }});
            }});
        }});
    }}, 800);
    </script>
    """.format(panel_data=system_panel_text, map_name=map_name)

    m.get_root().html.add_child(folium.Element(panel_script))

    # ── 导出 GeoJSON 供 MapLibre 版使用 ────
    print("\n  导出 GeoJSON 图层...")
    export_geojson_layers(stations_df, results_df, fire_data, tri_data,
                          geo_data, school_data, gw_data, aqs_data,
                          ej_data, cdc_data, superfund, usgs_data)

    # ── 输出 ───────────────────────────────
    suffix = ""
    if contaminant_filter:
        suffix += f"_{contaminant_filter.lower().replace(' ', '_')}"
    if after_fire:
        suffix += "_post_fire"
    out_path = os.path.join(OUT_DIR, f"water_quality_map{suffix}.html")
    m.save(out_path)
    print(f"\n✓ 地图已保存：{out_path}")
    print(f"  在浏览器中打开：file://{os.path.abspath(out_path)}")
    return out_path


# ─────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="构建 LA 水质交互地图")
    parser.add_argument("--contaminant", "-c", type=str, default=None,
                        help="过滤污染物（如 'Lead', 'Benzene', 'Turbidity'）")
    parser.add_argument("--after-fire", "-f", action="store_true",
                        help="只显示 2025-01-07 野火后数据")
    args = parser.parse_args()

    build_map(
        contaminant_filter=args.contaminant,
        after_fire=args.after_fire,
    )
