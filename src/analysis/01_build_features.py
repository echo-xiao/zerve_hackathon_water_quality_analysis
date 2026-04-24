"""
01_build_features.py — 构建供水系统级特征宽表

分析单元：LA County 213 个供水系统（居民实际饮用水来源）

目标变量（因变量）：
  ewg_score — EWG 超标综合评分
              = mean(times_above_guideline) across all contaminants
              数值越高 = 该供水系统污染物超出健康标准越严重

特征（自变量）：
  系统自身属性（来自 water_systems.geojson）：
    population          — 服务人口规模
    service_connections — 服务连接数
    is_public           — 公营(1) vs 私营(0)

  空间聚合特征（系统服务区内的污染源密度）：
    ces_score           — CalEnviroScreen 综合环境评分均值
    poverty_pct         — 贫困率均值
    tri_count           — TRI 工业排放设施数量
    superfund_count     — Superfund 污染场地数量
    pfas_factor         — PFAS 最大超标倍数
    pesticide_lbs       — 农药使用总量
    geotracker_count    — GeoTracker 地下污染清理点数量
    school_lead_max     — 系统内学校铅含量最大值 (ppb)
    fire_dist_km        — 距 2025 野火边界最近距离

输入：
  output/data/water_systems.geojson
  data/raw_data/ewg/CA*.json
  output/data/*.geojson（各污染源层）

输出：
  output/data/system_features.csv

运行：
  python src/analysis/01_build_features.py
"""

import os, json, glob, re
import numpy as np
import pandas as pd
import geopandas as gpd

ROOT     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(ROOT, "data", "raw_data")
OUT_DATA = os.path.join(ROOT, "output", "data")

# ── 1. 加载供水系统边界 ────────────────────────────────────────────────────────
print("加载供水系统边界...")
ws_gdf = gpd.read_file(os.path.join(OUT_DATA, "water_systems.geojson")).to_crs("EPSG:4326")
ws_gdf["pwsid"] = ws_gdf["SABL_PWSID"].astype(str)

# 计算服务区面积（km²），投影到 UTM 以保证精度
ws_utm = ws_gdf.to_crs("EPSG:32611")
ws_gdf["area_km2"] = (ws_utm.geometry.area / 1e6).clip(lower=0.01)  # 最小 0.01 km² 避免除零

# 系统自身属性
ws_gdf["population"]          = pd.to_numeric(ws_gdf["POPULATION"], errors="coerce").fillna(0)
ws_gdf["service_connections"] = pd.to_numeric(ws_gdf["SERVICE_CONNECTIONS"], errors="coerce").fillna(0)
ws_gdf["is_public"]           = (ws_gdf["OWNER_TYPE_CODE"].astype(str).str.upper() == "L").astype(int)

df = ws_gdf[["pwsid","WATER_SYSTEM_NAME","population","service_connections","is_public","area_km2"]].copy()
df = df.rename(columns={"WATER_SYSTEM_NAME": "system_name"})
print(f"  {len(df)} 个供水系统，面积范围：{df['area_km2'].min():.2f}–{df['area_km2'].max():.1f} km²")

# ── 2. 目标变量：EWG 超标评分 ──────────────────────────────────────────────────
print("构建 EWG 超标评分（目标变量）...")

def parse_times(val):
    """'572x' → 572.0，失败返回 None"""
    if not val:
        return None
    m = re.search(r"([\d,]+\.?\d*)", str(val).replace(",", ""))
    return float(m.group(1)) if m else None

ewg_files = glob.glob(os.path.join(DATA_DIR, "ewg", "CA*.json"))
ewg_scores = {}
ewg_n_contaminants = {}
ewg_worst_contaminant = {}
ewg_max_times = {}

for path in ewg_files:
    with open(path) as f:
        d = json.load(f)
    pwsid = d.get("pwsid", "")
    conts = d.get("contaminants", [])
    times_vals = [parse_times(c.get("times_above_guideline")) for c in conts]
    times_vals = [v for v in times_vals if v is not None]
    if times_vals:
        ewg_scores[pwsid]          = float(np.mean(times_vals))
        ewg_n_contaminants[pwsid]  = len(conts)
        ewg_max_times[pwsid]       = float(np.max(times_vals))
        idx = np.argmax([parse_times(c.get("times_above_guideline")) or 0 for c in conts])
        ewg_worst_contaminant[pwsid] = conts[idx].get("name", "")

df["ewg_score"]          = df["pwsid"].map(ewg_scores)
df["n_contaminants"]     = df["pwsid"].map(ewg_n_contaminants)
df["max_times_above"]    = df["pwsid"].map(ewg_max_times)
df["worst_contaminant"]  = df["pwsid"].map(ewg_worst_contaminant)
df = df.dropna(subset=["ewg_score"])
print(f"  {len(df)} 个系统有 EWG 数据，ewg_score 均值={df['ewg_score'].mean():.1f}x")

# 只保留有数据的系统边界
ws_gdf = ws_gdf[ws_gdf["pwsid"].isin(df["pwsid"])].copy()

# ── 辅助：点 GeoJSON 加载 ─────────────────────────────────────────────────────
def load_points(filename):
    path = os.path.join(OUT_DATA, filename)
    if not os.path.exists(path):
        print(f"  ⚠ 未找到 {filename}，跳过")
        return None
    with open(path) as f:
        fc = json.load(f)
    rows = []
    for feat in fc["features"]:
        props = feat["properties"].copy()
        lon, lat = feat["geometry"]["coordinates"]
        props["_lon"], props["_lat"] = lon, lat
        rows.append(props)
    if not rows:
        return None
    return gpd.GeoDataFrame(rows,
        geometry=gpd.points_from_xy([r["_lon"] for r in rows], [r["_lat"] for r in rows]),
        crs="EPSG:4326")

def _pwsid_col(joined):
    """sjoin 后 pwsid 可能叫 pwsid_right，统一返回正确列名"""
    for c in ["pwsid", "pwsid_right"]:
        if c in joined.columns:
            return c
    raise KeyError("pwsid column not found after sjoin")

def count_in_system(points_gdf, col_name):
    """统计每个供水系统内的点数量"""
    if points_gdf is None:
        return pd.DataFrame({"pwsid": [], col_name: []})
    pts = points_gdf.copy()
    pts["_cnt"] = 1
    joined = gpd.sjoin(pts, ws_gdf[["pwsid","geometry"]], predicate="within", how="left")
    pc = _pwsid_col(joined)
    agg = joined.dropna(subset=[pc]).groupby(pc)["_cnt"].sum().reset_index()
    agg.columns = ["pwsid", col_name]
    return agg

def agg_in_system(points_gdf, value_col, col_name, agg_func="mean"):
    """对供水系统内的点数据做聚合"""
    if points_gdf is None or value_col not in points_gdf.columns:
        return pd.DataFrame({"pwsid": [], col_name: []})
    joined = gpd.sjoin(points_gdf, ws_gdf[["pwsid","geometry"]], predicate="within", how="left")
    pc = _pwsid_col(joined)
    joined = joined.dropna(subset=[pc])
    agg = joined.groupby(pc)[value_col].agg(agg_func).reset_index()
    agg.columns = ["pwsid", col_name]
    return agg

# ── 3. CalEnviroScreen 评分 + 贫困率 ─────────────────────────────────────────
print("提取 CalEnviroScreen 特征...")
ej_gdf = load_points("ejscreen.geojson")
if ej_gdf is not None:
    ej_gdf["ces_score"]   = pd.to_numeric(ej_gdf["ces"], errors="coerce").fillna(0)
    ej_gdf["poverty_pct"] = ej_gdf["pov"].astype(str).str.replace("%","").apply(
        lambda x: float(x) if x.replace(".","").replace("-","").isdigit() else 0)
    df = df.merge(agg_in_system(ej_gdf, "ces_score",   "ces_score",   "mean"), on="pwsid", how="left")
    df = df.merge(agg_in_system(ej_gdf, "poverty_pct", "poverty_pct", "mean"), on="pwsid", how="left")
    print(f"  CES 均值={df['ces_score'].mean():.1f}")

# ── 4. TRI 工业设施 ───────────────────────────────────────────────────────────
print("提取 TRI 特征...")
df = df.merge(count_in_system(load_points("tri.geojson"), "tri_count"), on="pwsid", how="left")

# ── 5. Superfund 场地 ─────────────────────────────────────────────────────────
print("提取 Superfund 特征...")
df = df.merge(count_in_system(load_points("superfund.geojson"), "superfund_count"), on="pwsid", how="left")

# ── 6. PFAS 污染强度 ──────────────────────────────────────────────────────────
print("提取 PFAS 特征...")
pfas_gdf = load_points("pfas.geojson")
if pfas_gdf is not None:
    pfas_gdf["pfas_factor"] = pd.to_numeric(pfas_gdf.get("worst_factor", 0), errors="coerce").fillna(0)
    df = df.merge(agg_in_system(pfas_gdf, "pfas_factor", "pfas_factor", "max"), on="pwsid", how="left")

# ── 7. 农药使用量 ─────────────────────────────────────────────────────────────
print("提取农药特征...")
pest_gdf = load_points("pesticide.geojson")
if pest_gdf is not None:
    pest_gdf["lbs_used"] = pd.to_numeric(pest_gdf.get("lbs_used", 0), errors="coerce").fillna(0)
    df = df.merge(agg_in_system(pest_gdf, "lbs_used", "pesticide_lbs", "sum"), on="pwsid", how="left")

# ── 8. GeoTracker 污染地块 ────────────────────────────────────────────────────
print("提取 GeoTracker 特征...")
df = df.merge(count_in_system(load_points("geotracker.geojson"), "geotracker_count"), on="pwsid", how="left")

# ── 9. 学校铅含量最大值 ───────────────────────────────────────────────────────
print("提取学校铅含量特征...")
school_gdf = load_points("schools.geojson")
if school_gdf is not None:
    school_gdf["max_ppb"] = pd.to_numeric(school_gdf.get("max_ppb", 0), errors="coerce").fillna(0)
    df = df.merge(agg_in_system(school_gdf, "max_ppb", "school_lead_max", "max"), on="pwsid", how="left")

# ── 10. WQP 水源水质（地下水井 + 地表水站均值）────────────────────────────────
print("提取 WQP 水源水质特征...")
wqp_path = os.path.join(OUT_DATA, "wqp_stations.json")
if os.path.exists(wqp_path):
    with open(wqp_path) as f:
        wqp = json.load(f)

    coords    = wqp.get("station_coords", {})
    st_types  = wqp.get("station_type", {})
    data      = wqp.get("data", {})

    # 只保留地下水井和地表水站，排除海洋/海滩/配水系统
    VALID_TYPES = {"Well","River/Stream","Stream","River/Stream Perennial",
                   "River/stream Effluent-Dominated","Lake, Reservoir, Impoundment",
                   "Other-Surface Water","Spring"}
    valid_sids = {sid for sid,t in st_types.items() if t in VALID_TYPES}

    # 重点重金属/营养盐污染物
    TARGET_CONTS = ["砷 Arsenic","铅 Lead","铬 Chromium","硝酸盐 Nitrate","溶解固体 TDS"]

    # 每个站点计算各污染物全时段均值
    station_means = {}   # {sid: {cont: mean_val}}
    for cont in TARGET_CONTS:
        if cont not in data:
            continue
        for month, stations in data[cont].items():
            for sid, val in stations.items():
                if sid not in valid_sids:
                    continue
                station_means.setdefault(sid, {}).setdefault(cont, []).append(val)

    # 转为 GeoDataFrame
    rows = []
    for sid in station_means:
        if sid not in coords:
            continue
        lon, lat = coords[sid]
        row = {"_sid": sid, "_lon": lon, "_lat": lat}
        for cont, vals in station_means[sid].items():
            row[cont] = float(np.mean(vals))
        rows.append(row)

    if rows:
        wqp_gdf = gpd.GeoDataFrame(rows,
            geometry=gpd.points_from_xy([r["_lon"] for r in rows], [r["_lat"] for r in rows]),
            crs="EPSG:4326")

        # 每个供水系统内站点的均值
        for cont in TARGET_CONTS:
            if cont not in wqp_gdf.columns:
                continue
            col_name = "wqp_" + cont.split()[0].lower()   # e.g. wqp_arsenic
            df = df.merge(agg_in_system(wqp_gdf, cont, col_name, "mean"),
                          on="pwsid", how="left")
        print(f"  WQP 有效站点：{len(wqp_gdf)}，污染物：{[c for c in TARGET_CONTS if c in wqp_gdf.columns]}")
    else:
        print("  ⚠ 无有效 WQP 站点")
else:
    print("  ⚠ wqp_stations.json 未找到，跳过")

# ── 11（原10）. 距野火边界距离 (km) ──────────────────────────────────────────
print("计算距野火距离...")
fire_path = os.path.join(OUT_DATA, "fires_simple.geojson")
if os.path.exists(fire_path):
    fire_gdf  = gpd.read_file(fire_path).to_crs("EPSG:32611")
    ws_utm    = ws_gdf.to_crs("EPSG:32611")
    centroids = ws_utm.geometry.centroid
    fire_union = (fire_gdf.geometry.union_all()
                  if hasattr(fire_gdf.geometry, "union_all")
                  else fire_gdf.geometry.unary_union)
    dists = centroids.apply(
        lambda pt: 0.0 if fire_union.contains(pt) else fire_union.distance(pt) / 1000)
    ws_gdf["fire_dist_km"] = dists.round(3).values
    df = df.merge(ws_gdf[["pwsid","fire_dist_km"]], on="pwsid", how="left")
    print(f"  距野火距离：min={df['fire_dist_km'].min():.1f}km, max={df['fire_dist_km'].max():.1f}km")

# ── 11. 水源类型（EPA SDWIS）────────────────────────────────────────────────
print("合并水源类型...")
src_path = os.path.join(DATA_DIR, "sdwis", "source_type.csv")
if os.path.exists(src_path):
    src_df = pd.read_csv(src_path)[["pwsid","primary_source_code","source_label"]]
    df = df.merge(src_df, on="pwsid", how="left")
    # 是否为进口水（purchased/surface water from elsewhere）
    # SW 在 LA County 实际为进口水（LA 无本地地表水，SW 即 LADWP 等进口水系统）
    df["is_imported"] = df["primary_source_code"].isin(["SWP","GWP","SW"]).astype(int)
    # 是否为地下水（本地来源，受本地污染影响最大）
    df["is_groundwater"] = df["primary_source_code"].isin(["GW","GU"]).astype(int)
    print(f"  水源类型分布：\n{df['primary_source_code'].value_counts().to_string()}")
else:
    print("  ⚠ 水源类型数据未找到，跳过")
    df["is_imported"]    = 0
    df["is_groundwater"] = 0
    df["source_label"]   = "未知"

# ── 12. 面积归一化（计数类特征 / km²，农药总量 / km²）───────────────────────
print("面积归一化...")
# area_km2 已在步骤1随 ws_gdf 带入 df
if "area_km2" not in df.columns:
    df = df.merge(ws_gdf[["pwsid","area_km2"]], on="pwsid", how="left")
df["area_km2"] = df["area_km2"].fillna(df["area_km2"].median())

for col in ["tri_count","superfund_count","geotracker_count"]:
    if col in df.columns:
        df[f"{col}_per_km2"] = df[col] / df["area_km2"]

if "pesticide_lbs" in df.columns:
    df["pesticide_per_km2"] = df["pesticide_lbs"] / df["area_km2"]

# ── 13. SAFER 违规分 + 地下水超采分 ──────────────────────────────────────────
print("提取 SAFER 违规评分 + 地下水超采评分...")
safer_path = os.path.join(DATA_DIR, "epa_echo", "violations.json")
if os.path.exists(safer_path):
    with open(safer_path) as f:
        safer = json.load(f)
    safer_map = {r.get("WATER_SYSTEM_NUMBER","").strip(): r for r in safer}
    def _safe_num(val):
        try: return float(val)
        except: return 0.0
    df["violation_score"] = df["pwsid"].map(lambda p: (
        _safe_num(safer_map.get(p,{}).get("MONITORING_AND_REPORTING_VIOLATIONS_SCORE",0)) +
        _safe_num(safer_map.get(p,{}).get("TREATMENT_TECHNIUQE_VIOLATIONS_RAW_SCORE",0)) +
        _safe_num(safer_map.get(p,{}).get("OPERATOR_CERTIFICATION_VIOLATIONS_RAW_SCORE",0))
    ))
    df["gw_overdraft"] = df["pwsid"].map(lambda p:
        _safe_num(safer_map.get(p,{}).get("CRITICALLY_OVERDRAFTED_GROUNDWATER_BASIN_RAW_SCORE",0))
    )
    print(f"  violation_score 非零: {(df['violation_score']>0).sum()} 个系统")
    print(f"  gw_overdraft 非零:    {(df['gw_overdraft']>0).sum()} 个系统")
else:
    print("  ⚠ SAFER 数据未找到，跳过")
    df["violation_score"] = 0.0
    df["gw_overdraft"]    = 0.0

# ── 14. 铅管风险代理变量（CES lead 字段 + 住房年代）──────────────────────────
print("提取铅管风险代理变量...")
ces_path = os.path.join(DATA_DIR, "ejscreen", "la_ejscreen_tracts.json")
if os.path.exists(ces_path):
    with open(ces_path) as f:
        ces_raw = json.load(f)
    ces_df = pd.DataFrame(ces_raw)
    ces_df["lon"] = pd.to_numeric(ces_df.get("longitude", pd.Series()), errors="coerce")
    ces_df["lat"] = pd.to_numeric(ces_df.get("latitude",  pd.Series()), errors="coerce")
    ces_df["lead_score"] = pd.to_numeric(ces_df.get("lead", pd.Series()), errors="coerce")
    ces_df["housing_burden"] = pd.to_numeric(ces_df.get("housing_burden", pd.Series()), errors="coerce")
    ces_df = ces_df.dropna(subset=["lon","lat","lead_score"])
    ces_gdf = gpd.GeoDataFrame(
        ces_df, geometry=gpd.points_from_xy(ces_df["lon"], ces_df["lat"]), crs="EPSG:4326"
    )
    ws_4326 = ws_gdf.to_crs("EPSG:4326")
    joined_ces = gpd.sjoin(ces_gdf, ws_4326[["pwsid","geometry"]], how="inner", predicate="within")
    lead_agg = joined_ces.groupby("pwsid").agg(
        lead_score=("lead_score","mean"),
        housing_burden=("housing_burden","mean"),
    ).reset_index()
    df = df.merge(lead_agg, on="pwsid", how="left")
    print(f"  lead_score 覆盖: {df['lead_score'].notna().sum()} 个系统")
else:
    print("  ⚠ CES 数据未找到，跳过")
    df["lead_score"]    = 0.0
    df["housing_burden"] = 0.0

# ── 15. 水处理工艺（EPA SDWIS treatment table）──────────────────────────────
print("提取水处理工艺特征...")
treatment_path = os.path.join(DATA_DIR, "sdwis", "treatment.csv")
if os.path.exists(treatment_path):
    tr = pd.read_csv(treatment_path)
    # 高级处理：反渗透/纳滤/离子交换/活性炭 → 能去除砷/PFAS/硝酸盐
    ADVANCED = ["RO","NF","IX","GAC","BAC","OZ"]   # treatment_process_code
    # 软化：能去除部分重金属
    SOFTEN   = ["LI","LS"]
    if "treatment_process_code" in tr.columns and "pwsid" in tr.columns:
        tr["has_advanced"] = tr["treatment_process_code"].str.upper().isin(ADVANCED).astype(int)
        tr["has_softening"] = tr["treatment_process_code"].str.upper().isin(SOFTEN).astype(int)
        tr_agg = tr.groupby("pwsid").agg(
            has_advanced_treatment=("has_advanced","max"),
            has_softening=("has_softening","max"),
            n_treatment_steps=("treatment_process_code","count"),
        ).reset_index()
        df = df.merge(tr_agg, on="pwsid", how="left")
        df["has_advanced_treatment"] = df["has_advanced_treatment"].fillna(0)
        df["has_softening"]          = df["has_softening"].fillna(0)
        df["n_treatment_steps"]      = df["n_treatment_steps"].fillna(0)
        print(f"  高级处理系统: {(df['has_advanced_treatment']>0).sum()} 个")
        print(f"  软化处理系统: {(df['has_softening']>0).sum()} 个")
    else:
        print("  ⚠ treatment.csv 字段不符，跳过")
        df["has_advanced_treatment"] = 0.0
        df["has_softening"]          = 0.0
        df["n_treatment_steps"]      = 0.0
else:
    print("  ⚠ treatment.csv 未找到，跳过")
    df["has_advanced_treatment"] = 0.0
    df["has_softening"]          = 0.0
    df["n_treatment_steps"]      = 0.0

# ── 16. 铅管存量（CA Water Boards LSL inventory）────────────────────────────
print("提取铅管存量数据...")
lsl_path = os.path.join(DATA_DIR, "sdwis", "lsl_inventory.csv")
if os.path.exists(lsl_path):
    lsl = pd.read_csv(lsl_path)
    # 找 pwsid 列和铅管比例列
    pwsid_col = next((c for c in lsl.columns if "pwsid" in c.lower()), None)
    lead_col  = next((c for c in lsl.columns if "lead" in c.lower() and "pct" in c.lower()), None)
    if pwsid_col and lead_col:
        lsl = lsl[[pwsid_col, lead_col]].rename(columns={pwsid_col:"pwsid", lead_col:"lsl_pct"})
        lsl["lsl_pct"] = pd.to_numeric(lsl["lsl_pct"], errors="coerce").fillna(0)
        df = df.merge(lsl, on="pwsid", how="left")
        df["lsl_pct"] = df["lsl_pct"].fillna(0)
        print(f"  铅管数据覆盖: {(df['lsl_pct']>0).sum()} 个系统，最大值={df['lsl_pct'].max():.1f}%")
    else:
        print(f"  ⚠ 找不到匹配字段，列名: {list(lsl.columns)}")
        df["lsl_pct"] = 0.0
else:
    print("  ⚠ lsl_inventory.csv 未找到，跳过")
    df["lsl_pct"] = 0.0

# ── 17. 填充缺失值，输出 ──────────────────────────────────────────────────────
FEATURE_COLS = [
    "tri_count_per_km2", "superfund_count_per_km2",
    "pesticide_per_km2", "geotracker_count_per_km2", "school_lead_max",
    "is_imported", "is_groundwater",
    "wqp_砷", "wqp_铅", "wqp_硝酸盐", "wqp_tds",
    "pfas_factor",
    "lead_score", "housing_burden",
    "has_advanced_treatment", "has_softening", "n_treatment_steps",
    "lsl_pct",
]
for col in FEATURE_COLS:
    if col not in df.columns:
        df[col] = 0.0
df[FEATURE_COLS] = df[FEATURE_COLS].fillna(0)

out_path = os.path.join(OUT_DATA, "system_features.csv")
df.to_csv(out_path, index=False)

print(f"\n✅ 输出：{out_path}")
print(f"   供水系统数：{len(df)}，特征数：{len(FEATURE_COLS)}")
print(df[["pwsid","system_name","ewg_score"] + FEATURE_COLS].describe().round(2).to_string())
