"""
01_build_features.py — 构建 ZCTA 级特征宽表

输入（output/data/）：
  wqp_zcta_data.json, ejscreen.geojson, tri.geojson, superfund.geojson,
  pfas.geojson, pesticide.geojson, fires_simple.geojson, aqs_zcta_geo.geojson

输出：
  output/data/zcta_features.csv  — 一行 = 一个 ZCTA，列 = 特征 + 目标变量

运行：
  python src/analysis/01_build_features.py
  # 或在 Zerve Notebook 中逐 cell 执行
"""

import os, sys, json
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DATA = os.path.join(ROOT, "output", "data")

# ── 1. 加载 ZCTA 边界 ─────────────────────────────────────────────────────────
print("加载 ZCTA 边界...")
zcta_gdf = gpd.read_file(os.path.join(OUT_DATA, "aqs_zcta_geo.geojson")).to_crs("EPSG:4326")
zcta_gdf["zcta"] = zcta_gdf["zcta"].astype(str)
df = pd.DataFrame({"zcta": zcta_gdf["zcta"].values})
print(f"  {len(df)} 个 ZCTA")

# ── 2. 目标变量：WQP 综合水质评分 ─────────────────────────────────────────────
print("构建 WQP 水质评分（目标变量）...")
with open(os.path.join(OUT_DATA, "wqp_zcta_data.json")) as f:
    wqp = json.load(f)

max_vals = wqp.get("max_values", {})
zcta_scores: dict = {}
for cont, month_data in wqp["data"].items():
    p95 = max_vals.get(cont, 1) or 1
    for month_vals in month_data.values():
        for zcta, val in month_vals.items():
            norm = min(1.0, val / p95)
            zcta_scores.setdefault(zcta, []).append(norm)

wq_score = {z: float(np.mean(vals)) for z, vals in zcta_scores.items()}
df["wq_score"] = df["zcta"].map(wq_score)
df = df.dropna(subset=["wq_score"])
print(f"  {len(df)} 个 ZCTA 有 WQP 数据，均值={df['wq_score'].mean():.3f}")

# ── 辅助：点 GeoJSON → GeoDataFrame ──────────────────────────────────────────
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
    gdf = gpd.GeoDataFrame(rows, geometry=gpd.points_from_xy(
        [r["_lon"] for r in rows], [r["_lat"] for r in rows]), crs="EPSG:4326")
    return gdf

def sjoin_aggregate(points_gdf, agg_dict):
    """把点数据 spatial join 到 ZCTA，按 agg_dict 聚合"""
    joined = gpd.sjoin(points_gdf, zcta_gdf[["zcta", "geometry"]], predicate="within", how="left")
    joined = joined.dropna(subset=["zcta"])
    result = joined.groupby("zcta").agg(agg_dict).reset_index()
    result.columns = ["zcta"] + [c[0] if isinstance(c, tuple) else c for c in result.columns[1:]]
    return result

# ── 3. CalEnviroScreen 评分 + 贫困率 ─────────────────────────────────────────
print("提取 CalEnviroScreen 特征...")
ej_gdf = load_points("ejscreen.geojson")
if ej_gdf is not None:
    ej_gdf["ces_score"] = pd.to_numeric(ej_gdf["ces"], errors="coerce").fillna(0)
    ej_gdf["poverty_pct"] = ej_gdf["pov"].astype(str).str.replace("%","").apply(
        lambda x: float(x) if x.replace(".","").isdigit() else 0)
    ej_agg = gpd.sjoin(ej_gdf, zcta_gdf[["zcta","geometry"]], predicate="within", how="left")
    ej_agg = ej_agg.dropna(subset=["zcta"]).groupby("zcta").agg(
        ces_score=("ces_score","mean"), poverty_pct=("poverty_pct","mean")).reset_index()
    df = df.merge(ej_agg, on="zcta", how="left")
    print(f"  CES 均值={df['ces_score'].mean():.1f}, 贫困率均值={df['poverty_pct'].mean():.1f}%")

# ── 4. TRI 工业设施数量 ───────────────────────────────────────────────────────
print("提取 TRI 特征...")
tri_gdf = load_points("tri.geojson")
if tri_gdf is not None:
    tri_gdf["_cnt"] = 1
    tri_agg = gpd.sjoin(tri_gdf, zcta_gdf[["zcta","geometry"]], predicate="within", how="left")
    tri_agg = tri_agg.dropna(subset=["zcta"]).groupby("zcta")["_cnt"].sum().reset_index()
    tri_agg.columns = ["zcta", "tri_count"]
    df = df.merge(tri_agg, on="zcta", how="left")
    print(f"  TRI 设施数分布：{df['tri_count'].describe().to_dict()}")

# ── 5. Superfund 场地数量 ─────────────────────────────────────────────────────
print("提取 Superfund 特征...")
sf_gdf = load_points("superfund.geojson")
if sf_gdf is not None:
    sf_gdf["_cnt"] = 1
    sf_agg = gpd.sjoin(sf_gdf, zcta_gdf[["zcta","geometry"]], predicate="within", how="left")
    sf_agg = sf_agg.dropna(subset=["zcta"]).groupby("zcta")["_cnt"].sum().reset_index()
    sf_agg.columns = ["zcta", "superfund_count"]
    df = df.merge(sf_agg, on="zcta", how="left")

# ── 6. PFAS 污染强度 ──────────────────────────────────────────────────────────
print("提取 PFAS 特征...")
pfas_gdf = load_points("pfas.geojson")
if pfas_gdf is not None:
    pfas_gdf["pfas_factor"] = pd.to_numeric(pfas_gdf.get("worst_factor", 0), errors="coerce").fillna(0)
    pfas_agg = gpd.sjoin(pfas_gdf, zcta_gdf[["zcta","geometry"]], predicate="within", how="left")
    pfas_agg = pfas_agg.dropna(subset=["zcta"]).groupby("zcta")["pfas_factor"].max().reset_index()
    df = df.merge(pfas_agg, on="zcta", how="left")

# ── 7. 农药使用量 ─────────────────────────────────────────────────────────────
print("提取农药特征...")
pest_gdf = load_points("pesticide.geojson")
if pest_gdf is not None:
    pest_gdf["lbs_used"] = pd.to_numeric(pest_gdf.get("lbs_used", 0), errors="coerce").fillna(0)
    pest_agg = gpd.sjoin(pest_gdf, zcta_gdf[["zcta","geometry"]], predicate="within", how="left")
    pest_agg = pest_agg.dropna(subset=["zcta"]).groupby("zcta")["lbs_used"].sum().reset_index()
    pest_agg.columns = ["zcta", "pesticide_lbs"]
    df = df.merge(pest_agg, on="zcta", how="left")

# ── 8. 距野火边界距离 (km) ────────────────────────────────────────────────────
print("计算距野火距离...")
fire_path = os.path.join(OUT_DATA, "fires_simple.geojson")
if os.path.exists(fire_path):
    fire_gdf = gpd.read_file(fire_path).to_crs("EPSG:32611")
    zcta_utm = zcta_gdf.to_crs("EPSG:32611")
    centroids = zcta_utm.geometry.centroid
    fire_union = fire_gdf.geometry.union_all() if hasattr(fire_gdf.geometry, "union_all") else fire_gdf.geometry.unary_union
    dists = centroids.apply(lambda pt: fire_union.exterior.distance(pt) if fire_union.contains(pt) else fire_union.distance(pt))
    zcta_gdf["fire_dist_km"] = (dists / 1000).round(3).values
    df = df.merge(zcta_gdf[["zcta","fire_dist_km"]], on="zcta", how="left")
    print(f"  距野火距离：min={df['fire_dist_km'].min():.1f}km, max={df['fire_dist_km'].max():.1f}km")

# ── 9. 填充缺失值，输出 ────────────────────────────────────────────────────────
feature_cols = ["ces_score","poverty_pct","tri_count","superfund_count",
                "pfas_factor","pesticide_lbs","fire_dist_km"]
for col in feature_cols:
    if col not in df.columns:
        df[col] = 0.0
df[feature_cols] = df[feature_cols].fillna(0)

out_path = os.path.join(OUT_DATA, "zcta_features.csv")
df.to_csv(out_path, index=False)
print(f"\n✅ 输出：{out_path}")
print(f"   行数：{len(df)}，列数：{len(df.columns)}")
print(df[["zcta","wq_score"] + feature_cols].describe().round(3).to_string())
