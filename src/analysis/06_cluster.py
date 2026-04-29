"""
06_cluster.py — 县级分类（6类县型）

基于气候控制后残差 + 人为因素，对县进行 K-Means 聚类：
  1. 高效西部灌溉县   — 高ETo、高灌溉技术、高效率
  2. 雨养粮仓县       — 低灌溉依赖、高粮食产量
  3. 干旱高风险县     — 高ETo、低效率、漫灌为主
  4. 资源匮乏县       — 高贫困、低技术采用
  5. 高效精耕县       — 中等气候、高技术、高产值
  6. 过渡发展县       — 中等各项指标

用法（独立）：  python src/analysis/06_cluster.py
被 run_analysis.py 调用：  run(df, climate_cols, soil_cols, human_cols)
"""

import os, json, warnings
import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
warnings.filterwarnings("ignore")

sns.set_theme(style="whitegrid")

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _shared import OUTPUT_DIR, IMAGES_DIR, TARGET, LOG_TARGET, available_cols

K_MIN, K_MAX = 2, 12  # 肘点法搜索范围


def _find_elbow(inertias: list) -> int:
    """用二阶差分找肘点：变化率下降最快处"""
    ks = np.arange(K_MIN, K_MAX + 1)
    inertias = np.array(inertias)
    # 计算每段斜率
    slopes = np.diff(inertias)
    # 二阶差分：斜率变化最大处为肘点
    second_diff = np.diff(slopes)
    elbow_idx = int(np.argmax(second_diff)) + K_MIN + 1  # +2 因为两次diff各偏移1
    return int(np.clip(elbow_idx, K_MIN, K_MAX))

# 聚类使用的特征（气候 + 关键人为因素 + 效率）
CLUSTER_FEATURE_PRIORITY = [
    "eto_avg_in", "precip_deficit_in", "drought_intensity",
    "elevation_ft",
    "avg_farm_size_ac", "crop_diversity_hhi",
    "tenant_ratio", "poverty_rate",
    "irr_dependency",
]

# 根据聚类中心特征自动命名（基于实际可用变量）
def _label_cluster(centroid: dict) -> str:
    eto  = centroid.get("eto_avg_in", 0)
    eff  = centroid.get(LOG_TARGET, 0)
    pov  = centroid.get("poverty_rate", 0)
    farm = centroid.get("avg_farm_size_ac", 0)
    prec = centroid.get("precip_deficit_in", 0)

    if eto > 85:
        return "Extreme-Arid Low-Efficiency"
    if farm > 2000 and prec > 40:
        return "Arid Large-Farm"
    if eff > 7.5:
        return "High-Efficiency Grain Belt"
    if eff > 6 and prec < 40:
        return "Western High-Efficiency"
    if pov > 16:
        return "Resource-Constrained"
    return "Arid Large-Farm Low-Efficiency"


def run(df: pd.DataFrame, climate_cols: list, soil_cols: list, human_cols: list):
    print("\n" + "="*50)
    print("  06 县级分类（Clustering）")
    print("="*50)

    feat_cols = available_cols(df, CLUSTER_FEATURE_PRIORITY)
    target_col = LOG_TARGET if LOG_TARGET in df.columns else TARGET

    dm = df[feat_cols + [target_col, "fips"]].dropna(subset=[target_col])
    dm = dm.dropna(thresh=int(len(feat_cols) * 0.4))
    print(f"  聚类样本：{len(dm)} 县 × {len(feat_cols)} 特征")

    if len(dm) < K_MAX * 10:
        print("  ⚠ 样本不足，跳过"); return {}

    # 填充缺失值 + 标准化（排除全为NaN的列）
    feat_df = dm[feat_cols].apply(pd.to_numeric, errors="coerce")
    feat_cols = [c for c in feat_cols if feat_df[c].notna().any()]
    feat_df = feat_df[feat_cols]
    imp = SimpleImputer(strategy="median")
    X_raw = imp.fit_transform(feat_df)
    X_raw_with_target = np.column_stack([
        X_raw,
        pd.to_numeric(dm[target_col], errors="coerce").values
    ])
    feat_names = feat_cols + [target_col]

    scaler = StandardScaler()
    X = scaler.fit_transform(X_raw_with_target)

    # ── 肘点法确定最优 K ─────────────────────────────────────────────
    print(f"  肘点法搜索 K={K_MIN}~{K_MAX}...")
    inertias = []
    for k in range(K_MIN, K_MAX + 1):
        km_k = KMeans(n_clusters=k, random_state=42, n_init=10)
        km_k.fit(X)
        inertias.append(km_k.inertia_)

    best_k = _find_elbow(inertias)
    print(f"  肘点最优 K = {best_k}")

    # 画 Elbow 图
    ks = list(range(K_MIN, K_MAX + 1))
    inertias_norm = [v / inertias[0] * 100 for v in inertias]
    elbow_df = pd.DataFrame({"K": ks, "WCSS (%)": inertias_norm})

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=elbow_df, x="K", y="WCSS (%)",
                 marker="o", color="#1976D2", linewidth=2, markersize=8,
                 markerfacecolor="white", markeredgewidth=2, ax=ax)
    ax.fill_between(ks, inertias_norm, alpha=0.08, color="#1976D2")
    ax.axvline(best_k, color="#E53935", linestyle="--", linewidth=1.5,
               label=f"Optimal K = {best_k}  (elbow)")
    ax.set_xlabel("K (Number of Clusters)", fontsize=11)
    ax.set_ylabel("Within-Cluster Sum of Squares (relative, %)", fontsize=11)
    ax.set_title("K-Means Elbow Method — Optimal Number of Clusters",
                 fontsize=13, fontweight="bold")
    ax.set_xticks(ks)
    ax.legend(fontsize=11)
    sns.despine()
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "06_elbow.png"), dpi=150, bbox_inches="tight")
    plt.close()

    # K-Means 聚类（使用肘点 K）
    km = KMeans(n_clusters=best_k, random_state=42, n_init=20)
    labels = km.fit_predict(X)
    dm = dm.copy()
    dm["cluster"] = labels

    # 计算各簇中心（原始尺度）
    centers_raw = scaler.inverse_transform(km.cluster_centers_)
    centers = []
    for i, c in enumerate(centers_raw):
        d = {feat_names[j]: round(float(c[j]), 4) for j in range(len(feat_names))}
        d["cluster_id"] = i
        d["n_counties"] = int((labels == i).sum())
        d["cluster_name"] = _label_cluster(d)
        centers.append(d)

    # 按效率均值排序命名
    centers_df = pd.DataFrame(centers).sort_values(LOG_TARGET if LOG_TARGET in feat_names else TARGET, ascending=False)
    print(f"  聚类结果：")
    for _, r in centers_df.iterrows():
        print(f"    Cluster {int(r.cluster_id)}: {r.cluster_name}  ({int(r.n_counties)} 县)")

    # 保存县级聚类标签
    cluster_map = {}
    for _, row in dm[["fips","cluster"]].iterrows():
        cid = int(row["cluster"])
        name = next(c["cluster_name"] for c in centers if c["cluster_id"] == cid)
        cluster_map[str(row["fips"])] = {"cluster_id": cid, "cluster_name": name}

    out_path = os.path.join(OUTPUT_DIR, "06_cluster.json")
    result = {
        "n_clusters": best_k,
        "n_counties": len(dm),
        "cluster_centers": centers,
        "county_clusters": cluster_map,
    }
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"  结果 → {out_path}")

    # 把聚类标签写回 df（供后续步骤使用）
    fips_to_cluster = {str(r["fips"]): int(r["cluster"]) for _, r in dm[["fips","cluster"]].iterrows()}
    df["cluster_id"] = df["fips"].astype(str).map(fips_to_cluster)
    cluster_id_to_name = {c["cluster_id"]: c["cluster_name"] for c in centers}
    df["cluster_name"] = df["cluster_id"].map(cluster_id_to_name)

    return result


if __name__ == "__main__":
    from _shared import load_features, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
    df = load_features()
    run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
