"""
02_eda.py — 探索性分析
  - 目标变量分布
  - 气候/土壤/人为变量相关性排名
  - 县级空间分布地图（folium）

用法（独立）：  python src/analysis/02_eda.py
被 run_analysis.py 调用：  run(df, climate_cols, soil_cols, human_cols)
"""

import os, json
import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

sns.set_theme(style="whitegrid")

FEATURE_LABELS = {
    "crop_diversity_hhi":    "Crop Diversity HHI",
    "high_water_crop_share": "High-Water Crop Share",
    "poverty_rate":          "Poverty Rate",
    "median_income":         "Median Income",
    "avg_farm_size_ac":      "Avg Farm Size (ac)",
    "centerpivot_ratio":     "Center Pivot Ratio",
    "irr_dependency":        "Irrigation Dependency",
    "farm_count":            "Farm Count",
    "tenant_ratio":          "Tenant Ratio",
    "precip_deficit_in":     "Precip Deficit (in)",
    "drought_intensity":     "Drought Intensity",
    "elevation_ft":          "Elevation (ft)",
    "awc_mean":              "Soil AWC",
    "clay_pct":              "Clay %",
    "organic_matter":        "Organic Matter %",
}

def _lbl(col): return FEATURE_LABELS.get(col, col.replace("_", " ").title())

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _shared import OUTPUT_DIR, IMAGES_DIR, TARGET, LOG_TARGET, available_cols


def run(df: pd.DataFrame, climate_cols: list, soil_cols: list, human_cols: list):
    print("\n" + "="*50)
    print("  02 EDA")
    print("="*50)
    results = {}

    eff = df[TARGET].dropna()
    if len(eff) == 0:
        print("  ⚠ 目标变量全空，跳过 EDA"); return {}

    # ── 分布统计 ──────────────────────────────────────────────────────
    pcts = {f"p{p}": round(float(eff.quantile(p/100)), 2) for p in [10,25,50,75,90]}
    results["distribution"] = {
        "n": int(len(eff)),
        "mean": round(float(eff.mean()), 2),
        "median": round(float(eff.median()), 2),
        "std": round(float(eff.std()), 2),
        "quantiles": pcts,
    }
    print(f"  目标变量 n={len(eff)}  中位={pcts['p50']} $/af  P90={pcts['p90']}")

    # ── 分布图 ────────────────────────────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    eff_clip = eff.clip(upper=eff.quantile(0.99))
    sns.histplot(eff_clip, bins=60, ax=axes[0], color="#1976D2", edgecolor="white", linewidth=0.3)
    axes[0].axvline(float(eff.median()), color="#E53935", linestyle="--", linewidth=1.5,
                    label=f"Median = {pcts['p50']} $/af")
    axes[0].set_xlabel("Crop Water Efficiency ($/af)", fontsize=11)
    axes[0].set_ylabel("County Count", fontsize=11)
    axes[0].set_title("Water Efficiency Distribution (clipped at P99)", fontsize=12, fontweight="bold")
    axes[0].legend(fontsize=10)

    log_eff = np.log1p(eff)
    sns.histplot(log_eff, bins=60, ax=axes[1], color="#43A047", edgecolor="white", linewidth=0.3)
    axes[1].set_xlabel("log(1 + Water Efficiency)", fontsize=11)
    axes[1].set_ylabel("County Count", fontsize=11)
    axes[1].set_title("Log Water Efficiency Distribution (for modeling)", fontsize=12, fontweight="bold")
    plt.suptitle(f"Agricultural Water Efficiency Distribution (n={len(eff):,} Counties)",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "02_distribution.png"), dpi=150, bbox_inches="tight")
    plt.close()

    # ── 相关性分析 ────────────────────────────────────────────────────
    all_feats = available_cols(df, climate_cols + soil_cols + human_cols)

    # ── 特征覆盖率报告 ────────────────────────────────────────────────
    print(f"\n  特征覆盖率（非空县数 / {len(df)} 总县）:")
    for feat in climate_cols + soil_cols + human_cols:
        if feat not in df.columns:
            print(f"    {'':2}{'[无列]':<8} {feat}")
            continue
        n = df[feat].notna().sum()
        pct = n / len(df) * 100
        group = "气候" if feat in climate_cols else "土壤" if feat in soil_cols else "人为"
        flag = " ⚠ 低覆盖" if pct < 30 else ""
        print(f"    [{group}] {feat:<30} {n:>4}县 ({pct:4.0f}%){flag}")

    corr_rows = []
    for feat in all_feats:
        sub = df[[LOG_TARGET, feat]].dropna()
        if len(sub) < 50:
            print(f"    ⚠ {feat} 配对样本不足（{len(sub)}），跳过相关性计算")
            continue
        r, p = stats.pearsonr(sub[LOG_TARGET], sub[feat])
        group = ("气候" if feat in climate_cols else
                 "土壤" if feat in soil_cols else "人为")
        corr_rows.append({"feature": feat, "r": round(float(r), 3),
                           "p": round(float(p), 4), "group": group,
                           "significant": p < 0.05})
    corr_df = pd.DataFrame(corr_rows).sort_values("r", key=abs, ascending=False)
    results["correlations"] = corr_df.to_dict("records")

    print(f"\n  相关性 Top10（|r| 排序）:")
    for _, row in corr_df.head(10).iterrows():
        sig = "✓" if row["significant"] else " "
        print(f"    {sig} [{row['group']}] {row['feature']:<30} r={row['r']:+.3f}")

    # ── 相关性条形图 ──────────────────────────────────────────────────
    GROUP_EN  = {"气候": "Climate", "土壤": "Soil", "人为": "Human"}
    PALETTE   = {"Climate": "#D97B45", "Soil": "#8A7060", "Human": "#5B8DB8"}
    top20 = corr_df.head(20).copy()
    top20["feature_en"] = top20["feature"].map(_lbl)
    top20["group_en"]   = top20["group"].map(GROUP_EN)
    top20_plot = top20[::-1].reset_index(drop=True)

    fig, ax = plt.subplots(figsize=(10, 8))
    sns.barplot(data=top20_plot, x="r", y="feature_en",
                hue="group_en", palette=PALETTE, dodge=False,
                edgecolor="white", linewidth=0.4, ax=ax,
                legend=False)
    ax.axvline(0, color="#333333", linewidth=0.8)
    for i, row in top20_plot.iterrows():
        r   = row["r"]
        sig = " *" if row["significant"] else ""
        ax.text(r + (0.005 if r >= 0 else -0.005), i,
                f"{r:+.3f}{sig}", va="center",
                ha="left" if r >= 0 else "right", fontsize=8.5, color="#333333")
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=c, label=l) for l, c in PALETTE.items()],
              fontsize=10, loc="lower right")
    ax.set_xlabel("Pearson r  (vs. log Water Efficiency)", fontsize=11)
    ax.set_ylabel("")
    ax.set_title("Feature Correlations with Water Efficiency  (Top 20, * = p<0.05)",
                 fontsize=13, fontweight="bold")
    ax.set_xlim(-0.6, 0.6)
    sns.despine(ax=ax, left=True)
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "02_correlation.png"), dpi=150, bbox_inches="tight")
    plt.close()

    # ── 特征间共线性检测 ──────────────────────────────────────────────
    feat_df = df[all_feats].apply(pd.to_numeric, errors="coerce")
    feat_df = feat_df[[c for c in feat_df.columns if feat_df[c].notna().sum() >= 100]]
    feat_clean = feat_df.dropna()

    if len(feat_clean) >= 50 and len(feat_clean.columns) >= 2:
        # 相关矩阵热图 (seaborn heatmap)
        corr_mat = feat_clean.corr()
        corr_mat.index   = [_lbl(c) for c in corr_mat.index]
        corr_mat.columns = [_lbl(c) for c in corr_mat.columns]
        sz = max(8, len(corr_mat) * 0.7)
        fig, ax = plt.subplots(figsize=(sz, max(6, sz * 0.85)))
        annot = corr_mat.map(lambda v: f"{v:.2f}" if abs(v) > 0.5 else "")
        sns.heatmap(corr_mat, cmap="RdBu_r", vmin=-1, vmax=1, center=0,
                    annot=annot, fmt="", annot_kws={"size": 7},
                    linewidths=0.3, linecolor="#e0e0e0",
                    square=True, ax=ax,
                    cbar_kws={"shrink": 0.8})
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
        ax.set_yticklabels(ax.get_yticklabels(), fontsize=8)
        ax.set_title("Feature Correlation Matrix  (values shown where |r|>0.5)",
                     fontsize=12, fontweight="bold")
        plt.tight_layout()
        plt.savefig(os.path.join(IMAGES_DIR, "02_collinearity.png"), dpi=150, bbox_inches="tight")
        plt.close()

        # VIF 计算 + 迭代剔除高共线性特征
        try:
            from sklearn.linear_model import LinearRegression

            def _compute_vif(X_df):
                arr = X_df.values
                rows = []
                for i, col in enumerate(X_df.columns):
                    y_ = arr[:, i]
                    X_ = np.delete(arr, i, axis=1)
                    r2 = LinearRegression().fit(X_, y_).score(X_, y_)
                    rows.append({"feature": col, "vif": round(float(1 / (1 - r2) if r2 < 1 else 1e9), 2)})
                return pd.DataFrame(rows).sort_values("vif", ascending=False)

            VIF_THRESHOLD = 10
            working = feat_clean.copy()
            dropped_collinear = []
            while True:
                vif_df = _compute_vif(working)
                worst = vif_df.iloc[0]
                if worst["vif"] <= VIF_THRESHOLD:
                    break
                dropped_collinear.append(worst["feature"])
                working = working.drop(columns=[worst["feature"]])
                print(f"  ✂ 删除高共线性特征：{worst['feature']} (VIF={worst['vif']:.1f})")

            results["vif"] = vif_df.to_dict("records")
            results["dropped_collinear"] = dropped_collinear
            print(f"\n  VIF（剔除后，阈值={VIF_THRESHOLD}）:")
            for _, row in vif_df.iterrows():
                print(f"    {row['feature']:<30} VIF={row['vif']:>7.2f}")
            if dropped_collinear:
                print(f"  已剔除：{dropped_collinear}")
        except Exception as e:
            print(f"  VIF 计算失败: {e}")
            results["dropped_collinear"] = []

        # 高共线性对（|r| > 0.7）
        high_corr_pairs = []
        for i in range(len(corr_mat)):
            for j in range(i+1, len(corr_mat)):
                r = corr_mat.values[i, j]
                if abs(r) > 0.7:
                    high_corr_pairs.append({
                        "feat1": corr_mat.columns[i],
                        "feat2": corr_mat.columns[j],
                        "r": round(float(r), 3)
                    })
        high_corr_pairs.sort(key=lambda x: abs(x["r"]), reverse=True)
        results["high_collinear_pairs"] = high_corr_pairs
        if high_corr_pairs:
            print(f"\n  高共线性特征对（|r|>0.7）:")
            for p in high_corr_pairs:
                print(f"    {p['feat1']:<25} ↔ {p['feat2']:<25} r={p['r']:+.3f}")

    # ── 分组对比：高效率 vs 低效率县 ─────────────────────────────────
    log_eff = df[LOG_TARGET].dropna()
    q33, q66 = log_eff.quantile(0.33), log_eff.quantile(0.66)
    high = df[df[LOG_TARGET] >= q66]
    low  = df[df[LOG_TARGET] <= q33]
    compare = {}
    for feat in available_cols(df, human_cols)[:10]:
        h_med = high[feat].median()
        l_med = low[feat].median()
        if pd.notna(h_med) and pd.notna(l_med) and l_med != 0:
            compare[feat] = {
                "high_eff_median": round(float(h_med), 3),
                "low_eff_median":  round(float(l_med), 3),
                "ratio": round(float(h_med / l_med), 2)
            }
    results["high_vs_low"] = compare
    print(f"\n  高效率县 vs 低效率县（人为因素中位比较，Top5）:")
    for feat, v in list(compare.items())[:5]:
        print(f"    {feat:<30}  高效:{v['high_eff_median']}  低效:{v['low_eff_median']}  比值:{v['ratio']}")

    with open(os.path.join(OUTPUT_DIR, "02_eda.json"), "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  输出：02_distribution.png / 02_correlation.png / 02_eda.json")
    return results


if __name__ == "__main__":
    from water_efficiency import load_data, feature_engineering
    from _shared import CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
    df, c, s, h = feature_engineering(load_data())
    run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
