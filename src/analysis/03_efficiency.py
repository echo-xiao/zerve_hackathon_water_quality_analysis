"""
03_efficiency.py — 水效率分解：气候 vs 土壤 vs 人为因素占比

  - Random Forest MDI 分解各类因素的解释力
  - Lasso 回归（标准化系数）：控制气候后人为因素净效应
  - 干旱县 vs 湿润县分群对比

用法（独立）：  python src/analysis/03_efficiency.py
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

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import cross_val_score

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _shared import OUTPUT_DIR, IMAGES_DIR, TARGET, LOG_TARGET, available_cols


def run(df: pd.DataFrame, climate_cols: list, soil_cols: list, human_cols: list):
    print("\n" + "="*50)
    print("  03 水效率因素分解")
    print("="*50)

    c_cols = available_cols(df, climate_cols)
    s_cols = available_cols(df, soil_cols)
    h_cols = available_cols(df, human_cols)
    # 去重，防止列名重复导致 shape 不匹配
    seen = set()
    all_cols = [c for c in c_cols + s_cols + h_cols if not (c in seen or seen.add(c))]

    dm = df[[c for c in all_cols + [LOG_TARGET] if c in df.columns]].dropna(subset=[LOG_TARGET])
    all_cols = [c for c in all_cols if c in dm.columns]
    # 去掉 df 中重复列名
    dm = dm.loc[:, ~dm.columns.duplicated()]
    all_cols = [c for c in all_cols if c in dm.columns]
    dm = dm.dropna(thresh=int(len(all_cols) * 0.5))
    print(f"  建模样本：{len(dm)} 县 × {len(all_cols)} 特征")
    print(f"  特征列：{all_cols}")

    if len(dm) < 100:
        print("  ⚠ 样本不足，跳过"); return {}

    # 强制转数值（parquet 中部分列可能存为 object）
    feat_df = dm[all_cols].apply(pd.to_numeric, errors="coerce")
    X = pd.DataFrame(
        SimpleImputer(strategy="median").fit_transform(feat_df),
        columns=all_cols
    )
    y = dm[LOG_TARGET].values

    # ── Random Forest 全特征 ─────────────────────────────────────────
    rf = RandomForestRegressor(n_estimators=500, max_depth=8,
                               min_samples_leaf=5, random_state=42, n_jobs=-1)
    rf.fit(X, y)
    cv = cross_val_score(rf, X, y, cv=5, scoring="r2")
    print(f"  RF 5-fold R²: {cv.mean():.3f} ± {cv.std():.3f}")

    mdi = pd.Series(rf.feature_importances_, index=all_cols)

    # ── 三类因素占比 ──────────────────────────────────────────────────
    climate_pct = float(mdi[mdi.index.isin(c_cols)].sum())
    soil_pct    = float(mdi[mdi.index.isin(s_cols)].sum())
    human_pct   = float(mdi[mdi.index.isin(h_cols)].sum())
    total = climate_pct + soil_pct + human_pct

    print(f"\n  因素占比（MDI）：")
    print(f"    气候（不可控）: {climate_pct/total*100:.1f}%")
    print(f"    土壤（不可控）: {soil_pct/total*100:.1f}%")
    print(f"    人为（可干预）: {human_pct/total*100:.1f}%")

    # ── 占比饼图 ──────────────────────────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    wedges, texts, autotexts = axes[0].pie(
        [climate_pct, soil_pct, human_pct],
        labels=["Climate\n(uncontrollable)", "Soil\n(uncontrollable)", "Human\n(actionable)"],
        colors=["#EF6C00", "#795548", "#1976D2"],
        autopct="%1.1f%%", startangle=140,
        textprops={"fontsize": 11},
        pctdistance=0.7,
        wedgeprops={"edgecolor": "white", "linewidth": 2}
    )
    for at in autotexts:
        at.set_fontsize(12); at.set_fontweight("bold"); at.set_color("white")
    axes[0].set_title("Sources of Water Efficiency Variation\n(Random Forest MDI)",
                      fontsize=13, fontweight="bold")

    # Human factor importance ranking (MDI) — seaborn barplot
    human_mdi = mdi[mdi.index.isin(h_cols)].sort_values(ascending=True).tail(10)
    mdi_df = pd.DataFrame({
        "feature": [_lbl(c) for c in human_mdi.index],
        "importance": human_mdi.values
    })
    sns.barplot(data=mdi_df, x="importance", y="feature",
                color="#1976D2", edgecolor="white", linewidth=0.4, ax=axes[1])
    for bar in axes[1].patches:
        w = bar.get_width()
        axes[1].text(w + 0.002, bar.get_y() + bar.get_height() / 2,
                     f"{w:.3f}", va="center", fontsize=9)
    axes[1].set_xlabel("MDI Importance", fontsize=11)
    axes[1].set_ylabel("")
    axes[1].set_title("Human Factor Importance (Top 10)\n(Random Forest MDI)",
                      fontsize=13, fontweight="bold")
    axes[1].set_xlim(0, mdi_df["importance"].max() * 1.2)
    sns.despine(ax=axes[1], left=True)
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "03_decomposition.png"), dpi=150, bbox_inches="tight")
    plt.close()

    # ── Lasso：控制气候土壤后，人为因素净效应 ────────────────────────
    print(f"\n  Lasso 回归（标准化系数）...")
    Xs = StandardScaler().fit_transform(X)
    lasso = LassoCV(cv=5, random_state=42, max_iter=5000)
    lasso.fit(Xs, y)
    lasso_r2 = cross_val_score(lasso, Xs, y, cv=5, scoring="r2").mean()

    coef = pd.DataFrame({
        "feature": all_cols,
        "coef":    lasso.coef_,
        "group":   (["气候"]*len(c_cols) + ["土壤"]*len(s_cols) + ["人为"]*len(h_cols))
    })
    coef = coef[coef["coef"] != 0].sort_values("coef", key=abs, ascending=False)
    human_coef = coef[coef["group"] == "人为"]

    print(f"  Lasso R²={lasso_r2:.3f}  非零特征={len(coef)}")
    print(f"  正向效应（效率提升）：")
    for _, row in human_coef[human_coef["coef"] > 0].head(5).iterrows():
        print(f"    ↑ {row['feature']:<30} β={row['coef']:+.4f}")
    print(f"  负向效应（效率下降）：")
    for _, row in human_coef[human_coef["coef"] < 0].head(5).iterrows():
        print(f"    ↓ {row['feature']:<30} β={row['coef']:+.4f}")

    # ── 干旱 vs 湿润分群 ──────────────────────────────────────────────
    subgroup = {}
    if "precip_deficit_in" in df.columns:
        med = df["precip_deficit_in"].median()
        for name, gdf in [("干旱县", df[df["precip_deficit_in"] > med]),
                          ("湿润县", df[df["precip_deficit_in"] <= med])]:
            sub = gdf[h_cols + [LOG_TARGET]].dropna(subset=[LOG_TARGET])
            sub = sub.dropna(thresh=max(3, len(h_cols)//2))
            if len(sub) < 50: continue
            X_sub = SimpleImputer(strategy="median").fit_transform(sub[h_cols])
            rf_sub = RandomForestRegressor(200, max_depth=6, min_samples_leaf=5, random_state=42)
            rf_sub.fit(X_sub, sub[LOG_TARGET].values)
            imp = pd.Series(rf_sub.feature_importances_, index=h_cols).sort_values(ascending=False)
            subgroup[name] = {"n": int(len(sub)), "top5": imp.head(5).round(4).to_dict()}
            print(f"  {name} (n={len(sub)}) 最重要: {list(imp.index[:3])}")

    results = {
        "model_r2_cv": round(float(cv.mean()), 3),
        "factor_pct": {
            "climate": round(climate_pct/total, 3),
            "soil":    round(soil_pct/total, 3),
            "human":   round(human_pct/total, 3),
        },
        "lasso_r2": round(float(lasso_r2), 3),
        "human_positive_effects": human_coef[human_coef["coef"]>0].head(5)[["feature","coef"]].round(4).to_dict("records"),
        "human_negative_effects": human_coef[human_coef["coef"]<0].head(5)[["feature","coef"]].round(4).to_dict("records"),
        "subgroup": subgroup,
    }
    with open(os.path.join(OUTPUT_DIR, "03_efficiency.json"), "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  输出：03_decomposition.png / 03_efficiency.json")
    return results


if __name__ == "__main__":
    from water_efficiency import load_data, feature_engineering
    from _shared import CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
    df, _, __, ___ = feature_engineering(load_data())
    run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
