"""
05_shap.py — SHAP 归因：哪些可干预变量影响水效率最大？

  - 只用人为因素特征训练模型（已控制气候土壤）
  - SHAP summary plot（全局重要性）
  - SHAP beeswarm（方向性：正向/负向影响）
  - 高效率县 vs 低效率县 waterfall 对比

用法（独立）：  python src/analysis/05_shap.py
被 run_analysis.py 调用：  run(df, climate_cols, soil_cols, human_cols)
"""

import os, json, warnings
import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import shap
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
from sklearn.impute import SimpleImputer
from sklearn.model_selection import cross_val_score

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _shared import OUTPUT_DIR, IMAGES_DIR, LOG_TARGET, available_cols


def run(df: pd.DataFrame, climate_cols: list, soil_cols: list, human_cols: list):
    print("\n" + "="*50)
    print("  05 SHAP 归因 — 可干预变量")
    print("="*50)

    h_cols = available_cols(df, human_cols)
    if not h_cols:
        print("  ✗ 无人为因素特征，跳过"); return {}

    # 加入气候变量作为控制变量，但 SHAP 分析只解读人为因素
    ctrl_cols = available_cols(df, climate_cols + list(soil_cols))
    seen = set()
    all_cols = [c for c in ctrl_cols + h_cols if not (c in seen or seen.add(c))]

    dm = df[[c for c in all_cols + [LOG_TARGET] if c in df.columns]].dropna(subset=[LOG_TARGET])
    all_cols = [c for c in all_cols if c in dm.columns]
    dm = dm.loc[:, ~dm.columns.duplicated()]
    dm = dm.dropna(thresh=int(len(all_cols) * 0.5))
    print(f"  建模样本：{len(dm)} 县 × {len(h_cols)} 人为因素（+{len(ctrl_cols)} 控制变量）")

    if len(dm) < 100:
        print("  ✗ 样本不足，跳过"); return {}

    imp_data = SimpleImputer(strategy="median")
    feat_df = dm[all_cols].apply(pd.to_numeric, errors="coerce")
    X_all = pd.DataFrame(imp_data.fit_transform(feat_df), columns=all_cols)
    y = dm[LOG_TARGET].values

    rf = RandomForestRegressor(n_estimators=500, max_depth=8,
                               min_samples_leaf=5, random_state=42, n_jobs=-1)
    rf.fit(X_all, y)
    cv = cross_val_score(rf, X_all, y, cv=5, scoring="r2")
    print(f"  RF 5-fold R²: {cv.mean():.3f} ± {cv.std():.3f}")

    # ── SHAP 计算 ─────────────────────────────────────────────────────
    print("  计算 SHAP 值（TreeExplainer）...")
    explainer = shap.TreeExplainer(rf)
    shap_values = explainer.shap_values(X_all)

    # 只取人为因素的 SHAP
    h_idx  = [all_cols.index(c) for c in h_cols]
    X_human = X_all[h_cols]
    shap_human = shap_values[:, h_idx]

    shap_imp = pd.Series(
        np.abs(shap_human).mean(axis=0), index=h_cols
    ).sort_values(ascending=False)

    print(f"\n  Top 10 可干预变量（SHAP 重要性）：")
    for feat, val in shap_imp.head(10).items():
        print(f"    {feat:<35} {val:.5f}")

    results = {
        "model_r2_cv": round(float(cv.mean()), 3),
        "shap_importance": shap_imp.round(5).to_dict(),
        "top_actionable": list(shap_imp.head(5).index),
    }

    # ── SHAP Importance bar chart (seaborn) ──────────────────────────
    imp_df = pd.DataFrame({
        "feature":    [_lbl(c) for c in h_cols],
        "importance": np.abs(shap_human).mean(axis=0)
    }).sort_values("importance")

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.barplot(data=imp_df, x="importance", y="feature",
                color="#1976D2", edgecolor="white", linewidth=0.4, ax=ax)
    for bar in ax.patches:
        w = bar.get_width()
        ax.text(w + 0.005, bar.get_y() + bar.get_height() / 2,
                f"{w:.3f}", va="center", fontsize=9.5)
    r2 = cv.mean()
    ax.text(0.98, 0.02, f"Model R² = {r2:.2f}", transform=ax.transAxes,
            ha="right", va="bottom", fontsize=10,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#E3F2FD", edgecolor="none"))
    ax.set_xlabel("Mean |SHAP Value| (impact on log water efficiency)", fontsize=11)
    ax.set_ylabel("")
    ax.set_title("SHAP Feature Importance — Actionable Variables\n"
                 "(net effect after controlling Climate & Soil)",
                 fontsize=12, fontweight="bold")
    ax.set_xlim(0, imp_df["importance"].max() * 1.25)
    sns.despine(ax=ax, left=True)
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "05_shap_importance.png"),
                dpi=150, bbox_inches="tight")
    plt.close()

    # ── SHAP Violin Plot ───────────────────────────────────────────────
    from scipy.stats import gaussian_kde

    # Sort ascending so most important at top
    mean_abs = np.abs(shap_human).mean(axis=0)
    order     = np.argsort(mean_abs)
    feat_ord  = [h_cols[i] for i in order]
    shap_ord  = shap_human[:, order]
    labels_ord = [_lbl(f) for f in feat_ord]
    n_feat    = len(feat_ord)

    fig, ax = plt.subplots(figsize=(11, max(5, n_feat * 0.82 + 1.4)))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # Alternating row backgrounds
    for i in range(n_feat):
        ax.axhspan(i - 0.5, i + 0.5,
                   color="#F8FAFC" if i % 2 == 0 else "white",
                   zorder=0, linewidth=0)

    NEG_COLOR = "#C97B50"   # muted terracotta — negative SHAP (hurts efficiency)
    POS_COLOR = "#52966B"   # muted sage green — positive SHAP (helps efficiency)
    HALF_H    = 0.38        # half-height of violin

    for i in range(n_feat):
        sv = shap_ord[:, i]
        if sv.std() < 1e-9 or len(sv) < 5:
            continue

        try:
            kde = gaussian_kde(sv, bw_method=0.25)
            xg  = np.linspace(sv.min(), sv.max(), 500)
            d   = kde(xg)
            d   = d / d.max() * HALF_H   # normalize height

            # Split at x=0: negative side (orange) / positive side (green)
            neg_mask = xg <= 0
            pos_mask = xg >= 0

            if neg_mask.any():
                ax.fill_between(xg[neg_mask], i - d[neg_mask], i + d[neg_mask],
                                color=NEG_COLOR, alpha=0.75, linewidth=0, zorder=2)
            if pos_mask.any():
                ax.fill_between(xg[pos_mask], i - d[pos_mask], i + d[pos_mask],
                                color=POS_COLOR, alpha=0.75, linewidth=0, zorder=2)

            # Thin outline
            ax.plot(xg, i + d,  color="white", linewidth=0.6, zorder=3)
            ax.plot(xg, i - d,  color="white", linewidth=0.6, zorder=3)

            # Mean SHAP marker
            mean_sv = float(sv.mean())
            mean_d  = float(kde(mean_sv)) / kde(xg).max() * HALF_H
            ax.plot([mean_sv, mean_sv], [i - mean_d, i + mean_d],
                    color="white", linewidth=2.0, zorder=4)

        except Exception:
            pass

    # Center line
    ax.axvline(0, color="#94A3B8", linewidth=1.0, zorder=1)

    # Labels
    ax.set_yticks(range(n_feat))
    ax.set_yticklabels(labels_ord, fontsize=10.5)
    ax.set_xlabel("SHAP Value  (impact on log water efficiency)", fontsize=11, color="#475569")
    ax.set_title("SHAP Violin — Human Factors vs. Agricultural Water Efficiency",
                 fontsize=12, fontweight="bold", color="#0F172A", pad=12)

    # Color legend
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(facecolor=NEG_COLOR, label="Reduces efficiency", alpha=0.9),
                       Patch(facecolor=POS_COLOR, label="Increases efficiency", alpha=0.9)],
              fontsize=9, loc="lower right", frameon=True,
              framealpha=0.95, edgecolor="#E2E8F0")

    ax.grid(axis="x", color="#E2E8F0", linewidth=0.5, zorder=0)
    ax.set_ylim(-0.6, n_feat - 0.4)
    ax.tick_params(axis="y", length=0)
    sns.despine(ax=ax, left=True)
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "05_shap_beeswarm.png"),
                dpi=150, bbox_inches="tight")
    plt.close()

    # ── SHAP Contribution chart: best vs worst county (seaborn) ──────
    dm2 = dm.copy().reset_index(drop=True)
    dm2["pred"] = rf.predict(X_all)
    top_idx = int(dm2["pred"].idxmax())
    bot_idx = int(dm2["pred"].idxmin())

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    for ax, idx, title in [(axes[0], top_idx, "Highest Efficiency County"),
                            (axes[1], bot_idx,  "Lowest Efficiency County")]:
        sv = shap_values[idx, :][h_idx]
        contrib_df = pd.DataFrame({
            "feature":   [_lbl(c) for c in h_cols],
            "shap":      sv,
            "direction": ["Positive" if v >= 0 else "Negative" for v in sv],
        }).sort_values("shap", key=abs).tail(10)

        sns.barplot(data=contrib_df, x="shap", y="feature",
                    hue="direction",
                    palette={"Positive": "#EF5350", "Negative": "#42A5F5"},
                    dodge=False, edgecolor="white", linewidth=0.4, ax=ax)
        ax.axvline(0, color="#333333", linewidth=0.8)
        ax.set_xlabel("SHAP Value", fontsize=11)
        ax.set_ylabel("")
        ax.set_title(title, fontsize=12, fontweight="bold")
        ax.legend(title="Effect", fontsize=9)
        sns.despine(ax=ax, left=True)

    plt.suptitle("SHAP Contributions: Best vs. Worst Efficiency County",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "05_shap_waterfall.png"),
                dpi=150, bbox_inches="tight")
    plt.close()

    with open(os.path.join(OUTPUT_DIR, "05_shap.json"), "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  输出：05_shap_importance.png / 05_shap_beeswarm.png")
    print(f"        05_shap_waterfall.png / 05_shap.json")
    return results


if __name__ == "__main__":
    from water_efficiency import load_data, feature_engineering
    from _shared import CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
    df, _, __, ___ = feature_engineering(load_data())
    run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
