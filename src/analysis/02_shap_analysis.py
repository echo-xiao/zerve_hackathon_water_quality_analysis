"""
02_shap_analysis.py — RandomForest + SHAP 根因分析

输入：output/data/zcta_features.csv（由 01_build_features.py 生成）

输出：
  output/data/zcta_rootcause.json  — 每个 ZCTA 的 SHAP 分解（供地图加载）
  output/figures/shap_global.png   — 全局特征重要性图
  output/figures/shap_summary.png  — SHAP beeswarm 图

运行：
  python src/analysis/02_shap_analysis.py
"""

import os, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
import shap

ROOT     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DATA = os.path.join(ROOT, "output", "data")
FIG_DIR  = os.path.join(ROOT, "output", "figures")
os.makedirs(FIG_DIR, exist_ok=True)

FEATURE_COLS = ["ces_score","poverty_pct","tri_count","superfund_count",
                "pfas_factor","pesticide_lbs","fire_dist_km"]

FEATURE_LABELS = {
    "ces_score":       "CES 综合评分",
    "poverty_pct":     "贫困率 (%)",
    "tri_count":       "TRI 工业设施数",
    "superfund_count": "Superfund 场地数",
    "pfas_factor":     "PFAS 污染强度",
    "pesticide_lbs":   "农药使用量 (lbs)",
    "fire_dist_km":    "距野火距离 (km)",
}

# ── 1. 加载特征宽表 ────────────────────────────────────────────────────────────
print("加载特征宽表...")
df = pd.read_csv(os.path.join(OUT_DATA, "zcta_features.csv"))
df = df.dropna(subset=["wq_score"])
print(f"  {len(df)} 个 ZCTA，目标变量 wq_score 均值={df['wq_score'].mean():.3f}")

X = df[FEATURE_COLS].fillna(0)
y = df["wq_score"]

# ── 2. 训练 RandomForest ──────────────────────────────────────────────────────
print("训练 RandomForest...")
model = RandomForestRegressor(n_estimators=300, random_state=42, n_jobs=-1)
model.fit(X, y)

cv_r2 = cross_val_score(model, X, y, cv=5, scoring="r2")
print(f"  5-fold CV R²: {cv_r2.mean():.3f} ± {cv_r2.std():.3f}")
print(f"  训练集 R²:    {model.score(X, y):.3f}")

# ── 3. SHAP ───────────────────────────────────────────────────────────────────
print("计算 SHAP 值...")
explainer  = shap.TreeExplainer(model)
shap_vals  = explainer(X)                  # shape (n, n_features)
base_value = float(explainer.expected_value)
print(f"  base_value (全局均值): {base_value:.4f}")

# ── 4. 全局特征重要性图 ────────────────────────────────────────────────────────
mean_abs_shap = np.abs(shap_vals.values).mean(axis=0)
order = np.argsort(mean_abs_shap)

fig, ax = plt.subplots(figsize=(8, 4.5))
colors = ["#c0392b" if v > 0 else "#2980b9" for v in mean_abs_shap[order]]
bars = ax.barh([FEATURE_LABELS[FEATURE_COLS[i]] for i in order],
               mean_abs_shap[order], color="#c0392b", alpha=0.85)
ax.set_xlabel("平均 |SHAP 值|（对水质评分的平均影响）", fontsize=11)
ax.set_title("LA County 水质根因排名（全局）", fontsize=13, fontweight="bold", pad=12)
ax.spines[["top","right"]].set_visible(False)
ax.tick_params(axis="y", labelsize=10)
plt.tight_layout()
fig_path = os.path.join(FIG_DIR, "shap_global.png")
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
print(f"  ✓ {fig_path}")
plt.close()

# ── 5. SHAP beeswarm ──────────────────────────────────────────────────────────
import matplotlib
matplotlib.rcParams["font.family"] = "sans-serif"
fig, ax = plt.subplots(figsize=(9, 5))
shap.plots.beeswarm(shap_vals, max_display=7, show=False,
                    color_bar_label="特征值（标准化）")
plt.title("SHAP 分布图（每点=一个 ZCTA）", fontsize=12, fontweight="bold")
plt.tight_layout()
fig_path = os.path.join(FIG_DIR, "shap_summary.png")
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
print(f"  ✓ {fig_path}")
plt.close()

# ── 6. 输出 zcta_rootcause.json ───────────────────────────────────────────────
print("生成 zcta_rootcause.json...")
zcta_out = {}
for i, row in df.iterrows():
    zcta = str(row["zcta"])
    sv   = shap_vals.values[df.index.get_loc(i)]   # shape (n_features,)
    shap_dict = {feat: round(float(sv[j]), 4) for j, feat in enumerate(FEATURE_COLS)}

    # dominant = highest positive SHAP （最主要的有害因素）
    pos = {k: v for k, v in shap_dict.items() if v > 0}
    dominant = max(pos, key=pos.get) if pos else max(shap_dict, key=lambda k: abs(shap_dict[k]))

    zcta_out[zcta] = {
        "wq_score":       round(float(row["wq_score"]), 4),
        "dominant_cause": dominant,
        "dominant_label": FEATURE_LABELS[dominant],
        "shap":           shap_dict,
        "features": {feat: round(float(row[feat]), 3) for feat in FEATURE_COLS},
    }

out = {
    "meta": {
        "feature_labels": FEATURE_LABELS,
        "cv_r2_mean":     round(float(cv_r2.mean()), 4),
        "cv_r2_std":      round(float(cv_r2.std()),  4),
        "base_value":     round(base_value, 4),
        "n_zcta":         len(df),
    },
    "zcta": zcta_out,
}

out_path = os.path.join(OUT_DATA, "zcta_rootcause.json")
with open(out_path, "w", ensure_ascii=False) as f:
    json.dump(out, f, ensure_ascii=False, separators=(",",":"))
print(f"  ✓ {out_path}  ({len(zcta_out)} ZCTAs)")

# ── 7. 打印 Top-5 根因排名 ────────────────────────────────────────────────────
print("\n── 全局根因排名（按平均 |SHAP|）──")
ranked = sorted(zip(FEATURE_COLS, mean_abs_shap), key=lambda x: x[1], reverse=True)
for rank, (feat, imp) in enumerate(ranked, 1):
    bar = "█" * int(imp / max(mean_abs_shap) * 20)
    print(f"  {rank}. {FEATURE_LABELS[feat]:<20} {bar}  {imp:.4f}")

print("\n✅ 分析完成")
