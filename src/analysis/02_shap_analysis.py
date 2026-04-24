"""
02_shap_analysis.py — Ridge 线性回归特征贡献度分析

分析单元：LA County 供水系统（与 01_build_features.py 一致）

输入：output/data/system_features.csv（由 01_build_features.py 生成）

输出：
  output/data/system_rootcause.json — 每个供水系统的线性贡献分解（供地图加载）
  output/figures/coef_global.png    — 全局特征重要性图（标准化系数）

注意：本脚本使用 Ridge 线性回归 + 线性贡献分解，而非 SHAP 值。
贡献度定义：
  contribution_i = β_i × (x_i - μ_i) / σ_i
  即"该特征使该系统评分偏离全县均值多少分"，单位与 ewg_score 相同。
  所有特征贡献之和 ≈ 预测值 - 全县均值（base_value）。

运行：
  python src/analysis/02_shap_analysis.py
"""

import os, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score

ROOT     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DATA = os.path.join(ROOT, "output", "data")
FIG_DIR  = os.path.join(ROOT, "output", "figures")
os.makedirs(FIG_DIR, exist_ok=True)

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

FEATURE_LABELS = {
    "tri_count_per_km2":        "TRI 工业设施密度 (/km²)",
    "superfund_count_per_km2":  "Superfund 场地密度 (/km²)",
    "pesticide_per_km2":        "农药使用密度 (lbs/km²)",
    "geotracker_count_per_km2": "GeoTracker 污染点密度 (/km²)",
    "school_lead_max":          "学校铅含量最大值 (ppb)",
    "is_imported":              "进口水系统",
    "is_groundwater":           "地下水系统",
    "wqp_砷":                   "地下水砷浓度 (µg/L)",
    "wqp_铅":                   "地下水铅浓度 (µg/L)",
    "wqp_硝酸盐":                "地下水硝酸盐 (mg/L)",
    "wqp_tds":                  "地下水溶解固体 TDS (mg/L)",
    "pfas_factor":              "PFAS 永久性化学物超标倍数",
    "lead_score":               "铅暴露风险评分（CES）",
    "housing_burden":           "住房负担评分（CES）",
    "has_advanced_treatment":   "有高级水处理工艺",
    "has_softening":            "有软化处理工艺",
    "n_treatment_steps":        "水处理工艺步骤数",
    "lsl_pct":                  "铅管占比（%）",
}

# ── 1. 加载数据 ────────────────────────────────────────────────────────────────
print("加载特征宽表...")
df = pd.read_csv(os.path.join(OUT_DATA, "system_features.csv"))
df = df.dropna(subset=["ewg_score"])
print(f"  {len(df)} 个供水系统，ewg_score 均值={df['ewg_score'].mean():.1f}x")

X_raw = df[FEATURE_COLS].fillna(0)
y     = df["ewg_score"]

# ── 2. 标准化 + 训练 Ridge ────────────────────────────────────────────────────
print("训练 Ridge 线性回归...")
scaler = StandardScaler()
X_std  = scaler.fit_transform(X_raw)

model  = Ridge(alpha=10)
model.fit(X_std, y)

cv_folds = min(5, len(df))
cv_r2 = cross_val_score(model, X_std, y, cv=cv_folds, scoring="r2")
print(f"  {cv_folds}-fold CV R²: {cv_r2.mean():.3f} ± {cv_r2.std():.3f}")
print(f"  训练集 R²:    {model.score(X_std, y):.3f}")

base_value = float(y.mean())   # 全县均值 = 线性模型截距的直观等价
print(f"  全县均值 (base): {base_value:.2f}")

# 标准化系数（绝对值大 = 控制其他变量后净效应强）
coef = model.coef_   # shape (n_features,)
print("\n── 全局贡献度排名（标准化回归系数，控制其他变量后净效应）──")
ranked = sorted(zip(FEATURE_COLS, coef), key=lambda x: abs(x[1]), reverse=True)
max_abs = max(abs(c) for _, c in ranked)
for rank, (feat, c) in enumerate(ranked, 1):
    bar = "█" * int(abs(c) / max_abs * 20)
    sign = "+" if c >= 0 else "-"
    print(f"  {rank:>2}. {FEATURE_LABELS[feat]:<28} {sign}{abs(c):6.1f}  {bar}")

# ── 3. 全局特征重要性图 ────────────────────────────────────────────────────────
order = np.argsort(np.abs(coef))
fig, ax = plt.subplots(figsize=(9, 5.5))
colors = ["#c0392b" if coef[i] >= 0 else "#2980b9" for i in order]
ax.barh([FEATURE_LABELS[FEATURE_COLS[i]] for i in order],
        coef[order], color=colors, alpha=0.85)
ax.axvline(0, color="#333", lw=0.8)
ax.set_xlabel("标准化回归系数（控制其他变量后对 EWG 评分的净效应）", fontsize=10)
ax.set_title("LA County 水质影响因素排名（线性回归·全局）", fontsize=13, fontweight="bold", pad=12)
ax.spines[["top","right"]].set_visible(False)
plt.tight_layout()
fig_path = os.path.join(FIG_DIR, "coef_global.png")
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
print(f"\n  ✓ {fig_path}")
plt.close()

# ── 4. 输出 system_rootcause.json ─────────────────────────────────────────────
print("生成 system_rootcause.json...")

# 每个系统的线性贡献分解：β_i × (x_i - μ_i) / σ_i
# = 标准化系数 × 标准化后的特征值
feat_means = scaler.mean_
feat_stds  = scaler.scale_

system_out = {}
for _, row in df.iterrows():
    pwsid = str(row["pwsid"])
    x_raw = np.array([float(row.get(f, 0) or 0) for f in FEATURE_COLS])
    x_std = (x_raw - feat_means) / feat_stds

    # 每个特征的贡献（单位：ewg_score 分值）
    contrib = coef * x_std
    contrib_dict = {feat: round(float(contrib[j]), 2) for j, feat in enumerate(FEATURE_COLS)}

    # 最主要的正贡献因素（推高评分的首因）
    pos = {k: v for k, v in contrib_dict.items() if v > 0}
    dominant = max(pos, key=pos.get) if pos else max(contrib_dict, key=lambda k: abs(contrib_dict[k]))

    system_out[pwsid] = {
        "system_name":       str(row.get("system_name", "")),
        "ewg_score":         round(float(row["ewg_score"]), 4),
        "n_contaminants":    int(row.get("n_contaminants", 0)),
        "worst_contaminant": str(row.get("worst_contaminant", "")),
        "dominant_cause":    dominant,
        "dominant_label":    FEATURE_LABELS[dominant],
        "contribution":      contrib_dict,
        "features":          {feat: round(float(row.get(feat, 0) or 0), 3) for feat in FEATURE_COLS},
    }

out = {
    "meta": {
        "feature_labels": FEATURE_LABELS,
        "method":         "Ridge线性回归贡献分解",
        "cv_r2_mean":     round(float(cv_r2.mean()), 4),
        "cv_r2_std":      round(float(cv_r2.std()),  4),
        "base_value":     round(base_value, 4),
        "n_systems":      len(df),
    },
    "system": system_out,
}

out_path = os.path.join(OUT_DATA, "system_rootcause.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
print(f"  ✓ {out_path}  ({len(system_out)} 个供水系统)")
print("\n✅ 分析完成")
