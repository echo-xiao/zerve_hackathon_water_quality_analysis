"""
08_subgroup.py — 子群分析：异质性处理效应

对每个核心处理变量，按以下维度分群比较 ATE：
  1. 气候分群：干旱县 vs 湿润县（precip_deficit 中位切分）
  2. 土壤分群：好土壤 vs 差土壤（awc_mean 中位切分）
  3. 地理分群：West / Midwest / South / Northeast
  4. 聚类分群：06_cluster 的 3 个县型

目的：找出整体平均效应掩盖的子群差异
"""

import os, json, warnings
import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
warnings.filterwarnings("ignore")

sns.set_theme(style="whitegrid")

from sklearn.linear_model import LogisticRegressionCV
from sklearn.impute import SimpleImputer

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _shared import OUTPUT_DIR, IMAGES_DIR, LOG_TARGET, available_cols

# 处理变量（与 04_causal 一致）
TREATMENTS = [
    {"col": "poverty_rate",      "label": "High Poverty Rate",
     "confounders": ["precip_deficit_in", "drought_intensity", "elevation_ft",
                     "awc_mean", "clay_pct", "median_income", "avg_farm_size_ac"]},
    {"col": "avg_farm_size_ac",  "label": "Large Farm Size",
     "confounders": ["precip_deficit_in", "drought_intensity", "elevation_ft",
                     "awc_mean", "clay_pct", "median_income", "poverty_rate"]},
    {"col": "crop_diversity_hhi","label": "High Crop Concentration",
     "confounders": ["precip_deficit_in", "drought_intensity", "elevation_ft",
                     "awc_mean", "clay_pct", "median_income", "poverty_rate",
                     "avg_farm_size_ac"]},
]

# 地理分群映射（州 FIPS → 区域）
REGION_MAP = {
    "west":     {"04","06","08","16","30","32","35","41","49","53","56"},  # AZ CA CO ID MT NV NM OR UT WA WY
    "midwest":  {"17","18","19","20","26","27","29","31","38","39","46","55"},  # IL IN IA KS MI MN MO NE ND OH SD WI
    "south":    {"01","05","10","12","13","21","22","24","28","37","40","45","47","48","51","54"},
    "northeast":{"09","23","25","33","34","36","42","44","50"},
}

def _state_to_region(state_fips: str) -> str:
    sf = str(state_fips).zfill(2)
    for reg, fips_set in REGION_MAP.items():
        if sf in fips_set:
            return reg
    return "other"


def _ipw_ate(sub: pd.DataFrame, treatment_col: str, conf_cols: list) -> dict | None:
    """在子群内用 IPW 估计 ATE，返回 {ate, n, n_treat, n_ctrl}"""
    from scipy import stats as scipy_stats

    avail = available_cols(sub, conf_cols)
    needed = [treatment_col, LOG_TARGET] + avail
    dm = sub[needed].apply(pd.to_numeric, errors="coerce").dropna(
        subset=[treatment_col, LOG_TARGET])
    dm = dm.dropna(thresh=max(3, int(len(needed) * 0.4)))

    if len(dm) < 40:
        return None

    threshold = dm[treatment_col].median()
    dm = dm.copy()
    dm["T"] = (dm[treatment_col] > threshold).astype(int)
    n_t, n_c = int(dm["T"].sum()), int((dm["T"] == 0).sum())
    if n_t < 15 or n_c < 15:
        return None

    X = pd.DataFrame(
        SimpleImputer(strategy="median").fit_transform(dm[avail]),
        columns=avail, index=dm.index)
    T_arr = dm["T"].values
    Y_arr = dm[LOG_TARGET].values

    naive_ate = float(np.mean(Y_arr[T_arr == 1]) - np.mean(Y_arr[T_arr == 0]))
    _, p_val = scipy_stats.ttest_ind(Y_arr[T_arr == 1], Y_arr[T_arr == 0])

    if len(avail) >= 2:
        try:
            ps = LogisticRegressionCV(cv=5, max_iter=1000, random_state=42).fit(
                X, T_arr).predict_proba(X)[:, 1]
            raw_w = np.where(T_arr == 1, 1 / ps, 1 / (1 - ps))
            w = np.clip(raw_w, 0, np.percentile(raw_w, 95))
            ate = float(np.average(Y_arr * (2 * T_arr - 1), weights=w))
        except Exception:
            ate = naive_ate
    else:
        ate = naive_ate

    return {
        "ate": round(ate, 4),
        "ate_pct": round((np.exp(ate) - 1) * 100, 1),
        "naive_ate": round(naive_ate, 4),
        "p_value": round(float(p_val), 4),
        "n": len(dm), "n_treat": n_t, "n_ctrl": n_c,
    }


def _make_subgroups(df: pd.DataFrame) -> dict:
    groups = {}

    # 1. Climate subgroups
    if "precip_deficit_in" in df.columns:
        med = df["precip_deficit_in"].median()
        groups["Arid Counties"]  = df[df["precip_deficit_in"] > med]
        groups["Humid Counties"] = df[df["precip_deficit_in"] <= med]

    # 2. Soil subgroups
    if df["awc_mean"].notna().sum() > 200:
        med = df["awc_mean"].median()
        groups["Good Soil (High AWC)"] = df[df["awc_mean"] > med]
        groups["Poor Soil (Low AWC)"]  = df[df["awc_mean"] <= med]

    # 3. Geographic subgroups
    if "state_fips" in df.columns:
        df = df.copy()
        df["_region"] = df["state_fips"].apply(_state_to_region)
        for reg in ["west", "midwest", "south", "northeast"]:
            sub = df[df["_region"] == reg]
            if len(sub) >= 50:
                label_map = {"west": "West", "midwest": "Midwest",
                             "south": "South", "northeast": "Northeast"}
                groups[label_map[reg]] = sub

    # 4. Cluster subgroups
    if "cluster_name" in df.columns:
        for name, sub in df.groupby("cluster_name"):
            if len(sub) >= 40:
                groups[f"[{name}]"] = sub

    return groups


def run(df: pd.DataFrame, climate_cols: list, soil_cols: list, human_cols: list):
    print("\n" + "=" * 50)
    print("  08 子群分析 — 异质性处理效应")
    print("=" * 50)

    groups = _make_subgroups(df)
    print(f"  子群数量：{len(groups)}")
    for g, sub in groups.items():
        print(f"    {g}: {len(sub)} 县")

    all_results = {}

    for t in TREATMENTS:
        col, label, conf = t["col"], t["label"], t["confounders"]
        if col not in df.columns:
            print(f"\n  ✗ {label}: 列不存在")
            continue

        print(f"\n  ── {label} ──")
        t_results = {}

        for gname, gsub in groups.items():
            res = _ipw_ate(gsub, col, conf)
            if res is None:
                print(f"    {gname:<20} 样本不足")
                continue
            sig = "✓" if res["p_value"] < 0.05 else " "
            print(f"    {gname:<20} ATE={res['ate']:+.4f} ({res['ate_pct']:+.1f}%)  "
                  f"n={res['n']}  p={res['p_value']:.3f} {sig}")
            t_results[gname] = res

        all_results[label] = t_results

    # ── 可视化：子群 ATE 对比图 ─────────────────────────────────────
    _plot_subgroup_comparison(all_results)

    # ── 关键发现汇总 ────────────────────────────────────────────────
    findings = _extract_findings(all_results)
    print("\n  ── 关键异质性发现 ──")
    for f in findings:
        print(f"    {f}")

    out = {"subgroup_ates": all_results, "key_findings": findings}
    with open(os.path.join(OUTPUT_DIR, "08_subgroup.json"), "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n  输出：08_subgroup.png / 08_subgroup.json")
    return out


def _plot_subgroup_comparison(all_results: dict):
    n_treatments = len(all_results)
    if n_treatments == 0:
        return

    fig, axes = plt.subplots(1, n_treatments, figsize=(6 * n_treatments, 7))
    if n_treatments == 1:
        axes = [axes]

    GROUP_ORDER = ["Arid Counties","Humid Counties","Good Soil (High AWC)","Poor Soil (Low AWC)",
                   "West","Midwest","South","Northeast"]
    colors = {"Arid Counties":        "#EF5350",
              "Humid Counties":       "#42A5F5",
              "Good Soil (High AWC)": "#66BB6A",
              "Poor Soil (Low AWC)":  "#FFA726",
              "West":                 "#AB47BC",
              "Midwest":              "#26C6DA",
              "South":                "#FF7043",
              "Northeast":            "#78909C"}

    for ax, (label, t_res) in zip(axes, all_results.items()):
        plot_groups = {k: v for k, v in t_res.items() if not k.startswith("[")}
        if not plot_groups:
            plot_groups = t_res

        # Sort by GROUP_ORDER
        ordered = [(g, plot_groups[g]) for g in GROUP_ORDER if g in plot_groups]
        ordered += [(g, v) for g, v in plot_groups.items() if g not in GROUP_ORDER]

        gnames = [g for g, _ in ordered]
        ates   = [v["ate_pct"] for _, v in ordered]
        errs   = [abs(v["ate_pct"]) * 0.15 for _, v in ordered]
        cols   = [colors.get(g, "#90A4AE") for g in gnames]
        sig    = [v["p_value"] < 0.05 for _, v in ordered]

        plot_df = pd.DataFrame({
            "group":     gnames,
            "ate_pct":   ates,
            "err":       errs,
            "color":     cols,
            "sig":       sig,
        })
        sns.barplot(data=plot_df, x="ate_pct", y="group",
                    palette=dict(zip(gnames, cols)),
                    hue="group", dodge=False, legend=False,
                    alpha=0.85, edgecolor="white", linewidth=0.5, ax=ax)
        # error bars
        for i, (_, row) in enumerate(plot_df.iterrows()):
            ax.errorbar(row["ate_pct"], i, xerr=row["err"],
                        fmt="none", color="#555555", capsize=4, linewidth=1.2)
        ax.axvline(0, color="#333333", linewidth=0.9)

        span = max(ates + [0]) - min(ates + [0])
        for i, (_, row) in enumerate(plot_df.iterrows()):
            ate    = row["ate_pct"]
            x_off  = span * 0.03
            ha     = "left" if ate >= 0 else "right"
            x      = ate + (x_off if ate >= 0 else -x_off)
            ax.text(x, i, f"{ate:+.1f}%" + (" *" if row["sig"] else ""),
                    va="center", ha=ha, fontsize=9,
                    color="#1A237E" if row["sig"] else "#555555",
                    fontweight="bold" if row["sig"] else "normal")

        ax.set_xlabel("Change in Water Efficiency (%)", fontsize=11)
        ax.set_ylabel("")
        ax.set_title(f'"{label}"\nSubgroup Heterogeneous Effects', fontsize=11, fontweight="bold")
        margin = max(abs(v) for v in ates + [1]) * 0.5
        ax.set_xlim(min(ates + [0]) - margin, max(ates + [0]) + margin)
        sns.despine(ax=ax, left=True)

    plt.suptitle("Treatment Effect Heterogeneity by Subgroup  (IPW-ATE, * = p<0.05)",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(os.path.join(IMAGES_DIR, "08_subgroup.png"),
                dpi=150, bbox_inches="tight")
    plt.close()


def _extract_findings(all_results: dict) -> list:
    findings = []
    for label, t_res in all_results.items():
        # 找到最大与最小 ATE 的子群
        valid = {k: v for k, v in t_res.items()
                 if v["p_value"] < 0.1 and not k.startswith("[")}
        if len(valid) < 2:
            continue
        sorted_res = sorted(valid.items(), key=lambda x: x[1]["ate_pct"])
        worst = sorted_res[0]
        best  = sorted_res[-1]
        diff  = worst[1]["ate_pct"] - best[1]["ate_pct"]
        if abs(diff) > 5:
            findings.append(
                f"{label}：效应最强在「{worst[0]}」({worst[1]['ate_pct']:+.1f}%)，"
                f"最弱在「{best[0]}」({best[1]['ate_pct']:+.1f}%)，差距 {abs(diff):.1f}pct"
            )
    return findings


if __name__ == "__main__":
    from _shared import load_features, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
    df = load_features()
    run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
