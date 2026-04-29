"""
04_causal.py — 因果推断：灌溉技术采纳 → 水效率

研究问题：在控制气候、土壤、社会经济条件后，
          采用中心轴喷灌/滴灌技术的县，水效率有多大提升？

方法：
  - 处理变量（T）：centerpivot_ratio > 0.3（高喷灌采纳）
  - 结果变量（Y）：log_crop_water_eff
  - 混淆变量（X）：气候 + 土壤 + 社会经济
  - 估计量：DRLearner（Doubly Robust，对模型误设鲁棒）
  - 输出：ATE + CATE 分布（哪类县受益最大）

用法（独立）：  python src/analysis/04_causal.py
被 run_analysis.py 调用：  run(df, climate_cols, soil_cols, human_cols)
"""

import os, json, warnings
import numpy as np
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegressionCV

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _shared import OUTPUT_DIR, LOG_TARGET, available_cols




def _run_one(df, treatment_col, label, confounder_cols):
    """对单个处理变量估计因果效应，返回 result dict"""
    from scipy import stats as scipy_stats
    # 去重（保序）并排除处理变量本身
    conf_cols = list(dict.fromkeys(
        c for c in available_cols(df, confounder_cols)
        if c != treatment_col and c != LOG_TARGET
    ))
    needed = list(dict.fromkeys([treatment_col, LOG_TARGET] + conf_cols))
    sub = df[needed].apply(pd.to_numeric, errors="coerce").dropna(
        subset=[treatment_col, LOG_TARGET])
    sub = sub.dropna(thresh=int(len(needed) * 0.5))
    # 过滤掉在 sub 中不存在或全为 NaN 的列（SimpleImputer 会丢掉全空列导致形状不一致）
    conf_cols = [c for c in conf_cols if c in sub.columns and sub[c].notna().any()]

    if len(sub) < 60:
        print(f"  ✗ {label}: 有效样本 {len(sub)} 不足，跳过")
        return None

    threshold = sub[treatment_col].median()
    sub = sub.copy()
    sub["T"] = (sub[treatment_col] > threshold).astype(int)
    treat_n, ctrl_n = int(sub["T"].sum()), int((sub["T"]==0).sum())
    if treat_n < 30 or ctrl_n < 30:
        print(f"  ✗ {label}: 处理/对照组不足")
        return None

    X = pd.DataFrame(
        SimpleImputer(strategy="median").fit_transform(sub[conf_cols]),
        columns=conf_cols, index=sub.index)
    T_arr = sub["T"].values
    Y_arr = sub[LOG_TARGET].values

    naive_ate = float(np.mean(Y_arr[T_arr==1]) - np.mean(Y_arr[T_arr==0]))
    _, p_val = scipy_stats.ttest_ind(Y_arr[T_arr==1], Y_arr[T_arr==0])
    print(f"\n  [{label}]  n={len(sub)}  阈值={threshold:.2f}")
    print(f"  简单对比 ATE = {naive_ate:+.4f}  p={p_val:.4f}")

    result = {"label": label, "treatment_col": treatment_col,
              "threshold": round(threshold, 4),
              "n_treated": treat_n, "n_control": ctrl_n,
              "naive_ate": round(naive_ate, 4),
              "naive_p": round(float(p_val), 4)}

    try:
        from causalml.inference.meta import BaseDRLearner
        from sklearn.ensemble import RandomForestRegressor
        dr = BaseDRLearner(
            learner=RandomForestRegressor(n_estimators=200, max_depth=6,
                                          min_samples_leaf=5, random_state=42),
            treatment_effect_learner=GradientBoostingRegressor(
                n_estimators=200, max_depth=4, random_state=42))
        dr.fit(X.values, T_arr, Y_arr)
        cate = dr.predict(X.values).flatten()
        ate  = float(np.mean(cate))
        ate_se = float(np.std(cate) / np.sqrt(len(cate)))
        print(f"  DRLearner ATE = {ate:+.4f} ± {ate_se:.4f}  (~{(np.exp(ate)-1)*100:.1f}%)")
        if "precip_deficit_in" in sub.columns:
            med = sub["precip_deficit_in"].median()
            sub2 = sub.copy(); sub2["cate"] = cate
            arid  = float(sub2[sub2["precip_deficit_in"] > med]["cate"].mean())
            humid = float(sub2[sub2["precip_deficit_in"] <= med]["cate"].mean())
            print(f"  CATE: 干旱县 {arid:+.4f}  湿润县 {humid:+.4f}")
            result["cate_by_climate"] = {"干旱县": round(arid,4), "湿润县": round(humid,4)}
        result.update({"method":"DRLearner","ate":round(ate,4),
                       "ate_se":round(ate_se,4),
                       "ate_pct_change":round((np.exp(ate)-1)*100,1)})
    except ImportError:
        ps_model = LogisticRegressionCV(cv=5, max_iter=1000, random_state=42)
        ps_model.fit(X, T_arr)
        ps = ps_model.predict_proba(X)[:,1]
        w = np.clip(np.where(T_arr==1,1/ps,1/(1-ps)), 0, np.percentile(
            np.where(T_arr==1,1/ps,1/(1-ps)), 95))
        ate_ipw = float(np.average(Y_arr*(2*T_arr-1), weights=w))
        print(f"  IPW-ATE = {ate_ipw:+.4f}  (~{(np.exp(ate_ipw)-1)*100:.1f}%)")
        result.update({"method":"IPW","ate":round(ate_ipw,4),
                       "ate_pct_change":round((np.exp(ate_ipw)-1)*100,1)})
    return result


def run(df: pd.DataFrame, climate_cols: list, soil_cols: list, human_cols: list):
    print("\n" + "="*50)
    print("  04 因果推断 — 多处理变量")
    print("="*50)

    # 动态生成处理变量列表：每个人为因素轮流作为处理变量
    # 混淆变量 = 气候 + 土壤 + 其余人为因素
    h_avail = available_cols(df, human_cols)
    treatments = [
        {"col": col, "confounders": climate_cols + soil_cols +
         [c for c in h_avail if c != col]}
        for col in h_avail
    ]

    all_results = []
    for t in treatments:
        col = t["col"]
        if col not in df.columns:
            continue
        r = _run_one(df, col, col, t["confounders"])
        if r:
            all_results.append(r)

    if not all_results:
        print("  ✗ 所有处理变量均失败")
        return {}

    print("\n\n  ── 汇总 ──")
    print(f"  {'因素':<20} {'ATE':>8}  {'效率变化':>8}  {'p值':>6}")
    print(f"  {'-'*50}")
    for r in all_results:
        ate = r.get("ate", r.get("naive_ate", 0))
        pct = r.get("ate_pct_change", round((np.exp(ate)-1)*100, 1))
        p   = r.get("naive_p", "—")
        print(f"  {r['label']:<20} {ate:>+8.4f}  {pct:>+7.1f}%  {p:>6.4f}")

    out = {"treatments": all_results,
           # 兼容旧 API：取第一个有效结果
           "method": all_results[0].get("method",""),
           "ate": all_results[0].get("ate"),
           "ate_pct_change": all_results[0].get("ate_pct_change")}

    with open(os.path.join(OUTPUT_DIR, "04_causal.json"), "w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"\n  输出：04_causal.json")
    return out


if __name__ == "__main__":
    from water_efficiency import load_data, feature_engineering
    from _shared import CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
    df, _, __, ___ = feature_engineering(load_data())
    run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
