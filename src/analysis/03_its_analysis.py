"""
03_its_analysis.py — ITS 野火因果分析（Interrupted Time Series）

断点：2025-01-07（Palisades/Eaton 野火爆发）

输入：
  data/raw_data/aqs/wildfire_period_aqi.json  — EPA AQS PM2.5 日均值时序
  output/data/wqp_stations.json               — WQP 站点月度水质数据

输出：
  output/figures/its_aqs_pm25.png   — 空气质量 ITS 图（含反事实曲线）
  output/figures/its_wqp_heavy_metal.png  — 重金属 ITS 图
  stdout: ITS 回归系数表（β₂ 水平突变 + β₃ 趋势变化 + p 值）

方法（分段线性回归）：
  Y_t = β₀ + β₁·t + β₂·D_t + β₃·(t - T₀)·D_t + ε_t
  β₂：野火后浓度的即时水平变化
  β₃：野火后趋势斜率变化（负值 = 正在恢复）
  D_t = 1 if t >= 2025-01-07 else 0

运行：
  python src/analysis/03_its_analysis.py
"""

import os, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf
from datetime import datetime

ROOT     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(ROOT, "data", "raw_data")
OUT_DATA = os.path.join(ROOT, "output", "data")
FIG_DIR  = os.path.join(ROOT, "output", "figures")
os.makedirs(FIG_DIR, exist_ok=True)

FIRE_DATE = pd.Timestamp("2025-01-07")

# ═══════════════════════════════════════════════════════════════════════════════
# 分析 A：AQS PM2.5（日粒度）
# ═══════════════════════════════════════════════════════════════════════════════
print("── 分析 A：AQS PM2.5 ITS ──")
aqs_path = os.path.join(DATA_DIR, "aqs", "wildfire_period_aqi.json")

if os.path.exists(aqs_path):
    with open(aqs_path) as f:
        raw = json.load(f)

    # 取所有 PM2.5 站点的日均值（跨站点平均）
    records = raw.get("PM2.5", [])
    daily = (pd.DataFrame(records)
               .assign(date=lambda d: pd.to_datetime(d["date_local"]))
               .groupby("date")["aqi"].mean()
               .reset_index()
               .sort_values("date"))

    daily["t"]  = (daily["date"] - daily["date"].min()).dt.days
    daily["D"]  = (daily["date"] >= FIRE_DATE).astype(int)
    daily["tD"] = ((daily["date"] - FIRE_DATE).dt.days * daily["D"]).clip(lower=0)

    model = smf.ols("aqi ~ t + D + tD", data=daily).fit()
    print(model.summary().tables[1])

    # 反事实：D=0, tD=0
    cf = daily.copy()
    cf["D"] = 0; cf["tD"] = 0
    daily["pred"]       = model.predict(daily)
    daily["counterfact"]= model.predict(cf)

    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.scatter(daily["date"], daily["aqi"], s=8, alpha=0.4, color="#aaa", label="实测值")
    ax.plot(daily["date"], daily["pred"],       color="#c0392b", lw=2, label="ITS 拟合")
    ax.plot(daily["date"], daily["counterfact"],color="#2980b9", lw=1.5,
            linestyle="--", label="反事实（无野火）")
    ax.axvline(FIRE_DATE, color="#e67e22", lw=1.5, linestyle=":", label="野火爆发 2025-01-07")
    ax.set_ylabel("PM2.5 AQI", fontsize=11)
    ax.set_title(f"野火对空气质量的即时冲击（ITS）\nβ₂={model.params['D']:.1f}（p={model.pvalues['D']:.4f}）  "
                 f"β₃={model.params['tD']:.2f}/天", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9); ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    path = os.path.join(FIG_DIR, "its_aqs_pm25.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  ✓ {path}")
    plt.close()
else:
    print("  ⚠ AQS 数据未找到，跳过")

# ═══════════════════════════════════════════════════════════════════════════════
# 分析 B：WQP 重金属月度 ITS
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── 分析 B：WQP 重金属月度 ITS ──")
wqp_path = os.path.join(OUT_DATA, "wqp_stations.json")

if os.path.exists(wqp_path):
    with open(wqp_path) as f:
        wqp = json.load(f)

    TARGET_CONTAMINANTS = ["铅 Lead", "砷 Arsenic", "浊度 Turbidity", "铁 Iron"]
    data = wqp.get("data", {})

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()

    for ax, cont in zip(axes, TARGET_CONTAMINANTS):
        if cont not in data:
            ax.set_title(f"{cont}（无数据）"); continue

        # 跨所有站点取月均值
        month_means = {}
        for month, stations in data[cont].items():
            vals = list(stations.values())
            if vals:
                month_means[month] = np.mean(vals)

        ts = (pd.Series(month_means)
                .reset_index()
                .rename(columns={"index":"month", 0:"value"})
                .assign(date=lambda d: pd.to_datetime(d["month"] + "-01"))
                .sort_values("date"))

        ts["t"]  = range(len(ts))
        ts["D"]  = (ts["date"] >= FIRE_DATE).astype(int)
        ts["tD"] = (ts["t"] - ts[ts["D"]==1]["t"].min() if ts["D"].sum() > 0 else 0)
        ts["tD"] = ts.apply(lambda r: max(0, r["t"] - ts[ts["D"]==1]["t"].min())
                            if ts["D"].sum() > 0 else 0, axis=1)

        if ts["D"].sum() < 2:
            ax.plot(ts["date"], ts["value"], color="#555")
            ax.set_title(f"{cont}（野火后数据不足）")
            continue

        model = smf.ols("value ~ t + D + tD", data=ts).fit()
        cf = ts.copy(); cf["D"] = 0; cf["tD"] = 0

        ax.scatter(ts["date"], ts["value"], s=20, color="#aaa", zorder=3)
        ax.plot(ts["date"], model.predict(ts),    color="#c0392b", lw=2, label="ITS 拟合")
        ax.plot(ts["date"], model.predict(cf),    color="#2980b9", lw=1.5,
                linestyle="--", label="反事实")
        ax.axvline(FIRE_DATE, color="#e67e22", lw=1.5, linestyle=":", alpha=0.8)
        b2, p2 = model.params.get("D", 0), model.pvalues.get("D", 1)
        sig = "**" if p2 < 0.01 else ("*" if p2 < 0.05 else "")
        ax.set_title(f"{cont}\nβ₂={b2:.3f}{sig}（p={p2:.3f}）", fontsize=10, fontweight="bold")
        ax.legend(fontsize=7); ax.spines[["top","right"]].set_visible(False)

    plt.suptitle("野火对水质重金属的因果冲击（ITS 分段回归）", fontsize=13, fontweight="bold", y=1.01)
    plt.tight_layout()
    path = os.path.join(FIG_DIR, "its_wqp_heavy_metal.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  ✓ {path}")
    plt.close()
else:
    print("  ⚠ WQP 站点数据未找到，跳过")

print("\n✅ ITS 分析完成")
