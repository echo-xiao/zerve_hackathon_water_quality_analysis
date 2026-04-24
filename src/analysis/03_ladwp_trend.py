"""
03_ladwp_trend.py — LADWP 年报 PDF 趋势提取（2004-2024）

输入：data/raw_data/ladwp_pdf/LADWP_DWQR_{year}.pdf
输出：
  output/data/ladwp_trend.json  — 年度 × 污染物均值时序
  output/figures/ladwp_trend.png — 关键污染物 20 年趋势图

运行：python src/analysis/03_ladwp_trend.py
"""

import os, re, json
import pdfplumber
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

ROOT     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PDF_DIR  = os.path.join(ROOT, "data", "raw_data", "ladwp_pdf")
OUT_DATA = os.path.join(ROOT, "output", "data")
FIG_DIR  = os.path.join(ROOT, "output", "figures")
os.makedirs(FIG_DIR, exist_ok=True)

YEARS = list(range(2004, 2025))

# 要提取的污染物关键字 → 显示名称
CONTAMINANTS = {
    "Arsenic":        ("砷 Arsenic",       "µg/L", 10),
    "Nitrate":        ("硝酸盐 Nitrate",    "mg/L", 10),
    "Fluoride":       ("氟化物 Fluoride",   "mg/L",  2),
    "Chromium":       ("六价铬 Chromium-6", "µg/L", 10),
    "Bromate":        ("溴酸盐 Bromate",    "µg/L", 10),
    "Uranium":        ("铀 Uranium",        "pCi/L",20),
    "Turbidity":      ("浊度 Turbidity",    "NTU",   1),
    "Gross Alpha":    ("总α放射性",         "pCi/L",15),
}

MCL = {k: v[2] for k, v in CONTAMINANTS.items()}
LABEL = {k: v[0] for k, v in CONTAMINANTS.items()}


def get_full_text(year: int) -> str:
    path = os.path.join(PDF_DIR, f"LADWP_DWQR_{year}.pdf")
    if not os.path.exists(path):
        return ""
    with pdfplumber.open(path) as pdf:
        parts = []
        for page in pdf.pages:
            raw = page.extract_text() or ""
            # 2024 PDF 文字镜像，逐行反转
            if year == 2024:
                raw = "\n".join(line[::-1] for line in raw.split("\n"))
            parts.append(raw)
    return "\n".join(parts)


def extract_averages(text: str, keyword: str, mcl: float, year: int) -> list[float]:
    """
    在 keyword 附近提取实际测量均值，过滤掉 MCL/PHG 等标准值。
    - 2024: 值在关键字之前（PDF 镜像排版）
    - 其他年: 值在关键字之后，跳过 MCL/PHG
    """
    idx = text.find(keyword)
    if idx == -1:
        return []

    # 已知要过滤的标准值集合（MCL / PHG 等）
    known_standards = {10, 1000, 600, 2000, 0.004, 0.02, 0.1, 15, 20, 50, 5, 2, 4, 80, 500, 300}
    known_standards.add(mcl)

    if year == 2024:
        # 2024 镜像：值在关键字之前
        snippet = text[max(0, idx - 600): idx]
    else:
        # 其他年：找到 YES/NO 之后的实际测量值
        snippet_raw = text[idx: idx + 600]
        # 跳过 "YES xx 0.xxx" 模式（MCL + PHG）
        m = re.search(r'(YES|NO)\s+[\d.]+\s+[\d.]+\s*(.*)', snippet_raw, re.DOTALL)
        snippet = m.group(2) if m else snippet_raw[50:]

    tokens = re.findall(r"<?\s*(\d+\.?\d*)", snippet)
    vals = []
    for t in tokens:
        try:
            v = float(t)
            # 只保留合理范围内、非已知标准值的数
            if 0 < v < 200 and v not in known_standards and abs(v - round(v)) > 0.001 or (0 < v <= 10 and v not in known_standards):
                vals.append(v)
        except ValueError:
            pass
    # 去重后取小值（实际浓度通常低于 MCL）
    vals = [v for v in vals if v < mcl * 1.5]
    return vals[:8]


def representative_value(vals: list[float]) -> float | None:
    """取所有值的中位数作为该年该污染物的代表值"""
    if not vals:
        return None
    vals_sorted = sorted(vals)
    n = len(vals_sorted)
    mid = n // 2
    return vals_sorted[mid] if n % 2 == 1 else (vals_sorted[mid - 1] + vals_sorted[mid]) / 2


# ── 主循环 ────────────────────────────────────────────────────────────────────
print("提取 LADWP 年报数据（2004-2024）...")
records = []

for year in YEARS:
    text = get_full_text(year)
    if not text:
        print(f"  {year}: PDF 未找到，跳过")
        continue

    row = {"year": year}
    for kw in CONTAMINANTS:
        vals = extract_averages(text, kw, MCL[kw], year)
        row[kw] = representative_value(vals)

    # 简单打印关键值
    arsenic = f"{row['Arsenic']:.1f}" if row['Arsenic'] else "N/A"
    nitrate = f"{row['Nitrate']:.2f}" if row['Nitrate'] else "N/A"
    fluoride = f"{row['Fluoride']:.2f}" if row['Fluoride'] else "N/A"
    print(f"  {year}: 砷={arsenic} µg/L  硝酸盐={nitrate} mg/L  氟化物={fluoride} mg/L")
    records.append(row)

df = pd.DataFrame(records).set_index("year")

# ── 保存 JSON ────────────────────────────────────────────────────────────────
out = {
    "meta": {
        "source": "LADWP Annual Drinking Water Quality Reports 2004-2024",
        "contaminants": LABEL,
        "mcl": MCL,
        "note": "中位数值，来自各水处理厂 Average 列",
    },
    "data": {
        str(year): {kw: row[kw] for kw in CONTAMINANTS}
        for year, row in df.iterrows()
    },
}
out_path = os.path.join(OUT_DATA, "ladwp_trend.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2, default=lambda x: None if pd.isna(x) else x)
print(f"\n✓ {out_path}")

# ── 趋势图 ────────────────────────────────────────────────────────────────────
PLOT_CONTAMS = ["Arsenic", "Nitrate", "Fluoride", "Chromium", "Bromate", "Uranium"]
fig, axes = plt.subplots(2, 3, figsize=(15, 8))
axes = axes.flatten()

colors = ["#c0392b", "#2980b9", "#27ae60", "#8e44ad", "#e67e22", "#16a085"]

for ax, (kw, color) in zip(axes, zip(PLOT_CONTAMS, colors)):
    label = LABEL[kw]
    mcl   = MCL[kw]
    series = df[kw].dropna()

    ax.plot(series.index, series.values, "o-", color=color, lw=2, ms=5, label=label)
    ax.axhline(mcl, color="#e74c3c", lw=1, ls="--", alpha=0.6, label=f"MCL={mcl}")
    ax.set_title(label, fontsize=11, fontweight="bold")
    ax.set_xlabel("年份", fontsize=9)
    ax.set_ylabel(CONTAMINANTS[kw][1], fontsize=9)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(4))
    ax.tick_params(labelsize=8)
    ax.legend(fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)

    # 标注趋势
    if len(series) >= 4:
        first4 = series.iloc[:4].mean()
        last4  = series.iloc[-4:].mean()
        if last4 < first4 * 0.85:
            trend = "↓ 改善"
            tc = "#27ae60"
        elif last4 > first4 * 1.15:
            trend = "↑ 恶化"
            tc = "#c0392b"
        else:
            trend = "→ 稳定"
            tc = "#7f8c8d"
        ax.text(0.97, 0.95, trend, transform=ax.transAxes,
                ha="right", va="top", fontsize=10, color=tc, fontweight="bold")

plt.suptitle("LADWP 饮用水水质 20 年趋势（2004–2024）", fontsize=14, fontweight="bold", y=1.01)
plt.tight_layout()
fig_path = os.path.join(FIG_DIR, "ladwp_trend.png")
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"✓ {fig_path}")
print("\n✅ 完成")
