"""
07_insights.py — Actionable Insights

Three policy insight categories:
  1. Low-Hanging Fruit    — Good climate/soil but structural constraints; highest intervention ROI
  2. Virtual Water Export — Arid counties exporting water-intensive crops (policy warning)
  3. Dual Exposure        — High drought risk AND high irrigation dependence

用法（独立）：  python src/analysis/07_insights.py
被 run_analysis.py 调用：  run(df, climate_cols, soil_cols, human_cols)
"""

import os, json, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _shared import OUTPUT_DIR, TARGET, available_cols

TOP_N = 200  # 每类最多输出县数（地图显示用）


def _safe_pct(s: pd.Series) -> pd.Series:
    """百分位排名 0-100，越高越极端"""
    return s.rank(pct=True) * 100


def _county_record(row, extra: dict = None) -> dict:
    base = {
        "fips": str(row.get("fips", "")),
        "county": str(row.get("county", "")),
        "state": str(row.get("state", "")),
        "crop_water_eff": round(float(row.get(TARGET, 0) or 0), 2),
        "eff_percentile": round(float(row.get("eff_percentile", 0) or 0), 1),
    }
    if extra:
        base.update(extra)
    return base


def find_low_hanging_fruit(df: pd.DataFrame) -> list:
    """
    Low-Hanging Fruit: decent climate/soil but structural constraints
    (poverty / large farms) causing low efficiency → highest intervention ROI
    """
    cols = [TARGET, "eff_percentile", "precip_deficit_in", "awc_mean",
            "poverty_rate", "avg_farm_size_ac", "fips", "county", "state"]
    dm = df[[c for c in cols if c in df.columns]].dropna(subset=[TARGET]).copy()

    # 气候压力不极端：降水赤字 < P60
    climate_ok = dm["precip_deficit_in"] < dm["precip_deficit_in"].quantile(0.60)

    # 土壤尚可：awc > P40（有数据时）
    if dm["awc_mean"].notna().sum() > 100:
        soil_ok = dm["awc_mean"].fillna(dm["awc_mean"].median()) > dm["awc_mean"].quantile(0.40)
    else:
        soil_ok = pd.Series(True, index=dm.index)

    # 结构性约束：高贫困 OR 大农场（两者都是改善空间的来源）
    structural = (
        (dm["poverty_rate"] > dm["poverty_rate"].quantile(0.60)) |
        (dm["avg_farm_size_ac"] > dm["avg_farm_size_ac"].quantile(0.70))
    )

    # 效率偏低：< P40
    eff_low = dm["eff_percentile"] < 40

    sel = dm[climate_ok & soil_ok & structural & eff_low].sort_values(TARGET)

    results = []
    for _, row in sel.head(TOP_N).iterrows():
        results.append(_county_record(row, {
            "precip_deficit_in": round(float(row.get("precip_deficit_in") or 0), 1),
            "poverty_rate": round(float(row.get("poverty_rate") or 0), 1),
            "avg_farm_size_ac": round(float(row.get("avg_farm_size_ac") or 0), 0),
            "insight": "Low-Hanging Fruit",
        }))
    return results


def find_virtual_water_exporters(df: pd.DataFrame) -> list:
    """
    Virtual Water Exporters: arid counties (high precip deficit) + large irrigation
    scale + low efficiency → exporting scarce water embedded in crops
    """
    dm = df[["fips","county","state", TARGET, "eff_percentile",
             "precip_deficit_in", "drought_intensity",
             "irrigated_area_ac", "avg_farm_size_ac"]].dropna(subset=[TARGET]).copy()

    # 干旱压力高：降水赤字 > P70（真正干旱）
    drought = dm["precip_deficit_in"] > dm["precip_deficit_in"].quantile(0.70)

    # 大灌溉规模：irrigated_area_ac > P65（真正依赖灌溉）
    high_irr = dm["irrigated_area_ac"] > dm["irrigated_area_ac"].quantile(0.65)

    # 效率偏低：< P45
    eff_low = dm["eff_percentile"] < 45

    sel = dm[drought & high_irr & eff_low].copy()
    sort_col = "precip_deficit_in"
    sel = sel.sort_values(sort_col, ascending=False)

    results = []
    for _, row in sel.head(TOP_N).iterrows():
        crop_total = round(float(row.get("_crop_total", 0) or 0) / 1e6, 2)
        deficit = round(float(row.get("precip_deficit_in", 0) or 0), 1)
        results.append(_county_record(row, {
            "precip_deficit_in": deficit,
            "water_intensive_crop_M_bu": crop_total,
            "insight": "Virtual Water Exporter",
        }))
    return results


def find_dual_exposure(df: pd.DataFrame) -> list:
    """
    Dual Exposure: extreme drought stress + high irrigation-dependent farm scale
    → counties at risk of unsustainable water use under climate change
    Uses irrigated_area_ac/precip_avg_in as proxy for rain-supported irrigation ratio
    """
    dm = df[["fips","county","state", TARGET, "eff_percentile",
             "precip_deficit_in", "drought_intensity",
             "irrigated_area_ac", "precip_avg_in",
             "avg_farm_size_ac"]].dropna(subset=[TARGET]).copy()

    # 高干旱压力：降水赤字 > P65
    high_drought = dm["precip_deficit_in"] > dm["precip_deficit_in"].quantile(0.65)

    # 高灌溉强度：灌溉面积大 且 降雨稀少（irrigated_area / precip 高）
    dm["_irr_intensity"] = dm["irrigated_area_ac"] / dm["precip_avg_in"].replace(0, np.nan)
    high_irr = dm["_irr_intensity"] > dm["_irr_intensity"].quantile(0.65)

    # 经济暴露：作物产值高（损失大）
    high_exposure = dm["eff_percentile"] > 30  # 有实质农业产出

    sel = dm[high_drought & high_irr & high_exposure].copy()

    def _norm(s):
        rng = s.max() - s.min()
        return (s - s.min()) / rng if rng > 0 else s * 0

    sel["_risk_score"] = _norm(sel["precip_deficit_in"]) + _norm(sel["_irr_intensity"])
    sel = sel.sort_values("_risk_score", ascending=False)

    results = []
    for _, row in sel.head(TOP_N).iterrows():
        results.append(_county_record(row, {
            "precip_deficit_in": round(float(row.get("precip_deficit_in") or 0), 1),
            "irrigated_area_ac": round(float(row.get("irrigated_area_ac") or 0), 0),
            "irr_intensity": round(float(row.get("_irr_intensity") or 0), 2),
            "insight": "Dual Exposure",
        }))
    return results


def run(df: pd.DataFrame, climate_cols: list, soil_cols: list, human_cols: list):
    print("\n" + "="*50)
    print("  07 Actionable Insights")
    print("="*50)

    # 确保有效率百分位
    if "eff_percentile" not in df.columns and TARGET in df.columns:
        df = df.copy()
        df["eff_percentile"] = (df[TARGET].rank(pct=True) * 100).round(1)

    lhf  = find_low_hanging_fruit(df)
    vwe  = find_virtual_water_exporters(df)
    dual = find_dual_exposure(df)

    print(f"  Low-Hanging Fruit: {len(lhf)} counties")
    print(f"  Virtual Water Exporters: {len(vwe)} counties")
    print(f"  Dual Exposure: {len(dual)} counties")

    # Top policy priority counties = Low-Hanging Fruit (highest improvement potential)
    top50 = lhf[:TOP_N]

    # 把 insight flag 写回 df，供 county_wide.json 使用
    lhf_fips  = {r["fips"] for r in lhf}
    vwe_fips  = {r["fips"] for r in vwe}
    dual_fips = {r["fips"] for r in dual}
    df["is_lhf"]  = df["fips"].astype(str).isin(lhf_fips).astype(int)
    df["is_vwe"]  = df["fips"].astype(str).isin(vwe_fips).astype(int)
    df["is_dual"] = df["fips"].astype(str).isin(dual_fips).astype(int)

    result = {
        "low_hanging_fruit": lhf,
        "virtual_water_exporters": vwe,
        "dual_exposure": dual,
        "top50_policy_priority": top50,
    }

    out_path = os.path.join(OUTPUT_DIR, "07_insights.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"  结果 → {out_path}")

    return result


if __name__ == "__main__":
    from _shared import load_features, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
    df = load_features()
    run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
