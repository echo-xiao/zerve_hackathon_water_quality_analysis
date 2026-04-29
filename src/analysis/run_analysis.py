"""
run_analysis.py — 分析主入口

数据在内存中构建一次，依次传给各分析模块：
  02_eda        → 探索性分析
  03_efficiency → 水效率因素分解（气候/土壤/人为占比）
  04_causal     → 因果推断（喷灌采纳 → 水效率）
  05_shap       → SHAP 归因（可干预变量重要性）

用法：
  python src/analysis/run_analysis.py
  python src/analysis/run_analysis.py --steps 02,03
  python src/analysis/run_analysis.py --steps 04,05
"""

import os, sys, json, argparse, time, subprocess, glob
import warnings
warnings.filterwarnings("ignore")

# 自动从 GitHub 同步最新代码（zerve canvas 环境）
def _auto_pull():
    paths = glob.glob('/tmp/**/zerve_hackathon', recursive=True)
    if not paths:
        return
    cwd = paths[0]
    subprocess.run(['git', 'fetch', 'origin', 'main'],
                   capture_output=True, cwd=cwd)
    r = subprocess.run(['git', 'reset', '--hard', 'origin/main'],
                       capture_output=True, text=True, cwd=cwd)
    if r.stdout.strip():
        print(f"  [git sync] {r.stdout.strip()}")
_auto_pull()

# 把 src/analysis 加入路径，使子模块可直接 import
ANALYSIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ANALYSIS_DIR)

from _shared import OUTPUT_DIR, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS


# ── 解析参数 ──────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="农业水效率分析主入口")
    p.add_argument(
        "--steps",
        default="02,03,04,05,06,07",
        help="指定运行步骤，逗号分隔，如 02,03（默认全部）"
    )
    p.add_argument(
        "--skip-load",
        action="store_true",
        help="跳过 GCS 数据加载（调试用，需提前有 features.parquet）"
    )
    return p.parse_args()


# ── 数据加载 ──────────────────────────────────────────────────────────────
def build_dataframe(skip_load: bool = False) -> "pd.DataFrame":
    import pandas as pd

    if skip_load:
        from _shared import PARQUET_PATH
        if not os.path.exists(PARQUET_PATH):
            raise FileNotFoundError(
                f"--skip-load 模式下需要 {PARQUET_PATH}，请先正常运行一次"
            )
        print(f"  [skip-load] 从 {PARQUET_PATH} 加载")
        df = pd.read_parquet(PARQUET_PATH)
        return df

    # 从 water_efficiency.py 加载原始数据并做特征工程
    from water_efficiency import load_data, feature_engineering
    from _shared import PARQUET_PATH
    print("\n" + "="*50)
    print("  01 数据加载 & 特征工程")
    print("="*50)
    t0 = time.time()
    raw_df = load_data()
    df, _, __, ___ = feature_engineering(raw_df)
    print(f"  完成，耗时 {time.time()-t0:.1f}s  形状：{df.shape}")
    # 缓存到 parquet，下次可用 --skip-load 跳过 GCS 下载
    df.to_parquet(PARQUET_PATH, index=False)
    print(f"  已缓存 → {PARQUET_PATH}")
    return df


# ── 各步骤注册表 ──────────────────────────────────────────────────────────
STEP_MODULES = {
    "02": "02_eda",
    "03": "03_efficiency",
    "04": "04_causal",
    "05": "05_shap",
    "06": "06_cluster",
    "07": "07_insights",
    "08": "08_subgroup",
}

STEP_NAMES = {
    "02": "EDA 探索性分析",
    "03": "水效率因素分解",
    "04": "因果推断",
    "05": "SHAP 归因",
    "06": "县级分类",
    "07": "Actionable Insights",
    "08": "子群异质性分析",
}


def run_step(step_id: str, df, verbose: bool = True) -> dict:
    mod_name = STEP_MODULES[step_id]
    import importlib
    mod = importlib.import_module(mod_name)
    t0 = time.time()
    result = mod.run(df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS)
    elapsed = time.time() - t0
    if verbose:
        print(f"\n  [{step_id}] {STEP_NAMES[step_id]} 完成，耗时 {elapsed:.1f}s")
    return result or {}


# ── 汇总报告 ──────────────────────────────────────────────────────────────
def write_county_wide(df):
    """把宽表导出为 county_wide.json，供 API 层加载"""
    import numpy as np
    # 计算效率百分位
    eff = df["crop_water_eff"].rank(pct=True).round(3)
    df = df.copy()
    df["eff_percentile"] = (eff * 100).round(1)
    keep = [c for c in df.columns if c not in ["log_crop_water_eff"]]
    records = df[keep].replace({np.nan: None, np.inf: None, -np.inf: None}).to_dict("records")
    out = {r["fips"]: r for r in records if r.get("fips")}
    path = os.path.join(OUTPUT_DIR, "county_wide.json")
    with open(path, "w") as f:
        json.dump(out, f, ensure_ascii=False)
    print(f"  县级宽表 → {path}（{len(out)} 县）")


def write_summary(all_results: dict):
    summary_path = os.path.join(OUTPUT_DIR, "summary.json")
    with open(summary_path, "w") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"\n  汇总结果 → {summary_path}")

    print("\n" + "="*50)
    print("  分析完成 — 关键结论")
    print("="*50)

    if "03" in all_results:
        fp = all_results["03"].get("factor_pct", {})
        if fp:
            print(f"  因素占比：气候 {fp.get('climate',0)*100:.1f}%  "
                  f"土壤 {fp.get('soil',0)*100:.1f}%  "
                  f"人为 {fp.get('human',0)*100:.1f}%")

    if "04" in all_results:
        r4 = all_results["04"]
        treatments = r4.get("treatments", [])
        if treatments:
            print(f"  因果推断（IPW）各处理变量效果：")
            for t in treatments:
                pct = t.get("ate_pct_change", "?")
                print(f"    {t['label']:<20} ATE={t['ate']:+.4f}  ({pct:+.1f}%)")

    if "05" in all_results:
        top5 = all_results["05"].get("top_actionable", [])
        if top5:
            print(f"  SHAP Top5 可干预变量：{', '.join(top5)}")

    print("="*50)
    print(f"  输出目录：{OUTPUT_DIR}")


# ── 主函数 ────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    steps = [s.strip().zfill(2) for s in args.steps.split(",")]

    # 步骤 01 单独处理：重新从 GCS 加载数据
    if steps == ["01"]:
        build_dataframe(skip_load=False)
        print("  01 完成，数据已缓存到 features.parquet")
        return

    invalid = [s for s in steps if s not in STEP_MODULES]
    if invalid:
        print(f"  ✗ 未知步骤：{invalid}，可选：01（重建宽表）或 {list(STEP_MODULES.keys())}")
        sys.exit(1)

    print("\n" + "="*50)
    print("  农业水效率分析")
    print(f"  步骤：{steps}")
    print("="*50)

    df = build_dataframe(skip_load=args.skip_load)

    all_results = {}
    for step in steps:
        try:
            all_results[step] = run_step(step, df)
        except Exception as e:
            print(f"\n  ✗ 步骤 {step} 出错：{e}")
            import traceback; traceback.print_exc()

    write_county_wide(df)
    write_summary(all_results)
    inject_map(all_results)


def inject_map(all_results: dict):
    """把分析结果注入地图 HTML 占位符（county_wide + cluster + insights）"""
    import re
    BASE = os.path.dirname(os.path.dirname(ANALYSIS_DIR))
    out_dir = os.path.join(BASE, "output", "analysis")
    map_path = os.path.join(BASE, "output", "water_quality_map.html")

    if not os.path.exists(map_path):
        print(f"  ⚠ 未找到 {map_path}，跳过地图注入")
        return

    def _load(fname):
        p = os.path.join(out_dir, fname)
        return json.load(open(p)) if os.path.exists(p) else None

    county_wide   = _load("county_wide.json")
    if not county_wide:
        print("  ⚠ county_wide.json 未找到，跳过地图注入")
        return
    cluster_data  = _load("06_cluster.json") or {}
    insights_data = _load("07_insights.json") or {}

    county_clusters = cluster_data.get("county_clusters", {})
    lhf_fips  = {r["fips"] for r in insights_data.get("low_hanging_fruit", [])}
    vwe_fips  = {r["fips"] for r in insights_data.get("virtual_water_exporters", [])}
    dual_fips = {r["fips"] for r in insights_data.get("dual_exposure", [])}

    def _safe(v):
        if v is None: return None
        try:
            f = float(v)
            return None if f != f else round(f, 4)
        except Exception:
            return None

    # Load IPW causal estimate for crop_diversity_hhi
    causal_data = _load("04_causal.json") or {}
    hhi_treatment = next((t for t in causal_data.get("treatments", [])
                          if t.get("treatment_col") == "crop_diversity_hhi"), {})
    hhi_threshold = hhi_treatment.get("threshold", 0.523)
    hhi_ate_pct   = abs(hhi_treatment.get("ate_pct_change", 32.8))

    counties = []
    for fips, c in county_wide.items():
        eff = c.get("crop_water_eff")
        if eff is None:
            continue
        ci = county_clusters.get(fips, {})
        hhi = _safe(c.get("crop_diversity_hhi"))
        # Intervention potential: only for high-HHI (low diversity) counties;
        # scale by how far above threshold so darker = more room to improve.
        if hhi is not None and hhi > hhi_threshold:
            scale = (hhi - hhi_threshold) / max(1.0 - hhi_threshold, 0.01)
            intervention_gain_pct = round(scale * hhi_ate_pct, 1)
        else:
            intervention_gain_pct = None
        counties.append({
            "fips": fips, "county": c.get("county",""), "state": c.get("state",""),
            # 目标变量
            "crop_water_eff": round(float(eff), 2),
            "percentile": c.get("eff_percentile"),
            # 基础农业
            "irrigated_area_ac": _safe(c.get("irrigated_area_ac")),
            "est_water_af": _safe(c.get("est_water_af")),
            # 聚类 / 洞察标签
            "cluster_id": ci.get("cluster_id"), "cluster_name": ci.get("cluster_name",""),
            "is_lhf": 1 if fips in lhf_fips else 0,
            "is_vwe": 1 if fips in vwe_fips else 0,
            "is_dual": 1 if fips in dual_fips else 0,
            # 气候
            "eto_avg_in": _safe(c.get("eto_avg_in")),
            "precip_deficit_in": _safe(c.get("precip_deficit_in")),
            "drought_intensity": _safe(c.get("drought_intensity")),
            # 土壤
            "awc_mean": _safe(c.get("awc_mean")),
            "clay_pct": _safe(c.get("clay_pct")),
            "organic_matter": _safe(c.get("organic_matter")),
            # 人为 / 可干预因素
            "centerpivot_ratio": _safe(c.get("centerpivot_ratio")),
            "crop_diversity_hhi": hhi,
            "high_water_crop_share": _safe(c.get("high_water_crop_share")),
            "avg_farm_size_ac": _safe(c.get("avg_farm_size_ac")),
            "median_income": _safe(c.get("median_income")),
            "poverty_rate": _safe(c.get("poverty_rate")),
            # 因果干预潜力
            "intervention_gain_pct": intervention_gain_pct,
        })

    html = open(map_path, encoding="utf-8").read()
    data_json = json.dumps(counties, ensure_ascii=False, separators=(',', ':'))
    pattern = r'/\*COUNTY_ANALYSIS_PLACEHOLDER\*/.*?/\*END_PLACEHOLDER\*/'
    new_html, n = re.subn(pattern,
        f'/*COUNTY_ANALYSIS_PLACEHOLDER*/{data_json}/*END_PLACEHOLDER*/',
        html, flags=re.DOTALL)
    if n == 0:
        print("  ⚠ 地图 HTML 中未找到占位符，跳过注入")
        return

    # Auto-update chart images (thumbnail + modal, matched by id attribute)
    import base64
    images_dir = os.path.join(BASE, "output", "images")
    img_updates = [
        ("05_shap_beeswarm.png",  "shap-modal-img",      "Beeswarm"),
        ("05_shap_beeswarm.png",  "shap-thumb-img",       "Beeswarm thumb"),
        ("05_shap_waterfall.png", "waterfall-img",        "Waterfall thumb"),
        ("05_shap_waterfall.png", "waterfall-modal-img",  "Waterfall modal"),
        ("08_subgroup.png",       "subgroup-img",         "Subgroup thumb"),
        ("08_subgroup.png",       "subgroup-modal-img",   "Subgroup modal"),
    ]
    for fname, img_id, label in img_updates:
        path = os.path.join(images_dir, fname)
        if not os.path.exists(path):
            continue
        b64 = base64.b64encode(open(path, "rb").read()).decode()
        new_src = f"data:image/png;base64,{b64}"
        new_html, n = re.subn(
            rf'(<img[^>]*id="{img_id}"[^>]*src=")data:image/png;base64,[A-Za-z0-9+/=]+(")',
            rf'\g<1>{new_src}\g<2>',
            new_html
        )
        if n:
            print(f"  ✓ {label} updated ({len(b64)//1024} KB)")

    open(map_path, "w", encoding="utf-8").write(new_html)
    print(f"  ✓ 地图已注入 {len(counties)} 县数据 → {map_path}")


if __name__ == "__main__":
    main()
