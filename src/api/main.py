"""
Agricultural Water Efficiency API
FastAPI 后端 — 供前端地图调用

启动：
  uvicorn src.api.main:app --reload --port 8000

端点：
  GET  /county/{fips}          县级水效率详情 + SHAP 归因
  GET  /map/efficiency          全县效率数据（地图渲染用）
  GET  /opportunities           低效高潜力县列表
  POST /simulate                干预模拟（调整特征预测效率变化）
  POST /explain                 Gemini 自然语言解读
"""

import os, json
from pathlib import Path
from typing import Optional
from functools import lru_cache

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

# ── 路径 ────────────────────────────────────────────────────────────
import glob as _glob
_here = Path(__file__).resolve()
_zerve_dirs = _glob.glob('/tmp/**/zerve_hackathon', recursive=True)
if _zerve_dirs:
    BASE_DIR = Path(_zerve_dirs[0])
    OUTPUT_DIR   = BASE_DIR / "output"
    ANALYSIS_DIR = OUTPUT_DIR / "analysis"
    DATA_DIR     = OUTPUT_DIR / "data"
elif _here.parent.name == "app":
    BASE_DIR     = _here.parent
    OUTPUT_DIR   = Path("/tmp/zerve_output")
    ANALYSIS_DIR = OUTPUT_DIR / "analysis"
    DATA_DIR     = OUTPUT_DIR / "data"
else:
    BASE_DIR     = _here.parent.parent.parent
    OUTPUT_DIR   = BASE_DIR / "output"
    ANALYSIS_DIR = OUTPUT_DIR / "analysis"
    DATA_DIR     = OUTPUT_DIR / "data"

app = FastAPI(title="Agricultural Water Efficiency API", version="1.0")

GITHUB_RAW = "https://raw.githubusercontent.com/echo-xiao/zerve_hackathon_water_quality_analysis/main/output"
DATA_FILES = ["02_eda", "03_efficiency", "04_causal", "05_shap",
              "06_cluster", "07_insights", "summary", "county_wide"]

@app.on_event("startup")
async def download_data():
    import urllib.request
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    for name in DATA_FILES:
        p = ANALYSIS_DIR / f"{name}.json"
        if not p.exists():
            url = f"{GITHUB_RAW}/analysis/{name}.json"
            try:
                urllib.request.urlretrieve(url, str(p))
            except Exception as e:
                print(f"Failed to download {name}.json: {e}")
    # 下载地图 HTML
    map_p = OUTPUT_DIR / "water_quality_map.html"
    if not map_p.exists():
        try:
            urllib.request.urlretrieve(f"{GITHUB_RAW}/water_quality_map.html", str(map_p))
        except Exception as e:
            print(f"Failed to download map html: {e}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 数据加载（启动时缓存）──────────────────────────────────────────
@lru_cache(maxsize=1)
def _load_results():
    """加载 run_analysis.py 产出的所有 JSON 结果"""
    results = {}
    for name in ["02_eda", "03_efficiency", "04_causal", "05_shap",
                 "06_cluster", "07_insights", "summary"]:
        p = ANALYSIS_DIR / f"{name}.json"
        if p.exists():
            results[name] = json.loads(p.read_text())
    return results

@lru_cache(maxsize=1)
def _load_county_map():
    """加载县级效率宽表（run_analysis 产出的 county_wide.csv 转 dict）"""
    p = ANALYSIS_DIR / "county_wide.json"
    if p.exists():
        return json.loads(p.read_text())
    return {}

@lru_cache(maxsize=1)
def _load_geojson():
    p = DATA_DIR / "agri_county.geojson"
    if p.exists():
        return json.loads(p.read_text())
    return {"type": "FeatureCollection", "features": []}


# ── 模型 ────────────────────────────────────────────────────────────
class SimulateRequest(BaseModel):
    fips: str
    centerpivot_ratio: Optional[float] = None
    avg_farm_size_ac: Optional[float] = None
    crop_diversity_hhi: Optional[float] = None

class ExplainRequest(BaseModel):
    fips: str
    language: str = "zh"


# ── 端点 ────────────────────────────────────────────────────────────

@app.get("/debug/paths")
def debug_paths():
    import os
    dirs = {}
    for d in ['/tmp', '/app', '/files', '/data', '/home', '/mnt']:
        try:
            dirs[d] = os.listdir(d)[:10]
        except:
            dirs[d] = "not accessible"
    return {
        "base_dir": str(BASE_DIR),
        "analysis_dir": str(ANALYSIS_DIR),
        "analysis_dir_exists": ANALYSIS_DIR.exists(),
        "files": os.listdir(str(ANALYSIS_DIR)) if ANALYSIS_DIR.exists() else [],
        "container_dirs": dirs,
    }

@app.get("/")
def root():
    report_url = os.getenv(
        "ZERVE_REPORT_URL",
        "https://docs.google.com/document/d/1mHOlY64Tvxi4CmZJs_0Jb29I0Pm_rpIS3SCQJLQAwig/edit?usp=sharing"
    )
    return {
        "status": "ok",
        "message": "Agricultural Water Efficiency API",
        "report": report_url or None,
        "docs": "/docs",
    }


@app.get("/report")
def get_report():
    """Redirect to the project report (Google Docs)"""
    from fastapi.responses import RedirectResponse
    url = os.getenv(
        "ZERVE_REPORT_URL",
        "https://docs.google.com/document/d/1mHOlY64Tvxi4CmZJs_0Jb29I0Pm_rpIS3SCQJLQAwig/edit?usp=sharing"
    )
    return RedirectResponse(url=url)


@app.get("/county/{fips}")
def get_county(fips: str):
    """返回单个县的水效率详情、SHAP 归因、与全国/气候带对比"""
    county_map = _load_county_map()
    if not county_map:
        raise HTTPException(503, "分析数据未就绪，请先运行 run_analysis.py")

    fips = fips.zfill(5)
    county = county_map.get(fips)
    if not county:
        raise HTTPException(404, f"未找到 FIPS={fips} 的数据")

    results = _load_results()
    shap_data = results.get("05_shap", {})

    # 找该县的 SHAP waterfall 数据（如果有）
    county_shap = {}
    for item in shap_data.get("county_shap", []):
        if item.get("fips") == fips:
            county_shap = item
            break

    return {
        "fips": fips,
        "county": county.get("county"),
        "state": county.get("state"),
        "crop_water_eff": county.get("crop_water_eff"),
        "log_crop_water_eff": county.get("log_crop_water_eff"),
        "percentile": county.get("eff_percentile"),
        "irrigated_area_ac": county.get("irrigated_area_ac"),
        "est_water_af": county.get("est_water_af"),
        "composite_crop_value": county.get("composite_crop_value"),
        "climate": {
            "eto_avg_in": county.get("eto_avg_in"),
            "precip_avg_in": county.get("precip_avg_in"),
            "precip_deficit_in": county.get("precip_deficit_in"),
            "elevation_ft": county.get("elevation_ft"),
        },
        "human_factors": {
            "centerpivot_ratio": county.get("centerpivot_ratio"),
            "avg_farm_size_ac": county.get("avg_farm_size_ac"),
            "tenant_ratio": county.get("tenant_ratio"),
            "crop_diversity_hhi": county.get("crop_diversity_hhi"),
        },
        "shap_attribution": county_shap,
    }


@app.get("/map/efficiency")
def get_map_efficiency():
    """返回全县水效率数据，供前端 choropleth 渲染"""
    county_map = _load_county_map()
    if not county_map:
        raise HTTPException(503, "分析数据未就绪")

    features = []
    for fips, c in county_map.items():
        eff = c.get("crop_water_eff")
        if eff is None:
            continue
        features.append({
            "fips": fips,
            "county": c.get("county", ""),
            "state": c.get("state", ""),
            "crop_water_eff": eff,
            "percentile": c.get("eff_percentile"),
            "irrigated_area_ac": c.get("irrigated_area_ac"),
        })

    return {"count": len(features), "counties": features}


@app.get("/opportunities")
def get_opportunities(
    state: Optional[str] = None,
    top: int = 20,
    insight_type: Optional[str] = None,
):
    """
    返回政策优先县。insight_type 可选：
      low_hanging_fruit | virtual_water_exporters | dual_exposure | top50_policy_priority
    默认返回 low_hanging_fruit（低挂果实）
    """
    results = _load_results()
    insights = results.get("07_insights", {})

    key = insight_type or "low_hanging_fruit"
    opps = insights.get(key, [])

    # 兼容旧格式
    if not opps:
        opps = results.get("summary", {}).get("opportunities", [])

    if state:
        opps = [o for o in opps if o.get("state", "").upper() == state.upper()]

    return {
        "insight_type": key,
        "count": len(opps[:top]),
        "opportunities": opps[:top],
    }


@app.get("/map/county_full")
def get_map_county_full():
    """返回全县完整数据：效率 + 聚类 + insight flags，供地图多子模式渲染"""
    county_map = _load_county_map()
    if not county_map:
        raise HTTPException(503, "分析数据未就绪")

    # Compute intervention_gain_pct using IPW causal estimate for crop_diversity_hhi
    results = _load_results()
    hhi_treatments = results.get("04_causal", {}).get("treatments", [])
    hhi_t = next((t for t in hhi_treatments if t.get("treatment_col") == "crop_diversity_hhi"), {})
    hhi_threshold = hhi_t.get("threshold", 0.523)
    hhi_ate_pct = abs(hhi_t.get("ate_pct_change", 32.8))

    features = []
    for fips, c in county_map.items():
        eff = c.get("crop_water_eff")
        if eff is None:
            continue
        hhi = c.get("crop_diversity_hhi")
        if hhi is not None and hhi > hhi_threshold:
            scale = (hhi - hhi_threshold) / max(1.0 - hhi_threshold, 0.01)
            intervention_gain_pct = round(scale * hhi_ate_pct, 1)
        else:
            intervention_gain_pct = None
        features.append({
            "fips": fips,
            "county": c.get("county", ""),
            "state": c.get("state", ""),
            # 分析结果
            "crop_water_eff": eff,
            "percentile": c.get("eff_percentile"),
            "irrigated_area_ac": c.get("irrigated_area_ac"),
            "cluster_id": c.get("cluster_id"),
            "cluster_name": c.get("cluster_name", ""),
            "is_lhf": int(c.get("is_lhf", 0) or 0),
            "is_vwe": int(c.get("is_vwe", 0) or 0),
            "is_dual": int(c.get("is_dual", 0) or 0),
            # 气候
            "eto_avg_in": c.get("eto_avg_in"),
            "precip_deficit_in": c.get("precip_deficit_in"),
            "drought_intensity": c.get("drought_intensity"),
            # 土壤
            "awc_mean": c.get("awc_mean"),
            # 干预潜力
            "crop_diversity_hhi": hhi,
            "intervention_gain_pct": intervention_gain_pct,
        })

    return {"count": len(features), "counties": features}


@app.get("/clusters")
def get_clusters():
    """返回县级分类结果（6类县型）"""
    results = _load_results()
    cluster_data = results.get("06_cluster", {})
    if not cluster_data:
        raise HTTPException(503, "聚类数据未就绪，请先运行 run_analysis.py")
    return {
        "n_clusters": cluster_data.get("n_clusters"),
        "n_counties": cluster_data.get("n_counties"),
        "cluster_centers": cluster_data.get("cluster_centers", []),
    }


@app.get("/clusters/{fips}")
def get_county_cluster(fips: str):
    """返回某县的聚类归属"""
    results = _load_results()
    cluster_data = results.get("06_cluster", {})
    fips = fips.zfill(5)
    county_clusters = cluster_data.get("county_clusters", {})
    info = county_clusters.get(fips)
    if not info:
        raise HTTPException(404, f"未找到 FIPS={fips} 的聚类数据")
    return {"fips": fips, **info}


@app.get("/summary")
def get_summary():
    """返回全国汇总：因素占比、因果效应、Top 可干预变量"""
    results = _load_results()
    eff = results.get("03_efficiency", {})
    causal = results.get("04_causal", {})
    shap = results.get("05_shap", {})

    return {
        "factor_decomposition": eff.get("factor_pct", {}),
        "causal_effect": {
            "method": causal.get("method"),
            "ate": causal.get("ate"),
            "ate_pct_change": causal.get("ate_pct_change"),
        },
        "top_actionable_variables": shap.get("top_actionable", []),
        "model_r2": eff.get("model_r2_cv"),
        "n_counties": eff.get("n_counties"),
    }


@app.post("/simulate")
def simulate(req: SimulateRequest):
    """
    干预模拟：调整人为因素，用 IPW-ATE 因果估计量预测效率变化
    """
    import math
    county_map = _load_county_map()
    fips = req.fips.zfill(5)
    county = county_map.get(fips)
    if not county:
        raise HTTPException(404, f"未找到 FIPS={fips}")

    results = _load_results()
    causal_treatments = results.get("04_causal", {}).get("treatments", [])
    if not causal_treatments:
        raise HTTPException(503, "因果数据未就绪")

    # 构建因果效应查找表 {feature: {threshold, ate_log}}
    causal_map = {
        t["treatment_col"]: {
            "threshold": t["threshold"],
            "ate_log": t["ate"],  # log 尺度
        }
        for t in causal_treatments
    }

    interventions = {
        "centerpivot_ratio":  req.centerpivot_ratio,
        "avg_farm_size_ac":   req.avg_farm_size_ac,
        "crop_diversity_hhi": req.crop_diversity_hhi,
    }

    current_eff = county.get("crop_water_eff", 0) or 0
    current_log = math.log1p(max(current_eff, 0))
    delta_log_eff = 0.0
    changes = []

    for feat, new_val in interventions.items():
        if new_val is None:
            continue
        old_val = county.get(feat)
        if old_val is None:
            continue
        causal = causal_map.get(feat)
        if not causal:
            continue

        threshold = causal["threshold"]
        ate_log   = causal["ate_log"]

        # 判断干预方向：从处理侧 → 控制侧 or 控制侧 → 处理侧
        old_treated = (old_val > threshold)
        new_treated = (new_val > threshold)
        if old_treated == new_treated:
            delta = 0.0  # 同侧，无跨阈值效应
        elif old_treated and not new_treated:
            delta = -ate_log  # 从处理移到控制：反向 ATE
        else:
            delta = ate_log   # 从控制移到处理：正向 ATE

        delta_log_eff += delta
        changes.append({
            "feature": feat,
            "old": old_val,
            "new": new_val,
            "threshold": threshold,
            "delta_contribution": round(delta, 4),
        })

    new_log = current_log + delta_log_eff
    new_eff = math.expm1(new_log)

    return {
        "fips": fips,
        "current_efficiency": round(current_eff, 2),
        "simulated_efficiency": round(new_eff, 2),
        "delta_pct": round((new_eff - current_eff) / max(current_eff, 1) * 100, 1),
        "changes": changes,
    }


@app.post("/explain")
async def explain(req: ExplainRequest):
    """调用 Gemini 对县级数据生成自然语言解读"""
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if not gemini_key:
        raise HTTPException(503, "GEMINI_API_KEY 未配置")

    county_data = get_county(req.fips)

    import google.generativeai as genai
    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel("gemini-2.0-flash")

    lang = "中文" if req.language == "zh" else "English"
    prompt = f"""你是农业水资源分析专家，请用{lang}简洁解读以下县级农业水效率数据（3-4句话）：

县名：{county_data['county']}, {county_data['state']}
水效率：{county_data['crop_water_eff']} $/af（全国第{county_data.get('percentile', '?')}百分位）
灌溉面积：{county_data['irrigated_area_ac']} acres
气候条件：ETo={county_data['climate']['eto_avg_in']} in, 降水={county_data['climate']['precip_avg_in']} in
中心轴喷灌比例：{county_data['human_factors']['centerpivot_ratio']}
SHAP 主因：{county_data['shap_attribution']}

请指出该县效率高/低的主要原因和可改进方向。"""

    response = model.generate_content(prompt)
    return {"fips": req.fips, "explanation": response.text}


# ── 静态文件（前端地图）──────────────────────────────────────────
_static = OUTPUT_DIR
if _static.exists():
    app.mount("/static", StaticFiles(directory=str(_static)), name="static")

@app.get("/map")
def serve_map():
    p = OUTPUT_DIR / "water_quality_map.html"
    if p.exists():
        return FileResponse(str(p))
    raise HTTPException(404, "地图文件未找到")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
