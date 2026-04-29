"""
Agricultural Water Efficiency API
FastAPI backend for the interactive map

Run locally:
  uvicorn src.api.main:app --reload --port 8000

Endpoints:
  GET  /county/{fips}          County-level efficiency details + SHAP attribution
  GET  /map/efficiency          All-county efficiency data (map rendering)
  GET  /opportunities           Low-efficiency, high-potential county list
  POST /simulate                Intervention simulation (adjust features, predict efficiency change)
  POST /explain                 Gemini natural language explanation
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

# ── Paths ───────────────────────────────────────────────────────────
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
    # Download map HTML
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

# ── Data loading (cached at startup) ────────────────────────────────
@lru_cache(maxsize=1)
def _load_results():
    """Load all JSON results produced by run_analysis.py"""
    results = {}
    for name in ["02_eda", "03_efficiency", "04_causal", "05_shap",
                 "06_cluster", "07_insights", "summary"]:
        p = ANALYSIS_DIR / f"{name}.json"
        if p.exists():
            results[name] = json.loads(p.read_text())
    return results

@lru_cache(maxsize=1)
def _load_county_map():
    """Load county-level efficiency table produced by run_analysis.py"""
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


# ── Request models ──────────────────────────────────────────────────
class SimulateRequest(BaseModel):
    fips: str
    centerpivot_ratio: Optional[float] = None
    avg_farm_size_ac: Optional[float] = None
    crop_diversity_hhi: Optional[float] = None

class ExplainRequest(BaseModel):
    fips: str
    language: str = "zh"


# ── Endpoints ───────────────────────────────────────────────────────

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
    """Return single county water efficiency details, SHAP attribution, and national comparison"""
    county_map = _load_county_map()
    if not county_map:
        raise HTTPException(503, "Analysis data not ready — please run run_analysis.py first")

    fips = fips.zfill(5)
    county = county_map.get(fips)
    if not county:
        raise HTTPException(404, f"No data found for FIPS={fips}")

    results = _load_results()
    shap_data = results.get("05_shap", {})

    # Find SHAP waterfall data for this county (if available)
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
    """Return all-county efficiency data for choropleth map rendering"""
    county_map = _load_county_map()
    if not county_map:
        raise HTTPException(503, "Analysis data not ready")

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
    Return priority counties for policy intervention. insight_type options:
      low_hanging_fruit | virtual_water_exporters | dual_exposure | top50_policy_priority
    Defaults to low_hanging_fruit.
    """
    results = _load_results()
    insights = results.get("07_insights", {})

    key = insight_type or "low_hanging_fruit"
    opps = insights.get(key, [])

    # Fallback to legacy format
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
    """Return full county dataset: efficiency + clusters + insight flags for map rendering"""
    county_map = _load_county_map()
    if not county_map:
        raise HTTPException(503, "Analysis data not ready")

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
            # Analysis results
            "crop_water_eff": eff,
            "percentile": c.get("eff_percentile"),
            "irrigated_area_ac": c.get("irrigated_area_ac"),
            "cluster_id": c.get("cluster_id"),
            "cluster_name": c.get("cluster_name", ""),
            "is_lhf": int(c.get("is_lhf", 0) or 0),
            "is_vwe": int(c.get("is_vwe", 0) or 0),
            "is_dual": int(c.get("is_dual", 0) or 0),
            # Climate
            "eto_avg_in": c.get("eto_avg_in"),
            "precip_deficit_in": c.get("precip_deficit_in"),
            "drought_intensity": c.get("drought_intensity"),
            # Soil
            "awc_mean": c.get("awc_mean"),
            # Intervention potential
            "crop_diversity_hhi": hhi,
            "intervention_gain_pct": intervention_gain_pct,
        })

    return {"count": len(features), "counties": features}


@app.get("/clusters")
def get_clusters():
    """Return county cluster results (4 cluster types)"""
    results = _load_results()
    cluster_data = results.get("06_cluster", {})
    if not cluster_data:
        raise HTTPException(503, "Cluster data not ready — please run run_analysis.py first")
    return {
        "n_clusters": cluster_data.get("n_clusters"),
        "n_counties": cluster_data.get("n_counties"),
        "cluster_centers": cluster_data.get("cluster_centers", []),
    }


@app.get("/clusters/{fips}")
def get_county_cluster(fips: str):
    """Return cluster assignment for a specific county"""
    results = _load_results()
    cluster_data = results.get("06_cluster", {})
    fips = fips.zfill(5)
    county_clusters = cluster_data.get("county_clusters", {})
    info = county_clusters.get(fips)
    if not info:
        raise HTTPException(404, f"No cluster data found for FIPS={fips}")
    return {"fips": fips, **info}


@app.get("/summary")
def get_summary():
    """Return national summary: factor decomposition, causal effects, top actionable variables"""
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
    Intervention simulation: adjust human factors and predict efficiency change using IPW-ATE causal estimates
    """
    import math
    county_map = _load_county_map()
    fips = req.fips.zfill(5)
    county = county_map.get(fips)
    if not county:
        raise HTTPException(404, f"No data found for FIPS={fips}")

    results = _load_results()
    causal_treatments = results.get("04_causal", {}).get("treatments", [])
    if not causal_treatments:
        raise HTTPException(503, "Causal data not ready")

    # Build causal effect lookup table {feature: {threshold, ate_log}}
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

        # Determine intervention direction: treated→control or control→treated
        old_treated = (old_val > threshold)
        new_treated = (new_val > threshold)
        if old_treated == new_treated:
            delta = 0.0  # Same side — no threshold crossing effect
        elif old_treated and not new_treated:
            delta = -ate_log  # Treated → control: reverse ATE
        else:
            delta = ate_log   # Control → treated: forward ATE

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
    """Call Gemini to generate a natural language explanation of county-level data"""
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if not gemini_key:
        raise HTTPException(503, "GEMINI_API_KEY not configured")

    county_data = get_county(req.fips)

    import google.generativeai as genai
    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel("gemini-2.0-flash")

    lang = "Chinese" if req.language == "zh" else "English"
    prompt = f"""You are an agricultural water efficiency expert. Briefly explain the following county-level data in {lang} (3-4 sentences):

County: {county_data['county']}, {county_data['state']}
Water efficiency: {county_data['crop_water_eff']} $/af (national {county_data.get('percentile', '?')}th percentile)
Irrigated area: {county_data['irrigated_area_ac']} acres
Climate: ETo={county_data['climate']['eto_avg_in']} in, Precip={county_data['climate']['precip_avg_in']} in
Center pivot ratio: {county_data['human_factors']['centerpivot_ratio']}
SHAP attribution: {county_data['shap_attribution']}

Identify the main reasons for this county's efficiency level and suggest improvement directions."""

    response = model.generate_content(prompt)
    return {"fips": req.fips, "explanation": response.text}


# ── Static files (frontend map) ─────────────────────────────────────
_static = OUTPUT_DIR
if _static.exists():
    app.mount("/static", StaticFiles(directory=str(_static)), name="static")

@app.get("/map")
def serve_map():
    p = OUTPUT_DIR / "water_quality_map.html"
    if p.exists():
        return FileResponse(str(p))
    raise HTTPException(404, "Map file not found")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
