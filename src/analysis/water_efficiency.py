"""
water_efficiency.py — 数据加载层
从 GCS 读取各数据源，在内存中构建县级宽表，供分析模块使用。

用法：
  from water_efficiency import load_data, feature_engineering
  df, climate_cols, soil_cols, human_cols = feature_engineering(load_data())
"""

import os, json, warnings
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.env"))

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_DIR = os.path.join(BASE_DIR, "output/analysis")
os.makedirs(OUTPUT_DIR, exist_ok=True)

GCS_BUCKET  = os.getenv("GCS_BUCKET", "zerve_hackathon")
GCS_PROJECT = os.getenv("GCS_PROJECT", "gen-lang-client-0371685655")
GCS_PREFIX  = "raw_data"

_bucket = None
def _get_bucket():
    global _bucket
    if _bucket is None:
        from google.cloud import storage as gcs_storage
        _bucket = gcs_storage.Client(project=GCS_PROJECT).bucket(GCS_BUCKET)
    return _bucket

def _read_json(rel_path):
    blob = _get_bucket().blob(f"{GCS_PREFIX}/{rel_path}")
    if not blob.exists():
        return None
    try:
        raw = blob.download_as_bytes()
        return json.loads(raw) if raw.strip() else None
    except Exception:
        return None

def _list_blobs(prefix):
    return list(_get_bucket().list_blobs(prefix=f"{GCS_PREFIX}/{prefix}"))


# ══════════════════════════════════════════════════════════════════════
# 数据加载函数
# ══════════════════════════════════════════════════════════════════════
def _load_base():
    print("  加载全国县列表（Census）...")
    data = _read_json("census/national_counties.json")
    if not data:
        raise FileNotFoundError("census/national_counties.json 不存在，请先运行 fetch_all.py census")
    rows = []
    for rec in data:
        fips = str(rec.get("geoid", "")).zfill(5)
        if len(fips) != 5:
            continue
        name   = rec.get("name", "")
        parts  = name.split(", ") if name else []
        pop    = rec.get("total_population") or 0
        pov    = rec.get("population_below_poverty") or 0
        rows.append({
            "fips":             fips,
            "county":           parts[0] if parts else "",
            "state":            parts[1] if len(parts) > 1 else "",
            "state_fips":       str(rec.get("state_fips", "")).zfill(2),
            "population":       pop,
            "median_income":    rec.get("median_household_income"),
            "poverty_rate":     round(pov / max(pop, 1) * 100, 2) if pop else None,
            "edu_bachelors":    rec.get("edu_bachelors"),
        })
    df = pd.DataFrame(rows)
    df = df[df["state_fips"].astype(int) <= 56]
    print(f"    ✓ {len(df)} 县")
    return df

def _download_blob(blob):
    try:
        d = json.loads(blob.download_as_bytes())
        fips = d.get("fips","")
        year = d.get("year")
        etr  = d.get("etr")
        pr   = d.get("pr")
        if fips and year and etr is not None:
            return fips, {"year": year, "etr": etr, "pr": pr or 0}
    except Exception:
        pass
    return None, None

def _load_gridmet(df):
    print("  加载 gridMET（并行）...")
    blobs = _list_blobs("gridmet/")
    records = {}
    with ThreadPoolExecutor(max_workers=32) as ex:
        futs = {ex.submit(_download_blob, b): b for b in blobs}
        done = 0
        for fut in as_completed(futs):
            fips, row = fut.result()
            if fips and row:
                records.setdefault(fips, []).append(row)
            done += 1
            if done % 500 == 0:
                print(f"    {done}/{len(blobs)} blobs", end="\r", flush=True)
    print()
    rows = []
    for fips, yrs in records.items():
        ydf = pd.DataFrame(yrs).sort_values("year")
        # 只用 2022 年数据
        yr2022 = ydf[ydf["year"] == 2022]
        if yr2022.empty:
            yr2022 = ydf.iloc[[-1]]  # 无 2022 则用最新年
        eto = float(yr2022["etr"].iloc[0]) if yr2022["etr"].notna().any() else None
        pr  = float(yr2022["pr"].iloc[0])  if yr2022["pr"].notna().any()  else None
        if eto is None: continue
        rows.append({"fips": fips,
                     "eto_avg_in":       round(eto, 2),
                     "precip_avg_in":    round(pr, 2) if pr is not None else None,
                     "precip_deficit_in": round(eto - pr, 2) if pr is not None else None,
                     "eto_trend_slope":  None})
    if not rows:
        print("    ✗ 无 gridMET 数据")
        return df
    print(f"    ✓ {len(rows)} 县")
    return df.merge(pd.DataFrame(rows), on="fips", how="left")

def _load_nass_crops(df):
    print("  加载 NASS 作物产量...")
    # 单位：BU=蒲式耳, CWT=百磅, TONS=短吨
    crop_map = {
        "corn_county":     ("corn",   "bu"),
        "soybeans_county": ("soy",    "bu"),
        "wheat_county":    ("wheat",  "bu"),
        "cotton_county":   ("cotton", "bu"),
        "rice_county":     ("rice",   "cwt"),
        "hay_county":      ("hay",    "tons"),
    }
    all_rows = {}
    for fkey, (crop, unit) in crop_map.items():
        data = _read_json(f"nass_county/{fkey}.json")
        if not data: continue
        for rec in data:
            fips = rec.get("state_fips_code","").zfill(2) + rec.get("county_code","").zfill(3)
            if len(fips) != 5: continue
            try: val = float(rec["Value"].replace(",",""))
            except: continue
            all_rows.setdefault(fips, {})[f"{crop}_{rec.get('year',0)}"] = val

    rows = []
    for fips, vals in all_rows.items():
        row = {"fips": fips}
        for crop in ["corn","soy","wheat","cotton","rice","hay"]:
            for yr in [2022,2017,2012]:
                if f"{crop}_{yr}" in vals:
                    row[f"{crop}_prod"] = vals[f"{crop}_{yr}"]; break
        rows.append(row)
    if not rows:
        print("    ✗ 无作物产量数据"); return df
    ndf = pd.DataFrame(rows)

    # 换算成美元产值（2022 NASS 参考价）
    # corn: $6.54/bu, soy: $14.20/bu, wheat: $9.00/bu, cotton: $0.83/lb×480lb/bale
    # rice: $12.00/cwt, hay: $200/ton
    REVENUE_PRICES = {
        "corn":   ("corn_prod",   6.54),
        "soy":    ("soy_prod",   14.20),
        "wheat":  ("wheat_prod",  9.00),
        "cotton": ("cotton_prod", 0.83 * 480),   # bale → lbs → $
        "rice":   ("rice_prod",  12.00),          # $/cwt
        "hay":    ("hay_prod",  200.00),           # $/ton
    }
    for crop, (col, price) in REVENUE_PRICES.items():
        if col in ndf.columns:
            ndf[f"{crop}_rev"] = ndf[col].fillna(0) * price

    rev_cols = [f"{c}_rev" for c in REVENUE_PRICES if f"{c}_rev" in ndf.columns]
    total_rev = ndf[rev_cols].sum(axis=1).replace(0, np.nan)

    # 高耗水作物占比（水稻 + 干草 产值份额）
    high_water = sum(ndf.get(f"{c}_rev", 0) for c in ["rice","hay"]
                     if f"{c}_rev" in ndf.columns)
    ndf["high_water_crop_share"] = (high_water / total_rev).clip(0, 1).round(3)

    # 作物多样性 HHI（使用产值，覆盖全部 6 种作物）
    def hhi(r):
        v = [r.get(c, 0) or 0 for c in rev_cols]
        t = sum(v)
        return round(sum((x/t)**2 for x in v), 3) if t else np.nan
    ndf["crop_diversity_hhi"] = ndf.apply(hhi, axis=1)

    # 保留原始产量列（供 composite_crop_value 计算用）
    keep = ["fips","crop_diversity_hhi","high_water_crop_share",
            "corn_prod","soy_prod","wheat_prod","cotton_prod"]
    keep = [c for c in keep if c in ndf.columns]
    # 兼容旧列名
    for old, new in [("corn_prod","corn_prod_bu"),("soy_prod","soy_prod_bu"),
                     ("wheat_prod","wheat_prod_bu"),("cotton_prod","cotton_prod_bu")]:
        if old in ndf.columns:
            ndf = ndf.rename(columns={old: new})

    print(f"    ✓ {len(ndf)} 县（含水稻/干草，作物结构特征）")
    out_cols = ["fips","crop_diversity_hhi","high_water_crop_share",
                "corn_prod_bu","soy_prod_bu","wheat_prod_bu","cotton_prod_bu"]
    out_cols = [c for c in out_cols if c in ndf.columns]
    return df.merge(ndf[out_cols], on="fips", how="left")

def _load_nass_farms(df):
    print("  加载 NASS 农场规模...")
    queries = {"farm_count": "farm_count", "farm_sales": "farm_sales_usd", "farm_area": "avg_farm_size_ac"}
    rows = {}
    for fkey, col in queries.items():
        data = _read_json(f"nass_farms/{fkey}.json")
        if not data: continue
        for rec in data:
            fips = rec.get("state_fips_code","").zfill(2) + rec.get("county_code","").zfill(3)
            if len(fips) != 5: continue
            try: val = float(rec["Value"].replace(",",""))
            except: continue
            year = int(rec.get("year", 0))
            if year < 2017: continue
            rows.setdefault(fips, {})[f"{col}_{year}"] = val
    result = []
    for fips, vals in rows.items():
        row = {"fips": fips}
        for col in ["farm_count","farm_sales_usd","avg_farm_size_ac"]:
            for yr in [2022,2017]:
                if f"{col}_{yr}" in vals:
                    row[col] = vals[f"{col}_{yr}"]; break
        result.append(row)
    if not result:
        print("    ✗ 无农场规模数据"); return df
    print(f"    ✓ {len(result)} 县")
    return df.merge(pd.DataFrame(result), on="fips", how="left")

def _load_nass_irrigation(df):
    print("  加载 NASS 灌溉数据...")

    # ── 县级灌溉面积：优先用 NASS Census 宽口径数据，兜底用 ERS ──────
    irr_total = {}
    for rel_path in ["nass_irrigation/ag_land_irrigated_2022.json",      # AG LAND IRRIGATED，25k条最全
                     "nass_irrigation/fris_irrigated_area.json",         # FRIS 备用
                     "usda_ers/county_irrigated_area_2022.json"]:        # ERS，兜底
        irr_area_data = _read_json(rel_path)
        if not irr_area_data:
            continue
        # ag_land 文件按子分类拆行，只取顶级总量行避免重复计算
        is_ag_land = "ag_land" in rel_path
        tmp = {}
        for rec in irr_area_data:
            fips = rec.get("state_fips_code","").zfill(2) + rec.get("county_code","").zfill(3)
            if is_ag_land:
                short = (rec.get("short_desc") or rec.get("Short Desc") or "").strip()
                if short != "AG LAND, IRRIGATED - ACRES":
                    continue
            try:
                val = float(rec["Value"].replace(",",""))
                if val > 0:
                    tmp[fips] = tmp.get(fips, 0) + val
            except: continue
        if tmp:
            irr_total = tmp
            print(f"    ✓ irrigated_area_ac: {len(irr_total)} 县（来源：{rel_path}）")
            break

    if irr_total:
        area_df = pd.DataFrame([{"fips": k, "irrigated_area_ac": v} for k, v in irr_total.items()])
        df = df.merge(area_df, on="fips", how="left")

    return df

def _load_nass_operators(df):
    print("  加载 NASS 农场主特征...")
    data = _read_json("nass_operators/land_tenure.json")
    if not data: return df
    rows = {}
    for rec in data:
        fips = rec.get("state_fips_code","").zfill(2) + rec.get("county_code","").zfill(3)
        if len(fips) != 5: continue
        try: val = float(rec["Value"].replace(",",""))
        except: continue
        if int(rec.get("year",0)) < 2017: continue
        domcat = rec.get("domaincat_desc","").upper()
        d = rows.setdefault(fips, {})
        if "FULL OWNER" in domcat:   d["owner_ac"]      = d.get("owner_ac",0) + val
        elif "TENANT" in domcat:     d["tenant_ac"]     = d.get("tenant_ac",0) + val
        elif "PART OWNER" in domcat: d["part_owner_ac"] = d.get("part_owner_ac",0) + val
    result = []
    for fips, vals in rows.items():
        total = sum(vals.values())
        if total <= 0: continue
        result.append({"fips": fips,
                       "tenant_ratio": round((vals.get("tenant_ac",0)+vals.get("part_owner_ac",0)*0.5)/total,3)})
    if not result:
        print("    ✗ 无农场主数据"); return df
    print(f"    ✓ {len(result)} 县")
    return df.merge(pd.DataFrame(result), on="fips", how="left")

def _load_elevation(df):
    print("  加载海拔...")
    data = _read_json("elevation/county_elevation.json")
    if not data: return df
    rows = [{"fips": k, "elevation_ft": v["elevation_ft"]} for k,v in data.items() if "elevation_ft" in v]
    if not rows: return df
    print(f"    ✓ {len(rows)} 县")
    return df.merge(pd.DataFrame(rows), on="fips", how="left")


def _load_bls(df):
    print("  加载 BLS 失业率...")
    all_records = {}

    def _parse_blob(text):
        # 支持两种格式：
        # 1. 旧格式（按年分文件）：pipe 或空白分隔，state_fips|county_fips|...|year|...|rate
        # 2. 新格式（批量文件）：tab 分隔，series_id  year  period  value  ...
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("series") or line.startswith("LAU"): continue
            # 新批量格式：series_id 以 LAUCN 开头，长度 20 位
            parts = line.split("\t") if "\t" in line else (line.split("|") if "|" in line else line.split())
            try:
                if parts[0].strip().startswith("LAUCN"):
                    sid = parts[0].strip()          # e.g. LAUCN010010000000003
                    fips = sid[6:11]                # state(2) + county(3)
                    period = parts[2].strip()       # e.g. M13 (annual avg)
                    if period != "M13": continue
                    year = int(parts[1].strip())
                    rate = float(parts[3].strip())
                    all_records.setdefault(fips, {})[year] = rate
                else:
                    # 旧格式
                    if len(parts) < 8: continue
                    fips = parts[1].strip().zfill(2) + parts[2].strip().zfill(3)
                    year = int(parts[4].strip())
                    rate = float(parts[7].strip())
                    all_records.setdefault(fips, {})[year] = rate
            except: continue

    # 优先读 JSON 格式（新 API 方式）
    json_blob = _get_bucket().blob(f"{GCS_PREFIX}/bls_unemployment/la_county_all.json")
    if json_blob.exists():
        try:
            records = json.loads(json_blob.download_as_bytes())
            if records:
                rows = [{"fips": r["fips"],
                         "unemployment_avg": r["unemployment_avg"],
                         "unemployment_latest": r.get("unemployment_latest")}
                        for r in records]
                print(f"    ✓ {len(rows)} 县")
                return df.merge(pd.DataFrame(rows), on="fips", how="left")
        except: pass

    # 兜底：旧格式分年 txt 文件
    for blob in _list_blobs("bls_unemployment/"):
        try: _parse_blob(blob.download_as_text())
        except: continue

    rows = []
    for fips, yr_vals in all_records.items():
        if not yr_vals: continue
        rates = list(yr_vals.values())
        rows.append({"fips": fips,
                     "unemployment_avg":    round(float(np.mean(rates)), 2),
                     "unemployment_latest": yr_vals.get(max(yr_vals))})
    if not rows:
        print("    ✗ 无 BLS 数据"); return df
    print(f"    ✓ {len(rows)} 县")
    return df.merge(pd.DataFrame(rows), on="fips", how="left")

def _load_fema(df):
    print("  加载 FEMA NRI...")
    data = _read_json("fema_nri/county_risk.json")
    if not data:
        print("    ✗ 无 FEMA 数据"); return df
    rows = []
    for rec in data:
        fips = str(rec.get("stcofips","") or rec.get("STCOFIPS","") or "").zfill(5)
        if len(fips) != 5: continue
        rows.append({"fips": fips,
                     "flood_risk_score":   rec.get("CFLD_RISKS") or rec.get("cfld_risks"),
                     "drought_risk_score": rec.get("DRGT_RISKS") or rec.get("drgt_risks"),
                     "overall_risk_score": rec.get("RISK_SCORE") or rec.get("risk_score")})
    if not rows:
        print("    ✗ 无 FEMA 数据"); return df
    print(f"    ✓ {len(rows)} 县")
    return df.merge(pd.DataFrame(rows), on="fips", how="left")

def _load_rma(df):
    print("  加载 RMA 保险赔付...")
    blobs = _list_blobs("rma_insurance/")
    all_records = {}
    for blob in blobs:
        try:
            for line in blob.download_as_text().splitlines():
                parts = line.split("|")
                if len(parts) < 15: continue
                try:
                    fips = parts[1].strip().zfill(2) + parts[2].strip().zfill(3)
                    yr   = int(parts[0].strip())
                    prem = float(parts[11].strip().replace(",",""))
                    ind  = float(parts[12].strip().replace(",",""))
                    d = all_records.setdefault(fips, {}).setdefault(yr, {"premium":0,"indemnity":0})
                    d["premium"] += prem; d["indemnity"] += ind
                except: continue
        except: continue
    rows = []
    for fips, yrs in all_records.items():
        tp = sum(v["premium"] for v in yrs.values())
        ti = sum(v["indemnity"] for v in yrs.values())
        if tp <= 0: continue
        rows.append({"fips": fips, "insurance_loss_rate": round(ti/tp,3),
                     "insurance_premium_avg": round(tp/len(yrs),0)})
    if not rows:
        print("    ✗ 无 RMA 数据"); return df
    print(f"    ✓ {len(rows)} 县")
    return df.merge(pd.DataFrame(rows), on="fips", how="left")

def _load_bea_farm_income(df):
    print("  加载 BEA 县级农场主净收入...")
    data = _read_json("bea/farm_income.json")
    if not data:
        print("    ✗ 无 BEA 数据，请运行：python src/build/fetch_all.py bea_farm_income")
        return df
    rows = []
    for rec in data:
        geo = str(rec.get("GeoFips", "")).zfill(5)
        if len(geo) != 5 or geo == "00000": continue
        try:
            val = float(str(rec.get("DataValue", "")).replace(",", ""))
            rows.append({"fips": geo, "bea_farm_income": val * 1000})  # BEA 单位为千美元
        except: continue
    if not rows:
        print("    ✗ BEA 数据解析失败"); return df
    print(f"    ✓ {len(rows)} 县（农场主净收入，来源：IRS）")
    return df.merge(pd.DataFrame(rows), on="fips", how="left")

def _load_center_pivot(df):
    print("  加载中心轴灌溉（GEE）...")
    import io
    blob = _get_bucket().blob(f"{GCS_PREFIX}/centerpivot/county_centerpivot_2024.csv")
    if not blob.exists():
        print("    ✗ 无中心轴数据，请先运行 fetch_all.py center_pivot"); return df
    try:
        cdf = pd.read_csv(io.StringIO(blob.download_as_text()))
        cdf["fips"] = cdf["GEOID"].astype(str).str.zfill(5)
        # 用 NASS irrigated_area_ac 做分母，GEE total_crop_ac 偏小导致比例失真
        result = cdf[["fips", "centerpivot_ac"]].rename(
            columns={"centerpivot_ac": "centerpivot_area_ac"})
        # centerpivot_ratio 在 _add_derived 里用 irrigated_area_ac 计算
        print(f"    ✓ {len(result)} 县（中心轴喷灌比例，形态学检测）")
        return df.merge(result, on="fips", how="left")
    except Exception as e:
        print(f"    ✗ 加载失败: {e}"); return df

def _load_ssurgo(df):
    print("  加载 SSURGO 土壤...")
    from concurrent.futures import ThreadPoolExecutor, as_completed
    blobs = _list_blobs("ssurgo_county/")
    if not blobs:
        print("    ✗ 无 SSURGO 数据"); return df

    def _fetch(blob):
        try:
            d = json.loads(blob.download_as_bytes())
            fips = d.get("fips", "")
            if not fips: return None
            return {"fips": fips, "awc_mean": d.get("awc_mean"),
                    "sand_pct": d.get("sand_pct"), "clay_pct": d.get("clay_pct"),
                    "organic_matter": d.get("organic_matter")}
        except:
            return None

    rows = []
    with ThreadPoolExecutor(max_workers=32) as ex:
        for r in as_completed(ex.submit(_fetch, b) for b in blobs):
            res = r.result()
            if res:
                rows.append(res)

    if not rows:
        print("    ✗ 无 SSURGO 数据"); return df

    sdf = pd.DataFrame(rows)
    soil_cols = ["awc_mean", "clay_pct", "organic_matter"]
    # 诊断：有多少县实际有非空土壤数据
    for col in soil_cols:
        if col in sdf.columns:
            n = sdf[col].notna().sum()
            print(f"    {col}: {n}/{len(sdf)} 县有数据")
    # 过滤掉全部字段都是 None 的行（fetch 时 API 失败占位行）
    valid_mask = sdf[soil_cols].notna().any(axis=1)
    sdf = sdf[valid_mask]
    if sdf.empty:
        print("    ✗ SSURGO 数据全为空，请重新运行 fetch_all.py ssurgo_county")
        return df
    print(f"    ✓ {len(sdf)} 县有有效土壤数据（共 {len(rows)} 个 blob）")
    return df.merge(sdf, on="fips", how="left")

def _add_derived(df):
    # ── 中心轴百分位排名（规避 GEE 面积虚高导致比例全为1的问题）────────
    if "centerpivot_area_ac" in df.columns:
        df["centerpivot_ratio"] = df["centerpivot_area_ac"].rank(pct=True).round(4)

    # ── 估算用水量 ────────────────────────────────────────────────────
    df["est_water_af"] = (
        df.get("irrigated_area_ac", np.nan) * df.get("eto_avg_in", np.nan) / 12
    ).round(0)

    # ── 综合作物产值（2022 NASS 全国均价）────────────────────────────
    PRICES = {
        "corn_prod_bu":   6.54,
        "soy_prod_bu":   14.20,
        "wheat_prod_bu":  9.00,
        "cotton_prod_bu": 0.83,  # $/lb，棉花列单位为 bale（480 lb）
    }
    composite = pd.Series(0.0, index=df.index)
    valid_mask = pd.Series(False, index=df.index)
    for col, price in PRICES.items():
        if col in df.columns:
            s = df[col].fillna(0)
            composite += s * (480 * price if col == "cotton_prod_bu" else price)
            valid_mask |= df[col].notna()
    df["composite_crop_value"] = np.where(valid_mask, composite.round(0), np.nan)

    # ── 目标变量 ─────────────────────────────────────────────────────
    water = df["est_water_af"].replace(0, np.nan)
    df["crop_water_eff"] = (df["composite_crop_value"] / water).round(2)

    # ── 辅助指标 ─────────────────────────────────────────────────────
    df["drought_intensity"] = (
        df.get("precip_deficit_in", np.nan) / df.get("eto_avg_in", np.nan)
    ).replace([np.inf, -np.inf], np.nan).round(3)
    if "irrigated_area_ac" in df.columns and "avg_farm_size_ac" in df.columns:
        total_ac = (df["irrigated_area_ac"] + df["avg_farm_size_ac"] * df.get("farm_count", np.nan))
        df["irr_dependency"] = (df["irrigated_area_ac"] / total_ac.replace(0, np.nan)).round(3)
    return df


def load_data():
    print("\n[数据加载] 从 GCS 构建宽表...")
    df = _load_base()
    df = _load_gridmet(df)
    df = _load_nass_crops(df)
    df = _load_nass_farms(df)
    df = _load_nass_irrigation(df)
    df = _load_center_pivot(df)
    df = _load_nass_operators(df)
    df = _load_elevation(df)

    df = _load_fema(df)
    df = _load_ssurgo(df)
    df = _add_derived(df)
    print(f"  宽表构建完成: {df.shape[0]} 县 × {df.shape[1]} 列")

    CORE_COLS = [c for c in ["eto_avg_in", "irrigated_area_ac"] if c in df.columns]
    before = len(df)
    if CORE_COLS:
        df = df.dropna(subset=CORE_COLS)
    print(f"  过滤非农业县: {before-len(df)} 个 → 可用 {len(df)} 县")
    return df


# ══════════════════════════════════════════════════════════════════════
# 特征工程
# ══════════════════════════════════════════════════════════════════════
def feature_engineering(df):
    print("\n[特征工程]")

    if "crop_water_eff" in df.columns and df["crop_water_eff"].notna().any():
        print(f"  目标变量: crop_water_eff（综合作物产值/用水量，$/af）")
    else:
        print("  ⚠ crop_water_eff 全空，建模步骤将跳过")

    df["log_crop_water_eff"] = np.log1p(df["crop_water_eff"].clip(lower=0))

    CLIMATE_COLS = [c for c in ["eto_avg_in","precip_avg_in","precip_deficit_in",
                                "drought_intensity","elevation_ft"] if c in df.columns]
    SOIL_COLS    = [c for c in ["awc_mean","clay_pct","organic_matter"] if c in df.columns]
    HUMAN_COLS   = [c for c in ["centerpivot_ratio","irr_dependency",
                                "avg_farm_size_ac","farm_count","tenant_ratio",
                                "crop_diversity_hhi","high_water_crop_share",
                                "median_income","poverty_rate","drought_risk_score"] if c in df.columns]
    print(f"  气候变量: {len(CLIMATE_COLS)}  土壤变量: {len(SOIL_COLS)}  人为因素: {len(HUMAN_COLS)}")
    print(f"  目标变量有效样本: {df['crop_water_eff'].notna().sum()} 县")
    return df, CLIMATE_COLS, SOIL_COLS, HUMAN_COLS
