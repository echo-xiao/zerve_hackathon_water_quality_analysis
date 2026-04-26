#!/usr/bin/env python3
"""
build_agri.py — USDA NASS 农业用水效率完整分析

输出（output/data/）：
  agri_state.geojson   — 州级 choropleth（灌溉强度/$/加仑/趋势/机会成本）
  agri_crops.json      — 各州作物明细（popup用）
  agri_summary.json    — 全国作物排行（panel用）
  agri_county.geojson  — 县级灌溉面积分布（若已下载）

用法：
  python src/build/build_agri.py
"""

import json, os, sys, requests
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.env"))

from google.cloud import storage as gcs_storage

GCS_BUCKET  = os.getenv("GCS_BUCKET", "zerve_hackathon")
GCS_PROJECT = os.getenv("GCS_PROJECT", "gen-lang-client-0371685655")
GCS_PREFIX  = "raw_data"

OUT_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output/data")
os.makedirs(OUT_DATA, exist_ok=True)

GAL_PER_ACRE_FOOT = 325_851  # 1 acre-foot = 325,851 gallons


# ── GCS 下载 ──────────────────────────────────────────────────────────────────

def gcs_client():
    return gcs_storage.Client(project=GCS_PROJECT)

def gcs_download_json(rel_path: str, required=True):
    client = gcs_client()
    bucket = client.bucket(GCS_BUCKET)
    blob   = bucket.blob(f"{GCS_PREFIX}/{rel_path}")
    if not blob.exists():
        if required:
            print(f"  ✗ GCS 文件不存在: {rel_path}")
        return []
    print(f"  下载 {rel_path} ...", end=" ", flush=True)
    data = json.loads(blob.download_as_text())
    print(f"{len(data)} 条")
    return data


# ── 解析数值 ──────────────────────────────────────────────────────────────────

def parse_val(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ── 作物分类 ──────────────────────────────────────────────────────────────────

CROP_GROUPS = {
    "FORAGE":   ["HAY", "HAYLAGE", "ALFALFA", "CLOVER", "GRASS", "PASTURE"],
    "GRAIN":    ["CORN", "WHEAT", "RICE", "BARLEY", "SORGHUM", "OATS", "RYE", "SMALL GRAIN"],
    "OIL":      ["SOYBEANS", "SUNFLOWER", "CANOLA", "PEANUTS", "COTTON"],
    "VEGETABLE":["POTATOES", "TOMATOES", "LETTUCE", "ONIONS", "CARROTS",
                 "BROCCOLI", "SPINACH", "GARLIC", "PEPPERS", "CUCUMBERS",
                 "SWEET CORN", "BEANS", "PEAS", "ASPARAGUS", "CELERY", "VEGETABLE"],
    "ORCHARD":  ["ALMONDS", "WALNUTS", "PISTACHIOS", "PECANS", "ORANGES",
                 "LEMONS", "GRAPEFRUIT", "APPLES", "GRAPES", "PEACHES",
                 "CHERRIES", "STRAWBERRIES", "BLUEBERRIES", "BERRY", "ORCHARD"],
    "OTHER":    [],
}

def crop_group(name: str) -> str:
    n = name.upper()
    for g, keywords in CROP_GROUPS.items():
        if any(k in n for k in keywords):
            return g
    return "OTHER"

GROUP_COLOR = {
    "FORAGE":    "#5a9e6f",
    "GRAIN":     "#c8a84b",
    "OIL":       "#d4884a",
    "VEGETABLE": "#7ab358",
    "ORCHARD":   "#c06080",
    "OTHER":     "#8a9ba8",
}


# ── 产值换算表 ────────────────────────────────────────────────────────────────
# (yield_per_acre, preferred_price_unit, price_lookup_crop_name)
YIELD_TABLE = {
    "HAY":              (4.0,   "$ / TON",   "HAY"),
    "HAY & HAYLAGE":    (4.0,   "$ / TON",   "HAY"),
    "ALFALFA":          (4.0,   "$ / TON",   "HAY"),
    "HAYLAGE":          (3.5,   "$ / TON",   "HAY"),
    "PASTURELAND":      (2.0,   "$ / TON",   "HAY"),
    "CORN":             (175,   "$ / BU",    "CORN"),
    "WHEAT":            (46,    "$ / BU",    "WHEAT"),
    "SOYBEANS":         (50,    "$ / BU",    "SOYBEANS"),
    "BARLEY":           (70,    "$ / BU",    "BARLEY"),
    "OATS":             (60,    "$ / BU",    "OATS"),
    "RICE":             (75,    "$ / CWT",   "RICE"),
    "SORGHUM":          (70,    "$ / BU",    "SORGHUM"),
    "SMALL GRAINS":     (46,    "$ / BU",    "WHEAT"),
    "COTTON":           (900,   "$ / LB",    "COTTON"),
    "CANOLA":           (40,    "$ / CWT",   "CANOLA"),
    "PEANUTS":          (30,    "$ / CWT",   "PEANUTS"),
    "SUNFLOWER":        (15,    "$ / CWT",   "SUNFLOWER"),
    "TOMATOES":         (700,   "$ / CWT",   "TOMATOES"),
    "POTATOES":         (440,   "$ / CWT",   "POTATOES"),
    "LETTUCE":          (320,   "$ / CWT",   "LETTUCE"),
    "SWEET CORN":       (100,   "$ / CWT",   "SWEET CORN"),
    "ONIONS":           (430,   "$ / CWT",   "ONIONS"),
    "BEANS":            (20,    "$ / CWT",   "BEANS"),
    "BROCCOLI":         (75,    "$ / CWT",   "BROCCOLI"),
    "CELERY":           (700,   "$ / CWT",   "CELERY"),
    "CARROTS":          (27,    "$ / TON",   "CARROTS"),
    "GARLIC":           (8,     "$ / CWT",   "GARLIC"),
    "PEPPERS":          (200,   "$ / CWT",   "PEPPERS"),
    "CUCUMBERS":        (120,   "$ / CWT",   "CUCUMBERS"),
    "ASPARAGUS":        (25,    "$ / CWT",   "ASPARAGUS"),
    "VEGETABLE TOTALS": (200,   "$ / CWT",   "LETTUCE"),
    "ALMONDS":          (1500,  "$ / LB",    "ALMONDS"),
    "PISTACHIOS":       (2000,  "$ / LB",    "PISTACHIOS"),
    "WALNUTS":          (2.5,   "$ / TON",   "WALNUTS"),
    "GRAPES":           (8.0,   "$ / TON",   "GRAPES"),
    "APPLES":           (12.5,  "$ / TON",   "APPLES"),
    "PEACHES":          (10.0,  "$ / TON",   "PEACHES"),
    "CHERRIES":         (5.0,   "$ / TON",   "CHERRIES"),
    "BLUEBERRIES":      (5000,  "$ / LB",    "BLUEBERRIES"),
    "STRAWBERRIES":     (160,   "$ / CWT",   "STRAWBERRIES"),
    "BLACKBERRIES":     (4000,  "$ / LB",    "BLACKBERRIES"),
    "AVOCADOS":         (5.0,   "$ / TON",   "AVOCADOS"),
    "ORANGES":          (400,   "$ / BOX, PHD EQUIV", "ORANGES"),
    "LEMONS":           (200,   "$ / BOX, PHD EQUIV", "LEMONS"),
    "GRAPEFRUIT":       (300,   "$ / BOX, PHD EQUIV", "GRAPEFRUIT"),
    "ORCHARDS":         (8.0,   "$ / TON",   "GRAPES"),
    "BERRY TOTALS":     (4000,  "$ / LB",    "BLUEBERRIES"),
}

# 虚拟水出口系数：西部主要出口州苜蓿/干草约25-30%出口海外
VIRTUAL_WATER_EXPORT_STATES = {
    "CA": 0.30, "AZ": 0.28, "NV": 0.25, "OR": 0.20, "WA": 0.20,
    "ID": 0.18, "UT": 0.18, "NM": 0.15, "CO": 0.15, "MT": 0.12,
    "WY": 0.12, "ND": 0.08, "SD": 0.08, "KS": 0.08, "NE": 0.08,
}

HAY_CROPS = {"HAY", "HAY & HAYLAGE", "ALFALFA", "HAYLAGE"}


def build_price_lookup(price_records):
    by_crop_unit = defaultdict(lambda: defaultdict(list))
    for r in price_records:
        crop = r.get("commodity_desc", "").upper().strip()
        unit = r.get("unit_desc", "")
        if not unit.startswith("$"):
            continue
        val = parse_val(r.get("Value"))
        if val is not None and val > 0:
            by_crop_unit[crop][unit].append(val)
    return {crop: {u: sum(v)/len(v) for u, v in unit_vals.items()}
            for crop, unit_vals in by_crop_unit.items()}


def get_revenue_per_acre(crop_name: str, price_lookup: dict):
    name = crop_name.upper().strip()
    entry = YIELD_TABLE.get(name)
    if entry is None:
        for key in YIELD_TABLE:
            if key in name or name in key:
                entry = YIELD_TABLE[key]
                break
    if entry is None:
        return None
    yield_val, price_unit, price_crop = entry
    price = price_lookup.get(price_crop.upper(), {}).get(price_unit)
    if price is None:
        return None
    return yield_val * price


def build_wi_map(records):
    """water_applied → {(st,crop,year): avg_af_per_ac}"""
    raw = defaultdict(list)
    for r in records:
        st   = r.get("state_alpha", "").upper()
        crop = r.get("commodity_desc", "").upper().strip()
        year = str(r.get("year", ""))
        val  = parse_val(r.get("Value"))
        if st and crop and val is not None and val > 0:
            raw[(st, crop, year)].append(val)
    return {k: sum(v)/len(v) for k, v in raw.items()}


def build_area_map(records):
    """irrigated_area records → {(st,crop,year): avg_acres}"""
    raw = defaultdict(list)
    for r in records:
        st   = r.get("state_alpha", "").upper()
        crop = r.get("commodity_desc", "").upper().strip()
        year = str(r.get("year", ""))
        val  = parse_val(r.get("Value"))
        if st and crop and val is not None and val > 0:
            # Only use records with IRRIGATED in prodn_practice or no filter
            raw[(st, crop, year)].append(val)
    return {k: sum(v)/len(v) for k, v in raw.items()}


def compute_state_intensity(wi_map, area_map, year):
    """计算指定年份各州加权平均灌溉强度"""
    state_wt = defaultdict(lambda: [0.0, 0.0])  # [sum(wi*area), sum(area)]
    for (st, crop, yr), wi in wi_map.items():
        if yr != year:
            continue
        area = area_map.get((st, crop, year))
        if area:
            state_wt[st][0] += wi * area
            state_wt[st][1] += area
    return {st: v[0]/v[1] for st, v in state_wt.items() if v[1] > 0}


def pct(vals, p):
    s = sorted(v for v in vals if v is not None)
    if not s:
        return 0
    return s[max(0, min(len(s)-1, int(len(s)*p)))]


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main():
    print("=== build_agri.py — Full Agricultural Water Analysis ===\n")

    # 1. 下载数据
    print("1. 从 GCS 下载数据...")
    water_records   = gcs_download_json("usda_nass/water_applied.json")
    price_records   = gcs_download_json("usda_nass/price_received.json")
    area_2018       = gcs_download_json("usda_nass/irrigated_area_2018.json")
    area_2013       = gcs_download_json("usda_nass/irrigated_area_2013.json")
    area_2023       = gcs_download_json("usda_nass/irrigated_area_2023.json")
    county_records  = gcs_download_json("usda_nass/county_irrigated_area_all.json", required=False)

    if not water_records:
        print("  ✗ 无 water_applied 数据，退出")
        sys.exit(1)

    # 2. 构建查找表
    print("\n2. 构建查找表...")
    wi_map   = build_wi_map(water_records)
    area_map = build_area_map(area_2018 + area_2013 + area_2023)
    price_lookup = build_price_lookup(price_records)
    print(f"  water intensity: {len(wi_map)} 条")
    print(f"  irrigated area: {len(area_map)} 条 ({len(area_2013)} 条2013 + {len(area_2018)} 条2018 + {len(area_2023)} 条2023)")
    print(f"  price lookup: {len(price_lookup)} 种作物")

    # 3. 趋势分析：各年份州级强度
    print("\n3. 趋势分析（2013 vs 2023）...")
    # 使用 area 作为权重，若无 area 则用简单均值
    intensity_by_year = {}
    for yr in ["2013", "2018", "2023"]:
        st_wi = compute_state_intensity(wi_map, area_map, yr)
        if not st_wi:
            # fallback: simple mean of all wi values for that year
            raw = defaultdict(list)
            for (st, crop, y), wi in wi_map.items():
                if y == yr:
                    raw[st].append(wi)
            st_wi = {st: sum(v)/len(v) for st, v in raw.items()}
        intensity_by_year[yr] = st_wi
        print(f"  {yr}: {len(st_wi)} 州")

    # 4. 合并：per (state, crop) 取最优年份 + 计算 $/加仑
    print("\n4. 合并数据，计算效率指标...")
    YEAR_PREF = ["2018", "2023", "2013", "2017", "2022"]

    state_crops = defaultdict(dict)
    seen = set()
    all_keys = set(list(wi_map.keys()) + list(area_map.keys()))
    for (st, crop, year) in sorted(all_keys):
        if (st, crop) in seen:
            continue
        best_year = next((y for y in YEAR_PREF if (st, crop, y) in wi_map or (st, crop, y) in area_map), year)
        wi   = wi_map.get((st, crop, best_year))
        area = area_map.get((st, crop, best_year))
        if wi is not None or area is not None:
            rev  = get_revenue_per_acre(crop, price_lookup)
            dpg  = rev / (wi * GAL_PER_ACRE_FOOT) if (rev and wi and wi > 0) else None
            # 盈亏平衡水价（$/af）：亩均收入 / 灌溉强度
            breakeven = rev / wi if (rev and wi and wi > 0) else None
            state_crops[st][crop] = {
                "water_int":    wi,
                "area":         area or 0,
                "year":         best_year,
                "group":        crop_group(crop),
                "color":        GROUP_COLOR[crop_group(crop)],
                "rev_per_acre": round(rev, 2) if rev else None,
                "dpg":          round(dpg, 6) if dpg else None,
                "dpg_cents":    round(dpg * 100, 4) if dpg else None,
                "breakeven":    round(breakeven, 1) if breakeven else None,
            }
        seen.add((st, crop))

    print(f"  已处理 {len(state_crops)} 个州的数据")

    # 5. 各州汇总统计
    print("\n5. 汇总各州指标...")
    # 预加载土壤数据（SSURGO）——用于机会成本转换率计算
    def gcs_optional(rel):
        try:
            data = gcs_download_json(rel, required=False)
            return data if data else {}
        except Exception:
            return {}
    ssurgo_raw = gcs_optional("ssurgo/state_soil_capability.json")
    state_summary = {}
    for st, crops in state_crops.items():
        items = list(crops.items())
        total_wt   = total_area = 0
        total_rev_wt = total_rev_area = 0

        for crop, info in items:
            wi  = info.get("water_int")
            ar  = info.get("area") or 0
            rev = info.get("rev_per_acre")
            if wi and ar > 0:
                total_wt   += wi * ar
                total_area += ar
                if rev:
                    total_rev_wt   += rev * ar
                    total_rev_area += ar

        avg_wi  = total_wt   / total_area   if total_area   > 0 else None
        avg_rev = total_rev_wt / total_rev_area if total_rev_area > 0 else None
        avg_dpg = avg_rev / (avg_wi * GAL_PER_ACRE_FOOT) if (avg_rev and avg_wi and avg_wi > 0) else None

        # ── 机会成本分析 ─────────────────────────────────────────────────────
        # 方法：将低价值作物（苜蓿/饲料/粮食）的用水量假设改用于
        #       高价值作物（蔬菜/果树），按全国中位数 $/加仑计算差价
        # 基准：蔬菜全国中位数 ≈ 1.5¢/gallon，饲料 ≈ 0.35¢/gallon
        # 只统计有面积数据的饲料+低价值粮食作物
        SKIP_CROPS = {"CROPS, OTHER", "HORTICULTURE TOTALS", "VEGETABLE TOTALS",
                      "BERRY TOTALS", "SMALL GRAINS", "FIELD CROPS", "ORCHARDS",
                      "AG LAND", "HEMP", "PASTURELAND"}
        LOW_VALUE_GROUPS  = {"FORAGE", "GRAIN", "OIL"}
        HIGH_VALUE_TARGET_DPG = 1.5e-2  # $0.015/gallon = 1.5¢/gallon（蔬菜中位数基准）
        # ── 现实约束参数 ──────────────────────────────────────────────────────
        # SSURGO 土壤质量调整转换率：
        #   good_ratio (class 1-3 灌溉土壤占比) 越高，可转换比例越高
        #   Range: 0.15（全是劣质土壤）→ 0.40（全是优质土壤）
        # 无 SSURGO 数据时用默认 25%
        soil_good_ratio = ssurgo_raw.get(st, {}).get("good_ratio", 0.5)
        CONVERSION_RATE   = round(0.15 + 0.25 * soil_good_ratio, 3)  # 0.15–0.40
        # 转换过渡期（1-3年）平均产量为目标产值的70%（新地、新作物磨合期）
        TRANSITION_YIELD  = 0.70
        # 每州最大套利上限$2B：超过后市场饱和、价格压力会拉低实际收益
        MARKET_CAP_M      = 2_000.0

        opp_value_M = None
        opp_crop    = None
        opp_gain    = 0.0
        for crop, info in items:
            if crop in SKIP_CROPS:
                continue
            grp = info.get("group", "OTHER")
            ar  = info.get("area", 0) or 0
            wi  = info.get("water_int") or 0
            dpg = info.get("dpg") or 0
            if grp in LOW_VALUE_GROUPS and ar >= 500 and wi > 0 and dpg < HIGH_VALUE_TARGET_DPG:
                water_vol_gal = ar * wi * GAL_PER_ACRE_FOOT
                # 理论水资源套利价值
                raw_gain = water_vol_gal * (HIGH_VALUE_TARGET_DPG - dpg)
                # 施加现实约束
                opp_gain += raw_gain * CONVERSION_RATE * TRANSITION_YIELD
        if opp_gain > 0:
            # 市场饱和上限
            opp_value_M = round(min(opp_gain / 1e6, MARKET_CAP_M), 1)
        # 找最大低效作物
        low_val_crops = [(c, info.get("area", 0) or 0, info.get("water_int", 0) or 0)
                         for c, info in items
                         if info.get("group") in LOW_VALUE_GROUPS
                         and (info.get("area") or 0) >= 500 and c not in SKIP_CROPS]
        if low_val_crops:
            opp_crop = max(low_val_crops, key=lambda x: x[1] * x[2])[0]

        # ── 虚拟水出口估算 ──────────────────────────────────────────────────
        export_pct   = VIRTUAL_WATER_EXPORT_STATES.get(st, 0.05)
        virtual_water_B_gal = 0.0
        for crop, info in items:
            if crop.upper() in HAY_CROPS:
                ar = info.get("area", 0) or 0
                wi = info.get("water_int", 0) or 0
                if ar > 0 and wi > 0:
                    virtual_water_B_gal += ar * wi * GAL_PER_ACRE_FOOT * export_pct / 1e9
        virtual_water_B_gal = round(virtual_water_B_gal, 3) if virtual_water_B_gal > 0 else None

        # ── 盈亏平衡加权均值 ────────────────────────────────────────────────
        be_vals = [info["breakeven"] for c, info in items
                   if info.get("breakeven") and info.get("area", 0) > 0]
        avg_breakeven = round(sum(be_vals)/len(be_vals), 1) if be_vals else None

        # ── 趋势 ────────────────────────────────────────────────────────────
        i_2013 = intensity_by_year.get("2013", {}).get(st)
        i_2018 = intensity_by_year.get("2018", {}).get(st)
        i_2023 = intensity_by_year.get("2023", {}).get(st)
        trend_pct = round((i_2023 - i_2013) / i_2013 * 100, 1) if (i_2013 and i_2023 and i_2013 > 0) else None
        trend_abs = round(i_2023 - i_2013, 3) if (i_2013 and i_2023) else None

        # ── 作物清单 ─────────────────────────────────────────────────────────
        crop_list = []
        for crop, info in items:
            wi  = info.get("water_int", 0) or 0
            ar  = info.get("area", 0) or 0
            crop_list.append({
                "crop":         crop,
                "group":        info["group"],
                "color":        info["color"],
                "water_int":    round(wi, 3),
                "area":         round(ar),
                "total_water":  round(wi * ar) if wi and ar else None,
                "rev_per_acre": info.get("rev_per_acre"),
                "dpg":          info.get("dpg"),
                "dpg_cents":    info.get("dpg_cents"),
                "breakeven":    info.get("breakeven"),
                "year":         info["year"],
            })
        crop_list.sort(key=lambda x: (x["area"] or 0) * (x["water_int"] or 0), reverse=True)

        state_summary[st] = {
            "avg_intensity":    round(avg_wi, 3)  if avg_wi  else None,
            "avg_dpg":          round(avg_dpg, 6) if avg_dpg else None,
            "avg_dpg_cents":    round(avg_dpg * 100, 4) if avg_dpg else None,
            "avg_breakeven":    avg_breakeven,
            "opp_value_M":      opp_value_M,
            "opp_crop":         opp_crop,
            "virtual_water_B":  virtual_water_B_gal,
            "trend_pct":        trend_pct,
            "trend_abs":        trend_abs,
            "intensity_2013":   round(i_2013, 3) if i_2013 else None,
            "intensity_2018":   round(i_2018, 3) if i_2018 else None,
            "intensity_2023":   round(i_2023, 3) if i_2023 else None,
            "total_irr_area":   round(total_area),
            "n_crops":          len(crops),
            "top_crops":        crop_list[:20],
        }

    # 输出统计
    print(f"  {len(state_summary)} 个州")
    dpg_n   = sum(1 for v in state_summary.values() if v.get("avg_dpg"))
    trend_n = sum(1 for v in state_summary.values() if v.get("trend_pct") is not None)
    opp_n   = sum(1 for v in state_summary.values() if v.get("opp_value_M"))
    print(f"  $/加仑: {dpg_n} 州  |  趋势: {trend_n} 州  |  机会成本: {opp_n} 州")

    # 6. 加载补充数据（NOAA降水 / USGS地下水 / 农业销售额）
    print("\n5b. 加载补充数据...")
    noaa_precip  = gcs_optional("noaa/state_precip_2000_2023.json")
    usgs_gw      = gcs_optional("usgs/state_groundwater_trends.json")
    ag_sales_raw = gcs_optional("usda_nass/state_ag_sales.json")
    ssurgo_raw   = gcs_optional("ssurgo/state_soil_capability.json")

    # 清洗 ag_sales: 单位=$（直接）, 选择最小合理值（避免汇总累积）
    # 按州选 2022/2017 的最大记录 ÷ 1000（转为 $1000 单位→再→$M）
    ag_sales = {}
    for st, yrs in ag_sales_raw.items():
        v2022 = yrs.get("2022", 0) or 0
        v2017 = yrs.get("2017", 0) or 0
        # 值是 $ 单位，NASS 通常为 $1000 单位, 但字符串直接是数字 → 保持原始
        # 对合理性检查：CA ag sales ~$50B = $50,000,000,000 in $
        # 值 59,005,700,000 → divide by 1e6 → $59,005M ≈ $59B ✓
        ag_sales[st] = {"2022_M": round(v2022/1e6, 1), "2017_M": round(v2017/1e6, 1)}

    print(f"  NOAA降水: {len(noaa_precip)} 州 | USGS地下水: {len(usgs_gw)} 州 | 农业销售: {len(ag_sales)} 州 | SSURGO土壤: {len(ssurgo_raw)} 州")

    # 为每个州计算补充指标，合并到 state_summary
    for st, info in state_summary.items():
        # ── Herfindahl 作物多样性指数（按水资源消耗量加权）────────────────────
        # 权重 = 面积 × 灌溉强度（≈ 实际耗水量）
        # 比纯面积权重更准确：高耗水作物对风险的贡献比低耗水作物大得多
        crop_weights = {}
        for c, d in state_crops.get(st, {}).items():
            ar = d.get("area", 0) or 0
            wi = d.get("water_int", 0) or 0
            if ar > 0 and wi > 0:
                crop_weights[c] = ar * wi  # 按实际用水量计权
        total_w = sum(crop_weights.values())
        if total_w > 0:
            shares = [w / total_w for w in crop_weights.values()]
            hhi = sum(s ** 2 for s in shares)
        else:
            hhi = None
        info["hhi"] = round(hhi, 4) if hhi else None  # 0=多样 1=单一（水量维度）

        # ── SSURGO 土壤质量 ──────────────────────────────────────────────────
        soil_d = ssurgo_raw.get(st, {})
        info["soil_good_ratio"] = soil_d.get("good_ratio")   # class 1-3 占比
        info["soil_total_acres"] = soil_d.get("total_acres")

        # ── NOAA 降水趋势（线性回归斜率，英寸/年）────────────────────────────
        precip_by_year = noaa_precip.get(st, {})
        if len(precip_by_year) >= 10:
            years_sorted = sorted(precip_by_year.keys())
            xs = list(range(len(years_sorted)))
            ys = [precip_by_year[y] for y in years_sorted]
            n = len(xs)
            sx, sy = sum(xs), sum(ys)
            sxy = sum(x*y for x,y in zip(xs,ys))
            sx2 = sum(x**2 for x in xs)
            slope = (n*sxy - sx*sy) / (n*sx2 - sx**2)  # inches/year
            avg_precip = sy / n
            info["precip_trend_yr"] = round(slope, 4)   # neg=drying
            info["avg_precip_in"]   = round(avg_precip, 1)
            info["precip_2023"]     = precip_by_year.get(years_sorted[-1])
            # 三个普查年的降水值，用于前端趋势图叠加
            info["precip_2013"]     = precip_by_year.get("2013")
            info["precip_2018"]     = precip_by_year.get("2018")
        else:
            info["precip_trend_yr"] = None
            info["avg_precip_in"]   = None

        # ── USGS 地下水趋势（近5年均值 - 早5年均值，ft，正=越来越深=在枯竭）──
        gw_by_year = usgs_gw.get(st, {})
        if len(gw_by_year) >= 8:
            yrs_sorted = sorted(gw_by_year.keys())
            early = [gw_by_year[y] for y in yrs_sorted[:5]]
            late  = [gw_by_year[y] for y in yrs_sorted[-5:]]
            gw_trend = sum(late)/len(late) - sum(early)/len(early)
            info["gw_trend_ft"] = round(gw_trend, 2)  # pos=depleting
        else:
            info["gw_trend_ft"] = None

        # ── 农业用水效率比 $/af（农业销售额 ÷ 估算总灌溉用水量）────────────
        sales_M = ag_sales.get(st, {}).get("2022_M") or ag_sales.get(st, {}).get("2017_M")
        total_irr = info.get("total_irr_area", 0) or 0
        avg_wi    = info.get("avg_intensity", 0) or 0
        if sales_M and total_irr > 0 and avg_wi > 0:
            total_water_af = total_irr * avg_wi
            ag_sales_per_af = (sales_M * 1e6) / total_water_af  # $/af
            info["ag_sales_M"]      = round(sales_M, 1)
            info["ag_per_af"]       = round(ag_sales_per_af, 1)  # $/af
        else:
            info["ag_sales_M"]  = round(sales_M, 1) if sales_M else None
            info["ag_per_af"]   = None

        # ── 连续脆弱性评分：脆弱性 = 灌溉强度 × 干旱系数 / 每加仑产值 ───────
        # 比二元加总更严谨：同时满足"高耗水+变旱+低产值"才高分；
        # 各因素之间是乘积关系（复合风险），而非简单加总
        avg_wi_v   = info.get("avg_intensity") or 0
        avg_dpg_c  = info.get("avg_dpg_cents") or 0

        # 干旱乘数：基准1.0，变旱和地下水枯竭各自放大
        precip_mult = 1.0
        pt = info.get("precip_trend_yr")
        if pt is not None and pt < 0:
            # 每减少0.1英寸/年降水，脆弱性乘以1.05（复利效应）
            precip_mult = 1.0 + abs(pt) * 0.5

        gw_mult = 1.0
        gw = info.get("gw_trend_ft")
        if gw is not None and gw > 0:
            # 每英尺地下水位下降，脆弱性乘以1.10
            gw_mult = 1.0 + gw * 0.10

        # 效率恶化乘数：灌溉强度在上升说明更加依赖灌溉水
        eff_mult = 1.0
        tp = info.get("trend_pct")
        if tp is not None and tp > 0:
            # 每+10%灌溉强度上升，脆弱性乘以1.15（需水量在增加的同时水源在减少）
            eff_mult = 1.0 + (tp / 10) * 0.15

        drought_factor = precip_mult * gw_mult * eff_mult

        # 核心公式：脆弱性 = 灌溉强度 × 干旱系数 / 每加仑产值
        # 分母（产值）越高，说明水用得越值，可承受更高水价压力
        if avg_wi_v > 0 and avg_dpg_c > 0:
            vuln_raw = avg_wi_v * drought_factor / avg_dpg_c
        elif avg_wi_v > 0:
            vuln_raw = avg_wi_v * drought_factor   # 无产值数据：只用强度×干旱
        else:
            vuln_raw = None
        info["vuln_score"] = round(vuln_raw, 4) if vuln_raw is not None else None

    print(f"  HHI计算完成 | 降水趋势: {sum(1 for v in state_summary.values() if v.get('precip_trend_yr') is not None)} 州"
          f" | 地下水: {sum(1 for v in state_summary.values() if v.get('gw_trend_ft') is not None)} 州"
          f" | 用水效率: {sum(1 for v in state_summary.values() if v.get('ag_per_af') is not None)} 州")

    # 保存 agri_crops.json
    with open(os.path.join(OUT_DATA, "agri_crops.json"), "w") as f:
        json.dump(state_summary, f)
    print("  ✓ agri_crops.json")

    # 7. 全国作物汇总
    crop_national = defaultdict(lambda: {"water": [], "dpg": [], "breakeven": []})
    for st, crops in state_crops.items():
        for crop, info in crops.items():
            if info.get("water_int"):
                crop_national[crop]["water"].append(info["water_int"])
            if info.get("dpg"):
                crop_national[crop]["dpg"].append(info["dpg"])
            if info.get("breakeven"):
                crop_national[crop]["breakeven"].append(info["breakeven"])

    national = []
    for crop, data in crop_national.items():
        if len(data["water"]) < 3:
            continue
        avg_wi  = sum(data["water"]) / len(data["water"])
        avg_dpg = sum(data["dpg"])   / len(data["dpg"]) if data["dpg"] else None
        avg_be  = sum(data["breakeven"]) / len(data["breakeven"]) if data["breakeven"] else None
        national.append({
            "crop":             crop,
            "group":            crop_group(crop),
            "color":            GROUP_COLOR[crop_group(crop)],
            "avg_water_int":    round(avg_wi, 3),
            "avg_dpg":          round(avg_dpg, 6) if avg_dpg else None,
            "avg_dpg_cents":    round(avg_dpg * 100, 4) if avg_dpg else None,
            "avg_breakeven":    round(avg_be, 1) if avg_be else None,
            "n_states":         len(data["water"]),
        })
    national.sort(key=lambda x: x["avg_water_int"], reverse=True)

    with open(os.path.join(OUT_DATA, "agri_summary.json"), "w") as f:
        json.dump(national[:40], f)
    print(f"  ✓ agri_summary.json  ({len(national)} 种作物)")

    # 8. 下载州边界
    print("\n6. 下载州边界...")
    STATE_NAME_TO_ABBR = {
        "Alabama":"AL","Alaska":"AK","Arizona":"AZ","Arkansas":"AR","California":"CA",
        "Colorado":"CO","Connecticut":"CT","Delaware":"DE","District of Columbia":"DC",
        "Florida":"FL","Georgia":"GA","Hawaii":"HI","Idaho":"ID","Illinois":"IL",
        "Indiana":"IN","Iowa":"IA","Kansas":"KS","Kentucky":"KY","Louisiana":"LA",
        "Maine":"ME","Maryland":"MD","Massachusetts":"MA","Michigan":"MI","Minnesota":"MN",
        "Mississippi":"MS","Missouri":"MO","Montana":"MT","Nebraska":"NE","Nevada":"NV",
        "New Hampshire":"NH","New Jersey":"NJ","New Mexico":"NM","New York":"NY",
        "North Carolina":"NC","North Dakota":"ND","Ohio":"OH","Oklahoma":"OK","Oregon":"OR",
        "Pennsylvania":"PA","Rhode Island":"RI","South Carolina":"SC","South Dakota":"SD",
        "Tennessee":"TN","Texas":"TX","Utah":"UT","Vermont":"VT","Virginia":"VA",
        "Washington":"WA","West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY",
        "Puerto Rico":"PR",
    }
    states_url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
    r = requests.get(states_url, timeout=30)
    r.raise_for_status()
    states_geo = r.json()
    print(f"  ✓ {len(states_geo.get('features', []))} 个州")

    # 9. 归一化所有指标
    def norm_field(vals, reverse=False):
        """返回每个州的归一化值（0-1），reverse=True时大值→0"""
        clean = [v for v in vals if v is not None]
        if not clean:
            return {}
        p5_v  = pct(clean, 0.05)
        p95_v = pct(clean, 0.95)
        rng = max(p95_v - p5_v, 1e-6)
        def _n(v):
            if v is None: return None
            n = min(1.0, max(0.0, (v - p5_v) / rng))
            return round(1 - n if reverse else n, 3)
        return _n

    intensities  = [v["avg_intensity"]      for v in state_summary.values()]
    dpg_vals     = [v["avg_dpg_cents"]      for v in state_summary.values()]
    trend_vals   = [v["trend_pct"]          for v in state_summary.values()]
    opp_vals     = [v["opp_value_M"]        for v in state_summary.values()]
    hhi_vals     = [v.get("hhi")            for v in state_summary.values()]
    precip_vals  = [v.get("precip_trend_yr")for v in state_summary.values()]
    vuln_vals    = [v.get("vuln_score")     for v in state_summary.values()]
    eff_vals     = [v.get("ag_per_af")      for v in state_summary.values()]

    vw_vals   = [v.get("virtual_water_B") for v in state_summary.values()]
    norm_i    = norm_field(intensities)
    norm_d    = norm_field(dpg_vals)
    norm_vw   = norm_field(vw_vals)    # higher = more virtual water exported
    # For trend: negative = improving = green → reverse: lower trend_pct = higher norm
    norm_t    = norm_field(trend_vals, reverse=True)
    norm_o    = norm_field(opp_vals)
    norm_hhi  = norm_field(hhi_vals)              # higher = more monoculture
    norm_prec = norm_field(precip_vals, reverse=True)  # more-negative = drying = higher norm
    norm_vuln = norm_field(vuln_vals)              # 0-4 → higher = more vulnerable
    norm_eff  = norm_field(eff_vals)               # higher $/af = more efficient
    # Per-year normalization for time-slider (all 3 years normalized together for comparability)
    i2013_vals = [v.get("intensity_2013") for v in state_summary.values()]
    i2018_vals = [v.get("intensity_2018") for v in state_summary.values()]
    i2023_vals = [v.get("intensity_2023") for v in state_summary.values()]
    all_yr_vals = [v for v in i2013_vals + i2018_vals + i2023_vals if v is not None]
    norm_yr = norm_field(all_yr_vals)  # shared scale across all years

    # 10. 合并到 GeoJSON
    print("\n7. 合并 → agri_state.geojson...")
    features = []
    matched = 0
    for feat in states_geo.get("features", []):
        props = feat.get("properties", {})
        name  = props.get("name", "")
        abbr  = STATE_NAME_TO_ABBR.get(name, "").upper()
        info  = state_summary.get(abbr, {})
        if info:
            matched += 1
            top = info.get("top_crops", [])
            feat["properties"] = {
                "abbr":             abbr,
                "name":             name,
                # 灌溉强度
                "avg_intensity":    info.get("avg_intensity"),
                "norm":             norm_i(info.get("avg_intensity")),
                # 每加仑产值
                "avg_dpg_cents":    info.get("avg_dpg_cents"),
                "norm_dpg":         norm_d(info.get("avg_dpg_cents")),
                # 趋势
                "trend_pct":        info.get("trend_pct"),
                "norm_trend":       norm_t(info.get("trend_pct")),
                "intensity_2013":   info.get("intensity_2013"),
                "intensity_2018":   info.get("intensity_2018"),
                "intensity_2023":   info.get("intensity_2023"),
                "norm_2013":        norm_yr(info.get("intensity_2013")),
                "norm_2018":        norm_yr(info.get("intensity_2018")),
                "norm_2023":        norm_yr(info.get("intensity_2023")),
                # 机会成本
                "opp_value_M":      info.get("opp_value_M"),
                "norm_opp":         norm_o(info.get("opp_value_M")),
                "opp_crop":         info.get("opp_crop", ""),
                # 虚拟水
                "virtual_water_B":  info.get("virtual_water_B"),
                "norm_vw":          norm_vw(info.get("virtual_water_B")),
                # 补充指标
                "hhi":              info.get("hhi"),
                "norm_hhi":         norm_hhi(info.get("hhi")),
                "precip_trend_yr":  info.get("precip_trend_yr"),
                "avg_precip_in":    info.get("avg_precip_in"),
                "precip_2013":      info.get("precip_2013"),
                "precip_2018":      info.get("precip_2018"),
                "precip_2023":      info.get("precip_2023"),
                "norm_precip":      norm_prec(info.get("precip_trend_yr")),
                "gw_trend_ft":      info.get("gw_trend_ft"),
                "vuln_score":       info.get("vuln_score"),
                "norm_vuln":        norm_vuln(info.get("vuln_score")),
                "ag_per_af":        info.get("ag_per_af"),
                "ag_sales_M":       info.get("ag_sales_M"),
                "norm_eff":         norm_eff(info.get("ag_per_af")),
                "soil_good_ratio":  info.get("soil_good_ratio"),
                # 摘要
                "avg_breakeven":    info.get("avg_breakeven"),
                "total_irr_area":   info.get("total_irr_area"),
                "n_crops":          info.get("n_crops"),
                "top1_crop":        top[0]["crop"] if top else "",
            }
        else:
            feat["properties"] = {"abbr": abbr, "name": name}
        features.append(feat)

    with open(os.path.join(OUT_DATA, "agri_state.geojson"), "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
    print(f"  ✓ agri_state.geojson  ({matched}/{len(features)} 州有数据)")

    # 11. 县级 GeoJSON（若数据已就绪）
    if county_records:
        print("\n8. 构建县级 GeoJSON...")
        _build_county_geojson(county_records, state_summary)
    else:
        print("\n8. 跳过县级（数据尚未下载，稍后重新运行即可）")

    print("\n✅ 完成！数据已写入 output/data/")


def _build_county_geojson(county_records, state_summary):
    """
    县级灌溉分布 GeoJSON，含4个分析维度：
      - 主导作物类型（dominant_group / color）
      - 估算总用水量（est_water_af = area × state_wi）
      - 机会成本（opp_value_M）
      - 集中度分析（pareto）
    """
    # ── 1. 聚合县级数据 ────────────────────────────────────────────────────────
    county_data = defaultdict(lambda: {"total_area": 0.0, "crops": defaultdict(float),
                                       "state": "", "county": ""})
    for r in county_records:
        fips = r.get("state_fips_code", "").zfill(2) + r.get("county_ansi", r.get("county_code", "")).zfill(3)
        crop = r.get("commodity_desc", "").upper()
        val  = parse_val(r.get("Value"))
        if val and val > 0 and fips != "00000":
            county_data[fips]["total_area"] += val
            county_data[fips]["crops"][crop] += val
            county_data[fips]["state"]  = r.get("state_alpha", "")
            county_data[fips]["county"] = r.get("county_name", "")

    print(f"  {len(county_data)} 个县有灌溉数据")

    # ── 2. 注入州级指标 + 计算县级衍生指标 ────────────────────────────────────
    # 低效作物 dpg（全国均值近似）
    LOW_VALUE_DPG = {
        "FORAGE": 0.0004,   # ~0.04¢/gal
        "GRAIN":  0.0003,
        "OIL":    0.0004,
    }
    HIGH_TARGET_DPG = 0.015  # 1.5¢/gal 蔬菜基准

    for fips, d in county_data.items():
        st   = d["state"]
        info = state_summary.get(st, {})
        st_wi  = info.get("avg_intensity")   or 1.0   # af/ac
        st_dpg = info.get("avg_dpg")         or 0.001  # $/gal

        top_crop = max(d["crops"].items(), key=lambda x: x[1])[0] if d["crops"] else ""
        grp      = crop_group(top_crop)

        # 估算总用水量
        est_water_af = d["total_area"] * st_wi

        # 县级机会成本（低值作物面积 × 水强度 × 产值差）
        low_grps = {"FORAGE", "GRAIN", "OIL"}
        low_area = sum(v for crop, v in d["crops"].items()
                       if crop_group(crop) in low_grps)
        low_dpg  = LOW_VALUE_DPG.get(grp, 0.0004)
        opp_gain_gal = low_area * st_wi * GAL_PER_ACRE_FOOT * max(0, HIGH_TARGET_DPG - low_dpg)
        opp_value_M = opp_gain_gal / 1e6

        d.update({
            "top_crop":      top_crop,
            "group":         grp,
            "color":         GROUP_COLOR[grp],
            "est_water_af":  round(est_water_af, 0),
            "opp_value_M":   round(opp_value_M, 1),
            "st_wi":         st_wi,
            "st_dpg_cents":  round((st_dpg or 0) * 100, 3),
        })

    # ── 3. 归一化（用于 choropleth 着色）────────────────────────────────────
    water_vals = [d["est_water_af"] for d in county_data.values() if d["est_water_af"] > 0]
    opp_vals   = [d["opp_value_M"]  for d in county_data.values() if d["opp_value_M"]  > 0]
    area_vals  = [d["total_area"]   for d in county_data.values() if d["total_area"]   > 0]

    def norm95(vals, v):
        if not vals or v is None: return None
        p5  = sorted(vals)[max(0, int(len(vals)*0.05))]
        p95 = sorted(vals)[min(len(vals)-1, int(len(vals)*0.95))]
        rng = max(p95 - p5, 1e-6)
        return round(min(1.0, max(0.0, (v - p5) / rng)), 3)

    for d in county_data.values():
        d["norm_water"] = norm95(water_vals, d["est_water_af"])
        d["norm_opp"]   = norm95(opp_vals,   d["opp_value_M"])
        d["norm_area"]  = norm95(area_vals,   d["total_area"])

    # ── 4. 帕累托集中度分析 ──────────────────────────────────────────────────
    sorted_by_water = sorted(county_data.values(), key=lambda x: x["est_water_af"], reverse=True)
    total_water = sum(d["est_water_af"] for d in sorted_by_water)
    cum = 0
    pareto_50 = pareto_80 = 0
    for i, d in enumerate(sorted_by_water):
        cum += d["est_water_af"]
        if not pareto_50 and cum >= total_water * 0.5:
            pareto_50 = i + 1
        if not pareto_80 and cum >= total_water * 0.8:
            pareto_80 = i + 1
            break
    print(f"  集中度分析：前 {pareto_50} 县 = 50% 灌溉用水，前 {pareto_80} 县 = 80%")
    pareto_summary = {"top50pct": pareto_50, "top80pct": pareto_80,
                      "total_counties": len(county_data),
                      "total_water_MAF": round(total_water / 1e6, 2)}
    with open(os.path.join(OUT_DATA, "agri_pareto.json"), "w") as f:
        json.dump(pareto_summary, f)

    # ── 5. 下载县级边界，精度压缩到1位小数 ──────────────────────────────────
    counties_url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    try:
        resp = requests.get(counties_url, timeout=90)
        resp.raise_for_status()
        counties_geo = resp.json()
    except Exception as e:
        print(f"  ✗ 县边界下载失败: {e}")
        return

    def simplify_coords(c, prec=1):
        if isinstance(c[0], list):
            return [simplify_coords(x, prec) for x in c]
        return [round(c[0], prec), round(c[1], prec)]

    feats = []
    matched = 0
    for feat in counties_geo.get("features", []):
        fips = feat.get("id", "")
        d = county_data.get(fips)
        if not d or d["total_area"] <= 0:
            continue
        matched += 1
        geom = feat["geometry"]
        feats.append({
            "type": "Feature",
            "id":   fips,
            "geometry": {
                "type":        geom["type"],
                "coordinates": simplify_coords(geom["coordinates"]),
            },
            "properties": {
                "fips":         fips,
                "state":        d["state"],
                "county":       d["county"],
                "total_area":   round(d["total_area"]),
                "top_crop":     d["top_crop"],
                "group":        d["group"],
                "color":        d["color"],
                "est_water_af": d["est_water_af"],
                "opp_value_M":  d["opp_value_M"],
                "norm_water":   d["norm_water"],
                "norm_opp":     d["norm_opp"],
                "norm_area":    d["norm_area"],
                "st_wi":        d["st_wi"],
                "st_dpg_cents": d["st_dpg_cents"],
            }
        })

    out_path = os.path.join(OUT_DATA, "agri_county.geojson")
    with open(out_path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": feats}, f)
    sz = os.path.getsize(out_path) // 1024
    print(f"  ✓ agri_county.geojson  ({matched} 县多边形, {sz} KB)")


def build_commodity_prices():
    """从 GCS 下载商品价格 → output/data/commodity_prices.json"""
    import io, csv
    print("\n=== 商品期货价格处理 ===")
    client = gcs_client()
    bucket = client.bucket(GCS_BUCKET)

    CROPS = ["corn", "soybeans", "cotton", "rice", "wheat"]
    result = {}
    for name in CROPS:
        blob = bucket.blob(f"{GCS_PREFIX}/commodities/{name}_monthly.json")
        if not blob.exists():
            print(f"  ✗ commodities/{name}_monthly.json 未找到，跳过")
            continue
        data = json.loads(blob.download_as_text())
        prices = data.get("prices", [])
        if not prices:
            continue
        latest = prices[0]["close"]
        yr_ago = next((p["close"] for p in prices if p["date"][:4] == str(int(prices[0]["date"][:4])-1)), None)
        hi5 = max(p["close"] for p in prices)
        lo5 = min(p["close"] for p in prices)
        result[name] = {
            "symbol":    data.get("symbol"),
            "latest":    round(latest, 2),
            "date":      prices[0]["date"],
            "yr_chg_pct": round((latest/yr_ago - 1)*100, 1) if yr_ago else None,
            "hi5":       round(hi5, 2),
            "lo5":       round(lo5, 2),
            "pct_of_hi": round(latest/hi5*100, 1) if hi5 else None,
        }
        print(f"  ✓ {name}: ${latest:.2f}  ({'+' if result[name].get('yr_chg_pct',0)>=0 else ''}{result[name].get('yr_chg_pct','?')}% YoY)")

    out_path = os.path.join(OUT_DATA, "commodity_prices.json")
    with open(out_path, "w") as f:
        json.dump(result, f)
    print(f"  ✓ commodity_prices.json  ({len(result)} 种)")


def build_state_drought():
    """从 GCS 下载 Drought Monitor 州级 CSV → output/data/state_drought.json"""
    import io, csv as csvmod
    print("\n=== 州级干旱指数处理 ===")
    csvmod.field_size_limit(10_000_000)
    client = gcs_client()
    bucket = client.bucket(GCS_BUCKET)

    blob = bucket.blob(f"{GCS_PREFIX}/drought/state_drought_monitor.csv")
    if not blob.exists():
        print("  ✗ drought/state_drought_monitor.csv 未找到，跳过")
        return

    text = blob.download_as_text()
    if text.strip().startswith("<!"):
        print("  ✗ USDM 返回 HTML（API 已变更），跳过")
        return
    reader = csvmod.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        print("  ✗ CSV 为空")
        return

    # 找最新日期的每州数据
    from collections import defaultdict
    state_rows = defaultdict(list)
    for row in rows:
        abbr = (row.get("StateAbbreviation") or row.get("STATEABBR") or "").strip().upper()
        date = (row.get("MapDate") or row.get("MAPDATE") or "").strip()
        if abbr and date:
            state_rows[abbr].append(row)

    result = {}
    for abbr, rows_s in state_rows.items():
        latest = sorted(rows_s, key=lambda r: r.get("MapDate") or r.get("MAPDATE", ""), reverse=True)[0]
        def pct_col(row, *names):
            for n in names:
                v = row.get(n)
                if v not in (None, ""):
                    try: return round(float(v), 1)
                    except: pass
            return 0.0
        d0 = pct_col(latest, "D0", "None")
        d1 = pct_col(latest, "D1")
        d2 = pct_col(latest, "D2")
        d3 = pct_col(latest, "D3")
        d4 = pct_col(latest, "D4")
        # drought_pct = D1+D2+D3+D4 (moderate drought or worse)
        drought_pct = round(d1 + d2 + d3 + d4, 1)
        severe_pct  = round(d2 + d3 + d4, 1)
        date_str = (latest.get("MapDate") or latest.get("MAPDATE") or "")
        result[abbr] = {
            "date": date_str,
            "drought_pct": drought_pct,   # D1-D4
            "severe_pct":  severe_pct,    # D2-D4
            "d0": d0, "d1": d1, "d2": d2, "d3": d3, "d4": d4,
        }

    out_path = os.path.join(OUT_DATA, "state_drought.json")
    with open(out_path, "w") as f:
        json.dump(result, f)
    top = sorted(result.items(), key=lambda x: x[1]["drought_pct"], reverse=True)[:5]
    print(f"  ✓ state_drought.json  ({len(result)} 州)")
    top_str = ", ".join(f"{a}({v['drought_pct']}%)" for a, v in top)
    print(f"  最干旱: {top_str}")


if __name__ == "__main__":
    main()
    build_commodity_prices()
    build_state_drought()
