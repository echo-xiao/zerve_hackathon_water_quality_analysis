"""
LA Water Quality - 全量数据收集
所有数据源整合在一个脚本中

用法：
  python src/fetch_all.py              # 运行所有数据源（不含慢速 ewg_all）
  python src/fetch_all.py wqp census  # 只运行指定数据源
  python src/fetch_all.py ewg_all     # 单独运行全量 EWG（约 10 分钟）
  python src/fetch_all.py --list      # 列出所有可用数据源

数据源：
  wqp        Water Quality Portal（监测站 + 133K 检测记录）
  usgs       USGS 水文数据
  usgs_meas  USGS 实测水质时间序列（温度/DO/pH/流量等）
  ca         California Open Data（地下水 + 违规记录）
  la         LA Open Data
  epa_echo   EPA ECHO（设施 + 执法记录）
  epa_sdwis  EPA SDWIS（供水系统信息）
  ewg        EWG 主要 6 个系统（快速）
  ewg_all    EWG 全部 300+ 系统（慢，约 10 分钟）
  ladwp      LADWP 年度 PDF 报告（2004-2024）
  census     US Census 人口/收入数据（无需 Key）
  noaa       NOAA 气候数据（需要 .env 中的 NOAA_API_KEY）
  fire       2025 LA 野火边界 GeoJSON（Palisades + Eaton Fire）
  beach      Heal the Bay 海滩水质评级（LA County 海滩）
  hab        CA Water Board 有害藻华事件（HAB）
  cdpr       CDPR 农药使用报告（LA County）
  npdes      EPA ECHO NPDES 废水排放监测值
"""

import requests
import json
import os
import sys
import time
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

BASE_DIR = os.path.join(os.path.dirname(__file__), "../data/raw_data")


# ══════════════════════════════════════════════════════════════
# 1. Water Quality Portal (WQP)
# ══════════════════════════════════════════════════════════════
def fetch_wqp():
    out_dir = os.path.join(BASE_DIR, "wqp")
    os.makedirs(out_dir, exist_ok=True)

    print("  获取 LA County 监测站...")
    r = requests.get(
        "https://www.waterqualitydata.us/data/Station/search",
        params={
            "statecode": "US:06", "countycode": "US:06:037",
            # 不限 siteType，抓所有类型（含河流、湖泊、水井、海滩、设施等）
            "mimeType": "csv", "zip": "no"
        },
        headers={"Accept": "text/csv"}
    )
    with open(os.path.join(out_dir, "stations.csv"), "wb") as f:
        f.write(r.content)
    print("  ✓ stations.csv")
    time.sleep(1)

    print("  获取检测结果（2020至今）...")
    r = requests.get(
        "https://www.waterqualitydata.us/data/Result/search",
        params={
            "statecode": "US:06", "countycode": "US:06:037",
            "startDateLo": "01-01-2020", "mimeType": "csv", "zip": "no"
        },
        headers={"Accept": "text/csv"}, timeout=120
    )
    with open(os.path.join(out_dir, "results.csv"), "wb") as f:
        f.write(r.content)
    print("  ✓ results.csv")


# ══════════════════════════════════════════════════════════════
# 2. USGS Water Data
# ══════════════════════════════════════════════════════════════
def fetch_usgs():
    out_dir = os.path.join(BASE_DIR, "usgs")
    os.makedirs(out_dir, exist_ok=True)

    def safe_fetch(url, params, filename):
        r = requests.get(url, params=params)
        if r.status_code != 200 or not r.text.strip():
            print(f"  ⚠ {filename} 返回 {r.status_code}，跳过")
            return
        try:
            data = r.json()
            with open(os.path.join(out_dir, filename), "w") as f:
                json.dump(data, f, indent=2)
            print(f"  ✓ {filename}")
        except Exception:
            with open(os.path.join(out_dir, filename.replace(".json", ".txt")), "w") as f:
                f.write(r.text)
            print(f"  ✓ {filename} (原始格式)")

    print("  获取 LA 监测站列表...")
    safe_fetch("https://waterservices.usgs.gov/nwis/site/", {
        "format": "rdb", "stateCd": "ca", "countyCd": "037",
        "siteType": "ST,LK,GW", "siteStatus": "all", "hasDataTypeCd": "qw"
    }, "sites.txt")
    time.sleep(1)

    print("  获取历史水质数据...")
    safe_fetch("https://waterservices.usgs.gov/nwis/dv/", {
        "format": "json", "stateCd": "ca", "countyCd": "037",
        "parameterCd": "00095", "startDT": "2020-01-01", "endDT": "2024-12-31",
        "siteStatus": "all"
    }, "historical.json")


# ══════════════════════════════════════════════════════════════
# 3. California Open Data
# ══════════════════════════════════════════════════════════════
def fetch_ca_open_data():
    out_dir = os.path.join(BASE_DIR, "ca_open_data")
    os.makedirs(out_dir, exist_ok=True)

    print("  获取加州地下水质数据...")
    r = requests.get("https://data.ca.gov/api/3/action/datastore_search", params={
        "resource_id": "805e6762-1b82-48d9-b68f-5d79cca06ace",
        "filters": json.dumps({"gm_gis_county": "Los Angeles"}),
        "limit": 10000
    })
    with open(os.path.join(out_dir, "groundwater_stations.json"), "w") as f:
        json.dump(r.json(), f, indent=2)
    print("  ✓ groundwater_stations.json")
    time.sleep(1)

    print("  获取饮用水违规数据...")
    r = requests.get("https://data.ca.gov/api/3/action/datastore_search", params={
        "resource_id": "9ce012e2-5fd3-4372-a4dd-63294b0ce0f6", "limit": 10000
    })
    with open(os.path.join(out_dir, "drinking_water_violations.json"), "w") as f:
        json.dump(r.json(), f, indent=2)
    print("  ✓ drinking_water_violations.json")


# ══════════════════════════════════════════════════════════════
# 4. LA Open Data
# ══════════════════════════════════════════════════════════════
def fetch_la_open_data():
    out_dir = os.path.join(BASE_DIR, "la_open_data")
    os.makedirs(out_dir, exist_ok=True)

    print("  获取 LA 本地水质数据...")
    r = requests.get("https://data.lacity.org/resource/rcpd-miwk.json", params={"$limit": 10000})
    with open(os.path.join(out_dir, "water_quality.json"), "w") as f:
        json.dump(r.json(), f, indent=2)
    print("  ✓ water_quality.json")


# ══════════════════════════════════════════════════════════════
# 5. EPA ECHO
# ══════════════════════════════════════════════════════════════
def fetch_epa_echo():
    out_dir = os.path.join(BASE_DIR, "epa_echo")
    os.makedirs(out_dir, exist_ok=True)

    print("  获取 LA 供水设施列表...")
    r = requests.get("https://echodata.epa.gov/echo/sdw_rest_services.get_facilities", params={
        "output": "JSON", "p_st": "CA", "p_county": "LOS ANGELES", "p_act": "Y"
    })
    with open(os.path.join(out_dir, "facilities.json"), "w") as f:
        json.dump(r.json(), f, indent=2)
    print("  ✓ facilities.json")
    time.sleep(1)

    print("  获取违规详情...")
    r = requests.get("https://echodata.epa.gov/echo/sdw_rest_services.get_qid", params={
        "output": "JSON", "p_st": "CA", "p_county": "LOS ANGELES"
    })
    with open(os.path.join(out_dir, "violations.json"), "w") as f:
        json.dump(r.json(), f, indent=2)
    print("  ✓ violations.json")


# ══════════════════════════════════════════════════════════════
# 6. EPA SDWIS
# ══════════════════════════════════════════════════════════════
def fetch_epa_sdwis():
    out_dir = os.path.join(BASE_DIR, "epa_sdwis")
    os.makedirs(out_dir, exist_ok=True)

    print("  获取 LA 供水系统列表...")
    r = requests.get(
        "https://data.epa.gov/efservice/WATER_SYSTEM/PRIMACY_AGENCY_CODE/CA/CITY_NAME/LOS%20ANGELES/JSON"
    )
    if r.status_code == 200:
        with open(os.path.join(out_dir, "water_systems.json"), "w") as f:
            json.dump(r.json(), f, indent=2)
        print("  ✓ water_systems.json")
    else:
        print(f"  ⚠ SDWIS 返回 {r.status_code}，跳过")


# ══════════════════════════════════════════════════════════════
# 7. EWG 主要 6 个系统
# ══════════════════════════════════════════════════════════════
EWG_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
EWG_MAIN_SYSTEMS = {
    "ladwp":       "CA1910067",
    "burbank":     "CA1910046",
    "glendale":    "CA1910068",
    "pasadena":    "CA1910116",
    "long_beach":  "CA1910087",
    "santa_monica":"CA1910134",
}


def _parse_ewg_page(html):
    soup = BeautifulSoup(html, "html.parser")
    contaminants = []
    for item in soup.select(".contaminant-grid-item"):
        c = {}
        h3 = item.find("h3")
        if h3:
            c["name"] = h3.get_text(strip=True)
        for cls, key in [
            ("this-utility-text", "utility_level"),
            ("legal-limit-text", "legal_limit"),
            ("health-guideline-text", "health_guideline"),
            ("detect-times-greater-than", "times_above_guideline"),
            ("potentital-effect", "potential_effect"),
        ]:
            el = item.find(class_=cls)
            if el:
                text = el.get_text(strip=True)
                for prefix in ["This Utility:", "Legal Limit:", "EWG's Health Guideline:", "Potential Effect:"]:
                    text = text.replace(prefix, "").strip()
                c[key] = text
        if c.get("name"):
            contaminants.append(c)
    info = {}
    h1 = soup.find("h1")
    if h1:
        info["title"] = h1.get_text(strip=True)
    return contaminants, info


def fetch_ewg():
    out_dir = os.path.join(BASE_DIR, "ewg")
    os.makedirs(out_dir, exist_ok=True)

    for name, pws_id in EWG_MAIN_SYSTEMS.items():
        print(f"  获取 {name} ({pws_id})...")
        r = requests.get(f"https://www.ewg.org/tapwater/system.php?pws={pws_id}",
                         headers=EWG_HEADERS, timeout=30)
        if r.status_code != 200:
            print(f"  ✗ {name} 请求失败 ({r.status_code})")
            continue
        contaminants, info = _parse_ewg_page(r.text)
        result = {"pws_id": pws_id, "name": name, **info, "contaminants": contaminants}
        with open(os.path.join(out_dir, f"{name}.json"), "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        with open(os.path.join(out_dir, f"{name}_raw.html"), "w", encoding="utf-8") as f:
            f.write(r.text)
        print(f"  ✓ {name} ({len(contaminants)} 条污染物记录)")
        time.sleep(2)


# ══════════════════════════════════════════════════════════════
# 8. EWG 全部 300+ 系统
# ══════════════════════════════════════════════════════════════
def fetch_ewg_all():
    out_dir = os.path.join(BASE_DIR, "ewg")
    os.makedirs(out_dir, exist_ok=True)

    # 从 EPA 获取 LA County 所有供水系统
    print("  获取 LA County 供水系统列表...")
    all_systems = []
    for offset in range(0, 5000, 1000):
        r = requests.get(
            f"https://data.epa.gov/efservice/WATER_SYSTEM/PRIMACY_AGENCY_CODE/CA"
            f"/PWS_TYPE_CODE/CWS/PWS_ACTIVITY_CODE/A/ROWS/{offset}:{offset+1000}/JSON",
            timeout=60
        )
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        all_systems.extend(batch)
        if len(batch) < 1000:
            break

    la_systems = [s for s in all_systems if s.get("pwsid", "").startswith("CA19")]
    print(f"  找到 {len(la_systems)} 个 LA County 供水系统")
    with open(os.path.join(out_dir, "_la_water_systems.json"), "w") as f:
        json.dump(la_systems, f, indent=2)

    ok, skipped, errors = 0, 0, 0
    for i, s in enumerate(la_systems):
        pwsid = s["pwsid"]
        name = s["pws_name"]
        out_json = os.path.join(out_dir, f"{pwsid}.json")
        if os.path.exists(out_json):
            skipped += 1
            continue
        try:
            r = requests.get(f"https://www.ewg.org/tapwater/system.php?pws={pwsid}",
                             headers=EWG_HEADERS, timeout=20)
            if r.status_code != 200:
                errors += 1
                continue
            contaminants, info = _parse_ewg_page(r.text)
            with open(out_json, "w") as f:
                json.dump({"pwsid": pwsid, "name": name, **info, "contaminants": contaminants},
                          f, indent=2, ensure_ascii=False)
            print(f"  [{i+1}/{len(la_systems)}] ✓ {name[:40]} ({len(contaminants)} 污染物)")
            ok += 1
        except Exception as e:
            print(f"  [{i+1}/{len(la_systems)}] ✗ {name[:40]} ({e})")
            errors += 1
        time.sleep(1)

    print(f"  EWG ALL 完成：成功 {ok}，跳过 {skipped}，失败 {errors}")


# ══════════════════════════════════════════════════════════════
# 9. LADWP PDF 报告
# ══════════════════════════════════════════════════════════════
LADWP_PDF_URLS = {
    "2024": "/sites/default/files/2025-07/2025_BOOKLETS_2024_DWQR_E_digital_0.pdf",
    "2023": "/sites/default/files/2024-07/2024_DIGITAL_PUBLICATION_2023_Drinking_Water_Quality_Report_03_Print.pdf",
    "2022": "/sites/default/files/2023-08/2022_Drinking_Water_Quality_Report.pdf",
    "2021": "/sites/default/files/documents/2021_Drinking_Water_Quality_Report.pdf",
    "2020": "/sites/default/files/documents/2020_Drinking_Water_Quality_Report.pdf",
    "2019": "/sites/default/files/documents/2019_drinking_water_quality_report_final.pdf",
    "2018": "/sites/default/files/documents/DWQR_2018.pdf",
    "2017": "/sites/default/files/documents/DWQR_2017v9.pdf",
    "2016": "/sites/default/files/documents/2016_Drinking_Water_Quality_Report_FINAL.pdf",
    "2015": "/sites/default/files/documents/2015_Drinking_Water_Quality_Report_rev080916.pdf",
    "2014": "/sites/default/files/documents/2014_Drinking_Water_Quality_Report_FINAL.pdf",
    "2013": "/sites/default/files/documents/2013_Drinking_Water_Quality_Report.pdf",
    "2012": "/sites/default/files/documents/DWQR_2012_LoRez.pdf",
    "2011": "/sites/default/files/documents/DWQR_2011_FINAL_LoRes_3_.pdf",
    "2010": "/sites/default/files/documents/2010_WQR.pdf",
    "2009": "/sites/default/files/documents/AWQR_2009.pdf",
    "2008": "/sites/default/files/documents/2008AWQREnglishWeb.pdf",
    "2007": "/sites/default/files/documents/2007AWQREnglishWeb.pdf",
    "2006": "/sites/default/files/documents/2006_WQAR_hi.pdf",
    "2005": "/sites/default/files/documents/2005_WQAR.pdf",
    "2004": "/sites/default/files/documents/2004WQAR_web.pdf",
}


def fetch_ladwp():
    out_dir = os.path.join(BASE_DIR, "ladwp_pdf")
    os.makedirs(out_dir, exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    for year in sorted(LADWP_PDF_URLS.keys(), reverse=True):
        out_path = os.path.join(out_dir, f"LADWP_DWQR_{year}.pdf")
        if os.path.exists(out_path):
            print(f"  ⏭ {year} 已存在，跳过")
            continue
        r = requests.get("https://www.ladwp.com" + LADWP_PDF_URLS[year],
                         headers=headers, timeout=30)
        if r.status_code == 200 and len(r.content) > 1000:
            with open(out_path, "wb") as f:
                f.write(r.content)
            print(f"  ✓ {year} ({len(r.content) // 1024} KB)")
        else:
            print(f"  ✗ {year} 下载失败 ({r.status_code})")
        time.sleep(1)


# ══════════════════════════════════════════════════════════════
# 10. US Census ACS 5-Year
# ══════════════════════════════════════════════════════════════
ACS_YEAR = "2023"
ACS_BASE = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
CENSUS_VARS = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B19301_001E": "per_capita_income",
    "B17001_002E": "population_below_poverty",
    "B02001_002E": "race_white_alone",
    "B02001_003E": "race_black_alone",
    "B02001_004E": "race_native_alone",
    "B02001_005E": "race_asian_alone",
    "B02001_006E": "race_pacific_islander_alone",
    "B02001_007E": "race_other_alone",
    "B02001_008E": "race_two_or_more",
    "B03003_003E": "hispanic_or_latino",
    "B15003_001E": "edu_total_25plus",
    "B15003_017E": "edu_high_school_diploma",
    "B15003_022E": "edu_bachelors",
    "B25003_001E": "housing_total_units",
    "B25003_002E": "housing_owner_occupied",
    "B25077_001E": "median_home_value",
    # 住房建造年代（铅管污染代理变量：1970年前建造的房屋铅管风险高）
    "B25034_001E": "housing_age_total",
    "B25034_002E": "housing_built_2020_or_later",
    "B25034_003E": "housing_built_2010_2019",
    "B25034_004E": "housing_built_2000_2009",
    "B25034_005E": "housing_built_1990_1999",
    "B25034_006E": "housing_built_1980_1989",
    "B25034_007E": "housing_built_1970_1979",
    "B25034_008E": "housing_built_1960_1969",
    "B25034_009E": "housing_built_1950_1959",
    "B25034_010E": "housing_built_1940_1949",
    "B25034_011E": "housing_built_1939_or_earlier",
}


def _parse_census_val(v):
    if v is None:
        return None
    try:
        vi = int(v)
        return None if vi < 0 else vi
    except (ValueError, TypeError):
        return None


def fetch_census():
    out_dir = os.path.join(BASE_DIR, "census")
    os.makedirs(out_dir, exist_ok=True)
    var_list = ",".join(["NAME"] + list(CENSUS_VARS.keys()))

    # Census Tract 级别
    print("  获取 Census Tract 数据（LA County 2498 个）...")
    r = requests.get(ACS_BASE, params={
        "get": var_list,
        "for": "tract:*",
        "in": "state:06 county:037",
    }, timeout=60)
    if r.status_code == 200:
        raw = r.json()
        headers, rows = raw[0], raw[1:]
        records = []
        for row in rows:
            rec = dict(zip(headers, row))
            out = {
                "geoid": f"{rec.get('state')}{rec.get('county')}{rec.get('tract')}",
                "name": rec.get("NAME"),
                "state_fips": rec.get("state"),
                "county_fips": rec.get("county"),
                "tract_id": rec.get("tract"),
            }
            for api_var, readable in CENSUS_VARS.items():
                out[readable] = _parse_census_val(rec.get(api_var))
            records.append(out)
        with open(os.path.join(out_dir, "la_census_tracts.json"), "w") as f:
            json.dump(records, f, indent=2)
        print(f"  ✓ la_census_tracts.json — {len(records)} 个 tract")
    else:
        print(f"  ✗ Census Tract 请求失败：{r.status_code}")
    time.sleep(1)

    # ZIP Code (ZCTA) 级别
    print("  获取 ZIP Code 数据（全国查询后过滤 LA）...")
    LA_ZCTA_PREFIX = ["900","901","902","903","904","905","906","907",
                      "908","910","911","912","913","914","915","916"]
    r = requests.get(ACS_BASE, params={
        "get": "NAME,B19013_001E,B01003_001E,B02001_002E,B03003_003E,B17001_002E",
        "for": "zip code tabulation area:*",
    }, timeout=120)
    if r.status_code == 200:
        raw = r.json()
        headers, rows = raw[0], raw[1:]
        la_records = []
        for row in rows:
            rec = dict(zip(headers, row))
            zcta = rec.get("zip code tabulation area", "")
            if any(zcta.startswith(p) for p in LA_ZCTA_PREFIX):
                la_records.append({
                    "zcta": zcta,
                    "name": rec.get("NAME"),
                    "total_population": _parse_census_val(rec.get("B01003_001E")),
                    "median_household_income": _parse_census_val(rec.get("B19013_001E")),
                    "race_white_alone": _parse_census_val(rec.get("B02001_002E")),
                    "hispanic_or_latino": _parse_census_val(rec.get("B03003_003E")),
                    "population_below_poverty": _parse_census_val(rec.get("B17001_002E")),
                })
        with open(os.path.join(out_dir, "la_zcta_income.json"), "w") as f:
            json.dump(la_records, f, indent=2)
        print(f"  ✓ la_zcta_income.json — {len(la_records)} 个 ZIP code")
    else:
        print(f"  ✗ ZCTA 请求失败：{r.status_code}")
    time.sleep(1)

    # 城市级别
    print("  获取城市级人口数据...")
    LA_CITIES = {
        "Los Angeles","Burbank","Glendale","Pasadena","Long Beach","Santa Monica",
        "Compton","Inglewood","Torrance","Carson","Hawthorne","El Monte","Alhambra",
        "Pomona","Norwalk","West Covina","Downey","Whittier","Culver City",
        "Beverly Hills","West Hollywood","Malibu","Calabasas","Altadena",
    }
    r = requests.get(ACS_BASE, params={
        "get": "NAME,B19013_001E,B01003_001E,B03003_003E,B17001_002E,B25077_001E",
        "for": "place:*",
        "in": "state:06",
    }, timeout=60)
    if r.status_code == 200:
        raw = r.json()
        headers, rows = raw[0], raw[1:]
        records = []
        for row in rows:
            rec = dict(zip(headers, row))
            city = rec.get("NAME","").split(" city,")[0].split(" CDP,")[0].strip()
            if city in LA_CITIES:
                records.append({
                    "place_id": rec.get("place"),
                    "city_name": city,
                    "name": rec.get("NAME"),
                    "total_population": _parse_census_val(rec.get("B01003_001E")),
                    "median_household_income": _parse_census_val(rec.get("B19013_001E")),
                    "hispanic_or_latino": _parse_census_val(rec.get("B03003_003E")),
                    "population_below_poverty": _parse_census_val(rec.get("B17001_002E")),
                    "median_home_value": _parse_census_val(rec.get("B25077_001E")),
                })
        with open(os.path.join(out_dir, "la_cities_demographics.json"), "w") as f:
            json.dump(records, f, indent=2)
        print(f"  ✓ la_cities_demographics.json — {len(records)} 个城市")
    else:
        print(f"  ✗ Cities 请求失败：{r.status_code}")


# ══════════════════════════════════════════════════════════════
# 11. NOAA 气候数据
# ══════════════════════════════════════════════════════════════
NOAA_TOKEN = os.getenv("NOAA_API_KEY", "")
NOAA_BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2"
NOAA_STATIONS = {
    "GHCND:USW00023174": "LAX International Airport",
    "GHCND:USW00093134": "Burbank Bob Hope Airport",
    "GHCND:USW00023129": "Long Beach Airport",
    "GHCND:USC00045114": "Pasadena",
    "GHCND:USC00042319": "Downtown LA",
}


def _noaa_fetch_daily(station_id, start_date, end_date):
    all_results = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    while current <= end:
        year_end = min(datetime(current.year, 12, 31), end)
        chunk_start = current.strftime("%Y-%m-%d")
        chunk_end = year_end.strftime("%Y-%m-%d")
        offset = 1
        while True:
            try:
                r = requests.get(f"{NOAA_BASE}/data",
                    headers={"token": NOAA_TOKEN},
                    params={
                        "datasetid": "GHCND", "stationid": station_id,
                        "datatypeid": "PRCP,TMAX,TMIN,TAVG,AWND,SNOW",
                        "startdate": chunk_start, "enddate": chunk_end,
                        "limit": 1000, "offset": offset, "units": "metric",
                    }, timeout=90)
            except requests.exceptions.ReadTimeout:
                print(f"    ⚠ {chunk_start}~{chunk_end} offset={offset}: 超时，跳过")
                break
            if r.status_code != 200:
                print(f"    ⚠ {chunk_start}~{chunk_end}: {r.status_code}")
                break
            results = r.json().get("results", [])
            all_results.extend(results)
            if len(results) < 1000:
                print(f"    {chunk_start} ~ {chunk_end}: {len(all_results)} 条")
                break
            offset += 1000
            time.sleep(0.3)
        current = datetime(current.year + 1, 1, 1)
        time.sleep(0.5)
    return all_results


def fetch_noaa():
    if not NOAA_TOKEN:
        print("  ⚠ 未找到 NOAA_API_KEY，跳过")
        print("  申请地址：https://www.ncdc.noaa.gov/cdo-web/token")
        return

    out_dir = os.path.join(BASE_DIR, "noaa")
    os.makedirs(out_dir, exist_ok=True)

    # 气象站列表
    r = requests.get(f"{NOAA_BASE}/stations",
        headers={"token": NOAA_TOKEN},
        params={"locationid": "FIPS:06037", "datasetid": "GHCND", "limit": 1000},
        timeout=30)
    if r.status_code == 200:
        with open(os.path.join(out_dir, "stations.json"), "w") as f:
            json.dump(r.json(), f, indent=2)
        count = r.json().get("metadata", {}).get("resultset", {}).get("count", "?")
        print(f"  ✓ stations.json — {count} 个气象站")
    time.sleep(1)

    # 日气候数据（2023-2025）
    all_station_data = {}
    for station_id, station_name in NOAA_STATIONS.items():
        print(f"  [{station_name}]")
        records = _noaa_fetch_daily(station_id, "2023-01-01", "2025-12-31")
        all_station_data[station_id] = {"station_name": station_name, "data": records}
        time.sleep(1)
    with open(os.path.join(out_dir, "daily_climate.json"), "w") as f:
        json.dump(all_station_data, f, indent=2)
    total = sum(len(v["data"]) for v in all_station_data.values())
    print(f"  ✓ daily_climate.json — 共 {total} 条")

    # 野火前后专项（2024-10 至 2025-03）
    wildfire_data = {}
    for station_id, station_name in NOAA_STATIONS.items():
        print(f"  [野火期间] {station_name}")
        records = _noaa_fetch_daily(station_id, "2024-10-01", "2025-03-31")
        wildfire_data[station_id] = {
            "station_name": station_name,
            "period": "2024-10-01 to 2025-03-31",
            "note": "Covers pre/post Palisades Fire (Jan 2025)",
            "data": records
        }
        time.sleep(1)
    with open(os.path.join(out_dir, "wildfire_period_climate.json"), "w") as f:
        json.dump(wildfire_data, f, indent=2)
    total = sum(len(v["data"]) for v in wildfire_data.values())
    print(f"  ✓ wildfire_period_climate.json — 共 {total} 条")


# ══════════════════════════════════════════════════════════════
# 12. 2025 LA 野火边界 GeoJSON
# ══════════════════════════════════════════════════════════════
# Palisades Fire: 起火 2025-01-07，位于 Santa Monica Mountains
# Eaton Fire:     起火 2025-01-07，位于 Eaton Canyon / San Gabriel Mountains
# 两场火均于 2025-01-31 完全控制

FIRE_SOURCES = [
    {
        "name": "calfire_all_perimeters",
        "desc": "CAL FIRE 历史全量火灾边界（含 2025）",
        "url": "https://gis.data.cnra.ca.gov/api/download/v1/items/c3c10388e3b24cec8a954ba10458039d/geojson?layers=0",
        "filename": "calfire_all_perimeters.geojson",
        "timeout": 120,
    },
    {
        "name": "wfigs_2025",
        "desc": "WFIGS 2025 全年野火边界（NIFC 实时数据）",
        "url": "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/WFIGS_Interagency_Perimeters_YTD/FeatureServer/0/query",
        "params": {
            "where": "FireDiscoveryDateTime >= '2025-01-01'",
            "outFields": "*",
            "f": "geojson",
            "resultRecordCount": 2000,
        },
        "filename": "wfigs_2025_perimeters.geojson",
        "timeout": 60,
    },
    {
        "name": "la_county_2025",
        "desc": "LA County eGIS - Palisades & Eaton 火灾边界",
        "url": "https://egis-lacounty.hub.arcgis.com/maps/ad51845ea5fb4eb483bc2a7c38b2370c/about",
        "filename": None,  # 需手动下载，此处仅记录来源
        "timeout": 30,
    },
]


def fetch_fire():
    out_dir = os.path.join(BASE_DIR, "fire_perimeters")
    os.makedirs(out_dir, exist_ok=True)

    # Source 1: WFIGS 2025（NIFC ArcGIS FeatureServer）
    print("  获取 WFIGS 2025 野火边界（NIFC）...")
    src = FIRE_SOURCES[1]
    try:
        r = requests.get(src["url"], params=src["params"], timeout=src["timeout"])
        if r.status_code == 200:
            data = r.json()
            features = data.get("features", [])
            with open(os.path.join(out_dir, src["filename"]), "w") as f:
                json.dump(data, f, indent=2)
            print(f"  ✓ {src['filename']} — {len(features)} 个火灾边界")

            # 单独提取 Palisades 和 Eaton Fire
            la_fires = [
                feat for feat in features
                if any(name in (feat.get("properties", {}).get("IncidentName", "") or "").lower()
                       for name in ["palisades", "eaton"])
            ]
            if la_fires:
                la_geojson = {"type": "FeatureCollection", "features": la_fires}
                with open(os.path.join(out_dir, "la_2025_fires.geojson"), "w") as f:
                    json.dump(la_geojson, f, indent=2)
                print(f"  ✓ la_2025_fires.geojson — {len(la_fires)} 个 LA 火灾（Palisades + Eaton）")
            else:
                print("  ⚠ 未在 WFIGS 结果中找到 Palisades/Eaton，尝试 CAL FIRE 数据源")
        else:
            print(f"  ⚠ WFIGS 返回 {r.status_code}")
    except Exception as e:
        print(f"  ⚠ WFIGS 请求失败：{e}")

    time.sleep(2)

    # Source 2: CAL FIRE 全量（文件较大，约 50-100MB）
    print("  获取 CAL FIRE 全量历史边界（含 2025）...")
    src = FIRE_SOURCES[0]
    try:
        r = requests.get(src["url"], timeout=src["timeout"], stream=True)
        if r.status_code == 200:
            out_path = os.path.join(out_dir, src["filename"])
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            size_mb = os.path.getsize(out_path) / 1024 / 1024
            print(f"  ✓ {src['filename']} ({size_mb:.1f} MB)")

            # 过滤出 2025 年 LA 火灾
            with open(out_path) as f:
                all_data = json.load(f)
            features = all_data.get("features", [])
            la_2025 = [
                feat for feat in features
                if (feat.get("properties", {}).get("YEAR_", "") == "2025" or
                    "2025" in str(feat.get("properties", {}).get("ALARM_DATE", ""))) and
                any(name in (feat.get("properties", {}).get("FIRE_NAME", "") or "").upper()
                    for name in ["PALISADES", "EATON"])
            ]
            if la_2025:
                with open(os.path.join(out_dir, "la_2025_fires_calfire.geojson"), "w") as f:
                    json.dump({"type": "FeatureCollection", "features": la_2025}, f, indent=2)
                print(f"  ✓ la_2025_fires_calfire.geojson — {len(la_2025)} 个边界")
        else:
            print(f"  ⚠ CAL FIRE 返回 {r.status_code}")
    except Exception as e:
        print(f"  ⚠ CAL FIRE 请求失败：{e}")

    # 说明手动来源
    print("  ℹ LA County 精细边界可手动下载：")
    print("    https://egis-lacounty.hub.arcgis.com/maps/ad51845ea5fb4eb483bc2a7c38b2370c")


# ══════════════════════════════════════════════════════════════
# 13. CDC PLACES - Census Tract 级别健康结果数据
# ══════════════════════════════════════════════════════════════
# 用途：验证水质根因是否真正导致健康损害（癌症率、肾病率等）
# 无需 API Key，Socrata 公开数据

def fetch_cdc_places():
    out_dir = os.path.join(BASE_DIR, "cdc_places")
    os.makedirs(out_dir, exist_ok=True)

    print("  获取 CDC PLACES LA County Census Tract 健康数据...")
    all_records = []
    offset = 0
    limit = 50000

    while True:
        r = requests.get(
            "https://data.cdc.gov/resource/cwsq-ngmh.json",
            params={
                "stateabbr": "CA",
                "countyname": "Los Angeles",
                "$limit": limit,
                "$offset": offset,
            },
            timeout=60
        )
        if r.status_code != 200:
            print(f"  ✗ 请求失败：{r.status_code}")
            break
        batch = r.json()
        if not batch:
            break
        all_records.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.5)

    if all_records:
        with open(os.path.join(out_dir, "la_health_outcomes.json"), "w") as f:
            json.dump(all_records, f, indent=2)

        # 统计覆盖的健康指标
        measures = list({r.get("measureid") for r in all_records if r.get("measureid")})
        print(f"  ✓ la_health_outcomes.json — {len(all_records)} 条，{len(measures)} 个健康指标")
        print(f"    指标包括：{', '.join(sorted(measures)[:8])} ...")

        # 透视为宽表（每个 tract 一行，每个指标一列）
        tract_map = {}
        for rec in all_records:
            fips = rec.get("locationname", "")
            measure = rec.get("measureid", "")
            val = rec.get("data_value")
            if fips and measure:
                if fips not in tract_map:
                    tract_map[fips] = {"tractfips": fips,
                                       "stateabbr": rec.get("stateabbr"),
                                       "countyname": rec.get("countyname")}
                try:
                    tract_map[fips][measure.lower()] = float(val) if val else None
                except (ValueError, TypeError):
                    tract_map[fips][measure.lower()] = None

        wide = list(tract_map.values())
        with open(os.path.join(out_dir, "la_health_wide.json"), "w") as f:
            json.dump(wide, f, indent=2)
        print(f"  ✓ la_health_wide.json — {len(wide)} 个 Census Tract（宽表格式）")


# ══════════════════════════════════════════════════════════════
# 14. EPA TRI - 工业有毒物质排放清单
# ══════════════════════════════════════════════════════════════
# 用途：识别供水系统附近的工业污染源（ML 根因特征）
# 无需 API Key

def fetch_epa_tri():
    out_dir = os.path.join(BASE_DIR, "epa_tri")
    os.makedirs(out_dir, exist_ok=True)

    print("  获取 EPA TRI LA County 工业排放设施...")
    all_facilities = []
    for offset in range(0, 10000, 1000):
        r = requests.get(
            f"https://data.epa.gov/efservice/TRI_FACILITY"
            f"/STATE_ABBR/CA/COUNTY/LOS%20ANGELES"
            f"/ROWS/{offset}:{offset + 1000}/JSON",
            timeout=60
        )
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        all_facilities.extend(batch)
        if len(batch) < 1000:
            break
        time.sleep(0.3)

    with open(os.path.join(out_dir, "tri_facilities.json"), "w") as f:
        json.dump(all_facilities, f, indent=2)
    print(f"  ✓ tri_facilities.json — {len(all_facilities)} 个工业设施")
    time.sleep(1)

    # 获取排放量数据（EPA Envirofacts TRI_BASIC_DATA 表）
    print("  获取 TRI 排放量数据（2020-2024）...")
    all_releases = []
    for year in range(2020, 2025):
        r = requests.get(
            f"https://data.epa.gov/efservice/TRI_BASIC_DATA"
            f"/REPORTING_YEAR/{year}/STATE_ABBR/CA/COUNTY/LOS%20ANGELES"
            f"/ROWS/0:5000/JSON",
            timeout=60
        )
        if r.status_code == 200:
            batch = r.json()
            all_releases.extend(batch)
            print(f"    {year}: {len(batch)} 条排放记录")
        else:
            print(f"    {year}: ✗ {r.status_code}")
        time.sleep(0.5)

    with open(os.path.join(out_dir, "tri_releases.json"), "w") as f:
        json.dump(all_releases, f, indent=2)
    print(f"  ✓ tri_releases.json — 共 {len(all_releases)} 条排放记录")


# ══════════════════════════════════════════════════════════════
# 15. EPA AQS - 空气质量数据
# ══════════════════════════════════════════════════════════════
# 用途：野火→空气污染→水质污染因果链的中间环节
# 需要免费 API Key：https://aqs.epa.gov/data/api/signup?email=YOUR_EMAIL
# 在 .env 中添加：AQS_EMAIL=xxx  AQS_KEY=xxx

AQS_EMAIL = os.getenv("AQS_EMAIL", "")
AQS_KEY = os.getenv("AQS_KEY", "")
AQS_BASE = "https://aqs.epa.gov/data/api"

# 关键污染物参数代码
AQS_PARAMS = {
    "88101": "PM2.5",
    "42101": "CO",
    "42602": "NO2",
    "44201": "Ozone",
    "42401": "SO2",
    "88502": "PM2.5_nonFRM",  # 用于野火烟雾监测
}


def fetch_aqs():
    if not AQS_EMAIL or not AQS_KEY:
        print("  ⚠ 未找到 AQS_EMAIL / AQS_KEY，跳过")
        print("  申请地址：https://aqs.epa.gov/data/api/signup?email=YOUR_EMAIL")
        print("  申请后在 .env 添加：AQS_EMAIL=xxx  AQS_KEY=xxx")
        return

    out_dir = os.path.join(BASE_DIR, "aqs")
    os.makedirs(out_dir, exist_ok=True)

    # 获取 LA County 监测站列表
    print("  获取 LA County AQS 监测站...")
    r = requests.get(f"{AQS_BASE}/monitors/byCounty", params={
        "email": AQS_EMAIL, "key": AQS_KEY,
        "param": "88101", "bdate": "20230101", "edate": "20251231",
        "state": "06", "county": "037",
    }, timeout=60)
    if r.status_code == 200 and r.json().get("Header", [{}])[0].get("status") == "Success":
        stations = r.json().get("Data", [])
        with open(os.path.join(out_dir, "stations.json"), "w") as f:
            json.dump(stations, f, indent=2)
        print(f"  ✓ stations.json — {len(stations)} 个空气质量监测站")
    else:
        print(f"  ⚠ 监测站请求失败：{r.status_code} {r.text[:100]}")
    time.sleep(1)

    # AQS 每次只允许查询 1 个日历年，跨年需拆分
    def _aqs_daily(param_code, bdate, edate):
        """分年查询并合并结果"""
        from datetime import date
        start_year = int(bdate[:4])
        end_year = int(edate[:4])
        combined = []
        for year in range(start_year, end_year + 1):
            y_start = f"{year}0101" if year > start_year else bdate
            y_end   = f"{year}1231" if year < end_year   else edate
            r = requests.get(f"{AQS_BASE}/dailyData/byCounty", params={
                "email": AQS_EMAIL, "key": AQS_KEY,
                "param": param_code,
                "bdate": y_start, "edate": y_end,
                "state": "06", "county": "037",
            }, timeout=60)
            if r.status_code == 200 and r.json().get("Header", [{}])[0].get("status") == "Success":
                combined.extend(r.json().get("Data", []))
            time.sleep(0.5)
        return combined

    # 获取野火前后日均数据（2024-10-01 至 2025-03-31，拆分为两年查询）
    all_data = {}
    for param_code, param_name in AQS_PARAMS.items():
        print(f"  获取 {param_name} 日均数据（野火前后）...")
        try:
            data = _aqs_daily(param_code, "20241001", "20250331")
            all_data[param_name] = data
            print(f"    ✓ {param_name}: {len(data)} 条")
        except Exception as e:
            print(f"    ⚠ {param_name} 跳过：{e}")
            all_data[param_name] = []

    with open(os.path.join(out_dir, "wildfire_period_aqi.json"), "w") as f:
        json.dump(all_data, f, indent=2)
    total = sum(len(v) for v in all_data.values())
    print(f"  ✓ wildfire_period_aqi.json — 共 {total} 条空气质量记录")

    # 获取长期年度数据（2020-2025，每年单独查询）
    annual_data = {}
    for param_code, param_name in list(AQS_PARAMS.items())[:3]:  # PM2.5, CO, NO2
        print(f"  获取 {param_name} 年度数据（2020-2025）...")
        combined = []
        for year in range(2020, 2026):
            r = requests.get(f"{AQS_BASE}/annualData/byCounty", params={
                "email": AQS_EMAIL, "key": AQS_KEY,
                "param": param_code,
                "bdate": f"{year}0101", "edate": f"{year}1231",
                "state": "06", "county": "037",
            }, timeout=60)
            if r.status_code == 200 and r.json().get("Header", [{}])[0].get("status") == "Success":
                combined.extend(r.json().get("Data", []))
            time.sleep(0.5)
        annual_data[param_name] = combined
        print(f"    ✓ {param_name}: {len(combined)} 条年度统计")

    with open(os.path.join(out_dir, "annual_aqi.json"), "w") as f:
        json.dump(annual_data, f, indent=2)
    print(f"  ✓ annual_aqi.json — PM2.5/CO/NO2 年度统计")


# ══════════════════════════════════════════════════════════════
# 16. CA GeoTracker - 地下储油罐 & 污染清理地点
# ══════════════════════════════════════════════════════════════
# 用途：苯/MTBE 等石油衍生物污染的根因来源
# 无需 API Key

def fetch_geotracker():
    out_dir = os.path.join(BASE_DIR, "geotracker")
    os.makedirs(out_dir, exist_ok=True)

    # LA County 的边界框（用于空间查询）
    LA_BBOX = {
        "xmin": -118.9448, "ymin": 33.7037,
        "xmax": -117.6462, "ymax": 34.8233,
    }

    # GeoTracker ArcGIS Feature Service（CA Water Board 公开数据）
    endpoints = [
        {
            "name": "ust_sites",
            "desc": "地下储油罐（UST）污染地点",
            "url": "https://geotracker.waterboards.ca.gov/arcgis/rest/services/GEOTRACKER/GeoTrackerPublic/MapServer/0/query",
        },
        {
            "name": "cleanup_sites",
            "desc": "污染清理中地点",
            "url": "https://geotracker.waterboards.ca.gov/arcgis/rest/services/GEOTRACKER/GeoTrackerPublic/MapServer/1/query",
        },
    ]

    # CA GeoTracker via CALEPA EnviroStor (替代 GeoTracker 直接 API)
    # EnviroStor 是 CA DTSC 管理的污染地块数据库，公开 REST API
    print("  获取 EnviroStor 污染清理地点（CA DTSC）...")
    try:
        r = requests.get(
            "https://geotracker.waterboards.ca.gov/esi/search",
            params={
                "cmd": "search",
                "county": "19",   # LA County code
                "status": "Active",
                "output": "json",
                "rows": 5000,
            },
            timeout=60
        )
        if r.status_code == 200:
            data = r.json()
            sites = data if isinstance(data, list) else data.get("sites", data.get("results", []))
            with open(os.path.join(out_dir, "geotracker_sites.json"), "w") as f:
                json.dump(sites, f, indent=2)
            print(f"  ✓ geotracker_sites.json — {len(sites)} 个地点")
        else:
            print(f"  ⚠ GeoTracker ESI: {r.status_code}")
    except Exception as e:
        print(f"  ⚠ GeoTracker ESI 请求失败：{e}")
    time.sleep(1)

    # CA EnviroStor (DTSC) - ArcGIS Feature Service
    print("  获取 CA EnviroStor 污染地块（ArcGIS）...")
    try:
        r = requests.get(
            "https://services1.arcgis.com/BbKbPoacMHPqM6dN/arcgis/rest/services"
            "/EnviroStor_Public/FeatureServer/0/query",
            params={
                "where": "COUNTY_NAME='LOS ANGELES'",
                "outFields": "SITE_ID,SITE_NAME,SITE_TYPE,SITE_STATUS,ADDRESS,"
                             "CITY,LATITUDE,LONGITUDE,REGULATORY_STATUS",
                "f": "geojson",
                "resultRecordCount": 5000,
            },
            timeout=60
        )
        if r.status_code == 200:
            data = r.json()
            features = data.get("features", [])
            with open(os.path.join(out_dir, "envirostor_sites.geojson"), "w") as f:
                json.dump(data, f, indent=2)
            print(f"  ✓ envirostor_sites.geojson — {len(features)} 个污染地块")
        else:
            print(f"  ⚠ EnviroStor ArcGIS: {r.status_code}")
    except Exception as e:
        print(f"  ⚠ EnviroStor 请求失败：{e}")
    time.sleep(1)

    # EPA RCRA 危险废物设施（补充工业污染来源）
    print("  获取 EPA RCRA 危险废物设施...")
    try:
        r = requests.get(
            "https://data.epa.gov/efservice/RCRA_FACILITIES"
            "/STATE_CODE/CA/COUNTY_NAME/LOS ANGELES/ROWS/0:3000/JSON",
            timeout=60
        )
        if r.status_code == 200:
            data = r.json()
            with open(os.path.join(out_dir, "rcra_hazardous_facilities.json"), "w") as f:
                json.dump(data, f, indent=2)
            print(f"  ✓ rcra_hazardous_facilities.json — {len(data)} 个危险废物设施")
        else:
            print(f"  ⚠ RCRA: {r.status_code}")
    except Exception as e:
        print(f"  ⚠ RCRA 请求失败：{e}")


# ══════════════════════════════════════════════════════════════
# 17. EPA EJScreen - 环境正义综合指标（Census Tract 级别）
# ══════════════════════════════════════════════════════════════
# 用途：预聚合的环境负担指标，含 TRI 临近度、Superfund 临近度、
#       交通流量、废水排放等，直接作为 ML 特征使用
# 无需 API Key

def fetch_ejscreen():
    out_dir = os.path.join(BASE_DIR, "ejscreen")
    os.makedirs(out_dir, exist_ok=True)

    # EJScreen ArcGIS Feature Service - Census Tract 级别
    # EJScreen 数据托管在 EPA ArcGIS Online
    print("  获取 EPA EJScreen LA County Census Tract 数据...")
    all_features = []
    offset = 0
    batch_size = 1000

    while True:
        r = requests.get(
            "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services"
            "/EJScreen_2024_with_AS_CNMI_GU_VI/FeatureServer/2/query",
            params={
                "where": "ID LIKE '06037%'",
                "outFields": (
                    "ID,ACSTOTPOP,CANCER,RESP,PTRAF,PWDIS,PNPL,PRMP,PTSDF,"
                    "OZONE,PM25,DSLPM,PEOPCOLORPCT,LOWINCPCT,UNEMPPCT,"
                    "LINGISOPCT,LESSHSPCT,UNDER5PCT,OVER64PCT,DEMOGINX,"
                    "PRE1960PCT,ACSIPOVBAS"
                ),
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": batch_size,
            },
            timeout=60
        )

        if r.status_code != 200:
            print(f"  ⚠ 请求失败：{r.status_code}")
            break

        data = r.json()
        features = data.get("features", [])
        all_features.extend(features)

        if len(features) < batch_size:
            break
        offset += batch_size
        time.sleep(0.3)

    if all_features:
        records = [f.get("attributes", {}) for f in all_features]
        with open(os.path.join(out_dir, "la_ejscreen_tracts.json"), "w") as f:
            json.dump(records, f, indent=2)
        print(f"  ✓ la_ejscreen_tracts.json — {len(records)} 个 Census Tract")
        print(f"    字段含：癌症风险、PM2.5、TRI临近度、Superfund临近度、人口统计等")
    else:
        print("  ⚠ ArcGIS Online 未获取到数据，尝试备用下载...")


# ══════════════════════════════════════════════════════════════
# 18. USGS 实测水质时间序列
# ══════════════════════════════════════════════════════════════
def fetch_usgs_measurements():
    out_dir = os.path.join(BASE_DIR, "usgs")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "measurements.json")
    if os.path.exists(out_path):
        print("  ⏭ measurements.json 已存在，跳过")
        return

    print("  获取 LA County USGS 实测水质时间序列（温度/DO/pH/流量/电导/浊度）...")
    try:
        r = requests.get(
            "https://waterservices.usgs.gov/nwis/dv/",
            params={
                "format": "json",
                "stateCd": "ca",
                "countyCd": "037",
                "parameterCd": "00060,00010,00300,00400,00095,63680",
                "startDT": "2020-01-01",
                "endDT": "2025-03-31",
                "siteStatus": "all",
            },
            timeout=180,
        )
        if r.status_code != 200 or not r.text.strip():
            print(f"  ⚠ measurements 返回 {r.status_code}，跳过")
            return
        try:
            data = r.json()
            with open(out_path, "w") as f:
                json.dump(data, f, indent=2)
            ts_count = len(data.get("value", {}).get("timeSeries", []))
            print(f"  ✓ measurements.json（{ts_count} 时间序列）")
        except Exception as e:
            print(f"  ⚠ JSON 解析失败，保存原始文本：{e}")
            with open(os.path.join(out_dir, "measurements.txt"), "w") as f:
                f.write(r.text)
            print("  ✓ measurements.txt（原始格式）")
    except Exception as e:
        print(f"  ⚠ fetch_usgs_measurements 请求失败：{e}")


# ══════════════════════════════════════════════════════════════
# 19. Heal the Bay 海滩水质评级
# ══════════════════════════════════════════════════════════════
def fetch_heal_the_bay():
    out_dir = os.path.join(BASE_DIR, "beach")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "beach_quality.json")
    if os.path.exists(out_path):
        print("  ⏭ beach_quality.json 已存在，跳过")
        return

    results = []

    # 方法 1：尝试 Heal the Bay 官方 API
    print("  尝试 Heal the Bay API...")
    try:
        r = requests.get(
            "https://api.healthebay.org/v1/beaches",
            params={"county": "Los Angeles", "state": "CA"},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            beaches = data if isinstance(data, list) else data.get("beaches", data.get("results", []))
            for b in beaches:
                results.append({
                    "name": b.get("name", b.get("beach_name", "")),
                    "lat": b.get("lat", b.get("latitude", None)),
                    "lon": b.get("lon", b.get("longitude", None)),
                    "grade": b.get("grade", b.get("overall_grade", "")),
                    "last_sample_date": b.get("last_sample_date", b.get("sample_date", "")),
                    "bacteria_level": b.get("bacteria_level", b.get("enterococcus", "")),
                })
            print(f"  ✓ Heal the Bay API 返回 {len(results)} 个海滩")
        else:
            print(f"  ⚠ Heal the Bay API 返回 {r.status_code}，尝试备用来源")
    except Exception as e:
        print(f"  ⚠ Heal the Bay API 失败：{e}，尝试备用来源")

    time.sleep(1)

    # 方法 2：CEDEN CA Water Board 海滩数据
    if not results:
        print("  尝试 CA Water Board CEDEN 海滩数据...")
        try:
            r = requests.get(
                "https://data.ca.gov/api/3/action/datastore_search",
                params={
                    "resource_id": "c4ae47fc-f0fa-4b0d-a2c0-ed2b8a0dea0e",
                    "filters": json.dumps({"county": "Los Angeles"}),
                    "limit": 5000,
                },
                timeout=60,
            )
            if r.status_code == 200:
                data = r.json()
                records = data.get("result", {}).get("records", [])
                for rec in records:
                    results.append({
                        "name": rec.get("StationName", rec.get("station_name", "")),
                        "lat": rec.get("TargetLatitude", rec.get("latitude", None)),
                        "lon": rec.get("TargetLongitude", rec.get("longitude", None)),
                        "grade": rec.get("Result", ""),
                        "last_sample_date": rec.get("SampleDate", rec.get("sample_date", "")),
                        "bacteria_level": rec.get("ResultQualCode", ""),
                    })
                print(f"  ✓ CEDEN 返回 {len(records)} 条记录")
            else:
                print(f"  ⚠ CEDEN 返回 {r.status_code}")
        except Exception as e:
            print(f"  ⚠ CEDEN 请求失败：{e}")
        time.sleep(1)

    # 方法 3：从 WQP 站点过滤海滩/海洋类型站点
    if not results:
        print("  从 WQP 站点过滤海滩/海洋类型...")
        wqp_path = os.path.join(BASE_DIR, "wqp", "stations.csv")
        if os.path.exists(wqp_path):
            try:
                import csv
                with open(wqp_path, newline="", encoding="utf-8", errors="replace") as csvf:
                    reader = csv.DictReader(csvf)
                    for row in reader:
                        stype = row.get("MonitoringLocationTypeName", "")
                        if "Beach" in stype or "Ocean" in stype or "Coastal" in stype:
                            try:
                                lat = float(row.get("LatitudeMeasure", 0) or 0)
                                lon = float(row.get("LongitudeMeasure", 0) or 0)
                            except (ValueError, TypeError):
                                lat, lon = None, None
                            results.append({
                                "name": row.get("MonitoringLocationName", ""),
                                "lat": lat if lat else None,
                                "lon": lon if lon else None,
                                "grade": "",
                                "last_sample_date": "",
                                "bacteria_level": "",
                            })
                print(f"  ✓ 从 WQP 过滤出 {len(results)} 个海滩/海洋站点")
            except Exception as e:
                print(f"  ⚠ WQP 过滤失败：{e}")

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"  ✓ beach_quality.json（{len(results)} 条记录）")


# ══════════════════════════════════════════════════════════════
# 20. 有害藻华（HAB）
# ══════════════════════════════════════════════════════════════
def fetch_hab():
    out_dir = os.path.join(BASE_DIR, "hab")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "la_hab_events.json")
    if os.path.exists(out_path):
        print("  ⏭ la_hab_events.json 已存在，跳过")
        return

    results = []

    # CA FHAB Bloom Reports（正确 resource ID）
    for label, rid in [
        ("Bloom Reports", "c6a36b91-ad38-4611-8750-87ee99e497dd"),
        ("HAB Cases",     "67648948-034f-4882-bbc0-c07c7d38daf9"),
        ("HAB Results",   "9d4e1df4-0cd6-4165-9e63-effcafd9dccc"),
    ]:
        print(f"  获取 CA FHAB {label}...")
        try:
            r = requests.get(
                "https://data.ca.gov/api/3/action/datastore_search",
                params={
                    "resource_id": rid,
                    "filters": json.dumps({"County": "Los Angeles"}),
                    "limit": 5000,
                },
                timeout=60,
            )
            if r.status_code == 200 and r.json().get("success"):
                records = r.json().get("result", {}).get("records", [])
                results.extend(records)
                print(f"  ✓ {label}：{len(records)} 条")
            else:
                print(f"  ⚠ {label} 返回 {r.status_code}")
        except Exception as e:
            print(f"  ⚠ {label} 请求失败：{e}")
        time.sleep(1)

    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"  ✓ la_hab_events.json（{len(results)} 条记录）")


# ══════════════════════════════════════════════════════════════
# 21. CDPR 农药使用数据
# ══════════════════════════════════════════════════════════════
def fetch_cdpr_pesticides():
    out_dir = os.path.join(BASE_DIR, "cdpr")
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, "la_pesticide_use.json")
    if os.path.exists(out_path):
        print("  ⏭ la_pesticide_use.json 已存在，跳过")
        return

    # CDPR 通过年度 ZIP 包提供数据（2023年为最新），下载后过滤 LA County（county code=19）
    print("  下载 CDPR 农药使用报告 2023 ZIP...")
    import zipfile, io
    zip_url = "https://files.cdpr.ca.gov/pub/outgoing/pur_archives/pur2023.zip"
    try:
        r = requests.get(zip_url, timeout=180, stream=True)
        if r.status_code == 200:
            z = zipfile.ZipFile(io.BytesIO(r.content))
            # 主数据在 pur2023/pur_data/udc23_XX.txt
            data_files = [n for n in z.namelist()
                          if "/pur_data/udc" in n and n.endswith(".txt")]
            records = []
            for fn in data_files:
                with z.open(fn) as f:
                    lines = f.read().decode("latin-1").splitlines()
                    if not lines:
                        continue
                    header = [h.strip() for h in lines[0].split(",")]
                    for line in lines[1:]:
                        parts = line.split(",")
                        if len(parts) >= len(header):
                            rec = dict(zip(header, [p.strip() for p in parts]))
                            if rec.get("county_cd", "").strip() == "19":
                                records.append(rec)
            with open(out_path, "w") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            print(f"  ✓ la_pesticide_use.json（{len(records)} 条 LA County 农药记录）")
        else:
            print(f"  ⚠ CDPR ZIP 返回 {r.status_code}")
            with open(out_path, "w") as f:
                json.dump([], f)
    except Exception as e:
        print(f"  ⚠ CDPR 请求失败：{e}")
        with open(out_path, "w") as f:
            json.dump([], f)


# ══════════════════════════════════════════════════════════════
# 22. NPDES 排放监测值（EPA ECHO）
# ══════════════════════════════════════════════════════════════
def fetch_npdes():
    out_dir = os.path.join(BASE_DIR, "npdes")
    os.makedirs(out_dir, exist_ok=True)

    fac_path = os.path.join(out_dir, "la_npdes_facilities.json")
    dmr_path = os.path.join(out_dir, "la_npdes_dmr.json")

    # 获取 NPDES 设施列表
    if os.path.exists(fac_path):
        print("  ⏭ la_npdes_facilities.json 已存在，跳过")
    else:
        print("  获取 EPA ECHO NPDES 废水排放设施列表（LA County）...")
        try:
            # Step 1: get_facilities 返回 QueryID
            r = requests.get(
                "https://echodata.epa.gov/echo/cwa_rest_services.get_facilities",
                params={"output": "JSON", "p_st": "CA", "p_co": "Los Angeles", "p_act": "Y"},
                timeout=60,
            )
            if r.status_code == 200:
                meta = r.json()
                res_meta = meta.get("Results", {})
                qid = res_meta.get("QueryID")
                total = res_meta.get("QueryRows", 0)
                print(f"  QueryID={qid}, 共 {total} 个设施，逐页抓取...")
                # Step 2: get_qid 分页获取设施详情
                facilities = []
                page = 1
                while True:
                    r2 = requests.get(
                        "https://echodata.epa.gov/echo/cwa_rest_services.get_qid",
                        params={"output": "JSON", "qid": qid, "pageno": page},
                        timeout=60,
                    )
                    if r2.status_code != 200:
                        break
                    batch = r2.json().get("Results", {}).get("Facilities", [])
                    if not batch:
                        break
                    facilities.extend(batch)
                    if len(batch) < 1000:
                        break
                    page += 1
                    time.sleep(0.3)
                with open(fac_path, "w") as f:
                    json.dump(facilities, f, indent=2)
                print(f"  ✓ la_npdes_facilities.json（{len(facilities)} 个设施）")
            else:
                print(f"  ⚠ NPDES 设施列表返回 {r.status_code}")
                with open(fac_path, "w") as f:
                    json.dump([], f)
        except Exception as e:
            print(f"  ⚠ NPDES 设施列表请求失败：{e}")
            with open(fac_path, "w") as f:
                json.dump([], f)

    time.sleep(1)

    # 获取排放监测报告
    if os.path.exists(dmr_path):
        print("  ⏭ la_npdes_dmr.json 已存在，跳过")
    else:
        print("  获取 EPA ECHO NPDES 排放监测报告（2024 年）...")
        try:
            r = requests.get(
                "https://echodata.epa.gov/echo/dmr_rest_services.get_custom_data_annual",
                params={
                    "p_st": "CA",
                    "p_county": "LOS ANGELES",
                    "p_year": "2024",
                    "output": "JSON",
                },
                timeout=90,
            )
            if r.status_code == 200:
                data = r.json()
                with open(dmr_path, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"  ✓ la_npdes_dmr.json")
            else:
                print(f"  ⚠ NPDES DMR 返回 {r.status_code}")
                with open(dmr_path, "w") as f:
                    json.dump({}, f)
        except Exception as e:
            print(f"  ⚠ NPDES DMR 请求失败：{e}")
            with open(dmr_path, "w") as f:
                json.dump({}, f)


# ══════════════════════════════════════════════════════════════
# 主程序
# ══════════════════════════════════════════════════════════════
SOURCES = {
    "wqp":       ("Water Quality Portal（监测站 + 检测记录）",      fetch_wqp),
    "usgs":      ("USGS 水文数据",                                  fetch_usgs),
    "usgs_meas": ("USGS 实测水质时间序列（温度/DO/pH/流量等）",      fetch_usgs_measurements),
    "ca":        ("California Open Data（地下水 + 违规）",          fetch_ca_open_data),
    "la":        ("LA Open Data（本地水质）",                       fetch_la_open_data),
    "epa_echo":  ("EPA ECHO（设施 + 执法记录）",                    fetch_epa_echo),
    "epa_sdwis": ("EPA SDWIS（供水系统信息）",                      fetch_epa_sdwis),
    "ewg":       ("EWG 主要 6 系统（快）",                          fetch_ewg),
    "ewg_all":   ("EWG 全部 300+ 系统（慢，约 10 分钟）",           fetch_ewg_all),
    "ladwp":     ("LADWP 年度 PDF 报告 2004-2024",                  fetch_ladwp),
    "census":    ("US Census 人口/收入数据（无需 Key）",             fetch_census),
    "noaa":      ("NOAA 气候数据（需 NOAA_API_KEY）",               fetch_noaa),
    "fire":      ("2025 LA 野火边界 GeoJSON（Palisades + Eaton）",  fetch_fire),
    "cdc":       ("CDC PLACES 健康结果数据（癌症/肾病等，无需 Key）",fetch_cdc_places),
    "tri":       ("EPA TRI 工业有毒排放设施（无需 Key）",            fetch_epa_tri),
    "aqs":       ("EPA AQS 空气质量数据（需 AQS_EMAIL + AQS_KEY）",fetch_aqs),
    "geotracker":("CA GeoTracker 地下储油罐 + 污染地块（无需 Key）",fetch_geotracker),
    "ejscreen":  ("EPA EJScreen 环境正义综合指标（无需 Key）",       fetch_ejscreen),
    "beach":     ("Heal the Bay 海滩水质评级（LA County 海滩）",     fetch_heal_the_bay),
    "hab":       ("CA Water Board 有害藻华事件（HAB）",              fetch_hab),
    "cdpr":      ("CDPR 农药使用报告（LA County）",                  fetch_cdpr_pesticides),
    "npdes":     ("EPA ECHO NPDES 废水排放监测值",                   fetch_npdes),
}

DEFAULT_ORDER = ["wqp", "usgs", "usgs_meas", "ca", "la", "epa_echo", "epa_sdwis",
                 "ewg", "ladwp", "census", "noaa", "fire",
                 "cdc", "tri", "aqs", "geotracker", "ejscreen",
                 "beach", "hab", "cdpr", "npdes"]

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--list" in args or "-l" in args:
        print("\n可用数据源：")
        for key, (desc, _) in SOURCES.items():
            print(f"  {key:<12} {desc}")
        print("\n用法：")
        print("  python src/fetch_all.py              # 运行全部（不含 ewg_all）")
        print("  python src/fetch_all.py census noaa  # 只运行指定源")
        print("  python src/fetch_all.py ewg_all      # 全量 EWG（慢）")
        sys.exit(0)

    targets = args if args else DEFAULT_ORDER
    invalid = [t for t in targets if t not in SOURCES]
    if invalid:
        print(f"未知数据源：{invalid}，运行 --list 查看可用选项")
        sys.exit(1)

    print("=" * 55)
    print("LA Water Quality - 数据收集")
    print(f"目标：{', '.join(targets)}")
    print("=" * 55)

    start = time.time()
    for key in targets:
        desc, fn = SOURCES[key]
        print(f"\n{'─' * 55}")
        print(f"【{key.upper()}】{desc}")
        print("─" * 55)
        try:
            fn()
        except Exception as e:
            print(f"  ✗ 出错：{e}")
        time.sleep(1)

    elapsed = int(time.time() - start)
    print(f"\n{'=' * 55}")
    print(f"完成！耗时 {elapsed // 60}分{elapsed % 60}秒，数据在 data/raw_data/")
    print("=" * 55)
