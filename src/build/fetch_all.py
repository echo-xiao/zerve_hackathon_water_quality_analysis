"""
US National Water Quality & Resource Data Collector
直接写入 Google Cloud Storage，无需本地存储

用法：
  python src/build/fetch_all.py                         # 运行所有原始数据源（写入 GCS）
  python src/build/fetch_all.py wqp census              # 只运行指定数据源
  python src/build/fetch_all.py --list                  # 列出所有可用数据源
  python src/build/fetch_all.py --status                # 查看当前抓取进度
  python src/build/fetch_all.py --workers 8             # 指定并行线程数（默认4）
  python src/build/fetch_all.py --features              # 补充特征数据（更新 features.parquet）
  python src/build/fetch_all.py --features --skip-usgs  # 跳过慢速 USGS 地下水

需要在 .env 中设置：
  GCS_BUCKET=zerve_hackathon
"""

import requests
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from dotenv import load_dotenv
from google.cloud import storage as gcs_storage

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.env"))

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET", "zerve_hackathon")
GCS_PROJECT     = os.getenv("GCS_PROJECT", "gen-lang-client-0371685655")
GCS_PREFIX      = "raw_data"

STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}
ABBR_TO_FIPS = {v: k for k, v in STATE_FIPS.items()}
N_STATES     = len(STATE_FIPS)
WORKERS      = 16


# ── GCS 操作 ─────────────────────────────────────────────────────────

_gcs_client = None
_gcs_bucket = None
_gcs_lock   = threading.Lock()

def _get_bucket():
    global _gcs_client, _gcs_bucket
    with _gcs_lock:
        if _gcs_bucket is None:
            _gcs_client = gcs_storage.Client(project=GCS_PROJECT)
            _gcs_bucket = _gcs_client.bucket(GCS_BUCKET_NAME)
    return _gcs_bucket

def _key(rel_path: str) -> str:
    return f"{GCS_PREFIX}/{rel_path}"

def _exists(rel_path: str) -> bool:
    return _get_bucket().blob(_key(rel_path)).exists()

def _list_existing(prefix: str) -> set:
    """批量列出 GCS 中某前缀下所有文件，返回 rel_path 集合（一次请求）。"""
    full_prefix = f"{GCS_PREFIX}/{prefix}"
    return {
        b.name[len(f"{GCS_PREFIX}/"):]
        for b in _get_bucket().list_blobs(prefix=full_prefix)
    }

def _put_bytes(rel_path: str, content: bytes, content_type: str = "application/octet-stream"):
    blob = _get_bucket().blob(_key(rel_path))
    blob.upload_from_string(content, content_type=content_type)

def _put_json(rel_path: str, data):
    _put_bytes(rel_path, json.dumps(data).encode(), "application/json")

def _put_text(rel_path: str, text: str):
    _put_bytes(rel_path, text.encode(), "text/plain; charset=utf-8")

def _put_csv(rel_path: str, content: bytes):
    _put_bytes(rel_path, content, "text/csv")


# ── 进度条（线程安全）─────────────────────────────────────────────────

def _fmt_secs(secs: float) -> str:
    if secs == float("inf") or secs < 0:
        return "?"
    secs = int(secs)
    if secs >= 3600:
        return f"{secs // 3600}h{(secs % 3600) // 60}m"
    elif secs >= 60:
        return f"{secs // 60}m{secs % 60:02d}s"
    return f"{secs}s"


class Progress:
    def __init__(self, total: int, label: str = ""):
        self._lock   = threading.Lock()
        self.total   = max(1, total)
        self.done    = 0
        self.skipped = 0
        self.fetched = 0
        self.t0      = time.time()
        self._last_t = 0.0
        print(f"  → 共 {total} 个文件目标" + (f"（{label}）" if label else ""))

    def tick(self, skipped: bool = False):
        with self._lock:
            self.done += 1
            if skipped:
                self.skipped += 1
            else:
                self.fetched += 1
            now = time.time()
            if now - self._last_t >= 1.5 or self.done == self.total:
                self._last_t = now
                self._render()

    def _render(self):
        elapsed = time.time() - self.t0
        pct     = self.done / self.total * 100
        rate    = self.fetched / elapsed if elapsed > 0 and self.fetched > 0 else 0
        remain  = self.total - self.done
        eta_sec = remain / rate if rate > 0 else float("inf")
        W       = 28
        filled  = int(W * self.done / self.total)
        bar     = "█" * filled + "░" * (W - filled)
        line = (
            f"\r  [{bar}] {self.done}/{self.total} ({pct:.0f}%)"
            f"  ↓{self.fetched}获取 ⏭{self.skipped}跳过"
            f"  已用{_fmt_secs(elapsed)} ETA {_fmt_secs(eta_sec)}"
        )
        print(line.ljust(96), end="", flush=True)
        if self.done == self.total:
            print()

    def summary(self):
        elapsed = time.time() - self.t0
        print(f"  ✔ 完成：{self.fetched} 新获取 / {self.skipped} 跳过，耗时 {_fmt_secs(elapsed)}")


# ── 日期工具 ──────────────────────────────────────────────────────────

def iter_weeks(start: str, end: str):
    cur    = date.fromisoformat(start)
    end_dt = date.fromisoformat(end)
    while cur <= end_dt:
        week_end = min(cur + timedelta(days=6), end_dt)
        yield cur.isoformat(), week_end.isoformat()
        cur = week_end + timedelta(days=1)

def _weeks_list(start, end):
    return list(iter_weeks(start, end))

def iter_weeks_by_year(start_year, end_year):
    for year in range(start_year, end_year + 1):
        yield from iter_weeks(f"{year}-01-01", f"{year}-12-31")

def _weeks_by_year_list(start_year, end_year):
    return list(iter_weeks_by_year(start_year, end_year))


# ── 进度状态 ──────────────────────────────────────────────────────────

def show_status(targets: list):
    print("\n  正在读取 GCS bucket 文件列表...")
    existing = set(
        b.name for b in _get_bucket().list_blobs(prefix=GCS_PREFIX + "/")
    )

    WEEKS_MEAS = _weeks_list(MEAS_START, MEAS_END)
    WQP_YEARS  = list(range(WQP_START_YEAR, WQP_END_YEAR + 1))
    USGS_YEARS = list(range(2020, 2026))

    specs = {
        "wqp": {"label": "Water Quality Portal", "targets":
            [f"wqp/{a}_stations.csv" for a in STATE_FIPS.values()] +
            [f"wqp/{a}_results_{y}.csv" for a in STATE_FIPS.values() for y in WQP_YEARS]},
        "usgs": {"label": "USGS 水文数据", "targets":
            [f"usgs/{a}_sites.txt" for a in STATE_FIPS.values()] +
            [f"usgs/{a}_historical_{y}.json" for a in STATE_FIPS.values() for y in USGS_YEARS]},
        "usgs_meas": {"label": "USGS 实测时间序列", "targets":
            [f"usgs_meas/{a}/{ws}.json" for a in STATE_FIPS.values() for ws, _ in WEEKS_MEAS]},
        "epa_sdwis": {"label": "EPA SDWIS", "targets":
            [f"epa_sdwis/{a}_water_systems.json" for a in STATE_FIPS.values()]},
        "census": {"label": "US Census", "targets":
            ["census/national_counties.json"] +
            [f"census/{a}_tracts.json" for a in STATE_FIPS.values()]},
        "cdc": {"label": "CDC PLACES", "targets":
            [f"cdc_places/{a}_health_outcomes.json" for a in STATE_FIPS.values()]},
        "tri": {"label": "EPA TRI", "targets":
            [f"epa_tri/{a}_facilities.json" for a in STATE_FIPS.values()]},
        "npdes": {"label": "EPA ECHO NPDES", "targets":
            [f"npdes/{a}_facilities.json" for a in STATE_FIPS.values()] +
            [f"npdes/{a}_dmr.json" for a in STATE_FIPS.values()]},
    }

    print("\n" + "=" * 65)
    print(f"  数据抓取进度总览  [bucket: {GCS_BUCKET_NAME}]")
    print("=" * 65)
    print(f"  {'数据源':<14} {'完成':>6} {'总计':>6}  进度")
    print("  " + "-" * 55)
    grand_done = grand_total = 0
    for key in targets:
        spec = specs.get(key)
        if not spec:
            continue
        tgt_list = spec["targets"]
        total = len(tgt_list)
        done  = sum(1 for p in tgt_list if _key(p) in existing)
        pct   = done / total * 100 if total else 0
        W     = 15
        bar   = "█" * int(W * done / total) + "░" * (W - int(W * done / total))
        print(f"  {key:<14} {done:>6,} {total:>6,}  [{bar}] {pct:5.1f}%")
        grand_done  += done
        grand_total += total
    print("  " + "-" * 55)
    pct_all = grand_done / grand_total * 100 if grand_total else 0
    print(f"  {'合计':<14} {grand_done:>6,} {grand_total:>6,}  {pct_all:.1f}%")
    print("=" * 65 + "\n")


# ══════════════════════════════════════════════════════════════
# 1. WQP
# ══════════════════════════════════════════════════════════════
WQP_START_YEAR = 2020
WQP_END_YEAR   = 2025

def fetch_wqp(workers: int = WORKERS):
    years    = list(range(WQP_START_YEAR, WQP_END_YEAR + 1))
    existing = _list_existing("wqp/")
    prog     = Progress(N_STATES * (1 + len(years)), f"按州+年，{workers}线程→GCS")

    def _fetch_state(item):
        fips, abbr = item
        rel = f"wqp/{abbr}_stations.csv"
        if rel in existing:
            prog.tick(skipped=True)
        else:
            try:
                r = requests.get("https://www.waterqualitydata.us/data/Station/search",
                    params={"statecode": f"US:{fips}", "mimeType": "csv", "zip": "no"},
                    headers={"Accept": "text/csv"}, timeout=180)
                _put_csv(rel, r.content)
            except Exception as e:
                print(f"\n  ✗ {abbr} stations: {e}")
            prog.tick()
            time.sleep(1)
        for year in years:
            rel = f"wqp/{abbr}_results_{year}.csv"
            if rel in existing:
                prog.tick(skipped=True)
                continue
            try:
                r = requests.get("https://www.waterqualitydata.us/data/Result/search",
                    params={"statecode": f"US:{fips}", "startDateLo": f"01-01-{year}",
                            "startDateHi": f"12-31-{year}", "mimeType": "csv", "zip": "no"},
                    headers={"Accept": "text/csv"}, timeout=300)
                _put_csv(rel, r.content)
            except Exception as e:
                print(f"\n  ✗ {abbr} results {year}: {e}")
            prog.tick()
            time.sleep(2)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 2. USGS
# ══════════════════════════════════════════════════════════════
def fetch_usgs(workers: int = WORKERS):
    years    = list(range(2020, 2026))
    existing = _list_existing("usgs/")
    prog     = Progress(N_STATES * (1 + len(years)), f"按州+年，{workers}线程→GCS")

    def _fetch_state(item):
        fips, abbr = item
        rel = f"usgs/{abbr}_sites.txt"
        if rel in existing:
            prog.tick(skipped=True)
        else:
            try:
                r = requests.get("https://waterservices.usgs.gov/nwis/site/",
                    params={"format": "rdb", "stateCd": abbr.lower(),
                            "siteType": "ST,LK,GW", "siteStatus": "all", "hasDataTypeCd": "qw"},
                    timeout=120)
                if r.status_code == 200:
                    _put_text(rel, r.text)
            except Exception as e:
                print(f"\n  ✗ {abbr} sites: {e}")
            prog.tick()
            time.sleep(0.5)
        for year in years:
            rel = f"usgs/{abbr}_historical_{year}.json"
            if rel in existing:
                prog.tick(skipped=True)
                continue
            try:
                r = requests.get("https://waterservices.usgs.gov/nwis/dv/",
                    params={"format": "json", "stateCd": abbr.lower(), "parameterCd": "00095",
                            "startDT": f"{year}-01-01", "endDT": f"{year}-12-31", "siteStatus": "all"},
                    timeout=180)
                if r.status_code == 200 and r.text.strip():
                    _put_text(rel, r.text)
            except Exception as e:
                print(f"\n  ✗ {abbr} historical {year}: {e}")
            prog.tick()
            time.sleep(0.5)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 3. USGS measurements
# ══════════════════════════════════════════════════════════════
MEAS_PARAMS = "00060,00010,00300,00400,00095,63680"
MEAS_START  = "2020-01-01"
MEAS_END    = "2025-03-31"

def fetch_usgs_measurements(workers: int = WORKERS):
    weeks    = _weeks_list(MEAS_START, MEAS_END)
    existing = _list_existing("usgs_meas/")
    prog     = Progress(N_STATES * len(weeks), f"按州×周，{workers}线程→GCS")

    def _fetch_state(item):
        fips, abbr = item
        for w_start, w_end in weeks:
            rel = f"usgs_meas/{abbr}/{w_start}.json"
            if rel in existing:
                prog.tick(skipped=True)
                continue
            try:
                r = requests.get("https://waterservices.usgs.gov/nwis/dv/",
                    params={"format": "json", "stateCd": abbr.lower(), "parameterCd": MEAS_PARAMS,
                            "startDT": w_start, "endDT": w_end, "siteStatus": "all"},
                    timeout=180)
                if r.status_code == 200 and r.text.strip():
                    _put_json(rel, r.json())
            except Exception as e:
                print(f"\n  ✗ {abbr} {w_start}: {e}")
            prog.tick()
            time.sleep(0.3)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 4. EPA SDWIS
# ══════════════════════════════════════════════════════════════
def fetch_epa_sdwis(workers: int = WORKERS):
    existing = _list_existing("epa_sdwis/")
    prog     = Progress(N_STATES, f"按州，{workers}线程→GCS")

    def _fetch_state(item):
        fips, abbr = item
        rel = f"epa_sdwis/{abbr}_water_systems.json"
        if rel in existing:
            prog.tick(skipped=True)
            return
        records = []
        for offset in range(0, 100_000, 1000):
            try:
                r = requests.get(
                    f"https://data.epa.gov/efservice/WATER_SYSTEM/PRIMACY_AGENCY_CODE/{abbr}/ROWS/{offset}:{offset+1000}/JSON",
                    timeout=60)
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch or isinstance(batch, dict):
                    break
                records.extend(batch)
                if len(batch) < 1000:
                    break
                time.sleep(0.2)
            except Exception as e:
                print(f"\n  ✗ {abbr} offset={offset}: {e}")
                break
        _put_json(rel, records)
        prog.tick()
        time.sleep(0.3)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 6. Census
# ══════════════════════════════════════════════════════════════
ACS_YEAR = "2023"
ACS_BASE = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
CENSUS_VARS = {
    "B01003_001E": "total_population", "B19013_001E": "median_household_income",
    "B19301_001E": "per_capita_income", "B17001_002E": "population_below_poverty",
    "B02001_002E": "race_white_alone",  "B02001_003E": "race_black_alone",
    "B02001_005E": "race_asian_alone",  "B03003_003E": "hispanic_or_latino",
    "B15003_022E": "edu_bachelors",     "B25003_001E": "housing_total_units",
    "B25077_001E": "median_home_value", "B25034_001E": "housing_age_total",
    "B25034_011E": "housing_built_1939_or_earlier", "B25034_010E": "housing_built_1940_1949",
    "B25034_009E": "housing_built_1950_1959",       "B25034_008E": "housing_built_1960_1969",
    "B25034_007E": "housing_built_1970_1979",
}

def _parse_census_val(v):
    try:
        vi = int(v)
        return None if vi < 0 else vi
    except (ValueError, TypeError):
        return None

def fetch_census(workers: int = WORKERS):
    var_list = ",".join(["NAME"] + list(CENSUS_VARS.keys()))
    prog     = Progress(1 + N_STATES, f"全国County+按州Tract，{workers}线程→GCS")

    existing = _list_existing("census/")
    rel = "census/national_counties.json"
    if rel in existing:
        prog.tick(skipped=True)
    else:
        try:
            r = requests.get(ACS_BASE, params={"get": var_list, "for": "county:*", "in": "state:*"}, timeout=180)
            if r.status_code == 200:
                raw = r.json()
                hdrs = raw[0]
                records = []
                for row in raw[1:]:
                    rec = dict(zip(hdrs, row))
                    out = {"geoid": f"{rec.get('state')}{rec.get('county')}", "name": rec.get("NAME"),
                           "state_fips": rec.get("state"), "county_fips": rec.get("county")}
                    for k, v in CENSUS_VARS.items():
                        out[v] = _parse_census_val(rec.get(k))
                    records.append(out)
                _put_json(rel, records)
        except Exception as e:
            print(f"\n  ✗ national counties: {e}")
        prog.tick()

    def _fetch_state(item):
        fips, abbr = item
        rel = f"census/{abbr}_tracts.json"
        if rel in existing:
            prog.tick(skipped=True)
            return
        try:
            r = requests.get(ACS_BASE,
                params={"get": var_list, "for": "tract:*", "in": f"state:{fips} county:*"}, timeout=120)
            if r.status_code == 200:
                raw = r.json()
                hdrs = raw[0]
                records = []
                for row in raw[1:]:
                    rec = dict(zip(hdrs, row))
                    out = {"geoid": f"{rec.get('state')}{rec.get('county')}{rec.get('tract')}",
                           "name": rec.get("NAME"), "state_fips": rec.get("state"),
                           "county_fips": rec.get("county"), "tract_id": rec.get("tract")}
                    for k, v in CENSUS_VARS.items():
                        out[v] = _parse_census_val(rec.get(k))
                    records.append(out)
                _put_json(rel, records)
        except Exception as e:
            print(f"\n  ✗ {abbr} tracts: {e}")
        prog.tick()
        time.sleep(0.5)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 7. CDC PLACES
# ══════════════════════════════════════════════════════════════
def fetch_cdc_places(workers: int = WORKERS):
    existing = _list_existing("cdc_places/")
    prog     = Progress(N_STATES, f"按州，{workers}线程→GCS")

    def _fetch_state(item):
        fips, abbr = item
        rel = f"cdc_places/{abbr}_health_outcomes.json"
        if rel in existing:
            prog.tick(skipped=True)
            return
        records, offset = [], 0
        while True:
            try:
                r = requests.get("https://data.cdc.gov/resource/cwsq-ngmh.json",
                    params={"stateabbr": abbr, "$limit": 50000, "$offset": offset}, timeout=60)
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch:
                    break
                records.extend(batch)
                if len(batch) < 50000:
                    break
                offset += 50000
                time.sleep(0.5)
            except Exception as e:
                print(f"\n  ✗ {abbr}: {e}")
                break
        _put_json(rel, records)
        prog.tick()
        time.sleep(0.3)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 9. EPA TRI
# ══════════════════════════════════════════════════════════════
def fetch_epa_tri(workers: int = WORKERS):
    existing = _list_existing("epa_tri/")
    prog     = Progress(N_STATES, f"按州，{workers}线程→GCS")

    def _fetch_state(item):
        fips, abbr = item
        rel = f"epa_tri/{abbr}_facilities.json"
        if rel in existing:
            prog.tick(skipped=True)
            return
        facilities = []
        for offset in range(0, 50_000, 1000):
            try:
                r = requests.get(
                    f"https://data.epa.gov/efservice/TRI_FACILITY/STATE_ABBR/{abbr}/ROWS/{offset}:{offset+1000}/JSON",
                    timeout=60)
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch or isinstance(batch, dict):
                    break
                facilities.extend(batch)
                if len(batch) < 1000:
                    break
                time.sleep(0.2)
            except Exception as e:
                print(f"\n  ✗ {abbr} offset={offset}: {e}")
                break
        _put_json(rel, facilities)
        prog.tick()
        time.sleep(0.3)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 9. NPDES
# ══════════════════════════════════════════════════════════════
def fetch_npdes(workers: int = WORKERS):
    existing = _list_existing("npdes/")
    prog     = Progress(N_STATES * 2, f"设施+DMR，{workers}线程→GCS")

    def _fetch_state(item):
        fips, abbr = item
        rel = f"npdes/{abbr}_facilities.json"
        if rel in existing:
            prog.tick(skipped=True)
        else:
            try:
                r = requests.get("https://echodata.epa.gov/echo/cwa_rest_services.get_facilities",
                    params={"output": "JSON", "p_st": abbr, "p_act": "Y"}, timeout=60)
                facilities = []
                if r.status_code == 200:
                    qid  = r.json().get("Results", {}).get("QueryID")
                    page = 1
                    while qid:
                        r2 = requests.get("https://echodata.epa.gov/echo/cwa_rest_services.get_qid",
                            params={"output": "JSON", "qid": qid, "pageno": page}, timeout=60)
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
                _put_json(rel, facilities)
            except Exception as e:
                print(f"\n  ✗ {abbr} npdes: {e}")
                _put_json(rel, [])
            prog.tick()
            time.sleep(0.5)

        rel = f"npdes/{abbr}_dmr.json"
        if rel in existing:
            prog.tick(skipped=True)
        else:
            try:
                r = requests.get(
                    "https://echodata.epa.gov/echo/dmr_rest_services.get_custom_data_annual",
                    params={"p_st": abbr, "p_year": "2024", "output": "JSON"}, timeout=90)
                _put_json(rel, r.json() if r.status_code == 200 else {})
            except Exception as e:
                print(f"\n  ✗ {abbr} dmr: {e}")
                _put_json(rel, {})
            prog.tick()
            time.sleep(0.5)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, STATE_FIPS.items()))
    prog.summary()




def fetch_snotel(workers: int = 4):
    """NRCS SNOTEL 全美西部积雪量（SWE）+ 降水 → GCS raw_data/snotel/"""
    SNOTEL_STATES = ["AK","AZ","CA","CO","ID","MT","NM","NV","OR","UT","WA","WY"]
    existing = _list_existing("snotel/")
    prog = Progress(len(SNOTEL_STATES), f"西部{len(SNOTEL_STATES)}州 SNOTEL")

    def _fetch_state(abbr):
        rel = f"snotel/{abbr}_snotel_daily.csv"
        if rel in existing:
            prog.tick(skipped=True)
            return
        url = (
            "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/"
            "customMultipleStationReport/daily/start_of_period/"
            f"state=%22{abbr}%22%20AND%20network=%22SNTL%22/"
            "2000-01-01,2026-04-25/"
            "WTEQ::value,PREC::value,SNWD::value,TOBS::value"
        )
        try:
            r = requests.get(url, timeout=180)
            if r.status_code == 200:
                _put_csv(rel, r.content)
                n = r.content.count(b"\n")
                print(f"\n  ✓ snotel/{abbr}  ({n} rows)")
        except Exception as e:
            print(f"\n  ✗ SNOTEL {abbr}: {e}")
        prog.tick()

    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(_fetch_state, SNOTEL_STATES))
    prog.summary()




def fetch_usda_water(workers: int = 1):
    """USGS 全美农业/城市用水量（Water Use Data）→ GCS raw_data/water_use/"""
    rel = "water_use/us_water_use.rdb"
    if _exists(rel):
        print("  ✓ water_use/us_water_use.rdb 已存在，跳过")
        return
    print("  正在下载 USGS 全美用水量数据...")
    url = (
        "https://waterdata.usgs.gov/nwis/water_use"
        "?format=rdb&rdb_compression=value"
        "&wu_area=State&wu_year=ALL&wu_county=000"
        "&wu_category=TO,IR,PS,IN,AQ,MI,TE,CO"
        "&wu_county_nms=--All+Counties--"
        "&wu_basin_nms=--All+Basins--"
    )
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    _put_bytes(rel, r.content, "text/plain")
    n_rows = r.content.count(b"\n")
    print(f"  ✓ water_use/us_water_use.rdb  ({n_rows} rows)")


def fetch_drought(workers: int = 1):
    """US Drought Monitor 全美干旱指数 + NOAA 各州气温 → GCS raw_data/drought/"""
    # 1. US Drought Monitor 全美周度数据（2000—今）
    rel_dm = "drought/us_drought_monitor.csv"
    if not _exists(rel_dm):
        print("  正在下载 US Drought Monitor 全美数据...")
        url = (
            "https://droughtmonitor.unl.edu/DmData/DataDownload/ComprehensiveStatistics.aspx"
            "?aoi=national&startdate=2000-01-01&enddate=2026-04-25"
            "&timeseries=Weekly&statistic=0&type=1&statstype=1"
        )
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                _put_csv(rel_dm, r.content)
                n = r.content.count(b"\n")
                print(f"  ✓ drought/us_drought_monitor.csv  ({n} rows)")
            else:
                print(f"  ✗ Drought Monitor: HTTP {r.status_code}")
        except Exception as e:
            print(f"  ✗ Drought Monitor: {e}")
    else:
        print("  ✓ drought/us_drought_monitor.csv 已存在，跳过")

    # 2. US Drought Monitor 州级周度数据（2015—今）
    rel_state = "drought/state_drought_monitor.csv"
    if not _exists(rel_state):
        print("  正在下载 US Drought Monitor 州级数据...")
        url_state = (
            "https://droughtmonitor.unl.edu/DmData/DataDownload/ComprehensiveStatistics.aspx"
            "?aoi=state&startdate=2015-01-01&enddate=2026-04-25"
            "&timeseries=Weekly&statistic=0&type=1&statstype=1"
        )
        try:
            r = requests.get(url_state, timeout=180)
            if r.status_code == 200:
                _put_csv(rel_state, r.content)
                n = r.content.count(b"\n")
                print(f"  ✓ drought/state_drought_monitor.csv  ({n} rows)")
            else:
                print(f"  ✗ State Drought Monitor: HTTP {r.status_code}")
        except Exception as e:
            print(f"  ✗ State Drought Monitor: {e}")
    else:
        print("  ✓ drought/state_drought_monitor.csv 已存在，跳过")

    # 3. NOAA CDO 加州月均气温（Sacramento + LA）
    noaa_key = os.getenv("NOAA_API_KEY", "")
    for station, name in [("GHCND:USW00023271", "sacramento"), ("GHCND:USW00023174", "la")]:
        rel_t = f"drought/noaa_temp_{name}.json"
        if _exists(rel_t):
            print(f"  ✓ drought/noaa_temp_{name}.json 已存在，跳过")
            continue
        if not noaa_key:
            print("  ✗ 未找到 NOAA_API_KEY，跳过气温下载")
            break
        url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
        params = {
            "datasetid": "GHCND", "stationid": station,
            "datatypeid": "TAVG,TMAX,TMIN,PRCP",
            "startdate": "2018-01-01", "enddate": "2026-04-25",
            "limit": 1000, "units": "metric", "offset": 1,
        }
        headers = {"token": noaa_key}
        all_results = []
        while True:
            try:
                r = requests.get(url, params=params, headers=headers, timeout=30)
                if r.status_code != 200:
                    break
                data = r.json()
                results = data.get("results", [])
                all_results.extend(results)
                meta = data.get("metadata", {}).get("resultset", {})
                if params["offset"] + params["limit"] > meta.get("count", 0):
                    break
                params["offset"] += params["limit"]
                time.sleep(0.25)
            except Exception as e:
                print(f"  ✗ NOAA {name}: {e}")
                break
        if all_results:
            _put_json(rel_t, all_results)
            print(f"  ✓ drought/noaa_temp_{name}.json  ({len(all_results)} records)")


def fetch_usda_nass(workers: int = 1):
    """USDA NASS 全美作物灌溉用水量+产值+面积 → GCS raw_data/usda_nass/"""
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key")
        return

    queries = [
        # 灌溉用水量（每英亩acre-feet） — CENSUS, STATE
        ("water_applied", {
            "source_desc": "CENSUS", "agg_level_desc": "STATE",
            "statisticcat_desc": "WATER APPLIED", "unit_desc": "ACRE FEET / ACRE",
        }),
        # 灌溉用水量 — SURVEY（Farm & Ranch Irrigation Survey，更细粒度作物）
        ("water_applied_survey", {
            "source_desc": "SURVEY", "agg_level_desc": "STATE",
            "statisticcat_desc": "WATER APPLIED", "unit_desc": "ACRE FEET / ACRE",
        }),
        # 灌溉面积（露天灌溉） — CENSUS, STATE
        ("irrigated_area", {
            "source_desc": "CENSUS", "agg_level_desc": "STATE",
            "statisticcat_desc": "AREA HARVESTED",
            "prodn_practice_desc": "IN THE OPEN, IRRIGATED",
        }),
        # 灌溉面积 — SURVEY（更细粒度作物，如 ALMONDS/PISTACHIOS/GRAPES 等）
        ("irrigated_area_survey", {
            "source_desc": "SURVEY", "agg_level_desc": "STATE",
            "statisticcat_desc": "AREA HARVESTED",
            "prodn_practice_desc": "IRRIGATED",
        }),
        # 作物产量（实物单位BU/TONS等） — CENSUS, STATE，按普查年分开（避免>50K限制）
        ("crop_production_2017", {
            "source_desc": "CENSUS", "agg_level_desc": "STATE",
            "statisticcat_desc": "PRODUCTION", "year": "2017",
        }),
        ("crop_production_2022", {
            "source_desc": "CENSUS", "agg_level_desc": "STATE",
            "statisticcat_desc": "PRODUCTION", "year": "2022",
        }),
        # 作物价格（$/单位） — SURVEY, NATIONAL，用于计算产值
        ("price_received", {
            "source_desc": "SURVEY", "agg_level_desc": "NATIONAL",
            "statisticcat_desc": "PRICE RECEIVED",
        }),
    ]

    for name, extra_params in queries:
        rel = f"usda_nass/{name}.json"
        if _exists(rel):
            print(f"  ✓ usda_nass/{name}.json 已存在，跳过")
            continue
        print(f"  正在下载 USDA NASS {name}...")
        params = {
            "key": nass_key,
            "sector_desc": "CROPS",
            "year__GE": "2000",
            "format": "JSON",
            **extra_params,
        }
        try:
            r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                             params=params, timeout=120)
            data = r.json()
            records = data.get("data", [])
            _put_json(rel, records)
            print(f"  ✓ usda_nass/{name}.json  ({len(records)} records)")
        except Exception as e:
            print(f"  ✗ NASS {name}: {e}")
        time.sleep(1)


def fetch_ssurgo(workers: int = 1):
    """USDA NRCS SSURGO 土壤能力等级 → GCS raw_data/ssurgo/state_soil_capability.json

    土壤能力等级 (LCC irrigated, lcc1w):
      Class 1-3: 优质农业土壤（可转蔬菜/果树等高价值作物）
      Class 4+  : 受限土壤（饲料/粮食为主，转型困难）

    用途: 按州调整机会成本的转换率
      good_ratio (class 1-3 灌溉面积占比) → 调整实际可转换耕地比例 (0.15→0.40)
    """
    rel = "ssurgo/state_soil_capability.json"
    if _exists(rel):
        print(f"  ✓ {rel} 已存在，跳过")
        return

    GOOD_CLASSES = {"1", "1e", "1s", "1w",
                    "2", "2e", "2s", "2w",
                    "3", "3e", "3s", "3w"}

    result = {}
    print(f"  查询 SSURGO 灌溉土壤能力等级（{len(STATE_FIPS)} 州）...")

    for fips, abbr in STATE_FIPS.items():
        # Query land capability class for IRRIGATED land (lcc1w) per state
        # Correct join: mapunit.lkey → legend.lkey, filter by areasymbol prefix
        # iccdcd = Irrigated Capability Class dominant condition
        sql = (
            f"SELECT ma.iccdcd AS lcc, SUM(m.muacres) AS acres "
            f"FROM mapunit m "
            f"JOIN legend l ON l.lkey = m.lkey "
            f"JOIN muaggatt ma ON ma.mukey = m.mukey "
            f"WHERE LEFT(l.areasymbol, 2) = '{abbr}' "
            f"  AND ma.iccdcd IS NOT NULL "
            f"  AND m.muacres IS NOT NULL "
            f"GROUP BY ma.iccdcd "
            f"ORDER BY ma.iccdcd"
        )
        try:
            r = requests.post(
                "https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest",
                json={"query": sql, "format": "json+columnname"},
                timeout=90,
            )
            if r.status_code != 200:
                print(f"    {abbr}: HTTP {r.status_code}"); continue

            data = r.json()
            table = data.get("Table", [])
            if len(table) < 2:
                print(f"    {abbr}: empty"); continue

            hdrs = [h.lower() for h in table[0]]
            lcc_i = hdrs.index("lcc") if "lcc" in hdrs else 0
            ac_i  = hdrs.index("acres") if "acres" in hdrs else 1

            by_class = {}
            for row in table[1:]:
                lcc = str(row[lcc_i]).strip().lower()
                try:
                    ac = float(row[ac_i])
                except (ValueError, TypeError):
                    continue
                if lcc:
                    by_class[lcc] = by_class.get(lcc, 0) + round(ac, 0)

            total = sum(by_class.values())
            if total > 0:
                good = sum(v for k, v in by_class.items() if k in GOOD_CLASSES)
                result[abbr] = {
                    "by_class":    by_class,
                    "total_acres": round(total, 0),
                    "good_ratio":  round(good / total, 3),
                }
                print(f"    {abbr}: {len(by_class)} classes  good_ratio={result[abbr]['good_ratio']:.1%}")
            else:
                print(f"    {abbr}: zero acres")

        except Exception as e:
            print(f"    {abbr}: ERROR {e}")
        time.sleep(0.5)

    _put_json(rel, result)
    print(f"  ✓ ssurgo/state_soil_capability.json  ({len(result)} 州有数据)")


def fetch_usda_ers(workers: int = 1):
    """USDA NASS 主要出口作物县级产量（虚拟水出口分析）→ GCS raw_data/usda_ers/"""
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key")
        return

    # 主要出口作物：玉米、大豆、小麦、棉花、苜蓿（虚拟水出口大户）
    EXPORT_CROPS = ["CORN", "SOYBEANS", "WHEAT", "COTTON", "HAY & HAYLAGE", "ALMONDS"]

    # 县级灌溉面积（2022 普查，用于地图可视化）
    county_queries = [
        ("county_irrigated_area_2022", {"year": "2022"}),
    ]

    for name, extra_params in county_queries:
        rel = f"usda_ers/{name}.json"
        if _exists(rel):
            print(f"  ✓ usda_ers/{name}.json 已存在，跳过")
            continue
        print(f"  正在下载 {name}（县级）...")
        params = {
            "key": nass_key,
            "source_desc": "CENSUS",
            "sector_desc": "CROPS",
            "statisticcat_desc": "AREA HARVESTED",
            "prodn_practice_desc": "IN THE OPEN, IRRIGATED",
            "agg_level_desc": "COUNTY",
            "format": "JSON",
            **extra_params,
        }
        try:
            r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                             params=params, timeout=180)
            data = r.json()
            records = data.get("data", [])
            _put_json(rel, records)
            print(f"  ✓ usda_ers/{name}.json  ({len(records)} records)")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
        time.sleep(2)


# ══════════════════════════════════════════════════════════════
# 新数据源：Alpha Vantage 农产品期货
# ══════════════════════════════════════════════════════════════
def fetch_commodity_prices(workers: int = 1):
    """Alpha Vantage 农产品月度价格 → GCS raw_data/commodities/"""
    av_key = os.getenv("AV_API_KEY", "")
    if not av_key:
        print("  ✗ 未找到 AV_API_KEY")
        return

    # 主要灌溉作物对应 ETF/期货代码
    COMMODITIES = {
        "corn":     "CORN",
        "wheat":    "WEAT",
        "soybeans": "SOYB",
        "cotton":   "BAL",
        "rice":     "RICE",
    }

    for name, symbol in COMMODITIES.items():
        rel = f"commodities/{name}_monthly.json"
        if _exists(rel):
            print(f"  ✓ commodities/{name}_monthly.json 已存在，跳过")
            continue
        print(f"  正在下载 {name} ({symbol}) 月度价格...")
        try:
            r = requests.get("https://www.alphavantage.co/query", params={
                "function": "TIME_SERIES_MONTHLY",
                "symbol": symbol,
                "apikey": av_key,
            }, timeout=30)
            data = r.json()
            monthly = data.get("Monthly Time Series", {})
            if monthly:
                records = [{"date": d, "close": float(v["4. close"]), "volume": int(v["5. volume"])}
                           for d, v in sorted(monthly.items(), reverse=True)[:60]]  # 最近5年
                _put_json(rel, {"symbol": symbol, "name": name, "prices": records})
                print(f"  ✓ commodities/{name}_monthly.json  ({len(records)} months)")
            else:
                print(f"  ✗ {name}: {list(data.keys())}")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
        time.sleep(12)  # Alpha Vantage 免费 5次/分


# ══════════════════════════════════════════════════════════════
# 新数据源：USGS 地下水位趋势（Ogallala + 中央谷）
# ══════════════════════════════════════════════════════════════
def fetch_groundwater(workers: int = 1):
    """USGS NWIS 主要含水层地下水位日均值（年度聚合）→ GCS raw_data/groundwater/
    使用 dv endpoint，parameterCd=72019（地下水深度，ft below land surface），
    每个州取最近3年数据，由 build_agri.py 聚合出年均趋势。
    """
    AQUIFER_STATES = {
        "ogallala": ["NE", "KS", "TX", "OK", "CO", "SD", "WY", "NM"],
        "central_valley": ["CA"],
    }

    for aquifer, states in AQUIFER_STATES.items():
        for abbr in states:
            rel = f"groundwater/{aquifer}_{abbr}_dv.json"
            if _exists(rel):
                print(f"  ✓ {rel} 已存在，跳过")
                continue
            print(f"  正在下载 {aquifer}/{abbr} 地下水位（dv）...")
            try:
                r = requests.get("https://waterservices.usgs.gov/nwis/dv/", params={
                    "format": "json",
                    "stateCd": abbr.lower(),
                    "parameterCd": "72019",
                    "siteType": "GW",
                    "startDT": "2015-01-01",
                    "endDT": "2024-12-31",
                    "siteStatus": "all",
                    "statCd": "00003",   # daily mean
                }, timeout=180)
                if r.status_code == 200 and r.text.strip():
                    data = r.json()
                    n = len(data.get("value", {}).get("timeSeries", []))
                    _put_json(rel, data)
                    print(f"  ✓ groundwater/{aquifer}_{abbr}_dv.json  ({n} sites)")
                else:
                    print(f"  ✗ {abbr}: HTTP {r.status_code}")
            except Exception as e:
                print(f"  ✗ groundwater {abbr}: {e}")
            time.sleep(2)


# ══════════════════════════════════════════════════════════════
# 新数据源：NOAA 州级 Palmer 干旱指数（PDSI）
# ══════════════════════════════════════════════════════════════
def fetch_noaa_pdsi(workers: int = 1):
    """NOAA CDO Climate Division PDSI（州级，月度）→ GCS raw_data/noaa_pdsi/
    使用 CLIMDIV 数据集，locationid 格式 CLIMDIV:{ABBR}00（全州平均）。
    """
    noaa_key = os.getenv("NOAA_API_KEY", "")
    if not noaa_key:
        print("  ✗ 未找到 NOAA_API_KEY")
        return

    AGR_STATES = ["CA", "TX", "NE", "KS", "ID", "MT", "CO", "OR", "WA",
                  "AR", "ND", "SD", "MN", "IA", "IL", "IN", "OH", "GA"]

    for abbr in AGR_STATES:
        rel = f"noaa_pdsi/{abbr}_pdsi.json"
        if _exists(rel):
            print(f"  ✓ noaa_pdsi/{abbr}_pdsi.json 已存在，跳过")
            continue
        print(f"  正在下载 {abbr} PDSI (CLIMDIV)...")
        # NOAA Climate Division 使用两位州缩写 + "00" 表示全州汇总
        loc_id = f"CLIMDIV:{abbr}00"
        params = {
            "datasetid": "CLIMDIV",
            "datatypeid": "PDSI",
            "locationid": loc_id,
            "startdate": "2010-01-01",
            "enddate": "2024-12-31",
            "limit": 1000,
            "units": "standard",
            "offset": 1,
        }
        headers = {"token": noaa_key}
        all_results = []
        while True:
            try:
                r = requests.get("https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
                    params=params, headers=headers, timeout=30)
                if r.status_code != 200:
                    print(f"  ✗ {abbr}: HTTP {r.status_code}")
                    break
                data = r.json()
                results = data.get("results", [])
                if not results:
                    break
                all_results.extend(results)
                meta = data.get("metadata", {}).get("resultset", {})
                if params["offset"] + 1000 > meta.get("count", 0):
                    break
                params["offset"] += 1000
                time.sleep(0.3)
            except Exception as e:
                print(f"  ✗ PDSI {abbr}: {e}")
                break
        if all_results:
            _put_json(rel, all_results)
            print(f"  ✓ noaa_pdsi/{abbr}_pdsi.json  ({len(all_results)} records)")
        else:
            print(f"  ✗ {abbr}: no CLIMDIV PDSI data")
        time.sleep(0.5)


# ══════════════════════════════════════════════════════════════
# 新数据源：gridMET 县级气候数据（ETo + 降水）
# ══════════════════════════════════════════════════════════════
def fetch_gridmet(workers: int = 8):
    """gridMET 县级年度 ETo + 降水（2018/2022，用于计算县级作物需水量）
    策略：取每个县 agri_county.geojson 中心点 → 查 THREDDS CSV → 汇总年度值 → GCS
    """
    import io, csv as csvmod
    from collections import defaultdict

    # 加载已有县级数据取中心点
    county_geo_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../../output/data/agri_county.geojson"
    )
    if not os.path.exists(county_geo_path):
        print("  ✗ agri_county.geojson 未找到，请先运行 build_agri.py")
        return

    with open(county_geo_path) as f:
        county_geo = json.load(f)

    # 计算每个县的中心点（简单平均坐标）
    def centroid(coords):
        flat = []
        def _flatten(c):
            if isinstance(c[0], list):
                for x in c: _flatten(x)
            else:
                flat.append(c)
        _flatten(coords)
        lons = [p[0] for p in flat]
        lats = [p[1] for p in flat]
        return sum(lons)/len(lons), sum(lats)/len(lats)

    counties = []
    for feat in county_geo.get("features", []):
        fips = feat.get("id") or feat["properties"].get("fips", "")
        geom = feat["geometry"]
        try:
            lon, lat = centroid(geom["coordinates"])
            counties.append({"fips": fips, "lat": round(lat, 4), "lon": round(lon, 4)})
        except Exception:
            continue

    YEARS = list(range(2022, 2026))
    VARS  = {
        "etr": "agg_met_etr_1979_CurrentYear_CONUS.nc?var=daily_mean_reference_evapotranspiration_alfalfa",
        "pr":  "agg_met_pr_1979_CurrentYear_CONUS.nc?var=precipitation_amount",
    }
    BASE = "https://thredds.northwestknowledge.net:443/thredds/ncss/"
    existing = _list_existing("gridmet/")
    total = len(counties) * len(YEARS)
    prog  = Progress(total, f"{len(counties)} 县 × {len(YEARS)} 年，{workers} 线程")

    def _fetch_county(item):
        c, year = item
        fips = c["fips"]
        rel  = f"gridmet/{fips}_{year}.json"
        if rel in existing:
            prog.tick(skipped=True)
            return
        result = {"fips": fips, "year": year, "lat": c["lat"], "lon": c["lon"]}
        for var_name, var_path in VARS.items():
            url = (f"{BASE}{var_path}"
                   f"&latitude={c['lat']}&longitude={c['lon']}"
                   f"&time_start={year}-01-01&time_end={year}-12-31&accept=csv")
            try:
                r = requests.get(url, timeout=30)
                if r.status_code != 200:
                    result[var_name] = None
                    continue
                lines = r.text.strip().split("\n")
                total_val = 0.0
                count = 0
                for line in lines[1:]:  # skip header
                    parts = line.split(",")
                    if len(parts) >= 4:
                        try:
                            total_val += float(parts[3])
                            count += 1
                        except (ValueError, IndexError):
                            pass
                # gridMET NCSS 返回值需先除以 scale_factor=10，再 mm→inches
                result[var_name] = round(total_val * 0.1 / 25.4, 2) if count > 0 else None
            except Exception:
                result[var_name] = None
        _put_json(rel, result)
        prog.tick()
        time.sleep(0.05)

    items = [(c, yr) for c in counties for yr in YEARS]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_county, items))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 新数据源：USDA NASS 县级作物产量
# ══════════════════════════════════════════════════════════════
def fetch_nass_county_crops(workers: int = 1):
    """USDA NASS 县级主要作物产量（玉米/大豆/小麦）→ GCS raw_data/nass_county/"""
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key")
        return

    queries = [
        ("corn_county",     {"commodity_desc": "CORN",     "statisticcat_desc": "PRODUCTION", "unit_desc": "BU"}),
        ("soybeans_county", {"commodity_desc": "SOYBEANS", "statisticcat_desc": "PRODUCTION", "unit_desc": "BU"}),
        ("wheat_county",    {"commodity_desc": "WHEAT",    "statisticcat_desc": "PRODUCTION", "unit_desc": "BU"}),
        ("cotton_county",   {"commodity_desc": "COTTON",   "statisticcat_desc": "PRODUCTION", "unit_desc": "BU"}),
        ("rice_county",     {"commodity_desc": "RICE",     "statisticcat_desc": "PRODUCTION", "unit_desc": "CWT"}),
        ("hay_county",      {"commodity_desc": "HAY",      "statisticcat_desc": "PRODUCTION", "unit_desc": "TONS"}),
    ]

    for name, extra_params in queries:
        rel = f"nass_county/{name}.json"
        if _exists(rel):
            print(f"  ✓ nass_county/{name}.json 已存在，跳过")
            continue
        print(f"  正在下载 {name}（县级）...")
        params = {
            "key": nass_key,
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "agg_level_desc": "COUNTY",
            "year__GE": "2015",
            "format": "JSON",
            **extra_params,
        }
        try:
            r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                             params=params, timeout=180)
            data = r.json()
            records = data.get("data", [])
            _put_json(rel, records)
            print(f"  ✓ nass_county/{name}.json  ({len(records)} records)")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
        time.sleep(2)


# ══════════════════════════════════════════════════════════════
# 新数据源：BLS 县级年度失业率
# ══════════════════════════════════════════════════════════════
def fetch_bls_unemployment(workers: int = 1):
    """BLS LAUS 县级年度失业率（unemployment rate, M13 annual avg）
    使用 BLS Public API v2，按县 FIPS 批次查询，50个/批
    → GCS raw_data/bls_unemployment/la_county_all.json
    """
    rel = "bls_unemployment/la_county_all.json"
    if _exists(rel):
        existing = json.loads(_get_bucket().blob(f"{GCS_PREFIX}/{rel}").download_as_bytes())
        if existing:
            print(f"  ✓ {rel} 已存在（{len(existing)} 县），跳过")
            return
        print(f"  ⚠ {rel} 为空，重新抓取...")

    # 枚举全部 county FIPS
    all_fips = []
    for state_fips in STATE_FIPS:
        for county in range(1, 200, 2):   # 奇数 county code，美国惯例
            all_fips.append(f"{state_fips}{str(county).zfill(3)}")
        for county in [2, 4, 6, 8, 10, 12, 14, 16, 18, 20,  # 部分偶数（夏威夷/阿拉斯加）
                       510, 520, 530, 540, 550, 560, 570, 580, 590, 600]:
            all_fips.append(f"{state_fips}{str(county).zfill(3)}")

    # BLS series ID: LAUCN{5位FIPS}0000000003 = 失业率
    series_ids = [f"LAUCN{fips}0000000003" for fips in all_fips]

    bls_key = os.getenv("BLS_API_KEY", "")
    headers = {"Content-type": "application/json"}
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    all_data = {}   # fips -> {year: rate}
    batch_size = 50
    batches = [series_ids[i:i+batch_size] for i in range(0, len(series_ids), batch_size)]
    print(f"  BLS API 查询 {len(series_ids)} 个县级序列（{len(batches)} 批次）...")

    for i, batch in enumerate(batches):
        payload = {
            "seriesid": batch,
            "startyear": "2015",
            "endyear": "2024",
            "annualaverage": True,
        }
        if bls_key:
            payload["registrationkey"] = bls_key
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=60)
            data = r.json()
            if data.get("status") != "REQUEST_SUCCEEDED":
                continue
            for series in data.get("Results", {}).get("series", []):
                sid = series["seriesID"]
                fips = sid[6:11]
                for obs in series.get("data", []):
                    if obs.get("period") == "M13":   # M13 = annual average
                        try:
                            all_data.setdefault(fips, {})[int(obs["year"])] = float(obs["value"])
                        except: pass
        except Exception as e:
            pass
        if (i + 1) % 10 == 0:
            print(f"    {i+1}/{len(batches)} 批完成，已获取 {len(all_data)} 县...")
        time.sleep(0.5)

    # 整理成列表保存
    records = []
    for fips, yr_vals in all_data.items():
        if not yr_vals: continue
        rates = list(yr_vals.values())
        records.append({
            "fips": fips,
            "unemployment_avg":    round(float(sum(rates)/len(rates)), 2),
            "unemployment_latest": yr_vals.get(max(yr_vals)),
            "n_years": len(rates),
        })

    if not records:
        print("  ✗ BLS API 未返回有效数据")
        return
    _put_json(rel, records)
    print(f"  ✓ la_county_all.json  ({len(records)} 县)")


# ══════════════════════════════════════════════════════════════
# 新数据源：FEMA 国家风险指数（县级洪水风险）
# ══════════════════════════════════════════════════════════════
def fetch_fema_nri(workers: int = 1):
    """FEMA National Risk Index 县级洪水/干旱/综合风险 → GCS raw_data/fema_nri/"""
    import zipfile, io as _io, csv as _csv
    rel = "fema_nri/county_risk.json"
    if _exists(rel):
        print(f"  ✓ {rel} 已存在，跳过")
        return
    import io as _io, csv as _csv, zipfile as _zipfile
    urls = [
        ("zip", "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_Counties/NRI_Table_Counties.zip"),
        ("csv", "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_Counties/NRI_Table_Counties.csv"),
        ("zip", "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload/NRI_Table_Counties/NRI_Table_Counties.zip"),
    ]
    for fmt, url in urls:
        print(f"  正在下载 FEMA NRI（{fmt.upper()}）: {url[:70]}...")
        try:
            r = requests.get(url, timeout=180, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                print(f"    → HTTP {r.status_code}，跳过")
                continue
            if fmt == "zip":
                with _zipfile.ZipFile(_io.BytesIO(r.content)) as zf:
                    csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
                    if not csv_name:
                        print("    → ZIP 内无 CSV，跳过"); continue
                    with zf.open(csv_name) as f:
                        reader = _csv.DictReader(_io.TextIOWrapper(f, encoding="utf-8-sig"))
                        records = list(reader)
            else:
                reader = _csv.DictReader(_io.StringIO(r.content.decode("utf-8-sig")))
                records = list(reader)
            if records:
                _put_json(rel, records)
                print(f"  ✓ fema_nri/county_risk.json  ({len(records)} 县)")
                return
            print("    → CSV 为空，跳过")
        except Exception as e:
            print(f"    → 失败: {e}")
    print("  ✗ FEMA NRI 所有 URL 均失败")



# ══════════════════════════════════════════════════════════════
# 新数据源：USDA RMA 县级作物保险赔付
# ══════════════════════════════════════════════════════════════
def fetch_rma_insurance(workers: int = 1):
    """USDA RMA Summary of Business 县级作物保险赔付（2015-2024）→ GCS raw_data/rma_insurance/"""
    import zipfile, io as _io
    headers = {"User-Agent": "Mozilla/5.0"}
    for year in range(2015, 2025):
        rel = f"rma_insurance/sob{year}c.txt"
        if _exists(rel):
            print(f"  ✓ {rel} 已存在，跳过")
            continue
        print(f"  正在下载 RMA 县级保险赔付 {year}...")
        # 依次尝试多种 URL 格式
        url_candidates = [
            f"https://www.rma.usda.gov/data/sob/scoy/sob{year}c.zip",
            f"https://www.rma.usda.gov/data/sob/scoy/{year}/sob{year}c.zip",
            f"https://www.rma.usda.gov/data/sob/scoy/sobcov{year}.zip",
            f"https://www.rma.usda.gov/-/media/RMA/Cause-of-Loss/County-Data/sob{year}c.zip",
            f"https://www.rma.usda.gov/data/sob/scoy/sob{year}c.txt",
        ]
        success = False
        for url in url_candidates:
            try:
                r = requests.get(url, timeout=120, headers=headers)
                if r.status_code != 200 or len(r.content) < 1000:
                    continue
                # ZIP 格式
                if url.endswith(".zip"):
                    with zipfile.ZipFile(_io.BytesIO(r.content)) as zf:
                        txt_name = next((n for n in zf.namelist() if n.endswith(".txt")), None)
                        if not txt_name:
                            continue
                        text = zf.read(txt_name).decode("utf-8", errors="replace")
                else:
                    text = r.text
                _put_text(rel, text)
                print(f"  ✓ sob{year}c.txt  ({text.count(chr(10))} 行)  <- {url.split('/')[-1]}")
                success = True
                break
            except Exception:
                continue
        if not success:
            print(f"  ✗ RMA {year}: 所有 URL 均失败")
        time.sleep(1)


# ══════════════════════════════════════════════════════════════
# 新数据源：USDA NASS 县级农场数量/规模/总产值
# ══════════════════════════════════════════════════════════════
def fetch_nass_farm_operations(workers: int = 1):
    """USDA NASS 县级农场数量/平均面积/总销售额（普查年）→ GCS raw_data/nass_farms/"""
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key")
        return

    queries = [
        ("farm_count", {"commodity_desc": "FARM OPERATIONS", "statisticcat_desc": "OPERATIONS", "unit_desc": "OPERATIONS"}),
        ("farm_sales", {"commodity_desc": "COMMODITY TOTALS", "statisticcat_desc": "SALES", "unit_desc": "$"}),
        ("farm_area",  {"commodity_desc": "FARM OPERATIONS", "statisticcat_desc": "AREA OPERATED", "unit_desc": "ACRES / OPERATION"}),
    ]

    states = list(STATE_FIPS.values())
    for name, extra_params in queries:
        rel = f"nass_farms/{name}.json"
        if _exists(rel):
            print(f"  ✓ nass_farms/{name}.json 已存在，跳过")
            continue
        print(f"  正在下载 NASS 县级农场 {name}（{len(states)} 州）...")
        all_records = []
        t0 = time.time()
        for i, abbr in enumerate(states):
            try:
                params = {"key": nass_key, "source_desc": "CENSUS", "state_alpha": abbr,
                          "agg_level_desc": "COUNTY", "year__GE": "2012", "format": "JSON", **extra_params}
                r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                                 params=params, timeout=15)
                all_records.extend(r.json().get("data", []))
            except Exception:
                pass
            # 进度条
            done = i + 1
            pct = done / len(states)
            bar = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
            elapsed = time.time() - t0
            eta = elapsed / pct * (1 - pct) if pct > 0 else 0
            print(f"\r  [{bar}] {done}/{len(states)}  {abbr}  记录:{len(all_records)}  "
                  f"剩余:{int(eta)}s  ", end="", flush=True)
            time.sleep(0.3)
        print()  # 换行
        _put_json(rel, all_records)
        print(f"  ✓ nass_farms/{name}.json  ({len(all_records)} 条)")


# ══════════════════════════════════════════════════════════════
# 新数据源：EIA 州级年度电价（灌溉泵水成本代理）
# ══════════════════════════════════════════════════════════════
def fetch_eia_electricity(workers: int = 1):
    """EIA 州级年度平均电价 → GCS raw_data/eia_electricity/
    有 EIA_API_KEY 时走 API；无 key 时下载公开 Excel。
    免费注册：https://www.eia.gov/opendata/register.php
    """
    eia_key = os.getenv("EIA_API_KEY", "")
    rel_json = "eia_electricity/state_avgprice_annual.json"
    rel_xlsx = "eia_electricity/avgprice_annual.xlsx"

    if _exists(rel_json) or _exists(rel_xlsx):
        print("  ✓ EIA 电价数据已存在，跳过")
        return

    if eia_key:
        print("  正在下载 EIA 州级电价（API）...")
        results = []
        for sector in ["industrial", "commercial"]:
            try:
                r = requests.get("https://api.eia.gov/v2/electricity/retail-sales/data/", params={
                    "api_key": eia_key, "frequency": "annual", "data[0]": "price",
                    "facets[sectorName][]": sector, "start": "2015", "end": "2024", "length": 5000,
                }, timeout=60)
                if r.status_code == 200:
                    items = r.json().get("response", {}).get("data", [])
                    for item in items: item["sector"] = sector
                    results.extend(items)
            except Exception as e:
                print(f"  ✗ EIA {sector}: {e}")
            time.sleep(0.5)
        if results:
            _put_json(rel_json, results)
            print(f"  ✓ {rel_json}  ({len(results)} records)")
    else:
        print("  EIA_API_KEY 未设置，下载公开 Excel（建议注册免费 key）...")
        try:
            r = requests.get("https://www.eia.gov/electricity/data/state/avgprice_annual.xlsx",
                             timeout=60, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                _put_bytes(rel_xlsx, r.content,
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                print(f"  ✓ {rel_xlsx}  ({len(r.content)//1024} KB)")
            else:
                print(f"  ✗ EIA: HTTP {r.status_code}")
        except Exception as e:
            print(f"  ✗ EIA: {e}")


# ══════════════════════════════════════════════════════════════
# 新数据源：NASA MODIS 县级 NDVI 植被指数（ORNL DAAC REST，无需 auth）
# ══════════════════════════════════════════════════════════════
def fetch_modis_ndvi(workers: int = 8):
    """NASA MODIS MOD13A3 月度 NDVI（2016-2025，按县中心点）→ GCS raw_data/modis_ndvi/"""
    county_geo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../output/data/agri_county.geojson")
    if not os.path.exists(county_geo_path):
        print("  ✗ agri_county.geojson 未找到，请先运行 build_agri.py")
        return

    with open(county_geo_path) as f:
        county_geo = json.load(f)

    def centroid(coords):
        flat = []
        def _flatten(c):
            if isinstance(c[0], list):
                for x in c: _flatten(x)
            else:
                flat.append(c)
        _flatten(coords)
        lons = [p[0] for p in flat]; lats = [p[1] for p in flat]
        return sum(lons)/len(lons), sum(lats)/len(lats)

    counties = []
    for feat in county_geo.get("features", []):
        fips = feat.get("id") or feat["properties"].get("fips", "")
        try:
            lon, lat = centroid(feat["geometry"]["coordinates"])
            counties.append({"fips": fips, "lat": round(lat, 4), "lon": round(lon, 4)})
        except Exception:
            continue

    existing = _list_existing("modis_ndvi/")
    prog = Progress(len(counties), f"{len(counties)} 县 MODIS NDVI，{workers} 线程")

    def _fetch_county(c):
        fips = c["fips"]
        rel = f"modis_ndvi/{fips}.json"
        if rel in existing:
            prog.tick(skipped=True); return
        try:
            r = requests.get("https://modis.ornl.gov/rst/api/v1/MOD13A3/subset", params={
                "latitude": c["lat"], "longitude": c["lon"],
                "startDate": "A2016001", "endDate": "A2025365",
                "kmAboveBelow": 0, "kmLeftRight": 0,
            }, timeout=30)
            if r.status_code == 200:
                records = []
                for s in r.json().get("subset", []):
                    if "NDVI" in s.get("band", ""):
                        val = s.get("data", [None])[0]
                        if val is not None and val > -3000:
                            records.append({"date": s.get("calendar_date"), "ndvi": round(val * 0.0001, 4)})
                _put_json(rel, {"fips": fips, "lat": c["lat"], "lon": c["lon"], "ndvi": records})
            else:
                _put_json(rel, {"fips": fips, "ndvi": []})
        except Exception:
            pass
        prog.tick()
        time.sleep(0.3)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_county, counties))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 新数据源：USDA SCAN 网络土壤湿度站点数据
# ══════════════════════════════════════════════════════════════
def fetch_soil_moisture(workers: int = 4):
    """USDA NRCS SCAN 土壤湿度站点日均数据（2016-2025）→ GCS raw_data/soil_moisture/"""
    AGR_STATES = ["CA","TX","NE","KS","ID","MT","CO","OR","WA",
                  "AR","ND","SD","MN","IA","IL","IN","OH","GA","AZ","NM","OK","MO","WI","MI"]
    existing = _list_existing("soil_moisture/")
    prog = Progress(len(AGR_STATES), f"{len(AGR_STATES)} 州 SCAN 土壤湿度")

    def _fetch_state(abbr):
        rel = f"soil_moisture/{abbr}_scan_daily.csv"
        if rel in existing:
            prog.tick(skipped=True); return
        url = (
            "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/"
            "customMultipleStationReport/daily/start_of_period/"
            f"state=%22{abbr}%22%20AND%20network=%22SCAN%22/"
            "2016-01-01,2025-12-31/"
            "SMS:-2:value,SMS:-4:value,SMS:-8:value,PREC::value,TOBS::value"
        )
        try:
            r = requests.get(url, timeout=180)
            if r.status_code == 200 and len(r.content) > 500:
                _put_csv(rel, r.content)
                n_rows = r.content.count(b"\n"); print(f"\n  ✓ soil_moisture/{abbr}  ({n_rows} rows)")
            else:
                print(f"\n  ✗ SCAN {abbr}: HTTP {r.status_code}")
        except Exception as e:
            print(f"\n  ✗ SCAN {abbr}: {e}")
        prog.tick()
        time.sleep(0.5)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_state, AGR_STATES))
    prog.summary()


# ══════════════════════════════════════════════════════════════
# 主程序
# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
# 新数据源：USDA NASS 县级灌溉方式（滴灌/漫灌/喷灌）
# ══════════════════════════════════════════════════════════════
def fetch_nass_irrigation_method(workers: int = 1):
    """USDA NASS Farm Irrigation Survey 县级灌溉方式分布 → GCS raw_data/nass_irrigation/"""
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key")
        return

    # 注：NASS 灌溉方式细分数据仅在州级存在，县级无数据
    # 策略：先抓州级比例，再在 water_efficiency.py 中映射到县
    queries = [
        ("irrigated_area_by_method", {"statisticcat_desc": "AREA IRRIGATED", "domain_desc": "IRRIGATION PRACTICE"}),
        ("water_applied_by_method",  {"statisticcat_desc": "WATER APPLIED",  "domain_desc": "IRRIGATION PRACTICE"}),
    ]
    for name, extra_params in queries:
        rel = f"nass_irrigation/{name}.json"
        if _exists(rel):
            data = json.loads(_get_bucket().blob(f"{GCS_PREFIX}/{rel}").download_as_bytes())
            if data:  # 非空文件才跳过
                print(f"  ✓ nass_irrigation/{name}.json 已存在，跳过")
                continue
            print(f"  ⚠ nass_irrigation/{name}.json 为空，重新抓取（改为州级）...")
        print(f"  正在下载 NASS 灌溉方式 {name}（州级）...")
        params = {"key": nass_key, "source_desc": "CENSUS", "sector_desc": "CROPS",
                  "agg_level_desc": "STATE", "year__GE": "2013", "format": "JSON", **extra_params}
        try:
            r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/", params=params, timeout=180)
            records = r.json().get("data", [])
            if not records:
                print(f"  ✗ nass_irrigation {name}: API 返回 0 条，检查参数")
                continue
            _put_json(rel, records)
            print(f"  ✓ nass_irrigation/{name}.json  ({len(records)} records，州级)")
        except Exception as e:
            print(f"  ✗ nass_irrigation {name}: {e}")
        time.sleep(2)


# ══════════════════════════════════════════════════════════════
# 新数据源：SSURGO 县级土壤持水能力（AWC）
# ══════════════════════════════════════════════════════════════
def fetch_ssurgo_county(workers: int = 16):
    """USDA NRCS SSURGO 县级土壤持水能力 → GCS raw_data/ssurgo_county/"""
    county_geo_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../../output/data/agri_county.geojson"
    )
    if not os.path.exists(county_geo_path):
        print("  ✗ agri_county.geojson 未找到，请先运行 build_agri.py")
        return

    with open(county_geo_path) as f:
        features = json.load(f).get("features", [])

    fips_list = []
    for feat in features:
        fips = feat.get("id") or feat["properties"].get("fips", "")
        state_fips = fips[:2]
        abbr = feat["properties"].get("state") or STATE_FIPS.get(state_fips, "")
        county_name = feat["properties"].get("county", "").strip()
        if abbr and county_name and len(fips) == 5:
            fips_list.append((fips, abbr, county_name))

    existing = _list_existing("ssurgo_county/")
    prog = Progress(len(fips_list), f"{len(fips_list)} 县 SSURGO AWC，{workers} 线程")

    def _fetch_county(item):
        fips, abbr, county_name = item
        rel = f"ssurgo_county/{fips}.json"
        if rel in existing:
            prog.tick(skipped=True)
            return
        # 通过 laoverlap 按县名+州缩写查（areasymbol 不等于 FIPS，不能直接用）
        sql = (
            f"SELECT AVG(ch.awc_r) AS awc_mean, AVG(ch.sandtotal_r) AS sand_pct, "
            f"AVG(ch.claytotal_r) AS clay_pct, AVG(ch.om_r) AS organic_matter "
            f"FROM chorizon ch "
            f"JOIN component co ON co.cokey = ch.cokey "
            f"JOIN mapunit mu ON mu.mukey = co.mukey "
            f"JOIN legend l ON l.lkey = mu.lkey "
            f"JOIN laoverlap la ON la.lkey = l.lkey "
            f"WHERE la.areatypename = 'County or Parish' "
            f"AND la.areaname LIKE '%{county_name}%' "
            f"AND l.areasymbol LIKE '{abbr}%' "
            f"AND ch.hzdepb_r <= 100"
        )
        for attempt in range(3):   # 最多重试 3 次
            try:
                r = requests.post("https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest",
                    json={"query": sql, "format": "json+columnname"}, timeout=60)
                if r.status_code == 200:
                    table = r.json().get("Table", [])
                    if len(table) >= 2:
                        hdrs = [h.lower() for h in table[0]]
                        result = {"fips": fips}
                        for h, v in zip(hdrs, table[1]):
                            try: result[h] = round(float(v), 4) if v is not None else None
                            except (ValueError, TypeError): result[h] = None
                        # 只在有实际数据时保存，失败不保存（下次可重试）
                        if result.get("awc_mean") is not None:
                            _put_json(rel, result)
                    break   # 成功（含空结果），不再重试
                else:
                    time.sleep(2 ** attempt)   # 指数退避
            except Exception:
                time.sleep(2 ** attempt)
        prog.tick()
        time.sleep(0.2)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_county, fips_list))
    prog.summary()


def clear_ssurgo_empty(workers: int = 16):
    """删除 GCS 中 awc_mean 为 null 的 SSURGO 占位 blob，让 ssurgo_county 重新抓取。"""
    print("  扫描 ssurgo_county/ 中无效 blob（并发下载检查）...")
    bucket = _get_bucket()
    blobs = list(bucket.list_blobs(prefix=f"{GCS_PREFIX}/ssurgo_county/"))
    print(f"  共 {len(blobs)} 个 blob")

    to_delete = []
    lock = __import__("threading").Lock()

    def _check(b):
        try:
            d = json.loads(b.download_as_bytes())
            if d.get("awc_mean") is None:
                with lock:
                    to_delete.append(b)
        except Exception:
            with lock:
                to_delete.append(b)

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_check, b) for b in blobs]
        for i, _ in enumerate(as_completed(futs), 1):
            if i % 200 == 0:
                print(f"    检查 {i}/{len(blobs)}...", end="\r", flush=True)
    print()

    if not to_delete:
        print("  ✓ 无需清理，所有 blob 均有有效数据")
        return
    print(f"  删除 {len(to_delete)} 个无效 blob...")
    for b in to_delete:
        b.delete()
    print(f"  ✓ 已清理 {len(to_delete)} 个，请重新运行：python src/build/fetch_all.py ssurgo_county")


# ══════════════════════════════════════════════════════════════
# 新数据源：USDA EQIP 县级节水保护项目参与数据
# ══════════════════════════════════════════════════════════════
def fetch_eqip_conservation(workers: int = 1):
    """USDA NRCS EQIP 节水/灌溉效率保护项目县级资金（2015-2024）→ GCS raw_data/eqip/"""
    rel = "eqip/eqip_county_all.json"
    if _exists(rel):
        print(f"  ✓ {rel} 已存在，跳过")
        return
    print("  正在下载 USDA EQIP 县级数据...")
    # USDA Ag Data Commons - EQIP county-level obligations
    urls_to_try = [
        "https://www.nrcs.usda.gov/resources/data-and-reports/eqip-financial-assistance-obligation",
        "https://api.nal.usda.gov/nalt/search?query=EQIP+county&format=json",
    ]
    for url in urls_to_try:
        try:
            r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                try:
                    data = r.json()
                    if data:
                        _put_json(rel, data)
                        print(f"  ✓ eqip_county_all.json  ({len(data) if isinstance(data, list) else 1} records)")
                        return
                except Exception:
                    pass
        except Exception:
            pass

    # 备用：USDA QuickStats 保护实践（NRCS通过NASS发布的部分数据）
    nass_key = os.getenv("USDA_NASS_API", "")
    if nass_key:
        print("  尝试 NASS 保护实践数据...")
        try:
            r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/", params={
                "key": nass_key, "sector_desc": "ENVIRONMENTAL",
                "agg_level_desc": "COUNTY", "year__GE": "2015", "format": "JSON",
            }, timeout=120)
            records = r.json().get("data", [])
            if records:
                _put_json(rel, records)
                print(f"  ✓ eqip_county_all.json  ({len(records)} records)")
                return
        except Exception as e:
            print(f"  ✗ NASS environmental: {e}")
    print("  ✗ EQIP 县级数据暂无公开 API，需手动下载")
    print("  → 下载地址: https://www.nrcs.usda.gov/resources/data-and-reports/eqip-financial-assistance-obligation")


# ══════════════════════════════════════════════════════════════
# 新数据源：USGS 3DEP 县级海拔高度（地形代理）
# ══════════════════════════════════════════════════════════════
def fetch_elevation(workers: int = 8):
    """USGS 3DEP 县级中心点海拔（英尺）→ GCS raw_data/elevation/
    地形代理变量：海拔影响径流损失和灌溉方式选择
    """
    county_geo_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../../output/data/agri_county.geojson"
    )
    if not os.path.exists(county_geo_path):
        print("  ✗ agri_county.geojson 未找到，请先运行 build_agri.py")
        return

    with open(county_geo_path) as f:
        features = json.load(f).get("features", [])

    def centroid(coords):
        flat = []
        def _flatten(c):
            if isinstance(c[0], list):
                for x in c: _flatten(x)
            else:
                flat.append(c)
        _flatten(coords)
        lons = [p[0] for p in flat]; lats = [p[1] for p in flat]
        return sum(lons)/len(lons), sum(lats)/len(lats)

    counties = []
    for feat in features:
        fips = feat.get("id") or feat["properties"].get("fips", "")
        try:
            lon, lat = centroid(feat["geometry"]["coordinates"])
            counties.append({"fips": fips, "lat": round(lat, 4), "lon": round(lon, 4)})
        except Exception:
            continue

    existing = _list_existing("elevation/")
    # 批量存为单个文件，避免3000+小文件
    rel = "elevation/county_elevation.json"
    if rel in existing:
        print("  ✓ elevation/county_elevation.json 已存在，跳过")
        return

    prog = Progress(len(counties), f"{len(counties)} 县 USGS 3DEP 海拔，{workers} 线程")
    results = {}
    lock = threading.Lock()

    def _fetch_county(c):
        fips = c["fips"]
        try:
            r = requests.get("https://epqs.nationalmap.gov/v1/json", params={
                "x": c["lon"], "y": c["lat"], "units": "Feet", "includeDate": "False"
            }, timeout=15)
            if r.status_code == 200:
                val = r.json().get("value")
                if val is not None:
                    with lock:
                        results[fips] = {"elevation_ft": round(float(val), 1),
                                         "lat": c["lat"], "lon": c["lon"]}
        except Exception:
            pass
        prog.tick()
        time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_fetch_county, counties))

    _put_json(rel, results)
    print(f"  ✓ elevation/county_elevation.json  ({len(results)} 县)")


# ══════════════════════════════════════════════════════════════
# 新数据源：USDA NASS 县级农场主年龄 + 土地租赁比例
# ══════════════════════════════════════════════════════════════
def fetch_nass_operator_demographics(workers: int = 1):
    """USDA NASS 县级农场主年龄分布 + 土地租赁比例（普查年）→ GCS raw_data/nass_operators/
    关键特征：年长农场主采纳节水技术更慢；租地农场主不愿长期投资灌溉设备
    """
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key")
        return

    queries = [
        # 土地所有权（自有 / 部分租赁 / 全租赁）
        ("land_tenure", {
            "sector_desc": "DEMOGRAPHICS",
            "commodity_desc": "FARM OPERATIONS",
            "statisticcat_desc": "AREA OPERATED",
            "domain_desc": "TENURE",
        }),
        # 农场主性别（女性农场主比例）— 按州分批避免超50K限制
        ("operator_sex", {
            "sector_desc": "DEMOGRAPHICS",
            "commodity_desc": "PRODUCERS",
            "domain_desc": "PRODUCERS",
            "statisticcat_desc": "OPERATIONS",
        }),
        # 农场销售规模分布
        ("farm_sales_size", {
            "sector_desc": "DEMOGRAPHICS",
            "commodity_desc": "PRODUCERS",
            "domain_desc": "FARM SALES",
            "statisticcat_desc": "OPERATIONS",
        }),
    ]

    states = list(STATE_FIPS.values())
    for name, extra_params in queries:
        rel = f"nass_operators/{name}.json"
        if _exists(rel):
            print(f"  ✓ nass_operators/{name}.json 已存在，跳过")
            continue
        print(f"  正在下载 NASS 农场主特征 {name}（{len(states)} 州）...")
        all_records = []
        t0 = time.time()
        for i, abbr in enumerate(states):
            params = {
                "key": nass_key,
                "source_desc": "CENSUS",
                "agg_level_desc": "COUNTY",
                "state_alpha": abbr,
                "year__GE": "2017",
                "format": "JSON",
                **extra_params,
            }
            try:
                r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                                 params=params, timeout=15)
                all_records.extend(r.json().get("data", []))
            except Exception:
                pass
            done = i + 1
            pct = done / len(states)
            bar = "█" * int(pct * 20) + "░" * (20 - int(pct * 20))
            elapsed = time.time() - t0
            eta = elapsed / pct * (1 - pct) if pct > 0 else 0
            print(f"\r  [{bar}] {done}/{len(states)}  {abbr}  记录:{len(all_records)}  "
                  f"剩余:{int(eta)}s  ", end="", flush=True)
            time.sleep(0.3)
        print()
        if all_records:
            _put_json(rel, all_records)
            print(f"  ✓ nass_operators/{name}.json  ({len(all_records)} 条)")
        else:
            print(f"  ✗ nass_operators {name}: 无数据")

def fetch_bea_farm_income(workers: int = 1):
    """BEA 区域经济账户：县级农场主净收入（CAEMP25N Line 70）→ GCS raw_data/bea/farm_income.json
    数据来源：IRS 税务整合，不受 NASS (D) 压制限制，县级覆盖率接近 100%。
    免费 API Key：https://apps.bea.gov/api/signup/
    """
    rel = "bea/farm_income.json"
    if _exists(rel):
        print("  ✓ bea/farm_income.json 已存在，跳过")
        return

    bea_key = os.getenv("BEA_API_KEY", "")
    if not bea_key:
        print("  ✗ 未找到 BEA_API_KEY，请在 .env 中设置")
        print("    免费注册：https://apps.bea.gov/api/signup/")
        return

    print("  正在下载 BEA 县级农场主净收入（2022）...")
    params = {
        "UserID":      bea_key,
        "method":      "GetData",
        "datasetname": "Regional",
        "TableName":   "CAINC5N",
        "LineCode":    "55",        # Farm proprietors' income
        "GeoFips":     "COUNTY",
        "Year":        "2022",
        "ResultFormat":"json",
    }
    try:
        r = requests.get("https://apps.bea.gov/api/data/", params=params, timeout=60)
        data = r.json()
        if "BEAAPI" not in data or "Results" not in data["BEAAPI"]:
            print(f"  ✗ BEA API 返回异常: {data.get('BEAAPI',{}).get('Error','未知错误')}")
            return
        records = data["BEAAPI"]["Results"].get("Data", [])
        _put_json(rel, records)
        print(f"  ✓ bea/farm_income.json  ({len(records)} records)")
    except Exception as e:
        print(f"  ✗ BEA 下载失败: {e}")


def fetch_center_pivot(workers: int = 1):
    """GEE Python API：提交中心轴灌溉县级聚合任务，等待完成后数据自动写入 GCS。
    需要已通过 `earthengine authenticate` 完成认证。
    """
    rel = "centerpivot/county_centerpivot_2024.csv"
    if _exists(rel):
        blob = _get_bucket().blob(f"{GCS_PREFIX}/{rel}")
        if (blob.size or 0) > 1000:
            print(f"  ✓ centerpivot/county_centerpivot_2024.csv 已存在")
            return

    try:
        import ee
    except ImportError:
        print("  ✗ 缺少 earthengine-api，请运行：pip install earthengine-api")
        return

    try:
        ee.Initialize(project=GCS_PROJECT)
    except Exception:
        try:
            ee.Authenticate()
            ee.Initialize(project=GCS_PROJECT)
        except Exception as e:
            print(f"  ✗ GEE 认证失败：{e}")
            print("    请先在终端运行：earthengine authenticate")
            return

    print("  正在提交 GEE 中心轴检测任务（2024 CDL，全美县级）...")

    YEAR         = 2024
    PIVOT_RADIUS = 13    # 像素，30m 分辨率下 ≈ 390m（典型中心轴半径）

    counties = ee.FeatureCollection("TIGER/2018/Counties") \
                 .filter(ee.Filter.lte("STATEFP", "56"))

    cdl = ee.ImageCollection("USDA/NASS/CDL") \
            .filter(ee.Filter.calendarRange(YEAR, YEAR, "year")) \
            .first().select("cropland")

    cropland = cdl.gte(1).And(cdl.lte(80)).selfMask()

    # 形态学 opening：保留包含半径 PIVOT_RADIUS 圆形的区域（中心轴特征）
    eroded = cropland.focalMin(PIVOT_RADIUS, "circle", "pixels")
    opened = eroded.focalMax(PIVOT_RADIUS, "circle", "pixels")

    pixel_area = ee.Image.pixelArea().divide(4046.86)  # m² → 英亩

    centerpivot_ac = opened.unmask(0).multiply(pixel_area).rename("centerpivot_ac")
    total_crop_ac  = cropland.unmask(0).multiply(pixel_area).rename("total_crop_ac")

    stats = centerpivot_ac.addBands(total_crop_ac) \
        .reduceRegions(
            collection=counties,
            reducer=ee.Reducer.sum(),
            scale=30,
            crs="EPSG:5070"
        ) \
        .filter(ee.Filter.gt("total_crop_ac", 40)) \
        .map(lambda f: f.set(
            "centerpivot_ratio",
            ee.Number(f.get("centerpivot_ac"))
              .divide(ee.Number(f.get("total_crop_ac")))
              .min(1).multiply(1000).round().divide(1000)
        ).select(["GEOID", "STATEFP", "NAME",
                  "centerpivot_ac", "total_crop_ac", "centerpivot_ratio"]))

    TASK_DESC = f"county_centerpivot_{YEAR}"

    # ── 断点恢复：检查是否有已在运行的同名任务 ──────────────────────────
    task = None
    try:
        for t in ee.batch.Task.list():
            s = t.status()
            if s.get("description") == TASK_DESC and s.get("state") in ("READY", "RUNNING"):
                task = t
                elapsed = int(s.get("start_timestamp_ms", 0))
                print(f"  ↩ 发现已有任务正在运行（{TASK_DESC}），继续等待...")
                break
    except Exception:
        pass

    if task is None:
        task = ee.batch.Export.table.toCloudStorage(
            collection=stats,
            description=TASK_DESC,
            bucket=GCS_BUCKET_NAME,
            fileNamePrefix=f"{GCS_PREFIX}/centerpivot/county_centerpivot_{YEAR}",
            fileFormat="CSV"
        )
        task.start()
        print(f"  任务已提交（{TASK_DESC}），等待 GEE 完成（约 10-30 分钟）...")

    # ── 进度条 ────────────────────────────────────────────────────────────
    import time as _time
    BAR_WIDTH   = 30
    POLL_SEC    = 20
    elapsed_sec = 0
    ESTIMATE    = 1200  # 预计 20 分钟作为 100%

    print()
    while task.active():
        _time.sleep(POLL_SEC)
        elapsed_sec += POLL_SEC
        state    = task.status().get("state", "RUNNING")
        pct      = min(elapsed_sec / ESTIMATE, 0.99)
        filled   = int(BAR_WIDTH * pct)
        bar      = "█" * filled + "░" * (BAR_WIDTH - filled)
        mins, s  = divmod(elapsed_sec, 60)
        print(f"\r  [{bar}] {pct*100:4.1f}%  {mins:02d}:{s:02d}  {state:<10}", end="", flush=True)

    print()  # 换行
    final = task.status().get("state", "")
    if final == "COMPLETED":
        print(f"  ✓ GEE 导出完成（耗时 {elapsed_sec//60}m{elapsed_sec%60}s），数据已写入 GCS")
    else:
        err = task.status().get("error_message", "未知错误")
        print(f"  ✗ GEE 任务失败（{final}）：{err}")


def fetch_fris_irrigated_area(workers: int = 1):
    """
    USDA NASS 灌溉调查（FRIS/IWMS）县级灌溉面积
    数据来源：NASS QuickStats Census，short_desc = 'AG LAND, IRRIGATED - ACRES'
    预计覆盖 1500+ 县，保存至 GCS raw_data/nass_irrigation/fris_irrigated_area.json
    """
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key"); return

    rel = "nass_irrigation/fris_irrigated_area.json"
    if _exists(rel):
        data = json.loads(_get_bucket().blob(f"{GCS_PREFIX}/{rel}").download_as_bytes())
        if data:
            print(f"  ✓ {rel} 已存在（{len(data)} 条），跳过"); return
        print(f"  ⚠ {rel} 为空，重新抓取...")

    base = {
        "key": nass_key,
        "source_desc": "CENSUS",
        "agg_level_desc": "COUNTY",
        "format": "JSON",
    }

    # 按年份优先级：2022 → 2017 → 2012
    for year in ["2022", "2017", "2012"]:
        print(f"  尝试 Census {year}...")
        # 依次尝试不同 short_desc / 参数组合
        attempts = [
            {**base, "year": year,
             "short_desc": "AG LAND, IRRIGATED - ACRES"},
            {**base, "year": year,
             "sector_desc": "ECONOMICS", "commodity_desc": "AG LAND",
             "statisticcat_desc": "AREA", "util_practice_desc": "IRRIGATED"},
            {**base, "year": year,
             "sector_desc": "ECONOMICS", "commodity_desc": "AG LAND",
             "statisticcat_desc": "AREA", "prodn_practice_desc": "IRRIGATED"},
        ]
        for i, params in enumerate(attempts, 1):
            try:
                r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                                 params=params, timeout=180)
                resp = r.json()
                records = resp.get("data", [])
                if records:
                    _put_json(rel, records)
                    print(f"  ✓ fris_irrigated_area.json  {year}年 {len(records)} 条县级记录")
                    return
                err = resp.get("error", [""])[0] if resp.get("error") else ""
                print(f"    组合{i} → 0 条  {err[:80]}")
            except Exception as e:
                print(f"    组合{i} → 请求失败: {e}")
            time.sleep(0.5)

    # ── 兜底：直接下载 Census Quick Stats 公开 CSV ─────────────────────
    print("  API 全部返回 0 条，尝试直接下载公开 CSV...")
    # NASS 提供 2022 Census county-level 数据包（公开下载，无需 API key）
    csv_urls = [
        # 2022 Census of Agriculture county data (全量, ~200MB)
        "https://www.nass.usda.gov/Publications/AgCensus/2022/Full_Report/Volume_1,_Chapter_2_County_Level/st99_2_0001_0001.csv",
        # 2017 Census county data
        "https://www.nass.usda.gov/Publications/AgCensus/2017/Full_Report/Volume_1,_Chapter_2_County_Level/st99_2_0001_0001.csv",
    ]
    for url in csv_urls:
        try:
            print(f"  下载 {url[:70]}...")
            r = requests.get(url, timeout=300, stream=True)
            if r.status_code != 200:
                print(f"    → HTTP {r.status_code}，跳过")
                continue
            import io, csv as csv_mod
            lines = r.content.decode("latin-1").splitlines()
            reader = csv_mod.DictReader(lines)
            records = []
            for row in reader:
                desc = row.get("Short Desc", "").upper()
                if "IRRIGATED" in desc and "AG LAND" in desc and "ACRE" in desc:
                    level = row.get("Agg Level Desc", "").upper()
                    if level == "COUNTY":
                        records.append(row)
            if records:
                _put_json(rel, records)
                print(f"  ✓ 从公开 CSV 提取 {len(records)} 条县级灌溉面积记录")
                return
            print(f"    → 未找到匹配行")
        except Exception as e:
            print(f"    → 下载失败: {e}")

    print("  ✗ 所有方式均失败，irrigated_area_ac 将只有 171 县")


def fetch_nass_ag_land_irrigated(workers: int = 1):
    """USDA NASS Census 2022 县级总灌溉农地面积（AG LAND, IRRIGATED）→ GCS raw_data/nass_irrigation/ag_land_irrigated_2022.json"""
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key"); return

    rel = "nass_irrigation/ag_land_irrigated_2022.json"
    if _exists(rel):
        data = json.loads(_get_bucket().blob(f"{GCS_PREFIX}/{rel}").download_as_bytes())
        if data:
            print(f"  ✓ {rel} 已存在（{len(data)} 条），跳过"); return
        print(f"  ⚠ {rel} 为空，重新抓取...")

    base = {"key": nass_key, "agg_level_desc": "COUNTY", "year": "2022", "format": "JSON"}
    attempts = [
        # AG LAND, IRRIGATED — ECONOMICS sector（官方总灌溉农地，一县一行）
        {"source_desc": "CENSUS", "sector_desc": "ECONOMICS",
         "commodity_desc": "AG LAND", "statisticcat_desc": "AREA",
         "short_desc": "AG LAND, IRRIGATED - ACRES"},
        # 去掉 short_desc 用宽口径
        {"source_desc": "CENSUS", "sector_desc": "ECONOMICS",
         "commodity_desc": "AG LAND", "statisticcat_desc": "AREA",
         "util_practice_desc": "IRRIGATED"},
        # FARMS & LAND & ASSETS sector
        {"source_desc": "CENSUS", "sector_desc": "FARMS & LAND & ASSETS",
         "commodity_desc": "AG LAND", "statisticcat_desc": "AREA",
         "util_practice_desc": "IRRIGATED"},
    ]
    for i, extra in enumerate(attempts, 1):
        params = {**base, **extra}
        print(f"  尝试参数组合 {i}: {extra}")
        try:
            r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                             params=params, timeout=180)
            records = r.json().get("data", [])
            if records:
                _put_json(rel, records)
                print(f"  ✓ ag_land_irrigated_2022.json  ({len(records)} 条县级记录)")
                return
            print(f"    → 0 条  {r.json().get('error', '')}")
        except Exception as e:
            print(f"    → 请求失败: {e}")
        time.sleep(1)
    print("  ✗ 所有组合均返回 0 条")


def fetch_nass_irrigated_area(workers: int = 1):
    """USDA NASS Census 2022 县级灌溉面积 → GCS raw_data/nass_irrigation/county_irrigated_area_2022.json"""
    nass_key = os.getenv("USDA_NASS_API", "")
    if not nass_key:
        print("  ✗ 未找到 USDA_NASS_API key")
        return

    rel = "nass_irrigation/county_irrigated_area_2022.json"
    if _exists(rel):
        data = json.loads(_get_bucket().blob(f"{GCS_PREFIX}/{rel}").download_as_bytes())
        if data:
            print(f"  ✓ {rel} 已存在（{len(data)} 条），跳过")
            return
        print(f"  ⚠ {rel} 为空，重新抓取...")

    base = {
        "key": nass_key,
        "source_desc": "CENSUS",
        "agg_level_desc": "COUNTY",
        "year": "2022",
        "format": "JSON",
    }
    # 依次尝试不同参数组合，取第一个有数据的
    attempts = [
        # 1) AG LAND, IRRIGATED — 总灌溉农地面积，覆盖最广
        {"sector_desc": "ECONOMICS", "commodity_desc": "AG LAND",
         "statisticcat_desc": "AREA", "util_practice_desc": "IRRIGATED"},
        # 2) FIELD CROPS HARVESTED IRRIGATED
        {"sector_desc": "CROPS", "commodity_desc": "FIELD CROPS",
         "statisticcat_desc": "AREA HARVESTED", "prodn_practice_desc": "IRRIGATED"},
        # 3) 去掉 commodity_desc，只限 IRRIGATED
        {"sector_desc": "CROPS",
         "statisticcat_desc": "AREA HARVESTED", "prodn_practice_desc": "IRRIGATED"},
        # 4) 原有 ERS 口径（作为最后备用）
        {"sector_desc": "CROPS",
         "statisticcat_desc": "AREA HARVESTED", "prodn_practice_desc": "IN THE OPEN, IRRIGATED"},
    ]

    for i, extra in enumerate(attempts, 1):
        params = {**base, **extra}
        desc = ", ".join(f"{k}={v}" for k, v in extra.items())
        print(f"  尝试参数组合 {i}: {desc}")
        try:
            r = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                             params=params, timeout=180)
            records = r.json().get("data", [])
            if records:
                _put_json(rel, records)
                print(f"  ✓ county_irrigated_area_2022.json  ({len(records)} 条县级记录)")
                return
            err = r.json().get("error", ["无错误信息"])
            print(f"    → 0 条  {err}")
        except Exception as e:
            print(f"    → 请求失败: {e}")
        time.sleep(1)

    print("  ✗ 所有参数组合均返回 0 条，请登录 https://quickstats.nass.usda.gov 手动验证参数")



SOURCES = {
    "wqp":        ("Water Quality Portal（全国，按州+年）",          fetch_wqp),
    "usgs":       ("USGS 水文数据（全国，按州+年）",                 fetch_usgs),
    "usgs_meas":  ("USGS 实测时间序列（全国，按州+周）",             fetch_usgs_measurements),
    "epa_sdwis":  ("EPA SDWIS 供水系统（全国，按州）",               fetch_epa_sdwis),
    "census":     ("US Census 人口/收入（全国）",                    fetch_census),
    "cdc":        ("CDC PLACES 健康数据（全国，按州）",              fetch_cdc_places),
    "tri":        ("EPA TRI 工业有毒排放（全国，按州）",             fetch_epa_tri),
    "npdes":      ("EPA ECHO NPDES 废水排放（全国，按州）",          fetch_npdes),
    "snotel":     ("NRCS SNOTEL 全美西部积雪量/SWE",                fetch_snotel),
    "water_use":  ("USGS 全美农业/城市用水量",                      fetch_usda_water),
    "drought":    ("US Drought Monitor 全美 + NOAA 各州气温",        fetch_drought),
    "usda_nass":  ("USDA NASS 作物灌溉用水+产值+面积（全美）",      fetch_usda_nass),
    "usda_ers":   ("USDA NASS 县级灌溉数据（主要出口作物/虚拟水）",   fetch_usda_ers),
    "ssurgo":       ("USDA NRCS SSURGO 土壤能力等级（灌溉适宜性/换种约束）", fetch_ssurgo),
    "commodities":  ("Alpha Vantage 农产品月度期货价格（玉米/小麦/大豆/棉花）", fetch_commodity_prices),
    "groundwater":  ("USGS NWIS Ogallala+中央谷地下水位年均趋势",              fetch_groundwater),
    "noaa_pdsi":    ("NOAA CDO 主要农业州 Palmer 干旱指数（月度）",             fetch_noaa_pdsi),
    "gridmet":          ("gridMET 县级年度 ETo+降水（2022-2025，用于需水量计算）",    fetch_gridmet),
    "nass_county":      ("USDA NASS 县级作物产量（玉米/大豆/小麦/棉花，2015+）",      fetch_nass_county_crops),
    "bls_unemployment": ("BLS LAUS 县级年度失业率（2015-2024）",                       fetch_bls_unemployment),
    "fema_nri":         ("FEMA National Risk Index 县级洪水/干旱/综合风险",             fetch_fema_nri),
    "rma_insurance":    ("USDA RMA 县级作物保险赔付（2015-2024）",                        fetch_rma_insurance),
    "nass_farms":       ("USDA NASS 县级农场数量/规模/总销售额（普查年）",                 fetch_nass_farm_operations),
    "eia_electricity":  ("EIA 州级年度电价（灌溉泵水成本代理，需 EIA_API_KEY）",           fetch_eia_electricity),
    "modis_ndvi":       ("NASA MODIS 县级月度 NDVI 植被指数（2016-2025）",                 fetch_modis_ndvi),
    "soil_moisture":      ("USDA SCAN 土壤湿度站点日均数据（2016-2025）",                  fetch_soil_moisture),
    "nass_irrigation":    ("USDA NASS 县级灌溉方式（滴灌/漫灌/喷灌，普查年）",              fetch_nass_irrigation_method),
    "ssurgo_county":      ("USDA NRCS SSURGO 县级土壤持水能力 AWC",                         fetch_ssurgo_county),
    "ssurgo_clear":       ("清理 GCS 中 awc_mean 为 null 的 SSURGO 占位文件",               clear_ssurgo_empty),
    "eqip_conservation":    ("USDA NRCS EQIP 县级节水保护项目参与数据",                      fetch_eqip_conservation),
    "elevation":            ("USGS 3DEP 县级中心点海拔高度（地形代理变量）",                   fetch_elevation),
    "nass_operators":       ("USDA NASS 县级农场主年龄分布+土地租赁比例（普查年）",            fetch_nass_operator_demographics),
    "bea_farm_income":      ("BEA 县级农场主净收入（CAEMP25N，需 BEA_API_KEY）",               fetch_bea_farm_income),
    "center_pivot":         ("GEE Python API：2024 CDL 形态学检测县级中心轴喷灌比例",            fetch_center_pivot),
    "nass_irrigated_area":  ("USDA NASS Census 2022 县级灌溉收获面积（FIELD CROPS, IRRIGATED）",  fetch_nass_irrigated_area),
    "nass_ag_land_irr":     ("USDA NASS Census 2022 县级总灌溉农地面积（AG LAND, IRRIGATED）",     fetch_nass_ag_land_irrigated),
    "fris":                 ("USDA NASS FRIS 县级灌溉面积（AG LAND IRRIGATED，API+公开CSV兜底）",  fetch_fris_irrigated_area),
}

DEFAULT_ORDER = [
    "wqp", "usgs", "usgs_meas", "epa_sdwis",
    "census", "cdc", "tri", "npdes",
    "snotel", "water_use", "drought",
    "usda_nass", "usda_ers", "ssurgo",
    "commodities", "groundwater", "noaa_pdsi",
]

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--list" in args or "-l" in args:
        print("\n可用数据源：")
        for key, (desc, _) in SOURCES.items():
            print(f"  {key:<12} {desc}")
        print("\n用法：")
        print("  python src/build/fetch_all.py              # 运行全部")
        print("  python src/build/fetch_all.py census noaa  # 只运行指定源")
        print("  python src/build/fetch_all.py --workers 8  # 指定并行线程数")
        print("  python src/build/fetch_all.py --status     # 查看GCS进度")
        sys.exit(0)

    # 解析 --workers N
    workers = WORKERS
    if "--workers" in args:
        idx = args.index("--workers")
        if idx + 1 < len(args):
            try:
                workers = int(args[idx + 1])
                args = [a for i, a in enumerate(args) if i not in (idx, idx + 1)]
            except ValueError:
                pass

    if "--status" in args:
        remaining = [a for a in args if a != "--status"]
        show_status(remaining if remaining else DEFAULT_ORDER)
        sys.exit(0)

    targets = [a for a in args if not a.startswith("--")] if args else DEFAULT_ORDER
    invalid = [t for t in targets if t not in SOURCES]
    if invalid:
        print(f"未知数据源：{invalid}，运行 --list 查看可用选项")
        sys.exit(1)

    print("=" * 65)
    print(f"  US National Water Quality — 直接写入 GCS [{GCS_BUCKET_NAME}]")
    print(f"  目标：{', '.join(targets)}")
    print(f"  并行线程数：{workers}")
    print(f"  提示：Ctrl+C 可中断，重新运行自动跳过已上传文件")
    print("=" * 65)

    t0 = time.time()
    for key in targets:
        desc, fn = SOURCES[key]
        print(f"\n{'─' * 65}")
        print(f"  【{key.upper()}】{desc}")
        print("─" * 65)
        try:
            fn(workers=workers)
        except KeyboardInterrupt:
            print("\n\n  ⚠ 用户中断。已保存进度，重新运行可断点续跑。")
            sys.exit(0)
        except Exception as e:
            print(f"\n  ✗ 出错：{e}")
        time.sleep(1)

    elapsed = int(time.time() - t0)
    print(f"\n{'=' * 65}")
    print(f"  全部完成！耗时 {elapsed // 60}分{elapsed % 60}秒")
    print(f"  数据已存储在 gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}/")
    print("=" * 65)

