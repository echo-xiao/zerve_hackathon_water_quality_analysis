"""
US National Water Quality & Resource Data Collector
直接写入 Google Cloud Storage，无需本地存储

用法：
  python src/build/fetch_all.py                 # 运行所有数据源
  python src/build/fetch_all.py wqp census      # 只运行指定数据源
  python src/build/fetch_all.py --list          # 列出所有可用数据源
  python src/build/fetch_all.py --status        # 查看当前抓取进度
  python src/build/fetch_all.py --workers 8     # 指定并行线程数（默认4）

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
WORKERS      = 4


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

    # 2. NOAA CDO 加州月均气温（Sacramento + LA）
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
# 主程序
# ══════════════════════════════════════════════════════════════
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
    "ssurgo":     ("USDA NRCS SSURGO 土壤能力等级（灌溉适宜性/换种约束）", fetch_ssurgo),
}

DEFAULT_ORDER = [
    "wqp", "usgs", "usgs_meas", "epa_sdwis",
    "census", "cdc", "tri", "npdes",
    "snotel", "water_use", "drought",
    "usda_nass", "usda_ers", "ssurgo",
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
