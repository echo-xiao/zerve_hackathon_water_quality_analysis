#!/usr/bin/env python3
"""
Build AQS air quality data mapped to LA County ZCTA (zipcode) boundaries.
Output:
  - aqs_zcta_geo.geojson  : ZCTA polygons with id + assigned station (~2MB)
  - aqs_zcta_data.json    : {dates, aqi: {date: {zcta: aqi}}} (~500KB)
In the map: load geometry once, use setFeatureState() to update colors per date.
"""

import json, math, os, urllib.request, urllib.parse
import geopandas as gpd
from shapely.geometry import mapping

# ── Paths ──────────────────────────────────────────────────────────────
ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR   = os.path.join(ROOT, 'data', 'raw_data')
OUT_DIR    = os.path.join(ROOT, 'output', 'data')
ZCTA_CACHE = os.path.join(DATA_DIR, 'census', 'la_zcta_boundaries.geojson')

# ── 1. Load AQS daily PM2.5 data ──────────────────────────────────────
print("Loading AQS data...")
with open(os.path.join(DATA_DIR, 'aqs', 'wildfire_period_aqi.json')) as f:
    raw = json.load(f)

pm_records  = raw['PM2.5']
site_dates  = {}   # {site_name: {date: aqi}}
site_coords = {}   # {site_name: (lon, lat)}

for r in pm_records:
    site = r['local_site_name']
    date = r['date_local']
    aqi  = r.get('aqi')
    if aqi is None:
        continue
    if site not in site_dates:
        site_dates[site]  = {}
        site_coords[site] = (r['longitude'], r['latitude'])
    site_dates[site][date] = aqi

all_dates = sorted({d for s in site_dates.values() for d in s})
print(f"  {len(site_coords)} stations, {len(all_dates)} dates")

# ── 2. Load ZCTA list ─────────────────────────────────────────────────
with open(os.path.join(DATA_DIR, 'census', 'la_zcta_income.json')) as f:
    zcta_list = [r['zcta'] for r in json.load(f)]
print(f"  {len(zcta_list)} LA County ZCTAs")

# ── 3. Download ZCTA boundaries (cached) ──────────────────────────────
if not os.path.exists(ZCTA_CACHE):
    print("Downloading ZCTA boundaries from Census TIGER...")
    base = ("https://tigerweb.geo.census.gov/arcgis/rest/services/"
            "TIGERweb/PUMA_TAD_TAZ_UGA_ZCTA/MapServer/1/query")
    all_features = []
    batch = 50
    for i in range(0, len(zcta_list), batch):
        chunk = zcta_list[i:i+batch]
        where = "GEOID IN ('" + "','".join(chunk) + "')"
        url = base + '?' + urllib.parse.urlencode({
            'where': where, 'outFields': 'GEOID,ZCTA5',
            'f': 'geojson', 'outSR': '4326'
        })
        with urllib.request.urlopen(url, timeout=30) as resp:
            feats = json.loads(resp.read()).get('features', [])
            all_features.extend(feats)
            print(f"  Batch {i//batch+1}: {len(feats)} ZCTAs")
    with open(ZCTA_CACHE, 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': all_features}, f)
    print(f"  Saved {len(all_features)} boundaries")
else:
    print("Using cached ZCTA boundaries")

# ── 4. Load and project GeoDataFrame ──────────────────────────────────
gdf = gpd.read_file(ZCTA_CACHE).to_crs('EPSG:4326')
zcta_col = next((c for c in ['GEOID', 'ZCTA5', 'ZCTA5CE20'] if c in gdf.columns), None)
print(f"  {len(gdf)} ZCTAs loaded, using column '{zcta_col}'")

# Project to UTM zone 11N for accurate centroids
gdf_utm = gdf.to_crs('EPSG:32611')
gdf['centroid_x'] = gdf_utm.geometry.centroid.x
gdf['centroid_y'] = gdf_utm.geometry.centroid.y

# ── 5. Nearest-station assignment ─────────────────────────────────────
def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))

# Convert station coords to UTM for comparison (or just use haversine on lon/lat centroids)
# We'll use WGS84 centroid for distance (accurate enough for this purpose)
gdf_wgs = gdf.to_crs('EPSG:4326')
gdf['cx'] = gdf_wgs.geometry.centroid.x
gdf['cy'] = gdf_wgs.geometry.centroid.y

zcta_station = {}
for _, row in gdf.iterrows():
    best = min(site_coords, key=lambda s: haversine(row['cx'], row['cy'], *site_coords[s]))
    zcta_station[str(row[zcta_col])] = best

from collections import Counter
print("  ZCTAs per station:")
for site, cnt in Counter(zcta_station.values()).most_common():
    print(f"    {site}: {cnt}")

# ── 6. Build geometry-only GeoJSON (with zcta + station in properties) ─
print("Building geometry GeoJSON...")
geo_features = []
for i, (_, row) in enumerate(gdf.iterrows()):
    zcta = str(row[zcta_col])
    site = zcta_station.get(zcta, '')
    geo_features.append({
        'type': 'Feature',
        'id': i,                   # numeric id for setFeatureState
        'geometry': mapping(row.geometry),
        'properties': {'zcta': zcta, 'site': site},
    })

geo_out = {'type': 'FeatureCollection', 'features': geo_features}
geo_path = os.path.join(OUT_DIR, 'aqs_zcta_geo.geojson')
with open(geo_path, 'w') as f:
    json.dump(geo_out, f, separators=(',', ':'))
print(f"  Saved: {geo_path} ({os.path.getsize(geo_path)/1e6:.1f} MB)")

# ── 7. Forward-fill each station's AQI across all dates ───────────────
# Stations don't measure every day; carry last known value forward.
print("Forward-filling station AQI...")
station_filled = {}  # {site: {date: aqi}} with gaps filled
for site, daily in site_dates.items():
    filled = {}
    last = 0
    for date in all_dates:
        if date in daily:
            last = daily[date]
        filled[date] = last      # 0 until first measurement, then carried forward
    station_filled[site] = filled
    measured = sum(1 for d in all_dates if d in daily)
    print(f"  {site}: {measured} measured days → {len(all_dates)} filled")

# ── 8. Build AQI lookup: {date: {zcta: aqi}} ──────────────────────────
print("Building AQI lookup table...")
aqi_by_date = {}
max_aqi = 0
for date in all_dates:
    day = {}
    for zcta, site in zcta_station.items():
        aqi = station_filled[site][date]
        if aqi:
            day[zcta] = aqi
            max_aqi = max(max_aqi, aqi)
    aqi_by_date[date] = day

# Also build zcta→id mapping
zcta_id = {str(row[zcta_col]): i for i, (_, row) in enumerate(gdf.iterrows())}

data_out = {
    'dates': all_dates,
    'max_aqi': max_aqi,
    'zcta_id': zcta_id,
    'station_coords': {site: list(coords) for site, coords in site_coords.items()},
    'aqi': aqi_by_date,
}
data_path = os.path.join(OUT_DIR, 'aqs_zcta_data.json')
with open(data_path, 'w') as f:
    json.dump(data_out, f, separators=(',', ':'))
print(f"  Saved: {data_path} ({os.path.getsize(data_path)/1e6:.1f} MB)")

print("\nDone!")
