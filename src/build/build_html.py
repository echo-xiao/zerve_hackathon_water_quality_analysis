#!/usr/bin/env python3
"""build_html.py — 生成完整农业用水效率分析地图"""

import json, os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../.env"))

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "../../output/data")
OUT  = os.path.join(BASE, "../../output/water_quality_map.html")

def load(fname):
    with open(os.path.join(DATA, fname)) as f:
        return json.load(f)

def jsdump(obj):
    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False)

def main():
    print("=== build_html.py ===")
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if not gemini_key:
        print("  ⚠ GEMINI_API_KEY 未设置，AI 分析按钮将不可用")

    geo     = load("agri_state.geojson")
    crops   = load("agri_crops.json")
    summary = load("agri_summary.json")
    pareto  = load("agri_pareto.json")
    county  = load("agri_county.geojson")

    geo_js     = jsdump(geo)
    crops_js   = jsdump(crops)
    summary_js = jsdump(summary)
    pareto_js  = jsdump(pareto)
    county_js  = jsdump(county)

    html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>美国农业用水效率分析</title>
<link href="https://unpkg.com/maplibre-gl@4/dist/maplibre-gl.css" rel="stylesheet">
<script src="https://unpkg.com/maplibre-gl@4/dist/maplibre-gl.js"></script>
<style>
:root{{
  --sans:Arial,sans-serif;--pal:'Palatino Linotype',Palatino,serif;
  --cream:#f2ede4;--dark:#1a1a1a;--line:#e0dbd4;
  --green:#3a8c5a;--gold:#c4870a;--blue:#2171b5;--purple:#6a3d9a;--red:#c0392b;
}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:var(--pal);background:var(--cream);overflow:hidden;}}
#map{{position:absolute;inset:0;}}
#title{{
  position:absolute;top:12px;left:50%;transform:translateX(-50%);
  z-index:20;text-align:center;white-space:nowrap;pointer-events:none;
}}
#title h1{{font-size:16px;color:#1a1a1a;font-weight:bold;text-shadow:0 1px 4px rgba(242,237,228,.95);}}
#title p{{font-family:var(--sans);font-size:8.5px;color:#666;margin-top:2px;text-shadow:0 1px 3px rgba(242,237,228,.9);letter-spacing:.5px;}}
/* ── Left panel ── */
#left{{
  position:absolute;top:12px;left:12px;z-index:21;width:196px;
  background:#fff;border:1px solid var(--line);border-radius:4px;
  box-shadow:0 4px 18px rgba(0,0,0,.12);overflow:hidden;
}}
.lhdr{{background:var(--dark);color:#fff;padding:7px 12px;
  font-family:var(--sans);font-size:9px;letter-spacing:2px;text-transform:uppercase;}}
#mode-list{{border-bottom:1px solid #f0f0f0;}}
.mrow{{
  display:flex;align-items:center;gap:8px;padding:7px 11px;
  cursor:pointer;border-left:3px solid transparent;
  font-family:var(--sans);font-size:10.5px;color:#555;transition:all .15s;
}}
.mrow:hover{{background:#faf8f4;}}
.mrow.active{{background:#f3fbf6;color:var(--dark);font-weight:700;}}
.mrow.active[data-mode=overview]   {{border-left-color:var(--green);}}
.mrow.active[data-mode=value]      {{border-left-color:var(--gold);background:#fdf8f0;}}
.mrow.active[data-mode=trend]      {{border-left-color:var(--blue);background:#f0f6ff;}}
.mrow.active[data-mode=opp]        {{border-left-color:var(--purple);background:#f7f0ff;}}
.mrow.active[data-mode=vuln]       {{border-left-color:#d73027;background:#fff5f2;}}
.mrow.active[data-mode=diversity]  {{border-left-color:#fc8d59;background:#fff8f0;}}
.mrow.active[data-mode=climate]    {{border-left-color:#2c7bb6;background:#f0f6ff;}}
.mrow.active[data-mode=eff]        {{border-left-color:#2ca25f;background:#f0fbf4;}}
.mrow.active[data-mode=vw]         {{border-left-color:#08519c;background:#f0f4ff;}}
.mrow.active[data-mode=wps]        {{border-left-color:#d73027;background:#fff5f2;}}
.mrow.active[data-mode=county]    {{border-left-color:#e67e22;background:#fff8f0;}}
#county-sub{{display:none;padding:4px 11px 5px;border-bottom:1px solid #f0f0f0;gap:3px;flex-wrap:wrap;}}
.csub-btn{{font-family:var(--sans);font-size:8px;padding:2px 8px;border:1px solid #e0e0e0;
  background:#f8f8f8;color:#888;cursor:pointer;border-radius:3px;transition:all .12s;}}
.csub-btn.active{{background:#e67e22;color:#fff;border-color:#e67e22;}}
/* ── Price slider ── */
#price-ctrl{{border-top:1px solid #f0f0f0;padding:5px 12px 7px;}}
#price-row{{display:flex;align-items:center;gap:6px;margin-bottom:3px;}}
#price-row label{{font-family:var(--sans);font-size:8.5px;color:#666;white-space:nowrap;}}
#price-val{{font-family:var(--sans);font-size:8.5px;font-weight:700;color:var(--dark);min-width:40px;text-align:right;}}
#price-slider{{flex:1;accent-color:#d73027;cursor:pointer;}}
#price-note{{font-family:var(--sans);font-size:7px;color:#aaa;line-height:1.4;}}
/* ── State info panel (right side) ── */
#state-panel{{
  position:absolute;top:12px;right:12px;z-index:21;width:278px;
  background:#fff;border:1px solid var(--line);border-radius:4px;
  box-shadow:0 4px 18px rgba(0,0,0,.12);
  display:none;flex-direction:column;max-height:calc(100vh - 90px);
}}
#sp-hdr{{
  background:var(--dark);color:#fff;padding:7px 10px 7px 12px;
  font-family:var(--sans);font-size:9px;letter-spacing:2px;text-transform:uppercase;
  display:flex;align-items:center;justify-content:space-between;flex-shrink:0;
}}
#sp-close{{background:none;border:none;color:#aaa;cursor:pointer;font-size:16px;line-height:1;padding:0;}}
#sp-close:hover{{color:#fff;}}
#sp-body{{overflow-y:auto;flex:1;min-height:0;padding:10px 12px 12px;}}
.sp-nm{{font-size:15px;font-weight:bold;margin-bottom:2px;}}
.sp-sub{{font-family:var(--sans);font-size:8px;color:#aaa;margin-bottom:8px;}}
.sp-grid{{display:grid;grid-template-columns:1fr 1fr;gap:4px;margin-bottom:8px;}}
.sp-cell{{background:#f8f7f4;border-radius:3px;padding:4px 7px;}}
.sp-lbl{{font-family:var(--sans);font-size:7px;color:#aaa;text-transform:uppercase;letter-spacing:.5px;}}
.sp-val{{font-size:13px;font-weight:bold;color:var(--dark);margin-top:1px;line-height:1.1;}}
.sp-unit{{font-family:var(--sans);font-size:7px;color:#bbb;}}
.sp-bars{{display:flex;gap:3px;align-items:flex-end;height:36px;margin:3px 0 6px;}}
.sp-bcol{{display:flex;flex-direction:column;align-items:center;flex:1;}}
.sp-bpair{{display:flex;gap:1px;align-items:flex-end;}}
.sp-birr{{border-radius:2px 2px 0 0;width:9px;}}
.sp-bprc{{border-radius:2px 2px 0 0;width:6px;opacity:.7;}}
.sp-byr{{font-family:var(--sans);font-size:7px;color:#ccc;margin-top:1px;}}
.sp-crops{{max-height:110px;overflow-y:auto;margin-bottom:6px;}}
.sp-crow{{display:flex;align-items:center;gap:4px;padding:2px 0;border-bottom:1px solid #f5f5f5;}}
.sp-cdot{{width:6px;height:6px;border-radius:50%;flex-shrink:0;}}
.sp-cnm{{flex:1;font-family:var(--sans);font-size:9px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
.sp-cwi{{font-family:var(--sans);font-size:8px;color:#999;flex-shrink:0;min-width:28px;text-align:right;}}
.sp-cdpg{{font-family:var(--sans);font-size:8px;color:#c4870a;flex-shrink:0;min-width:28px;text-align:right;}}
.sp-ai{{border-top:1px solid #f0f0f0;padding-top:7px;margin-top:4px;}}
.sp-ai-btn{{font-family:var(--sans);font-size:9px;padding:4px 10px;border-radius:3px;cursor:pointer;border:1px solid #d0d0d0;background:#f8f8f8;color:#555;transition:all .15s;}}
.sp-ai-btn:hover{{background:#1a1a1a;color:#fff;border-color:#1a1a1a;}}
.sp-ai-btn:disabled{{opacity:.5;cursor:default;}}
.sp-ai-load{{font-family:var(--sans);font-size:9px;color:#aaa;margin-top:5px;display:none;}}
.sp-ai-text{{font-size:10.5px;line-height:1.7;color:#444;margin-top:7px;display:none;}}
.sp-ai-text h2,.sp-ai-text h3,.sp-ai-text h4{{font-family:var(--sans);font-weight:600;color:#222;margin:8px 0 3px;}}
.sp-ai-text h2{{font-size:12px;}}.sp-ai-text h3{{font-size:11.5px;}}.sp-ai-text h4{{font-size:11px;}}
.sp-ai-text p{{margin:4px 0;}}
.sp-ai-text ul{{margin:4px 0 4px 14px;padding:0;list-style:disc;}}
.sp-ai-text li{{margin:2px 0;}}
.sp-ai-text strong{{color:#1a1a1a;font-weight:600;}}
.sp-ai-text em{{color:#555;font-style:italic;}}
/* ── Enhanced trend sparkline ── */
#trend-wrap{{margin:6px 0 8px;}}
#trend-label{{font-family:var(--sans);font-size:7.5px;color:#aaa;margin-bottom:3px;}}
#trend-bar{{display:flex;gap:3px;align-items:flex-end;height:40px;}}
.tb-col{{display:flex;flex-direction:column;align-items:center;gap:1px;flex:1;}}
.tb-yr{{font-family:var(--sans);font-size:7px;color:#ccc;}}
.tb-pair{{display:flex;gap:1px;align-items:flex-end;width:100%;justify-content:center;}}
.tb-irr{{border-radius:2px 2px 0 0;width:10px;}}
.tb-prc{{border-radius:2px 2px 0 0;width:6px;opacity:.7;}}
#trend-legend{{display:flex;gap:8px;margin-top:3px;}}
.tl-item{{display:flex;align-items:center;gap:3px;font-family:var(--sans);font-size:7px;color:#aaa;}}
.tl-swatch{{width:8px;height:6px;border-radius:1px;}}
.mrow-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0;}}
#grad-wrap{{padding:7px 12px 5px;border-bottom:1px solid #f0f0f0;}}
#lgd-grad{{height:7px;border-radius:3px;margin:3px 0;}}
.lgd-row{{display:flex;justify-content:space-between;font-family:var(--sans);font-size:7.5px;color:#aaa;}}
#list-hdr{{padding:6px 12px 2px;font-family:var(--sans);font-size:8.5px;
  font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#888;}}
#ovr-list{{padding:2px 12px 8px;max-height:38vh;overflow-y:auto;}}
.pl-row{{display:flex;align-items:center;gap:4px;margin-bottom:4px;}}
.pl-rk{{font-size:8px;color:#ddd;width:12px;text-align:right;flex-shrink:0;}}
.pl-dot{{width:6px;height:6px;border-radius:50%;flex-shrink:0;}}
.pl-nm{{flex:1;font-family:var(--sans);font-size:9.5px;color:#333;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.pl-bw{{width:44px;height:4px;background:#f0f0f0;border-radius:2px;flex-shrink:0;overflow:hidden;}}
.pl-b{{height:100%;border-radius:2px;}}
.pl-v{{font-family:var(--sans);font-size:8.5px;font-weight:700;color:#555;
  min-width:30px;text-align:right;flex-shrink:0;}}
/* ── Right crop panel ── */
#right{{
  position:absolute;top:12px;right:12px;z-index:21;width:170px;
  background:#fff;border:1px solid var(--line);border-radius:4px;
  box-shadow:0 4px 18px rgba(0,0,0,.12);overflow:hidden;
  display:none;flex-direction:column;max-height:calc(100vh - 90px);
}}
#right .rhdr{{background:var(--dark);color:#fff;padding:7px 12px;
  font-family:var(--sans);font-size:9px;letter-spacing:2px;text-transform:uppercase;flex-shrink:0;}}
#right .rhdr-sub{{font-family:var(--sans);font-size:7.5px;color:#aaa;
  padding:3px 12px 5px;border-bottom:1px solid #f0f0f0;flex-shrink:0;}}
#right .clear-btn{{
  font-family:var(--sans);font-size:8px;padding:3px 12px 4px;color:#888;
  border:none;background:none;cursor:pointer;text-align:left;
  border-bottom:1px solid #f0f0f0;width:100%;flex-shrink:0;
}}
#right .clear-btn:hover{{color:#d73027;}}
#crop-list{{overflow-y:auto;flex:1;padding:3px 0;}}
.crop-btn{{
  display:flex;align-items:center;gap:6px;width:100%;
  padding:5px 10px;border:none;background:none;cursor:pointer;
  text-align:left;border-left:3px solid transparent;
  font-family:var(--sans);font-size:9.5px;color:#444;transition:all .12s;
}}
.crop-btn:hover{{background:#faf8f4;}}
.crop-btn.active{{border-left-color:var(--green);background:#f3fbf6;color:var(--dark);font-weight:700;}}
.crop-btn .cb-swatch{{width:7px;height:7px;border-radius:50%;flex-shrink:0;}}
.crop-btn .cb-vals{{margin-left:auto;text-align:right;flex-shrink:0;}}
.crop-btn .cb-wi{{font-size:7.5px;color:#aaa;display:block;}}
.crop-btn .cb-dpg{{font-size:7.5px;color:var(--gold);display:block;}}
.maplibregl-ctrl-top-left{{top:52px !important;}}
.maplibregl-ctrl-bottom-left{{bottom:14px !important;}}
</style>
</head>
<body>
<div id="map"></div>
<div id="title">
  <h1>美国农业用水效率分析</h1>
  <p>USDA NASS 农业灌溉普查 · 灌溉强度 / 每加仑产值 / 效率趋势 / 重配潜力 · 点击地图查看洞察</p>
</div>

<!-- Left panel -->
<div id="left">
  <div class="lhdr">视图模式</div>
  <div id="mode-list">
    <div class="mrow active" data-mode="overview" onclick="setMode('overview')">
      <div class="mrow-dot" style="background:var(--green)"></div>灌溉强度
    </div>
    <div class="mrow" data-mode="value" onclick="setMode('value')">
      <div class="mrow-dot" style="background:var(--gold)"></div>每加仑产值
    </div>
    <div class="mrow" data-mode="trend" onclick="setMode('trend')">
      <div class="mrow-dot" style="background:var(--blue)"></div>效率趋势
    </div>
    <div class="mrow" data-mode="opp" onclick="setMode('opp')">
      <div class="mrow-dot" style="background:var(--purple)"></div>重配潜力
    </div>
    <div class="mrow" data-mode="vuln" onclick="setMode('vuln')">
      <div class="mrow-dot" style="background:#d73027"></div>干旱脆弱性
    </div>
    <div class="mrow" data-mode="diversity" onclick="setMode('diversity')">
      <div class="mrow-dot" style="background:#fc8d59"></div>作物集中风险
    </div>
    <div class="mrow" data-mode="climate" onclick="setMode('climate')">
      <div class="mrow-dot" style="background:#2c7bb6"></div>旱情趋势
    </div>
    <div class="mrow" data-mode="eff" onclick="setMode('eff')">
      <div class="mrow-dot" style="background:#2ca25f"></div>农业用水效益
    </div>
    <div class="mrow" data-mode="vw" onclick="setMode('vw')">
      <div class="mrow-dot" style="background:#08519c"></div>虚拟水出口
    </div>
    <div class="mrow" data-mode="wps" onclick="setMode('wps')">
      <div class="mrow-dot" style="background:#d73027"></div>水价敏感分析
    </div>
    <div class="mrow" data-mode="county" onclick="setMode('county')">
      <div class="mrow-dot" style="background:#e67e22"></div>县级分布
    </div>
  </div>
  <div id="county-sub">
    <button class="csub-btn active" data-csub="water" onclick="setCountySub('water')">耗水量</button>
    <button class="csub-btn" data-csub="opp" onclick="setCountySub('opp')">转换机会</button>
    <button class="csub-btn" data-csub="crop" onclick="setCountySub('crop')">主导作物</button>
  </div>
  <div id="grad-wrap">
    <div id="lgd-grad"></div>
    <div class="lgd-row"><span id="lgd-lo">低</span><span id="lgd-hi">高（af/ac）</span></div>
  </div>
  <!-- Year selector — shown only in overview mode -->
  <div id="year-ctrl" style="display:none;border-bottom:1px solid #f0f0f0;padding:4px 12px 6px;">
    <div style="font-family:Arial,sans-serif;font-size:7.5px;color:#aaa;margin-bottom:3px;text-transform:uppercase;letter-spacing:1px;">普查年份</div>
    <div style="display:flex;gap:0;">
      <button class="yr-btn active" onclick="setYear('all')" id="yr-all" style="flex:1;font-family:Arial,sans-serif;font-size:8px;padding:3px 2px;border:1px solid #e0e0e0;background:#1a1a1a;color:#fff;cursor:pointer;border-radius:3px 0 0 3px;">全段</button>
      <button class="yr-btn" onclick="setYear('2013')" id="yr-2013" style="flex:1;font-family:Arial,sans-serif;font-size:8px;padding:3px 2px;border:1px solid #e0e0e0;background:#f8f8f8;color:#888;cursor:pointer;">2013</button>
      <button class="yr-btn" onclick="setYear('2018')" id="yr-2018" style="flex:1;font-family:Arial,sans-serif;font-size:8px;padding:3px 2px;border:1px solid #e0e0e0;background:#f8f8f8;color:#888;cursor:pointer;">2018</button>
      <button class="yr-btn" onclick="setYear('2023')" id="yr-2023" style="flex:1;font-family:Arial,sans-serif;font-size:8px;padding:3px 2px;border:1px solid #e0e0e0;background:#f8f8f8;color:#888;cursor:pointer;border-radius:0 3px 3px 0;">2023</button>
    </div>
  </div>
  <div id="list-hdr">全国最耗水作物</div>
  <div id="ovr-list"></div>
  <!-- Water price slider — shown only in wps mode -->
  <div id="price-ctrl" style="display:none">
    <div id="price-row">
      <label>假设水价</label>
      <input id="price-slider" type="range" min="50" max="2000" value="200" step="50"
             oninput="onPriceChange(this.value)">
      <span id="price-val">$200/af</span>
    </div>
    <div id="price-note">绿色=在此水价下仍盈利 红色=亏损（盈亏平衡水价 &lt; 滑块值则盈利）</div>
  </div>
</div>

<!-- County GeoJSON data (parsed lazily on map load) -->
<script id="county-data" type="application/json">{county_js}</script>

<!-- State info panel -->
<div id="state-panel">
  <div id="sp-hdr">
    <span id="sp-hdr-title">州级详情</span>
    <button id="sp-close" onclick="closeStatePanel()">×</button>
  </div>
  <div id="sp-body"></div>
</div>

<!-- Right crop filter panel -->
<div id="right">
  <div class="rhdr">作物筛选</div>
  <div class="rhdr-sub">af/ac 强度　|　¢/加仑产值</div>
  <button class="clear-btn" onclick="applyCropFilter('')">✕ 清除筛选（显示全部）</button>
  <div id="crop-list"></div>
</div>


<script>
const GC={{FORAGE:'#5a9e6f',GRAIN:'#c8a84b',OIL:'#d4884a',VEGETABLE:'#7ab358',ORCHARD:'#c06080',OTHER:'#8a9ba8'}};
const SKIP=new Set(['CROPS, OTHER','HORTICULTURE TOTALS','FIELD CROPS','HEMP','PASTURELAND','AG LAND']);
const CROP_MODES=new Set(['overview','value','wps']);
let mode='overview', activeCropFilter=null, activeYear='all', countySubMode='water';
let agriCounty = null;
function _getCounty(){{
  if(!agriCounty) agriCounty=JSON.parse(document.getElementById('county-data').textContent);
  return agriCounty;
}}

// ── 配色方案 ────────────────────────────────────────────────────────────────
const PALETTES={{
  overview:{{
    grad:'linear-gradient(to right,#e8f5ec,#a8d4b8,#3a8c5a,#0f3d22)',
    lo:'低',hi:'高（af/ac）',
    color:['case',['!=',['get','norm'],null],
      ['interpolate',['linear'],['get','norm'],0,'#e8f5ec',.25,'#b8dfc5',.5,'#7dbd99',.75,'#3a8c5a',1,'#0f3d22'],
      '#e8e4dc']
  }},
  value:{{
    grad:'linear-gradient(to right,#fdf3e0,#f5c87a,#d4890a,#7a4a00)',
    lo:'低',hi:'高（¢/加仑）',
    color:['case',['!=',['get','norm_dpg'],null],
      ['interpolate',['linear'],['get','norm_dpg'],0,'#fdf3e0',.25,'#f5c87a',.5,'#d4890a',.75,'#a05a00',1,'#7a4a00'],
      '#e8e4dc']
  }},
  trend:{{
    grad:'linear-gradient(to right,#08519c,#6baed6,#f7f7f7,#fc8d59,#d73027)',
    lo:'改善（↓）',hi:'恶化（↑）',
    color:['case',['!=',['get','norm_trend'],null],
      ['interpolate',['linear'],['get','norm_trend'],
        0,'#08519c',.35,'#6baed6',.5,'#f7f7f7',.65,'#fc8d59',1,'#d73027'],
      '#e8e4dc']
  }},
  opp:{{
    grad:'linear-gradient(to right,#f2f0f7,#cbc9e2,#9e9ac8,#6a51a3,#3f007d)',
    lo:'低',hi:'高（重配$）',
    color:['case',['!=',['get','norm_opp'],null],
      ['interpolate',['linear'],['get','norm_opp'],0,'#f2f0f7',.25,'#cbc9e2',.5,'#9e9ac8',.75,'#6a51a3',1,'#3f007d'],
      '#e8e4dc']
  }},
  vuln:{{
    grad:'linear-gradient(to right,#ffffcc,#feb24c,#f03b20,#bd0026)',
    lo:'低风险',hi:'高脆弱（综合）',
    color:['case',['!=',['get','norm_vuln'],null],
      ['interpolate',['linear'],['get','norm_vuln'],0,'#ffffcc',.33,'#feb24c',.66,'#f03b20',1,'#bd0026'],
      '#e8e4dc']
  }},
  diversity:{{
    grad:'linear-gradient(to right,#edf8e9,#bae4b3,#74c476,#e6550d,#a63603)',
    lo:'多样化',hi:'单一作物依赖',
    color:['case',['!=',['get','norm_hhi'],null],
      ['interpolate',['linear'],['get','norm_hhi'],0,'#edf8e9',.4,'#74c476',.7,'#fc8d59',1,'#a63603'],
      '#e8e4dc']
  }},
  climate:{{
    grad:'linear-gradient(to right,#2c7bb6,#abd9e9,#f7f7f7,#fdae61,#d7191c)',
    lo:'湿润趋势',hi:'快速变旱',
    color:['case',['!=',['get','norm_precip'],null],
      ['interpolate',['linear'],['get','norm_precip'],0,'#2c7bb6',.35,'#abd9e9',.5,'#f7f7f7',.65,'#fdae61',1,'#d7191c'],
      '#e8e4dc']
  }},
  eff:{{
    grad:'linear-gradient(to right,#f7fcfd,#99d8c9,#2ca25f,#00441b)',
    lo:'低效益',hi:'高效益（$/af）',
    color:['case',['!=',['get','norm_eff'],null],
      ['interpolate',['linear'],['get','norm_eff'],0,'#f7fcfd',.33,'#99d8c9',.66,'#2ca25f',1,'#00441b'],
      '#e8e4dc']
  }},
  vw:{{
    grad:'linear-gradient(to right,#eff3ff,#bdd7e7,#6baed6,#2171b5,#08519c)',
    lo:'低',hi:'虚拟水出口（十亿加仑/年）',
    color:['case',['!=',['get','norm_vw'],null],
      ['interpolate',['linear'],['get','norm_vw'],0,'#eff3ff',.33,'#bdd7e7',.66,'#6baed6',1,'#08519c'],
      '#e8e4dc']
  }},
  wps:{{
    grad:'linear-gradient(to right,#1a9850,#91cf60,#ffffbf,#fc8d59,#d73027)',
    lo:'盈利（低水价）',hi:'亏损（高水价）',
    color:['case',['boolean',['feature-state','wps_loss'],false],
      '#d73027',
      ['case',['boolean',['feature-state','wps_profit'],false],'#1a9850','#ffffbf']]
  }}
}};

function _setStateLayersVisible(v){{
  const vis=v?'visible':'none';
  ['agri-fill','agri-line','agri-hover'].forEach(id=>map.setLayoutProperty(id,'visibility',vis));
}}
function _setCountyLayersVisible(v){{
  const vis=v?'visible':'none';
  ['county-fill','county-line','county-hover'].forEach(id=>{{
    if(map.getLayer(id)) map.setLayoutProperty(id,'visibility',vis);
  }});
}}

function setMode(m){{
  mode=m;
  document.querySelectorAll('.mrow').forEach(r=>{{
    r.classList.toggle('active',r.dataset.mode===m);
  }});
  const isCounty=m==='county';
  document.getElementById('county-sub').style.display=isCounty?'flex':'none';

  if(isCounty){{
    _setStateLayersVisible(false);
    // Lazy-init county layers on first activation
    if(!map.getSource('county')){{
      map.addSource('county',{{type:'geojson',data:_getCounty(),generateId:true}});
      map.addLayer({{id:'county-fill',type:'fill',source:'county',
        paint:{{'fill-color':['case',['!=',['get','norm_water'],null],
          ['interpolate',['linear'],['get','norm_water'],0,'#eff3ff',.33,'#bdd7e7',.66,'#6baed6',1,'#08519c'],
          '#e8e4dc'],'fill-opacity':0.82}}}});
      map.addLayer({{id:'county-line',type:'line',source:'county',
        paint:{{'line-color':'#fff','line-width':0.3,'line-opacity':0.5}}}});
      map.addLayer({{id:'county-hover',type:'fill',source:'county',
        paint:{{'fill-color':'#000','fill-opacity':['case',['boolean',['feature-state','hover'],false],0.08,0]}}}});
      let hovCty=null;
      map.on('mousemove','county-fill',e=>{{
        map.getCanvas().style.cursor='pointer';
        if(hovCty!==null) map.setFeatureState({{source:'county',id:hovCty}},{{hover:false}});
        hovCty=e.features[0].id;
        map.setFeatureState({{source:'county',id:hovCty}},{{hover:true}});
      }});
      map.on('mouseleave','county-fill',()=>{{
        map.getCanvas().style.cursor='';
        if(hovCty!==null) map.setFeatureState({{source:'county',id:hovCty}},{{hover:false}});
        hovCty=null;
      }});
      map.on('click','county-fill',e=>{{showCountyPanel(e.features[0].properties);}});
    }} else {{
      _setCountyLayersVisible(true);
    }}
    document.getElementById('list-hdr').style.display='none';
    document.getElementById('ovr-list').style.display='none';
    document.getElementById('grad-wrap').style.display='none';
    document.getElementById('year-ctrl').style.display='none';
    document.getElementById('price-ctrl').style.display='none';
    document.getElementById('right').style.display='none';
    setCountySub(countySubMode);
    return;
  }}
  _setStateLayersVisible(true);
  _setCountyLayersVisible(false);

  const isOverview=m==='overview';
  document.getElementById('list-hdr').style.display='';
  document.getElementById('ovr-list').style.display='';
  document.getElementById('grad-wrap').style.display='';
  document.getElementById('year-ctrl').style.display=isOverview?'block':'none';
  document.getElementById('price-ctrl').style.display=(m==='wps')?'block':'none';
  // Crop panel: only for modes with per-crop data in agri_crops.json
  const showCrop=CROP_MODES.has(m);
  document.getElementById('right').style.display=showCrop?'flex':'none';
  if(!showCrop&&activeCropFilter) applyCropFilter('');
  syncPanelLayout();

  if(m==='wps'){{
    document.getElementById('list-hdr').textContent='水价敏感度';
    document.getElementById('lgd-grad').style.background=PALETTES.wps.grad;
    document.getElementById('lgd-lo').textContent='盈利';
    document.getElementById('lgd-hi').textContent='亏损';
    onPriceChange(document.getElementById('price-slider').value);
    buildOvrList('wps');
    return;
  }}
  const pal=PALETTES[m]||PALETTES.overview;
  document.getElementById('lgd-grad').style.background=pal.grad;
  document.getElementById('lgd-lo').textContent=pal.lo;
  document.getElementById('lgd-hi').textContent=pal.hi;
  buildOvrList(m);
  if(isOverview && activeYear!=='all'){{
    applyYearView(activeYear);
  }} else {{
    map.getSource('agri').setData(window._agriGeo);
    applyPalette(pal);
  }}
  if(activeCropFilter) applyCropFilter(activeCropFilter);
}}

function setCountySub(sub){{
  countySubMode=sub;
  document.querySelectorAll('.csub-btn').forEach(b=>b.classList.toggle('active',b.dataset.csub===sub));
  if(!map.getLayer('county-fill')) return;
  let color,grad,lo,hi;
  if(sub==='water'){{
    grad='linear-gradient(to right,#eff3ff,#bdd7e7,#6baed6,#2171b5,#08519c)';
    lo='少'; hi='耗水量（af/年）';
    color=['case',['!=',['get','norm_water'],null],
      ['interpolate',['linear'],['get','norm_water'],0,'#eff3ff',.33,'#bdd7e7',.66,'#6baed6',1,'#08519c'],
      '#e8e4dc'];
  }} else if(sub==='opp'){{
    grad='linear-gradient(to right,#f2f0f7,#cbc9e2,#9e9ac8,#6a51a3,#3f007d)';
    lo='低'; hi='转换机会（$）';
    color=['case',['!=',['get','norm_opp'],null],
      ['interpolate',['linear'],['get','norm_opp'],0,'#f2f0f7',.33,'#cbc9e2',.66,'#9e9ac8',1,'#3f007d'],
      '#e8e4dc'];
  }} else {{
    // crop type
    grad='linear-gradient(to right,#5a9e6f,#c8a84b,#d4884a,#7ab358,#c06080)';
    lo='FORAGE'; hi='ORCHARD';
    color=['match',['get','group'],
      'FORAGE','#5a9e6f','GRAIN','#c8a84b','OIL','#d4884a',
      'VEGETABLE','#7ab358','ORCHARD','#c06080','#8a9ba8'];
  }}
  map.setPaintProperty('county-fill','fill-color',color);
  document.getElementById('grad-wrap').style.display='block';
  document.getElementById('lgd-grad').style.background=grad;
  document.getElementById('lgd-lo').textContent=lo;
  document.getElementById('lgd-hi').textContent=hi;
  buildOvrList('county');
}}

function setYear(yr){{
  activeYear=yr;
  document.querySelectorAll('.yr-btn').forEach(b=>{{
    const isActive=(b.id==='yr-'+yr||(yr==='all'&&b.id==='yr-all'));
    b.style.background=isActive?'#1a1a1a':'#f8f8f8';
    b.style.color=isActive?'#fff':'#888';
  }});
  if(mode!=='overview') return;
  if(yr==='all'){{
    map.getSource('agri').setData(window._agriGeo);
    applyPalette(PALETTES.overview);
  }} else {{
    applyYearView(yr);
  }}
}}

// ── Water price sensitivity ───────────────────────────────────────────────────
let _currentStateProps=null;
function onPriceChange(val){{
  document.getElementById('price-val').textContent='\$'+val+'/af';
  if(mode!=='wps') return;
  // Color states by whether their avg breakeven > price (profitable) or not
  const price=parseFloat(val);
  agriGeo.features.forEach(f=>{{
    const be=f.properties.avg_breakeven;
    const sid=f.id;
    if(be!=null){{
      map.setFeatureState({{source:'agri',id:sid}},{{
        wps_profit: be<=price, wps_loss: be>price
      }});
    }}
  }});
  map.setPaintProperty('agri-fill','fill-color',PALETTES.wps.color);
  buildOvrList('wps');
}}

// ── Markdown → HTML (lightweight, for Gemini output) ─────────────────────────
function mdToHtml(md){{
  const escaped=md.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  return escaped
    .replace(/^### (.+)$/gm,'<h4>$1</h4>')
    .replace(/^## (.+)$/gm,'<h3>$1</h3>')
    .replace(/^# (.+)$/gm,'<h2>$1</h2>')
    .replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>')
    .replace(/\*(.+?)\*/g,'<em>$1</em>')
    .replace(/^[\*\-] (.+)$/gm,'<li>$1</li>')
    .replace(/(<li>.*<\/li>\\n?)+/g,s=>'<ul>'+s+'</ul>')
    .replace(/\\n{{2,}}/g,'</p><p>')
    .replace(/^(?!<[hul])/,'<p>')
    .replace(/(?<![>])$/,'</p>');
}}

// ── Gemini AI state summary ───────────────────────────────────────────────────
const GEMINI_KEY='{gemini_key}';
async function generateAISummary(){{
  if(!_currentStateProps) return;
  const p=_currentStateProps;
  const info=agriCrops[p.abbr]||{{}};

  // ── 全国对比基准 ──
  const allStates=Object.values(agriCrops).filter(v=>v.avg_intensity);
  const natAvgWI=(allStates.reduce((s,v)=>s+(+v.avg_intensity||0),0)/allStates.length).toFixed(2);
  const natAvgDPG=(allStates.filter(v=>v.avg_dpg_cents).reduce((s,v)=>s+(+v.avg_dpg_cents||0),0)/allStates.filter(v=>v.avg_dpg_cents).length).toFixed(2);
  const sortedByWI=Object.entries(agriCrops).filter(([,v])=>v.avg_intensity).sort((a,b)=>(+b[1].avg_intensity||0)-(+a[1].avg_intensity||0));
  const wiRank=sortedByWI.findIndex(([st])=>st===p.abbr)+1;
  const sortedByDPG=Object.entries(agriCrops).filter(([,v])=>v.avg_dpg_cents).sort((a,b)=>(+b[1].avg_dpg_cents||0)-(+a[1].avg_dpg_cents||0));
  const dpgRank=sortedByDPG.findIndex(([st])=>st===p.abbr)+1;
  const sortedByVuln=Object.entries(agriCrops).filter(([,v])=>{{const g=agriGeo.features.find(f=>f.properties.abbr===v);return true;}}).map(([st,v])=>{{const gp=agriGeo.features.find(f=>f.properties.abbr===st)?.properties||{{}};return [st,gp.vuln_score||0];}}).sort((a,b)=>b[1]-a[1]);
  const vulnRank=sortedByVuln.findIndex(([st])=>st===p.abbr)+1;
  const nTotal=allStates.length;

  // ── 三年趋势 ──
  const i13=(+p.intensity_2013||0).toFixed(2);
  const i18=(+p.intensity_2018||0).toFixed(2);
  const i23=(+p.intensity_2023||0).toFixed(2);
  const pr13=(+p.precip_2013||0).toFixed(1);
  const pr23=(+p.precip_2023||0).toFixed(1);
  const trendPct=p.trend_pct!=null?((+p.trend_pct>0?'+':'')+(+p.trend_pct).toFixed(1)+'%'):'—';

  // ── 完整作物结构 ──
  const crops=(info.top_crops||[]).filter(c=>!SKIP.has(c.crop));
  const cropDetail=crops.slice(0,10).map(c=>
    `  • ${{c.crop}}：面积${{c.area?.toLocaleString()||'—'}}英亩，灌溉强度${{(+c.water_int).toFixed(2)}}af/ac，产值${{c.dpg_cents!=null?(+c.dpg_cents).toFixed(2)+'¢/加仑':'无产值'}},盈亏平衡${{c.breakeven!=null?'\$'+c.breakeven.toFixed(0)+'/af':'—'}}`
  ).join('\\n');
  const highWater=crops.filter(c=>(+c.water_int||0)>1.5).map(c=>c.crop).join('、')||'无';
  const lowValue=crops.filter(c=>c.dpg_cents!=null&&(+c.dpg_cents)<0.2).map(c=>c.crop).join('、')||'无';
  const highValue=crops.filter(c=>c.dpg_cents!=null&&(+c.dpg_cents)>1.0).map(c=>c.crop).join('、')||'无';

  const wi=(+p.avg_intensity||0).toFixed(2);
  const dpg=(+p.avg_dpg_cents||0).toFixed(2);

  const prompt=`你是美国农业水资源政策分析师，兼具数据科学和农业金融背景。

请对以下【${{p.name||p.abbr}}州】数据进行深度分析，输出结构化中文报告，分4个维度，总计300-400字：

**① 现状诊断**（该州用水效率在全国的位置，与均值的偏差，10年演变轨迹）
**② 核心风险**（结合旱情趋势、作物结构、地下水、脆弱性，指出1-2个最紧迫问题）
**③ 结构性机会**（具体指出哪些高耗水低产值作物可替换为哪些高产值作物，预估可节水%或增收$）
**④ 政策优先级**（按"影响力/可行性"排序，给出2-3条可操作建议）

━━━ 数据 ━━━

【州级核心指标】
- 灌溉强度：${{wi}} af/ac（全国均值${{natAvgWI}}，全国排名第${{wiRank}}/${{nTotal}}，越高越耗水）
- 每加仑产值：${{dpg}}¢（全国均值${{natAvgDPG}}¢，产值排名第${{dpgRank}}/${{nTotal}}）
- 干旱脆弱性：${{p.vuln_score!=null?(+p.vuln_score).toFixed(2):'—'}}（全国排名第${{vulnRank}}/${{nTotal}}）
- 作物集中HHI：${{p.hhi!=null?(+p.hhi).toFixed(3):'—'}}（0=多样 1=单一依赖）
- 土壤适宜度：${{p.soil_good_ratio!=null?Math.round(+p.soil_good_ratio*100)+'% Class1-3优质土':'无数据'}}
- 重配潜力：${{p.opp_value_M!=null?'\$'+(+p.opp_value_M>=1000?((+p.opp_value_M/1000).toFixed(1)+'B'):(+p.opp_value_M).toFixed(0)+'M'):'—'}}（低效水→高值作物）
- 农业用水效益：${{p.ag_per_af!=null?'\$'+Math.round(+p.ag_per_af)+'/af':'—'}}
- 虚拟水出口：${{p.virtual_water_B!=null?(+p.virtual_water_B).toFixed(2)+'B加仑/年':'—'}}
- 灌溉总面积：${{info.total_irr_area?(+info.total_irr_area).toLocaleString()+'英亩':'—'}}
- 农业销售额：${{p.ag_sales_M!=null?'\$'+(+p.ag_sales_M).toFixed(0)+'M':'—'}}
- 地下水趋势：${{p.gw_trend_ft!=null?((+p.gw_trend_ft>0?'+':'')+p.gw_trend_ft.toFixed(2)+'ft/yr'):'无数据'}}（负=下降）

【三年趋势（USDA普查）】
灌溉强度：${{i13}} → ${{i18}} → ${{i23}} af/ac（总变化${{trendPct}}）
降水：${{pr13}}" (2013) → ${{pr23}}" (2023)，趋势${{p.precip_trend_yr!=null?((+p.precip_trend_yr).toFixed(3)+'"·年'):'无数据'}}

【作物结构（按面积前10）】
${{cropDetail||'无作物数据'}}

【自动识别】
- 高耗水作物（>1.5af/ac）：${{highWater}}
- 低产值作物（<0.2¢/加仑）：${{lowValue}}
- 高价值作物（>1.0¢/加仑）：${{highValue}}
━━━━━━━━━━━━━━`;

  const btn=document.getElementById('popup-ai-btn');
  const loading=document.getElementById('popup-ai-load');
  const textEl=document.getElementById('popup-ai-text');
  if(!btn) return;
  btn.disabled=true; btn.textContent='分析中…';
  loading.style.display='block'; textEl.style.display='none';
  try{{
    const resp=await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key=${{GEMINI_KEY}}`,
      {{method:'POST',headers:{{'Content-Type':'application/json'}},
        body:JSON.stringify({{contents:[{{parts:[{{text:prompt}}]}}]}})}}
    );
    const data=await resp.json();
    const text=data?.candidates?.[0]?.content?.parts?.[0]?.text||'分析失败，请重试';
    if(textEl){{textEl.innerHTML=mdToHtml(text); textEl.style.display='block';}}
  }} catch(e){{
    if(textEl){{textEl.textContent='网络错误：'+e.message; textEl.style.display='block';}}
  }}
  if(loading) loading.style.display='none';
  if(btn){{btn.disabled=false; btn.textContent='重新生成 ✦';}}
}}

function applyYearView(yr){{
  const normKey='norm_'+yr;
  const valKey='intensity_'+yr;
  const geo=window._agriGeo;
  const feats=geo.features.map(f=>{{
    const p=f.properties;
    return {{...f,properties:{{...p,_yr_norm:p[normKey]||null,_yr_val:p[valKey]||null}}}};
  }});
  map.getSource('agri').setData({{...geo,features:feats}});
  map.setPaintProperty('agri-fill','fill-color',
    ['case',['!=',['get','_yr_norm'],null],
      ['interpolate',['linear'],['get','_yr_norm'],0,'#e8f5ec',.25,'#b8dfc5',.5,'#7dbd99',.75,'#3a8c5a',1,'#0f3d22'],
      '#e8e4dc']);
}}

function applyPalette(pal){{
  if(!map._loaded) return;
  map.getSource('agri').setData(window._agriGeo);
  map.setPaintProperty('agri-fill','fill-color',pal.color);
}}

function buildOvrList(m){{
  const list=document.getElementById('ovr-list');
  list.innerHTML='';
  if(m==='overview'){{
    const data=[...agriSummary].sort((a,b)=>(b.avg_water_int||0)-(a.avg_water_int||0));
    const valid=data.filter(c=>!SKIP.has(c.crop)&&c.avg_water_int);
    const maxV=Math.max(...valid.map(c=>c.avg_water_int||0));
    document.getElementById('list-hdr').textContent='全国最耗水作物';
    valid.slice(0,12).forEach((c,i)=>{{
      const v=c.avg_water_int; const bar=`<div class='pl-b' style='width:${{Math.round(v/maxV*100)}}%;background:${{c.color}}'></div>`;
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'><span class='pl-rk'>${{i+1}}</span>
        <div class='pl-dot' style='background:${{c.color}}'></div>
        <span class='pl-nm'>${{c.crop}}</span>
        <div class='pl-bw'>${{bar}}</div>
        <span class='pl-v'>${{v.toFixed(2)}}</span></div>`);
    }});
  }} else if(m==='value'){{
    const data=[...agriSummary].sort((a,b)=>(b.avg_dpg_cents||0)-(a.avg_dpg_cents||0));
    const valid=data.filter(c=>!SKIP.has(c.crop)&&c.avg_dpg_cents);
    const maxV=Math.max(...valid.map(c=>c.avg_dpg_cents||0));
    document.getElementById('list-hdr').textContent='全国最高产值作物';
    valid.slice(0,12).forEach((c,i)=>{{
      const v=c.avg_dpg_cents; const bar=`<div class='pl-b' style='width:${{Math.round(v/maxV*100)}}%;background:${{c.color}}'></div>`;
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'><span class='pl-rk'>${{i+1}}</span>
        <div class='pl-dot' style='background:${{c.color}}'></div>
        <span class='pl-nm'>${{c.crop}}</span>
        <div class='pl-bw'>${{bar}}</div>
        <span class='pl-v'>${{v.toFixed(2)}}¢</span></div>`);
    }});
  }} else if(m==='trend'){{
    document.getElementById('list-hdr').textContent='效率变化最大的州';
    const stateArr=Object.entries(agriCrops)
      .filter(([,v])=>v.trend_pct!=null)
      .sort((a,b)=>Math.abs(b[1].trend_pct)-Math.abs(a[1].trend_pct))
      .slice(0,12);
    stateArr.forEach(([st,v])=>{{
      const t=v.trend_pct; const col=t<0?'#3a8c5a':'#c0392b';
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:${{col}}'></div>
        <span class='pl-nm'>${{st}}</span>
        <span class='pl-v' style='color:${{col}}'>${{t>0?'+':''}}${{t.toFixed(1)}}%</span></div>`);
    }});
  }} else if(m==='opp'){{
    document.getElementById('list-hdr').textContent='重配潜力最大的州';
    const stateArr=Object.entries(agriCrops)
      .filter(([,v])=>v.opp_value_M)
      .sort((a,b)=>b[1].opp_value_M-a[1].opp_value_M)
      .slice(0,12);
    const maxV=stateArr[0]?.[1]?.opp_value_M||1;
    stateArr.forEach(([st,v])=>{{
      const m2=v.opp_value_M; const bar=`<div class='pl-b' style='width:${{Math.round(m2/maxV*100)}}%;background:#9e9ac8'></div>`;
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:#9e9ac8'></div>
        <span class='pl-nm'>${{st}}</span>
        <div class='pl-bw'>${{bar}}</div>
        <span class='pl-v'>\$${{(m2/1000).toFixed(1)}}B</span></div>`);
    }});
  }} else if(m==='vuln'){{
    document.getElementById('list-hdr').textContent='干旱脆弱性最高的州';
    // vuln_score from agriCrops; use geo features for norm_vuln
    const geoMap={{}};
    agriGeo.features.forEach(f=>{{if(f.properties.abbr)geoMap[f.properties.abbr]=f.properties;}});
    const stateArr=Object.entries(agriCrops)
      .map(([st,v])=>([st,geoMap[st]?.vuln_score||0]))
      .sort((a,b)=>b[1]-a[1]).slice(0,12);
    stateArr.forEach(([st,score])=>{{
      const col=score>=3?'#bd0026':score>=2?'#f03b20':score>=1?'#feb24c':'#ffffcc';
      const labels=['','⚡','⚡⚡','⚡⚡⚡','⚡⚡⚡⚡'];
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:${{col}}'></div>
        <span class='pl-nm'>${{st}}</span>
        <span class='pl-v' style='color:${{col}}'>${{labels[score]||''}} ${{score}}/4</span></div>`);
    }});
  }} else if(m==='diversity'){{
    document.getElementById('list-hdr').textContent='作物集中度最高的州';
    const geoMap={{}};
    agriGeo.features.forEach(f=>{{if(f.properties.abbr)geoMap[f.properties.abbr]=f.properties;}});
    const stateArr=Object.entries(agriCrops)
      .map(([st])=>([st,geoMap[st]?.hhi||null]))
      .filter(([,h])=>h!=null)
      .sort((a,b)=>b[1]-a[1]).slice(0,12);
    const maxV=stateArr[0]?.[1]||1;
    stateArr.forEach(([st,h])=>{{
      const bar=`<div class='pl-b' style='width:${{Math.round(h/maxV*100)}}%;background:#fc8d59'></div>`;
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:#fc8d59'></div>
        <span class='pl-nm'>${{st}}</span>
        <div class='pl-bw'>${{bar}}</div>
        <span class='pl-v'>${{h.toFixed(3)}}</span></div>`);
    }});
  }} else if(m==='climate'){{
    document.getElementById('list-hdr').textContent='旱情加剧最快的州';
    const geoMap={{}};
    agriGeo.features.forEach(f=>{{if(f.properties.abbr)geoMap[f.properties.abbr]=f.properties;}});
    const stateArr=Object.entries(agriCrops)
      .map(([st])=>([st,geoMap[st]?.precip_trend_yr]))
      .filter(([,t])=>t!=null)
      .sort((a,b)=>a[1]-b[1]).slice(0,12);  // most negative = most drying
    stateArr.forEach(([st,t])=>{{
      const col=t<-0.3?'#d7191c':t<-0.1?'#fdae61':'#abd9e9';
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:${{col}}'></div>
        <span class='pl-nm'>${{st}}</span>
        <span class='pl-v' style='color:${{col}}'>${{t>0?'+':''}}${{t.toFixed(3)}}"</span></div>`);
    }});
  }} else if(m==='eff'){{
    document.getElementById('list-hdr').textContent='农业用水效益最高的州';
    const geoMap={{}};
    agriGeo.features.forEach(f=>{{if(f.properties.abbr)geoMap[f.properties.abbr]=f.properties;}});
    const stateArr=Object.entries(agriCrops)
      .map(([st])=>([st,geoMap[st]?.ag_per_af||null]))
      .filter(([,e])=>e!=null)
      .sort((a,b)=>b[1]-a[1]).slice(0,12);
    const maxV=stateArr[0]?.[1]||1;
    stateArr.forEach(([st,e])=>{{
      const bar=`<div class='pl-b' style='width:${{Math.round(e/maxV*100)}}%;background:#2ca25f'></div>`;
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:#2ca25f'></div>
        <span class='pl-nm'>${{st}}</span>
        <div class='pl-bw'>${{bar}}</div>
        <span class='pl-v'>\$${{Math.round(e)}}</span></div>`);
    }});
  }} else if(m==='vw'){{
    document.getElementById('list-hdr').textContent='虚拟水出口最多的州';
    const geoMap={{}};
    agriGeo.features.forEach(f=>{{if(f.properties.abbr)geoMap[f.properties.abbr]=f.properties;}});
    const stateArr=Object.entries(agriCrops)
      .map(([st])=>([st,geoMap[st]?.virtual_water_B||null]))
      .filter(([,v])=>v!=null&&v>0)
      .sort((a,b)=>b[1]-a[1]).slice(0,12);
    const maxV=stateArr[0]?.[1]||1;
    stateArr.forEach(([st,v])=>{{
      const bar=`<div class='pl-b' style='width:${{Math.round(v/maxV*100)}}%;background:#6baed6'></div>`;
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:#6baed6'></div>
        <span class='pl-nm'>${{st}}</span>
        <div class='pl-bw'>${{bar}}</div>
        <span class='pl-v'>${{v.toFixed(2)}}B</span></div>`);
    }});
  }} else if(m==='county'){{
    const sub=countySubMode;
    const feats=_getCounty().features.map(f=>f.properties);
    if(sub==='water'){{
      document.getElementById('list-hdr').textContent='耗水量最大的县';
      document.getElementById('list-hdr').style.display='';
      document.getElementById('ovr-list').style.display='';
      const top=feats.filter(p=>p.est_water_af>0).sort((a,b)=>b.est_water_af-a.est_water_af).slice(0,12);
      const maxV=top[0]?.est_water_af||1;
      top.forEach(p=>{{
        const v=+p.est_water_af; const vStr=v>=1e6?(v/1e6).toFixed(1)+'M':Math.round(v/1000)+'K';
        const col=GC[p.group]||'#8a9ba8';
        const bar=`<div class='pl-b' style='width:${{Math.round(v/maxV*100)}}%;background:${{col}}'></div>`;
        list.insertAdjacentHTML('beforeend',
          `<div class='pl-row'>
          <div class='pl-dot' style='background:${{col}}'></div>
          <span class='pl-nm'>${{p.county||p.fips}}, ${{p.state}}</span>
          <div class='pl-bw'>${{bar}}</div>
          <span class='pl-v'>${{vStr}}</span></div>`);
      }});
    }} else if(sub==='opp'){{
      document.getElementById('list-hdr').textContent='转换机会最大的县';
      document.getElementById('list-hdr').style.display='';
      document.getElementById('ovr-list').style.display='';
      const top=feats.filter(p=>p.opp_value_M>0).sort((a,b)=>b.opp_value_M-a.opp_value_M).slice(0,12);
      const maxV=top[0]?.opp_value_M||1;
      top.forEach(p=>{{
        const v=+p.opp_value_M; const vStr=v>=1000?'\$'+(v/1000).toFixed(1)+'B':'\$'+Math.round(v)+'M';
        const bar=`<div class='pl-b' style='width:${{Math.round(v/maxV*100)}}%;background:#9e9ac8'></div>`;
        list.insertAdjacentHTML('beforeend',
          `<div class='pl-row'>
          <div class='pl-dot' style='background:#9e9ac8'></div>
          <span class='pl-nm'>${{p.county||p.fips}}, ${{p.state}}</span>
          <div class='pl-bw'>${{bar}}</div>
          <span class='pl-v'>${{vStr}}</span></div>`);
      }});
    }} else {{
      document.getElementById('list-hdr').textContent='作物分布前12县';
      document.getElementById('list-hdr').style.display='';
      document.getElementById('ovr-list').style.display='';
      const top=feats.filter(p=>p.top_crop).sort((a,b)=>(+b.total_area||0)-(+a.total_area||0)).slice(0,12);
      top.forEach(p=>{{
        const col=GC[p.group]||'#8a9ba8';
        list.insertAdjacentHTML('beforeend',
          `<div class='pl-row'>
          <div class='pl-dot' style='background:${{col}}'></div>
          <span class='pl-nm'>${{p.county||p.fips}}, ${{p.state}}</span>
          <span class='pl-v' style='font-size:8px;color:#888'>${{(p.top_crop||'').slice(0,8)}}</span></div>`);
      }});
    }}
  }} else if(m==='wps'){{
    const priceAf=parseFloat(document.getElementById('price-slider')?.value||200);
    document.getElementById('list-hdr').textContent=`${{priceAf}}/af时盈利作物`;
    const profitable=agriSummary.filter(c=>!SKIP.has(c.crop)&&c.avg_breakeven!=null&&c.avg_breakeven<=priceAf);
    const losing=agriSummary.filter(c=>!SKIP.has(c.crop)&&c.avg_breakeven!=null&&c.avg_breakeven>priceAf);
    profitable.slice(0,6).forEach(c=>{{
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:#1a9850'></div>
        <span class='pl-nm'>${{c.crop}}</span>
        <span class='pl-v' style='color:#1a9850'>\$${{Math.round(c.avg_breakeven)}}</span></div>`);
    }});
    losing.slice(0,6).forEach(c=>{{
      list.insertAdjacentHTML('beforeend',
        `<div class='pl-row'>
        <div class='pl-dot' style='background:#d73027'></div>
        <span class='pl-nm'>${{c.crop}}</span>
        <span class='pl-v' style='color:#d73027'>\$${{Math.round(c.avg_breakeven)}}</span></div>`);
    }});
  }}
}}

// ── Map setup ────────────────────────────────────────────────────────────────
const map=new maplibregl.Map({{
  container:'map',
  style:'https://tiles.openfreemap.org/styles/positron',
  center:[-98.5,39.5],zoom:4,maxZoom:12,minZoom:2
}});

const agriGeo     = {geo_js};
const agriCrops   = {crops_js};
const agriSummary = {summary_js};
const agriPareto  = {pareto_js};

window._agriGeo=agriGeo;
window._cropByState={{}};
for(const [st,info] of Object.entries(agriCrops)){{
  for(const c of (info.top_crops||[])){{
    if(!window._cropByState[st]) window._cropByState[st]={{}};
    window._cropByState[st][c.crop]=c;
  }}
}}

map._loaded=false;
map.on('load',()=>{{
  map._loaded=true;

  // State layer
  map.addSource('agri',{{type:'geojson',data:agriGeo,generateId:true}});
  map.addLayer({{id:'agri-fill',type:'fill',source:'agri',
    paint:{{'fill-color':PALETTES.overview.color,'fill-opacity':0.82}}}});
  map.addLayer({{id:'agri-line',type:'line',source:'agri',
    paint:{{'line-color':'#fff','line-width':0.5,'line-opacity':0.6}}}});
  map.addLayer({{id:'agri-hover',type:'fill',source:'agri',
    paint:{{'fill-color':'#000','fill-opacity':['case',['boolean',['feature-state','hover'],false],0.07,0]}}}});


  // Hover
  let hovered=null;
  map.on('mousemove','agri-fill',e=>{{
    map.getCanvas().style.cursor='pointer';
    if(hovered!==null) map.setFeatureState({{source:'agri',id:hovered}},{{hover:false}});
    hovered=e.features[0].id;
    map.setFeatureState({{source:'agri',id:hovered}},{{hover:true}});
  }});
  map.on('mouseleave','agri-fill',()=>{{
    map.getCanvas().style.cursor='';
    if(hovered!==null) map.setFeatureState({{source:'agri',id:hovered}},{{hover:false}});
    hovered=null;
  }});

  // Click state → right panel
  map.on('click','agri-fill',e=>{{
    const p=e.features[0].properties;
    showStatePanel(p, agriCrops[p.abbr]||{{}});
  }});

  // County layers added lazily on first county mode activation

  // Build right panel crop list
  (function(){{
    const cl=document.getElementById('crop-list');
    const seen=new Set();
    // Collect unique crops with color from agriCrops top_crops
    const cropMeta={{}};
    for(const [,info] of Object.entries(agriCrops)){{
      for(const c of (info.top_crops||[])){{
        if(!cropMeta[c.crop]) cropMeta[c.crop]=c.color||'#999';
      }}
    }}
    agriSummary.filter(c=>!SKIP.has(c.crop)&&c.avg_water_int>0)
      .sort((a,b)=>b.avg_water_int-a.avg_water_int)
      .forEach(c=>{{
        if(seen.has(c.crop)) return; seen.add(c.crop);
        const wi=c.avg_water_int.toFixed(2);
        const dpg=c.avg_dpg_cents!=null?c.avg_dpg_cents.toFixed(2)+'¢':'—';
        const col=cropMeta[c.crop]||'#999';
        const btn=document.createElement('button');
        btn.className='crop-btn'; btn.dataset.crop=c.crop;
        btn.innerHTML=`<div class='cb-swatch' style='background:${{col}}'></div>
          <span style='flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap'>${{c.crop}}</span>
          <div class='cb-vals'><span class='cb-wi'>${{wi}}</span><span class='cb-dpg'>${{dpg}}</span></div>`;
        btn.onclick=()=>applyCropFilter(activeCropFilter===c.crop?'':c.crop);
        cl.appendChild(btn);
      }});
  }})();

  buildOvrList('overview');
  document.getElementById('lgd-grad').style.background=PALETTES.overview.grad;

  // Show national overview in state panel on load
  showNationalPanel();
}});

function showCountyPanel(p){{
  const countyName=p.county||p.fips;
  const stAbbr=p.state||'';
  const area=(+p.total_area||0).toLocaleString();
  const waterAf=(+p.est_water_af||0);
  const waterStr=waterAf>=1e6?(waterAf/1e6).toFixed(2)+'M af':waterAf>=1000?(waterAf/1000).toFixed(0)+'K af':waterAf.toFixed(0)+' af';
  const oppM=(+p.opp_value_M||0);
  const oppStr=oppM>=1000?'\$'+(oppM/1000).toFixed(1)+'B':oppM>0?'\$'+oppM.toFixed(0)+'M':'—';
  const stWI=(+p.st_wi||0).toFixed(2);
  const stDpg=(+p.st_dpg_cents||0);
  const effAf=stDpg>0?(stDpg/100*325851).toFixed(0):'—';
  const col=GC[p.group]||'#8a9ba8';

  // State crop info from agriCrops (for comparison)
  const stInfo=agriCrops[stAbbr]||{{}};
  const stWIStr=stInfo.avg_intensity?(+stInfo.avg_intensity).toFixed(2):'—';
  const stOppStr=stInfo.opp_value_M?('\$'+(+stInfo.opp_value_M>=1000?((+stInfo.opp_value_M/1000).toFixed(1)+'B'):(+stInfo.opp_value_M).toFixed(0)+'M')):'—';

  // Pareto hint: is this county in the top water users?
  const allWater=_getCounty().features.map(f=>(+f.properties.est_water_af||0)).sort((a,b)=>b-a);
  const totalW=allWater.reduce((s,v)=>s+v,0);
  let cumW=0,paretoRank=0;
  for(let i=0;i<allWater.length;i++){{ cumW+=allWater[i]; if(cumW>=totalW*0.5&&!paretoRank) paretoRank=i+1; }}
  const rankAmong=allWater.findIndex(w=>w<=waterAf)+1;
  const pctNat=totalW>0?(waterAf/totalW*100).toFixed(2):'—';

  const html=`<div style='font-family:Palatino Linotype,Palatino,serif'>
<div class='sp-nm' style='font-size:14px'>${{countyName}} County</div>
<div class='sp-sub'>${{stAbbr}} · 主导作物：<span style='color:${{col}};font-weight:700'>${{p.top_crop||'—'}}</span> <span style='display:inline-block;width:7px;height:7px;border-radius:50%;background:${{col}};vertical-align:middle'></span></div>
<div class='sp-grid'>
  <div class='sp-cell'><div class='sp-lbl'>灌溉面积</div><div class='sp-val'>${{area}}</div><div class='sp-unit'>英亩</div></div>
  <div class='sp-cell'><div class='sp-lbl'>估算耗水量</div><div class='sp-val'>${{waterStr}}</div><div class='sp-unit'>年</div></div>
  <div class='sp-cell'><div class='sp-lbl'>转换机会</div><div class='sp-val' style='color:#6a3d9a'>${{oppStr}}</div><div class='sp-unit'>低效→高值</div></div>
  <div class='sp-cell'><div class='sp-lbl'>效率（州均）</div><div class='sp-val' style='color:#2ca25f'>${{effAf}}</div><div class='sp-unit'>$/af</div></div>
</div>
<div style='background:#f8f7f4;border-radius:3px;padding:5px 8px;margin-bottom:7px;font-family:Arial,sans-serif;font-size:8.5px;line-height:1.6'>
  <span style='color:#888'>全国占比：</span><b>${{pctNat}}%</b> 农业用水
  <span style='color:#aaa;margin-left:8px'>· 州均水强度：</span><b>${{stWIStr}} af/ac</b>
  <span style='color:#aaa;margin-left:8px'>· 州重配潜力：</span><b>${{stOppStr}}</b>
</div>
<div style='font-family:Arial,sans-serif;font-size:8px;color:#aaa;border-top:1px solid #f0f0f0;padding-top:6px;line-height:1.5'>
  效率数据为州级代理值（USDA NASS 县级无单位水强度）<br>
  耗水量 = 灌溉面积 × 州均灌溉强度（${{stWI}} af/ac）
</div>
</div>`;
  document.getElementById('sp-hdr-title').textContent=countyName+' County';
  document.getElementById('sp-body').innerHTML=html;
  document.getElementById('state-panel').style.display='flex';
  syncPanelLayout();
}}

function showNationalPanel(){{
  const svgW=244;
  // Top crops by water intensity (national)
  const topCrops=agriSummary.filter(c=>!SKIP.has(c.crop)&&c.avg_water_int>0)
    .sort((a,b)=>b.avg_water_int-a.avg_water_int).slice(0,10);
  const maxWI=Math.max(...topCrops.map(c=>c.avg_water_int),0.001);
  const cLblW=90,cValW=30,cPad=4,cBarAreaW=svgW-cLblW-cValW-cPad*2-2;
  const cRowH=17;
  const cropRows=topCrops.map((c,i)=>{{
    const bw=Math.max(2,Math.round(c.avg_water_int/maxWI*cBarAreaW));
    const y=i*cRowH+4;
    const nm=c.crop.length>15?c.crop.slice(0,14)+'…':c.crop;
    const dpgTxt=c.avg_dpg_cents!=null?c.avg_dpg_cents.toFixed(2)+'¢':'—';
    const col=GC[c.group]||'#aaa';
    return `<text x="${{cPad}}" y="${{y+11}}" font-size="8.5" fill="#555" font-family="Arial">${{nm}}</text>
      <rect x="${{cPad+cLblW}}" y="${{y+2}}" width="${{bw}}" height="11" fill="${{col}}" rx="2" opacity="0.82"/>
      <text x="${{cPad+cLblW+cBarAreaW+3}}" y="${{y+8}}" font-size="7.5" fill="#888" font-family="Arial">${{c.avg_water_int.toFixed(2)}}</text>
      <text x="${{cPad+cLblW+cBarAreaW+3}}" y="${{y+16}}" font-size="7" fill="#c4870a" font-family="Arial">${{dpgTxt}}</text>`;
  }}).join('');
  const cropSvgH=topCrops.length*cRowH+6;
  const cropSvg=`<svg width="${{svgW}}" height="${{cropSvgH}}" xmlns="http://www.w3.org/2000/svg">${{cropRows}}</svg>`;

  // National stats
  const totalArea=Object.values(agriCrops).reduce((s,v)=>s+(+v.total_irr_area||0),0);
  const totalOpp=Object.values(agriCrops).reduce((s,v)=>s+(+v.opp_value_M||0),0);
  const avgIntens=agriSummary.find(c=>c.crop==='HAY & HAYLAGE')?.avg_water_int||0;

  const html=`<div style='font-family:Palatino Linotype,Palatino,serif'>
<div class='sp-nm' style='font-size:13px'>全美农业用水概览</div>
<div class='sp-sub'>USDA NASS · 2013–2023 灌溉普查</div>
<div class='sp-grid'>
  <div class='sp-cell'><div class='sp-lbl'>覆盖州数</div><div class='sp-val'>${{Object.keys(agriCrops).length}}</div><div class='sp-unit'>个州</div></div>
  <div class='sp-cell'><div class='sp-lbl'>总灌溉面积</div><div class='sp-val'>${{(totalArea/1e6).toFixed(1)}}M</div><div class='sp-unit'>英亩</div></div>
  <div class='sp-cell'><div class='sp-lbl'>作物种类</div><div class='sp-val'>${{agriSummary.length}}</div><div class='sp-unit'>主要作物</div></div>
  <div class='sp-cell'><div class='sp-lbl'>重配潜力</div><div class='sp-val' style='color:#6a3d9a'>\$${{(totalOpp/1e3).toFixed(0)}}B</div><div class='sp-unit'>全国合计</div></div>
</div>
<div style='font-family:Arial,sans-serif;font-size:7.5px;font-weight:700;color:#888;margin:6px 0 2px'>
  全国最耗水作物　<span style='font-weight:400;color:#aaa'>af/ac 强度 · ¢ 产值</span>
</div>
${{cropSvg}}
<div style='font-family:Arial,sans-serif;font-size:8px;color:#aaa;margin-top:8px;line-height:1.5;border-top:1px solid #f0f0f0;padding-top:6px'>
  点击地图上任意州查看该州详细数据与 AI 分析
</div>
</div>`;
  document.getElementById('sp-hdr-title').textContent='全国概览';
  document.getElementById('sp-body').innerHTML=html;
  document.getElementById('state-panel').style.display='flex';
  syncPanelLayout();
}}

// ── Crop filter — dims states without selected crop ──────────────────────────
function applyCropFilter(cropName){{
  activeCropFilter=cropName||null;
  // Update crop button states
  document.querySelectorAll('.crop-btn').forEach(b=>
    b.classList.toggle('active',b.dataset.crop===cropName));
  const geo=window._agriGeo;
  if(!geo) return;
  if(!cropName){{
    map.setPaintProperty('agri-fill','fill-opacity',0.82);
    const pal=PALETTES[mode]||PALETTES.overview;
    if(mode!=='wps') applyPalette(pal);
    return;
  }}
  const byState=window._cropByState;
  const feats=geo.features.map(f=>{{
    const hasCrop=byState[f.properties.abbr]?.[cropName]!=null?1:0;
    return {{...f,properties:{{...f.properties,_has_crop:hasCrop}}}};
  }});
  map.getSource('agri').setData({{...geo,features:feats}});
  map.setPaintProperty('agri-fill','fill-opacity',
    ['case',['==',['get','_has_crop'],1],0.85,0.10]);
}}

// ── Panel layout sync ─────────────────────────────────────────────────────────
function syncPanelLayout(){{
  const stateOpen=document.getElementById('state-panel').style.display==='flex';
  // Crop filter sits to the left of state panel when both visible
  document.getElementById('right').style.right=stateOpen?'302px':'12px';
}}

// ── State info panel ──────────────────────────────────────────────────────────
function closeStatePanel(){{
  document.getElementById('state-panel').style.display='none';
  _currentStateProps=null;
  syncPanelLayout();
}}

function showStatePanel(p, info){{
  _currentStateProps=p;
  const t=p.trend_pct;
  const wi=(+p.avg_intensity||0)>0?(+p.avg_intensity).toFixed(2):'—';
  const dpg=(+p.avg_dpg_cents||0)>0?(+p.avg_dpg_cents).toFixed(2)+'¢':'—';
  const tStr=t!=null?((+t>0?'+':'')+(+t).toFixed(1)+'%'):'—';
  const tCol=t!=null?((+t<0?'#2171b5':'#c0392b')):'#aaa';
  const oppM=p.opp_value_M;
  const oppStr=oppM!=null?('\$'+(+oppM>=1000?((+oppM/1000).toFixed(1)+'B'):(+oppM).toFixed(0)+'M')):'—';
  const vs=p.vuln_score;
  const vulnStr=vs!=null?(+vs).toFixed(2):'—';
  const vulnCol=+vs>2?'#bd0026':+vs>1?'#f03b20':+vs>0.5?'#feb24c':'#2ca25f';
  const soil=p.soil_good_ratio;
  const soilStr=soil!=null?Math.round(+soil*100)+'%':'—';
  const soilCol=+soil>0.6?'#2ca25f':+soil>0.35?'#fc8d59':'#d73027';
  const hhi=p.hhi; const hhiStr=hhi!=null?(+hhi).toFixed(3):'—';
  const hhiCol=+hhi>0.5?'#d73027':+hhi>0.25?'#fc8d59':'#2ca25f';
  const eff=p.ag_per_af; const effStr=eff!=null?'\$'+Math.round(+eff).toLocaleString():'—';
  const sub=(info?.total_irr_area?(+info.total_irr_area).toLocaleString()+' 英亩灌溉 · ':'')+
            (info?.n_crops?info.n_crops+' 种作物':'');
  // ── SVG trend line chart (灌溉强度 vs 降水，三年) ─────────────────────────
  const iVals=[+p.intensity_2013||0,+p.intensity_2018||0,+p.intensity_2023||0];
  const prVals=[+p.precip_2013||0,+p.precip_2018||0,+p.precip_2023||0];
  const svgW=244,svgH=58,padL=14,padR=14,padT=8,padB=18;
  const plotW=svgW-padL-padR,plotH=svgH-padT-padB;
  const maxI=Math.max(...iVals,0.001);const maxP=Math.max(...prVals,0.001);
  const xp=(i)=>padL+i*(plotW/2);
  const iy=(v)=>padT+plotH*(1-v/maxI);
  const py=(v)=>padT+plotH*(1-v/maxP);
  const tColor=(+t||0)<0?'#2171b5':'#e05e2b';
  const iLinePts=iVals.map((v,i)=>`${{xp(i)}},${{iy(v)}}`).join(' ');
  const pLinePts=prVals.map((v,i)=>`${{xp(i)}},${{py(v)}}`).join(' ');
  const yrLbls=['2013','2018','2023'].map((yr,i)=>`<text x="${{xp(i)}}" y="${{svgH-2}}" text-anchor="middle" font-size="8" fill="#bbb" font-family="Arial">${{yr}}</text>`).join('');
  const iDotsSvg=iVals.map((v,i)=>`<circle cx="${{xp(i)}}" cy="${{iy(v)}}" r="3" fill="${{tColor}}"/>`).join('');
  const pDotsSvg=prVals.map((v,i)=>`<circle cx="${{xp(i)}}" cy="${{py(v)}}" r="2.5" fill="#74c476" opacity="0.85"/>`).join('');
  const iValLbls=iVals.map((v,i)=>`<text x="${{xp(i)}}" y="${{iy(v)-5}}" text-anchor="middle" font-size="7.5" fill="${{tColor}}" font-family="Arial">${{v.toFixed(1)}}</text>`).join('');
  // % change arrows between years
  const chgLbls=[[0,1],[1,2]].map(([a,b])=>{{
    if(!iVals[a]) return '';
    const pct=((iVals[b]-iVals[a])/iVals[a]*100);
    const mx=(xp(a)+xp(b))/2; const my=(iy(iVals[a])+iy(iVals[b]))/2-4;
    const col=pct<0?'#3a8c5a':'#c0392b';
    return `<text x="${{mx}}" y="${{my}}" text-anchor="middle" font-size="7" fill="${{col}}" font-family="Arial">${{pct>0?'+':''}}${{pct.toFixed(0)}}%</text>`;
  }}).join('');
  const trendSvg=`<svg width="${{svgW}}" height="${{svgH}}" xmlns="http://www.w3.org/2000/svg">
    <polyline points="${{pLinePts}}" fill="none" stroke="#74c476" stroke-width="1.5" stroke-dasharray="3,2" opacity="0.7"/>
    <polyline points="${{iLinePts}}" fill="none" stroke="${{tColor}}" stroke-width="2.5"/>
    ${{iDotsSvg}}${{pDotsSvg}}${{iValLbls}}${{chgLbls}}${{yrLbls}}
  </svg>`;

  // ── SVG crop bar chart (水平条形，按灌溉强度排序) ─────────────────────────
  const cropData=(info?.top_crops||[]).slice(0,8);
  const maxWI=Math.max(...cropData.map(c=>+c.water_int||0),0.001);
  const cLblW=80,cValW=30,cPad=4,cBarAreaW=svgW-cLblW-cValW-cPad*2-2;
  const cRowH=17;const cropSvgH=Math.max(cropData.length*cRowH+6,20);
  const cropRows=cropData.map((c,i)=>{{
    const bw=Math.max(2,Math.round((+c.water_int||0)/maxWI*cBarAreaW));
    const y=i*cRowH+4;
    const nm=c.crop.length>13?c.crop.slice(0,12)+'…':c.crop;
    const dpgTxt=c.dpg_cents!=null?(+c.dpg_cents).toFixed(1)+'¢':'—';
    return `<text x="${{cPad}}" y="${{y+11}}" font-size="8.5" fill="#555" font-family="Arial">${{nm}}</text>
      <rect x="${{cPad+cLblW}}" y="${{y+2}}" width="${{bw}}" height="11" fill="${{c.color||'#aaa'}}" rx="2" opacity="0.82"/>
      <text x="${{cPad+cLblW+cBarAreaW+3}}" y="${{y+8}}" font-size="7.5" fill="#888" font-family="Arial">${{(+c.water_int||0).toFixed(2)}}</text>
      <text x="${{cPad+cLblW+cBarAreaW+3}}" y="${{y+16}}" font-size="7" fill="#c4870a" font-family="Arial">${{dpgTxt}}</text>`;
  }}).join('');
  const cropSvg=cropData.length
    ?`<svg width="${{svgW}}" height="${{cropSvgH}}" xmlns="http://www.w3.org/2000/svg">${{cropRows}}</svg>`
    :'<span style="color:#aaa;font-size:9px;font-family:Arial">暂无数据</span>';

  // Crop-specific row if filter active
  let cropRow='';
  if(activeCropFilter && window._cropByState?.[p.abbr]?.[activeCropFilter]){{
    const cd=window._cropByState[p.abbr][activeCropFilter];
    cropRow=`<div style='background:#f0f9ff;border-radius:3px;padding:4px 7px;margin-bottom:6px;font-family:Arial,sans-serif;font-size:9px'>
      <b style='color:#2171b5'>▶ 筛选作物：${{activeCropFilter}}</b><br>
      灌溉强度 ${{cd.water_int?.toFixed(2)||'—'}} af/ac · 产值 ${{cd.dpg_cents?.toFixed(2)||'—'}}¢/加仑
    </div>`;
  }}

  // Year comparison table (2013 / 2018 / 2023)
  const i13=iVals[0].toFixed(2),i18=iVals[1].toFixed(2),i23=iVals[2].toFixed(2);
  const trendPct=tStr;
  const yrRows=[['灌溉强度(af/ac)',i13,i18,i23,trendPct],
    ['降水(英寸)',prVals[0].toFixed(1),prVals[1].toFixed(1),prVals[2].toFixed(1),
      prVals[0]>0?((prVals[2]-prVals[0])/prVals[0]*100>0?'+':'')+((prVals[2]-prVals[0])/prVals[0]*100).toFixed(0)+'%':'—']];
  const yrTable=`<table style='width:100%;border-collapse:collapse;font-family:Arial,sans-serif;font-size:8px;margin-bottom:6px'>
    <tr style='color:#aaa'><td style='padding:2px 0'></td><td style='text-align:center'>2013</td><td style='text-align:center'>2018</td><td style='text-align:center;color:${{tColor}}'>2023</td><td style='text-align:right;font-size:7px'>变化</td></tr>
    ${{yrRows.map(([lbl,v1,v2,v3,chg])=>`<tr style='border-top:1px solid #f5f5f5'><td style='color:#888;padding:2px 0'>${{lbl}}</td><td style='text-align:center;color:#bbb'>${{v1}}</td><td style='text-align:center;color:#999'>${{v2}}</td><td style='text-align:center;font-weight:700;color:${{tColor}}'>${{v3}}</td><td style='text-align:right;color:${{+chg>0?'#c0392b':'#3a8c5a'}}'>${{chg}}</td></tr>`).join('')}}
  </table>`;

  const html=`<div style='font-family:Palatino Linotype,Palatino,serif'>
<div class='sp-nm'>${{p.name||p.abbr}}</div>
<div class='sp-sub'>${{sub||'—'}}</div>
${{cropRow}}
<div class='sp-grid'>
  <div class='sp-cell'><div class='sp-lbl'>灌溉强度</div><div class='sp-val'>${{wi}}</div><div class='sp-unit'>af/ac</div></div>
  <div class='sp-cell'><div class='sp-lbl'>产值</div><div class='sp-val' style='color:#c4870a'>${{dpg}}</div><div class='sp-unit'>美分/加仑</div></div>
  <div class='sp-cell'><div class='sp-lbl'>效率趋势</div><div class='sp-val' style='color:${{tCol}}'>${{tStr}}</div><div class='sp-unit'>2013→2023</div></div>
  <div class='sp-cell'><div class='sp-lbl'>重配潜力</div><div class='sp-val' style='color:#6a3d9a'>${{oppStr}}</div><div class='sp-unit'>低效水改高值</div></div>
  <div class='sp-cell'><div class='sp-lbl'>脆弱性</div><div class='sp-val' style='color:${{vulnCol}}'>${{vulnStr}}</div><div class='sp-unit'>强度×旱情÷产值</div></div>
  <div class='sp-cell'><div class='sp-lbl'>土壤适宜</div><div class='sp-val' style='color:${{soilCol}}'>${{soilStr}}</div><div class='sp-unit'>Class 1-3</div></div>
  <div class='sp-cell'><div class='sp-lbl'>作物集中HHI</div><div class='sp-val' style='color:${{hhiCol}}'>${{hhiStr}}</div><div class='sp-unit'>0=多样 1=单一</div></div>
  <div class='sp-cell'><div class='sp-lbl'>用水效益</div><div class='sp-val' style='color:#2ca25f'>${{effStr}}</div><div class='sp-unit'>$/英亩·英尺</div></div>
</div>
<div style='font-family:Arial,sans-serif;font-size:7.5px;color:#aaa;margin-bottom:3px'>
  <span style='color:${{tColor}}'>▬</span> 灌溉强度（af/ac）　<span style='color:#74c476'>┅</span> 降水变化
</div>
${{trendSvg}}
${{yrTable}}
<div style='font-family:Arial,sans-serif;font-size:7.5px;font-weight:700;color:#888;margin:6px 0 2px'>
  主要作物　<span style='font-weight:400;color:#aaa'>af/ac 灌溉强度 · ¢ 产值</span>
</div>
${{cropSvg}}
<div class='sp-ai'>
  <button class='sp-ai-btn' id='popup-ai-btn' onclick='generateAISummary()'>Gemini 2.5 Pro 分析 ✦</button>
  <div class='sp-ai-load' id='popup-ai-load'>分析中…</div>
  <div class='sp-ai-text' id='popup-ai-text'></div>
</div>
</div>`;
  document.getElementById('sp-hdr-title').textContent=p.name||p.abbr||'州级详情';
  document.getElementById('sp-body').innerHTML=html;
  document.getElementById('state-panel').style.display='flex';
  syncPanelLayout();
}}

</script>
</body>
</html>"""

    with open(OUT, "w", encoding="utf-8") as f:
        f.write(html)
    sz = os.path.getsize(OUT) // 1024
    print(f"✓ 生成 {OUT}  ({sz} KB)")

if __name__ == "__main__":
    main()
