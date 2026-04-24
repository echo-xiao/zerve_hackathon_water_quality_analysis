# Zerve Hackathon - LA Water Quality Root Cause Intelligence

## 项目简介

**LA Water Quality Root Cause Intelligence** 是一个基于因果推断与机器学习的水质分析平台，专注于洛杉矶地区饮用水质量的根因溯源与行动指导。

用户输入地址或供水系统，平台自动生成完整分析报告：污染现状、历史趋势、根本成因、健康风险评估，及个人行动建议。

**截止日期**：2026年4月29日

---

## 水质领域的关键洞见

这些反直觉洞见直接影响了分析设计：

| 洞见 | 核心内容 | 对本项目的影响 |
|------|---------|-------------|
| **时间分辨率决定可见性** | 月度采样会错过暴雨冲刷事件——这类事件携带全年污染负荷的80% | 优先连续监测数据；ITS 捕捉突变 |
| **缺失数据本身有信息** | 贫困/偏远地区监测站少，"数据缺失"指向被系统性低估的污染 | 监测密度作为环境正义指标 |
| **消毒副产品悖论** | 氯消毒产生的DBPs毒性可能强于原始污染物；合规不等于安全 | 分析THMs等DBPs，不只关注原始污染物 |
| **地下水延迟效应** | 地下水污染延迟数十年才进入河流；今天治理对应几十年前的排放 | DiD/ITS 需要拉长时间窗口 |
| **生态系统有阈值** | 污染超过临界点后系统突变到新稳态，难以逆转，非线性退化 | 识别跳变点，不假设线性恢复 |
| **磷积累的长尾** | 即使今天停止排放，土壤中积累的磷还会持续释放数十年 | 治理效果评估需要长时间序列 |
| **Shifting Baseline** | 每一代人用已退化的状态作为"正常"基准，感知不到累积退化 | 对比地质尺度基准，不只看近十年 |
| **土地利用预测水质** | 流域农业/不透水面比例对水质的预测力有时超过直接监测 | 将土地利用纳入 ML 特征 |
| **生物指标做时间积分** | 底栖生物群落整合过去数月水质信息，比化学瞬时采样更诚实 | 生物指标作为校验化学指标的参照 |
| **水是信息载体（eDNA）** | 水中环境DNA可重建整个流域生态系统信息，颠倒监测逻辑 | 扩展数据源思路 |
| **测量改变了被测量的对象** | 工厂会刻意选址在监测盲区；"无数据"和"水质好"不是同一件事 | 监测站空间分布本身作为分析变量 |
| **合规周期制造污染脉冲** | 供水系统知道检测时间表，检测前加大处理力度——月度数据合规，检测间隙未必 | 质疑月度采样的代表性；关注检测时序规律 |
| **治理投入越多发现问题越多** | 加大监测投入的地区违规记录反而更多——不是水质更差，而是检测出来了 | 违规记录需结合监测密度归一化解读 |
| **水质差会制造贫困** | 铅暴露在认知发育期（0-6岁）造成IQ损失，15-20年后影响区域劳动力收入 | 因果方向可能双向；不能仅假设贫困→水质差 |
| **治理的J曲线效应** | 更换铅管施工扰动会使铅浓度短暂飙升，做正确的事先变得更糟 | 数据中的"治理后恶化"可能是修复过程的正常阶段 |

---

## 产品定位

### 核心用户问题

> 1. "我家的水安全吗？"
> 2. "如果不安全，根本原因是什么？"
> 3. "我能做什么去改善？"

现有工具（EWG、EPA ECHO、LADWP官网）只能回答第一个问题。本项目用因果推断回答第二个，并将结果转化为可操作建议回答第三个。

### 核心差异化

**所有竞品都只回答"是什么"，没有人回答"为什么"和"我该怎么办"。**

| 能力 | EWG | ECHO | Aquasight | SkyTL | **本项目** |
|------|:---:|:---:|:---:|:---:|:---:|
| 地址级精度查询 | ✗ | ✗ | ✗ | ✗ | ✅ |
| 解释污染根本成因 | ✗ | ✗ | ✗ | ✗ | ✅ |
| 因果推断（DiD/ITS/合成控制）| ✗ | ✗ | ✗ | ✗ | ✅ |
| 根因贡献度排序（SHAP）| ✗ | ✗ | ✗ | ✗ | ✅ |
| 个性化行动建议 | ✗ | ✗ | ✗ | ✗ | ✅ |
| 环境正义量化 | ✗ | ✗ | ✗ | ✗ | ✅ |

---

## 核心功能

**1. 交互式水质地图**
18层可叠加图层，MapLibre GL JS + OpenFreeMap Positron，NYT Palatino 配色，底部时间轴动画。点击供水系统边界 → 浮动信息面板（人口/违规记录/风险等级）。

**2. 因果推断引擎**
DiD / ITS / 合成控制法，量化野火、政策、设施对水质的真实因果影响，而非简单相关性。

**3. ML 根因分析**
XGBoost + SHAP，输出每个供水系统/ZCTA 的 Top-5 根因贡献度排序。当前 Ridge 模型：CV R²=0.534，19个特征，Top驱动：PFAS超标倍数（+130.4）、铅风险（-11.5）、贫困率（-11.2）。

**4. 自动分析报告**
按地址/供水系统/污染物查询，生成结构化报告：执行摘要 → 现状 → 趋势 → 根因（SHAP图）→ 健康风险 → 行动建议。

**5. 个人行动建议**

| 根因诊断 | 行动建议 |
|---------|---------|
| 野火苯/THMs 升高 | 活性炭滤水器（NSF/ANSI 53）；避免热水直饮 |
| 铅管老化 | 用水前放水30秒；申请免费铅检测 |
| 硝酸盐超标（农业径流）| 反渗透（RO）滤水器；婴幼儿禁用自来水 |
| PFAS 永久性化学物质 | RO 或活性炭+离子交换复合滤水器 |
| 微生物指标异常 | 立即煮沸；使用瓶装水；等待供水局通知 |
| 系统性合规问题 | 向 CA State Water Board 投诉 |

**6. REST API 部署**
```
GET /report?system_id=CA1910067
GET /actions?contaminant=benzene&cause=wildfire
GET /causal?event=palisades_fire&contaminant=benzene
GET /map?contaminant=lead&date_range=2024-2025
```

---

## 分析议题

### 主线：野火因果冲击

**核心问题**：2025年1月 Palisades + Eaton 大火对 LA 饮用水的因果影响是多少？

两条污染路径：
- **路径A（水源污染）**：灰烬径流 → 水库 → 处理厂进水变差，影响相对短期
- **路径B（管网内部）**：高温使管道PVC/HDPE涂层热解 → 释放苯/VOC → 渗入管内水体。**火灭后数月仍持续释放，处理厂无法截获**（2018年 Camp Fire 实测：家中苯浓度高达40,000 ppb，MCL为5 ppb）

经GeoJSON空间叠加确认，11个供水系统服务区与2025野火边界直接重叠（LADWP、Pasadena Water & Power等）。

识别策略：ITS 断点 = 2025-01-07；处理组 = 11个受灾系统；对照组 = LA 其余260+系统；控制变量：NOAA气候数据

深度扩展：中介分析打通完整机制链——野火 → PM2.5（AQS）→ 酸性沉降 → 管道腐蚀 → 铅/铜溶出（WQP）

### 支线议题

| 议题 | 方法 | 数据状态 |
|------|------|---------|
| 环境正义：低收入社区水质是否系统性更差？| IV + CATE（causalml XLearner）| ✅ |
| 20年趋势预测 + 历史突变点检测 | Prophet + Bayesian Changepoint（ruptures）| ✅ |
| 供水系统风险评分 + 根因分解 | Ridge + 标准化系数贡献度 | ✅ |
| NPDES 废水排放 → 下游水质 | 空间滞后 + 准DiD | ✅ |
| 农药径流 → 地下水硝酸盐 | 缓冲区空间回归（0-1km / 1-3km / >3km）| ✅ |
| 儿童铅暴露专项（学校）| RD（1986年禁管令为断点）| ✅ |
| 污染暴露 → 健康结果 | 截面空间回归 + IV | ✅ |

### 因果方法选择逻辑

```
观测到相关性
      ↓
能找到"准实验"（外生冲击）吗？
      ↓
冲击是时间点？→ DiD / ITS
冲击是地理边界/阈值？→ RD
找不到自然实验？→ IV
      ↓
因果链是什么？→ Mediation（总效应 = 直接 + 间接）
异质性效应？→ CATE（causalml meta-learner）
```

---

## 技术架构

```
数据层（22个数据源）
├── 水质核心：WQP(13万条) / EWG(200系统) / LADWP PDF(21年) / SDWIS(672设施)
├── 根因特征：Census / NOAA / CAL FIRE / CalEnviroScreen / TRI / AQS / GeoTracker
└── 扩展：NPDES / CDPR农药 / CDC PLACES / 学校铅采样 / USGS / HAB / PFAS

分析层
├── 因果推断：statsmodels / linearmodels / pysyncon
│   └── DiD → ITS → 合成控制 → 中介分析 → IV → RD → CATE
├── ML 根因：XGBoost+SHAP / RandomForest / Prophet / CUSUM / Isolation Forest
└── 空间分析：geopandas / Moran's I / sjoin / 缓冲区回归

产品层（Zerve 平台）
├── 交互地图：MapLibre GL JS + OpenFreeMap Positron（18图层，WebGL加速）
├── 报告生成：Zerve Conversational Reports
└── API 部署：Zerve Deployments
```

**18个地图图层**：野火边界 / 供水系统边界 / WQP污染热力图 / WQP月度时间轴 / TRI工业设施 / GeoTracker污染点 / 学校铅含量 / 地下水 / 空气质量(AQS) / 环境公正(EJScreen) / CDC健康结果 / Superfund / USGS水文站 / HAB藻华 / 海滩水质 / NPDES废水排放 / 农药使用 / PFAS污染点

---

## 数据源

### 水质监测门户（Water Quality Portal / WQP）
- **颗粒度**：单次采样记录（站点 × 日期 × 污染物）
- **时间范围**：2020-01-01 — 2025-12-30
- **规模**：133,254条检测记录 / 5,660个监测站
- **关键字段**：`MonitoringLocationIdentifier` `ActivityStartDate` `CharacteristicName`（污染物名，数百种） `ResultMeasureValue` `ResultMeasure/MeasureUnitCode` `ResultStatusIdentifier` `DetectionQuantitationLimitMeasure/MeasureValue`
- **站点字段**：`MonitoringLocationTypeName`（水源类型） `HUCEightDigitCode`（流域编码） `LatitudeMeasure` `LongitudeMeasure`

### CA SAFER 风险评估（饮用水违规）
- **颗粒度**：供水系统级（每系统一行快照）
- **时间范围**：最新快照（含 `FAILING_START_DATE` 历史记录）
- **规模**：204个 LA County 供水系统
- **关键字段**：`WATER_SYSTEM_NUMBER` `SYSTEM_NAME` `POPULATION` `SERVICE_CONNECTIONS` `FINAL_SAFER_STATUS`（Failing / At-Risk / Not At-Risk 等5类） `CURRENT_FAILING` `PRIMARY_MCL_VIOLATION` `PRIMARY_ANALYTES` `WATER_QUALITY_SCORE` `CALENVIRO_SCREEN_SCORE` `MHI`（中位家庭收入）

### CalEnviroScreen 4.0（EJScreen）
- **颗粒度**：Census Tract
- **时间范围**：静态快照（v4.0，2021年基准）
- **规模**：2,343个 LA County Tract
- **关键字段**：`ces_40_score`（综合环境负担分） `pm25` `ozone` `diesel_pm` `drinking_water` `lead` `pesticides` `tox_release` `cleanup_sites` `groundwater_threats` `asthma` `low_birth_weight` `poverty` `unemployment` `housing_burden`（含各指标百分位数）

### 美国人口普查 ACS 5年估算（US Census ACS）
- **颗粒度**：Census Tract
- **时间范围**：静态快照（ACS 5-Year Estimate）
- **规模**：2,498个 LA County Tract
- **关键字段**：`total_population` `median_household_income` `per_capita_income` `population_below_poverty` `hispanic_or_latino` `race_*`（7个种族分类） `edu_bachelors` `housing_built_*`（完整建造年代分层，最早至1939年前） `median_home_value`

### EPA 空气质量监测系统（AQS）
- **颗粒度**：日均值（站点级）
- **时间范围**：2024-10-01 — 2025-03-31（野火专项）
- **规模**：33,827条记录，13个监测站
- **污染物**：PM2.5、CO、NO₂、Ozone、SO₂
- **关键字段**：`date_local` `arithmetic_mean` `first_max_value` `aqi` `parameter` `units_of_measure` `latitude` `longitude` `local_site_name`

### NOAA 气候数据
- **颗粒度**：日值（站点级）
- **时间范围**：2023-01-01 — 2024-12-31
- **规模**：5个气象站
- **关键字段**：`date` `datatype`（TMAX/TMIN/PRCP/SNOW/AWND等） `value` `station`

### 加州野火边界数据（CAL FIRE）
- **颗粒度**：单次火灾多边形
- **时间范围**：历史全量 + 2025年 Palisades/Eaton 精确边界
- **关键字段**：`FIRE_NAME` `ALARM_DATE`（点火日） `CONT_DATE`（扑灭日） `GIS_ACRES` `CAUSE` `YEAR_`

### EPA 有毒物质排放清单（TRI）
- **颗粒度**：设施级（静态位置）
- **规模**：5,115个 LA County 设施
- **关键字段**：`tri_facility_id` `facility_name` `fac_latitude` `fac_longitude` `parent_co_name` `standardized_parent_company`
- **注意**：排放量字段（`tri_releases`）因 EPA Envirofacts 接口下线暂缺

### 加州污染场地追踪系统（GeoTracker）
- **颗粒度**：污染场地级
- **时间范围**：1966 — 2026（`BEGIN_DATE`）
- **规模**：14,379个 LA County 场地
- **关键字段**：`CASE_TYPE`（LUST/UST/Cleanup Program Site等14类） `STATUS` `BEGIN_DATE` `POTENTIAL_CONTAMINANTS_OF_CONCERN` `DISCHARGE_SOURCE` `CALENVIROSCREEN4_SCORE` `DISADVANTAGED_COMMUNITY` `CALWATER_WATERSHED_NAME`

### EPA 废水排放许可证系统（NPDES）
- **颗粒度**：设施级（含月均流量时序）
- **规模**：1,508个设施（其中86个有月均流量数据）
- **关键字段**：`CWPName` `SourceID` `FacLat` `FacLon` `CWPActualAverageFlowNmbr`（月均流量MGD） `MasterExternalPermitNmbr` `PercentPeopleOfColor` `FacPopDen`

### 加州农药使用记录（CDPR PUR 2023）
- **颗粒度**：单次施药记录（地块 × 日期 × 化学品）
- **时间范围**：2023全年
- **规模**：153,048条记录（9,313条含COMTRS空间坐标）
- **关键字段**：`chemname` `lbs_chm_used` `acre_treated` `applic_dt` `site_name`（作物/场地类型） `comtrs`（Township-Range-Section空间编码） `product_name` `applic_cnt`

### 美国疾控中心健康数据（CDC PLACES）
- **颗粒度**：Census Tract
- **规模**：2,474个 LA County Tract
- **关键字段（与水质相关）**：`casthma`（哮喘率） `cancer`（癌症率） `diabetes` `copd` `chd`（冠心病） `stroke` `obesity` `bphigh`（高血压） `mhlth`（心理健康不佳天数） `ghlth`（整体健康自评）

### 加州学校饮用水铅含量采样（CA DDW）
- **颗粒度**：单次采样记录（学校 × 采样点 × 日期）
- **时间范围**：2017-02-22 — 2020-01-15
- **规模**：5,979条记录（1,164所学校已地理编码）
- **关键字段**：`school_name` `district` `pws_id` `sample_date` `result`（铅浓度） `rpt_unit` `action_level_exceedance` `ale_follow_up_action` `water_system_name`

### 加州有害藻华监测（CA FHAB）
- **颗粒度**：藻华事件级
- **时间范围**：2016 — 2026（`Observation_Date`）
- **规模**：212个 LA County 藻华事件
- **关键字段**：`Water_Body_Name` `Observation_Date` `Advisory_Date` `Case_Status` `Bloom_Size` `Water_Body_Type` `Drinking_Water_Source` `Reported_Advisory_Types`

### 供水系统边界（CA Water Board GIS）
- **颗粒度**：供水系统多边形
- **规模**：273个系统（213原始 + 60个1km缓冲区）
- **关键字段**：`SABL_PWSID` `WATER_SYSTEM_NAME` `BOUNDARY_TYPE` `POPULATION` `SERVICE_CONNECTIONS` `OWNER_TYPE_CODE`

### 加州地下水监测（CA DWR）
- **颗粒度**：监测站级
- **规模**：37,103个 LA County 地下水站点

### 其他补充数据源
- **EWG**：200个供水系统污染物超标记录（静态快照）
- **EPA ECHO**：672个供水设施合规记录
- **LADWP PDF**：21份年度水质报告（2004–2024），通过 pdfplumber/camelot 结构化提取

---

## 运行说明

```bash
pip install -r requirements.txt

# 下载数据
python src/build/fetch_all.py           # 全部数据源
python src/build/fetch_all.py --list    # 查看可用数据源

# 构建地图数据
python src/build/build_aqs_zipcode.py   # 首次运行（Census TIGER ZCTA边界）
python src/build/build.py               # WQP + GeoJSON → output/data/

# 分析 pipeline（Zerve Notebook）
python src/analysis/01_build_features.py   # ZCTA特征宽表 → zcta_features.csv
python src/analysis/02_shap_analysis.py    # RandomForest + SHAP → zcta_rootcause.json
python src/analysis/03_its_analysis.py     # 野火ITS因果分析 → 时序断点图
python src/analysis/04_ej_analysis.py      # 环境正义分析 → 箱线图 + 政策建议

# 本地预览
cd output && python -m http.server 8000
# 访问 http://localhost:8000/water_quality_map.html
```

所需 API Key（均免费）：
- `NOAA_API_KEY`：https://www.ncdc.noaa.gov/cdo-web/token
- `AQS_EMAIL` + `AQS_KEY`：https://aqs.epa.gov/data/api/signup?email=YOUR_EMAIL

---

## Zerve Hackathon 对齐

| 评分维度 | 权重 | 如何满足 |
|---------|------|---------|
| **Analytical Depth** | 35% | DiD/ITS/CATE/中介分析 + XGBoost SHAP，远超描述统计 |
| **End-to-End Workflow** | 30% | 数据抓取 → 因果分析 → 交互地图 → 报告 → API，全链路打通 |
| **Storytelling & Clarity** | 20% | 三条叙事线：野火归因 / 环境公平 / 行动建议 |
| **Creativity & Ambition** | 15% | 首个将学术级因果推断应用到消费者水质分析的工具 |

**提交 Checklist**：
- [ ] Public Zerve project（无报错可运行）
- [ ] 300字英文摘要
- [ ] 3分钟 demo 视频
- [ ] Social media post @ZerveAI
- [ ] ⭐ 部署为 API（bonus 加分）

**开发进度**：

| 阶段 | 状态 |
|------|------|
| 数据采集（22个数据源）| ✅ 完成 |
| 交互地图（18层）| ✅ 完成 |
| EDA + 平行趋势验证 | 🔄 进行中 |
| DiD + ITS 因果分析 | ⬜ 待开始 |
| ML 根因（SHAP）| ⬜ 待开始 |
| 中介分析 + CATE | ⬜ 待开始 |
| Zerve 报告 + API 部署 | ⬜ 待开始 |
| 提交（摘要 + 视频）| ⬜ 截止 2026-04-29 |
