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
- **潜在洞见**：
  - `DetectionQuantitationLimitMeasure` 揭示隐藏污染——"未检出"不是"不存在"，是"低于设备检测限"。把所有未检出记录的检测下限可视化，可以发现某些流域的"清洁"记录其实来自精度极差的仪器
  - `HUCEightDigitCode` 流域聚合后按上下游排列站点，对同一污染物做时序对比，可以看到污染事件在流域里"传播"的时间差，进而识别中间污染源
  - `ResultStatusIdentifier` 的分布揭示数据生产的政治——对比不同机构在同一地点的 `Rejected` 率，如果某机构的异常高值系统性被标记为拒绝，本身就是一条线索
  - 多污染物"指纹"聚类比单指标更能反推污染源类型：BTEX 同升→石油泄漏；硝酸盐+磷酸盐同升→农业径流；铅+铜同升→管网腐蚀
  - 2020–2025 横跨疫情（工业骤降）和2025野火两个天然对照实验，足够做 ITS 分析
  - 监测站空间密度与社区收入高度相关——所有基于 WQP 的"水质地图"都在结构性低估弱势社区的污染程度
  - **未被充分挖掘的字段层**：
    - `HydrologicEvent = "Storm"`（704条）：暴雨期间专项采样，可直接量化"月度采样漏掉了多少"——同站点暴雨 vs 常规样本浓度比值是最直接的污染负荷估算
    - `HydrologicEvent = "Affected by fire"`（47条，全部为2022年）：2025野火影响尚未被标注，必须用空间叠加而非依赖此字段
    - `ActivityMediaName = "Soil"`（1,852条）：土壤样本含PCBs、有机氯农药等遗留工业化学品，与地表水浓度对比可估算渗出时间延迟
    - `ActivityMediaName = "Biological"`（1,855条）：物种计数数据，是水质变化的生物积分指标，敏感物种消失早于化学指标超标
    - `ResultSampleFractionText`：Dissolved（9,151条）vs Total（4,888条）——溶解态是生物可利用的真实毒性，而现有报告几乎全用 Total，系统性高估或低估健康风险
    - `ActivityDepthHeightMeasure`（2,599条，深度0.3—695英尺）：垂直剖面数据，表层与底层污染物浓度差异巨大，取水口深度决定了居民实际喝到的水质
    - 质控样本（3,032条）：Field Replicate 的误差率直接量化每个站点的测量可靠性，但从未被用于数据质量评估

### CA SAFER 风险评估（饮用水违规）
- **颗粒度**：供水系统级（每系统一行快照）
- **时间范围**：最新快照（含 `FAILING_START_DATE` 历史记录）
- **规模**：204个 LA County 供水系统
- **关键字段**：`WATER_SYSTEM_NUMBER` `SYSTEM_NAME` `POPULATION` `SERVICE_CONNECTIONS` `FINAL_SAFER_STATUS`（Failing / At-Risk / Not At-Risk 等5类） `CURRENT_FAILING` `PRIMARY_MCL_VIOLATION` `PRIMARY_ANALYTES` `WATER_QUALITY_SCORE` `CALENVIRO_SCREEN_SCORE` `MHI`（中位家庭收入）
- **潜在洞见**：
  - `FAILING_START_DATE` 揭示失败持续时长比状态本身更重要——长期 Failing 的系统不是在等待修复，而是被系统性放弃了；与 `MHI` 叠加后会发现持续时间与社区收入强相关
  - `WATER_QUALITY_SCORE` 高但 `FINAL_SAFER_STATUS` 为 "Not At-Risk" 的系统：水质实际差但综合评级掩盖了问题，评级体系的加权方式值得质疑
  - `PRIMARY_ANALYTES` 的地理聚集性——同类污染物在空间上聚集往往指向共同的区域性污染源（地质/农业/工业），而非各系统的独立问题
  - 按 `SERVICE_CONNECTIONS` 分组看水质分布：规模小的系统普遍更差，但规模不是原因，而是决定了系统有没有能力去修复——这是两件不同的事
  - `CALENVIRO_SCREEN_SCORE` 和 `MHI` 同时存在可以分离两种不平等：是因为穷还是因为周围工业设施多导致水质差？前者需要补贴，后者需要执法，政策含义完全不同
  - 交叉验证陷阱：系统从 Failing 名单消失不一定是水质改善——用 WQP 实测浓度验证，若浓度未下降则"改善"是虚假的，可能只是监测频率降低

### CalEnviroScreen 4.0（EJScreen）
- **颗粒度**：Census Tract
- **时间范围**：静态快照（v4.0，2021年基准）
- **规模**：2,343个 LA County Tract
- **关键字段**：`ces_40_score`（综合环境负担分） `pm25` `ozone` `diesel_pm` `drinking_water` `lead` `pesticides` `tox_release` `cleanup_sites` `groundwater_threats` `asthma` `low_birth_weight` `poverty` `unemployment` `housing_burden`（含各指标百分位数）
- **潜在洞见**：
  - `ces_40_score` 是加权合成指数，权重本身是政治决定——找出"污染暴露极高但综合分不高"的 Tract，这些是被合成方式系统性低估的社区（通常因社会经济指标相对好而稀释了污染权重）
  - `drinking_water` 分项与 WQP/SAFER 实测数据对比：差距最大的 Tract 是 CES 作为政策工具最容易失灵的地方
  - `asthma` 和 `low_birth_weight` 是累积暴露的滞后信号——`pm25` 已改善但哮喘率还高，说明清洁了环境不等于清洁了身体；滞后效应大小可用 v3.0 vs v4.0 历史版本估算
  - 各指标百分位解耦后的"异常"Tract 比相关性更有信息量：`lead` 高但 `cleanup_sites` 低 → 铅来源是老管网/油漆而非工业，干预逻辑完全不同
  - 2021年基准被2025野火打破——受灾 Tract 的实际污染负担已与 CES 分数严重脱节，用 CES 分配野火救灾资源会系统性低估高收入受灾 Tract
  - `ces_40_score` 百分位是全州相对排名——LA 内部"最好"的 Tract 在全州仍可能处于中等偏差水平，用内部排名做公平性分析会低估整个 LA 相对于农村地区的系统性优势

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
- **潜在洞见**：
  - `arithmetic_mean` vs `first_max_value` 的分裂：日均值低不代表安全，峰值才是哮喘患者的真实风险——能区分"稳定性污染"和"爆发性污染"
  - 13个站点覆盖整个 LA County，空间插值不是测量而是猜测；监测站集中在易达地区，工业走廊和东 LA 低收入社区在监测盲区
  - 野火时间窗口从 2024-10 开始有基准期，2025-01-07 点火——CO 升高说明不完全燃烧，NO₂ 升高说明建筑燃烧，SO₂ 升高说明工业设施被点燃；五种污染物组合能识别野火烧到了什么
  - `aqi` 与 `arithmetic_mean` 换算非线性——健康风险对应浓度，不对应 AQI 数字，用 `arithmetic_mean` 做健康分析更准确
  - 野火烟雾特征：PM2.5 急升 + CO 升 + Ozone 下降（烟雾阻挡紫外线抑制光化学反应）；只有 PM2.5 升而其他不一致，来源可能是尘暴或工业事故
  - 没有异常信号的站点有时比有异常信号的站点更值得怀疑——地形可能把烟雾完全引向别处

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
- **潜在洞见**：
  - 排放量缺失时，设施密度（每平方公里设施数）本身就是有效的 ML 特征——设施存在意味着潜在泄漏、运输事故、挥发排放，这些不会出现在官方申报里
  - `standardized_parent_company` 聚合后揭示企业网络：少数集团控制大量设施且集中在同几个低收入社区，污染是系统性布局而非分散个体行为
  - TRI 设施位置与 CalEnviroScreen `tox_release` 指标对比——两者来源不同，差距最大的设施是数据渠道失灵的地方
  - 与 GeoTracker 500m 空间匹配：TRI 设施附近有清理案例的组合，是历史排放变成地下污染的直接证据链，比自我申报数据更诚实
  - TRI 只覆盖年排放量超过阈值的大型设施——大量小型设施的累积排放更难管控，用 TRI 密度推断污染风险会系统性漏掉工业走廊边缘地带
  - 把 TRI 设施位置与卫星 NO₂ 数据空间匹配：若周围实测值远高于历史申报，说明自我报告机制存在系统性低报

### 加州污染场地追踪系统（GeoTracker）
- **颗粒度**：污染场地级
- **时间范围**：1966 — 2026（`BEGIN_DATE`）
- **规模**：14,379个 LA County 场地
- **关键字段**：`CASE_TYPE`（LUST/UST/Cleanup Program Site等14类） `STATUS` `BEGIN_DATE` `POTENTIAL_CONTAMINANTS_OF_CONCERN` `DISCHARGE_SOURCE` `CALENVIROSCREEN4_SCORE` `DISADVANTAGED_COMMUNITY` `CALWATER_WATERSHED_NAME`
- **潜在洞见**：
  - `BEGIN_DATE` 跨越60年，按十年分组可以看到污染爆发年代对应什么经济事件——污染跟随资本流动，不是随机发生的
  - `STATUS` 为"已关闭"不等于"已清洁"——关闭标准是"风险可接受"而非"污染物归零"；找出关闭10年以上但地块上方现在是学校/公园/住宅的案例
  - `CALWATER_WATERSHED_NAME` 连接地下污染和地表水体：把同一流域的 GeoTracker 案例密度与 WQP 地表水检测对比，可以追踪苯/MTBE 从地下渗入地表水的时间延迟
  - `DISADVANTAGED_COMMUNITY` 直接可做环境正义分析：对比弱势 vs 非弱势社区的案例密度、关闭率、从 BEGIN_DATE 到关闭的平均耗时——关闭更快可能意味着标准更低，关闭更慢意味着资源更少
  - `DISCHARGE_SOURCE` 和 `POTENTIAL_CONTAMINANTS_OF_CONCERN` 两字段不一致指向填报错误或真正的混合污染，比单字段分析更有价值
  - 14,379个案例绝大多数是 LUST（地下储油罐泄漏）；把 LUST 密度地图与现有商业数据叠加，找出前加油站地块现在是什么用途（餐厅、日托中心？）
  - GeoTracker 是"被发现的污染"而非"存在的污染"——GeoTracker 密度低但 CalEnviroScreen `cleanup_sites` 分高的 Tract，是被系统性忽视的污染地带

### EPA 废水排放许可证系统（NPDES）
- **颗粒度**：设施级（含月均流量时序）
- **规模**：1,508个设施（其中86个有月均流量数据）
- **关键字段**：`CWPName` `SourceID` `FacLat` `FacLon` `CWPActualAverageFlowNmbr`（月均流量MGD） `MasterExternalPermitNmbr` `PercentPeopleOfColor` `FacPopDen`
- **潜在洞见**：
  - 86/1,508 的数据覆盖缺口本身是信息——用设施规模、许可证类型、所在流域预测其余1,422个设施的排放量，缺失可以被推断
  - `CWPActualAverageFlowNmbr` 是月均值，极端事件被平滑——月均流量与 NOAA 月降雨量高度相关的设施是合流制管网（雨污混流），暴雨时直接溢流进河道
  - `PercentPeopleOfColor` 和 `FacPopDen` 自带环境正义字段：有色人种比例高的社区周边设施违规更多但处罚更少，是执法不平等的直接证据
  - 月均流量时序 × 下游 WQP 水质时间差：排放量变化往往外生（设备维修、季节性停产），是最干净的因果识别之一
  - `MasterExternalPermitNmbr` 频繁变更可能意味着违规后以新主体重新注册——"新设施"实际上是旧设施换了法律外壳继续运营
  - NPDES 是合法排放的证据，不是污染证据——把许可排放量与 WQP 下游实测浓度对比，超出许可量能解释的部分指向存在于数据库之外的非法排放点

### 加州农药使用记录（CDPR PUR 2023）
- **颗粒度**：单次施药记录（地块 × 日期 × 化学品）
- **时间范围**：2023全年
- **规模**：153,048条记录（9,313条含COMTRS空间坐标）
- **关键字段**：`chemname` `lbs_chm_used` `acre_treated` `applic_dt` `site_name`（作物/场地类型） `comtrs`（Township-Range-Section空间编码） `product_name` `applic_cnt`
- **潜在洞见**：
  - 只有6%的记录（9,313/153,048）有精确空间坐标——哪些化学品空间记录最差？高毒性农药的申报精度往往最低，申报精度和监管压力相关
  - `lbs_chm_used` / `acre_treated` = 单位面积施药强度，比总量更有意义；强度异常高的地块对应特定病虫害爆发或违规操作，而非仅仅是大农场
  - `applic_dt` 的季节峰值 × NOAA 降雨数据：LA 降雨集中在冬季，秋季施药后接降雨的"高风险径流窗口"里的施药量占比是关键指标
  - 同一地块同日施用的化学品组合做关联分析——混合物毒性远超单品，在任何单指标监测里都看不到
  - `site_name` 里高尔夫球场和城市绿化的单位面积施药量往往高于农田，且紧邻住宅区
  - 2023年施药强度高的 COMTRS 地块 × 附近地下水站点5年趋势：农药在地下水里半衰期多年，2023年的施药会在未来几年才体现在水质里
  - CDPR 施药密度低但 CalEnviroScreen `pesticides` 分高的区域，是无法被任何申报系统捕捉的非法/未申报施药地带

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
- **潜在洞见**：
  - `Observation_Date` 到 `Advisory_Date` 的时间差是响应速度的度量——饮用水源地的响应是否比娱乐水体更快？如果差异不显著，说明监管框架在保护基础设施而不是人
  - `Bloom_Size` × `Drinking_Water_Source` 的组合是直接健康风险指标——大型藻华发生在饮用水源但只发布娱乐预警，说明监管框架在保护游泳者而不是喝水的人
  - 10年时序可以验证藻华是否在加速、季节是否在延长——结合 NOAA 气温数据，直接验证气候变化对饮用水安全的影响
  - `Water_Body_Name` 反复出现的水体有结构性富营养化问题——把这些水体与 CDPR 农药施用数据和 NPDES 排放点做空间溯源
  - `Case_Status` 关闭速度 × 所在社区收入：关闭快可能意味着标准低，而非水质真正改善
  - 把 FHAB 水体与 WQP 监测站分布叠加——两个数据库都没有覆盖的水体，藻华风险完全不可见，但可能通过地下水或径流影响下游饮用水源

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
