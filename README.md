# Zerve Hackathon - LA Water Quality Root Cause Intelligence

## 项目简介

**LA Water Quality Root Cause Intelligence** 是一个基于因果推断与机器学习的交互式水质分析平台，专注于洛杉矶地区饮用水质量问题的根因溯源与行动指导。

用户输入地址或供水系统，平台自动生成一份完整的水质分析报告：污染现状、历史趋势、根本成因、健康风险评估，以及针对当前处境的个人行动建议。

**截止日期**：2026年4月29日

---

## 产品定位

### 核心用户问题

> 1. "我家的水安全吗？"
> 2. "如果不安全，根本原因是什么？"
> 3. "在这种处境下，我能做什么去改善我家的水质？"

现有工具（EWG、EPA ECHO、LADWP 官网）只能回答第一个问题——告诉你"污染物超标了"，但无法继续：
- 这个污染物**为什么**超标？是野火、老化管网、地质因素，还是监管失职？
- 这个问题是**偶发**还是**系统性**的？短期会好转吗？
- 作为普通居民，**现在能做什么**？换滤水器够吗？哪种滤芯针对这个污染物有效？

本项目用因果推断和 ML 方法回答第二个问题，并将分析结果转化为**可操作的个人行动建议**回答第三个问题。

---

## 竞品分析

### 现有产品及其局限

| 竞品 | 类型 | 能做什么 | 不能做什么 |
|------|------|---------|-----------|
| **EWG Tap Water Database** | 消费者查询 | 按供水系统查污染物列表；通用滤水器推荐 | 不解释为什么超标；无地址级别精度；无根因分析；静态历史数据 |
| **EPA ECHO** | 监管合规 | 设施违规执法记录；合规数据下载 | 面向监管机构，非消费者；只显示违规结果，不解释成因；无行动建议 |
| **MyWaterQuality.ca.gov** | 政府信息 | 有害藻华监测；野火后水质警告 | 无地址级查询；无根因分析；面向监管协调而非消费者 |
| **NRDC 工具** | 倡导机构 | 铅管地图；政策倡导资源 | 无消费者查询；无个性化建议；学术/倡导导向 |
| **Water Quality Portal** | 数据仓库 | 4.3亿条水质记录下载 | 纯数据仓库，零分析工具；面向研究者，非消费者 |
| **Aquasight** | B2B AI 平台 | 泵站预测性维护；管网异常检测；运营优化 | 仅服务水务公司，不面向消费者；预测≠因果，不解释污染来源 |
| **SkyTL** | B2B AI 预测 | 卫星+AI预测盐度/浊度/赤潮；提前2小时预警 | 仅服务水务公司；预测事件影响，不解释根本原因；无消费者接口 |
| **商业过滤器网站** (HomeWater/WaterVerge 等) | 电商导购 | 邮编查污染物 → 推荐滤水器 | 本质是卖货；推荐通用而非个性化；无根因解释 |

### 核心差异化：我们唯一做到的事

**所有竞品都只回答"是什么"，没有人回答"为什么"和"我该怎么办"。**

| 能力 | EWG | ECHO | Aquasight | SkyTL | **本项目** |
|------|:---:|:---:|:---:|:---:|:---:|
| 地址级精度查询 | ✗ | ✗ | ✗ | ✗ | ✅ |
| 解释污染根本成因 | ✗ | ✗ | ✗ | ✗ | ✅ |
| 因果推断（DiD/ITS/合成控制）| ✗ | ✗ | ✗ | ✗ | ✅ |
| SHAP 根因贡献度排序 | ✗ | ✗ | ✗ | ✗ | ✅ |
| 个性化行动建议（滤芯型号/沸水时机/投诉渠道）| ✗ | ✗ | ✗ | ✗ | ✅ |
| 环境正义量化（低收入社区系统性更差？）| ✗ | ✗ | ✗ | ✗ | ✅ |
| 消费者可读报告 | 部分 | ✗ | ✗ | ✗ | ✅ |

### Pitch 核心话术

- **vs. EWG**："EWG 告诉你水里有硝酸盐，我们告诉你为什么有——是上游5英里的农业径流叠加今年干旱造成的，这是 DiD 分析结果，置信区间 95%。"
- **vs. Aquasight/SkyTL**："他们做预测，我们做因果。预测能告诉你明天水会变差，因果告诉你是谁的错、能不能改变。"
- **vs. 所有竞品**："没有任何一个现有工具使用 DiD、ITS 或合成控制法来量化污染根因。我们是第一个把学术级因果推断带到消费者水质分析的产品。"

---

## 核心功能

### 1. 交互式水质地图

- 基于 LA County 5,659 个监测站的地理数据，渲染污染物热力图
- 支持按污染物类型、时间范围、供水系统筛选
- 叠加图层：2025 野火边界（Palisades + Eaton）、社区收入分布、供水系统边界
- 点击任意区域 → 触发该区域的完整根因分析报告

### 2. 因果推断引擎

针对关键事件量化其对水质的**真实因果影响**，而非简单相关性：

- **Difference-in-Differences (DiD)**：受灾区域 vs 对照区域、事件前后双重对比，隔离混淆因素
- **Synthetic Control Method**：为无法直接配对的地区构造合成对照组
- **Interrupted Time Series (ITS)**：检测政策干预节点前后趋势的结构性断点

核心问题示例：
- Palisades 大火（2025年1月）使受灾水务系统的苯/THMs 浓度**额外**上升了多少？
- 2022年 PFAS 新标准出台后，违规率下降了多少个百分点？

### 3. ML 根因分析

将多维度特征输入机器学习模型，识别影响水质的关键驱动因素：

**输入特征**：水源类型、供水系统规模、社区收入（Census）、历史违规次数、与野火区域的距离、管网年龄（1970前建房比例）、气候因素（NOAA）、CalEnviroScreen 分数、TRI 工业设施临近度

**模型方法**：
- **XGBoost + SHAP**：定量输出每个因素的贡献度，给出 Top-5 根因排序
- **随机森林**：非线性根因排序与交叉验证
- **K-Means / HDBSCAN 聚类**：识别具有相似污染模式的供水系统群组，帮助用户理解"我家属于哪类问题"
- **Prophet 时序预测**：基于 LADWP 21 年历史数据预测未来 5 年污染物趋势，标记潜在违规风险系统

### 4. 逐查询自动生成分析报告

每次查询（按地址 / 供水系统 / 污染物）自动生成结构化报告：

```
报告结构
├── 执行摘要（3句话结论）
├── 当前水质状态（vs 法定标准 / EWG 健康标准）
├── 历史趋势（时序图，最近5年）
├── 因果分析（主要事件对水质的量化影响）
├── 根因排序（SHAP 图，Top-5 影响因素）
├── 健康风险评级（低 / 中 / 高 / 极高）
└── 个人行动建议
```

### 5. 个人行动建议引擎

区别于所有现有工具的核心功能：基于根因分析结果，为居民生成**具体可执行的改善方案**。

**逻辑链：根因 → 风险 → 行动**

| 根因诊断 | 风险 | 行动建议 |
|---------|------|---------|
| 野火导致苯/THMs 升高 | 高 | 活性炭滤水器（NSF/ANSI 53）；避免热水直饮；关注供水局通知 |
| 铅管老化渗出 | 高 | 用水前放水30秒；NSF/ANSI 53 认证滤芯；申请免费铅检测 |
| 硝酸盐超标（农业污染）| 中 | 反渗透（RO）滤水器；婴幼儿禁用自来水 |
| PFAS 永久性化学物质 | 高 | RO 或活性炭+离子交换复合滤水器；避免加热饮用 |
| 微生物指标异常 | 极高 | 立即煮沸；使用瓶装水过渡；等待供水局通知 |
| 系统性合规问题 | 中 | 向 CA State Water Board 投诉；联系社区倡导组织 |

**行动建议分四层时间维度**：
- **今天**：临时防护措施，立即降低暴露风险
- **1-4周**：购置适合当前污染类型的滤水设备（含具体型号推荐）
- **1-3个月**：监测供水局整改进展，复查检测结果
- **长期**：系统性问题的投诉渠道与社区行动资源

### 6. 部署为可查询 API

```
GET /report?system_id=CA1910067          → 完整分析报告（含行动建议）
GET /actions?contaminant=benzene&cause=wildfire  → 针对性行动建议
GET /causal?event=palisades_fire&contaminant=benzene → 因果估计值
GET /map?contaminant=lead&date_range=2024-2025   → 地图热力数据
```

---

## 核心分析议题

### 议题一：野火因果冲击（主线）

**核心问题**：2025年1月 Palisades + Eaton 大火对 LA 饮用水质量的因果影响是多少？

- 方法：DiD（处理组 vs 对照组）+ ITS（时序断点）双重验证
- 处理组：火灾边界内供水系统（已获取 GeoJSON）
- 对照组：地理相近但未受灾的供水系统
- 关键污染物：苯、三卤甲烷（THMs）、铅、砷、浊度
- 控制变量：NOAA 气候数据（排除季节/干旱干扰）
- **深度扩展**：中介分析（Mediation Analysis）打通完整机制链——野火 → PM2.5 飙升（AQS）→ 酸性沉降 → 管道腐蚀 → 铅/铜溶出（WQP），量化 AQS 作为中介变量的贡献比例

### 议题二：环境正义因果检验

**核心问题**：低收入社区的水质更差，是因果关系还是混淆变量？

- 方法：工具变量（IV）控制内生性，隔离收入对水质的净效应
- 数据：EWG 200+ 系统 + Census 2,498 Tract 收入数据 + CA SAFER 违规记录 + 水系服务区边界（空间叠加）
- **深度扩展**：条件平均处理效应（CATE）——野火对低收入社区的冲击是否系统性大于富裕社区？工具：`causalml` XLearner / DRLearner（meta-learner 框架）
- **深度扩展**：回归断点（RD）——利用 CalEnviroScreen 政策阈值（补贴/干预边界）做准自然实验

### 议题三：20年趋势预测 + 异常检测

**核心问题**：主要污染物未来5年走势？历史上每次突变发生在何时、为何发生？

- 方法：Prophet 时序预测（趋势 + 季节性分解）
- 数据：LADWP PDF 报告（2004-2024，21份）pdfplumber/camelot 结构化提取
- **深度扩展**：CUSUM / Bayesian Changepoint Detection（`ruptures` 库）精确定位历史每个污染物浓度断点，叠加外部事件（野火、政策、基础设施升级）解释断点成因——比 ITS 更细粒度

### 议题四：供水系统风险评分 + 污染物级根因

**核心问题**：LA County 200+ 供水系统中，谁是系统性高风险？各污染物的根因是否不同？

- 方法：综合评分模型 + 分污染物 XGBoost + SHAP
- 数据：EWG 200+ JSON + CA SAFER violations + CalEnviroScreen + Census + TRI 设施临近度
- **深度扩展**：针对苯、铅、THMs、硝酸盐分别建模，每种污染物输出独立的 SHAP 根因排序——用户能看到"我家铅高"和"我家 THMs 高"的根因是完全不同的特征驱动的

---

## 技术架构

```
数据层
├── WQP API                   → 133,254 条检测记录，5,660 个监测站
├── EWG Scraper               → 200 个供水系统污染物数据
├── LADWP PDFs                → 21年历史年报（2004-2024）
├── EPA ECHO / SDWIS          → 672 个供水设施 + 违规执法记录
├── CA SAFER Risk Assessment  → 204 个 LA County 系统风险评估
├── CA Open Data 地下水        → 37,103 个 LA County 地下水站点
├── US Census ACS             → 2,498 Tract 人口/收入/住房年代
├── NOAA Climate              → 5站日气候数据（2023-2025）
├── CAL FIRE GeoJSON          → Palisades + Eaton 火灾边界 + 全量历史
├── CDC PLACES                → 2,474 个 Tract 健康结果（癌症/肾病等）
├── EPA TRI                   → 5,115 个工业有毒排放设施
├── EPA AQS                   → 33,827 条野火前后空气质量日均值
├── CA GeoTracker             → 14,379 个 LUST/UST 污染清理地点
├── CalEnviroScreen 4.0       → 2,343 个 Tract 综合环境负担指标
├── 水系服务区边界（GIS）      → 213 个供水系统地理边界多边形
├── LA Open Data              → 本地水质检测记录
└── CA 学校铅水采样            → 5,979 条 LA County 学校铅采样记录

分析层
├── 数据清洗与特征工程   (pandas, geopandas)
├── 空间叠加分析         (geopandas / shapely) ← Census Tract 映射到供水系统
├── 因果推断引擎         (statsmodels, linearmodels, pysyncon)
│   ├── DiD              ← 野火对水质的净因果效应
│   ├── ITS              ← 时序断点结构性检验
│   └── 合成控制法        ← LADWP 等大系统的反事实构造
├── ML 根因分析          (XGBoost + SHAP, scikit-learn)
│   ├── XGBoost + SHAP   ← Top-5 根因贡献度排序
│   ├── 随机森林          ← 非线性根因交叉验证
│   └── K-Means/HDBSCAN  ← 供水系统污染模式聚类
├── 时序预测             (Prophet) ← LADWP 21年历史 → 未来5年趋势
└── 环境正义量化         (statsmodels)
    ├── 工具变量（IV）    ← 分离收入对水质的净因果效应
    └── 回归断点（RD）    ← 利用政策边界做准自然实验

产品层（Zerve 平台）
├── 交互式地图           (folium / plotly)
├── 自动报告生成         (Zerve Conversational Reports)
└── REST API 部署        (Zerve Deployments)
```

---

## 分析方法与工具栈

### 因果推断方法

| 方法 | 场景 | 工具库 | 核心公式/思路 |
|------|------|--------|-------------|
| **DiD（差中差）** | 野火对水质的净因果效应 | `statsmodels`, `linearmodels` | `Y = β₀ + β₁·Treated + β₂·Post + β₃·(Treated×Post) + 控制变量`，β₃ 即因果效应 |
| **ITS（中断时序）** | 检测野火/政策节点的时序结构性断点 | `statsmodels` | `Y_t = β₀ + β₁·t + β₂·D_t + β₃·(t-T₀)·D_t`，β₂ 为水平突变，β₃ 为趋势突变 |
| **合成控制法** | LADWP 等大系统无好对照组时的反事实构造 | `pysyncon` | 用未受灾系统加权组合构造"没有野火时的 LADWP" |
| **中介分析** | 打通野火→空气→水质完整机制链 | `statsmodels`, `pingouin` | 分解总效应 = 直接效应 + 间接效应（经由 PM2.5 中介）|
| **工具变量（IV）** | 分离收入对水质的净因果效应，控制内生性 | `linearmodels` IV2SLS | 候选工具变量：历史红线划定、1970前建房比例 |
| **CATE（条件平均处理效应）** | 野火冲击对低收入/少数族裔社区是否更大 | `causalml` XLearner / DRLearner | Meta-learner 框架，输出异质性处理效应分布 |
| **回归断点（RD）** | 利用 CalEnviroScreen 政策阈值做准自然实验 | `rdrobust`（Python port）| 比较政策边界两侧水质差异，识别干预净效应 |

### 机器学习方法

| 方法 | 场景 | 工具库 |
|------|------|--------|
| **XGBoost + SHAP** | 根因贡献度排序，分污染物独立建模（苯/铅/THMs/硝酸盐）| `xgboost`, `shap` |
| **随机森林** | 非线性根因交叉验证，特征重要性对比 | `scikit-learn` |
| **K-Means / HDBSCAN 聚类** | 按污染模式对 200+ 供水系统分组 | `scikit-learn`, `hdbscan` |
| **Prophet 时序预测** | LADWP 21年数据 → 未来5年趋势预测 | `prophet` |
| **CUSUM / Bayesian Changepoint** | 精确定位历史污染物浓度突变时间点，对比外部事件 | `ruptures`, `bayesian_changepoint_detection` |
| **Isolation Forest** | 检测异常高值监测站/时间点（数据质量 + 异常预警）| `scikit-learn` |

### 空间分析方法

| 方法 | 场景 | 工具库 |
|------|------|--------|
| **空间连接（sjoin）** | Census Tract 属性映射到供水系统服务区（用水系边界 GeoJSON）| `geopandas` |
| **缓冲区分析** | 计算每个供水系统与 TRI 设施/GeoTracker 污染点的距离特征 | `geopandas`, `shapely` |
| **热力图渲染** | 污染物浓度地理分布，支持时间轴动画 | `folium`, `plotly` |
| **Moran's I 空间自相关** | 检验水质问题是否存在地理聚集性（非随机分布）| `pysal`, `esda` |

### 交互地图技术细节（NYT 社论风格）

受 [NYT 红线地图](https://www.nytimes.com/interactive/2020/08/24/climate/racism-redlining-cities-global-warming.html) 和 [NYT Hotter Hometown](https://www.nytimes.com/interactive/2023/us/climate-change-local-temperature.html) 启发，本项目实现同类叙事交互体验：

| NYT 原版技术 | 本项目实现方式 |
|-------------|--------------|
| **ai2html**（Illustrator → SVG 静态底图）| **CartoDB Positron** 瓦片底图（同款极简白底风格）|
| **D3.js scrollytelling**（滚动触发叙事）| **TimestampedGeoJson 时间滑块**（月度污染动画）|
| **Google Places Autocomplete**（位置切换 → 模板文字注入）| **GeoJSON click handler + 预计算 lookup table**（点击供水系统边界 → 右侧面板动态渲染）|
| **WebGL 自定义 globe** | **Folium + Leaflet.js**（轻量、无需 WebGL）|

**关键实现**：`system_panel_data.json`（212 个供水系统预聚合统计）在构建时通过空间连接生成，以 JS 对象形式内嵌入 HTML。点击任何供水系统边界即可触发面板，展示：人口规模、服务连接数、违规记录数、CalEnviroScreen 环境公正评分、贫困率、哮喘发病率及综合风险等级（Low / Moderate / High / Critical）。

### PDF 数据提取

| 工具 | 适用场景 |
|------|---------|
| `pdfplumber` | 提取 LADWP 年报中的文字表格（简单格式）|
| `camelot` | 提取复杂多列表格（需 Ghostscript）|
| `tabula-py` | Java 后端，适合扫描版 PDF 中的表格 |

### 参考研究方法论

- **DiD + 空间权重矩阵**：参考 [Currie et al. (2015)](https://www.nber.org/papers/w21049) 关于污染与健康的空间 DiD 设计
- **合成控制**：参考 Abadie & Gardeazabal (2003) 原始论文；`pysyncon` 文档
- **SHAP 解释因果性**：注意 SHAP 值是特征重要性而非因果效应，需结合 DiD 结果双重验证
- **环境正义 IV 设计**：参考 Banzhaf et al. (2019) *Environmental Justice: Establishing Causality*

---

## 数据源

### 水质核心数据

| 数据源 | 文件夹 | 实际规模 | 获取方式 |
|--------|--------|---------|---------|
| **Water Quality Portal (WQP)** | `data/raw_data/wqp/` | 133,254 条检测记录，5,660 个监测站 | `fetch_all.py wqp` |
| **EWG Tap Water Database** | `data/raw_data/ewg/` | 200 个供水系统（293个文件含 HTML）| `fetch_all.py ewg_all` |
| **LADWP PDF 报告** | `data/raw_data/ladwp_pdf/` | 21 份年报（2004-2024，共 106MB）| `fetch_all.py ladwp` |
| **California Open Data 地下水** | `data/raw_data/ca_open_data/` | 37,103 个 LA County 地下水站点 | `fetch_all.py ca`（filter: `gm_gis_county=LOS ANGELES`）|
| **CA SAFER 违规/风险评估** | `data/raw_data/ca_open_data/` | 204 个 LA County 供水系统风险评估（含违规类型）| CA Open Data resource `255887bb` |
| **EPA ECHO 供水设施** | `data/raw_data/epa_echo/` | 672 个 LA County 供水系统（SDWIS WATER_SYSTEM 表）| `fetch_all.py epa_echo`（ECHO SDW API 已下线，改用 Envirofacts）|
| **EPA SDWIS** | `data/raw_data/epa_sdwis/` | 75 个 LA 城区供水系统详情 | `fetch_all.py epa_sdwis` |
| **LA Open Data** | `data/raw_data/la_open_data/` | 本地水质检测记录 | `fetch_all.py la` |

### 根因分析特征数据

| 数据源 | 文件夹 | 实际规模 | 根因用途 | 获取方式 |
|--------|--------|---------|---------|---------|
| **US Census ACS 5-Year** | `data/raw_data/census/` | 2,498 Tract + 262 ZIP + 25 城市（人口/收入/种族/住房年代）| 环境正义；铅管代理变量（1970前建房比例）| `fetch_all.py census` |
| **NOAA Climate Data** | `data/raw_data/noaa/` | 5站日气候数据（2023-2025）+ 野火专项（2024-10~2025-03）| DiD 控制变量（排除季节/干旱干扰）| `fetch_all.py noaa`（需 `NOAA_API_KEY`）|
| **CA Fire Perimeter 2025** | `data/raw_data/fire_perimeters/` | Palisades + Eaton GeoJSON + CAL FIRE 全量历史边界（241MB）| 野火 DiD 处理组划定 | `fetch_all.py fire` |
| **CDC PLACES** | `data/raw_data/cdc_places/` | 98,959 条健康记录，2,474 个 Tract（癌症/肾病/糖尿病等）| 验证水质污染是否导致健康损害 | `fetch_all.py cdc` |
| **EPA TRI 工业设施** | `data/raw_data/epa_tri/` | 5,115 个 LA County 工业有毒排放设施（位置 + 化学品）| 化学污染工业根因；ML 特征（TRI 临近度）| `fetch_all.py tri`（排放量数据 `tri_releases.json` 因 EPA Envirofacts 接口下线暂缺）|
| **EPA AQS 空气质量** | `data/raw_data/aqs/` | 33,827 条野火前后日均记录（PM2.5/CO/NO2/Ozone/SO2）+ 年度统计 2020-2025 | 野火→空气→水质因果链中间环节 | `fetch_all.py aqs`（需 `AQS_EMAIL` + `AQS_KEY`）|
| **CA GeoTracker** | `data/raw_data/geotracker/` | 14,379 个 LA County LUST/UST 污染清理地点（苯/MTBE 等）| 石油衍生物污染根因 | CA Open Data resource `dc042197`（官方 API 被 Cloudflare 拦截，改用 CA Open Data）|
| **CalEnviroScreen 4.0** | `data/raw_data/ejscreen/` | 2,343 个 LA County Census Tract 综合环境负担指标 | ML 直接特征：PM2.5、Ozone、清理地点临近度、污染负担分、贫困率、哮喘率等 | CA Open Data resource `9a90474a` XLSX（替代 EPA EJScreen，后者网站已下线）|
| **水系服务区边界（GIS）** | `data/raw_data/water_system_boundaries/` | 213 个 LA County 供水系统地理边界多边形 | 空间分析核心：Census Tract → 供水系统空间叠加，计算每系统服务的人口/收入分布 | CA Water Board GIS FeatureServer |
| **CA 学校铅水采样** | `data/raw_data/school_lead/` | 5,979 条 LA County 学校铅采样记录（含超标标记、整改状态）| 铅污染根因验证；学校 PWS ID 可关联供水系统 | CA Open Data resource `5ebb2d68` |

---

## 数据文件说明

```
data/raw_data/
├── wqp/
│   ├── stations.csv                    # 5,660 监测站（经纬度、水源类型）
│   └── results.csv                     # 133,254 条检测结果（2020至今）
├── ewg/
│   ├── ladwp.json, burbank.json ...    # 6 个主要系统污染物详情 + HTML
│   ├── CA19xxxxx.json                  # 200 个系统完整数据
│   └── _la_water_systems.json          # 系统索引（200 个 CA19 系统）
├── ladwp_pdf/
│   └── LADWP_DWQR_2024.pdf ~ 2004.pdf # 21 份年度报告（共 106MB）
├── ca_open_data/
│   ├── groundwater_stations.json       # 37,103 个 LA County 地下水站点
│   └── drinking_water_violations.json  # 204 个 LA County 系统违规/风险评估（CA SAFER）
├── epa_echo/
│   ├── facilities.json                 # 672 个 LA County 供水设施（SDWIS）
│   └── violations.json                 # 204 个系统风险评估（含 MCL 违规/监测违规等）
├── epa_sdwis/
│   └── water_systems.json              # 75 个 LA 城区供水系统详情
├── la_open_data/
│   └── water_quality.json              # LA 本地水质检测记录
├── usgs/
│   ├── realtime.json                   # USGS 水文实时数据（空）
│   └── la_water_gauges.json            # 11,115 个 LA County 水文站（NWIS，含水质监测站）
├── census/
│   ├── la_census_tracts.json           # 2,498 Tract（人口/收入/种族/住房年代）
│   ├── la_zcta_income.json             # 262 ZIP code 收入数据
│   └── la_cities_demographics.json     # 25 个城市人口统计
├── noaa/
│   ├── stations.json                   # LA County 气象站列表
│   ├── daily_climate.json              # 5站 2023-2025 日气候数据
│   └── wildfire_period_climate.json    # 野火专项（2024-10 至 2025-03）
├── fire_perimeters/
│   ├── la_2025_fires_calfire.geojson   # Palisades + Eaton 精确边界
│   └── calfire_all_perimeters.geojson  # CAL FIRE 全量历史边界（241MB）
├── cdc_places/
│   ├── la_health_outcomes.json         # 98,959 条健康记录（长格式）
│   └── la_health_wide.json             # 2,474 个 Tract 宽表（每指标一列）
├── epa_tri/
│   ├── tri_facilities.json             # 5,115 个 LA County 工业有毒排放设施（坐标已从 DDMMSS 转为十进制）
│   ├── tri_releases.json               # ⚠ 空（EPA Envirofacts TRI_BASIC_DATA 表已下线）
│   └── superfund_npl.json              # 298 个 EPA SEMS Superfund 污染场地（含坐标）
├── aqs/
│   ├── stations.json                   # 13 个 LA County 空气质量监测站
│   ├── wildfire_period_aqi.json        # 33,827 条野火前后日均值（PM2.5/CO/NO2/Ozone/SO2）
│   └── annual_aqi.json                 # PM2.5/CO/NO2 年度统计（2020-2025）
├── geotracker/
│   └── geotracker_sites.json           # 14,379 个 LA County LUST/UST 污染地点
│                                       # 来源：CA Open Data（官方 API 被 Cloudflare 拦截）
├── ejscreen/
│   └── la_ejscreen_tracts.json         # 2,343 个 LA County Census Tract 综合环境指标
│                                       # 来源：CalEnviroScreen 4.0（替代 EPA EJScreen，后者已下线）
│                                       # 含：CES Score、PM2.5、Ozone、Diesel PM、Cleanup Sites、
│                                       #     Groundwater Threats、Pollution Burden、
│                                       #     Asthma、Low Birth Weight、Poverty、Housing Burden 等
├── water_system_boundaries/
│   └── la_water_system_boundaries.geojson  # 213 个 LA County 供水系统服务区地理边界
│                                           # 来源：CA Water Board GIS FeatureServer
│                                           # 用途：Census Tract → 供水系统空间叠加的关键桥梁
└── school_lead/
    ├── la_school_lead_sampling.json    # 5,979 条 LA County 学校铅水采样记录
    │                                   # 字段：学校名、PWS ID、采样日期、铅浓度、是否超标、整改状态
    │                                   # 来源：CA Open Data（CA State Water Board）
    └── la_school_lead_geocoded.json    # 1,164 所学校（US Census 批量地理编码，含铅浓度统计）
```

---

## Zerve Hackathon 对齐

**评分标准与项目对应**：

| 评分维度 | 权重 | 本项目如何满足 |
|---------|------|--------------|
| **Analytical Depth** | 35% | 因果推断（DiD/ITS/CATE/中介分析）+ XGBoost SHAP 根因排序，远超描述统计 |
| **End-to-End Workflow** | 30% | 数据抓取（15+源）→ 清洗 → 因果分析 → 交互地图 → 报告生成 → API 部署，全链路打通 |
| **Storytelling & Clarity** | 20% | 三条叙事线（野火归因 / 环境公平 / 行动建议），300字摘要 + 3分钟视频 |
| **Creativity & Ambition** | 15% | 首个将学术级因果推断应用到消费者饮用水分析的工具；14层交互地图 |

**提交 Checklist**：
- [ ] Public Zerve project（无报错可运行）
- [ ] 300字英文摘要
- [ ] 3分钟 demo 视频
- [ ] Social media post @ZerveAI
- [ ] ⭐ 部署为 API（bonus 加分）

---

## 分析报告框架

### 核心议题

> **2025年LA野火对饮用水水质的因果影响与环境公平性分析**
>
> 自然实验：Palisades + Eaton 野火（2025-01-07）作为外生冲击，结合因果推断量化野火的真实水质影响，并检验污染负担是否系统性落在弱势社区。

---

### 第一章：研究设计

**识别策略**
```
自然实验框架
  ├── 时间断点：2025-01-07（精确）
  ├── 地理边界：Palisades + Eaton 火区 GeoJSON（已获取）
  ├── 处理强度：按距火区距离分层（0-2km / 2-5km / 5-10km / 对照）
  └── 平行趋势验证：2020-2024年处理组 vs 对照组趋势一致性
```

**三个子问题**
1. 野火对哪些污染物有显著因果影响？效应持续多久？
2. 低收入/少数族裔社区是否系统性承担超出比例的污染暴露？
3. 水质恶化通过哪些路径传导至健康结果（水路径 vs 空气路径）？

---

### 第二章：数据与描述性分析

- 关键污染物时序图（苯、THMs、浊度、铅）：野火前后均值对比
- 处理组 vs 对照组平行趋势图（DiD 合法性基础）
- 14层交互地图：污染分布 × 人口特征 × 工业设施 × 健康结果空间叠加

---

### 第三章：因果分析

**3.1 DiD — 野火对水质的即时效应**

```
Y_ist = α_i + λ_t + β·(Treated_i × Post_t) + X_it·γ + ε_ist

β 即因果效应：野火使污染物浓度额外升高多少（ppb）
α_i：站点固定效应 | λ_t：月份固定效应
X_it：降水量、气温（NOAA）、历史 GeoTracker 密度
```

输出：β 点估计 + 95% 置信区间 + 事件研究图（event study plot）

**3.2 ITS — 污染物动态恢复路径**

```
Y_t = α + β₁t + β₂D_t + β₃(t - T₀)·D_t + ε_t

β₂：浓度水平突变（immediate impact）
β₃：斜率变化（恢复速度，负值 = 正在恢复）
```

输出：分污染物 ITS 图（含反事实曲线）+ 恢复半衰期估计

**3.3 空间回归 — 环境公平性因果检验**

```
Moran's I → 检验水质问题是否空间聚集

空间滞后模型（SLM）：
Violation_i = β₀ + β₁·Income_i + β₂·Minority_i + β₃·CES_score_i
            + β₄·log(距TRI距离) + β₅·log(距Superfund距离)
            + ρ·W·Violation_i + ε_i

工具变量（IV）：历史工业区划（外生）→ 当前TRI设施密度 → 水质违规
```

输出：控制混淆因素后，低收入社区额外超标概率 + 环境负担超额地图

**3.4 CATE — 谁最受影响？（因果森林）**

```python
from econml.dml import CausalForestDML
# 处理变量 T：是否在野火影响区
# 结果变量 Y：野火后污染物浓度变化
# 异质性变量 X：[收入, 少数族裔比例, CES评分, 距医院距离, 人口密度]
```

输出：CATE 分布图 + 因果特征重要性 + 最高风险子群画像

---

### 第四章：中介分析 — 污染传导路径

```
野火
  ├─→ 路径A（空气）：PM2.5升高 → 哮喘/COPD加重（AQS + CDC PLACES）
  └─→ 路径B（水质）：灰烬径流 → 浊度↑ → THMs/苯升高 → 癌症/肾病风险
```

输出：水路径 vs 空气路径各自贡献占比——水污染是独立于空气污染的额外伤害

---

### 第五章：儿童铅暴露专项

- 学校铅浓度 × 供水系统年龄 × 社区 CES 评分
- RD 设计：以 EPA 行动水平 15 ppb 为断点
- **输出**：高风险学校 Top 20 清单（学校名、地址、铅浓度、建议措施）

---

### 第六章：结论与政策建议

| 受众 | 建议 | 依据 |
|------|------|------|
| LADWP / 水务局 | 优先加密监测这5个供水系统 | DiD + ITS |
| LA County 公共卫生 | 20所学校需立即铅管检查 | 儿童铅专项 |
| 政策制定者 | 环境许可审批纳入 CES 评分权重 | 空间回归 |

---

## 开发计划

| 阶段 | 任务 | 状态 |
|------|------|------|
| ✅ 数据采集 | 15+数据源抓取、坐标修复、地理编码 | 完成 |
| ✅ 交互地图 | 14层 Folium 地图 + 时间滑块 + NYT风格点击面板 | 完成 |
| 🔄 分析第一章 | EDA + 平行趋势验证 + 描述统计图 | 进行中 |
| ⬜ 分析第三章 | DiD + 事件研究图 + ITS | 待开始 |
| ⬜ 分析第三章 | 空间回归 + CATE 因果森林 | 待开始 |
| ⬜ 分析第四章 | 中介分析（水路径 vs 空气路径）| 待开始 |
| ⬜ 分析第五章 | 儿童铅专项 Top 20 清单 | 待开始 |
| ⬜ 产品层 | Zerve 自动报告生成 + API 部署 | 待开始 |
| ⬜ 提交 | 300字摘要 + 3分钟视频 + Devpost | 截止 2026-04-29 |

---

## 环境配置

```bash
pip install -r requirements.txt

# 运行所有数据源
python src/fetch_all.py

# 只运行指定数据源
python src/fetch_all.py census noaa fire

# 全量 EWG（300+ 系统，约10分钟）
python src/fetch_all.py ewg_all

# 查看所有可用数据源
python src/fetch_all.py --list
```

所需 API Key（均免费）：
- `NOAA_API_KEY`：https://www.ncdc.noaa.gov/cdo-web/token
- `AQS_EMAIL` + `AQS_KEY`：https://aqs.epa.gov/data/api/signup?email=YOUR_EMAIL
