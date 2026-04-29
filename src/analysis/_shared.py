"""
公共模块：特征列定义 + parquet 加载
所有分析脚本通过 from _shared import * 使用
"""
import os
import pandas as pd

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_DIR   = os.path.join(BASE_DIR, "output", "analysis")
IMAGES_DIR   = os.path.join(BASE_DIR, "output", "images")
PARQUET_PATH = os.path.join(OUTPUT_DIR, "features.parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(IMAGES_DIR, exist_ok=True)

# ── 目标变量 ──────────────────────────────────────────────────────────
TARGET    = "crop_water_eff"     # 综合作物产值 / 估算用水量（$/af）
LOG_TARGET = "log_crop_water_eff"

# ── 特征分组 ──────────────────────────────────────────────────────────
CLIMATE_COLS = [
    # eto_avg_in 和 precip_avg_in 已移除：
    #   precip_deficit_in = eto - precip，三者共线（VIF=inf）
    "precip_deficit_in",    # 降水赤字（ETo - Precip），综合干旱压力
    "drought_intensity",    # 干旱强度（独立代理，VIF已大幅降低）
    "elevation_ft",         # 海拔（地形，不共线）
]

SOIL_COLS = [
    "awc_mean",        # 0-50cm 有效持水量（最重要土壤特征）
    "clay_pct",        # 黏粒含量（%）；sand_pct 与 clay_pct 高度负相关，保留一个
    "organic_matter",  # 有机质含量（%）
]

# 人为/可干预变量（核心研究对象）
HUMAN_COLS = [
    # 灌溉技术
    "centerpivot_ratio",        # 中心轴面积百分位排名（GEE，规避虚高问题）
    "irr_dependency",           # 灌溉依赖度
    # 农场结构
    "avg_farm_size_ac",         # 平均农场面积
    "farm_count",               # 农场数量
    "tenant_ratio",             # 租赁经营比例
    "crop_diversity_hhi",       # 作物多样性 HHI（越低越多样）
    "high_water_crop_share",    # 高耗水作物（水稻+干草）产值占比
    # 社会经济
    "median_income",            # 县域中位收入
    "poverty_rate",             # 贫困率
]

ALL_FEATURE_COLS = CLIMATE_COLS + SOIL_COLS + HUMAN_COLS


def load_features(min_target_coverage: float = 0.0) -> pd.DataFrame:
    """加载 features.parquet，过滤掉目标变量缺失的行"""
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(
            f"找不到 {PARQUET_PATH}\n"
            "请先运行：python src/analysis/01_build_features.py"
        )
    df = pd.read_parquet(PARQUET_PATH)
    if TARGET in df.columns:
        before = len(df)
        df = df[df[TARGET].notna()].copy()
        print(f"  加载 {len(df)} 县（过滤掉 {before-len(df)} 个目标变量缺失县）")
    avail_human  = [c for c in HUMAN_COLS  if c in df.columns]
    avail_climate = [c for c in CLIMATE_COLS if c in df.columns]
    avail_soil    = [c for c in SOIL_COLS    if c in df.columns]
    print(f"  特征：气候 {len(avail_climate)} / 土壤 {len(avail_soil)} / 人为 {len(avail_human)}")
    return df


MIN_COVERAGE = 0.30  # 覆盖率低于 30% 的特征自动排除


def available_cols(df: pd.DataFrame, col_list: list) -> list:
    dropped = _load_dropped_collinear()
    result = []
    for c in col_list:
        if c not in df.columns or c in dropped:
            continue
        coverage = df[c].notna().mean()
        if coverage < MIN_COVERAGE:
            print(f"  ⚠ 自动排除低覆盖特征：{c} ({coverage:.0%})")
            continue
        result.append(c)
    return result


def _load_dropped_collinear() -> set:
    """读取 02_eda.json 中被 VIF 迭代剔除的特征列表"""
    import json
    p = os.path.join(OUTPUT_DIR, "02_eda.json")
    if not os.path.exists(p):
        return set()
    try:
        return set(json.load(open(p)).get("dropped_collinear", []))
    except Exception:
        return set()

def to_numeric_df(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """把指定列强制转为数值（parquet 中部分列可能存为 object）"""
    return df[cols].apply(pd.to_numeric, errors="coerce")
