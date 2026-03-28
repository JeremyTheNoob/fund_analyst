"""
Pydantic 数据模型 — fund_quant_v2
定义整个 Pipeline 的数据契约（输入/输出 Schema）
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
from pydantic import BaseModel, ConfigDict, field_validator


# ============================================================
# 通用基础模型
# ============================================================

class BaseSchema(BaseModel):
    """所有 Schema 的基类，允许 pandas/numpy 类型"""
    model_config = ConfigDict(arbitrary_types_allowed=True)


# ============================================================
# Layer 1 — 原始数据层（data_loader 输出）
# ============================================================

class FundBasicInfo(BaseSchema):
    """基金基础信息"""
    symbol: str
    name: str
    type_raw: str = ""                  # 原始类型文本（如 "偏股混合型基金"）
    type_category: str = "equity"       # 标准化类型：equity/bond/index/sector/mixed/qdii/money/commodity
    establish_date: str = ""
    scale: str = ""
    company: str = ""
    manager: str = ""
    purchase_status: str = ""          # 申购状态（开放申购/暂停申购等）
    redeem_status: str = ""            # 赎回状态（开放赎回/暂停赎回等）
    min_purchase: float = 0.0           # 购买起点（元）
    latest_nav: float = 0.0             # 最新单位净值（非累计净值）
    benchmark_text: str = ""
    benchmark_parsed: Dict[str, Any] = {}
    fee_manage: float = 0.0
    fee_sale: float = 0.0
    fee_redeem: float = 0.0
    fee_custody: float = 0.0
    fee_total: float = 0.0


class NavData(BaseSchema):
    """净值历史数据"""
    symbol: str
    df: pd.DataFrame    # 列：date / nav / ret

    @property
    def empty(self) -> bool:
        """返回 DataFrame 是否为空"""
        return self.df.empty

    @field_validator("df")
    @classmethod
    def check_df(cls, v: pd.DataFrame) -> pd.DataFrame:
        required = {"date", "nav", "ret"}
        if not required.issubset(set(v.columns)):
            raise ValueError(f"NavData.df 缺少必要列，当前列：{list(v.columns)}")
        return v


class HoldingsData(BaseSchema):
    """持仓数据"""
    symbol: str
    stock_ratio: float = 0.0
    bond_ratio: float = 0.0
    cash_ratio: float = 0.0
    cb_ratio: float = 0.0           # 可转债占比
    top10_stocks: List[Dict] = []   # [{code, name, ratio, ...}]
    bond_details: List[Dict] = []   # [{name, type, rating, ratio}]
    asset_allocation: Dict[str, float] = {}


class FactorData(BaseSchema):
    """FF 因子数据"""
    df: pd.DataFrame    # 列：date / Mkt / SMB / HML / Short_MOM [/ RMW]


class BenchmarkData(BaseSchema):
    """基准收益率"""
    df: pd.DataFrame    # 列：date / bm_ret（权益类基准）
    description: str = ""


class BondYieldData(BaseSchema):
    """国债收益率 + 信用利差"""
    df: pd.DataFrame    # 列：date / yield_2y / yield_10y / credit_spread（真实数据）


# ============================================================
# Layer 2 — 清洗后数据层（processor 输出）
# ============================================================

class CleanNavData(BaseSchema):
    """清洗后的净值数据"""
    symbol: str
    df: pd.DataFrame        # 列：date / nav / ret（已去极端值 + 连续性验证）
    warnings: List[str] = []   # 清洗过程中发现的问题


class CleanBondData(BaseSchema):
    """清洗后的债券数据（BondDataPipeline 输出）"""
    symbol: str
    nav_df: pd.DataFrame
    yield_df: pd.DataFrame
    warnings: List[str] = []


# ============================================================
# Layer 3 — 计算结果层（engine 输出）
# ============================================================

class CommonMetrics(BaseSchema):
    """通用量化指标"""
    annualized_return: float = 0.0
    cumulative_return: float = 0.0
    volatility: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_duration: int = 0
    recovery_days: Optional[int] = None
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    skewness: float = 0.0
    kurtosis: float = 0.0
    monthly_win_rate: float = 0.0   # 月度胜率 vs 基准


class EquityMetrics(BaseSchema):
    """权益类分析结果"""
    common: CommonMetrics
    # FF 因子回归
    model_type: str = "ff3"         # capm / ff3 / ff5 / carhart
    alpha: float = 0.0              # 年化 Alpha
    beta: float = 0.0
    r_squared: float = 0.0
    factor_loadings: Dict[str, float] = {}  # SMB/HML/MOM/RMW 暴露
    # 信息比率 & 跟踪误差
    information_ratio: float = 0.0
    tracking_error: float = 0.0
    # Brinson 归因
    brinson: Dict[str, float] = {}  # allocation/selection/interaction
    # 风格
    style_drift_flag: bool = False
    rolling_beta_20d: List[float] = []
    rolling_beta_60d: List[float] = []
    # 雷达图评分
    radar_scores: Dict[str, float] = {}
    # 风格箱（Morningstar Style Box）
    style_box: Dict[str, Any] = {}  # {size: 1-3, value: 1-3, trajectory: [{quarter, size, value}]}
    # 综合评分（0-100）
    overall_score: float = 0.0
    score_grade: str = "B"


class BondMetrics(BaseSchema):
    """固收类分析结果"""
    common: CommonMetrics
    # 三因子回归
    alpha_bond: float = 0.0
    factor_loadings: Dict[str, float] = {}  # short_rate / long_rate / credit
    r_squared: float = 0.0
    # 久期 & 凸性
    duration: float = 0.0
    convexity: float = 0.0
    # WACS 信用评分
    wacs_score: float = 0.0
    credit_breakdown: Dict[str, float] = {}
    # HHI 集中度
    hhi: float = 0.0
    # 压力测试
    stress_results: List[Dict] = []
    # 信用利差历史（用于信用利差走势图）
    credit_spread_history: Optional[pd.DataFrame] = None  # 列：date / spread
    # 综合评分
    overall_score: float = 0.0
    score_grade: str = "B"


class IndexMetrics(BaseSchema):
    """指数/ETF 效率分析结果"""
    common: CommonMetrics
    # 效率指标
    tracking_error: float = 0.0
    tracking_error_annualized: float = 0.0
    information_ratio: float = 0.0
    correlation: float = 0.0
    # 成本拆解
    total_expense_ratio: float = 0.0
    cash_drag: float = 0.0
    rebalance_impact: float = 0.0
    enhanced_return: float = 0.0
    # 折溢价
    premium_discount_mean: float = 0.0
    premium_discount_std: float = 0.0
    premium_discount_grade: str = "良好"
    # 工具评分
    tool_score: float = 0.0
    tool_grade: str = "B"     # A+ / A / B / C / D


class ConvertibleBondMetrics(BaseSchema):
    """转债/固收+ 分析结果"""
    common: CommonMetrics
    # 识别结果
    cb_fund_type: str = "mixed"     # pure_bond / cb_fund / mixed / fixed_plus
    cb_confidence: str = "medium"   # high / medium / low
    # 权益暴露
    equity_exposure: float = 0.0    # 综合权益暴露 E_total
    delta_avg: float = 0.0
    premium_avg: float = 0.0
    # 估值
    iv_spread: float = 0.0
    ytm: float = 0.0
    bond_floor: float = 0.0
    # 股票 Alpha
    stock_alpha: float = 0.0
    # 综合评分
    overall_score: float = 0.0
    score_grade: str = "B"


# ============================================================
# Layer 4 — 报告层（reporter 输出）
# ============================================================

class FundReport(BaseSchema):
    """最终输出报告（结构化，供展示层使用）"""
    symbol: str
    fund_type: str
    basic: FundBasicInfo
    # 根据类型，以下字段至多一个非 None
    equity_metrics: Optional[EquityMetrics] = None
    bond_metrics: Optional[BondMetrics] = None
    index_metrics: Optional[IndexMetrics] = None
    cb_metrics: Optional[ConvertibleBondMetrics] = None
    # 文字报告（翻译层生成）
    text_report: Dict[str, str] = {}    # {headline, body, advice, risk_warning}
    # 性格标签
    tags: List[str] = []
    # 图表数据（字典，用于前端渲染）
    chart_data: Dict[str, Any] = {}
    # 清洗/分析过程警告
    warnings: List[str] = []


# ============================================================
# Pipeline 中间状态（内部传递用）
# ============================================================

class PipelineState(BaseSchema):
    """Pipeline 中间状态，贯穿整个分析流程"""
    symbol: str
    # Stage 1: 原始数据
    basic_info: Optional[FundBasicInfo] = None
    nav_raw: Optional[NavData] = None
    holdings: Optional[HoldingsData] = None
    factor_data: Optional[FactorData] = None
    benchmark: Optional[BenchmarkData] = None
    yield_data: Optional[BondYieldData] = None
    # Stage 2: 清洗后
    nav_clean: Optional[CleanNavData] = None
    # Stage 3: 计算结果
    metrics: Optional[Any] = None       # EquityMetrics | BondMetrics | IndexMetrics | ConvertibleBondMetrics
    # Stage 4: 最终报告
    report: Optional[FundReport] = None
    # 错误 & 警告
    errors: List[str] = []
    warnings: List[str] = []
