"""
schema_v2.py — 资产维度数据模型（v2 重构）

按资产维度（股票/利率债/信用债/可转债）组织分析指标，
不再按基金类型（权益/债券/指数）分路。

与 schema.py 的关系：
- schema.py 保留，作为旧代码的数据契约
- schema_v2.py 用于新的报告生成流程
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict


class BaseSchema(BaseModel):
    """所有 Schema 的基类，允许 pandas/numpy 类型"""
    model_config = ConfigDict(arbitrary_types_allowed=True)


# ============================================================
# 资产概览
# ============================================================

class FundAssetOverview(BaseSchema):
    """基金资产概览"""
    fund_name: str
    fund_code: str
    fund_type: str                    # 原始类型文本（如"混合型-偏股"）
    manager_name: str = ""
    manager_start_date: str = ""      # 经理任职日期
    manager_days: int = 0             # 任职天数
    aum: float = 0.0                  # 规模（亿元）
    inception_date: str = ""          # 成立日期
    fee_total: float = 0.0            # 总费率
    benchmark_text: str = ""          # 业绩比较基准

    # 资产结构
    asset_allocation: Dict[str, float] = {}  # {"股票": 0.6, "债券": 0.3, "现金": 0.1, "其他": 0.0}

    # 持有标记（决定是否展示对应板块）
    has_stock: bool = False
    has_rate_bond: bool = False       # 利率债（国债/政金债）
    has_credit_bond: bool = False     # 信用债（含城投/地产）
    has_cb: bool = False              # 可转债

    # 债券细分比例
    rate_bond_ratio: float = 0.0      # 利率债占净值比例
    credit_bond_ratio: float = 0.0    # 信用债占净值比例
    cb_ratio: float = 0.0             # 可转债占净值比例


# ============================================================
# 股票维度指标
# ============================================================

class StockAssetMetrics(BaseSchema):
    """股票资产维度指标"""

    # === 拟买入模式 ===
    tri_deviation: Optional[float] = None        # 全收益脱水（含权TRI偏离度 %）
    pe_percentile: Optional[float] = None        # 估值水位（PE_TTM 历史分位 %）
    weighted_peg: Optional[float] = None         # 业绩匹配度（加权PEG）
    erp: Optional[float] = None                  # 股权溢价 ERP = 1/PE - 10年债收益率
    ldays: Optional[float] = None                # 流动性穿透（Top10 变现天数）
    blackswan_loss: Optional[float] = None       # 黑天鹅压测（PE回归10%分位预期跌幅 %）

    # === 已持有模式（额外） ===
    style_drift_r2: Optional[float] = None       # 风格漂移（R²）
    style_consistency_r2: Optional[float] = None # 风格一致性（R²矩阵对角均值）
    alpha_trend: Optional[List[Dict]] = None     # 滚动Alpha趋势 [{date, alpha}]
    excess_drawdown: Optional[float] = None      # 超额回撤（最大）
    pe_extreme: Optional[bool] = None            # P/E 是否处于极端值（>95% 或 <5%）
    stop_profit_signal: Optional[str] = None     # 分批止盈信号描述

    # === 通用（两种模式共享） ===
    alpha_annual: Optional[float] = None         # 年化 Alpha（相对基金合同基准）
    r_squared: Optional[float] = None            # 风格 R²
    beta: Optional[float] = None                 # Beta

    # === Top10 持仓明细 ===
    top10_details: Optional[List[Dict[str, Any]]] = None
    # 每项: {code, name, ratio, pe_ttm, pb, avg_amount_20d, ldays}

    # === 图表数据 ===
    alpha_trend_df: Optional[pd.DataFrame] = None  # {date, alpha} 用于画图
    r2_matrix_df: Optional[pd.DataFrame] = None     # R²矩阵 用于画图


# ============================================================
# 利率债维度指标
# ============================================================

class RateBondMetrics(BaseSchema):
    """利率债指标"""

    # === 拟买入 ===
    tri_deviation: Optional[float] = None        # 全收益脱水
    duration: Optional[float] = None             # 加权久期 Duration（年）
    term_spread: Optional[float] = None          # 期限利差 Spread（10Y-2Y, bp）
    dv01: Optional[float] = None                 # 利率敏感度 DV01（万一价值，bp）
    drawdown_recovery_days: Optional[int] = None # 最大回撤修复天数

    # === 已持有（额外） ===
    max_drawdown: Optional[float] = None         # 最大回撤
    institution_ratio: Optional[float] = None    # 底层机构占比

    # === 通用 ===
    yield_curve_shape: Optional[str] = None      # 收益率曲线形态：陡峭/平坦/倒挂


# ============================================================
# 信用债维度指标
# ============================================================

class CreditBondMetrics(BaseSchema):
    """信用债指标"""

    # === 拟买入 ===
    ytm: Optional[float] = None                  # 静态收益率 YTM
    avg_rating: Optional[str] = None             # 平均信用评级（如"AAA"/"AA+"）
    institution_ratio_change: Optional[float] = None  # 机构持有比例变化（最近两期）

    # === 已持有（额外） ===
    default_warning: Optional[str] = None        # 违约预警描述
    reinvestment_risk: Optional[str] = None      # 再投资风险描述

    # === 通用 ===
    credit_spread_latest: Optional[float] = None  # 最新信用利差（bp）
    credit_spread_trend: Optional[str] = None     # 利差走势：收窄/走阔/平稳

    # === 图表数据 ===
    credit_spread_df: Optional[pd.DataFrame] = None  # {date, spread} 用于画图


# ============================================================
# 可转债维度指标
# ============================================================

class CBMetrics(BaseSchema):
    """可转债指标"""

    # === 拟买入 ===
    conv_premium_rate: Optional[float] = None    # 加权转股溢价率 %
    bond_floor_premium: Optional[float] = None   # 加权纯债溢价率 / 债底溢价率 %
    avg_conv_price: Optional[float] = None       # 加权转债价格（元）

    # === 已持有（额外） ===
    ytm: Optional[float] = None                  # 加权 YTM
    is_double_high: Optional[bool] = None        # 价格/溢价率"双高"
    double_high_list: Optional[List[Dict]] = None  # 双高转债列表
    bond_floor_failed: Optional[bool] = None     # 债底保护失效（YTM转负）
    blackswan_cb_loss: Optional[float] = None    # 股债双杀模拟跌幅 %

    # === 动态处方 ===
    stock_like_warning: Optional[str] = None     # 类股化提示（价格>130且溢价率>30%）


# ============================================================
# 多基金组合指标
# ============================================================

class PortfolioMetrics(BaseSchema):
    """多基金组合分析指标"""

    # 行业与风格重合度
    corr_matrix: Optional[pd.DataFrame] = None       # R² 相关性矩阵
    industry_concentration: Optional[Dict[str, float]] = None  # 行业集中度
    overlap_heatmap: Optional[pd.DataFrame] = None   # 持仓热力图数据
    top_overlap_stocks: Optional[List[Dict]] = None  # 重叠最高的Top10股票

    # 综合流动性（股票）
    weighted_ldays: Optional[float] = None           # 加权 Ldays
    avg_institution_ratio: Optional[float] = None    # 底层机构占比平均值

    # 组合久期与利率敏感度（债券）
    weighted_dv01: Optional[float] = None            # 加权 DV01
    weighted_recovery_days: Optional[float] = None   # 加权回撤修复天数

    # 单基金列表
    fund_list: Optional[List[Dict[str, Any]]] = None  # [{code, name, fund_type}]


# ============================================================
# 完整报告
# ============================================================

class AssetDimensionReport(BaseSchema):
    """资产维度完整报告

    路由逻辑：
    - 模式 mode: "buy"（拟买入）/ "hold"（已持有）/ "portfolio"（多基金组合）
    - 1只基金：buy 或 hold 模式
    - 2+只基金：portfolio 模式
    """
    mode: str = "buy"                        # buy / hold / portfolio
    overview: Optional[FundAssetOverview] = None
    stock: Optional[StockAssetMetrics] = None
    rate_bond: Optional[RateBondMetrics] = None
    credit_bond: Optional[CreditBondMetrics] = None
    cb: Optional[CBMetrics] = None
    portfolio: Optional[PortfolioMetrics] = None  # 仅 portfolio 模式
    warnings: List[str] = []

    # 基金不满一年的替代建议
    fund_too_new: bool = False
    alternative_funds: Optional[List[Dict[str, str]]] = None
    # [{code, name, manager}]

    # 多基金时各单基金报告
    sub_reports: Optional[Dict[str, "AssetDimensionReport"]] = None
