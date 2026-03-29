"""
统一报告分发器（Pipeline 主控） — fund_quant_v2
数据获取 → 标准化清洗 → 模块化计算 → 统一报告分发

这是整个系统的核心调度器，非 Streamlit UI 入口。
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from models.schema import (
    FundReport, PipelineState,
    FundBasicInfo, NavData, HoldingsData,
)
from data_loader.equity_loader import (
    load_basic_info, load_nav, load_stock_holdings,
    load_ff_factors, build_benchmark, load_bond_index,
)
from data_loader.bond_loader import (
    load_bond_holdings, load_treasury_yields,
    load_bond_composite_index, load_cb_holdings_with_details,
)
from data_loader.index_loader import (
    load_etf_nav_and_price, load_benchmark_index,
    load_etf_holdings_ratios, infer_benchmark_code,
)
from processor.data_cleaner import (
    clean_nav_data, BondDataPipeline,
)
from engine.equity_engine import run_equity_analysis
from engine.bond_engine import run_bond_analysis
from engine.index_engine import run_index_analysis
from engine.convertible_bond_engine import run_cb_analysis
from reporter.translator import (
    generate_text_report, generate_equity_tags,
)
from reporter.chart_gen import generate_chart_data

logger = logging.getLogger(__name__)

# 不支持的基金类型及提示
UNSUPPORTED_TYPES = {
    "money":     "该基金为货币基金，主要适合现金管理，暂不支持量化分析。",
    "qdii":      "该基金为 QDII 基金（境外投资），当前版本暂不支持境外市场分析。",
    "commodity": "该基金为商品基金（黄金/原油/REITs），当前版本暂不支持商品类分析。",
}


# ============================================================
# 主分析函数（外部调用入口）
# ============================================================

def analyze_fund(
    symbol: str,
    years: int = 5,
    since_inception: bool = False,
    verbose: bool = False,
) -> FundReport:
    """
    全流程基金分析入口。

    Pipeline:
        1. load_basic_info → 基金基本信息 + 类型识别
        2. 类型检查 → 不支持类型直接返回友好提示
        3. load_nav → 净值数据
        4. load_holdings → 持仓数据
        5. clean_nav → 数据清洗
        6. load_factors / load_yields → 因子/利率数据
        7. run_*_analysis → 模块化计算
        8. generate_text_report + generate_chart_data → 报告生成

    Args:
        symbol: 基金代码（6位，如 "000001"）
        years:  拉取净值历史年数（默认 5 年）
        since_inception: 是否从成立日起分析
        verbose: 是否打印调试信息

    Returns:
        FundReport（结构化报告，含文字 + 图表数据）
    """
    state = PipelineState(symbol=symbol)

    if verbose:
        logger.setLevel(logging.DEBUG)

    # =========================================================
    # Stage 1: 基金基本信息
    # =========================================================
    try:
        basic = load_basic_info(symbol)
        state.basic_info = basic
        logger.info(f"[analyze_fund] {symbol} 基本信息加载完成: {basic.name} ({basic.type_category})")
    except Exception as e:
        msg = f"基金基本信息获取失败: {e}"
        logger.error(f"[analyze_fund] {msg}")
        state.errors.append(msg)
        return _error_report(symbol, msg)

    # =========================================================
    # Stage 2: 不支持类型检查
    # =========================================================
    fund_type = basic.type_category
    if fund_type in UNSUPPORTED_TYPES:
        return _unsupported_report(basic, UNSUPPORTED_TYPES[fund_type])

    # =========================================================
    # Stage 3: 净值数据
    # =========================================================
    try:
        nav_raw = load_nav(
            symbol, years=years, since_inception=since_inception,
        )
        state.nav_raw = nav_raw

        if nav_raw.df.empty:
            return _error_report(symbol, "净值数据为空，无法进行分析")

        logger.info(f"[analyze_fund] {symbol} 净值数据: {len(nav_raw.df)} 行")
    except Exception as e:
        return _error_report(symbol, f"净值数据获取失败: {e}")

    # =========================================================
    # Stage 4: 持仓数据（并行策略）
    # =========================================================
    try:
        if fund_type in ("bond", "convertible_bond"):
            holdings = load_bond_holdings(symbol)
        else:
            holdings = load_stock_holdings(symbol)
        state.holdings = holdings
    except Exception as e:
        logger.warning(f"[analyze_fund] {symbol} 持仓数据获取失败（使用默认值）: {e}")
        holdings = _default_holdings(symbol, fund_type)
        state.holdings = holdings

    # 自动识别转债基金（cb_ratio > 30%）
    if fund_type in ("bond", "mixed") and holdings.cb_ratio > 0.30:
        fund_type = "convertible_bond"
        logger.info(f"[analyze_fund] {symbol} 自动识别为转债基金（cb_ratio={holdings.cb_ratio:.1%}）")

    # =========================================================
    # Stage 5: 数据清洗
    # =========================================================
    clean_nav = clean_nav_data(nav_raw)
    state.nav_clean = clean_nav

    if clean_nav.df.empty:
        return _error_report(symbol, "净值数据清洗后为空")

    if clean_nav.warnings:
        state.warnings.extend(clean_nav.warnings)

    # =========================================================
    # Stage 6 & 7: 因子/利率数据 + 模块化计算
    # =========================================================
    start_str, end_str = _get_date_range(clean_nav.df)

    try:
        if fund_type in ("equity", "mixed", "sector"):
            report = _run_equity_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str, fund_type)

        elif fund_type == "bond":
            report = _run_bond_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str)

        elif fund_type == "index":
            report = _run_index_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str)

        elif fund_type == "convertible_bond":
            report = _run_cb_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str)

        else:
            # 未知类型 fallback 到权益
            logger.warning(f"[analyze_fund] {symbol} 未知类型 {fund_type}，fallback 到权益分析")
            report = _run_equity_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str, "equity")

    except Exception as e:
        logger.exception(f"[analyze_fund] {symbol} 分析引擎异常: {e}")
        return _error_report(symbol, f"分析引擎异常: {e}")

    # 添加清洗警告
    if state.warnings:
        report.warnings = list(state.warnings)

    return report


# ============================================================
# 各类型 Pipeline
# ============================================================

def _run_equity_pipeline(
    symbol: str,
    basic: FundBasicInfo,
    clean_nav,
    holdings: HoldingsData,
    start_str: str,
    end_str: str,
    fund_type: str,
) -> FundReport:
    """权益类 Pipeline"""
    # 因子数据
    factors  = load_ff_factors(start_str, end_str)
    # 基准
    benchmark = build_benchmark(basic, start_str, end_str)

    # 计算
    metrics = run_equity_analysis(
        nav=clean_nav,
        factors=factors,
        holdings=holdings,
        benchmark=benchmark,
        fund_type=fund_type,
    )

    # 性格标签
    tags = generate_equity_tags(metrics)

    # 构建报告
    # 只有当基金有明确的业绩基准时，才添加基准数据到图表中
    has_benchmark = (
        basic.benchmark_parsed and
        basic.benchmark_parsed.get("components") and
        len(basic.benchmark_parsed.get("components", [])) > 0
    )

    chart_data = {
        "nav_df": clean_nav.df,
        "holdings": {
            "top10_stocks": holdings.top10_stocks,
            "stock_ratio": holdings.stock_ratio,
            "bond_ratio": holdings.bond_ratio,
            "cash_ratio": holdings.cash_ratio,
            "cb_ratio": holdings.cb_ratio,
        },
    }
    if has_benchmark:
        chart_data["benchmark_df"] = benchmark.df

    report = FundReport(
        symbol=symbol,
        fund_type=fund_type,
        basic=basic,
        equity_metrics=metrics,
        tags=tags,
        chart_data=chart_data,
    )

    report.text_report = generate_text_report(report)
    report.chart_data.update(generate_chart_data(report))
    return report


def _run_bond_pipeline(
    symbol: str,
    basic: FundBasicInfo,
    clean_nav,
    holdings: HoldingsData,
    start_str: str,
    end_str: str,
) -> FundReport:
    """固收类 Pipeline"""
    # 国债收益率 + 信用利差（真实数据，修复旧 Bug）
    yield_data = load_treasury_yields(start_str, end_str)
    bond_idx   = load_bond_composite_index(start_str, end_str)

    # 债券数据流水线清洗
    pipeline = BondDataPipeline(symbol)
    clean_bond = pipeline.run(clean_nav, yield_data, bond_idx)

    # 计算
    metrics = run_bond_analysis(
        clean_data=clean_bond,
        holdings=holdings,
        fund_type="bond",
    )

    # 信用利差历史数据（用于图表）
    credit_spread_history = None
    if yield_data is not None and not yield_data.df.empty:
        credit_spread_history = yield_data.df[["date", "credit_spread"]].copy()
        # 计算平滑后的利差
        credit_spread_history["spread_smooth"] = credit_spread_history["credit_spread"].rolling(
            window=5, min_periods=1
        ).mean()

    # 将信用利差历史数据添加到 BondMetrics
    if metrics is not None:
        metrics.credit_spread_history = credit_spread_history

    report = FundReport(
        symbol=symbol,
        fund_type="bond",
        basic=basic,
        bond_metrics=metrics,
        tags=[],
        chart_data={
            "nav_df": clean_nav.df,
            "holdings": {
                "top10_stocks": [],
                "stock_ratio": holdings.stock_ratio,
                "bond_ratio": holdings.bond_ratio,
                "cash_ratio": holdings.cash_ratio,
                "cb_ratio": holdings.cb_ratio,
                "bond_details": holdings.bond_details,
            },
        },
    )

    report.text_report = generate_text_report(report)
    report.chart_data.update(generate_chart_data(report))
    return report


def _run_index_pipeline(
    symbol: str,
    basic: FundBasicInfo,
    clean_nav,
    holdings: HoldingsData,
    start_str: str,
    end_str: str,
) -> FundReport:
    """指数/ETF Pipeline"""
    # ETF 折溢价数据
    etf_data = None
    try:
        etf_data = load_etf_nav_and_price(symbol)
    except Exception:
        pass

    # 标的指数
    bm_code = infer_benchmark_code(basic.name, basic.benchmark_parsed)
    bm_df   = load_benchmark_index(bm_code, start_str, end_str)

    # ETF 持仓（现金占比）
    etf_holdings = load_etf_holdings_ratios(symbol)

    # 计算
    metrics = run_index_analysis(
        nav=clean_nav,
        holdings=etf_holdings,
        basic=basic,
        benchmark_df=bm_df,
        etf_data=etf_data,
    )

    report = FundReport(
        symbol=symbol,
        fund_type="index",
        basic=basic,
        index_metrics=metrics,
        tags=[],
        chart_data={"nav_df": clean_nav.df},
    )

    report.text_report = generate_text_report(report)
    report.chart_data.update(generate_chart_data(report))
    return report


def _run_cb_pipeline(
    symbol: str,
    basic: FundBasicInfo,
    clean_nav,
    holdings: HoldingsData,
    start_str: str,
    end_str: str,
) -> FundReport:
    """转债/固收+ Pipeline"""
    # 可转债持仓详情
    cb_df = None
    try:
        cb_df = load_cb_holdings_with_details(symbol)
    except Exception:
        pass

    # 计算
    metrics = run_cb_analysis(
        nav=clean_nav,
        holdings=holdings,
        basic=basic,
        cb_holdings_df=cb_df,
    )

    report = FundReport(
        symbol=symbol,
        fund_type="convertible_bond",
        basic=basic,
        cb_metrics=metrics,
        tags=[],
        chart_data={
            "nav_df": clean_nav.df,
            "holdings": {
                "top10_stocks": [],
                "stock_ratio": holdings.stock_ratio,
                "bond_ratio": holdings.bond_ratio,
                "cash_ratio": holdings.cash_ratio,
                "cb_ratio": holdings.cb_ratio,
                "bond_details": holdings.bond_details,
            },
        },
    )

    report.text_report = generate_text_report(report)
    report.chart_data.update(generate_chart_data(report))
    return report


# ============================================================
# 辅助函数
# ============================================================

def _get_date_range(nav_df: pd.DataFrame) -> tuple[str, str]:
    """从净值 DataFrame 提取起止日期字符串"""
    dates = pd.to_datetime(nav_df["date"]).sort_values()
    start = dates.min().strftime("%Y%m%d")
    end   = dates.max().strftime("%Y%m%d")
    return start, end


def _default_holdings(symbol: str, fund_type: str) -> HoldingsData:
    """持仓数据获取失败时的默认值"""
    defaults = {
        "equity": dict(stock_ratio=0.85, bond_ratio=0.10, cash_ratio=0.05),
        "bond":   dict(stock_ratio=0.02, bond_ratio=0.88, cash_ratio=0.10),
        "mixed":  dict(stock_ratio=0.55, bond_ratio=0.35, cash_ratio=0.10),
        "index":  dict(stock_ratio=0.95, bond_ratio=0.00, cash_ratio=0.05),
        "sector": dict(stock_ratio=0.90, bond_ratio=0.05, cash_ratio=0.05),
    }
    d = defaults.get(fund_type, defaults["equity"])
    return HoldingsData(symbol=symbol, **d)


def _error_report(symbol: str, message: str) -> FundReport:
    """生成错误报告"""
    basic = FundBasicInfo(symbol=symbol, name=symbol)
    return FundReport(
        symbol=symbol,
        fund_type="unknown",
        basic=basic,
        text_report={
            "headline":     f"【{symbol}】分析失败",
            "body":         message,
            "advice":       "请检查基金代码是否正确，或稍后重试。",
            "risk_warning": "⚠️ 无法完成分析",
        },
    )


def _unsupported_report(basic: FundBasicInfo, message: str) -> FundReport:
    """生成不支持类型的友好提示"""
    return FundReport(
        symbol=basic.symbol,
        fund_type=basic.type_category,
        basic=basic,
        text_report={
            "headline":     f"【{basic.name}】— 暂不支持该基金类型",
            "body":         message,
            "advice":       "当前版本专注于权益类、纯债类、指数ETF和转债类基金的深度分析。",
            "risk_warning": "ℹ️ 非分析错误，该基金类型超出当前版本支持范围。",
        },
    )
