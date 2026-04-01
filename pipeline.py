"""
统一报告分发器（Pipeline 主控） — fund_quant_v2
数据获取 → 标准化清洗 → 模块化计算 → 统一报告分发

这是整个系统的核心调度器，非 Streamlit UI 入口。
"""

from __future__ import annotations
import logging

import numpy as np
import pandas as pd

from models.schema import (
    FundReport, PipelineState,
    FundBasicInfo, HoldingsData,
)
from data_loader.base_api import get_fund_type_em
from data_loader.equity_loader import (
    load_basic_info, load_nav, load_stock_holdings,
    load_ff_factors, build_benchmark,
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
from reporter.hybrid_equity_report_writer import generate_hybrid_equity_report
from reporter.hybrid_bond_report_writer import generate_hybrid_bond_report
from reporter.hybrid_flexible_report_writer import generate_hybrid_flexible_report
from reporter.hybrid_absreturn_report_writer import generate_hybrid_absreturn_report
from reporter.bond_short_report_writer import generate_bond_short_report
from reporter.bond_mixed2_report_writer import generate_bond_mixed2_report
from reporter.bond_mixed1_report_writer import generate_bond_mixed1_report
from reporter.idx_stock_report_writer import generate_idx_stock_report
from reporter.idx_bond_report_writer import generate_idx_bond_report
from reporter.chart_gen import generate_chart_data
from utils.common import audit_logger

logger = logging.getLogger(__name__)

# ============================================================
# 类型映射（fund_name_em 权威类型 → 分析框架标识）
# ============================================================

# 分析框架常量
FRAMEWORK_STOCK = "stock"                    # 股票型
FRAMEWORK_HYBRID_EQUITY = "hybrid_equity"    # 混合型-偏股
FRAMEWORK_HYBRID_BALANCED = "hybrid_balanced"# 混合型-平衡
FRAMEWORK_HYBRID_FLEXIBLE = "hybrid_flexible"# 混合型-灵活
FRAMEWORK_HYBRID_ABSRETURN = "hybrid_absreturn"  # 混合型-绝对收益
FRAMEWORK_HYBRID_BOND = "hybrid_bond"        # 混合型-偏债
FRAMEWORK_BOND_LONG = "bond_long"            # 债券型-长债
FRAMEWORK_BOND_SHORT = "bond_short"          # 债券型-中短债
FRAMEWORK_BOND_MIXED2 = "bond_mixed2"        # 债券型-混合二级
FRAMEWORK_BOND_MIXED1 = "bond_mixed1"        # 债券型-混合一级
FRAMEWORK_IDX_STOCK = "idx_stock"            # 指数型-股票
FRAMEWORK_IDX_BOND = "idx_bond"              # 指数型-固收
FRAMEWORK_UNSUPPORTED = "unsupported"        # 不支持的类型

# 框架路由信息：{分析框架: (模型大类, 持仓加载方式, 中文名)}
FRAMEWORK_ROUTING: dict[str, tuple[str, str, str]] = {
    FRAMEWORK_STOCK:            ("equity", "stock", "股票型"),
    FRAMEWORK_HYBRID_EQUITY:    ("equity", "stock", "混合型-偏股"),
    FRAMEWORK_HYBRID_BALANCED:  ("equity", "stock", "混合型-平衡"),
    FRAMEWORK_HYBRID_FLEXIBLE:  ("equity", "stock", "混合型-灵活"),
    FRAMEWORK_HYBRID_ABSRETURN: ("equity", "stock", "混合型-绝对收益"),
    FRAMEWORK_HYBRID_BOND:      ("bond",   "bond",  "混合型-偏债"),
    FRAMEWORK_BOND_LONG:        ("bond",   "bond",  "债券型-长债"),
    FRAMEWORK_BOND_SHORT:       ("bond",   "bond",  "债券型-中短债"),
    FRAMEWORK_BOND_MIXED2:      ("bond",   "bond",  "债券型-混合二级"),
    FRAMEWORK_BOND_MIXED1:      ("bond",   "bond",  "债券型-混合一级"),
    FRAMEWORK_IDX_STOCK:        ("index",  "stock", "指数型-股票"),
    FRAMEWORK_IDX_BOND:         ("index",  "bond",  "指数型-固收"),
}

# fund_name_em 原始类型 → 分析框架（一步到位）
FUND_TYPE_MAP: dict[str, str] = {
    # --- 股票类 ---
    "股票型":               FRAMEWORK_STOCK,
    # --- 混合类 ---
    "混合型-偏股":          FRAMEWORK_HYBRID_EQUITY,
    "混合型-平衡":          FRAMEWORK_HYBRID_BALANCED,
    "混合型-灵活":          FRAMEWORK_HYBRID_FLEXIBLE,
    "混合型-绝对收益":      FRAMEWORK_HYBRID_ABSRETURN,
    "混合型-偏债":          FRAMEWORK_HYBRID_BOND,
    # --- 债券类 ---
    "债券型-长债":          FRAMEWORK_BOND_LONG,
    "债券型-中短债":        FRAMEWORK_BOND_SHORT,
    "债券型-混合二级":      FRAMEWORK_BOND_MIXED2,
    "债券型-混合一级":      FRAMEWORK_BOND_MIXED1,
    # --- 指数类 ---
    "指数型-股票":          FRAMEWORK_IDX_STOCK,
    "指数型-固收":          FRAMEWORK_IDX_BOND,
    # --- 不支持 ---
    "指数型-海外股票":      FRAMEWORK_UNSUPPORTED,
    "指数型-其他":          FRAMEWORK_UNSUPPORTED,
    "货币型-普通货币":      FRAMEWORK_UNSUPPORTED,
    "货币型-浮动净值":      FRAMEWORK_UNSUPPORTED,
    "FOF-稳健型":           FRAMEWORK_UNSUPPORTED,
    "FOF-进取型":           FRAMEWORK_UNSUPPORTED,
    "FOF-均衡型":           FRAMEWORK_UNSUPPORTED,
    "QDII-普通股票":        FRAMEWORK_UNSUPPORTED,
    "QDII-纯债":           FRAMEWORK_UNSUPPORTED,
    "QDII-混合偏股":        FRAMEWORK_UNSUPPORTED,
    "QDII-混合债":          FRAMEWORK_UNSUPPORTED,
    "QDII-混合灵活":        FRAMEWORK_UNSUPPORTED,
    "QDII-混合平衡":        FRAMEWORK_UNSUPPORTED,
    "QDII-商品":            FRAMEWORK_UNSUPPORTED,
    "QDII-REITs":           FRAMEWORK_UNSUPPORTED,
    "QDII-FOF":             FRAMEWORK_UNSUPPORTED,
    "商品":                 FRAMEWORK_UNSUPPORTED,
    "Reits":                FRAMEWORK_UNSUPPORTED,
    "REITs":                FRAMEWORK_UNSUPPORTED,
}

# 不支持的类型及提示文案
UNSUPPORTED_MESSAGES: dict[str, str] = {
    "货币型-普通货币":      "该基金为货币基金，主要适合现金管理，暂不支持量化分析。",
    "货币型-浮动净值":      "该基金为浮动净值型货币基金，暂不支持量化分析。",
    "FOF-稳健型":           "该基金为 FOF 基金（基金中基金），当前版本暂不支持 FOF 分析。",
    "FOF-进取型":           "该基金为 FOF 基金（基金中基金），当前版本暂不支持 FOF 分析。",
    "FOF-均衡型":           "该基金为 FOF 基金（基金中基金），当前版本暂不支持 FOF 分析。",
    "QDII-普通股票":        "该基金为 QDII 基金（境外投资），当前版本暂不支持境外市场分析。",
    "QDII-纯债":           "该基金为 QDII 基金（境外投资），当前版本暂不支持境外市场分析。",
    "QDII-混合偏股":        "该基金为 QDII 基金（境外投资），当前版本暂不支持境外市场分析。",
    "QDII-混合债":          "该基金为 QDII 基金（境外投资），当前版本暂不支持境外市场分析。",
    "QDII-混合灵活":        "该基金为 QDII 基金（境外投资），当前版本暂不支持境外市场分析。",
    "QDII-混合平衡":        "该基金为 QDII 基金（境外投资），当前版本暂不支持境外市场分析。",
    "QDII-商品":            "该基金为 QDII 基金（境外投资），当前版本暂不支持商品类分析。",
    "QDII-REITs":           "该基金为 QDII 基金（境外投资），当前版本暂不支持 REITs 分析。",
    "QDII-FOF":             "该基金为 QDII 基金（境外投资），当前版本暂不支持 FOF 分析。",
    "商品":                 "该基金为商品基金（黄金/原油等），当前版本暂不支持商品类分析。",
    "Reits":                "该基金为 REITs，当前版本暂不支持 REITs 分析。",
    "REITs":                "该基金为 REITs，当前版本暂不支持 REITs 分析。",
    "指数型-海外股票":      "该基金为海外指数基金，当前版本暂不支持境外指数分析。",
    "指数型-其他":          "该基金类型暂不支持分析。",
}


def resolve_framework(symbol: str) -> tuple[str, str]:
    """
    从 fund_name_em 获取权威类型，一步映射到分析框架标识。

    Args:
        symbol: 基金代码（6位）

    Returns:
        (framework_id, raw_type)
        framework_id: 分析框架标识（如 "stock", "bond_long", "hybrid_bond" 等）
        raw_type: fund_name_em 原始类型（如 "混合型-偏债"），用于日志和显示
    """
    raw_type = get_fund_type_em(symbol)

    if not raw_type:
        logger.warning(f"[resolve_framework] {symbol} fund_name_em 未获取到类型或类型为空")
        return FRAMEWORK_UNSUPPORTED, ""

    framework = FUND_TYPE_MAP.get(raw_type)

    if framework is None:
        # 未知类型 → 标记为不支持，而非静默 fallback
        logger.warning(f"[resolve_framework] {symbol} 未知类型 '{raw_type}'，标记为不支持")
        return FRAMEWORK_UNSUPPORTED, raw_type

    if framework == FRAMEWORK_UNSUPPORTED:
        return FRAMEWORK_UNSUPPORTED, raw_type

    return framework, raw_type


# ============================================================
# 主分析函数（外部调用入口）
# ============================================================

@audit_logger
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
    except Exception as e:
        msg = f"基金基本信息获取失败: {e}"
        logger.error(f"[analyze_fund] {msg}")
        state.errors.append(msg)
        return _error_report(symbol, msg)

    # =========================================================
    # Stage 2: 类型识别（fund_name_em 权威类型 → 分析框架）
    # =========================================================
    framework, raw_type = resolve_framework(symbol)
    routing = FRAMEWORK_ROUTING.get(framework)
    model_type = routing[0] if routing else "equity"   # equity/bond/index/cb
    holdings_load = routing[1] if routing else "stock"  # stock/bond
    framework_name = routing[2] if routing else "未知"

    logger.info(f"[analyze_fund] {symbol} 类型识别: {raw_type or '未知'} → 框架={framework}（{framework_name}）")

    # 不支持类型 → 直接返回友好提示
    if framework == FRAMEWORK_UNSUPPORTED:
        msg = UNSUPPORTED_MESSAGES.get(raw_type, "该基金类型暂不支持分析。")
        return _unsupported_report(basic, msg)

    # =========================================================
    # Stage 3: 净值数据
    # =========================================================
    try:
        nav_raw = load_nav(
            symbol, years=years, since_inception=since_inception,
        )
        state.nav_raw = nav_raw

        # 三重守卫：None 检查 + df None 检查 + 空 DataFrame 检查
        if nav_raw is None or nav_raw.df is None or nav_raw.df.empty:
            return _error_report(symbol, "净值数据为空，无法进行分析")

        logger.info(f"[analyze_fund] {symbol} 净值数据: {len(nav_raw.df)} 行")
    except Exception as e:
        return _error_report(symbol, f"净值数据获取失败: {e}")

    # =========================================================
    # Stage 4: 持仓数据（按框架决定加载方式）
    # =========================================================
    try:
        if holdings_load == "bond":
            holdings = load_bond_holdings(symbol)
        else:
            holdings = load_stock_holdings(symbol)
        state.holdings = holdings
    except Exception as e:
        logger.warning(f"[analyze_fund] {symbol} 持仓数据获取失败（使用默认值）: {e}")
        holdings = _default_holdings(symbol, model_type)
        state.holdings = holdings

    # 债券类基金中，转债占比 > 30% 自动切换到转债模型
    if model_type == "bond" and holdings.cb_ratio > 0.30:
        model_type = "cb"
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
    # Stage 6 & 7: 模块化计算（按模型类型路由）
    # =========================================================
    start_str, end_str = _get_date_range(clean_nav.df)

    try:
        if model_type == "equity":
            report = _run_equity_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str, framework)

        elif model_type == "bond":
            report = _run_bond_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str, framework)

        elif model_type == "index":
            report = _run_index_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str, framework)

        elif model_type == "cb":
            report = _run_cb_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str, framework)

        else:
            logger.warning(f"[analyze_fund] {symbol} 未知模型类型 {model_type}，fallback 到权益分析")
            report = _run_equity_pipeline(symbol, basic, clean_nav, holdings, start_str, end_str, "stock")

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
    framework: str,
) -> FundReport:
    """权益类 Pipeline（stock / hybrid_equity / hybrid_balanced / hybrid_flexible / hybrid_absreturn）"""
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
        fund_type=framework,
    )

    # 性格标签
    tags = generate_equity_tags(metrics)

    # 混合型基金：加载历史资产配置
    is_hybrid = framework in (
        FRAMEWORK_HYBRID_EQUITY, FRAMEWORK_HYBRID_BALANCED,
        FRAMEWORK_HYBRID_FLEXIBLE, FRAMEWORK_HYBRID_ABSRETURN,
    )
    historical_allocation = []
    if is_hybrid:
        try:
            from data_loader.equity_loader import load_historical_asset_allocation
            historical_allocation = load_historical_asset_allocation(symbol)
        except Exception as e:
            logger.warning(f"[analyze_fund] {symbol} 历史资产配置加载失败: {e}")

    # 构建报告
    # 修复：无论 has_benchmark 是否为 True，只要 benchmark.df 有有效数据就传入 chart_data
    # build_benchmark() 在 benchmark_parsed 为空时也会生成默认基准（沪深300全收益）
    chart_data = {
        "nav_df": clean_nav.df,
        "holdings": {
            "top10_stocks": holdings.top10_stocks,
            "stock_ratio": holdings.stock_ratio,
            "bond_ratio": holdings.bond_ratio,
            "cash_ratio": holdings.cash_ratio,
            "cb_ratio": holdings.cb_ratio,
            "historical_allocation": historical_allocation,
        },
    }
    # 始终尝试传入基准数据（只要非空）
    if benchmark.df is not None and not benchmark.df.empty:
        chart_data["benchmark_df"] = benchmark.df
        logger.info(f"[pipeline] {symbol} 基准数据已传入 chart_data: {benchmark.df.shape[0]} 行")
    else:
        logger.warning(f"[pipeline] {symbol} 基准数据为空，图表可能缺少基准曲线")

    report = FundReport(
        symbol=symbol,
        fund_type=framework,
        basic=basic,
        equity_metrics=metrics,
        tags=tags,
        chart_data=chart_data,
    )

    # 混合型偏股/灵活/绝对收益：使用专属深度报告
    if is_hybrid:
        report.text_report = generate_text_report(report)  # 基础诊断仍保留
        # 灵活配置型使用专属5板块报告
        if framework == FRAMEWORK_HYBRID_FLEXIBLE:
            report.chart_data["hybrid_flexible_report"] = generate_hybrid_flexible_report(report)
            report.chart_data.update(generate_chart_data(report))
        elif framework == FRAMEWORK_HYBRID_ABSRETURN:
            # 先生成绝对收益报告（写入 absreturn_vol_stability 等数据），
            # 再生成图表（volatility_band 依赖这些数据）
            report.chart_data["hybrid_absreturn_report"] = generate_hybrid_absreturn_report(report)
            report.chart_data.update(generate_chart_data(report))
        else:
            # 混合型偏股/平衡：使用6板块报告
            report.chart_data["hybrid_equity_report"] = generate_hybrid_equity_report(report)
            report.chart_data.update(generate_chart_data(report))
    else:
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
    framework: str,
) -> FundReport:
    """债券类 Pipeline（hybrid_bond / bond_long / bond_short / bond_mixed1 / bond_mixed2）"""
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
        fund_type=framework,
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

    # ── 混合型-偏债 / 混合二级 / 混合一级：额外加载资产配置 + 转债详情 ──
    is_hybrid_bond = (framework == FRAMEWORK_HYBRID_BOND)
    is_bond_mixed2 = (framework == FRAMEWORK_BOND_MIXED2)
    is_bond_mixed1 = (framework == FRAMEWORK_BOND_MIXED1)
    historical_allocation = []
    top10_stocks = []
    cb_holdings_df = None
    if is_hybrid_bond or is_bond_mixed2 or is_bond_mixed1:
        try:
            from data_loader.equity_loader import (
                load_stock_holdings, load_historical_asset_allocation,
            )
            equity_holdings = load_stock_holdings(symbol)
            top10_stocks = equity_holdings.top10_stocks
            # 如果 equity_holdings 获取到了更准确的 stock_ratio，使用它
            if equity_holdings.stock_ratio > 0:
                holdings.stock_ratio = equity_holdings.stock_ratio
                holdings.cash_ratio = equity_holdings.cash_ratio
            # 加载历史资产配置
            historical_allocation = load_historical_asset_allocation(symbol)
            logger.info(
                f"[pipeline] {symbol} {framework}: stock={holdings.stock_ratio:.1%}, "
                f"bond={holdings.bond_ratio:.1%}, cash={holdings.cash_ratio:.1%}, "
                f"historical_quarters={len(historical_allocation)}"
            )
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} {framework} 额外数据加载失败: {e}")

    # ── 混合二级债基：额外加载转债持仓详情 ──
    if is_bond_mixed2 and holdings.cb_ratio > 0.01:
        try:
            cb_holdings_df = load_cb_holdings_with_details(symbol)
            if cb_holdings_df is not None and not cb_holdings_df.empty:
                logger.info(
                    f"[pipeline] {symbol} bond_mixed2: "
                    f"转债详情 {len(cb_holdings_df)} 只"
                )
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} bond_mixed2 转债详情加载失败: {e}")

    # ── 混合一级债基：加载转债持仓详情 + 转债 Beta ──
    if is_bond_mixed1 and holdings.cb_ratio > 0.01:
        try:
            cb_holdings_df = load_cb_holdings_with_details(symbol)
            if cb_holdings_df is not None and not cb_holdings_df.empty:
                logger.info(
                    f"[pipeline] {symbol} bond_mixed1: "
                    f"转债详情 {len(cb_holdings_df)} 只"
                )
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} bond_mixed1 转债详情加载失败: {e}")

    report = FundReport(
        symbol=symbol,
        fund_type=framework,
        basic=basic,
        bond_metrics=metrics,
        tags=[],
        chart_data={
            "nav_df": clean_nav.df,
            "benchmark_df": bond_idx,
            "holdings": {
                "top10_stocks": top10_stocks,
                "stock_ratio": holdings.stock_ratio,
                "bond_ratio": holdings.bond_ratio,
                "cash_ratio": holdings.cash_ratio,
                "cb_ratio": holdings.cb_ratio,
                "bond_details": holdings.bond_details,
                "historical_allocation": historical_allocation,
            },
            "cb_holdings_df": cb_holdings_df,
            "cb_beta": {},
        },
    )

    report.text_report = generate_text_report(report)
    report.chart_data.update(generate_chart_data(report))

    # ── 混合一级债基：计算转债 Beta（中证转债指数 000832）+ 溢价率数据 ──
    if is_bond_mixed1:
        try:
            nav_df = clean_nav.df
            if nav_df is not None and not nav_df.empty:
                fund_ret = nav_df["tr_ret"].dropna() if "tr_ret" in nav_df.columns else nav_df["ret"].dropna()
                # 加载中证转债指数（000832）历史数据
                from data_loader.base_api import load_cb_index_hist, load_cb_value_analysis
                start_dt = nav_df["date"].min().strftime("%Y%m%d") if "date" in nav_df.columns else "20200101"
                end_dt = nav_df["date"].max().strftime("%Y%m%d") if "date" in nav_df.columns else None
                cb_idx_df = load_cb_index_hist("000832", start_date=start_dt, end_date=end_dt)
                if not fund_ret.empty and cb_idx_df is not None and not cb_idx_df.empty and "close" in cb_idx_df.columns:
                    cb_idx_df["bm_ret"] = cb_idx_df["close"].pct_change().fillna(0)
                    bm_ret = cb_idx_df[["date", "bm_ret"]].set_index("date")["bm_ret"]
                    aligned = pd.concat([fund_ret, bm_ret], axis=1, join="inner").dropna()
                    aligned.columns = ["fund", "bm"]
                    if len(aligned) > 60:
                        cov_mat = np.cov(aligned["fund"], aligned["bm"])
                        var_bm = cov_mat[1, 1]
                        if var_bm > 1e-10:
                            beta = cov_mat[0, 1] / var_bm
                            corr = aligned["fund"].corr(aligned["bm"])
                            r2 = corr ** 2
                            report.chart_data["cb_beta"] = {
                                "beta": round(float(beta), 4),
                                "r_squared": round(float(r2), 4),
                                "correlation": round(float(corr), 4),
                            }
                            logger.info(
                                f"[pipeline] {symbol} bond_mixed1: "
                                f"cb_beta={beta:.4f}, R²={r2:.4f}, corr={corr:.4f}"
                            )
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} bond_mixed1 转债Beta计算失败: {e}")

        # 加载全市场转债估值数据
        try:
            from data_loader.base_api import load_cb_value_analysis
            cb_val_df = load_cb_value_analysis()
            if cb_val_df is not None and not cb_val_df.empty:
                report.chart_data["cb_value_analysis"] = cb_val_df
                logger.info(f"[pipeline] {symbol} bond_mixed1: 转债估值数据 {len(cb_val_df)} 条")
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} bond_mixed1 转债估值数据加载失败: {e}")
            logger.warning(f"[pipeline] {symbol} bond_mixed1 转债Beta计算失败: {e}")

    # ── 混合型-偏债：生成专属5板块深度报告 ──
    if is_hybrid_bond:
        report.chart_data["hybrid_bond_report"] = generate_hybrid_bond_report(report)
        logger.info(f"[pipeline] {symbol} 已生成偏债混合型5板块深度报告")

    # ── 债券型-中短债：生成专属5板块深度报告 ──
    if framework == FRAMEWORK_BOND_SHORT:
        report.chart_data["bond_short_report"] = generate_bond_short_report(report)
        logger.info(f"[pipeline] {symbol} 已生成中短债5板块深度报告")

    # ── 债券型-混合二级：生成专属5板块深度报告 ──
    if framework == FRAMEWORK_BOND_MIXED2:
        report.chart_data["bond_mixed2_report"] = generate_bond_mixed2_report(report)
        logger.info(f"[pipeline] {symbol} 已生成混合二级债基5板块深度报告")

    # ── 债券型-混合一级：生成专属5板块深度报告 ──
    if framework == FRAMEWORK_BOND_MIXED1:
        report.chart_data["bond_mixed1_report"] = generate_bond_mixed1_report(report)
        logger.info(f"[pipeline] {symbol} 已生成混合一级债基5板块深度报告")

    # ── 债券型-长债：加载10年国债技术分析数据（利率专题图表） ──
    if framework == FRAMEWORK_BOND_LONG:
        try:
            from data_loader.idx_bond_loader import load_y10y_technical_analysis
            rate_data = load_y10y_technical_analysis()
            chart_df = rate_data.get("chart_df")
            if chart_df is not None and not chart_df.empty:
                report.chart_data["y10y_chart_df"] = chart_df
                logger.info(f"[pipeline] {symbol} bond_long: y10y_chart_df 已加载")
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} bond_long 利率专题数据加载失败: {e}")

    return report


def _run_index_pipeline(
    symbol: str,
    basic: FundBasicInfo,
    clean_nav,
    holdings: HoldingsData,
    start_str: str,
    end_str: str,
    framework: str,
) -> FundReport:
    """指数/ETF Pipeline（idx_stock / idx_bond）"""
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

    chart_data = {
        "nav_df": clean_nav.df,
        "benchmark_df": bm_df,  # 添加基准数据
    }

    # ── 指数型-股票：加载额外数据并生成5板块深度报告 ──
    if framework == FRAMEWORK_IDX_STOCK:
        try:
            from data_loader.index_stock_loader import (
                classify_fund_subtype, is_etf, load_fund_fee_detail,
                load_index_valuation, calc_valuation_percentile,
                load_index_cons_weights, build_concentration_analysis,
                load_etf_liquidity, get_rebalance_info,
                normalize_index_code, get_index_name,
            )

            # 基金子类型识别
            subtype = classify_fund_subtype(basic.name)
            etf_flag = is_etf(basic.name, symbol)

            # 费率数据
            fee_detail = load_fund_fee_detail(symbol)

            # PE/PB 估值
            valuation_df = load_index_valuation(bm_code)
            pe_pct = calc_valuation_percentile(valuation_df, metric="pe_ttm")
            pb_pct = calc_valuation_percentile(valuation_df, metric="pb")

            # 从 valuation_df 提取股息率
            if valuation_df is not None and not valuation_df.empty and "dividend_yield" in valuation_df.columns:
                pe_pct["dividend_yield"] = float(valuation_df.iloc[-1]["dividend_yield"])
            else:
                pe_pct["dividend_yield"] = None

            # 成份股权重
            weights_df = load_index_cons_weights(bm_code)
            concentration = build_concentration_analysis(weights_df)

            # ETF 流动性
            liquidity = {}
            if etf_flag:
                liquidity = load_etf_liquidity(symbol)

            # 调仓信息
            rebalance_info = get_rebalance_info()

            # 指数名称
            index_name = get_index_name(bm_code)

            extra_data = {
                "subtype": subtype,
                "is_etf": etf_flag,
                "fee_detail": fee_detail,
                "valuation_df": valuation_df,
                "pe_percentile": pe_pct,
                "pb_percentile": pb_pct,
                "concentration": concentration,
                "liquidity": liquidity,
                "rebalance_info": rebalance_info,
                "index_name": index_name,
            }

            logger.info(
                f"[pipeline] {symbol} idx_stock: subtype={subtype}, "
                f"etf={etf_flag}, ter={fee_detail['total_expense_ratio']*100:.2f}%, "
                f"pe_zone={pe_pct.get('zone', '?')}"
            )

        except Exception as e:
            logger.warning(f"[pipeline] {symbol} idx_stock 额外数据加载失败: {e}")
            extra_data = {
                "subtype": "passive",
                "is_etf": False,
                "fee_detail": {},
                "valuation_df": None,
                "pe_percentile": {},
                "pb_percentile": {},
                "concentration": {},
                "liquidity": {},
                "rebalance_info": {},
                "index_name": basic.benchmark_text or "标的指数",
            }

    # ── 指数型-固收：加载额外数据并生成5板块深度报告 ──
    if framework == FRAMEWORK_IDX_BOND:
        try:
            from data_loader.idx_bond_loader import (
                calc_daily_tracking_deviation,
                estimate_duration_from_holdings,
                analyze_credit_alignment,
                monitor_rebalance_loss,
                load_idx_bond_fee,
                analyze_coupon_coverage,
                build_fee_erosion_model,
                estimate_portfolio_ytm,
                load_y10y_technical_analysis,
                get_bond_index_name,
            )

            # 债券持仓明细
            bond_details = []
            if holdings and holdings.bond_details:
                bond_details = holdings.bond_details
            elif holdings and hasattr(holdings, 'bond_details'):
                bond_details = holdings.bond_details or []

            # 日偏离度分析
            nav_df_for_dev = clean_nav.df
            bm_df_for_dev = bm_df
            tracking_deviation = calc_daily_tracking_deviation(
                nav_df_for_dev, bm_df_for_dev
            )

            # 久期估算
            duration_analysis = estimate_duration_from_holdings(bond_details)

            # 信用等级对齐
            credit_analysis = analyze_credit_alignment(
                bond_details, basic.benchmark_text or ""
            )

            # 调仓损耗监测
            rebalance_monitor = monitor_rebalance_loss(
                nav_df_for_dev, bm_df_for_dev
            )

            # 费率
            fee_detail = load_idx_bond_fee(symbol)

            # 票息覆盖率
            ann_ret_val = metrics.common.annualized_return if metrics and metrics.common else 0
            coupon_coverage = analyze_coupon_coverage(
                ann_ret_val,
                fee_detail.get("total_expense_ratio", 0),
            )

            # 费率侵蚀模型
            fee_erosion = build_fee_erosion_model(
                ann_ret_val,
                fee_detail.get("total_expense_ratio", 0),
            )

            # 10年国债技术分析
            rate_analysis = load_y10y_technical_analysis()

            # YTM 估算
            y10y_current = rate_analysis.get("current_y10y")
            ytm_estimate = estimate_portfolio_ytm(
                duration_analysis, credit_analysis, y10y_current
            )

            # 指数名称
            idx_name = basic.benchmark_text or "标的指数"
            if bm_code:
                idx_name = get_bond_index_name(bm_code)

            extra_data = {
                "tracking_deviation": tracking_deviation,
                "duration_analysis": duration_analysis,
                "credit_analysis": credit_analysis,
                "rebalance_monitor": rebalance_monitor,
                "fee_detail": fee_detail,
                "coupon_coverage": coupon_coverage,
                "fee_erosion": fee_erosion,
                "ytm_estimate": ytm_estimate,
                "rate_analysis": rate_analysis,
                "index_name": idx_name,
            }

            logger.info(
                f"[pipeline] {symbol} idx_bond: "
                f"tracking_quality={tracking_deviation.get('quality', '?')}, "
                f"est_dur={duration_analysis.get('estimated_duration', 0):.1f}Y, "
                f"is_credit_downgrade={credit_analysis.get('is_credit_downgrade', False)}"
            )

        except Exception as e:
            logger.warning(f"[pipeline] {symbol} idx_bond 额外数据加载失败: {e}")
            extra_data = {
                "tracking_deviation": {},
                "duration_analysis": {},
                "credit_analysis": {},
                "rebalance_monitor": {},
                "fee_detail": {},
                "coupon_coverage": {},
                "fee_erosion": {},
                "ytm_estimate": {},
                "rate_analysis": {},
                "index_name": basic.benchmark_text or "标的指数",
            }

    report = FundReport(
        symbol=symbol,
        fund_type=framework,
        basic=basic,
        index_metrics=metrics,
        tags=[],
        chart_data=chart_data,
    )

    report.text_report = generate_text_report(report)
    report.chart_data.update(generate_chart_data(report))

    # ── 指数型-股票：生成5板块深度报告 ──
    if framework == FRAMEWORK_IDX_STOCK:
        try:
            deep_report = generate_idx_stock_report(report, extra_data)
            report.chart_data["idx_stock_report"] = deep_report
            # 将估值数据传入 chart_data 供图表渲染
            report.chart_data["pe_valuation_df"] = extra_data.get("valuation_df")
            report.chart_data["index_concentration"] = extra_data.get("concentration", {})
            logger.info(f"[pipeline] {symbol} 已生成指数型-股票5板块深度报告")
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} idx_stock 报告生成失败: {e}")

    # ── 指数型-固收：生成5板块深度报告 ──
    if framework == FRAMEWORK_IDX_BOND:
        try:
            deep_report = generate_idx_bond_report(report, extra_data)
            report.chart_data["idx_bond_report"] = deep_report
            # 将利率分析数据传入 chart_data 供图表渲染
            report.chart_data["y10y_chart_df"] = extra_data.get("rate_analysis", {}).get("chart_df")
            logger.info(f"[pipeline] {symbol} 已生成指数型-固收5板块深度报告")
        except Exception as e:
            logger.warning(f"[pipeline] {symbol} idx_bond 报告生成失败: {e}")

    return report


def _run_cb_pipeline(
    symbol: str,
    basic: FundBasicInfo,
    clean_nav,
    holdings: HoldingsData,
    start_str: str,
    end_str: str,
    framework: str,
) -> FundReport:
    """转债/固收+ Pipeline（自动识别或框架指定）"""
    # 可转债持仓详情
    cb_df = None
    try:
        cb_df = load_cb_holdings_with_details(symbol)
    except Exception:
        pass

    # 加载基准数据（股债复合基准）
    # 转债基金需要基准来计算超额收益和绘制对比图表
    bond_idx = load_bond_composite_index(start_str, end_str)

    # 计算
    metrics = run_cb_analysis(
        nav=clean_nav,
        holdings=holdings,
        basic=basic,
        cb_holdings_df=cb_df,
    )

    report = FundReport(
        symbol=symbol,
        fund_type=framework,
        basic=basic,
        cb_metrics=metrics,
        tags=[],
        chart_data={
            "nav_df": clean_nav.df,
            "benchmark_df": bond_idx,  # 添加基准数据
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


def _default_holdings(symbol: str, model_type: str) -> HoldingsData:
    """持仓数据获取失败时的默认值（按模型类型）"""
    defaults = {
        "equity": dict(stock_ratio=0.85, bond_ratio=0.10, cash_ratio=0.05),
        "bond":   dict(stock_ratio=0.02, bond_ratio=0.88, cash_ratio=0.10),
        "index":  dict(stock_ratio=0.95, bond_ratio=0.00, cash_ratio=0.05),
        "cb":     dict(stock_ratio=0.05, bond_ratio=0.70, cash_ratio=0.05),
    }
    d = defaults.get(model_type, defaults["equity"])
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
