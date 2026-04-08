"""
equity_engine_v2.py — 资产维度·股票指标计算

按资产维度计算股票相关指标，不依赖基金类型分类。

指标清单：
  拟买入：TRI脱水、PE分位、PEG、ERP、Ldays、黑天鹅压测
  已持有：风格漂移(R²)、止盈信号(PE极端)、超额回撤、滚动Alpha趋势、Alpha衰减
  通用：年化Alpha、R²、Beta
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from models.schema import (
    CleanNavData, BenchmarkData, HoldingsData,
)
from models.schema_v2 import StockAssetMetrics
from engine.common_metrics import (
    capm_alpha, beta, information_ratio, tracking_error,
    max_drawdown, recovery_days, annualized_return,
)
from data_loader.stock_metrics_loader import load_top10_stock_metrics

logger = logging.getLogger(__name__)


# ============================================================
# 主入口
# ============================================================

def run_stock_analysis(
    nav: CleanNavData,
    benchmark: BenchmarkData,
    holdings: HoldingsData,
    mode: str = "buy",  # "buy" or "hold"
    yield_10y: Optional[float] = None,  # 10年国债收益率（用于ERP计算）
    fund_code: str = "",  # 基金代码（用于获取基金规模）
) -> StockAssetMetrics:
    """
    股票维度完整分析。

    Args:
        nav: 清洗后净值数据
        benchmark: 基准数据（基金合同基准或沪深300全收益）
        holdings: 持仓数据
        mode: "buy"（拟买入）/ "hold"（已持有）
        yield_10y: 10年国债收益率（%），用于 ERP 计算

    Returns:
        StockAssetMetrics
    """
    nav_df = nav.df.copy()
    result = StockAssetMetrics()

    if nav_df.empty or "ret" not in nav_df.columns:
        return result

    fund_ret = nav_df.set_index("date")["ret"]
    fund_rets = fund_ret.values
    dates = pd.DatetimeIndex(nav_df["date"])

    # === 基准对齐 ===
    bm_rets = None
    if benchmark is None or benchmark.df is None or benchmark.df.empty:
        logger.warning("[equity_v2] 基准数据为空，Alpha/R²/Beta 等依赖基准的指标将无法计算")
    else:
        bm_rets = _align_benchmark(fund_ret, benchmark)

    # === 通用指标 ===
    if bm_rets is not None:
        result.alpha_annual = _calc_alpha(fund_rets, bm_rets)
        result.r_squared = _calc_r_squared(fund_rets, bm_rets)
        result.beta = _calc_beta(fund_rets, bm_rets)
    else:
        logger.info("[equity_v2] 基准为空，跳过 Alpha/R²/Beta 计算")

    # === Top10 持仓加载 ===
    top10 = holdings.top10_stocks if holdings.top10_stocks else []
    if top10:
        # 统一键名：中文键 → 英文键
        normalized = []
        for s in top10:
            entry = {
                "code": s.get("code") or s.get("股票代码", ""),
                "name": s.get("name") or s.get("股票名称", ""),
                "ratio": _safe_float(s.get("ratio") or s.get("占净值比例", 0)),
            }
            normalized.append(entry)
        enriched = load_top10_stock_metrics(normalized, fund_code=fund_code)
        result.top10_details = enriched

        # 加权 PEG
        result.weighted_peg = _calc_weighted_peg(enriched)

        # 加权 PE 分位
        result.pe_percentile = _calc_weighted_pe_percentile(enriched)

        # 流动性穿透（加权 Ldays）
        result.ldays = _calc_weighted_ldays(enriched)

        # ERP = 1/PE - 10年债收益率
        if yield_10y is not None:
            result.erp = _calc_weighted_erp(enriched, yield_10y)

        # 黑天鹅压测：PE 回归 10% 分位 → 预期跌幅
        result.blackswan_loss = _calc_blackswan_loss(enriched)

        # 已持有模式：PE 极端值检测
        if mode == "hold":
            result.pe_extreme = _detect_pe_extreme(enriched)

    # === 全收益脱水（TRI 偏离度） ===
    if benchmark is not None:
        result.tri_deviation = _calc_tri_deviation(fund_ret, benchmark)

    # === 已持有模式额外指标 ===
    if mode == "hold" and benchmark is not None:
        # 滚动 Alpha 趋势
        alpha_df = _calc_rolling_alpha(fund_ret, benchmark)
        result.alpha_trend_df = alpha_df
        if alpha_df is not None and not alpha_df.empty:
            result.alpha_trend = alpha_df.to_dict("records")

        # 超额回撤
        result.excess_drawdown = _calc_excess_drawdown(fund_ret, benchmark)

        # Alpha 衰减（近 3 个月 vs 近 12 个月）
        result.stop_profit_signal = _detect_alpha_decay(fund_ret, benchmark)

    # === 风格 R² 矩阵（多风格基准） ===
    r2_matrix = _calc_style_r2_matrix(fund_ret)
    if r2_matrix is not None:
        result.r2_matrix_df = r2_matrix
        result.style_drift_r2 = _detect_style_drift(r2_matrix)
        result.style_consistency_r2 = _calc_style_consistency(r2_matrix)

    return result


# ============================================================
# 核心计算函数
# ============================================================

def _align_benchmark(fund_ret: pd.Series, benchmark: BenchmarkData) -> np.ndarray:
    """对齐基准收益率序列"""
    if benchmark.df.empty or "bm_ret" not in benchmark.df.columns:
        return np.zeros(len(fund_ret))

    bm_df = benchmark.df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    fund_dates = pd.to_datetime(fund_ret.index)

    bm_s = bm_df.set_index("date")["bm_ret"]
    common_dates = fund_dates.intersection(bm_s.index)

    if len(common_dates) < 10:
        logger.warning("[equity_v2] 基准对齐不足10天")
        return np.zeros(len(fund_ret))

    aligned = bm_s.reindex(fund_dates).ffill().fillna(0).values
    return aligned[:len(fund_ret)]


def _calc_alpha(fund_rets: np.ndarray, bm_rets: np.ndarray) -> Optional[float]:
    """CAPM Alpha（年化）"""
    try:
        a = capm_alpha(fund_rets, bm_rets)
        return round(a, 4) if a else None
    except Exception:
        return None


def _calc_beta(fund_rets: np.ndarray, bm_rets: np.ndarray) -> Optional[float]:
    """Beta"""
    try:
        b = beta(fund_rets, bm_rets)
        return round(b, 4) if b else None
    except Exception:
        return None


def _calc_r_squared(fund_rets: np.ndarray, bm_rets: np.ndarray) -> Optional[float]:
    """R²（基金 vs 基准）"""
    n = min(len(fund_rets), len(bm_rets))
    if n < 30:
        return None
    try:
        corr = np.corrcoef(fund_rets[:n], bm_rets[:n])[0, 1]
        return round(corr ** 2, 4) if not np.isnan(corr) else None
    except Exception:
        return None


def _calc_tri_deviation(fund_ret: pd.Series, benchmark: BenchmarkData) -> Optional[float]:
    """
    全收益脱水（含权TRI偏离度）

    计算：基金累计收益 - 基准累计收益（几何法）
    正值 = 基金跑赢基准，负值 = 跑输
    """
    if benchmark.df.empty or "bm_ret" not in benchmark.df.columns:
        return None

    fund_cum = (1 + fund_ret).cumprod() - 1
    bm_df = benchmark.df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    bm_s = bm_df.set_index("date")["bm_ret"].reindex(fund_ret.index).fillna(0)
    bm_cum = (1 + bm_s).cumprod() - 1

    # 对齐
    aligned = pd.DataFrame({"fund": fund_cum, "bm": bm_cum}).dropna()
    if len(aligned) < 20:
        return None

    deviation = (aligned["fund"].iloc[-1] - aligned["bm"].iloc[-1]) * 100
    return round(float(deviation), 2)


def _calc_weighted_peg(enriched_stocks: List[Dict]) -> Optional[float]:
    """加权 PEG（按持仓比例加权）"""
    pegs = []
    weights = []
    for s in enriched_stocks:
        peg = s.get("peg")
        ratio = _safe_float(s.get("ratio") or s.get("占净值比例", 0))
        if peg is not None and peg > 0 and ratio and ratio > 0:
            pegs.append(peg)
            weights.append(ratio)

    if not pegs or not weights:
        return None

    total_w = sum(weights)
    if total_w <= 0:
        return None

    return round(sum(p * w for p, w in zip(pegs, weights)) / total_w, 2)


def _calc_weighted_pe_percentile(enriched_stocks: List[Dict]) -> Optional[float]:
    """加权 PE 历史分位"""
    pcts = []
    weights = []
    for s in enriched_stocks:
        pct = s.get("pe_percentile")
        ratio = _safe_float(s.get("ratio") or s.get("占净值比例", 0))
        if pct is not None and ratio and ratio > 0:
            pcts.append(pct)
            weights.append(ratio)

    if not pcts or not weights:
        return None

    total_w = sum(weights)
    if total_w <= 0:
        return None

    return round(sum(p * w for p, w in zip(pcts, weights)) / total_w, 1)


def _calc_weighted_ldays(enriched_stocks: List[Dict]) -> Optional[float]:
    """
    加权流动性穿透（Ldays）。

    Ldays > 30 的个股视为流动性极差，cap 到 30 天参与加权计算，
    避免个别极端值（如停牌/冷门股）拉高整体结果。
    """
    LDAYS_CAP = 30.0  # 单只股票 Ldays 上限

    ldays_list = []
    weights = []
    for s in enriched_stocks:
        ld = s.get("ldays")
        ratio = _safe_float(s.get("ratio") or s.get("占净值比例", 0))
        if ld is not None and ratio and ratio > 0:
            # 超过上限的 cap 住
            capped_ld = min(ld, LDAYS_CAP)
            ldays_list.append(capped_ld)
            weights.append(ratio)

    if not ldays_list or not weights:
        return None

    total_w = sum(weights)
    if total_w <= 0:
        return None

    return round(sum(p * w for p, w in zip(ldays_list, weights)) / total_w, 1)


def _calc_weighted_erp(enriched_stocks: List[Dict], yield_10y: float) -> Optional[float]:
    """
    加权股权溢价 ERP = 1/PE(TTM) - 10年国债收益率

    ERP > 0 说明股票相对债券有吸引力（PE 低或利率低）
    """
    erps = []
    weights = []
    for s in enriched_stocks:
        pe = s.get("pe_ttm")
        ratio = _safe_float(s.get("ratio") or s.get("占净值比例", 0))
        if pe and pe > 0 and ratio and ratio > 0:
            erp = (1.0 / pe - yield_10y / 100.0) * 100  # 转为百分点
            erps.append(erp)
            weights.append(ratio)

    if not erps or not weights:
        return None

    total_w = sum(weights)
    if total_w <= 0:
        return None

    return round(sum(p * w for p, w in zip(erps, weights)) / total_w, 2)


def _calc_blackswan_loss(enriched_stocks: List[Dict]) -> Optional[float]:
    """
    黑天鹅压测：PE 回归历史 10% 分位时的预期跌幅

    公式：跌幅 = (当前PE - PE_10pct) / 当前PE
    用 Top10 加权
    """
    losses = []
    weights = []
    for s in enriched_stocks:
        pe = s.get("pe_ttm")
        pct = s.get("pe_percentile")
        ratio = _safe_float(s.get("ratio") or s.get("占净值比例", 0))

        # 如果有历史分位数据，用 10% 分位估算
        # 如果没有，用经验值：PE 下降 40% 作为压力情景
        if pe and pe > 0 and ratio and ratio > 0:
            if pct is not None and pct > 15:
                # 10% 分位 ≈ 当前 PE × (1 - 回调幅度)
                # 简化：假设 PE 回调到历史 10% 分位
                # 10%分位 ≈ 当前PE × 0.5 ~ 0.7（经验值）
                stress_pe = pe * 0.5  # 保守估计
            else:
                stress_pe = pe * 0.7  # 已经在低位，回调空间有限

            loss_pct = (stress_pe - pe) / pe * 100  # 负值
            losses.append(loss_pct)
            weights.append(ratio)

    if not losses or not weights:
        return None

    total_w = sum(weights)
    if total_w <= 0:
        return None

    return round(sum(p * w for p, w in zip(losses, weights)) / total_w, 2)


# ============================================================
# 已持有模式指标
# ============================================================

def _detect_pe_extreme(enriched_stocks: List[Dict]) -> Optional[bool]:
    """检测 PE 是否处于极端值（>95% 或 <5%）"""
    pcts = [s.get("pe_percentile") for s in enriched_stocks if s.get("pe_percentile") is not None]
    if not pcts:
        return None
    # 加权 PE 分位 > 80% 视为偏高
    avg_pct = sum(pcts) / len(pcts)
    return avg_pct > 80 or avg_pct < 10


def _calc_rolling_alpha(
    fund_ret: pd.Series,
    benchmark: BenchmarkData,
    window: int = 60,  # 60个交易日 ≈ 3个月
) -> Optional[pd.DataFrame]:
    """滚动 Alpha 趋势"""
    if benchmark.df.empty or "bm_ret" not in benchmark.df.columns:
        return None

    bm_df = benchmark.df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    bm_s = bm_df.set_index("date")["bm_ret"]

    aligned = pd.DataFrame({
        "fund": fund_ret,
        "bm": bm_s.reindex(fund_ret.index).fillna(0),
    }).dropna()

    if len(aligned) < window + 10:
        return None

    alphas = []
    dates_out = []

    for i in range(window, len(aligned)):
        window_data = aligned.iloc[i - window: i]
        try:
            r = window_data["fund"].values
            bm = window_data["bm"].values
            a = capm_alpha(r, bm)
            alphas.append(a)
            dates_out.append(aligned.index[i])
        except Exception:
            alphas.append(0.0)
            dates_out.append(aligned.index[i])

    df = pd.DataFrame({"date": dates_out, "alpha": alphas})
    df["alpha"] = df["alpha"].round(4)
    return df


def _calc_excess_drawdown(fund_ret: pd.Series, benchmark: BenchmarkData) -> Optional[float]:
    """超额回撤（几何超额收益的最大回撤）"""
    if benchmark.df.empty or "bm_ret" not in benchmark.df.columns:
        return None

    bm_df = benchmark.df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    bm_s = bm_df.set_index("date")["bm_ret"]

    # 几何超额收益
    excess = (1 + fund_ret) / (1 + bm_s.reindex(fund_ret.index).fillna(0)) - 1
    excess = excess.dropna()

    if len(excess) < 20:
        return None

    cum = (1 + excess).cumprod()
    running_max = cum.cummax()
    drawdown = (cum - running_max) / running_max

    return round(float(drawdown.min()) * 100, 2)


def _detect_alpha_decay(fund_ret: pd.Series, benchmark: BenchmarkData) -> Optional[str]:
    """
    检测 Alpha 衰减信号。

    近3个月 Alpha vs 近12个月 Alpha：
    - 近3月 > 近12月：Alpha 加速 → 无信号
    - 近3月 < 近12月 * 0.5：Alpha 衰减显著 → "近期 Alpha 衰减明显"
    - 近3月 < 0 且 近12月 > 0：由正转负 → "Alpha 由正转负"
    """
    if benchmark.df.empty or "bm_ret" not in benchmark.df.columns:
        return None

    bm_df = benchmark.df.copy()
    bm_df["date"] = pd.to_datetime(bm_df["date"])
    bm_s = bm_df.set_index("date")["bm_ret"]

    aligned = pd.DataFrame({
        "fund": fund_ret,
        "bm": bm_s.reindex(fund_ret.index).fillna(0),
    }).dropna()

    if len(aligned) < 250:  # 需要至少 1 年数据
        return None

    # 近 12 个月 Alpha
    r_12m = aligned["fund"].values[-250:]
    bm_12m = aligned["bm"].values[-250:]
    alpha_12m = capm_alpha(r_12m, bm_12m)

    # 近 3 个月 Alpha
    r_3m = aligned["fund"].values[-60:]
    bm_3m = aligned["bm"].values[-60:]
    alpha_3m = capm_alpha(r_3m, bm_3m)

    if alpha_12m is None or alpha_3m is None:
        return None

    if alpha_3m < 0 and alpha_12m > 0:
        return "Alpha 由正转负"
    elif alpha_12m > 0 and alpha_3m < alpha_12m * 0.5:
        return "近期 Alpha 衰减明显"
    elif alpha_3m < 0:
        return "持续跑输基准"

    return None


# ============================================================
# 风格 R² 矩阵
# ============================================================

def _calc_style_r2_matrix(fund_ret: pd.Series) -> Optional[pd.DataFrame]:
    """
    计算基金与多个风格基准的 R² 矩阵。

    基准列表：沪深300、中证500、中证1000、创业板指、国证价值、国证成长
    """
    indices = {
        "沪深300": "sh000300",
        "中证500": "sh000905",
        "中证1000": "sh000852",
        "创业板指": "sz399006",
        "国证价值": "sz399371",
        "国证成长": "sz399370",
    }

    try:
        from data_loader.equity_loader import load_index_daily
    except ImportError:
        return None

    start = fund_ret.index.min()
    end = fund_ret.index.max()

    results = {}
    for name, code in indices.items():
        try:
            df = load_index_daily(code, str(start)[:10], str(end)[:10])
            if df is not None and not df.empty:
                bm_s = df.set_index("date")["ret"].reindex(fund_ret.index).fillna(0)
                corr = fund_ret.corr(bm_s)
                results[name] = round(corr ** 2, 4) if not np.isnan(corr) else 0.0
        except Exception:
            results[name] = 0.0

    if not results:
        return None

    df = pd.DataFrame.from_dict(results, orient="index", columns=["R²"])
    return df


def _detect_style_drift(r2_matrix: pd.DataFrame) -> Optional[float]:
    """
    风格漂移检测。

    方法：如果最大 R² 和次大 R² 差距 < 0.1，说明风格模糊
    返回最大 R² 值，值越低风格越不清晰
    """
    if r2_matrix is None or r2_matrix.empty:
        return None

    sorted_r2 = r2_matrix["R²"].sort_values(ascending=False)
    if len(sorted_r2) < 2:
        return round(sorted_r2.iloc[0], 4)

    top1 = sorted_r2.iloc[0]
    top2 = sorted_r2.iloc[1]

    return round(top1, 4)


def _calc_style_consistency(r2_matrix: pd.DataFrame) -> Optional[float]:
    """风格一致性：最大 R²（越高越稳定）"""
    if r2_matrix is None or r2_matrix.empty:
        return None
    return round(r2_matrix["R²"].max(), 4)


# ============================================================
# 工具
# ============================================================

def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None
