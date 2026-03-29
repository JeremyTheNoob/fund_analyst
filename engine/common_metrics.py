"""
通用量化指标引擎 — fund_quant_v2
单一事实来源（SSoT）：所有数学公式集中于此，严禁在业务层重复定义
"""

from __future__ import annotations
from typing import Optional, Union
import logging

import numpy as np
import pandas as pd

from utils.common import FinancialConfig

logger = logging.getLogger(__name__)

# 统一类型别名
ArrayLike = Union[pd.Series, np.ndarray]

# 使用统一配置（向后兼容）
TRADING_DAYS = FinancialConfig.TRADING_DAYS_YEAR


def _to_array(x: ArrayLike) -> np.ndarray:
    """统一转换为 ndarray，去除 NaN"""
    if isinstance(x, pd.Series):
        x = x.values
    arr = np.asarray(x, dtype=float)
    return arr[~np.isnan(arr)]


# ============================================================
# 收益率指标
# ============================================================

def annualized_return(
    returns: ArrayLike,
    periods_per_year: int = TRADING_DAYS,
    method: str = "geometric",
) -> float:
    """
    年化收益率。
    method: 'geometric'（几何均值，默认）或 'arithmetic'（算术均值）
    """
    arr = _to_array(returns)
    if len(arr) == 0:
        return 0.0
    if method == "geometric":
        cum = np.prod(1 + arr)
        n   = len(arr)
        return float(cum ** (periods_per_year / n) - 1)
    else:
        return float(np.mean(arr) * periods_per_year)


def cumulative_return(returns: ArrayLike) -> float:
    """累计收益率（复利）"""
    arr = _to_array(returns)
    if len(arr) == 0:
        return 0.0
    return float(np.prod(1 + arr) - 1)


def max_drawdown(returns: ArrayLike) -> float:
    """
    最大回撤（负值）。
    使用净值曲线（cumprod），不依赖 price 序列。
    """
    arr = _to_array(returns)
    if len(arr) < 2:
        return 0.0
    nav = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(nav)
    dd   = (nav - peak) / peak
    return float(dd.min())


def max_drawdown_duration(returns: ArrayLike) -> int:
    """最大回撤持续期（交易日数）"""
    arr = _to_array(returns)
    if len(arr) < 2:
        return 0
    nav  = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(nav)
    dd   = (nav - peak) / peak

    max_dd = dd.min()
    if max_dd == 0:
        return 0

    trough_idx = int(np.argmin(dd))
    # 回撤起点：trough 之前最后一次 dd == 0 的位置
    prior = np.where(dd[:trough_idx] == 0)[0]
    start_idx = int(prior[-1]) if len(prior) > 0 else 0
    return trough_idx - start_idx


def recovery_days(returns: ArrayLike) -> Optional[int]:
    """
    从最大回撤谷底到恢复至前高的交易日数。
    若尚未恢复，返回 None。
    """
    arr = _to_array(returns)
    if len(arr) < 2:
        return None
    nav  = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(nav)
    dd   = (nav - peak) / peak

    trough_idx = int(np.argmin(dd))
    if trough_idx >= len(arr) - 1:
        return None

    after_trough = dd[trough_idx:]
    recovery = np.where(after_trough >= 0)[0]
    if len(recovery) == 0:
        return None
    return int(recovery[0])


def volatility(
    returns: ArrayLike,
    periods_per_year: int = TRADING_DAYS,
    method: str = "std",
) -> float:
    """
    年化波动率。
    method: 'std'（标准差）或 'ewm'（指数加权，lambda=0.94）
    """
    arr = _to_array(returns)
    if len(arr) < 2:
        return 0.0
    if method == "ewm":
        lam = 0.94
        weights = np.array([lam ** i for i in range(len(arr) - 1, -1, -1)])
        weights /= weights.sum()
        mean = np.dot(weights, arr)
        var  = np.dot(weights, (arr - mean) ** 2)
        return float(np.sqrt(var * periods_per_year))
    else:
        return float(np.std(arr, ddof=1) * np.sqrt(periods_per_year))


def downside_volatility(
    returns: ArrayLike,
    threshold: float = 0.0,
    periods_per_year: int = TRADING_DAYS,
) -> float:
    """下行波动率（仅计算低于 threshold 的收益）"""
    arr = _to_array(returns)
    if len(arr) < 2:
        return 0.0
    down = arr[arr < threshold] - threshold
    if len(down) == 0:
        return 0.0
    return float(np.sqrt(np.mean(down ** 2) * periods_per_year))


def skewness(returns: ArrayLike) -> float:
    """偏度"""
    arr = _to_array(returns)
    if len(arr) < 3:
        return 0.0
    return float(pd.Series(arr).skew())


def kurtosis(returns: ArrayLike) -> float:
    """超额峰度（Fisher 定义，正态分布 = 0）"""
    arr = _to_array(returns)
    if len(arr) < 4:
        return 0.0
    return float(pd.Series(arr).kurtosis())


# ============================================================
# 风险调整后收益
# ============================================================

def sharpe_ratio(
    returns: ArrayLike,
    risk_free_rate: float = 0.0,
    periods_per_year: int = TRADING_DAYS,
) -> float:
    """夏普比率"""
    arr = _to_array(returns)
    if len(arr) < 2:
        return 0.0
    rf_daily = risk_free_rate / periods_per_year
    excess   = arr - rf_daily
    std_e    = np.std(excess, ddof=1)
    if std_e < 1e-10:
        return 0.0
    return float(np.mean(excess) / std_e * np.sqrt(periods_per_year))


def sortino_ratio(
    returns: ArrayLike,
    risk_free_rate: float = 0.0,
    periods_per_year: int = TRADING_DAYS,
) -> float:
    """Sortino 比率（分母用下行波动率）"""
    arr = _to_array(returns)
    if len(arr) < 2:
        return 0.0
    rf_daily = risk_free_rate / periods_per_year
    excess   = arr - rf_daily
    dv = downside_volatility(arr, threshold=rf_daily, periods_per_year=periods_per_year)
    if dv < 1e-10:
        return 0.0
    return float(np.mean(excess) * periods_per_year / dv)


def calmar_ratio(
    returns: ArrayLike,
    periods_per_year: int = TRADING_DAYS,
) -> float:
    """卡玛比率 = 年化收益 / |最大回撤|"""
    arr  = _to_array(returns)
    mdd  = abs(max_drawdown(arr))
    if mdd < 1e-10:
        return 0.0
    ann  = annualized_return(arr, periods_per_year)
    return float(ann / mdd)


def information_ratio(
    returns: ArrayLike,
    benchmark_returns: ArrayLike,
    periods_per_year: int = TRADING_DAYS,
) -> float:
    """信息比率 = 年化超额收益 / 年化跟踪误差"""
    r  = _to_array(returns)
    bm = _to_array(benchmark_returns)
    n  = min(len(r), len(bm))
    if n < 5:
        return 0.0
    excess = r[:n] - bm[:n]
    te = np.std(excess, ddof=1)
    if te < FinancialConfig.PRECISION_EPSILON:
        return 0.0
    return float(np.mean(excess) / te * np.sqrt(periods_per_year))


def tracking_error(
    returns: ArrayLike,
    benchmark_returns: ArrayLike,
    periods_per_year: int = TRADING_DAYS,
) -> float:
    """年化跟踪误差"""
    r  = _to_array(returns)
    bm = _to_array(benchmark_returns)
    n  = min(len(r), len(bm))
    if n < 5:
        return 0.0
    excess = r[:n] - bm[:n]
    return float(np.std(excess, ddof=1) * np.sqrt(periods_per_year))


def beta(returns: ArrayLike, benchmark_returns: ArrayLike) -> float:
    """贝塔系数（OLS 斜率）"""
    r  = _to_array(returns)
    bm = _to_array(benchmark_returns)
    n  = min(len(r), len(bm))
    if n < 5:
        return 1.0
    bm_var = np.var(bm[:n], ddof=1)
    if bm_var < 1e-10:
        return 1.0
    return float(np.cov(r[:n], bm[:n], ddof=1)[0, 1] / bm_var)


def capm_alpha(
    returns: ArrayLike,
    benchmark_returns: ArrayLike,
    risk_free_rate: float = 0.0,
    periods_per_year: int = TRADING_DAYS,
) -> float:
    """CAPM Alpha（年化）"""
    r   = _to_array(returns)
    bm  = _to_array(benchmark_returns)
    n   = min(len(r), len(bm))
    if n < 5:
        return 0.0
    rf  = risk_free_rate / periods_per_year
    b   = beta(r[:n], bm[:n])
    alpha_daily = np.mean(r[:n] - rf) - b * np.mean(bm[:n] - rf)
    return float(alpha_daily * periods_per_year)


# ============================================================
# 月度胜率
# ============================================================

def monthly_win_rate(
    returns: ArrayLike,
    benchmark_returns: ArrayLike,
    dates: Optional[pd.DatetimeIndex] = None,
) -> float:
    """
    月度胜率：基金月度收益跑赢基准的概率。

    Args:
        returns: 日度基金收益率
        benchmark_returns: 日度基准收益率
        dates: 对应日期索引（若传入则按月分组；否则直接用 returns 作月度序列）
    """
    r  = np.asarray(returns, dtype=float)
    bm = np.asarray(benchmark_returns, dtype=float)

    if dates is not None and len(dates) == len(r):
        # 按月聚合
        fund_s = pd.Series(r, index=dates)
        bm_s   = pd.Series(bm[:len(dates)], index=dates)
        fund_m = (1 + fund_s).resample("ME").prod() - 1
        bm_m   = (1 + bm_s).resample("ME").prod() - 1
        common = fund_m.index.intersection(bm_m.index)
        if len(common) < 3:
            return 0.5
        excess = fund_m.loc[common] - bm_m.loc[common]
    else:
        n = min(len(r), len(bm))
        if n < 3:
            return 0.5
        excess = r[:n] - bm[:n]

    return float((excess > 0).mean())


# ============================================================
# 分位数工具
# ============================================================

def historical_percentile(current: float, history: ArrayLike) -> float:
    """
    计算当前值在历史序列中的百分位数（0-100）。

    修复旧系统 Bug：
    旧版 fetch_stock_valuation_alert 使用 np.random.uniform(10, 90)
    作为 PE 分位，完全随机，无实际意义。
    本函数用真实历史数据计算分位数。
    """
    arr = _to_array(history)
    if len(arr) == 0:
        return 50.0
    return float((arr < current).mean() * 100)


# ============================================================
# 辅助工具
# ============================================================

def normalize_score(
    value: float,
    min_val: float,
    max_val: float,
    invert: bool = False,
) -> float:
    """
    将 value 线性归一化到 [0, 100]。
    invert=True 时值越小得分越高（如最大回撤、波动率）。
    """
    if max_val <= min_val:
        return 50.0
    score = (value - min_val) / (max_val - min_val) * 100
    score = max(0.0, min(100.0, score))
    return float(100.0 - score if invert else score)


def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """安全除法，分母为 0 时返回 default"""
    if abs(b) < 1e-10:
        return default
    return float(a / b)


# ============================================================
# 超额收益计算（用于超额收益动态曲线）
# ============================================================

def geometric_excess_return(
    fund_ret: pd.Series,
    benchmark_ret: pd.Series,
) -> pd.Series:
    """
    计算几何超额收益序列。
    
    公式：excess_ret = (1 + fund_ret) / (1 + benchmark_ret) - 1
    
    优势：
    - 复利效应下更准确，反映"如果我买基准，比买基金少赚多少"
    - 加法一致性：(1+ret_fund) = (1+excess) * (1+ret_bm)
    
    注意事项：
    - 必须确保 fund_ret 和 benchmark_ret 通过 inner join 对齐日期
    - 如果 benchmark_ret 出现 -1（极端情况），返回 0 并记录警告
    
    Args:
        fund_ret: 基金日收益率（pd.Series，索引为日期）
        benchmark_ret: 基准日收益率（pd.Series，索引为日期）
    
    Returns:
        超额收益率序列（pd.Series，与对齐后的日期索引一致）
    """
    # 创建 DataFrame，自动 inner join 对齐日期
    df = pd.DataFrame({
        "fund_ret": fund_ret,
        "bm_ret": benchmark_ret,
    }).dropna()
    
    if df.empty:
        return pd.Series(dtype=float)
    
    # 处理极端情况：bm_ret = -1 会导致分母为零
    extreme_mask = df["bm_ret"] <= -0.999  # 接近 -1 的情况
    if extreme_mask.any():
        logger.warning(
            f"[geometric_excess_return] 检测到 {extreme_mask.sum()} 个极端值 "
            f"(基准收益率 <= -99.9%)，已置为 0"
        )
        df.loc[extreme_mask, "bm_ret"] = 0
    
    # 计算几何超额收益
    excess_ret = (1 + df["fund_ret"]) / (1 + df["bm_ret"]) - 1
    
    return excess_ret


def cumulative_excess_return(
    excess_ret: pd.Series,
) -> pd.Series:
    """
    计算累计超额收益。
    
    公式：cum_excess = (1 + excess_ret).cumprod() - 1
    
    零点重置：
    - 曲线从 0% 开始
    - 如果分析区间变化（如近1年），重新以区间首日为基点归零
    
    Args:
        excess_ret: 日度超额收益率（pd.Series）
    
    Returns:
        累计超额收益序列（pd.Series，与 excess_ret 索引一致）
    """
    if excess_ret.empty:
        return pd.Series(dtype=float)

    cum_excess = (1 + excess_ret).cumprod() - 1
    return cum_excess


def extract_credit_spread_history(yield_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """从债券收益率数据中提取信用利差历史。

    Args:
        yield_df: 债券收益率数据，必须包含 "date" 和 "credit_spread" 列

    Returns:
        包含 "date" 和 "spread" 列的 DataFrame，如果数据无效则返回 None
    """
    if yield_df.empty or "credit_spread" not in yield_df.columns:
        return None

    try:
        # 提取日期和信用利差数据
        spread_df = yield_df[["date", "credit_spread"]].copy()
        spread_df["date"] = pd.to_datetime(spread_df["date"])
        spread_df = spread_df.sort_values("date")
        # 重命名列以匹配图表函数期望的格式
        spread_df = spread_df.rename(columns={"credit_spread": "spread"})
        # 移除 NaN 值
        spread_df = spread_df.dropna(subset=["spread"])
        return spread_df
    except Exception:
        return None
