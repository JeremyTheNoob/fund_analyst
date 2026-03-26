"""
通用工具函数
提供重试、格式化、安全除法等纯函数工具
"""

import time
from functools import wraps
from typing import Callable, Any, Optional
import pandas as pd
import numpy as np


# ============================================================
# 🔄 重试装饰器
# ============================================================
def retry_on_failure(retries: int = 3, delay: float = 1.0):
    """
    自动重试装饰器。遇到任何异常自动重试，最后一次失败才向上抛出。

    Args:
        retries: 最大重试次数
        delay: 重试间隔（秒）

    用法：
        @retry_on_failure(retries=3, delay=2)
        def fetch_data():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exc = None
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if i < retries - 1:
                        time.sleep(delay)
            # 全部重试失败，静默返回None（由各函数内部返回空DataFrame）
            return None
        return wrapper
    return decorator


# ============================================================
# 📊 数据格式化
# ============================================================
def fmt_pct(v: Optional[float], decimals: int = 1) -> str:
    """
    格式化百分比为字符串

    Args:
        v: 数值（如 0.0523 表示 5.23%）
        decimals: 小数位数

    Returns:
        格式化字符串（如 "+5.2%" 或 "N/A"）

    Examples:
        >>> fmt_pct(0.0523)
        '+5.2%'
        >>> fmt_pct(-0.0315)
        '-3.2%'
        >>> fmt_pct(None)
        'N/A'
    """
    if v is None:
        return 'N/A'
    return f'{v*100:+.{decimals}f}%'


def fmt_f(v: Optional[float], decimals: int = 2) -> str:
    """
    格式化浮点数为字符串

    Args:
        v: 数值
        decimals: 小数位数

    Returns:
        格式化字符串（如 "3.14" 或 "N/A"）

    Examples:
        >>> fmt_f(3.14159, 2)
        '3.14'
        >>> fmt_f(None)
        'N/A'
    """
    if v is None:
        return 'N/A'
    return f'{v:.{decimals}f}'


def fmt_currency(v: Optional[float], unit: str = '万') -> str:
    """
    格式化金额为中文单位字符串

    Args:
        v: 金额（元）
        unit: 单位（万/亿）

    Returns:
        格式化字符串（如 "12.34亿"）

    Examples:
        >>> fmt_currency(1234567890, '亿')
        '12.35亿'
    """
    if v is None:
        return 'N/A'

    if unit == '亿':
        return f'{v / 1e8:.2f}亿'
    elif unit == '万':
        return f'{v / 1e4:.2f}万'
    else:
        return f'{v:,.0f}'


# ============================================================
# 🛡️ 安全计算
# ============================================================
def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    安全除法，避免除零错误

    Args:
        numerator: 分子
        denominator: 分母
        default: 除零时的默认返回值

    Returns:
        除法结果或默认值

    Examples:
        >>> safe_divide(10, 2)
        5.0
        >>> safe_divide(10, 0)
        0.0
    """
    if denominator == 0 or pd.isna(denominator):
        return default
    return numerator / denominator


def safe_pct_change(old: float, new: float) -> float:
    """
    安全计算百分比变化

    Args:
        old: 旧值
        new: 新值

    Returns:
        百分比变化（new - old）/old，除零时返回0
    """
    if old == 0 or pd.isna(old):
        return 0.0
    return (new - old) / old


# ============================================================
# 📏 归一化与评分
# ============================================================
def normalize_score(
    value: float,
    min_val: float,
    max_val: float,
    clip: bool = True
) -> float:
    """
    将数值归一化到 0-100 分

    Args:
        value: 原始值
        min_val: 最小值
        max_val: 最大值
        clip: 是否裁剪超出范围（防止负分或超过100分）

    Returns:
        归一化后的分数（0-100）

    Examples:
        >>> normalize_score(0.05, 0.0, 0.10)
        50.0
        >>> normalize_score(0.15, 0.0, 0.10, clip=True)
        100.0
    """
    if max_val == min_val:
        return 50.0

    score = (value - min_val) / (max_val - min_val) * 100

    if clip:
        score = max(0, min(100, score))

    return score


def sigmoid_transform(x: float, center: float = 0.0, scale: float = 1.0) -> float:
    """
    Sigmoid变换，将任意数值映射到 0-1

    Args:
        x: 输入值
        center: 中心点（对应0.5）
        scale: 缩放系数

    Returns:
        变换后的值（0-1）

    Examples:
        >>> sigmoid_transform(0, 0, 1)
        0.5
        >>> sigmoid_transform(2, 0, 1)  # 大于中心，>0.5
        0.88
    """
    return 1 / (1 + np.exp(-(x - center) / scale))


# ============================================================
# 📅 时间处理
# ============================================================
def get_date_range(years: int = 5) -> tuple:
    """
    获取日期范围（从今天往前推指定年数）

    Args:
        years: 年数

    Returns:
        (start_date, end_date) 字符串，格式 "YYYY-MM-DD"

    Examples:
        >>> get_date_range(5)
        ('2021-03-20', '2026-03-20')
    """
    from datetime import datetime, timedelta

    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 365)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def quarter_to_date(quarter: str) -> str:
    """
    将季度字符串转换为日期字符串

    Args:
        quarter: 季度字符串，如 "2024Q1"

    Returns:
        日期字符串，如 "2024-03-31"

    Examples:
        >>> quarter_to_date("2024Q1")
        '2024-03-31'
    """
    year, q = int(quarter[:4]), int(quarter[-1])

    quarter_end_month = {
        1: '03-31',
        2: '06-30',
        3: '09-30',
        4: '12-31',
    }

    return f"{year}-{quarter_end_month[q]}"


# ============================================================
# 🔍 数据验证
# ============================================================
def is_valid_series(series: pd.Series, min_length: int = 10) -> bool:
    """
    验证Series是否有效（非空、长度足够）

    Args:
        series: pandas Series
        min_length: 最小长度要求

    Returns:
        是否有效
    """
    if series is None or series.empty:
        return False
    if len(series) < min_length:
        return False
    return True


def drop_duplicates_sorted(df: pd.DataFrame, subset: str = 'date') -> pd.DataFrame:
    """
    去重并按日期排序

    Args:
        df: DataFrame
        subset: 用于去重的列名

    Returns:
        处理后的DataFrame
    """
    if df.empty:
        return df

    return df.drop_duplicates(subset=subset).sort_values(subset=subset).reset_index(drop=True)


# ============================================================
# 🧪 统计辅助
# ============================================================
def annualize_return(daily_return: float, days: int = 252) -> float:
    """
    将日收益率年化

    Args:
        daily_return: 日收益率
        days: 年化天数

    Returns:
        年化收益率
    """
    return (1 + daily_return) ** days - 1


def annualize_volatility(daily_vol: float, days: int = 252) -> float:
    """
    将日波动率年化

    Args:
        daily_vol: 日波动率
        days: 年化天数

    Returns:
        年化波动率
    """
    return daily_vol * np.sqrt(days)


def calculate_sharpe(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    """
    计算夏普比率

    Args:
        returns: 收益率序列
        risk_free_rate: 无风险利率

    Returns:
        夏普比率
    """
    if returns.empty or returns.std() == 0:
        return 0.0

    excess_returns = returns - risk_free_rate / 252  # 日度无风险利率
    return excess_returns.mean() / returns.std() * np.sqrt(252)


def calculate_max_drawdown(nav_series: pd.Series) -> tuple:
    """
    计算最大回撤

    Args:
        nav_series: 净值序列

    Returns:
        (max_drawdown, recovery_days)
    """
    if nav_series.empty:
        return 0.0, 0

    # 计算累计净值曲线
    cum_nav = nav_series / nav_series.iloc[0]

    # 计算滚动最高点
    rolling_max = cum_nav.expanding().max()

    # 计算回撤
    drawdown = (cum_nav - rolling_max) / rolling_max

    # 最大回撤
    max_dd = drawdown.min()

    # 找到最大回撤位置
    max_dd_idx = drawdown.idxmin()
    max_dd_pos = drawdown.index.get_loc(max_dd_idx)

    # 计算恢复天数
    if max_dd_pos < len(nav_series) - 1:
        after_dd = cum_nav[max_dd_idx:]
        recovery = after_dd[after_dd >= cum_nav[max_dd_idx]].first_valid_index()
        if recovery is not None:
            recovery_days = (recovery - max_dd_idx).days
        else:
            recovery_days = None  # 未恢复
    else:
        recovery_days = None

    return max_dd, recovery_days


def calculate_information_ratio(
    fund_returns: pd.Series,
    benchmark_returns: pd.Series
) -> float:
    """
    计算信息比率

    Args:
        fund_returns: 基金收益率序列
        benchmark_returns: 基准收益率序列

    Returns:
        信息比率
    """
    if fund_returns.empty or benchmark_returns.empty:
        return 0.0

    excess_returns = fund_returns - benchmark_returns

    if excess_returns.std() == 0:
        return 0.0

    return excess_returns.mean() / excess_returns.std() * np.sqrt(252)
