"""
日期工具模块 — fund_quant_v2
统一日期处理逻辑，避免重复代码
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Union
import pandas as pd

from utils.common import FinancialConfig

logger = logging.getLogger(__name__)


# ============================================================
# 📅 日期格式常量
# ============================================================
DATE_FORMATS = {
    "standard": "%Y-%m-%d",           # 标准格式：2024-03-29
    "compact": "%Y%m%d",             # 紧凑格式：20240329
    "chinese": "%Y年%m月%d日",       # 中文格式：2024年03月29日
    "month": "%Y-%m",                # 月度格式：2024-03
    "time": "%Y-%m-%d %H:%M:%S",     # 带时间格式
}


# ============================================================
# 📅 日期解析函数
# ============================================================

def parse_date(
    date_str: str,
    fmt: str = "standard",
    default: Optional[datetime] = None,
) -> Optional[datetime]:
    """
    解析日期字符串

    Args:
        date_str: 日期字符串
        fmt: 日期格式（key in DATE_FORMATS 或自定义格式字符串）
        default: 解析失败时的默认值

    Returns:
        datetime 对象或 None
    """
    if not date_str or pd.isna(date_str):
        return default

    # 如果 fmt 是预定义格式
    if fmt in DATE_FORMATS:
        fmt = DATE_FORMATS[fmt]

    try:
        return datetime.strptime(date_str, fmt)
    except (ValueError, TypeError) as e:
        logger.warning(f"日期解析失败: {date_str} (格式: {fmt}) - {e}")
        return default


def format_date(
    date_obj: Union[datetime, pd.Timestamp],
    fmt: str = "standard",
) -> str:
    """
    格式化日期对象

    Args:
        date_obj: datetime 或 Timestamp 对象
        fmt: 日期格式（key in DATE_FORMATS 或自定义格式字符串）

    Returns:
        格式化后的日期字符串
    """
    if pd.isna(date_obj) or date_obj is None:
        return ""

    # 如果是 Timestamp，转换为 datetime
    if isinstance(date_obj, pd.Timestamp):
        date_obj = date_obj.to_pydatetime()

    # 如果 fmt 是预定义格式
    if fmt in DATE_FORMATS:
        fmt = DATE_FORMATS[fmt]

    try:
        return date_obj.strftime(fmt)
    except Exception as e:
        logger.warning(f"日期格式化失败: {date_obj} (格式: {fmt}) - {e}")
        return ""


def get_trading_date(
    base_date: Union[datetime, pd.Timestamp, str],
    offset_days: int,
    holidays: Optional[list] = None,
) -> datetime:
    """
    获取交易日（简单估算，排除周末）

    Args:
        base_date: 基准日期
        offset_days: 偏移天数（正向为未来，负向为过去）
        holidays: 节假日列表（可选）

    Returns:
        交易日 datetime
    """
    if isinstance(base_date, str):
        base_date = parse_date(base_date)
    elif isinstance(base_date, pd.Timestamp):
        base_date = base_date.to_pydatetime()

    if base_date is None:
        return datetime.now()

    date = base_date + timedelta(days=offset_days)
    offset = offset_days

    # 调整周末
    while offset != 0:
        # 检查是否为周末（5=周六，6=周日）
        if date.weekday() < 5 and (not holidays or date.date() not in holidays):
            offset += 1 if offset > 0 else -1

        date = date + timedelta(days=1 if offset > 0 else -1)

    return date


def get_date_range(
    start_date: Union[datetime, pd.Timestamp, str],
    end_date: Union[datetime, pd.Timestamp, str],
    freq: str = "D",
) -> pd.DatetimeIndex:
    """
    获取日期范围

    Args:
        start_date: 开始日期
        end_date: 结束日期
        freq: 频率（D=日，M=月，Y=年）

    Returns:
        日期序列 DatetimeIndex
    """
    if isinstance(start_date, str):
        start_date = parse_date(start_date)
    elif isinstance(start_date, pd.Timestamp):
        start_date = start_date.to_pydatetime()

    if isinstance(end_date, str):
        end_date = parse_date(end_date)
    elif isinstance(end_date, pd.Timestamp):
        end_date = end_date.to_pydatetime()

    if start_date is None or end_date is None:
        return pd.DatetimeIndex([])

    return pd.date_range(start=start_date, end=end_date, freq=freq)


def years_between(
    start_date: Union[datetime, pd.Timestamp, str],
    end_date: Union[datetime, pd.Timestamp, str],
) -> float:
    """
    计算两个日期之间的年数

    Args:
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        年数（基于交易日数）
    """
    if isinstance(start_date, str):
        start_date = parse_date(start_date)
    elif isinstance(start_date, pd.Timestamp):
        start_date = start_date.to_pydatetime()

    if isinstance(end_date, str):
        end_date = parse_date(end_date)
    elif isinstance(end_date, pd.Timestamp):
        end_date = end_date.to_pydatetime()

    if start_date is None or end_date is None:
        return 0.0

    delta_days = (end_date - start_date).days
    return delta_days / FinancialConfig.TRADING_DAYS_YEAR
