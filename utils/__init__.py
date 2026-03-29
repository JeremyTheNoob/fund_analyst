"""
通用工具模块 — fund_quant_v2
集中管理通用常量和工具函数，避免代码重复和硬编码
"""

from utils.common import (
    FinancialConfig,
    NetworkConfig,
    LogConfig,
    setup_global_logging,
    audit_logger,
    format_duration,
    safe_divide,
    clip_value,
)
from utils.date_utils import (
    parse_date,
    format_date,
    get_trading_date,
    get_date_range,
    years_between,
)

__all__ = [
    # 通用常量
    "FinancialConfig",
    "NetworkConfig",
    "LogConfig",
    # 日志与监控
    "setup_global_logging",
    "audit_logger",
    # 工具函数
    "format_duration",
    "safe_divide",
    "clip_value",
    # 日期工具
    "parse_date",
    "format_date",
    "get_trading_date",
    "get_date_range",
    "years_between",
]
