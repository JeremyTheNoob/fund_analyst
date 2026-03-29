"""
通用工具模块 — fund_quant_v2
集中管理通用常量和工具函数，避免代码重复和硬编码
"""

import logging
from functools import wraps
from typing import Callable
import time


# ============================================================
# 🧮 金融计算常量
# ============================================================
class FinancialConfig:
    """金融计算常量（解决 P3 魔法数字）"""

    # 精度控制
    PRECISION_EPSILON = 1e-6  # 浮点数比较精度（严格相等）
    EXCESS_TOLERANCE = 1e-4   # 超额收益容差（1个基点的1/100）

    # 时间参数
    TRADING_DAYS_YEAR = 252   # 年化交易日数
    MONTHS_PER_YEAR = 12      # 月数
    DAYS_PER_MONTH = 21       # 月度交易日（估算）
    SECONDS_PER_DAY = 86400   # 一天秒数

    # 权重验证
    WEIGHT_SUM_TOLERANCE = 1e-6  # 权重和容差

    # 阈值
    MIN_SAMPLE_SIZE = 30       # 最小样本量
    MIN_OBS_FOR_OLS = 60      # OLS 最少观测数
    MAX_DURATION_YEARS = 15.0  # 久期上限（年）

    # 缺失数据
    MAX_MISSING_RATIO = 0.50  # 最大缺失比例


# ============================================================
# 🌐 网络请求配置
# ============================================================
class NetworkConfig:
    """网络请求配置（解决 P2 超时分散）"""

    DEFAULT_TIMEOUT = 30        # 默认超时（秒）
    MAX_RETRIES = 3             # 最大重试次数
    RETRY_DELAY = 1.0           # 重试间隔（秒）
    CONNECT_TIMEOUT = 10        # 连接超时
    READ_TIMEOUT = 20           # 读取超时


# ============================================================
# 📝 日志配置
# ============================================================
class LogConfig:
    """日志格式配置（解决 P3 格式不统一）"""

    LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s"
    LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LOG_LEVEL = logging.INFO

    # 日志级别颜色（终端输出）
    LEVEL_COLORS = {
        "DEBUG": "\033[36m",    # 青色
        "INFO": "\033[32m",     # 绿色
        "WARNING": "\033[33m",  # 黄色
        "ERROR": "\033[31m",    # 红色
        "CRITICAL": "\033[35m", # 紫色
    }
    RESET_COLOR = "\033[0m"


# ============================================================
# 🔧 工具函数
# ============================================================


def setup_global_logging():
    """初始化全局日志配置"""
    logging.basicConfig(
        level=LogConfig.LOG_LEVEL,
        format=LogConfig.LOG_FORMAT,
        datefmt=LogConfig.LOG_DATE_FORMAT,
    )


def audit_logger(func: Callable) -> Callable:
    """
    核心函数审计装饰器（解决 P2 耗时监控与出入口日志）

    功能：
    1. 记录函数调用入口（函数名 + 参数）
    2. 执行函数并捕获异常
    3. 记录函数调用出口（执行时间 + 结果状态）
    4. 异常自动记录到 ERROR 日志

    使用方法：
    ```python
    @audit_logger
    def my_function(x, y):
        return x + y
    ```
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        start_time = time.perf_counter()

        # 入口日志
        args_str = f"{args if args else ''}"
        kwargs_str = f"{kwargs if kwargs else ''}"
        logger.info(f"▶️ START: {func.__name__} | Args: {args_str} | Kwargs: {kwargs_str}")

        try:
            result = func(*args, **kwargs)
            duration = time.perf_counter() - start_time

            # 出口日志与性能监控
            logger.info(f"✅ END: {func.__name__} | Elapsed: {duration:.3f}s")
            return result
        except Exception as e:
            duration = time.perf_counter() - start_time
            logger.error(
                f"❌ CRASH: {func.__name__} | Elapsed: {duration:.3f}s | Error: {str(e)}",
                exc_info=True
            )
            raise

    return wrapper


def format_duration(seconds: float) -> str:
    """
    格式化时间（秒）为可读字符串

    Args:
        seconds: 秒数

    Returns:
        格式化时间字符串（如 "1h 23m 45.67s"）
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.2f}s"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    安全除法，避免除零错误

    Args:
        numerator: 分子
        denominator: 分母
        default: 除零时的默认返回值

    Returns:
        除法结果或默认值
    """
    if abs(denominator) < FinancialConfig.PRECISION_EPSILON:
        return default
    return numerator / denominator


def clip_value(value: float, min_val: float, max_val: float) -> float:
    """
    裁剪数值到指定范围

    Args:
        value: 原始值
        min_val: 最小值
        max_val: 最大值

    Returns:
        裁剪后的值
    """
    return max(min_val, min(value, max_val))
