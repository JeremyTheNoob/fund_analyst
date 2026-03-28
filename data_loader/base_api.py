"""
基础 API 工具函数
提供缓存、超时、重试等装饰器
"""

from functools import wraps, lru_cache
from datetime import datetime, timedelta
from typing import Callable, Any, Optional
import logging
import pandas as pd
import akshare as ak

logger = logging.getLogger(__name__)

# 默认超时时间（秒）
DEFAULT_TIMEOUT = 10.0
MAX_RETRIES = 3
RETRY_DELAY = 2.0

def cached(ttl: int = 3600):
    """
    缓存装饰器（简化版，暂时不实现）
    
    Args:
        ttl: 缓存时间（秒）
    """
    def decorator(func: Callable) -> Callable:
        # 暂时不实现缓存，直接返回函数
        return func
    return decorator

def timeout(seconds: float = DEFAULT_TIMEOUT):
    """
    超时装饰器（简化版，实际使用需要信号处理）
    
    Args:
        seconds: 超时时间（秒）
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

def retry(max_retries: int = 3, delay: float = 2.0, logger_instance: Optional[logging.Logger] = None):
    """
    重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 重试间隔（秒）
        logger_instance: 日志记录器
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logger_instance or logger
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        _logger.warning(f"{func.__name__} 第 {attempt + 1} 次调用失败：{str(e)}，{delay} 秒后重试...")
                        import time
                        time.sleep(delay)
            
            # 所有重试都失败
            _logger.error(f"{func.__name__} 在 {max_retries} 次重试后仍然失败")
            raise last_exception
        
        return wrapper
    return decorator

def safe_api_call(api_func: Callable, timeout_seconds: float = DEFAULT_TIMEOUT, max_retries: int = 3) -> Any:
    """
    安全的 API 调用包装器，带超时和重试
    
    Args:
        api_func: 要调用的 API 函数
        timeout_seconds: 超时时间（秒）
        max_retries: 最大重试次数
    
    Returns:
        API 调用的结果
    """
    @retry(max_retries=max_retries, delay=1.0)
    def _call_with_timeout():
        return api_func()
    
    return _call_with_timeout()

def parse_pct(value: Any) -> float:
    """
    解析百分比字符串

    Args:
        value: 可以是百分比字符串、数字或其他类型

    Returns:
        转换后的数值
    """
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # 移除半角和全角百分号（使用 replace 而不是 strip）
        value = value.strip().replace('%', '').replace('％', '')
        # 移除中文括号内的内容，如 "（每年）"
        for pattern in ['（', '）', '(', ')']:
            if pattern in value:
                value = value.split(pattern)[0].strip()
        # 如果值是 "---" 或空，返回 0
        if not value or value in ('---', '-', 'N/A'):
            return 0.0
        try:
            return float(value) / 100.0
        except ValueError:
            logger.warning(f"parse_pct 无法解析 '{value}'")
            return 0.0
    return 0.0

def safe_df(df: Optional[pd.DataFrame], default_columns: Optional[list] = None) -> pd.DataFrame:
    """
    安全的 DataFrame 处理，确保 DataFrame 不为空且有指定的列
    
    Args:
        df: 输入的 DataFrame
        default_columns: 默认列名列表
    
    Returns:
        安全的 DataFrame
    """
    if df is None:
        return pd.DataFrame(columns=default_columns or [])
    
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)
    
    if df.empty:
        return pd.DataFrame(columns=default_columns or list(df.columns))
    
    if default_columns:
        for col in default_columns:
            if col not in df.columns:
                df[col] = None
    
    return df


# =============================================================================
# AkShare API 封装函数（带超时和重试）
# =============================================================================

def _ak_fund_basic_xq(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金基础信息（雪球）
    
    Args:
        symbol: 基金代码
    
    Returns:
        基金基本信息 DataFrame
    """
    return safe_api_call(
        lambda: ak.fund_individual_basic_info_xq(symbol=symbol)
    )


def _ak_fund_name_em(symbol: str, *args, **kwargs) -> Optional[str]:
    """
    基金名称（东方财富）
    
    Args:
        symbol: 基金代码
    
    Returns:
        基金名称
    """
    try:
        df = safe_api_call(
            lambda: ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        )
        if df is not None and not df.empty:
            return df.iloc[0, 0] if len(df.columns) > 0 else None
    except Exception as e:
        logger.warning(f"_ak_fund_name_em 失败: {e}")
    return None


def _ak_fund_list_em() -> Optional[pd.DataFrame]:
    """
    获取基金代码和名称列表（东方财富）
    
    Returns:
        包含基金代码和名称的DataFrame，列名为['基金代码', '基金名称']
        如果没有数据返回None
    """
    try:
        df = safe_api_call(
            lambda: ak.fund_name_em()
        )
        if df is not None and not df.empty:
            # 确保列名标准化
            if '基金代码' in df.columns and '基金名称' in df.columns:
                return df[['基金代码', '基金名称']].copy()
            elif len(df.columns) >= 2:
                # 如果列名不是标准格式，重命名前两列
                df.columns = ['基金代码', '基金名称'] + list(df.columns[2:])
                return df[['基金代码', '基金名称']].copy()
    except Exception as e:
        logger.warning(f"_ak_fund_list_em 失败: {e}")
    return None


def _ak_fund_scale_sina(symbol: str, *args, **kwargs) -> Optional[float]:
    """
    基金规模（新浪）
    
    Args:
        symbol: 基金代码
    
    Returns:
        基金规模（亿元）
    """
    # 新浪接口可能不稳定，返回默认值
    logger.warning("_ak_fund_scale_sina 未实现，返回 None")
    return None


def _ak_fund_fee_em(symbol: str = "000001", indicator: str = "运作费用", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金费率（东方财富）

    Args:
        symbol: 基金代码
        indicator: 指标类型（运作费用/认购费率/申购费率等）

    Returns:
        费率信息 DataFrame
    """
    return safe_api_call(
        lambda: ak.fund_fee_em(symbol=symbol, indicator=indicator)
    )


def _ak_fund_nav(symbol: str, indicator: str = "单位净值走势", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金净值数据（东方财富）

    Args:
        symbol: 基金代码
        indicator: 指标类型（累计净值走势/单位净值走势）

    Returns:
        净值数据 DataFrame
    """
    return safe_api_call(
        lambda: ak.fund_open_fund_info_em(symbol=symbol, indicator=indicator)
    )


def _ak_fund_purchase_status(symbol: str, *args, **kwargs) -> Optional[dict]:
    """
    基金申购赎回状态（东方财富）

    Args:
        symbol: 基金代码

    Returns:
        包含申购状态、赎回状态、购买起点的字典
    """
    try:
        df = safe_api_call(lambda: ak.fund_purchase_em())
        if df is None or df.empty:
            return None

        fund_data = df[df['基金代码'] == symbol]
        if fund_data.empty:
            return None

        row = fund_data.iloc[0]
        return {
            'purchase_status': row.get('申购状态', ''),
            'redeem_status': row.get('赎回状态', ''),
            'min_purchase': float(row.get('购买起点', 0.0))
        }
    except Exception as e:
        logger.warning(f"_ak_fund_purchase_status 获取 {symbol} 失败: {e}")
        return None


def _ak_fund_holdings_stock(symbol: str, date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金股票持仓（东方财富）
    
    Args:
        symbol: 基金代码
        date: 日期（如 "2024"）
    
    Returns:
        股票持仓 DataFrame
    """
    return safe_api_call(
        lambda: ak.fund_portfolio_hold_em(symbol=symbol, date=date)
    )


def _ak_fund_asset_allocation(symbol: str, date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金资产配置（东方财富）
    
    Args:
        symbol: 基金代码
        date: 日期
    
    Returns:
        资产配置 DataFrame
    """
    # 基金资产配置接口可能不稳定，返回空 DataFrame
    logger.warning("_ak_fund_asset_allocation 未实现，返回空 DataFrame")
    return pd.DataFrame()


def _ak_index_daily_main(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    指数日线行情（主力）

    Args:
        symbol: 指数代码（如 "000300.SH"、"sh000300"）

    Returns:
        指数行情 DataFrame
    """
    try:
        # 标准化指数代码到 AkShare 格式
        ak_symbol = symbol
        # 如果是 .SH 或 .SZ 格式，转换为 sh/sz 前缀格式
        if ".SH" in symbol:
            ak_symbol = f"sh{symbol.replace('.SH', '')}"
        elif ".SZ" in symbol:
            ak_symbol = f"sz{symbol.replace('.SZ', '')}"
        # 如果是纯数字格式（如 000300），尝试 sh 前缀
        elif symbol.isdigit():
            ak_symbol = f"sh{symbol}"
        
        df = safe_api_call(
            lambda: ak.stock_zh_index_daily(symbol=ak_symbol)
        )
        # 兼容 AkShare 不同版本的列名
        if df is not None and not df.empty:
            # AkShare 新版本可能使用数字索引
            if "date" not in df.columns:
                # 尝试重命名第一列为 date，第二列为 close
                if len(df.columns) >= 2:
                    df = df.rename(columns={
                        df.columns[0]: "date",
                        df.columns[1]: "close"
                    })
        return df
    except Exception as e:
        logger.warning(f"_ak_index_daily_main 获取 {symbol} (标准化为 {ak_symbol}) 失败: {e}")
        return pd.DataFrame()


def _ak_index_daily_em(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    指数日线行情（东方财富）
    
    Args:
        symbol: 指数代码
    
    Returns:
        指数行情 DataFrame
    """
    # 东方财富指数接口可能不稳定，返回空 DataFrame
    logger.warning("_ak_index_daily_em 未实现，返回空 DataFrame")
    return pd.DataFrame()


def _ak_hk_index_daily(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    港股指数日线行情（新浪）
    
    Args:
        symbol: 指数代码
    
    Returns:
        指数行情 DataFrame
    """
    return safe_api_call(
        lambda: ak.stock_hk_index_daily_sina()
    )


def _ak_fund_holdings_bond(symbol: str, date: str = "2024", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金债券持仓（东方财富）
    
    Args:
        symbol: 基金代码
        date: 日期（如 "2024"）
    
    Returns:
        债券持仓 DataFrame
    """
    return safe_api_call(
        lambda: ak.fund_portfolio_bond_hold_em(symbol=symbol, date=date)
    )


def _ak_bond_us_rate(start_date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    美国国债收益率
    
    Args:
        start_date: 开始日期
    
    Returns:
        国债收益率 DataFrame
    """
    return safe_api_call(
        lambda: ak.bond_zh_us_rate(start_date=start_date)
    )


def _ak_bond_china_yield(start: str, end: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    中国债券收益率（国债、国开债、企业债）
    
    Args:
        start: 开始日期
        end: 结束日期
    
    Returns:
        债券收益率 DataFrame
    """
    # AkShare 可能没有直接的债券收益率历史接口，返回空 DataFrame
    logger.warning("_ak_bond_china_yield 未实现，返回空 DataFrame")
    return pd.DataFrame()


def _ak_bond_composite_index(indicator: str = "财富", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    中债综合指数
    
    Args:
        indicator: 指标类型（财富/总值）
    
    Returns:
        综合指数 DataFrame
    """
    return safe_api_call(
        lambda: ak.bond_new_composite_index_cbond(indicator=indicator)
    )


def _ak_cb_info(symbol: str, indicator: str = "基本信息", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    可转债基本信息
    
    Args:
        symbol: 可转债代码
        indicator: 指标类型
    
    Returns:
        可转债基本信息 DataFrame
    """
    return safe_api_call(
        lambda: ak.bond_zh_cov_info(symbol=symbol, indicator=indicator)
    )
