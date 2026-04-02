"""
基础 API 工具函数
提供缓存、超时、重试等装饰器
"""

from functools import wraps
from typing import Callable, Any, Optional
import logging
import pandas as pd
import akshare as ak
from data_loader.akshare_timeout import call_with_timeout, with_timeout

logger = logging.getLogger(__name__)

# 默认超时时间（秒）
DEFAULT_TIMEOUT = 10.0
MAX_RETRIES = 3
RETRY_DELAY = 2.0

# P1-优化：关键 API 超时配置
API_TIMEOUTS = {
    "fund_basic": 15.0,           # 基金基本信息
    "fund_nav": 20.0,             # 基金净值历史
    "fund_holdings": 15.0,        # 基金持仓
    "index_daily": 10.0,          # 指数日线数据
    "bond_index": 10.0,           # 债券指数
    "default": 10.0               # 默认超时
}

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
    超时装饰器（P1-优化：使用 call_with_timeout 实现真实超时控制）

    Args:
        seconds: 超时时间（秒）
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return call_with_timeout(func, args=args, kwargs=kwargs, timeout=seconds)
            except TimeoutError as e:
                logger.error(f"{func.__name__} 超时（{seconds}秒）: {e}")
                raise TimeoutError(f"{func.__name__} 超时（{seconds}秒）")
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
    安全的 API 调用包装器，带超时和重试（P1-优化）
    
    Args:
        api_func: 要调用的 API 函数
        timeout_seconds: 超时时间（秒）
        max_retries: 最大重试次数
    
    Returns:
        API 调用的结果
    """
    @retry(max_retries=max_retries, delay=1.0)
    def _call_with_timeout():
        try:
            return call_with_timeout(api_func, timeout=timeout_seconds)
        except TimeoutError as e:
            logger.error(f"safe_api_call 超时（{timeout_seconds}秒）: {e}")
            raise
    
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
    优先从 Supabase 缓存读取，缓存 24 小时。
    
    Args:
        symbol: 基金代码
    
    Returns:
        基金基本信息 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_basic_xq", ttl_seconds=86400, expect_df=True, symbol=symbol)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.fund_individual_basic_info_xq(symbol=symbol)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("fund_basic_xq", result, expect_df=True, symbol=symbol)
        except Exception:
            pass

    return result


def _ak_fund_name_em(symbol: str, *args, **kwargs) -> Optional[str]:
    """
    基金名称（东方财富）
    带缓存（5 分钟 TTL，与净值共享前缀）。
    
    Args:
        symbol: 基金代码
    
    Returns:
        基金名称
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_nav", ttl_seconds=300, expect_df=True, symbol=symbol, indicator="单位净值走势")
        if cached is not None and not cached.empty:
            # 净值接口返回的第一列第一行是基金名称
            try:
                return str(cached.iloc[0, 0])
            except (IndexError, KeyError):
                pass
    except Exception:
        pass

    try:
        df = safe_api_call(
            lambda: ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
        )
        if df is not None and not df.empty:
            # 写入缓存（与净值共享）
            try:
                from data_loader.cache_layer import cache_set as _cs
                _cs("fund_nav", df, expect_df=True, symbol=symbol, indicator="单位净值走势")
            except Exception:
                pass
            return str(df.iloc[0, 0]) if len(df.columns) > 0 else None
    except Exception as e:
        logger.warning(f"_ak_fund_name_em 失败: {e}")
    return None


def _ak_fund_list_em() -> Optional[pd.DataFrame]:
    """
    获取基金代码和名称列表（东方财富）
    全量接口，带 Supabase 缓存（1h TTL）。
    
    Returns:
        包含基金代码和名称的DataFrame，列名为['基金代码', '基金名称']
        如果没有数据返回None
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_list_all", ttl_seconds=3600, expect_df=True)
        if cached is not None and not cached.empty:
            return cached
    except Exception:
        pass

    try:
        df = safe_api_call(
            lambda: ak.fund_name_em()
        )
        if df is not None and not df.empty:
            # 确保列名标准化
            if '基金代码' in df.columns and '基金名称' in df.columns:
                result = df[['基金代码', '基金名称']].copy()
            elif len(df.columns) >= 2:
                # 如果列名不是标准格式，重命名前两列
                df.columns = ['基金代码', '基金名称'] + list(df.columns[2:])
                result = df[['基金代码', '基金名称']].copy()
            else:
                return None

            # 写入缓存
            try:
                from data_loader.cache_layer import cache_set as _cs
                _cs("fund_list_all", result, expect_df=True)
            except Exception:
                pass

            return result
    except Exception as e:
        logger.warning(f"_ak_fund_list_em 失败: {e}")
    return None


def get_fund_type_em(symbol: str) -> Optional[str]:
    """
    从 fund_name_em 获取基金的权威类型（如 "混合型-偏股"、"债券型-长债"）

    使用内存缓存避免重复调用 fund_name_em（全量接口较重）。
    优先从 Supabase 缓存读取，缓存未命中时才调 API。

    Args:
        symbol: 基金代码（6位）

    Returns:
        基金类型字符串，获取失败返回 None
    """
    # 1. 先查 Supabase 缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_type", ttl_seconds=604800, expect_df=False, symbol=symbol)
        if cached is not None:
            return cached
    except Exception:
        pass

    # 2. 再查内存缓存（向后兼容）
    if not hasattr(get_fund_type_em, "_cache"):
        try:
            df = safe_api_call(lambda: ak.fund_name_em())
            if df is not None and not df.empty and "基金代码" in df.columns and "基金类型" in df.columns:
                get_fund_type_em._cache = dict(zip(df["基金代码"], df["基金类型"]))
            else:
                get_fund_type_em._cache = {}
        except Exception as e:
            logger.warning(f"[get_fund_type_em] fund_name_em 加载失败: {e}")
            get_fund_type_em._cache = {}

    result = get_fund_type_em._cache.get(symbol)

    # 3. 写入 Supabase 缓存（供其他 worker 复用）
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("fund_type", result, expect_df=False, symbol=symbol)
        except Exception:
            pass

    return result


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
    优先从 Supabase 缓存读取，缓存 7 天（费率极少变动）。

    Args:
        symbol: 基金代码
        indicator: 指标类型（运作费用/认购费率/申购费率等）

    Returns:
        费率信息 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_fee", ttl_seconds=604800, expect_df=True, symbol=symbol, indicator=indicator)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.fund_fee_em(symbol=symbol, indicator=indicator)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("fund_fee", result, expect_df=True, symbol=symbol, indicator=indicator)
        except Exception:
            pass

    return result


def _ak_fund_nav(symbol: str, indicator: str = "单位净值走势", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金净值数据（东方财富）
    优先从 Supabase 缓存读取，缓存 5 分钟。

    Args:
        symbol: 基金代码
        indicator: 指标类型（累计净值走势/单位净值走势）

    Returns:
        净值数据 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_nav", ttl_seconds=300, expect_df=True, symbol=symbol, indicator=indicator)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.fund_open_fund_info_em(symbol=symbol, indicator=indicator)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("fund_nav", result, expect_df=True, symbol=symbol, indicator=indicator)
        except Exception:
            pass

    return result


def _ak_fund_purchase_status(symbol: str, *args, **kwargs) -> Optional[dict]:
    """
    基金申购赎回状态（东方财富）
    全量接口 fund_purchase_em，带 Supabase 缓存（24h TTL）。
    缓存策略：全量 DataFrame 缓存，每次只取目标基金。

    Args:
        symbol: 基金代码

    Returns:
        包含申购状态、赎回状态、购买起点的字典
    """
    # 尝试读缓存（全量数据缓存为一条记录）
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached_df = cache_get("fund_purchase_all", ttl_seconds=86400, expect_df=True)
        if cached_df is not None and not cached_df.empty:
            fund_data = cached_df[cached_df['基金代码'] == symbol]
            if not fund_data.empty:
                row = fund_data.iloc[0]
                return {
                    'purchase_status': row.get('申购状态', ''),
                    'redeem_status': row.get('赎回状态', ''),
                    'min_purchase': float(row.get('购买起点', 0.0) or 0.0)
                }
    except Exception:
        pass

    try:
        df = safe_api_call(lambda: ak.fund_purchase_em())
        if df is not None and not df.empty:
            # 写入缓存（全量）
            try:
                from data_loader.cache_layer import cache_set as _cs
                _cs("fund_purchase_all", df, expect_df=True)
            except Exception:
                pass

            fund_data = df[df['基金代码'] == symbol]
            if not fund_data.empty:
                row = fund_data.iloc[0]
                return {
                    'purchase_status': row.get('申购状态', ''),
                    'redeem_status': row.get('赎回状态', ''),
                    'min_purchase': float(row.get('购买起点', 0.0) or 0.0)
                }
        return None
    except Exception as e:
        logger.warning(f"_ak_fund_purchase_status 获取 {symbol} 失败: {e}")
        return None


def _ak_fund_holdings_stock(symbol: str, date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金股票持仓（东方财富）
    优先从 Supabase 缓存读取，缓存 24 小时。
    
    Args:
        symbol: 基金代码
        date: 日期（如 "2024"）
    
    Returns:
        股票持仓 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_holdings_stock", ttl_seconds=86400, expect_df=True, symbol=symbol, date=date)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.fund_portfolio_hold_em(symbol=symbol, date=date)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("fund_holdings_stock", result, expect_df=True, symbol=symbol, date=date)
        except Exception:
            pass

    return result


def _ak_fund_asset_allocation(symbol: str, date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金资产配置（雪球数据源）
    优先从 Supabase 缓存读取，缓存 7 天（季报数据更新频率低）。
    
    通过 akshare fund_individual_detail_hold_xq 接口获取基金大类资产配置比例。
    返回 DataFrame 列：['资产类型', '仓位占比']，行包含：股票、债券、现金、其他等。
    
    Args:
        symbol: 基金代码
        date: 日期（格式 YYYYMMDD，如 "20240930"）
    
    Returns:
        资产配置 DataFrame，失败返回空 DataFrame
    """
    # 尝试读缓存（季报数据，7 天 TTL）
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_asset_alloc", ttl_seconds=604800, expect_df=True, symbol=symbol, date=date)
        if cached is not None:
            return cached
    except Exception:
        pass

    def _fetch():
        import akshare as ak
        return ak.fund_individual_detail_hold_xq(symbol=symbol, date=date)
    
    try:
        df = safe_api_call(_fetch, timeout_seconds=10.0, max_retries=2)
        if df is not None and not df.empty:
            # 标准化列名
            df.columns = ["资产类型", "占净值比例(%)"]

            # 写入缓存
            try:
                from data_loader.cache_layer import cache_set as _cs
                _cs("fund_asset_alloc", df, expect_df=True, symbol=symbol, date=date)
            except Exception:
                pass

        return df
    except Exception as e:
        logger.warning(f"[_ak_fund_asset_allocation] {symbol} {date} 获取失败: {e}")
        return pd.DataFrame()


def _ak_index_daily_main(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    指数日线行情（主力）
    优先从 Supabase 缓存读取，缓存 24 小时。

    Args:
        symbol: 指数代码（如 "000300.SH"、"sh000300"）

    Returns:
        指数行情 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("index_daily", ttl_seconds=86400, expect_df=True, symbol=symbol)
        if cached is not None:
            return cached
    except Exception:
        pass

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
        
        # 写入缓存（用原始 symbol 作为 key，而非标准化后的）
        if df is not None:
            try:
                from data_loader.cache_layer import cache_set as _cs
                _cs("index_daily", df, expect_df=True, symbol=symbol)
            except Exception:
                pass
        
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
    优先从 Supabase 缓存读取，缓存 24 小时。

    Args:
        symbol: 指数代码（如 "HSI"）

    Returns:
        指数行情 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("hk_index_daily", ttl_seconds=86400, expect_df=True, symbol=symbol)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.stock_hk_index_daily_sina(symbol=symbol)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("hk_index_daily", result, expect_df=True, symbol=symbol)
        except Exception:
            pass

    return result


def _ak_fund_holdings_bond(symbol: str, date: str = "2024", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金债券持仓（东方财富）
    优先从 Supabase 缓存读取，缓存 24 小时。
    
    Args:
        symbol: 基金代码
        date: 日期（如 "2024"）
    
    Returns:
        债券持仓 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("fund_holdings_bond", ttl_seconds=86400, expect_df=True, symbol=symbol, date=date)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.fund_portfolio_bond_hold_em(symbol=symbol, date=date)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("fund_holdings_bond", result, expect_df=True, symbol=symbol, date=date)
        except Exception:
            pass

    return result


def load_cb_index_hist(symbol: str = "000832", start_date: str = "20200101", end_date: str = None) -> pd.DataFrame:
    """
    加载中证转债指数历史日线数据（AkShare index_zh_a_hist）。
    优先从 Supabase 缓存读取，缓存 24 小时。
    
    Returns:
        DataFrame with columns: date, open, high, low, close, volume
        日期从近到远排列。如果加载失败返回空 DataFrame。
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("cb_index_hist", ttl_seconds=86400, expect_df=True, symbol=symbol, start_date=start_date, end_date=end_date)
        if cached is not None:
            return cached
    except Exception:
        pass

    import time as _time
    if end_date is None:
        end_date = pd.Timestamp.now().strftime("%Y%m%d")
    for attempt in range(3):
        try:
            df = ak.index_zh_a_hist(
                symbol=symbol, period="daily",
                start_date=start_date, end_date=end_date,
            )
            if df is not None and not df.empty:
                # 标准化列名
                col_map = {}
                for c in df.columns:
                    cl = c.lower()
                    if "日期" in c or "date" in c.lower():
                        col_map[c] = "date"
                    elif "开盘" in c or cl == "open":
                        col_map[c] = "open"
                    elif "收盘" in c or cl == "close":
                        col_map[c] = "close"
                    elif "最高" in c or cl == "high":
                        col_map[c] = "high"
                    elif "最低" in c or cl == "low":
                        col_map[c] = "low"
                    elif "成交" in c or cl == "volume":
                        col_map[c] = "volume"
                if col_map:
                    df = df.rename(columns=col_map)
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").reset_index(drop=True)

                # 写入缓存
                try:
                    from data_loader.cache_layer import cache_set as _cs
                    _cs("cb_index_hist", df, expect_df=True, symbol=symbol, start_date=start_date, end_date=end_date)
                except Exception:
                    pass

                return df
        except Exception as e:
            logger.debug(f"load_cb_index_hist 尝试 {attempt+1} 失败: {e}")
            _time.sleep(3)
    logger.warning(f"load_cb_index_hist({symbol}) 加载失败（3次重试）")
    return pd.DataFrame()


def load_cb_value_analysis() -> pd.DataFrame:
    """
    加载全市场可转债价值分析数据（AkShare bond_zh_cov_value_analysis）。
    优先从 Supabase 缓存读取，缓存 24 小时。
    
    Returns:
        DataFrame with columns: 日期, 收盘价, 纯债价值, 转股价值, 纯债溢价率, 转股溢价率
        包含全市场所有转债的平均估值水位。
        如果加载失败返回空 DataFrame。
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("cb_value_analysis", ttl_seconds=86400, expect_df=True)
        if cached is not None:
            return cached
    except Exception:
        pass

    import time as _time
    for attempt in range(3):
        try:
            df = ak.bond_zh_cov_value_analysis()
            if df is not None and not df.empty:
                df["日期"] = pd.to_datetime(df["日期"])
                df = df.sort_values("日期").reset_index(drop=True)

                # 写入缓存
                try:
                    from data_loader.cache_layer import cache_set as _cs
                    _cs("cb_value_analysis", df, expect_df=True)
                except Exception:
                    pass

                return df
        except Exception as e:
            logger.debug(f"load_cb_value_analysis 尝试 {attempt+1} 失败: {e}")
            _time.sleep(3)
    logger.warning("load_cb_value_analysis 加载失败（3次重试）")
    return pd.DataFrame()


def _ak_bond_us_rate(start_date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    国债收益率
    优先从 Supabase 缓存读取，缓存 24 小时。

    Args:
        start_date: 开始日期

    Returns:
        国债收益率 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("bond_us_rate", ttl_seconds=86400, expect_df=True, start_date=start_date)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.bond_zh_us_rate(start_date=start_date)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("bond_us_rate", result, expect_df=True, start_date=start_date)
        except Exception:
            pass

    return result


def _ak_bond_china_yield(start: str, end: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    中国债券信息网-国债及其他债券收益率曲线
    带 Supabase 缓存（24h TTL）。

    包含 3 条曲线：中债中短期票据收益率曲线(AAA)、中债商业银行普通债收益率曲线(AAA)、中债国债收益率曲线
    期限：3月/6月/1年/3年/5年/7年/10年/30年

    AkShare 限制：单次请求 start_date 到 end_date 需小于一年，超期返回空 DataFrame。
    本函数自动按年分段请求后拼接。

    Args:
        start: 开始日期 (YYYYMMDD)
        end: 结束日期 (YYYYMMDD)

    Returns:
        宽格式 DataFrame，index=date，columns 包含曲线名称（如"中债中短期票据收益率曲线(AAA)"）
    """
    from datetime import datetime, timedelta

    # 尝试读缓存（整体缓存）
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("bond_china_yield", ttl_seconds=86400, expect_df=True, start=start, end=end)
        if cached is not None:
            return cached
    except Exception:
        pass

    # 将 YYYYMMDD 转为 datetime
    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")

    # 接口限制一年内，按年分段请求
    chunks = []
    current = start_dt
    while current <= end_dt:
        # 每段最多一年（留1天余量避免边界问题）
        segment_end = min(current + timedelta(days=364), end_dt)
        raw = safe_api_call(
            lambda s=current.strftime("%Y%m%d"), e=segment_end.strftime("%Y%m%d"): ak.bond_china_yield(start_date=s, end_date=e)
        )
        if raw is not None and not raw.empty:
            chunks.append(raw)
        current = segment_end + timedelta(days=1)

    if not chunks:
        logger.warning(f"_ak_bond_china_yield 无数据返回 (start={start}, end={end})")
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)

    # Pivot 长格式 → 宽格式：index=日期, columns=曲线名称
    # 需要对所有期限列（3月/6月/1年/3年/5年/7年/10年/30年）分别 pivot
    tenor_cols = [c for c in df.columns if c not in ("曲线名称", "日期")]
    if not tenor_cols:
        return pd.DataFrame()

    # 合并所有期限，列名格式："3年_中债中短期票据收益率曲线(AAA)"
    pivoted_parts = []
    for tenor in tenor_cols:
        sub = df.pivot(index="日期", columns="曲线名称", values=tenor)
        sub.columns = [f"{tenor}_{col}" for col in sub.columns]
        pivoted_parts.append(sub)

    result = pd.concat(pivoted_parts, axis=1, join="outer")
    result.index.name = "date"

    # 写入缓存
    try:
        from data_loader.cache_layer import cache_set as _cs
        _cs("bond_china_yield", result, expect_df=True, start=start, end=end)
    except Exception:
        pass

    return result


def _ak_bond_composite_index(indicator: str = "财富", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    中债综合指数
    优先从 Supabase 缓存读取，缓存 24 小时。

    Args:
        indicator: 指标类型（财富/总值）

    Returns:
        综合指数 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("bond_composite", ttl_seconds=86400, expect_df=True, indicator=indicator)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.bond_new_composite_index_cbond(indicator=indicator)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("bond_composite", result, expect_df=True, indicator=indicator)
        except Exception:
            pass

    return result


def _ak_cb_info(symbol: str, indicator: str = "基本信息", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    可转债基本信息
    带 Supabase 缓存（24h TTL）。
    
    Args:
        symbol: 可转债代码
        indicator: 指标类型
    
    Returns:
        可转债基本信息 DataFrame
    """
    # 尝试读缓存
    try:
        from data_loader.cache_layer import cache_get, cache_set
        cached = cache_get("cb_info", ttl_seconds=86400, expect_df=True, symbol=symbol, indicator=indicator)
        if cached is not None:
            return cached
    except Exception:
        pass

    result = safe_api_call(
        lambda: ak.bond_zh_cov_info(symbol=symbol, indicator=indicator)
    )

    # 写入缓存
    if result is not None:
        try:
            from data_loader.cache_layer import cache_set as _cs
            _cs("cb_info", result, expect_df=True, symbol=symbol, indicator=indicator)
        except Exception:
            pass

    return result
