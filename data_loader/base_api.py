"""
基础 API 工具函数
提供缓存、超时、重试等装饰器

数据来源：本地 SQLite 数据库（data/fund_data.db）
零网络依赖，所有查询走 db_accessor.py
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import Callable, Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# 默认超时时间（秒）— 保留给 safe_api_call 等通用工具
DEFAULT_TIMEOUT = 10.0
MAX_RETRIES = 3
RETRY_DELAY = 2.0


# =============================================================================
# 装饰器 & 通用工具（不涉及数据源）
# =============================================================================

def cached(ttl: int = 3600):
    """缓存装饰器（简化版，暂时不实现）"""
    def decorator(func: Callable) -> Callable:
        return func
    return decorator


def retry(max_retries: int = 3, delay: float = 2.0, logger_instance: Optional[logging.Logger] = None):
    """重试装饰器"""
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

            _logger.error(f"{func.__name__} 在 {max_retries} 次重试后仍然失败")
            raise last_exception
        return wrapper
    return decorator


def parse_pct(value: Any) -> float:
    """解析百分比字符串"""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip().replace('%', '').replace('％', '')
        for pattern in ['（', '）', '(', ')']:
            if pattern in value:
                value = value.split(pattern)[0].strip()
        if not value or value in ('---', '-', 'N/A'):
            return 0.0
        try:
            return float(value) / 100.0
        except ValueError:
            logger.warning(f"parse_pct 无法解析 '{value}'")
            return 0.0
    return 0.0


def safe_df(df: Optional[pd.DataFrame], default_columns: Optional[list] = None) -> pd.DataFrame:
    """安全的 DataFrame 处理"""
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
# safe_api_call — 保留接口，内部不再调用 AkShare
# =============================================================================

def safe_api_call(api_func: Callable, timeout_seconds: float = DEFAULT_TIMEOUT, max_retries: int = 3) -> Any:
    """
    安全的 API 调用包装器（保留接口签名，供各 loader 使用）。
    在 SQLite 模式下，这个函数仅作为兼容层存在。
    """
    return api_func()


def call_with_timeout(func, timeout=10.0, args=(), kwargs=None):
    """兼容层：保留接口供 equity_holdings_loader 等使用"""
    if kwargs is None:
        kwargs = {}
    return func(*args, **kwargs)


def timeout(seconds: float = DEFAULT_TIMEOUT):
    """超时装饰器（保留接口）"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
# SQLite 数据访问函数（替代原 AkShare API 调用）
# =============================================================================

def _get_db():
    """延迟导入 db_accessor，避免循环依赖"""
    from data_loader.db_accessor import DB
    return DB


# ----- 基金基本信息 -----

def _ak_fund_basic_xq(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金基础信息（原雪球源）。
    现从 fund_individual_basic_xq 表读取。
    返回 DataFrame，列: item, value, 基金代码
    """
    try:
        db = _get_db()
        df = db.query_df(
            'SELECT * FROM fund_individual_basic_xq WHERE "基金代码" = ?',
            (symbol,),
        )
        if df.empty:
            return None
        return df
    except Exception as e:
        logger.warning(f"[_ak_fund_basic_xq] {symbol} 读取失败: {e}")
        return None


def _ak_fund_name_em(symbol: str, *args, **kwargs) -> Optional[str]:
    """基金名称（原东方财富）。现从 fund_name_em 表读取。"""
    try:
        from data_loader.db_accessor import get_fund_name
        return get_fund_name(symbol)
    except Exception as e:
        logger.warning(f"[_ak_fund_name_em] {symbol} 读取失败: {e}")
        return None


def _ak_fund_list_em(*args, **kwargs) -> Optional[pd.DataFrame]:
    """全量基金代码和名称列表。现从 fund_name_em 表读取。"""
    try:
        db = _get_db()
        return db.query_df(
            'SELECT "基金代码", "基金简称" AS "基金名称" FROM fund_name_em'
        )
    except Exception as e:
        logger.warning(f"[_ak_fund_list_em] 读取失败: {e}")
        return None


def get_fund_type_em(symbol: str) -> Optional[str]:
    """获取基金权威类型（如 "混合型-偏股"）。现从 fund_name_em 表读取。"""
    try:
        from data_loader.db_accessor import get_fund_type
        return get_fund_type(symbol)
    except Exception as e:
        logger.warning(f"[get_fund_type_em] {symbol} 读取失败: {e}")
        return None


def _ak_fund_scale_sina(symbol: str, *args, **kwargs) -> Optional[float]:
    """基金规模 — 现从 fund_meta 表读取 latest_aum。"""
    try:
        from data_loader.db_accessor import get_fund_basic_info
        info = get_fund_basic_info(symbol)
        if info and info.get("latest_aum"):
            try:
                return float(info["latest_aum"])
            except (ValueError, TypeError):
                pass
    except Exception as e:
        logger.warning(f"[_ak_fund_scale_sina] {symbol} 读取失败: {e}")
    return None


def _ak_fund_fee_em(symbol: str = "000001", indicator: str = "运作费用", *args, **kwargs) -> Optional[pd.DataFrame]:
    """基金费率。现从 fund_fee_em 表读取。"""
    try:
        from data_loader.db_accessor import get_fund_fee
        df = get_fund_fee(symbol)
        if not df.empty:
            return df
    except Exception as e:
        logger.warning(f"[_ak_fund_fee_em] {symbol} 读取失败: {e}")
    return None


def _ak_fund_nav(symbol: str, indicator: str = "单位净值走势", *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金净值数据（原东方财富）。
    indicator="单位净值走势" → fund_nav 表
    indicator="累计净值走势" → fund_nav_acc 表
    """
    try:
        from data_loader.db_accessor import get_fund_nav, get_fund_nav_acc
        if "累计" in indicator:
            df = get_fund_nav_acc(symbol)
        else:
            df = get_fund_nav(symbol)
        if df.empty:
            return None
        return df
    except Exception as e:
        logger.warning(f"[_ak_fund_nav] {symbol} 读取失败: {e}")
        return None


def _ak_fund_purchase_status(symbol: str, *args, **kwargs) -> Optional[dict]:
    """基金申购赎回状态。现从 fund_purchase_em 表读取。"""
    try:
        from data_loader.db_accessor import get_fund_purchase_status
        row = get_fund_purchase_status(symbol)
        if row:
            return {
                'purchase_status': row.get('申购状态', ''),
                'redeem_status': row.get('赎回状态', ''),
                'min_purchase': float(row.get('购买起点', 0.0) or 0.0)
            }
    except Exception as e:
        logger.warning(f"[_ak_fund_purchase_status] {symbol} 读取失败: {e}")
    return None


def _ak_fund_holdings_stock(symbol: str, date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """基金股票持仓。现从 fund_stock_holdings 表读取。"""
    try:
        from data_loader.db_accessor import get_stock_holdings
        df = get_stock_holdings(symbol)
        if df.empty:
            return None
        return df
    except Exception as e:
        logger.warning(f"[_ak_fund_holdings_stock] {symbol} 读取失败: {e}")
        return None


def _ak_fund_holdings_bond(symbol: str, date: str = "2024", *args, **kwargs) -> Optional[pd.DataFrame]:
    """基金债券持仓。现从 fund_bond_holdings 表读取。"""
    try:
        from data_loader.db_accessor import get_bond_holdings
        df = get_bond_holdings(symbol)
        if df.empty:
            return None
        return df
    except Exception as e:
        logger.warning(f"[_ak_fund_holdings_bond] {symbol} 读取失败: {e}")
        return None


def _ak_fund_asset_allocation(symbol: str, date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    基金资产配置（原雪球源）。
    现从 fund_hold_detail 表读取（列: 资产类型, 仓位占比, 基金代码, date）。
    """
    try:
        from data_loader.db_accessor import get_asset_allocation
        df = get_asset_allocation(symbol)
        if df.empty:
            return pd.DataFrame(columns=["资产类型", "占净值比例(%)"])
        # 标准化列名以兼容下游
        if "资产类型" in df.columns and "仓位占比" in df.columns:
            df = df.rename(columns={"仓位占比": "占净值比例(%)"})
        return df
    except Exception as e:
        logger.warning(f"[_ak_fund_asset_allocation] {symbol} 读取失败: {e}")
        return pd.DataFrame(columns=["资产类型", "占净值比例(%)"])


# ----- 指数数据 -----

def _ak_index_daily_main(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """
    A股指数日线行情。
    现从 style_idx / total_return_idx 表读取。
    """
    try:
        from data_loader.db_accessor import get_style_index, get_total_return_index
        # 尝试全收益指数
        df = get_total_return_index(symbol)
        if not df.empty and "date" in df.columns and "close" in df.columns:
            # 标准化列名和类型
            df["date"] = pd.to_datetime(df["date"])
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["close"])
            return df

        # 尝试风格指数
        df = get_style_index(symbol)
        if not df.empty and "date" in df.columns and "close" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["close"])
            return df
    except Exception as e:
        logger.warning(f"[_ak_index_daily_main] {symbol} 读取失败: {e}")
    return pd.DataFrame()


def _ak_index_daily_em(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """指数日线行情（东方财富）— 未实现，返回空 DataFrame。"""
    return pd.DataFrame()


def _ak_hk_index_daily(symbol: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """港股指数日线 — SQLite 中无对应表，返回空 DataFrame。"""
    return pd.DataFrame()


# ----- ETF -----

def _ak_etf_hist_em(symbol: str, period: str = "daily", start_date: str = None,
                     end_date: str = None, adjust: str = "qfq", *args, **kwargs) -> Optional[pd.DataFrame]:
    """ETF 二级市场行情。现从 fund_etf_hist 表读取。"""
    try:
        from data_loader.db_accessor import get_etf_hist
        df = get_etf_hist(symbol)
        if df.empty:
            return None
        # 日期范围过滤
        if start_date:
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= pd.Timestamp(start_date)]
        if end_date:
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] <= pd.Timestamp(end_date)]
        return df
    except Exception as e:
        logger.warning(f"[_ak_etf_hist_em] {symbol} 读取失败: {e}")
        return None


# ----- 债券数据 -----

def _ak_bond_us_rate(start_date: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """美国国债收益率 — SQLite 中无对应表，返回空 DataFrame。"""
    return None


def _ak_bond_china_yield(start: str, end: str, *args, **kwargs) -> Optional[pd.DataFrame]:
    """中债收益率曲线。现从 bond_china_yield 表读取。"""
    try:
        from data_loader.db_accessor import get_bond_china_yield_range
        df = get_bond_china_yield_range(start, end)
        if df.empty:
            return pd.DataFrame()
        # 确保日期索引
        if "date" in df.columns:
            df.index.name = "date"
        return df
    except Exception as e:
        logger.warning(f"[_ak_bond_china_yield] 读取失败: {e}")
        return pd.DataFrame()


def _ak_bond_composite_index(indicator: str = "财富", *args, **kwargs) -> Optional[pd.DataFrame]:
    """中债综合指数。现从 bond_daily_hist 表读取。"""
    try:
        from data_loader.db_accessor import get_bond_daily_hist
        # 默认取中债综合指数
        df = get_bond_daily_hist("CBA00127")
        if not df.empty:
            return df
        # 如果找不到，返回全量
        return get_bond_daily_hist(None)
    except Exception as e:
        logger.warning(f"[_ak_bond_composite_index] 读取失败: {e}")
        return pd.DataFrame()


# ----- 可转债数据 -----

def _ak_cb_info(symbol: str, indicator: str = "基本信息", *args, **kwargs) -> Optional[pd.DataFrame]:
    """可转债基本信息。现从 cb_info 表读取。"""
    try:
        from data_loader.db_accessor import get_cb_info
        row = get_cb_info(symbol)
        if row:
            return pd.DataFrame([row])
    except Exception as e:
        logger.warning(f"[_ak_cb_info] {symbol} 读取失败: {e}")
    return None


def load_cb_index_hist(symbol: str = "000832", start_date: str = "20200101",
                       end_date: str = None) -> pd.DataFrame:
    """中证转债指数历史日线。现从 total_return_idx 表读取。"""
    try:
        from data_loader.db_accessor import get_total_return_index
        df = get_total_return_index(symbol)
        if df.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        if start_date:
            df = df[df["date"] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df["date"] <= pd.Timestamp(end_date)]
        return df.sort_values("date").reset_index(drop=True)
    except Exception as e:
        logger.warning(f"[load_cb_index_hist] {symbol} 读取失败: {e}")
        return pd.DataFrame()


def load_cb_value_analysis() -> pd.DataFrame:
    """全市场可转债价值分析数据。现从 cb_value_analysis 表读取。"""
    try:
        from data_loader.db_accessor import get_cb_value_analysis
        df = get_cb_value_analysis()
        if df.empty:
            return pd.DataFrame(columns=["date", "收盘价", "纯债价值", "转股价值", "纯债溢价率", "转股溢价率"])
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)
    except Exception as e:
        logger.warning(f"[load_cb_value_analysis] 读取失败: {e}")
        return pd.DataFrame()
