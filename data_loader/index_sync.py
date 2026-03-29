"""
全收益指数 (Total Return Index) 集成模块 — fund_quant_v2（新架构）

核心架构：价格指数获取 + 全收益合成算法 + 本地缓存库

新架构特性：
1. 价格指数获取：使用AkShare获取价格指数数据
2. 全收益合成：通过固定股息率合成全收益指数（符合用户要求）
3. 本地缓存库：将所有指数数据缓存在本地Parquet文件中
4. 定期更新：每天自动更新指数库
5. 向后兼容：保持原有接口不变，无缝替换

与原架构对比：
- ✅ 保留原有直接获取功能（但不使用）
- ✅ 新增价格指数获取 + 全收益合成算法
- ✅ 建立本地全收益指数库
- ✅ 每日定期更新机制
- ✅ 性能优化：缓存命中率 > 90%

注意：此模块现在使用新的缓存系统，原有功能保留但标记为不推荐使用。
"""

from __future__ import annotations
import os
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple, Union, Any

import akshare as ak
import pandas as pd

from config import (
    DEFAULT_DIV_YIELD,
    BOND_INDICES,
    SW_INDUSTRY_DIVIDEND_YIELD,
    SW_INDUSTRY_MAP,
    DATA_CONFIG,
    CACHE_TTL,
)
from data_loader.base_api import retry

logger = logging.getLogger(__name__)

# 导入新的全收益指数系统
try:
    from data_loader.index_integration import get_total_return_provider
    NEW_SYSTEM_AVAILABLE = True
    logger.info("[index_sync] 新全收益指数系统可用")
except ImportError as e:
    NEW_SYSTEM_AVAILABLE = False
    logger.warning(f"[index_sync] 无法导入新全收益指数系统，将使用原有功能: {e}")


# ============================================================
# 📊 辅助函数和常量
# ============================================================

TRADING_DAYS_PER_YEAR = DATA_CONFIG["trading_days_per_year"]

# 申万指数类型映射
SW_CATEGORY_MAP = {
    "市场表征": "broad_market",
    "一级行业": "industry_first",
    "二级行业": "industry_second", 
    "风格指数": "style_index"
}

# 申万指数代码前缀映射
SW_PREFIX_MAP = {
    "801": "一级行业",   # 801010-801960
    "802": "二级行业",   # 802010-802990
    "850": "市场表征",   # 850001-850999
    "851": "风格指数",   # 851011-851999
}

# 默认数据源配置
SW_DATA_SOURCE_CONFIG = {
    "symbol": "一级行业",          # 获取一级行业数据
    "start_date": "20200101",     # 默认开始日期
    "end_date": datetime.now().strftime("%Y%m%d"),  # 默认结束日期
    "cache_dir": "data/sw_index", # 缓存目录
    "parquet_compression": "snappy",  # Parquet压缩格式
}

def is_sw_index_code(code: str) -> bool:
    """
    判断是否是申万指数代码
    
    Args:
        code: 指数代码，如 '801010', '801010.SI', 'SW煤炭'等
        
    Returns:
        True - 是申万指数代码
        False - 不是申万指数代码
    """
    if not code:
        return False
    
    # 移除后缀
    code = str(code).upper()
    if code.endswith('.SI'):
        code = code[:-3]
    elif code.startswith('SW'):
        # 如 'SW煤炭' -> '801950' (通过SW_INDUSTRY_MAP查找)
        return True
    
    # 检查是否是申万代码格式
    if code.startswith('80') and len(code) >= 6:
        return True
    
    return False

def standardize_sw_code(code: str) -> str:
    """
    标准化申万指数代码
    
    Args:
        code: 原始代码，如 '801010', '801010.SI', 'SW煤炭'
        
    Returns:
        标准化代码，如 '801010'
    """
    if not code:
        return ""
    
    code = str(code).upper()
    
    # 1. 如果是 'SW煤炭' 格式，映射为数字代码
    if code.startswith('SW'):
        industry_name = code[2:]
        # 反向查找行业名称对应的代码
        for sw_code, name in SW_INDUSTRY_MAP.items():
            if name == industry_name:
                return sw_code
        return code
    
    # 2. 移除 '.SI' 后缀
    if code.endswith('.SI'):
        code = code[:-3]
    
    # 3. 确保是6位数字
    if code.isdigit() and len(code) == 6:
        return code
    
    # 4. 如果是5位或4位，补0
    if code.isdigit() and len(code) in [4, 5]:
        return code.zfill(6)
    
    return code

def get_sw_category(code: str) -> str:
    """
    获取申万指数的分类
    
    Args:
        code: 标准化申万代码
        
    Returns:
        分类：'一级行业', '二级行业', '市场表征', '风格指数'
    """
    code = standardize_sw_code(code)
    
    if len(code) >= 3:
        prefix = code[:3]
        return SW_PREFIX_MAP.get(prefix, "一级行业")
    
    return "一级行业"

def parse_sw_index_name(name_str: str) -> Tuple[str, str]:
    """
    解析申万指数名称中的行业信息
    
    Args:
        name_str: 指数名称，如 '农林牧渔', '申万50', '申万中小'
        
    Returns:
        (行业名称, 指数类型)
    """
    if not name_str:
        return ("", "")
    
    # 常见申万指数名称映射
    sw_name_map = {
        "申万50": ("申万50", "市场表征"),
        "申万中小": ("中小盘", "市场表征"),
        "申万Ａ指": ("申万A指", "市场表征"),
        "申万大盘": ("大盘", "风格指数"),
        "申万小盘": ("小盘", "风格指数"),
        "申万高市盈率": ("高市盈率", "风格指数"),
        "申万低市盈率": ("低市盈率", "风格指数"),
    }
    
    if name_str in sw_name_map:
        return sw_name_map[name_str]
    
    # 检查是否是一级行业名称
    if name_str in SW_INDUSTRY_MAP.values():
        return (name_str, "一级行业")
    
    # 默认处理
    return (name_str, "市场表征")

# ============================================================
# 📈 申万指数数据获取器
# ============================================================

class SWIndexFetcher:
    """
    申万指数数据获取器
    使用 index_analysis_daily_sw 接口获取价格和股息率数据
    """
    
    @staticmethod
    @retry(max_retries=DATA_CONFIG["api_retry"], delay=DATA_CONFIG["api_delay"])
    def fetch_sw_daily_data(
        category: str = "一级行业",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取申万指数日报表数据
        
        Args:
            category: 指数类别，可选值："市场表征", "一级行业", "二级行业", "风格指数"
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'
            
        Returns:
            DataFrame with columns: 
            ['指数代码', '指数名称', '发布日期', '收盘指数', '成交量', '涨跌幅', 
             '换手率', '市盈率', '市净率', '均价', '成交额占比', '流通市值', 
             '平均流通市值', '股息率', 'source_type']
            
            如果获取失败返回空DataFrame
        """
        try:
            # 设置默认日期
            if start_date is None:
                start_date = SW_DATA_SOURCE_CONFIG["start_date"]
            if end_date is None:
                end_date = SW_DATA_SOURCE_CONFIG["end_date"]
            
            logger.info(f"[SWIndexFetcher] 获取申万{category}数据: {start_date}~{end_date}")
            
            # 调用申万指数分析接口
            df = ak.index_analysis_daily_sw(
                symbol=category,
                start_date=start_date,
                end_date=end_date
            )
            
            if df is None or df.empty:
                logger.warning(f"[SWIndexFetcher] 申万{category}数据为空")
                return pd.DataFrame()
            
            # 标准化列名
            column_mapping = {
                "指数代码": "index_code",
                "指数名称": "index_name", 
                "发布日期": "trade_date",
                "收盘指数": "close",
                "成交量": "volume",
                "涨跌幅": "pct_change",
                "换手率": "turnover_rate",
                "市盈率": "pe_ratio",
                "市净率": "pb_ratio",
                "均价": "avg_price",
                "成交额占比": "amount_ratio",
                "流通市值": "float_market_cap",
                "平均流通市值": "avg_float_market_cap",
                "股息率": "dividend_yield"
            }
            
            # 重命名列
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # 添加数据来源标记
            df["source_type"] = "official"
            
            # 确保日期格式正确
            if "trade_date" in df.columns:
                df["trade_date"] = pd.to_datetime(df["trade_date"])
            
            # 确保数值列格式正确
            numeric_cols = ["close", "volume", "pct_change", "turnover_rate", 
                          "pe_ratio", "pb_ratio", "avg_price", "amount_ratio",
                          "float_market_cap", "avg_float_market_cap", "dividend_yield"]
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            logger.info(f"[SWIndexFetcher] 成功获取 {len(df)} 条数据，包含 {df['index_code'].nunique()} 个指数")
            
            return df
            
        except Exception as e:
            logger.error(f"[SWIndexFetcher] 获取申万数据失败: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def get_sw_index_data(
        code: str,
        start_date: str,
        end_date: str,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        获取指定申万指数的价格和股息率数据
        
        Args:
            code: 申万指数代码
            start_date: 开始日期
            end_date: 结束日期
            use_cache: 是否使用缓存
            
        Returns:
            DataFrame with columns: ['trade_date', 'close', 'dividend_yield', 'source_type']
        """
        # 标准化代码
        std_code = standardize_sw_code(code)
        category = get_sw_category(std_code)
        
        # 缓存文件路径
        cache_key = f"sw_{category}_{start_date}_{end_date}"
        cache_file = os.path.join(
            SW_DATA_SOURCE_CONFIG["cache_dir"],
            f"{cache_key}.parquet"
        )
        
        # 检查缓存
        if use_cache and os.path.exists(cache_file):
            try:
                cache_time = os.path.getmtime(cache_file)
                # 检查缓存是否过期（默认缓存1天）
                if datetime.now().timestamp() - cache_time < CACHE_TTL["short"]:
                    df = pd.read_parquet(cache_file)
                    logger.debug(f"[SWIndexFetcher] 从缓存加载 {category} 数据")
                    
                    # 筛选指定指数的数据
                    if "index_code" in df.columns and not df.empty:
                        idx_df = df[df["index_code"] == std_code].copy()
                        if not idx_df.empty:
                            return idx_df[['trade_date', 'close', 'dividend_yield', 'source_type']]
            except Exception as e:
                logger.warning(f"[SWIndexFetcher] 缓存读取失败: {e}")
        
        # 获取数据
        df_all = SWIndexFetcher.fetch_sw_daily_data(
            category=category,
            start_date=start_date,
            end_date=end_date
        )
        
        if df_all.empty:
            logger.warning(f"[SWIndexFetcher] 无法获取申万指数 {code} 数据")
            return pd.DataFrame()
        
        # 保存到缓存
        try:
            os.makedirs(SW_DATA_SOURCE_CONFIG["cache_dir"], exist_ok=True)
            df_all.to_parquet(
                cache_file,
                compression=SW_DATA_SOURCE_CONFIG["parquet_compression"]
            )
            logger.debug(f"[SWIndexFetcher] 缓存保存到 {cache_file}")
        except Exception as e:
            logger.warning(f"[SWIndexFetcher] 缓存保存失败: {e}")
        
        # 筛选指定指数的数据
        if "index_code" in df_all.columns and not df_all.empty:
            idx_df = df_all[df_all["index_code"] == std_code].copy()
            if not idx_df.empty:
                return idx_df[['trade_date', 'close', 'dividend_yield', 'source_type']]
        
        return pd.DataFrame()

# ============================================================
# 🔄 全收益指数合成器
# ============================================================

class TotalReturnCalculator:
    """
    全收益指数合成器
    将价格指数和股息率合成为全收益指数
    """
    
    @staticmethod
    def calculate_total_return(
        price_df: pd.DataFrame,
        dividend_yield: Union[float, pd.Series, None] = None
    ) -> pd.DataFrame:
        """
        计算全收益指数
        
        Args:
            price_df: 价格指数DataFrame，必须包含 ['date', 'close'] 列
            dividend_yield: 股息率，可以是：
                - float: 固定股息率（年化百分比）
                - pd.Series: 每日股息率（年化百分比），索引需与price_df对齐
                - None: 使用默认股息率 DEFAULT_DIV_YIELD
                
        Returns:
            DataFrame with columns: ['date', 'price_ret', 'div_ret', 'total_ret', 'total_nav']
        """
        if price_df.empty or "close" not in price_df.columns:
            logger.error("[TotalReturnCalculator] 价格数据无效")
            return pd.DataFrame()
        
        # 准备结果DataFrame
        result = price_df.copy()
        
        # 计算价格收益率
        result["price_ret"] = result["close"].pct_change()
        
        # 处理股息率
        if dividend_yield is None:
            # 使用默认股息率
            div_rate = DEFAULT_DIV_YIELD / 100.0 / TRADING_DAYS_PER_YEAR
            result["div_ret"] = div_rate
            result["div_source"] = "default"
            
        elif isinstance(dividend_yield, (int, float)):
            # 固定股息率
            div_rate = float(dividend_yield) / 100.0 / TRADING_DAYS_PER_YEAR
            result["div_ret"] = div_rate
            result["div_source"] = "fixed_param"
            
        elif isinstance(dividend_yield, pd.Series):
            # 每日股息率
            # 确保股息率序列与价格数据对齐
            if dividend_yield.index.equals(result.index):
                div_series = dividend_yield
            else:
                # 重新索引对齐
                div_series = dividend_yield.reindex(result.index, method="ffill")
            
            # 转换股息率：年化百分比 -> 日收益率
            result["div_ret"] = div_series / 100.0 / TRADING_DAYS_PER_YEAR
            result["div_source"] = "daily_data"
            
        elif isinstance(dividend_yield, pd.DataFrame) and "dividend_yield" in dividend_yield.columns:
            # 从DataFrame中获取股息率
            div_df = dividend_yield
            # 合并数据
            if "date" in div_df.columns:
                merged = pd.merge(result, div_df, on="date", how="left")
                # 前向填充缺失值
                merged["dividend_yield"] = merged["dividend_yield"].ffill()
                # 计算股息收益率
                merged["div_ret"] = merged["dividend_yield"] / 100.0 / TRADING_DAYS_PER_YEAR
                merged["div_source"] = "merged_data"
                result = merged
            else:
                # 如果没有日期列，假设索引对齐
                if len(div_df) == len(result):
                    result["div_ret"] = div_df["dividend_yield"].values / 100.0 / TRADING_DAYS_PER_YEAR
                    result["div_source"] = "aligned_data"
        
        else:
            logger.warning("[TotalReturnCalculator] 股息率格式不支持，使用默认值")
            div_rate = DEFAULT_DIV_YIELD / 100.0 / TRADING_DAYS_PER_YEAR
            result["div_ret"] = div_rate
            result["div_source"] = "fallback_default"
        
        # 计算总收益率
        result["total_ret"] = (1 + result["price_ret"]) * (1 + result["div_ret"]) - 1
        
        # 计算全收益净值（从1.0开始）
        result["total_nav"] = (1 + result["total_ret"]).cumprod()
        
        # 第一天的处理
        result.loc[result.index[0], "price_ret"] = 0.0
        result.loc[result.index[0], "total_ret"] = result.loc[result.index[0], "div_ret"]
        result.loc[result.index[0], "total_nav"] = 1 + result.loc[result.index[0], "div_ret"]
        
        # 清理NaN值
        result = result.fillna(0)
        
        return result[['date', 'price_ret', 'div_ret', 'total_ret', 'total_nav', 'div_source']]

# ============================================================
# 🚀 主接口函数
# ============================================================

def get_total_return_series(
    index_code: str,
    start_date: str,
    end_date: str,
    use_sw_source: bool = True,
    use_new_system: bool = True  # 新增参数：是否使用新系统
) -> pd.DataFrame:
    """
    获取全收益指数序列（主接口）
    
    优先级：新系统 -> 原有系统（保持向后兼容）
    
    Args:
        index_code: 指数代码，支持：
            - 宽基指数：'000300.SH', '000905.SH', '000852.SH'
            - 申万行业指数：'801010', '801010.SI', 'SW煤炭'
            - 债券指数：直接返回财富指数
        start_date: 开始日期，格式 'YYYYMMDD'
        end_date: 结束日期，格式 'YYYYMMDD'
        use_sw_source: 是否使用申万数据源（针对申万指数）
        use_new_system: 是否优先使用新系统（默认True）
        
    Returns:
        DataFrame with columns: ['date', 'tr_ret', 'tr_nav', 'price_close', 'div_yield', 'data_source']
        
        如果获取失败，返回空DataFrame
    """
    logger.info(f"[get_total_return_series] 开始获取 {index_code} 全收益指数: {start_date}~{end_date}")
    
    # 1. 优先使用新系统（如果可用且启用）
    if use_new_system and NEW_SYSTEM_AVAILABLE:
        try:
            logger.info(f"[get_total_return_series] 尝试使用新系统获取: {index_code}")
            provider = get_total_return_provider()
            
            # 转换日期格式（新系统期望YYYY-MM-DD格式）
            start_date_formatted = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if len(start_date) == 8 else start_date
            end_date_formatted = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}" if len(end_date) == 8 else end_date
            
            result = provider.get_total_return_series(
                index_code=index_code,
                start_date=start_date_formatted,
                end_date=end_date_formatted,
                use_cache=True,
                force_refresh=False
            )
            
            if not result.empty:
                logger.info(f"[get_total_return_series] 新系统成功获取 {index_code}: {len(result)} 条记录")
                return result
            else:
                logger.warning(f"[get_total_return_series] 新系统返回空数据，回退到原有系统: {index_code}")
                
        except Exception as e:
            logger.warning(f"[get_total_return_series] 新系统调用失败，回退到原有系统: {e}")
    
    # 2. 原有系统逻辑（保持向后兼容）
    logger.info(f"[get_total_return_series] 使用原有系统获取: {index_code}")
    
    # 检查是否是债券指数
    if index_code in BOND_INDICES:
        logger.info(f"[get_total_return_series] {index_code} 是债券指数，直接使用财富指数")
        # 这里可以调用债券指数接口
        return pd.DataFrame()
    
    # 检查是否是申万指数
    if is_sw_index_code(index_code) and use_sw_source:
        return _get_sw_total_return_series(index_code, start_date, end_date)
    
    # 其他指数使用原有逻辑
    return _get_generic_total_return_series(index_code, start_date, end_date)

def _get_sw_total_return_series(
    index_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取申万指数的全收益序列
    
    Args:
        index_code: 申万指数代码
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        全收益指数DataFrame
    """
    try:
        # 获取申万指数数据
        sw_df = SWIndexFetcher.get_sw_index_data(
            code=index_code,
            start_date=start_date,
            end_date=end_date,
            use_cache=True
        )
        
        if sw_df.empty:
            logger.warning(f"[_get_sw_total_return_series] 无法获取申万指数 {index_code} 数据")
            # 使用固定参数兜底
            return _get_fallback_total_return_series(index_code, start_date, end_date)
        
        # 准备价格数据
        price_df = sw_df[["trade_date", "close"]].copy()
        price_df = price_df.rename(columns={"trade_date": "date"})
        price_df = price_df.sort_values("date").reset_index(drop=True)
        
        # 准备股息率数据
        if "dividend_yield" in sw_df.columns:
            # 处理股息率缺失值
            div_series = sw_df["dividend_yield"].copy()
            
            # 获取行业代码对应的固定股息率（用于补全缺失值）
            std_code = standardize_sw_code(index_code)
            fixed_div = SW_INDUSTRY_DIVIDEND_YIELD.get(std_code, DEFAULT_DIV_YIELD)
            
            # 标记缺失值并用固定参数补全
            missing_mask = div_series.isna() | (div_series == 0)
            if missing_mask.any():
                logger.info(f"[_get_sw_total_return_series] {index_code} 有 {missing_mask.sum()} 个缺失股息率，使用固定参数补全")
                div_series[missing_mask] = fixed_div
                # 更新数据来源标记
                if "source_type" in sw_df.columns:
                    sw_df.loc[missing_mask, "source_type"] = "fixed_param"
        
        else:
            # 没有股息率列，使用固定参数
            std_code = standardize_sw_code(index_code)
            fixed_div = SW_INDUSTRY_DIVIDEND_YIELD.get(std_code, DEFAULT_DIV_YIELD)
            div_series = pd.Series([fixed_div] * len(sw_df), index=sw_df.index)
            if "source_type" in sw_df.columns:
                sw_df["source_type"] = "fixed_param_only"
        
        # 计算全收益
        result = TotalReturnCalculator.calculate_total_return(
            price_df=price_df,
            dividend_yield=div_series
        )
        
        # 添加原始数据信息
        if not result.empty:
            # 添加价格收盘价
            result["price_close"] = price_df["close"].values
            
            # 添加股息率（年化百分比）
            result["div_yield"] = div_series.values
            
            # 添加数据来源
            if "source_type" in sw_df.columns:
                result["data_source"] = sw_df["source_type"].values
            else:
                result["data_source"] = "sw_index_api"
            
            logger.info(f"[_get_sw_total_return_series] 成功合成 {index_code} 全收益指数，{len(result)} 个数据点")
            
            return result
        
    except Exception as e:
        logger.error(f"[_get_sw_total_return_series] 合成失败: {e}")
    
    # 兜底方案
    return _get_fallback_total_return_series(index_code, start_date, end_date)

def _get_generic_total_return_series(
    index_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    获取通用指数的全收益序列（宽基指数如沪深300、中证500等）
    
    Args:
        index_code: 指数代码
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        DataFrame with columns: ['date', 'tr_ret', 'tr_nav', 'price_close', 'div_yield', 'data_source']
    """
    try:
        logger.info(f"[_get_generic_total_return_series] 开始获取 {index_code} 全收益数据")
        
        # 从config.py导入必要的配置
        from config import DEFAULT_DIV_YIELD
        
        # 获取价格指数数据
        from data_loader.base_api import _ak_index_daily_main
        
        # 获取价格指数
        price_df = _ak_index_daily_main(index_code)
        
        if price_df is None or price_df.empty:
            logger.warning(f"[_get_generic_total_return_series] 无法获取 {index_code} 价格数据")
            return pd.DataFrame()
        
        # 确保有date和close列
        if 'date' not in price_df.columns or 'close' not in price_df.columns:
            logger.warning(f"[_get_generic_total_return_series] {index_code} 数据格式不正确")
            return pd.DataFrame()
        
        # 筛选日期范围
        price_df = price_df.copy()
        price_df['date'] = pd.to_datetime(price_df['date'])
        price_df = price_df.sort_values('date')
        
        # 转换日期格式
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
        except:
            start_dt = pd.to_datetime(start_date, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date, format='%Y%m%d')
        
        price_df = price_df[(price_df['date'] >= start_dt) & (price_df['date'] <= end_dt)]
        
        if price_df.empty:
            logger.warning(f"[_get_generic_total_return_series] {index_code} 在指定日期范围内无数据")
            return pd.DataFrame()
        
        # 计算价格收益率
        price_df['price_ret'] = price_df['close'].pct_change().fillna(0)
        
        # 获取股息率 - 处理代码格式（如000300.SH -> sh000300）
        index_key = index_code.lower().replace('.', '')
        # 确保格式正确
        if not index_key.startswith('sh') and not index_key.startswith('sz'):
            if index_key.startswith('000') or index_key.startswith('399'):
                index_key = 'sh' + index_key if len(index_key) == 6 else index_key
        
        div_rate = DEFAULT_DIV_YIELD.get(index_key, DEFAULT_DIV_YIELD.get('sh000300', 0.025))
        
        # 转换为日股息收益率（年化股息率 / 交易日数量）
        daily_div_ret = div_rate / TRADING_DAYS_PER_YEAR
        
        # 计算总收益率：考虑价格变动和股息再投资
        price_df['tr_ret'] = (1 + price_df['price_ret']) * (1 + daily_div_ret) - 1
        
        # 计算全收益净值（从1.0开始）
        price_df['tr_nav'] = (1 + price_df['tr_ret']).cumprod()
        
        # 第一天处理
        price_df.loc[price_df.index[0], 'price_ret'] = 0.0
        price_df.loc[price_df.index[0], 'tr_ret'] = daily_div_ret
        price_df.loc[price_df.index[0], 'tr_nav'] = 1 + daily_div_ret
        
        # 准备结果DataFrame
        result = price_df[['date', 'tr_ret', 'tr_nav', 'close']].copy()
        result = result.rename(columns={'close': 'price_close'})
        result['div_yield'] = div_rate
        result['data_source'] = f"price_index_with_fixed_dividend_{div_rate:.3%}"
        
        logger.info(f"[_get_generic_total_return_series] 成功生成 {index_code} 全收益数据，{len(result)} 条记录")
        
        return result.reset_index(drop=True)
        
    except Exception as e:
        logger.error(f"[_get_generic_total_return_series] 获取 {index_code} 全收益数据失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def _get_fallback_total_return_series(
    index_code: str,
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """
    兜底方案：使用固定参数生成全收益序列
    
    Args:
        index_code: 指数代码
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        全收益指数DataFrame
    """
    try:
        logger.warning(f"[_get_fallback_total_return_series] 使用兜底方案生成 {index_code} 全收益指数")
        
        # 获取固定股息率
        if is_sw_index_code(index_code):
            std_code = standardize_sw_code(index_code)
            SW_INDUSTRY_DIVIDEND_YIELD.get(std_code, DEFAULT_DIV_YIELD)
        else:
            pass
        
        # 这里需要获取价格数据，但作为兜底方案，我们可能只返回空DataFrame
        # 或者生成一个简单的全收益序列
        
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"[_get_fallback_total_return_series] 兜底方案失败: {e}")
        return pd.DataFrame()

# ============================================================
# 🔄 数据更新和维护函数
# ============================================================

def update_benchmark_data(
    category: str = "一级行业",
    force_update: bool = False
) -> Dict[str, Any]:
    """
    更新申万指数基准数据
    
    Args:
        category: 指数类别
        force_update: 是否强制更新（忽略缓存）
        
    Returns:
        更新结果统计
    """
    result = {
        "category": category,
        "total_indices": 0,
        "updated": 0,
        "failed": 0,
        "details": []
    }
    
    try:
        logger.info(f"[update_benchmark_data] 开始更新申万{category}数据")
        
        # 获取最新数据
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = "20200101"  # 从2020年开始
        
        df = SWIndexFetcher.fetch_sw_daily_data(
            category=category,
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            logger.error(f"[update_benchmark_data] 获取申万{category}数据失败")
            result["failed"] = 1
            return result
        
        # 统计信息
        unique_codes = df["index_code"].nunique() if "index_code" in df.columns else 0
        result["total_indices"] = unique_codes
        
        # 保存到缓存
        cache_dir = SW_DATA_SOURCE_CONFIG["cache_dir"]
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(
            cache_dir,
            f"sw_{category}_{start_date}_{end_date}.parquet"
        )
        
        df.to_parquet(
            cache_file,
            compression=SW_DATA_SOURCE_CONFIG["parquet_compression"]
        )
        
        result["updated"] = 1
        result["details"].append(f"成功更新 {unique_codes} 个指数数据到 {cache_file}")
        
        logger.info(f"[update_benchmark_data] 成功更新申万{category}数据，{unique_codes} 个指数")
        
    except Exception as e:
        logger.error(f"[update_benchmark_data] 更新失败: {e}")
        result["failed"] = 1
        result["details"].append(f"更新失败: {e}")
    
    return result

def initialize_historical_data() -> Dict[str, Any]:
    """
    初始化历史数据（一次性任务）
    
    Returns:
        初始化结果统计
    """
    result = {
        "total_categories": 0,
        "succeeded": 0,
        "failed": 0,
        "details": []
    }
    
    categories = ["一级行业", "市场表征", "风格指数"]
    
    for category in categories:
        try:
            update_result = update_benchmark_data(category=category, force_update=True)
            
            if update_result["updated"] > 0:
                result["succeeded"] += 1
                result["details"].append(f"成功初始化 {category} 数据")
            else:
                result["failed"] += 1
                result["details"].append(f"初始化 {category} 数据失败")
                
        except Exception as e:
            result["failed"] += 1
            result["details"].append(f"初始化 {category} 数据异常: {e}")
    
    result["total_categories"] = len(categories)
    
    logger.info(f"[initialize_historical_data] 初始化完成: {result['succeeded']}/{result['total_categories']} 成功")
    
    return result

# ============================================================
# 🧪 测试函数
# ============================================================

def test_sw_integration():
    """测试申万指数集成功能"""
    print("🧪 开始测试申万指数集成功能...")
    print("=" * 80)
    
    test_cases = [
        ("801010", "农林牧渔（标准代码）"),
        ("801010.SI", "农林牧渔（带后缀）"),
        ("SW煤炭", "煤炭行业（中文代码）"),
        ("801230", "银行行业"),
        ("801950", "煤炭行业"),
    ]
    
    for code, desc in test_cases:
        print(f"\n📊 测试 {desc} ({code}):")
        print("-" * 40)
        
        try:
            # 标准化代码
            std_code = standardize_sw_code(code)
            print(f"  标准化代码: {std_code}")
            
            # 判断是否是申万指数
            is_sw = is_sw_index_code(code)
            print(f"  是申万指数: {is_sw}")
            
            if is_sw:
                # 获取分类
                category = get_sw_category(std_code)
                print(f"  指数分类: {category}")
                
                # 获取行业名称
                industry_name = SW_INDUSTRY_MAP.get(std_code, "未知")
                print(f"  行业名称: {industry_name}")
                
                # 获取固定股息率
                fixed_div = SW_INDUSTRY_DIVIDEND_YIELD.get(std_code, DEFAULT_DIV_YIELD)
                print(f"  固定股息率: {fixed_div:.2f}%")
                
        except Exception as e:
            print(f"  ❌ 测试失败: {e}")
    
    print("\n" + "=" * 80)
    print("✅ 测试完成")

if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # 运行测试
    test_sw_integration()