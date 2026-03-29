"""
全收益指数 (Total Return Index) 动态合成模块 — fund_quant_v2

核心逻辑：价格指数 + 历史股息率 = 全收益指数
历史股息率 = 当日年化股息率 / 252

功能要求：
1. 多指数支持：支持沪深300 (000300)、中证500 (000905)、中证1000 (001000) 以及常见的行业指数
2. 数据抓取：index_zh_a_hist（价格指数） + index_analysis_daily（股息率）
3. 本地缓存：Parquet 格式，增量更新
4. 兜底机制：接口失败时使用预设常数股息率
5. 输出规范：date 和 tr_ret（全收益日收益率）的干净 DataFrame

注意：
1. 股息率单位统一化处理（百分数转小数）
2. 债券指数直接使用财富指数版本（无需合成）
3. Parquet 格式保存时间序列数据
"""

from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Union

import akshare as ak
import pandas as pd

from config import (
    DEFAULT_DIV_YIELD,
    BOND_INDICES,
    SW_INDEX_SUFFIXES,
    SW_INDUSTRY_DIVIDEND_YIELD,
    SW_INDUSTRY_MAP,
    DATA_CONFIG,
)
from data_loader.base_api import retry

logger = logging.getLogger(__name__)


# ============================================================
# 📊 辅助函数和常量
# ============================================================

TRADING_DAYS_PER_YEAR = DATA_CONFIG["trading_days_per_year"]

# 指数代码标准化映射（统一到AkShare格式）
INDEX_CODE_STANDARDIZATION = {
    "000300": "sh000300",     # 沪深300
    "000300.SH": "sh000300",
    "000905": "sh000905",     # 中证500
    "000905.SH": "sh000905",
    "001000": "sh000852",     # 中证1000（实际使用sh000852）
    "001000.SH": "sh000852",
    "000016": "sh000016",     # 上证50
    "000016.SH": "sh000016",
    "399006": "sz399006",     # 创业板指
    "399006.SZ": "sz399006",
    "000688": "sh000688",     # 科创50
    "000688.SH": "sh000688",
}

# 申万一级行业指数映射（示例）
SW_INDUSTRY_MAP = {
    "801010.SI": "农林牧渔",
    "801020.SI": "采掘",
    "801030.SI": "化工",
    "801040.SI": "钢铁",
    "801050.SI": "有色金属",
    "801080.SI": "电子",
    "801110.SI": "家用电器",
    "801120.SI": "食品饮料",
    "801130.SI": "纺织服装",
    "801140.SI": "轻工制造",
    "801150.SI": "医药生物",
    "801160.SI": "公用事业",
    "801170.SI": "交通运输",
    "801180.SI": "房地产",
    "801200.SI": "商业贸易",
    "801210.SI": "休闲服务",
    "801230.SI": "银行",
    "801780.SI": "非银金融",
    "801880.SI": "汽车",
    "801890.SI": "机械设备",
    "801740.SI": "国防军工",
    "801750.SI": "计算机",
    "801760.SI": "传媒",
    "801770.SI": "通信",
    "801730.SI": "电气设备",
    "801720.SI": "建筑材料",
    "801710.SI": "建筑装饰",
    "801950.SI": "综合",
}


def standardize_index_code(code: str) -> str:
    """
    标准化指数代码，统一为AkShare格式
    """
    # 如果已经是AkShare格式（如sh000300），直接返回
    if code.startswith(("sh", "sz", "hk")):
        return code
    
    # 检查标准映射
    if code in INDEX_CODE_STANDARDIZATION:
        return INDEX_CODE_STANDARDIZATION[code]
    
    # 检查是否是申万行业指数
    for suffix in SW_INDEX_SUFFIXES:
        if code.endswith(suffix):
            return code
    
    # 默认返回原值
    return code


def is_bond_index(code: str) -> bool:
    """
    判断是否是债券指数（财富指数版本本身是全收益的，无需合成）
    """
    std_code = standardize_index_code(code)
    
    # 检查债券指数列表
    if std_code in BOND_INDICES:
        return True
    
    # 检查是否包含债券关键词
    bond_keywords = ["bond", "债券", "债", "中债", "综合"]
    return any(keyword in std_code.lower() for keyword in bond_keywords)


# ============================================================
# 📈 数据抓取模块
# ============================================================

class PriceIndexFetcher:
    """价格指数数据抓取器"""
    
    @staticmethod
    @retry(max_retries=DATA_CONFIG["api_retry"], delay=DATA_CONFIG["api_delay"])
    def fetch_price_series(code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        抓取价格指数日行情数据
        使用 ak.index_zh_a_hist 接口
        
        返回：DataFrame with columns ['date', 'close']
        """
        std_code = standardize_index_code(code)
        
        try:
            # 指数历史数据（日频）
            df = ak.index_zh_a_hist(
                symbol=std_code, 
                period="daily",
                start_date=start_date,
                end_date=end_date
            )
            
            if df is None or df.empty:
                logger.warning(f"[PriceIndexFetcher] 无法获取指数 {code} ({std_code}) 的价格数据")
                return pd.DataFrame(columns=["date", "close"])
            
            # 标准化列名
            if "日期" in df.columns:
                df = df.rename(columns={"日期": "date"})
            if "收盘" in df.columns:
                df = df.rename(columns={"收盘": "close"})
            elif "close" in df.columns:
                df = df.rename(columns={"close": "close"})
            
            # 确保有必要的列
            if "date" not in df.columns or "close" not in df.columns:
                logger.warning(f"[PriceIndexFetcher] 指数 {code} 返回数据列名异常: {df.columns.tolist()}")
                return pd.DataFrame(columns=["date", "close"])
            
            # 提取必要列并清洗
            df = df[["date", "close"]].copy()
            df["date"] = pd.to_datetime(df["date"])
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna().sort_values("date")
            
            logger.info(f"[PriceIndexFetcher] 成功获取指数 {code} {start_date}~{end_date} 价格数据，共 {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"[PriceIndexFetcher] 获取指数 {code} 价格数据失败: {e}")
            return pd.DataFrame(columns=["date", "close"])


class DividendYieldFetcher:
    """股息率数据抓取和清洗器"""
    
    @staticmethod
    def is_sw_industry_index(code: str) -> bool:
        """
        判断是否为申万一级行业指数
        申万行业指数特征：以 .SI、.SW、.C 结尾，或者代码格式如 801010.SI
        """
        # 检查常见的申万行业指数后缀
        for suffix in SW_INDEX_SUFFIXES:
            if code.endswith(suffix):
                return True
        
        # 检查801系列（申万一级行业）
        if code.startswith("801") and any(code.endswith(s) for s in [".SI", ".SW", ".C", ""]):
            return True
            
        # 检查行业名称映射
        for industry_name in SW_INDUSTRY_DIVIDEND_YIELD.keys():
            if code.endswith(f".SW{industry_name}") or code.endswith(f".SI{industry_name}"):
                return True
                
        return False
    
    @staticmethod
    def get_sw_industry_name(code: str) -> str:
        """
        获取申万行业指数对应的行业名称
        返回格式：如 "银行"、"煤炭" 等
        """
        # 处理常见的申万行业指数格式
        code_str = str(code)
        
        # 1. 处理类似 "SW煤炭.SI" 的格式
        if code_str.startswith("SW") and any(code_str.endswith(suffix) for suffix in SW_INDEX_SUFFIXES):
            # 提取行业名称，如 "SW煤炭.SI" → "煤炭"
            suffix_pos = min([code_str.find(suffix) for suffix in SW_INDEX_SUFFIXES if suffix in code_str])
            if suffix_pos > 2:  # "SW"之后有内容
                industry_part = code_str[2:suffix_pos]
                
                # 查找匹配的行业名称
                for industry_name in SW_INDUSTRY_DIVIDEND_YIELD.keys():
                    if industry_part == industry_name:
                        return industry_name
                    elif industry_part in industry_name or industry_name in industry_part:
                        return industry_name
        
        # 2. 处理801系列代码
        if code_str.startswith("801"):
            # 提取801开头的数字部分
            base_code = code_str.split(".")[0] if "." in code_str else code_str
            
            # 801系列数字映射
            sw_code_map = {
                "801010": "农林牧渔", "801020": "采掘", "801030": "化工", "801040": "钢铁",
                "801050": "有色金属", "801080": "电子", "801110": "家用电器", "801120": "食品饮料",
                "801130": "纺织服装", "801140": "轻工制造", "801150": "医药生物", "801160": "公用事业",
                "801170": "交通运输", "801180": "房地产", "801200": "商业贸易", "801210": "休闲服务",
                "801230": "银行", "801780": "非银金融", "801880": "汽车", "801890": "机械设备",
                "801740": "国防军工", "801750": "计算机", "801760": "传媒", "801770": "通信",
                "801730": "电气设备", "801720": "建筑材料", "801710": "建筑装饰", "801950": "综合",
            }
            
            if base_code in sw_code_map:
                return sw_code_map[base_code]
        
        # 3. 检查是否有直接包含的行业名称
        for industry_name in SW_INDUSTRY_DIVIDEND_YIELD.keys():
            if industry_name in code_str:
                return industry_name
        
        # 4. 默认返回"综合"
        return "综合"
    
    @staticmethod
    def get_sw_industry_dividend_yield(code: str) -> float:
        """
        获取申万行业的固定股息率参数
        返回：年度化股息率（小数形式，如0.025表示2.5%）
        """
        industry_name = DividendYieldFetcher.get_sw_industry_name(code)
        
        # 从固定参数表中获取股息率
        if industry_name in SW_INDUSTRY_DIVIDEND_YIELD:
            div_yield = SW_INDUSTRY_DIVIDEND_YIELD[industry_name]
            logger.info(f"[DividendYieldFetcher] 申万行业指数 {code} ({industry_name}) 使用固定股息率参数: {div_yield:.3f} ({div_yield*100:.1f}%)")
            return div_yield
        else:
            # 默认使用中等水平1.5%
            logger.warning(f"[DividendYieldFetcher] 申万行业指数 {code} ({industry_name}) 未找到固定参数，使用默认值: 0.015 (1.5%)")
            return 0.015
    
    @staticmethod
    @retry(max_retries=DATA_CONFIG["api_retry"], delay=DATA_CONFIG["api_delay"])
    def fetch_dividend_yield(code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        抓取指数股息率数据
        优先使用申万行业固定参数，否则使用 ak.index_analysis_daily 接口
        
        返回：DataFrame with columns ['date', 'dividend_yield'] （小数形式，如0.025表示2.5%）
        """
        # 如果是申万行业指数，使用固定参数
        if DividendYieldFetcher.is_sw_industry_index(code):
            div_yield = DividendYieldFetcher.get_sw_industry_dividend_yield(code)
            
            # 创建固定股息率的DataFrame（覆盖整个时间范围）
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")
            df = pd.DataFrame({
                "date": date_range,
                "dividend_yield": div_yield  # 使用固定参数
            })
            
            logger.info(f"[DividendYieldFetcher] 申万行业指数 {code} 使用固定股息率参数: {div_yield:.3f}")
            return df
        
        std_code = standardize_index_code(code)
        
        try:
            # 指数每日估值数据（包含股息率）
            df = ak.index_analysis_daily(symbol=std_code)
            
            if df is None or df.empty:
                logger.warning(f"[DividendYieldFetcher] 无法获取指数 {code} ({std_code}) 的股息率数据")
                return pd.DataFrame(columns=["date", "dividend_yield"])
            
            # 标准化列名
            col_mapping = {}
            if "日期" in df.columns:
                col_mapping["日期"] = "date"
            elif "trade_date" in df.columns:
                col_mapping["trade_date"] = "date"
                
            if "股息率" in df.columns:
                col_mapping["股息率"] = "dividend_yield"
            elif "dividend_yield" in df.columns:
                col_mapping["dividend_yield"] = "dividend_yield"
            elif "div_yield" in df.columns:
                col_mapping["div_yield"] = "dividend_yield"
            
            if not col_mapping:
                logger.warning(f"[DividendYieldFetcher] 指数 {code} 返回数据列名异常: {df.columns.tolist()}")
                return pd.DataFrame(columns=["date", "dividend_yield"])
            
            df = df.rename(columns=col_mapping)
            
            # 确保有必要的列
            if "date" not in df.columns or "dividend_yield" not in df.columns:
                logger.warning(f"[DividendYieldFetcher] 指数 {code} 返回数据缺少必要列")
                return pd.DataFrame(columns=["date", "dividend_yield"])
            
            # 提取必要列并清洗
            df = df[["date", "dividend_yield"]].copy()
            df["date"] = pd.to_datetime(df["date"])
            
            # 股息率单位统一化：确保为小数形式（0.025表示2.5%）
            df["dividend_yield"] = pd.to_numeric(df["dividend_yield"], errors="coerce")
            
            # 检查最大值，如果大于1，则认为是百分数形式，需要除以100
            if df["dividend_yield"].max() > 1.0:
                df["dividend_yield"] = df["dividend_yield"] / 100.0
                logger.debug("[DividendYieldFetcher] 股息率数据转换为小数形式（原为百分数）")
            
            df = df.dropna().sort_values("date")
            
            # 过滤日期范围
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
            
            logger.info(f"[DividendYieldFetcher] 成功获取指数 {code} {start_date}~{end_date} 股息率数据，共 {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"[DividendYieldFetcher] 获取指数 {code} 股息率数据失败: {e}")
            return pd.DataFrame(columns=["date", "dividend_yield"])
    
    @staticmethod
    def normalize_dividend_yield(df: pd.DataFrame) -> pd.DataFrame:
        """
        股息率数据标准化清洗
        """
        if df.empty:
            return df
        
        # 确保日期索引
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        
        # 填充缺失值（向前填充）
        df["dividend_yield"] = df["dividend_yield"].ffill()
        
        # 确保没有负值
        df["dividend_yield"] = df["dividend_yield"].clip(lower=0.0)
        
        return df.reset_index()


# ============================================================
# 💾 缓存管理模块
# ============================================================

class CacheManager:
    """本地缓存管理器（Parquet格式）"""
    
    CACHE_DIR = "data/indices"
    
    @classmethod
    def _get_cache_path(cls, code: str, data_type: str) -> str:
        """
        获取缓存文件路径
        data_type: 'price' 或 'dividend' 或 'total_return'
        """
        # 创建缓存目录
        os.makedirs(cls.CACHE_DIR, exist_ok=True)
        
        # 使用MD5哈希作为文件名，避免特殊字符问题
        filename = f"{code}_{data_type}.parquet"
        return os.path.join(cls.CACHE_DIR, filename)
    
    @classmethod
    def save_to_cache(cls, code: str, data_type: str, df: pd.DataFrame) -> bool:
        """
        保存数据到Parquet缓存
        """
        try:
            if df.empty:
                logger.warning(f"[CacheManager] 尝试保存空数据到缓存: {code}/{data_type}")
                return False
            
            cache_path = cls._get_cache_path(code, data_type)
            
            # 确保日期列为datetime类型
            df_cache = df.copy()
            if "date" in df_cache.columns:
                df_cache["date"] = pd.to_datetime(df_cache["date"])
            
            # 保存为Parquet格式
            df_cache.to_parquet(cache_path, index=False)
            logger.debug(f"[CacheManager] 已保存 {len(df)} 条 {data_type} 数据到 {cache_path}")
            return True
            
        except Exception as e:
            logger.error(f"[CacheManager] 保存缓存失败 {code}/{data_type}: {e}")
            return False
    
    @classmethod
    def load_from_cache(cls, code: str, data_type: str) -> pd.DataFrame:
        """
        从Parquet缓存加载数据
        """
        cache_path = cls._get_cache_path(code, data_type)
        
        if not os.path.exists(cache_path):
            logger.debug(f"[CacheManager] 缓存文件不存在: {cache_path}")
            return pd.DataFrame()
        
        try:
            df = pd.read_parquet(cache_path)
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            logger.debug(f"[CacheManager] 从缓存加载 {len(df)} 条 {data_type} 数据")
            return df
        except Exception as e:
            logger.error(f"[CacheManager] 加载缓存失败 {cache_path}: {e}")
            return pd.DataFrame()
    
    @classmethod
    def get_latest_date(cls, code: str, data_type: str) -> Optional[datetime]:
        """
        获取缓存中最新日期
        """
        df = cls.load_from_cache(code, data_type)
        if df.empty or "date" not in df.columns:
            return None
        
        return df["date"].max()
    
    @classmethod
    def incremental_update(cls, code: str, data_type: str, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        增量更新缓存
        返回：合并后的完整数据
        """
        if new_df.empty:
            return cls.load_from_cache(code, data_type)
        
        # 加载现有缓存
        cached_df = cls.load_from_cache(code, data_type)
        
        if cached_df.empty:
            # 无缓存，直接保存新数据
            cls.save_to_cache(code, data_type, new_df)
            return new_df
        
        # 合并数据，去重
        merged_df = pd.concat([cached_df, new_df], ignore_index=True)
        if "date" in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=["date"], keep="last")
            merged_df = merged_df.sort_values("date")
        
        # 保存更新后的缓存
        cls.save_to_cache(code, data_type, merged_df)
        
        logger.info(f"[CacheManager] 增量更新 {code}/{data_type}: 新增 {len(new_df)} 条，总计 {len(merged_df)} 条")
        return merged_df


# ============================================================
# 📈 全收益指数合成模块
# ============================================================

class TotalReturnCalculator:
    """全收益指数合成计算器"""
    
    @staticmethod
    def calculate_total_return(price_df: pd.DataFrame, div_input: Union[pd.DataFrame, float]) -> pd.DataFrame:
        """
        计算全收益指数
        
        参数：
        - price_df: 价格指数DataFrame（必须包含'date'和'close'列）
        - div_input: 可以是股息率DataFrame（包含'date'和'dividend_yield'列），也可以是固定的股息率（float）
        
        公式：
        价格收益率：price_ret = price_t / price_{t-1} - 1
        股息日收益率：div_daily = dividend_yield / TRADING_DAYS_PER_YEAR
        全收益率：total_ret = (1 + price_ret) * (1 + div_daily) - 1
        
        返回：DataFrame with columns ['date', 'tr_ret']（全收益日收益率）
        """
        if price_df.empty:
            logger.warning("[TotalReturnCalculator] 价格数据为空，无法计算全收益")
            return pd.DataFrame(columns=["date", "tr_ret"])
        
        # 处理股息率输入（支持DataFrame或float）
        if isinstance(div_input, (int, float)):
            # 如果输入是固定股息率，创建固定的DataFrame
            div_yield = float(div_input)
            div_df = pd.DataFrame({
                "date": price_df["date"].copy(),
                "dividend_yield": div_yield
            })
            logger.debug(f"[TotalReturnCalculator] 使用固定股息率: {div_yield:.4f}")
        elif isinstance(div_input, pd.DataFrame):
            div_df = div_input.copy()
        else:
            logger.error(f"[TotalReturnCalculator] 股息率输入类型错误: {type(div_input)}")
            return pd.DataFrame(columns=["date", "tr_ret"])
        
        # 准备价格数据
        price_data = price_df.copy()
        price_data["date"] = pd.to_datetime(price_data["date"])
        price_data = price_data.set_index("date").sort_index()
        
        # 计算价格收益率
        price_data["price_ret"] = price_data["close"].pct_change().fillna(0)
        
        # 处理股息率数据
        if div_df.empty:
            # 无股息率数据，使用默认值
            logger.info("[TotalReturnCalculator] 无股息率数据，使用默认股息率")
            # 默认年化股息率使用预设值或2.5%
            default_annual_yield = 0.025  # 2.5%
            price_data["div_daily"] = default_annual_yield / TRADING_DAYS_PER_YEAR
        else:
            # 准备股息率数据
            div_data = div_df.copy()
            div_data["date"] = pd.to_datetime(div_data["date"])
            div_data = div_data.set_index("date").sort_index()
            
            # 将股息率数据对齐到价格数据日期
            div_aligned = div_data.reindex(price_data.index, method="ffill")
            
            # 计算股息日收益率
            # 注意：股息率已经是年化的，需要除以交易日数得到日频
            price_data["div_daily"] = div_aligned["dividend_yield"] / TRADING_DAYS_PER_YEAR
        
        # 计算全收益率
        price_data["tr_ret"] = (1 + price_data["price_ret"]) * (1 + price_data["div_daily"]) - 1
        
        # 准备输出
        result_df = price_data[["tr_ret"]].copy()
        result_df = result_df.reset_index()
        result_df.columns = ["date", "tr_ret"]
        
        # 清理可能的NaN值
        result_df = result_df.dropna()
        
        logger.info(f"[TotalReturnCalculator] 成功计算全收益指数，共 {len(result_df)} 个交易日")
        return result_df


# ============================================================
# 🛡️ 兜底机制处理模块
# ============================================================

class FallbackHandler:
    """兜底机制处理器"""
    
    @staticmethod
    def get_default_dividend_yield(code: str) -> float:
        """
        获取预设默认股息率
        
        返回：年化股息率（小数形式）
        """
        std_code = standardize_index_code(code)
        
        # 从配置中获取默认值
        if std_code in DEFAULT_DIV_YIELD:
            return DEFAULT_DIV_YIELD[std_code]
        
        # 根据指数类型提供默认值
        if "300" in code or "sh000300" in std_code:
            return 0.025  # 沪深300：2.5%
        elif "500" in code or "sh000905" in std_code:
            return 0.018  # 中证500：1.8%
        elif "1000" in code or "sh000852" in std_code:
            return 0.015  # 中证1000：1.5%
        elif "50" in code or "sh000016" in std_code:
            return 0.028  # 上证50：2.8%
        
        # 通用默认值
        return 0.020  # 2.0%
    
    @staticmethod
    def create_fallback_dividend_data(price_df: pd.DataFrame, code: str) -> pd.DataFrame:
        """
        创建兜底股息率数据（使用预设常数）
        """
        if price_df.empty:
            return pd.DataFrame(columns=["date", "dividend_yield"])
        
        default_yield = FallbackHandler.get_default_dividend_yield(code)
        
        result_df = price_df[["date"]].copy()
        result_df["dividend_yield"] = default_yield
        
        logger.info(f"[FallbackHandler] 为指数 {code} 使用兜底股息率: {default_yield:.3%}")
        return result_df


# ============================================================
# 🏭 主管理器
# ============================================================

class IndexSyncManager:
    """全收益指数同步管理器"""
    
    def __init__(self):
        self.price_fetcher = PriceIndexFetcher()
        self.div_fetcher = DividendYieldFetcher()
        self.cache_manager = CacheManager()
        self.tr_calculator = TotalReturnCalculator()
        self.fallback_handler = FallbackHandler()
    
    def get_total_return_series(self, code: str, start_date: Optional[str] = None, 
                               end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取全收益指数系列
        
        主入口函数：返回包含 date 和 tr_ret（全收益日收益率）的干净 DataFrame
        
        逻辑：
        1. 检查是否是债券指数（直接使用价格指数）
        2. 尝试从缓存加载全收益数据
        3. 增量更新价格和股息率数据
        4. 合成全收益指数
        5. 缓存结果
        """
        # 标准化指数代码
        std_code = standardize_index_code(code)
        
        # 设置默认日期范围（最近5年）
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y%m%d")
        
        logger.info(f"[IndexSyncManager] 开始获取指数 {code} ({std_code}) 全收益数据")
        
        # 1. 检查是否是债券指数
        if is_bond_index(std_code):
            logger.info(f"[IndexSyncManager] 指数 {code} 是债券指数，直接使用价格指数")
            return self._get_bond_index_series(std_code, start_date, end_date)
        
        # 2. 尝试从缓存加载全收益数据
        cached_total_return = self.cache_manager.load_from_cache(std_code, "total_return")
        if not cached_total_return.empty:
            # 检查缓存是否覆盖所需日期范围
            cache_start = cached_total_return["date"].min()
            cache_end = cached_total_return["date"].max()
            need_start = pd.to_datetime(start_date)
            need_end = pd.to_datetime(end_date)
            
            if cache_start <= need_start and cache_end >= need_end:
                # 缓存完全覆盖所需范围
                filtered = cached_total_return[
                    (cached_total_return["date"] >= need_start) & 
                    (cached_total_return["date"] <= need_end)
                ]
                logger.info(f"[IndexSyncManager] 使用缓存的全收益数据，共 {len(filtered)} 条")
                return filtered
        
        # 3. 增量更新价格数据
        price_df = self._sync_price_data(std_code, start_date, end_date)
        if price_df.empty:
            logger.error(f"[IndexSyncManager] 无法获取指数 {code} 价格数据")
            return pd.DataFrame(columns=["date", "tr_ret"])
        
        # 4. 增量更新股息率数据
        div_yield_df = self._sync_dividend_data(std_code, start_date, end_date, price_df)
        
        # 5. 合成全收益指数
        total_return_df = self.tr_calculator.calculate_total_return(price_df, div_yield_df)
        
        if total_return_df.empty:
            logger.error(f"[IndexSyncManager] 无法计算指数 {code} 全收益数据")
            return pd.DataFrame(columns=["date", "tr_ret"])
        
        # 6. 缓存结果
        self.cache_manager.save_to_cache(std_code, "total_return", total_return_df)
        
        logger.info(f"[IndexSyncManager] 成功生成指数 {code} 全收益数据，共 {len(total_return_df)} 条")
        return total_return_df[["date", "tr_ret"]]
    
    def _get_bond_index_series(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取债券指数系列（直接使用价格指数，无需合成）
        """
        # 尝试从缓存加载
        cached_price = self.cache_manager.load_from_cache(code, "price")
        
        # 增量更新
        latest_date = self.cache_manager.get_latest_date(code, "price")
        if latest_date:
            # 需要更新的起始日期（缓存最新日期的下一天）
            update_start = (latest_date + timedelta(days=1)).strftime("%Y%m%d")
            if update_start <= end_date:
                new_price = self.price_fetcher.fetch_price_series(code, update_start, end_date)
                if not new_price.empty:
                    price_df = self.cache_manager.incremental_update(code, "price", new_price)
                else:
                    price_df = cached_price
            else:
                price_df = cached_price
        else:
            # 无缓存，全量抓取
            price_df = self.price_fetcher.fetch_price_series(code, start_date, end_date)
            if not price_df.empty:
                self.cache_manager.save_to_cache(code, "price", price_df)
        
        if price_df.empty:
            return pd.DataFrame(columns=["date", "tr_ret"])
        
        # 债券指数直接使用价格收益率作为全收益率
        price_df = price_df.copy()
        price_df["date"] = pd.to_datetime(price_df["date"])
        price_df = price_df.sort_values("date")
        price_df["tr_ret"] = price_df["close"].pct_change().fillna(0)
        
        # 过滤日期范围
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        result_df = price_df[(price_df["date"] >= start_dt) & (price_df["date"] <= end_dt)]
        
        return result_df[["date", "tr_ret"]]
    
    def _sync_price_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        同步价格数据（增量更新）
        """
        # 尝试从缓存加载
        cached_price = self.cache_manager.load_from_cache(code, "price")
        
        if cached_price.empty:
            # 无缓存，全量抓取
            price_df = self.price_fetcher.fetch_price_series(code, start_date, end_date)
            if not price_df.empty:
                self.cache_manager.save_to_cache(code, "price", price_df)
            return price_df
        
        # 检查缓存最新日期
        latest_date = self.cache_manager.get_latest_date(code, "price")
        if not latest_date:
            return cached_price
        
        # 需要更新的起始日期（缓存最新日期的下一天）
        update_start = (latest_date + timedelta(days=1)).strftime("%Y%m%d")
        
        if update_start <= end_date:
            # 抓取新数据
            new_price = self.price_fetcher.fetch_price_series(code, update_start, end_date)
            if not new_price.empty:
                # 增量更新
                price_df = self.cache_manager.incremental_update(code, "price", new_price)
                return price_df
        
        # 直接返回缓存数据
        return cached_price
    
    def _sync_dividend_data(self, code: str, start_date: str, end_date: str, 
                           price_df: pd.DataFrame) -> pd.DataFrame:
        """
        同步股息率数据（增量更新，带兜底机制）
        """
        # 尝试从缓存加载
        cached_dividend = self.cache_manager.load_from_cache(code, "dividend")
        
        if cached_dividend.empty:
            # 无缓存，尝试抓取
            div_df = self.div_fetcher.fetch_dividend_yield(code, start_date, end_date)
            
            if div_df.empty:
                # 抓取失败，使用兜底机制
                logger.warning(f"[IndexSyncManager] 无法获取指数 {code} 股息率数据，使用兜底机制")
                div_df = self.fallback_handler.create_fallback_dividend_data(price_df, code)
            
            # 标准化和保存
            div_df = self.div_fetcher.normalize_dividend_yield(div_df)
            if not div_df.empty:
                self.cache_manager.save_to_cache(code, "dividend", div_df)
            
            return div_df
        
        # 检查缓存最新日期
        latest_date = self.cache_manager.get_latest_date(code, "dividend")
        if not latest_date:
            return cached_dividend
        
        # 需要更新的起始日期（缓存最新日期的下一天）
        update_start = (latest_date + timedelta(days=1)).strftime("%Y%m%d")
        
        if update_start <= end_date:
            # 尝试抓取新数据
            new_div = self.div_fetcher.fetch_dividend_yield(code, update_start, end_date)
            
            if new_div.empty:
                # 抓取失败，使用兜底机制创建新数据
                logger.warning(f"[IndexSyncManager] 无法增量获取指数 {code} 股息率数据，使用兜底机制")
                # 获取需要更新的价格数据部分
                update_price = price_df[price_df["date"] >= pd.to_datetime(update_start)]
                if not update_price.empty:
                    new_div = self.fallback_handler.create_fallback_dividend_data(update_price, code)
            
            if not new_div.empty:
                # 增量更新
                new_div = self.div_fetcher.normalize_dividend_yield(new_div)
                div_df = self.cache_manager.incremental_update(code, "dividend", new_div)
                return div_df
        
        # 直接返回缓存数据
        return cached_dividend


# ============================================================
# 🚀 全局单例和管理函数
# ============================================================

# 全局管理器实例
_index_sync_manager = None

def get_index_sync_manager() -> IndexSyncManager:
    """获取全局索引同步管理器实例（单例模式）"""
    global _index_sync_manager
    if _index_sync_manager is None:
        _index_sync_manager = IndexSyncManager()
    return _index_sync_manager

def get_total_return_series(code: str, start_date: Optional[str] = None, 
                           end_date: Optional[str] = None) -> pd.DataFrame:
    """
    全局函数：获取全收益指数系列
    
    这是模块的主要对外接口
    """
    manager = get_index_sync_manager()
    return manager.get_total_return_series(code, start_date, end_date)

def clear_cache(code: Optional[str] = None, data_type: Optional[str] = None):
    """
    清除缓存
    
    Parameters:
    - code: 指数代码，如果为None则清除所有缓存
    - data_type: 数据类型（'price', 'dividend', 'total_return'），如果为None则清除所有类型
    """
    cache_dir = CacheManager.CACHE_DIR
    
    if not os.path.exists(cache_dir):
        return
    
    import glob
    
    if code is None and data_type is None:
        # 清除所有缓存
        files = glob.glob(os.path.join(cache_dir, "*.parquet"))
    elif code is not None and data_type is None:
        # 清除指定指数的所有类型缓存
        pattern = f"{code}_*.parquet"
        files = glob.glob(os.path.join(cache_dir, pattern))
    elif code is not None and data_type is not None:
        # 清除指定指数和类型的缓存
        pattern = f"{code}_{data_type}.parquet"
        files = glob.glob(os.path.join(cache_dir, pattern))
    else:
        # data_type不为None但code为None的情况
        pattern = f"*_{data_type}.parquet"
        files = glob.glob(os.path.join(cache_dir, pattern))
    
    for file in files:
        try:
            os.remove(file)
            logger.info(f"[CacheManager] 已清除缓存文件: {file}")
        except Exception as e:
            logger.error(f"[CacheManager] 清除缓存文件失败 {file}: {e}")
    
    logger.info(f"[CacheManager] 已清除 {len(files)} 个缓存文件")