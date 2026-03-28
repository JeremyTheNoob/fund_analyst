"""
全收益指数缓存管理器 — fund_quant_v2
负责价格指数获取、全收益合成、本地缓存管理

核心功能：
1. 价格指数获取与缓存
2. 全收益指数合成与缓存
3. 缓存过期检查
4. 增量更新机制
"""

from __future__ import annotations
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np

from data_loader.index_cache_config import (
    PRICE_INDEX_CACHE_DIR,
    TOTAL_RETURN_CACHE_DIR,
    INDEX_METADATA_FILE,
    SUPPORTED_INDEXES,
    INDEX_ALIAS_MAP,
    CACHE_TTL,
    SYNTHESIS_CONFIG,
    get_price_index_filename,
    get_total_return_filename,
    get_metadata_filename,
    TRADING_DAYS_PER_YEAR,
)

# 导入现有的价格指数获取函数
from data_loader.base_api import _ak_index_daily_main

logger = logging.getLogger(__name__)


@dataclass
class IndexMetadata:
    """指数元数据"""
    index_code: str
    index_name: str
    index_type: str
    last_updated: str  # ISO格式时间字符串
    data_source: str
    data_points: int
    start_date: str
    end_date: str
    dividend_yield: float
    cache_status: str  # "valid", "expired", "missing"
    checksum: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IndexMetadata":
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PriceIndexFetcher:
    """价格指数获取器"""
    
    def __init__(self):
        self.cache_dir = PRICE_INDEX_CACHE_DIR
        
    def get_price_index(
        self, 
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        获取价格指数数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            force_refresh: 是否强制刷新缓存
            
        Returns:
            DataFrame with columns: ['date', 'close']
        """
        # 标准化指数代码
        normalized_code = self._normalize_index_code(index_code)
        
        # 检查是否支持该指数
        if normalized_code not in SUPPORTED_INDEXES:
            logger.warning(f"[PriceIndexFetcher] 不支持的指数代码: {index_code} -> {normalized_code}")
            # 尝试获取该指数的配置信息
            index_config = self._get_index_config(normalized_code)
        else:
            index_config = SUPPORTED_INDEXES[normalized_code]
        
        # 检查缓存
        cache_file = os.path.join(self.cache_dir, get_price_index_filename(normalized_code))
        metadata_file = os.path.join(self.cache_dir, get_metadata_filename(normalized_code))
        
        # 如果缓存有效且不需要强制刷新，则返回缓存数据
        if not force_refresh and os.path.exists(cache_file) and os.path.exists(metadata_file):
            metadata = self._load_metadata(metadata_file)
            if metadata and self._is_cache_valid(metadata, index_config):
                logger.info(f"[PriceIndexFetcher] 使用缓存价格指数数据: {normalized_code}")
                try:
                    df = pd.read_parquet(cache_file)
                    # 筛选日期范围
                    if start_date or end_date:
                        df = self._filter_by_date(df, start_date, end_date)
                    return df
                except Exception as e:
                    logger.error(f"[PriceIndexFetcher] 读取缓存失败: {e}")
        
        # 需要重新获取数据
        logger.info(f"[PriceIndexFetcher] 开始获取价格指数: {normalized_code}")
        df = self._fetch_price_index(normalized_code)
        
        if df.empty:
            logger.error(f"[PriceIndexFetcher] 无法获取价格指数数据: {normalized_code}")
            return pd.DataFrame()
        
        # 创建元数据
        metadata = IndexMetadata(
            index_code=normalized_code,
            index_name=index_config.get('name', normalized_code),
            index_type=index_config.get('type', 'unknown'),
            last_updated=datetime.now().isoformat(),
            data_source='akshare',
            data_points=len(df),
            start_date=df['date'].min().strftime('%Y-%m-%d'),
            end_date=df['date'].max().strftime('%Y-%m-%d'),
            dividend_yield=index_config.get('default_div_yield', SYNTHESIS_CONFIG['default_div_yield']),
            cache_status='valid',
            checksum=self._calculate_checksum(df)
        )
        
        # 保存缓存和元数据
        self._save_data(df, cache_file)
        self._save_metadata(metadata, metadata_file)
        
        # 筛选日期范围
        if start_date or end_date:
            df = self._filter_by_date(df, start_date, end_date)
        
        return df
    
    def _normalize_index_code(self, index_code: str) -> str:
        """标准化指数代码"""
        # 去除空格和特殊字符，保持原有大小写
        code = index_code.strip()
        
        # 首先检查是否已经在支持列表中（保持原有大小写）
        if code in SUPPORTED_INDEXES:
            return code
        
        # 检查别名映射（不改变大小写）
        if code in INDEX_ALIAS_MAP:
            return INDEX_ALIAS_MAP[code]
        
        # 同时检查大写版本
        upper_code = code.upper()
        if upper_code in INDEX_ALIAS_MAP:
            return INDEX_ALIAS_MAP[upper_code]
        
        # 标准化格式（但保持sh/sz小写）
        if '.' not in code:
            # 没有后缀的代码
            if code.upper().startswith('801') and len(code) == 6:
                # 申万行业指数
                return f"{code.upper()}.SI"
            elif code.startswith('000'):
                # 确保sh前缀小写
                return f"sh{code}"
            elif code.startswith('399'):
                # 确保sz前缀小写
                return f"sz{code}"
        
        # 最后检查小写版本是否在支持列表中
        lower_code = code.lower()
        if lower_code in SUPPORTED_INDEXES:
            return lower_code
        
        return code
    
    def _get_index_config(self, index_code: str) -> Dict[str, Any]:
        """获取指数配置（如果不在支持列表中，创建默认配置）"""
        if index_code in SUPPORTED_INDEXES:
            return SUPPORTED_INDEXES[index_code]
        
        # 创建默认配置
        return {
            'name': index_code,
            'type': 'unknown',
            'default_div_yield': SYNTHESIS_CONFIG['default_div_yield'],
            'update_frequency': 'daily',
            'min_history_days': 60,
        }
    
    def _fetch_price_index(self, index_code: str) -> pd.DataFrame:
        """从AkShare获取价格指数数据"""
        try:
            # 调用现有的价格指数获取函数
            df = _ak_index_daily_main(index_code)
            
            if df is None or df.empty:
                logger.error(f"[PriceIndexFetcher] AkShare返回空数据: {index_code}")
                return pd.DataFrame()
            
            # 确保有必要的列
            if 'date' not in df.columns or 'close' not in df.columns:
                logger.error(f"[PriceIndexFetcher] 数据格式不正确: {index_code}")
                return pd.DataFrame()
            
            # 标准化日期格式
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # 确保数据质量
            df = df.drop_duplicates(subset=['date'])
            
            logger.info(f"[PriceIndexFetcher] 成功获取价格指数数据: {index_code}, {len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"[PriceIndexFetcher] 获取价格指数数据失败: {index_code}, {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def _filter_by_date(self, df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
        """按日期范围筛选数据"""
        if df.empty:
            return df
        
        filtered_df = df.copy()
        
        if start_date:
            try:
                start_dt = pd.to_datetime(start_date)
                filtered_df = filtered_df[filtered_df['date'] >= start_dt]
            except:
                logger.warning(f"[PriceIndexFetcher] 无效的开始日期: {start_date}")
        
        if end_date:
            try:
                end_dt = pd.to_datetime(end_date)
                filtered_df = filtered_df[filtered_df['date'] <= end_dt]
            except:
                logger.warning(f"[PriceIndexFetcher] 无效的结束日期: {end_date}")
        
        return filtered_df
    
    def _is_cache_valid(self, metadata: IndexMetadata, index_config: Dict[str, Any]) -> bool:
        """检查缓存是否有效"""
        # 检查是否过期
        try:
            last_updated = datetime.fromisoformat(metadata.last_updated)
            now = datetime.now()
            
            # 根据更新频率确定过期时间
            update_freq = index_config.get('update_frequency', 'daily')
            ttl_hours = {
                'daily': 24,
                'weekly': 168,
                'monthly': 720,
            }.get(update_freq, 24)
            
            # 检查是否过期
            if (now - last_updated).total_seconds() > ttl_hours * 3600:
                logger.info(f"[PriceIndexFetcher] 缓存已过期: {metadata.index_code}, 最后更新: {metadata.last_updated}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"[PriceIndexFetcher] 检查缓存有效性失败: {e}")
            return False
    
    def _calculate_checksum(self, df: pd.DataFrame) -> str:
        """计算数据校验和"""
        import hashlib
        # 使用日期和收盘价的哈希作为校验和
        data_str = df[['date', 'close']].to_string(index=False)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _save_data(self, df: pd.DataFrame, filepath: str):
        """保存数据到Parquet文件"""
        try:
            df.to_parquet(filepath, index=False)
            logger.info(f"[PriceIndexFetcher] 数据已保存: {filepath}")
        except Exception as e:
            logger.error(f"[PriceIndexFetcher] 保存数据失败: {filepath}, {e}")
    
    def _save_metadata(self, metadata: IndexMetadata, filepath: str):
        """保存元数据到JSON文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata.to_dict(), f, ensure_ascii=False, indent=2)
            logger.info(f"[PriceIndexFetcher] 元数据已保存: {filepath}")
        except Exception as e:
            logger.error(f"[PriceIndexFetcher] 保存元数据失败: {filepath}, {e}")
    
    def _load_metadata(self, filepath: str) -> Optional[IndexMetadata]:
        """从JSON文件加载元数据"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return IndexMetadata.from_dict(data)
        except Exception as e:
            logger.error(f"[PriceIndexFetcher] 加载元数据失败: {filepath}, {e}")
            return None


class TotalReturnSynthesizer:
    """全收益指数合成器"""
    
    def __init__(self):
        self.price_fetcher = PriceIndexFetcher()
        self.cache_dir = TOTAL_RETURN_CACHE_DIR
    
    def get_total_return_index(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        获取全收益指数数据
        
        算法步骤：
        1. 获取价格指数数据
        2. 获取股息率数据（从配置或实际数据）
        3. 合成全收益指数
        4. 缓存结果
        
        Args:
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            force_refresh: 是否强制刷新缓存
            
        Returns:
            DataFrame with columns: ['date', 'price_close', 'price_ret', 'div_yield', 'div_ret', 'total_ret', 'total_nav']
        """
        # 标准化指数代码
        normalized_code = self._normalize_index_code(index_code)
        
        # 检查是否支持该指数
        if normalized_code not in SUPPORTED_INDEXES:
            logger.warning(f"[TotalReturnSynthesizer] 不支持的指数代码: {index_code} -> {normalized_code}")
            return pd.DataFrame()
        
        index_config = SUPPORTED_INDEXES[normalized_code]
        
        # 检查缓存
        cache_file = os.path.join(self.cache_dir, get_total_return_filename(normalized_code))
        metadata_file = os.path.join(self.cache_dir, get_metadata_filename(f"tr_{normalized_code}"))
        
        # 如果缓存有效且不需要强制刷新，则返回缓存数据
        if not force_refresh and os.path.exists(cache_file) and os.path.exists(metadata_file):
            metadata = self._load_metadata(metadata_file)
            if metadata and self._is_cache_valid(metadata, index_config):
                logger.info(f"[TotalReturnSynthesizer] 使用缓存全收益指数数据: {normalized_code}")
                try:
                    df = pd.read_parquet(cache_file)
                    # 筛选日期范围
                    if start_date or end_date:
                        df = self._filter_by_date(df, start_date, end_date)
                    return df
                except Exception as e:
                    logger.error(f"[TotalReturnSynthesizer] 读取缓存失败: {e}")
        
        # 需要重新合成数据
        logger.info(f"[TotalReturnSynthesizer] 开始合成全收益指数: {normalized_code}")
        
        # 1. 获取价格指数数据
        price_df = self.price_fetcher.get_price_index(
            normalized_code, 
            start_date=start_date, 
            end_date=end_date,
            force_refresh=force_refresh
        )
        
        if price_df.empty:
            logger.error(f"[TotalReturnSynthesizer] 无法获取价格指数数据: {normalized_code}")
            return pd.DataFrame()
        
        # 2. 获取股息率
        dividend_yield = index_config.get('default_div_yield', SYNTHESIS_CONFIG['default_div_yield'])
        
        # 3. 合成全收益指数
        total_return_df = self._synthesize_total_return(price_df, dividend_yield)
        
        if total_return_df.empty:
            logger.error(f"[TotalReturnSynthesizer] 全收益指数合成失败: {normalized_code}")
            return pd.DataFrame()
        
        # 4. 创建元数据
        metadata = IndexMetadata(
            index_code=normalized_code,
            index_name=index_config.get('name', normalized_code),
            index_type='total_return',
            last_updated=datetime.now().isoformat(),
            data_source='synthesized',
            data_points=len(total_return_df),
            start_date=total_return_df['date'].min().strftime('%Y-%m-%d'),
            end_date=total_return_df['date'].max().strftime('%Y-%m-%d'),
            dividend_yield=dividend_yield,
            cache_status='valid',
            checksum=self._calculate_checksum(total_return_df)
        )
        
        # 5. 保存缓存和元数据
        self._save_data(total_return_df, cache_file)
        self._save_metadata(metadata, metadata_file)
        
        logger.info(f"[TotalReturnSynthesizer] 成功合成全收益指数: {normalized_code}, {len(total_return_df)} 条记录")
        return total_return_df
    
    def _normalize_index_code(self, index_code: str) -> str:
        """标准化指数代码"""
        return self.price_fetcher._normalize_index_code(index_code)
    
    def _synthesize_total_return(self, price_df: pd.DataFrame, dividend_yield: float) -> pd.DataFrame:
        """
        合成全收益指数
        
        算法公式：
        daily_div_ret = dividend_yield / TRADING_DAYS_PER_YEAR
        total_ret = (1 + price_ret) * (1 + daily_div_ret) - 1
        total_nav = (1 + total_ret).cumprod()
        """
        if price_df.empty or 'date' not in price_df.columns or 'close' not in price_df.columns:
            return pd.DataFrame()
        
        # 创建结果DataFrame
        result = price_df.copy()
        result = result.sort_values('date').reset_index(drop=True)
        
        # 计算价格收益率
        result['price_close'] = result['close']
        result['price_ret'] = result['price_close'].pct_change().fillna(0)
        
        # 计算股息收益率（年化股息率转日收益率）
        daily_div_ret = dividend_yield / TRADING_DAYS_PER_YEAR
        result['div_yield'] = dividend_yield
        result['div_ret'] = daily_div_ret
        
        # 计算总收益率
        result['total_ret'] = (1 + result['price_ret']) * (1 + result['div_ret']) - 1
        
        # 计算全收益净值（从1.0开始）
        result['total_nav'] = (1 + result['total_ret']).cumprod()
        
        # 第一天处理：只有股息收益，没有价格变动
        result.loc[0, 'price_ret'] = 0.0
        result.loc[0, 'total_ret'] = daily_div_ret
        result.loc[0, 'total_nav'] = 1 + daily_div_ret
        
        # 添加数据源标记
        result['data_source'] = 'synthesized'
        
        # 选择需要的列
        result = result[['date', 'price_close', 'price_ret', 'div_yield', 'div_ret', 'total_ret', 'total_nav', 'data_source']]
        
        return result
    
    def _filter_by_date(self, df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
        """按日期范围筛选数据"""
        return self.price_fetcher._filter_by_date(df, start_date, end_date)
    
    def _is_cache_valid(self, metadata: IndexMetadata, index_config: Dict[str, Any]) -> bool:
        """检查缓存是否有效"""
        return self.price_fetcher._is_cache_valid(metadata, index_config)
    
    def _calculate_checksum(self, df: pd.DataFrame) -> str:
        """计算数据校验和"""
        return self.price_fetcher._calculate_checksum(df)
    
    def _save_data(self, df: pd.DataFrame, filepath: str):
        """保存数据到Parquet文件"""
        self.price_fetcher._save_data(df, filepath)
    
    def _save_metadata(self, metadata: IndexMetadata, filepath: str):
        """保存元数据到JSON文件"""
        self.price_fetcher._save_metadata(metadata, filepath)
    
    def _load_metadata(self, filepath: str) -> Optional[IndexMetadata]:
        """从JSON文件加载元数据"""
        return self.price_fetcher._load_metadata(filepath)


class IndexCacheManager:
    """指数缓存管理器（主入口）"""
    
    def __init__(self):
        self.price_fetcher = PriceIndexFetcher()
        self.total_return_synthesizer = TotalReturnSynthesizer()
        logger.info("[IndexCacheManager] 初始化完成")
    
    def get_price_index(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """获取价格指数数据"""
        return self.price_fetcher.get_price_index(index_code, start_date, end_date, force_refresh)
    
    def get_total_return_index(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """获取全收益指数数据"""
        return self.total_return_synthesizer.get_total_return_index(index_code, start_date, end_date, force_refresh)
    
    def get_both_indices(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        force_refresh: bool = False
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """同时获取价格指数和全收益指数数据"""
        price_df = self.get_price_index(index_code, start_date, end_date, force_refresh)
        total_return_df = self.get_total_return_index(index_code, start_date, end_date, force_refresh)
        return price_df, total_return_df
    
    def get_index_info(self, index_code: str) -> Dict[str, Any]:
        """获取指数信息"""
        normalized_code = self.price_fetcher._normalize_index_code(index_code)
        
        if normalized_code in SUPPORTED_INDEXES:
            return SUPPORTED_INDEXES[normalized_code].copy()
        else:
            return {
                'code': normalized_code,
                'name': normalized_code,
                'type': 'unknown',
                'default_div_yield': SYNTHESIS_CONFIG['default_div_yield'],
                'supported': False,
            }
    
    def list_supported_indices(self) -> List[Dict[str, Any]]:
        """列出所有支持的指数"""
        result = []
        for code, config in SUPPORTED_INDEXES.items():
            result.append({
                'code': code,
                'name': config['name'],
                'type': config['type'],
                'div_yield': config['default_div_yield'],
                'update_freq': config['update_frequency'],
            })
        return result
    
    def cleanup_old_cache(self, days: int = 30) -> int:
        """
        清理过期缓存文件
        
        Args:
            days: 保留多少天内的缓存文件
            
        Returns:
            清理的文件数量
        """
        import glob
        import time
        
        cleaned_count = 0
        cutoff_time = time.time() - (days * 24 * 3600)
        
        # 清理价格指数缓存
        price_cache_pattern = os.path.join(PRICE_INDEX_CACHE_DIR, "*.parquet")
        for filepath in glob.glob(price_cache_pattern):
            if os.path.getmtime(filepath) < cutoff_time:
                try:
                    os.remove(filepath)
                    cleaned_count += 1
                    logger.info(f"[IndexCacheManager] 清理过期缓存: {filepath}")
                except Exception as e:
                    logger.error(f"[IndexCacheManager] 清理缓存失败: {filepath}, {e}")
        
        # 清理全收益指数缓存
        tr_cache_pattern = os.path.join(TOTAL_RETURN_CACHE_DIR, "*.parquet")
        for filepath in glob.glob(tr_cache_pattern):
            if os.path.getmtime(filepath) < cutoff_time:
                try:
                    os.remove(filepath)
                    cleaned_count += 1
                    logger.info(f"[IndexCacheManager] 清理过期缓存: {filepath}")
                except Exception as e:
                    logger.error(f"[IndexCacheManager] 清理缓存失败: {filepath}, {e}")
        
        logger.info(f"[IndexCacheManager] 清理完成，共清理 {cleaned_count} 个文件")
        return cleaned_count


# 全局实例
_manager_instance = None

def get_index_cache_manager() -> IndexCacheManager:
    """获取全局缓存管理器实例"""
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = IndexCacheManager()
    return _manager_instance