"""
基准数据缓存优化方案

解决 P1-2 问题：基准数据重复加载
"""

import logging
from functools import lru_cache
from typing import Optional
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================
# 基准数据缓存池
# ============================================================

class BenchmarkCachePool:
    """
    基准数据缓存池（单例模式）

    功能：
    1. 缓存已加载的基准数据
    2. 支持跨基金复用（如沪深300）
    3. 自动记录加载次数，用于性能分析
    """

    _instance = None  # 单例实例
    _initialized = False

    def __new__(cls):
        """单例模式：确保全局只有一个缓存池"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化（仅执行一次）"""
        if not self._initialized:
            self._cache = {}  # {cache_key: (df, description, timestamp)}
            self._stats = {}  # {cache_key: hit_count}
            self._initialized = True
            logger.info("[BenchmarkCachePool] 缓存池初始化完成")

    def _make_cache_key(self, code: str, start: str, end: str) -> str:
        """
        生成缓存键

        Args:
            code: 指数代码（如 "sh000300"）
            start: 开始日期（格式：YYYYMMDD）
            end: 结束日期（格式：YYYYMMDD）

        Returns:
            缓存键（如 "sh000300_20200101_20250101"）
        """
        return f"{code}_{start}_{end}"

    def get(
        self,
        code: str,
        start: str,
        end: str,
    ) -> Optional[tuple[pd.DataFrame, str]]:
        """
        从缓存获取基准数据（P1-优化：支持日期范围裁剪）

        策略：
        1. 首先尝试精确匹配（code + start + end）
        2. 如果精确匹配失败，尝试查找同一 code 的缓存（范围可能更大）
        3. 如果找到更大的缓存，根据请求的日期范围进行裁剪

        Args:
            code: 指数代码
            start: 开始日期
            end: 结束日期

        Returns:
            (df, description) 或 None（缓存未命中）
        """
        # P1-优化：先尝试精确匹配
        cache_key = self._make_cache_key(code, start, end)

        if cache_key in self._cache:
            df, description, _ = self._cache[cache_key]
            self._stats[cache_key] = self._stats.get(cache_key, 0) + 1
            logger.info(f"[BenchmarkCachePool] 缓存命中（精确）: {cache_key}（命中次数：{self._stats[cache_key]}）")
            return df.copy(), description

        # P1-优化：如果没有精确匹配，尝试查找同一 code 的缓存（范围可能更大）
        for cached_key, (df, desc, _) in self._cache.items():
            cached_code = cached_key.split('_')[0]
            if cached_code == code:
                # 找到同一 code 的缓存，尝试裁剪
                request_start = pd.to_datetime(start, format='%Y%m%d')
                request_end = pd.to_datetime(end, format='%Y%m%d')

                # 检查缓存的日期范围是否覆盖请求的范围
                cached_df = df.copy()
                cached_df['date'] = pd.to_datetime(cached_df['date'])
                cached_start = cached_df['date'].min()
                cached_end = cached_df['date'].max()

                if cached_start <= request_start and cached_end >= request_end:
                    # 裁剪到请求的范围
                    cropped_df = cached_df[
                        (cached_df['date'] >= request_start) &
                        (cached_df['date'] <= request_end)
                    ].copy()

                    # 更新统计（使用原始 cache_key）
                    self._stats[cached_key] = self._stats.get(cached_key, 0) + 1
                    logger.info(f"[BenchmarkCachePool] 缓存命中（裁剪）: {cached_key} -> 请求范围 {start}~{end}（命中次数：{self._stats[cached_key]}）")
                    return cropped_df, desc

        return None

    def set(
        self,
        code: str,
        start: str,
        end: str,
        df: pd.DataFrame,
        description: str = "",
    ) -> None:
        """
        将基准数据存入缓存

        Args:
            code: 指数代码
            start: 开始日期
            end: 结束日期
            df: 基准数据 DataFrame
            description: 描述信息
        """
        cache_key = self._make_cache_key(code, start, end)
        self._cache[cache_key] = (df, description, datetime.now())
        logger.info(f"[BenchmarkCachePool] 缓存存入: {cache_key}（缓存数量：{len(self._cache)}）")

    def clear(self) -> None:
        """清空缓存（用于测试或强制刷新）"""
        cache_count = len(self._cache)
        self._cache.clear()
        self._stats.clear()
        logger.info(f"[BenchmarkCachePool] 缓存已清空（共清理 {cache_count} 条记录）")

    def get_stats(self) -> dict:
        """获取缓存统计信息"""
        return {
            "cache_size": len(self._cache),
            "hit_stats": self._stats.copy(),
        }


# ============================================================
# 全局缓存池实例
# ============================================================

_benchmark_cache_pool = BenchmarkCachePool()


def get_benchmark_cache_pool() -> BenchmarkCachePool:
    """获取全局基准缓存池实例"""
    return _benchmark_cache_pool


# ============================================================
# 装饰器：自动缓存基准加载函数
# ============================================================

def benchmark_cached(func):
    """
    装饰器：自动缓存基准数据加载函数

    使用示例：
        @benchmark_cached
        def load_benchmark_data(code: str, start: str, end: str) -> pd.DataFrame:
            # ... 网络请求 ...
            return df
    """
    def wrapper(code: str, start: str, end: str, *args, **kwargs):
        # 尝试从缓存获取
        cached_result = _benchmark_cache_pool.get(code, start, end)
        if cached_result is not None:
            return cached_result

        # 缓存未命中，调用原函数
        result = func(code, start, end, *args, **kwargs)

        # 解析返回值（支持 (df, description) 或仅 df）
        if isinstance(result, tuple):
            df, description = result
        else:
            df = result
            description = ""

        # 存入缓存
        if isinstance(df, pd.DataFrame) and not df.empty:
            _benchmark_cache_pool.set(code, start, end, df, description)

        return result

    return wrapper


# ============================================================
# 性能监控
# ============================================================

def print_benchmark_cache_stats():
    """打印缓存统计信息"""
    stats = _benchmark_cache_pool.get_stats()
    print("\n" + "=" * 60)
    print("基准数据缓存统计")
    print("=" * 60)
    print(f"缓存数量: {stats['cache_size']}")
    print(f"命中次数统计:")
    for cache_key, hit_count in stats['hit_stats'].items():
        print(f"  {cache_key}: {hit_count} 次")
    print("=" * 60 + "\n")


# ============================================================
# 测试代码
# ============================================================

if __name__ == "__main__":
    # 测试缓存功能
    print("测试基准数据缓存池")

    cache = get_benchmark_cache_pool()

    # 测试 1: 缓存未命中
    print("\n[测试 1] 缓存未命中")
    result = cache.get("sh000300", "20200101", "20250101")
    print(f"结果: {result}")

    # 测试 2: 缓存存入
    print("\n[测试 2] 缓存存入")
    dates = pd.date_range('2020-01-01', '2025-01-01')
    df = pd.DataFrame({
        'date': dates,
        'bm_ret': [0.01] * len(dates)
    })
    cache.set("sh000300", "20200101", "20250101", df, "沪深300全收益")

    # 测试 3: 缓存命中
    print("\n[测试 3] 缓存命中")
    result = cache.get("sh000300", "20200101", "20250101")
    print(f"结果: {result[0].shape if result else None}")

    # 测试 4: 缓存统计
    print("\n[测试 4] 缓存统计")
    print_benchmark_cache_stats()

    # 测试 5: 装饰器
    print("\n[测试 5] 装饰器")
    @benchmark_cached
    def load_mock_benchmark(code: str, start: str, end: str):
        print(f"  [模拟加载] 加载 {code}")
        return df, "模拟基准"

    # 第一次调用（缓存未命中）
    print("第一次调用:")
    result1 = load_mock_benchmark("sh000300", "20200101", "20250101")

    # 第二次调用（缓存命中）
    print("第二次调用:")
    result2 = load_mock_benchmark("sh000300", "20200101", "20250101")

    print("\n缓存统计:")
    print_benchmark_cache_stats()


# ============================================================
# 全局单例实例（供外部导入使用）
# ============================================================
benchmark_cache = get_benchmark_cache_pool()
