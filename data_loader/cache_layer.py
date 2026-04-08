"""
缓存层 — fund_quant_v2（SQLite 模式简化版）

生产代码不再使用此模块。
所有数据读取已迁移到 db_accessor.py（SQLite 优先）。

保留的函数仅用于 scripts/ 目录下的数据采集和预热脚本。
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _get_client():
    """SQLite 模式下返回 None"""
    return None


def is_ready() -> bool:
    """SQLite 模式下始终返回 False"""
    return False


def cache_get(prefix: str, ttl_seconds: int = 3600, expect_df: bool = False, **kwargs) -> Optional[Any]:
    """SQLite 模式下始终返回 None"""
    return None


def cache_set(prefix: str, value: Any, expect_df: bool = False, **kwargs) -> bool:
    """SQLite 模式下始终返回 False"""
    return False


def cache_get_large(cache_key: str, ttl_seconds: int = 86400) -> Optional[pd.DataFrame]:
    """SQLite 模式下始终返回 None"""
    return None


def cache_set_large(cache_key: str, df: pd.DataFrame) -> bool:
    """SQLite 模式下始终返回 False"""
    return False
