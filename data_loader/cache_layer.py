"""
Supabase 缓存层 — fund_quant_v2
提供统一的缓存读写接口，优先从 Supabase 读取，过期或缺失时回退到 AkShare API。

设计原则：
- 对 data_loader 层透明：通过 cached_api() 装饰器包裹现有 API 调用
- TTL 分级：基金级数据短 TTL，市场级数据长 TTL
- 优雅降级：Supabase 不可用时自动回退到实时 API
"""

from __future__ import annotations

import json
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional
from functools import wraps

import pandas as pd

logger = logging.getLogger(__name__)

# ============================================================
# 全局客户端（懒加载）
# ============================================================
_client = None
_ready = False


def _get_client():
    """懒加载 Supabase 客户端"""
    global _client, _ready
    if _client is not None:
        return _client

    try:
        from config import SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_ENABLED
        if not SUPABASE_ENABLED:
            logger.info("[cache] Supabase 已禁用，使用实时 API")
            _ready = False
            return None

        from supabase import create_client
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        _ready = True
        logger.info("[cache] Supabase 客户端初始化成功")
        return _client
    except Exception as e:
        logger.warning(f"[cache] Supabase 初始化失败，回退到实时 API: {e}")
        _ready = False
        return None


def is_ready() -> bool:
    """检查 Supabase 是否可用"""
    if _client is None:
        _get_client()
    return _ready


# ============================================================
# 缓存键生成
# ============================================================

def _make_cache_key(prefix: str, **kwargs) -> str:
    """生成缓存键：prefix:param1=value1:param2=value2"""
    parts = [prefix]
    for k, v in sorted(kwargs.items()):
        if v is None:
            continue
        val_str = str(v)
        # 对长字符串（如 DataFrame）取 MD5 避免键过长
        if len(val_str) > 200:
            val_str = hashlib.md5(val_str.encode()).hexdigest()[:12]
        parts.append(f"{k}={val_str}")
    return ":".join(parts)


# ============================================================
# JSON 序列化辅助
# ============================================================

def _serialize_df(df: pd.DataFrame) -> str:
    """DataFrame → JSON 字符串（压缩列类型为 list）"""
    # 大表（>1000行）用 CSV 格式，体积更小
    if len(df) > 1000:
        return "__CSV__:" + df.to_csv(index=False)
    return df.to_json(orient="records", date_format="iso")


def _deserialize_df(json_str: str) -> Optional[pd.DataFrame]:
    """JSON 字符串 → DataFrame（含有效性校验）"""
    try:
        from io import StringIO
        if json_str.startswith("__CSV__:"):
            df = pd.read_csv(StringIO(json_str[8:]), dtype=str)
        else:
            df = pd.read_json(StringIO(json_str), orient="records", dtype=str)

        # 校验：DataFrame 必须非空且至少有 1 列
        if df is None or df.empty or len(df.columns) == 0:
            logger.warning("[cache] 反序列化得到空 DataFrame，数据可能损坏")
            return None

        return df
    except Exception as e:
        logger.warning(f"[cache] DataFrame 反序列化失败: {e}")
        return None


def _serialize_value(val: Any) -> str:
    """通用值序列化"""
    if isinstance(val, pd.DataFrame):
        return _serialize_df(val)
    return json.dumps(val, ensure_ascii=False, default=str)


def _deserialize_value(json_str: str, expect_df: bool = False) -> Any:
    """通用值反序列化"""
    if expect_df:
        return _deserialize_df(json_str)
    try:
        return json.loads(json_str)
    except Exception:
        return None


# ============================================================
# 核心读写操作
# ============================================================

def cache_get(
    prefix: str,
    ttl_seconds: int,
    expect_df: bool = False,
    **kwargs,
) -> Optional[Any]:
    """
    从 Supabase 读取缓存。

    Args:
        prefix: 缓存前缀（如 "nav", "index_daily"）
        ttl_seconds: 缓存有效时间（秒）
        expect_df: 是否期望返回 DataFrame
        **kwargs: 用于生成唯一 key 的参数

    Returns:
        缓存数据，或 None（未命中/过期）
    """
    client = _get_client()
    if client is None:
        return None

    cache_key = _make_cache_key(prefix, **kwargs)

    try:
        resp = (
            client.table("data_cache")
            .select("value, updated_at")
            .eq("cache_key", cache_key)
            .maybe_single()
            .execute()
        )

        if resp is None or not resp.data:
            return None

        updated_at = resp.data["updated_at"]
        value_str = resp.data["value"]

        # 解析时间（Supabase 返回 ISO 格式）
        try:
            # 处理时区：Supabase 返回 UTC 时间
            cached_time = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            age = (now - cached_time).total_seconds()
        except Exception:
            age = ttl_seconds + 1  # 解析失败视为过期

        if age > ttl_seconds:
            logger.debug(f"[cache] 过期: {cache_key} (age={age:.0f}s, ttl={ttl_seconds}s)")
            return None

        result = _deserialize_value(value_str, expect_df=expect_df)
        if result is not None:
            logger.debug(f"[cache] 命中: {cache_key} (age={age:.0f}s)")
        return result

    except Exception as e:
        logger.warning(f"[cache] 读取失败: {cache_key}: {e}")
        return None


def cache_set(
    prefix: str,
    value: Any,
    expect_df: bool = False,
    **kwargs,
) -> bool:
    """
    写入缓存到 Supabase（upsert）。

    Args:
        prefix: 缓存前缀
        value: 要缓存的数据
        expect_df: 是否为 DataFrame
        **kwargs: 用于生成唯一 key 的参数

    Returns:
        是否写入成功
    """
    client = _get_client()
    if client is None:
        return False

    cache_key = _make_cache_key(prefix, **kwargs)

    try:
        value_str = _serialize_value(value)

        # 自动路由：大表走 Storage，不塞 data_cache 的 text 字段
        if (isinstance(value, pd.DataFrame)
                and cache_key in _LARGE_TABLE_KEYS
                and len(value_str) > _STORAGE_SIZE_THRESHOLD):
            parquet_data = _serialize_large_df(value)
            path = f"{cache_key}.parquet"
            ok = storage_set(path, parquet_data)
            if ok:
                logger.debug(f"[cache] 大表路由到 Storage: {cache_key} ({len(parquet_data):,} bytes)")
                return True
            else:
                logger.warning(f"[cache] Storage 写入失败，回退到 data_cache: {cache_key}")

        if len(value_str) > 20_000_000:  # 20MB 上限保护（大表用 CSV 压缩）
            logger.warning(f"[cache] 数据过大，跳过缓存: {cache_key} ({len(value_str)} bytes)")
            return False

        client.table("data_cache").upsert(
            {"cache_key": cache_key, "value": value_str},
            on_conflict="cache_key",
        ).execute()

        logger.debug(f"[cache] 写入: {cache_key} ({len(value_str)} bytes)")
        return True

    except Exception as e:
        logger.warning(f"[cache] 写入失败: {cache_key}: {e}")
        return False


# ============================================================
# 装饰器：缓存 API 调用
# ============================================================

def cached_api(
    prefix: str,
    ttl: str = "medium",
    key_params: Optional[list] = None,
    expect_df: bool = False,
):
    """
    API 缓存装饰器。

    用法：
        @cached_api(prefix="nav", ttl="short")
        def load_nav(symbol):
            ...  # 原始 AkShare 调用

    Args:
        prefix: 缓存前缀
        ttl: TTL 级别（"short"/"medium"/"long"/"very_long"），对应 config.CACHE_TTL
        key_params: 哪些函数参数用于生成缓存键（None = 全部）
        expect_df: 返回值是否为 DataFrame
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            from config import CACHE_TTL
            ttl_seconds = CACHE_TTL.get(ttl, 3600)

            # 提取 key 参数
            import inspect
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            if key_params:
                key_dict = {k: bound.arguments.get(k) for k in key_params}
            else:
                key_dict = {k: v for k, v in bound.arguments.items()
                           if k != "self" and not k.startswith("_")}

            # 尝试读缓存
            cached = cache_get(prefix, ttl_seconds, expect_df=expect_df, **key_dict)
            if cached is not None:
                return cached

            # 缓存未命中 → 调用原始函数
            result = func(*args, **kwargs)

            if result is not None:
                cache_set(prefix, result, expect_df=expect_df, **key_dict)

            return result

        return wrapper
    return decorator


# ============================================================
# 批量预热（用于定时任务）
# ============================================================

def warm_cache(prefix: str, value: Any, ttl_hint: str = "long", **kwargs) -> bool:
    """主动预热缓存（供定时脚本调用）"""
    return cache_set(prefix, value, expect_df=isinstance(value, pd.DataFrame), **kwargs)


# ============================================================
# Supabase Storage 层 — 大表数据存储
# ============================================================

# 需要走 Storage 的大表 cache_key 白名单
_LARGE_TABLE_KEYS = frozenset({
    "fund_manager_all",
    "stock_metrics_all",
    "fund_list_all",
    "fund_purchase_all",
})

# 序列化后体积阈值：超过此值走 Storage（否则仍走 data_cache 表）
_STORAGE_SIZE_THRESHOLD = 100_000  # 100 KB


def storage_get(path: str) -> Optional[bytes]:
    """
    从 Supabase Storage 下载文件（二进制）。

    Args:
        path: 文件路径（如 "fund_manager_all.parquet"）

    Returns:
        文件内容 bytes，或 None
    """
    client = _get_client()
    if client is None:
        return None
    try:
        data = client.storage.from_("fund-cache").download(path)
        logger.debug(f"[storage] 下载成功: {path} ({len(data):,} bytes)")
        return data
    except Exception as e:
        logger.debug(f"[storage] 下载失败 {path}: {e}")
        return None


def storage_set(path: str, data: bytes) -> bool:
    """
    上传文件到 Supabase Storage（upsert）。

    Args:
        path: 文件路径
        data: 二进制内容

    Returns:
        是否上传成功
    """
    client = _get_client()
    if client is None:
        return False
    try:
        client.storage.from_("fund-cache").upload(
            path, data, file_options={"upsert": True, "content-type": "application/octet-stream"}
        )
        logger.debug(f"[storage] 上传成功: {path} ({len(data):,} bytes)")
        return True
    except Exception as e:
        logger.warning(f"[storage] 上传失败 {path}: {e}")
        return False


def _serialize_large_df(df: pd.DataFrame) -> bytes:
    """
    大表 DataFrame → Parquet 二进制。

    Parquet 优势：
    - 二进制格式，不存在中文编码问题
    - 自带 schema（列名不会丢失或损坏）
    - zstd 压缩率比 CSV/JSON 高 3-5 倍
    """
    buf = pd.io.common.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="zstd", index=False)
    return buf.getvalue()


def _deserialize_large_df(data: bytes) -> Optional[pd.DataFrame]:
    """
    Parquet 二进制 → DataFrame（含校验）。
    """
    try:
        df = pd.read_parquet(pd.io.common.BytesIO(data))
        if df is None or df.empty or len(df.columns) == 0:
            logger.warning("[storage] Parquet 反序列化得到空 DataFrame")
            return None
        logger.debug(f"[storage] Parquet 解析成功: {len(df)} 行, {len(df.columns)} 列")
        return df
    except Exception as e:
        logger.warning(f"[storage] Parquet 反序列化失败: {e}")
        return None


def cache_get_large(
    cache_key: str,
    ttl_seconds: int = 86_400,
) -> Optional[pd.DataFrame]:
    """
    从 Supabase Storage 读取大表数据。

    优先尝试 Storage（Parquet），如果不存在则回退到 data_cache 表。

    Args:
        cache_key: 缓存键（如 "fund_manager_all"）
        ttl_seconds: TTL（暂未用于 Storage，预留）

    Returns:
        DataFrame 或 None
    """
    # 路径 1: Storage (Parquet)
    path = f"{cache_key}.parquet"
    data = storage_get(path)
    if data is not None:
        df = _deserialize_large_df(data)
        if df is not None:
            return df

    # 路径 2: 回退到 data_cache 表（兼容旧数据）
    logger.debug(f"[storage] Storage 未命中，回退到 data_cache: {cache_key}")
    return cache_get(cache_key, ttl_seconds, expect_df=True)


def cache_set_large(
    cache_key: str,
    df: pd.DataFrame,
) -> bool:
    """
    将大表 DataFrame 写入 Supabase Storage（Parquet 格式）。

    同时也写入 data_cache 表作为备份（方便直接在 Supabase 控制台查看）。

    Args:
        cache_key: 缓存键
        df: DataFrame

    Returns:
        是否写入成功
    """
    if df is None or df.empty:
        return False

    # 序列化为 Parquet
    parquet_data = _serialize_large_df(df)
    path = f"{cache_key}.parquet"

    # 写入 Storage
    ok = storage_set(path, parquet_data)
    if ok:
        logger.info(
            f"[storage] 大表写入 Storage: {cache_key} "
            f"({len(df)} 行, {len(parquet_data):,} bytes)"
        )
    else:
        logger.warning(f"[storage] Storage 写入失败，回退到 data_cache: {cache_key}")

    # 同时写入 data_cache 作为备份
    cache_ok = cache_set(cache_key, df, expect_df=True)

    return ok or cache_ok
