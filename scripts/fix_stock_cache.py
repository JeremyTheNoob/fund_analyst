#!/usr/bin/env python3
"""快速修复：清除 stock_metrics_all 的坏缓存，让预热脚本可以重新写入"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data_loader.cache_layer import _get_client, cache_get

# 先看一下现在的缓存长什么样
df = cache_get("stock_metrics_all", ttl_seconds=86400, expect_df=True)
if df is not None and not df.empty:
    print(f"当前缓存: {len(df)} 行, 列名: {list(df.columns)}")
    print(df.head(3))
else:
    print("当前无缓存")

# 删除坏缓存
client = _get_client()
if client:
    resp = client.table("data_cache").delete().eq("cache_key", "stock_metrics_all").execute()
    print(f"\n已删除 stock_metrics_all 缓存")
else:
    print("Supabase 不可用")
