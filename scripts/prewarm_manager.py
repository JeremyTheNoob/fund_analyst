"""
基金经理全量数据上传脚本 — fund_quant_v2

将本地 fund_manager_current.csv 一次性上传到 Supabase data_cache，
作为全局共享数据（Cloud 部署不再需要本地 CSV）。

运行方式：
    python -m scripts.prewarm_manager
    python -m scripts.prewarm_manager --force   # 强制覆盖已有缓存
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="上传基金经理全量数据到 Supabase")
    parser.add_argument("--force", action="store_true", help="强制覆盖已有缓存")
    args = parser.parse_args()

    # 检查本地文件
    from data_loader.cache_paths import FUND_MANAGER_CURRENT
    from pathlib import Path

    csv_path = Path(FUND_MANAGER_CURRENT)
    if not csv_path.exists():
        logger.error(f"❌ 本地文件不存在: {csv_path}")
        logger.info("💡 请先运行: python -m scripts.build_manager_current")
        sys.exit(1)

    import pandas as pd
    df = pd.read_csv(csv_path, dtype=str)
    logger.info(f"📊 本地经理表: {len(df):,} 行, {len(df.columns)} 列, {csv_path.stat().st_size / 1024 / 1024:.1f} MB")

    # 检查 Supabase 连接
    from data_loader.cache_layer import is_ready, cache_get_large, cache_set_large
    if not is_ready():
        logger.error("❌ Supabase 不可用，请检查配置")
        sys.exit(1)

    # 检查已有缓存
    if not args.force:
        existing = cache_get_large("fund_manager_all", 86_400)
        if existing is not None and not existing.empty:
            logger.info(f"⚠️ Supabase 已有经理数据: {len(existing):,} 行（更新日期见 updated_at）")
            logger.info("💡 使用 --force 强制覆盖")
            sys.exit(0)

    # 上传（优先 Storage Parquet，同时备份 data_cache 表）
    logger.info("⬆️ 开始上传到 Supabase Storage...")
    start_t = time.time()
    ok = cache_set_large("fund_manager_all", df)
    elapsed = time.time() - start_t

    if ok:
        logger.info(f"✅ 上传成功！{len(df):,} 行，耗时 {elapsed:.1f}s")
        logger.info("💡 Cloud 部署时将自动从此缓存读取，不再依赖本地 CSV")
    else:
        logger.error("❌ 上传失败，请检查 Supabase 连接和缓存大小限制")
        sys.exit(1)


if __name__ == "__main__":
    main()
