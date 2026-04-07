#!/usr/bin/env python3
"""
预热脚本 v2：全量 A 股个股指标 → Supabase 缓存

改进（相比 v1）：
- 6 线程，避免 AkShare 限流
- 每只股票 3 次重试，间隔 1s
- 断点续传：每 100 只自动保存中间结果到 Supabase
- 只拉取 stock_value_em（PE/PB/PEG），不单独拉成交额（节省一半时间）
- 进度显示更清晰

用法：
  python scripts/run_prewarm_stock.py
  python scripts/run_prewarm_stock.py --limit 20    # 测试模式
"""

import argparse
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

WORKERS = 6
RETRY_TIMES = 3
RETRY_DELAY = 1.0
BATCH_SIZE = 100  # 每隔多少只自动存一次中间结果
TIMEOUT = 20  # 单只股票超时（秒）


# ============================================================
# 工具
# ============================================================

class AtomicCounter:
    def __init__(self):
        self._val = 0
        self._lock = threading.Lock()

    def inc(self):
        with self._lock:
            self._val += 1

    @property
    def value(self):
        return self._val


def safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        if np.isnan(v) or np.isinf(v):
            return None
        return round(v, 4)
    except (ValueError, TypeError):
        return None


# ============================================================
# 股票代码获取
# ============================================================

def get_a_share_codes() -> List[str]:
    """从本地持仓文件提取全量 A 股代码"""
    from data_loader.cache_paths import HISTORY_DIR

    for fp in sorted(HISTORY_DIR.glob("fund_portfolio_hold_em_*.csv"), reverse=True):
        try:
            df = pd.read_csv(fp, dtype=str, usecols=["股票代码"], low_memory=True)
            codes = sorted(set(
                c for c in df["股票代码"].dropna().str.strip().unique()
                if len(c) == 6 and c.isdigit() and c[0] in ("0", "3", "6")
            ))
            logger.info(f"从 {fp.name} 提取到 {len(codes)} 只 A 股")
            return codes
        except Exception as e:
            logger.warning(f"读取 {fp.name} 失败: {e}")
            continue

    logger.error("未找到基金持仓文件")
    return []


def load_existing_metrics() -> set:
    """加载已有的缓存，用于断点续传"""
    try:
        from data_loader.cache_layer import cache_get
        df = cache_get("stock_metrics_all", ttl_seconds=86400, expect_df=True)
        if df is not None and not df.empty and "code" in df.columns:
            codes = set(df["code"].astype(str).str.zfill(6).tolist())
            logger.info(f"已有缓存 {len(codes)} 只股票，将跳过")
            return codes
        else:
            logger.info("已有缓存为空或格式异常，将全量拉取")
    except Exception as e:
        logger.info(f"读取已有缓存失败，将全量拉取: {e}")
    return set()


# ============================================================
# 数据拉取（带重试）
# ============================================================

def fetch_stock_value(code: str) -> Optional[Dict[str, Any]]:
    """拉取单只股票 PE/PB/PEG，最多重试 3 次"""
    import akshare as ak
    from data_loader.akshare_timeout import call_with_timeout

    for attempt in range(1, RETRY_TIMES + 1):
        try:
            df = call_with_timeout(
                ak.stock_value_em, kwargs={"symbol": code}, timeout=TIMEOUT
            )
            if df is None or df.empty:
                return None

            latest = df.iloc[-1]
            result = {
                "code": code,
                "pe_ttm": safe_float(latest.get("PE(TTM)")),
                "pb": safe_float(latest.get("市净率")),
                "peg": safe_float(latest.get("PEG值")),
                "close": safe_float(latest.get("当日收盘价")),
                "market_cap": safe_float(latest.get("总市值")),
                "date": str(latest.get("数据日期", "")),
            }

            # PE 历史分位
            pe_series = pd.to_numeric(df["PE(TTM)"], errors="coerce").dropna()
            if len(pe_series) > 60:
                result["pe_percentile"] = round(
                    (pe_series.iloc[-1] < pe_series).mean() * 100, 1
                )
            else:
                result["pe_percentile"] = None

            return result

        except TimeoutError:
            if attempt < RETRY_TIMES:
                time.sleep(RETRY_DELAY)
                continue
            return None
        except Exception as e:
            if attempt < RETRY_TIMES:
                time.sleep(RETRY_DELAY)
                continue
            # 静默跳过
            return None

    return None


# ============================================================
# 批量拉取
# ============================================================

def batch_fetch(codes: List[str], existing: set, limit: int = 0):
    """批量拉取，跳过已有缓存，支持中间保存"""
    # 过滤掉已缓存的
    target = [c for c in (codes[:limit] if limit > 0 else codes) if c not in existing]
    total = len(target)

    if total == 0:
        logger.info("所有股票均已缓存，无需拉取")
        return pd.DataFrame()

    logger.info(f"需要拉取 {total} 只（已有 {len(existing)} 只，跳过）")

    success = AtomicCounter()
    fail = AtomicCounter()
    lock = threading.Lock()
    all_records: List[Dict] = []
    start_t = time.time()

    from data_loader.cache_layer import cache_set

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(fetch_stock_value, code): code for code in target}

        for i, future in enumerate(as_completed(futures), 1):
            code = futures[future]
            try:
                result = future.result(timeout=TIMEOUT + 5)
                if result is not None:
                    success.inc()
                    with lock:
                        all_records.append(result)
                else:
                    fail.inc()
            except Exception:
                fail.inc()

            # 进度日志
            if i % 50 == 0 or i == total:
                elapsed = time.time() - start_t
                rate = i / elapsed if elapsed > 0 else 0
                eta = (total - i) / rate / 60 if rate > 0 else 0
                logger.info(
                    f"  [{i}/{total}] "
                    f"✅{success.value} ❌{fail.value} "
                    f"{rate:.1f}/s ETA {eta:.0f}min"
                )

            # 中间保存（每 BATCH_SIZE 只）
            if i % BATCH_SIZE == 0 and all_records:
                with lock:
                    if all_records:
                        df_batch = pd.DataFrame(all_records)
                        ok = cache_set("stock_metrics_all", df_batch, expect_df=True)
                        if ok:
                            logger.info(f"  💾 中间保存: {len(all_records)} 只")
                        else:
                            logger.warning(f"  ⚠️ 中间保存失败")

    if not all_records:
        logger.warning("未获取到任何数据")
        return pd.DataFrame()

    elapsed = time.time() - start_t
    logger.info(f"拉取完成: ✅{success.value} ❌{fail.value} 耗时 {elapsed/60:.1f}min")
    return pd.DataFrame(all_records)


# ============================================================
# 合并已有缓存 + 新数据，写入 Supabase
# ============================================================

def merge_and_save(new_df: pd.DataFrame) -> bool:
    """直接写入新数据（覆盖旧缓存）"""
    from data_loader.cache_layer import cache_set

    combined = new_df

    # 标准化
    combined["code"] = combined["code"].astype(str).str.zfill(6)
    cols = ["code", "pe_ttm", "pb", "peg", "close", "market_cap",
            "date", "pe_percentile"]
    combined = combined[[c for c in cols if c in combined.columns]]

    ok = cache_set("stock_metrics_all", combined, expect_df=True)
    if ok:
        size_kb = len(combined.to_json(orient="records")) / 1024
        logger.info(f"✅ Supabase 写入成功: {len(combined)} 只股票, {size_kb:.0f} KB")
    else:
        logger.error("❌ Supabase 写入失败")
    return ok


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="预热全量A股个股指标")
    parser.add_argument("--limit", type=int, default=0, help="只拉取前N只（测试用）")
    parser.add_argument("--from-local", type=str, default="", help="从本地CSV恢复数据（跳过拉取）")
    args = parser.parse_args()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("🚀 个股指标预热 v2（6线程 + 断点续传）")
    logger.info(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if args.limit:
        logger.info(f"   模式: 测试（前 {args.limit} 只）")
    if args.from_local:
        logger.info(f"   模式: 从本地CSV恢复 ({args.from_local})")
    logger.info("=" * 60)

    # 检查 Supabase
    from data_loader.cache_layer import is_ready
    if not is_ready():
        logger.error("❌ Supabase 不可用，请检查 config.py")
        sys.exit(1)
    logger.info("✅ Supabase 连接正常")

    if args.from_local:
        # 从本地 CSV 恢复
        new_df = pd.read_csv(args.from_local)
        logger.info(f"从 CSV 恢复: {len(new_df)} 行, 列: {list(new_df.columns)}")
    else:
        # 获取股票代码
        codes = get_a_share_codes()
        if not codes:
            logger.error("❌ 无法获取股票代码")
            sys.exit(1)

        # 断点续传：加载已有缓存
        existing = load_existing_metrics()

        # 如果已有足够数据（超过 80%），直接结束
        if len(existing) > 0 and len(existing) >= len(codes) * 0.8:
            logger.info(f"已有 {len(existing)} 只缓存（总量 {len(codes)}），足够使用，跳过拉取")
            sys.exit(0)

        # 批量拉取
        new_df = batch_fetch(codes, existing, limit=args.limit)

    if new_df.empty:
        logger.info("没有数据需要保存")
        sys.exit(0)

    # 本地备份（防止写入失败）
    backup_path = PROJECT_ROOT / "data" / "stock_metrics_backup.csv"
    backup_path.parent.mkdir(exist_ok=True)
    new_df.to_csv(backup_path, index=False)
    logger.info(f"💾 本地备份: {backup_path} ({len(new_df)} 行)")

    # 写入 Supabase
    merge_and_save(new_df)

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"✅ 全部完成！总耗时 {elapsed/60:.1f} 分钟")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
