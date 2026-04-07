"""
大规模活跃基金预热脚本 — fund_quant_v2
一次性缓存活跃基金的净值数据到 Supabase。

使用多进程并发（非多线程），规避 AkShare libmini_racer V8 引擎的线程安全问题。

筛选策略：
1. fund_name_em() 全量基金列表
2. fund_purchase_em() 筛选「开放申购」+「开放赎回」
3. 去除货币/理财/QDII 等不支持类型
4. 预热：单位净值 + 累计净值（最核心数据）

运行方式：
    # 完整预热（5进程并发，自动跳过已有缓存）
    python -m scripts.prewarm_active_funds

    # 限制数量（测试用）
    python -m scripts.prewarm_active_funds --limit 100

    # 只预热净值（跳过全局数据）
    python -m scripts.prewarm_active_funds --nav-only

    # 调整进程数
    python -m scripts.prewarm_active_funds --workers 3

    # 跳过已有缓存（增量更新）
    python -m scripts.prewarm_active_funds --skip-existing
"""

from __future__ import annotations

import argparse
import logging
import time
import sys
import os
from multiprocessing import Pool, Manager
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# 1. 活跃基金筛选
# ============================================================

# 不支持的基金类型关键词（不预热）
SKIP_TYPE_KEYWORDS = [
    "货币", "理财", "QDII", "REITs", "FOF",
    "商品", "黄金", "原油", "白银",
]

# 不支持的基金名称关键词
SKIP_NAME_KEYWORDS = [
    "货币", "理财", "QDII", "REITs", "FOF",
]


def fetch_active_funds(limit: int = 0, offset: int = 0) -> list[dict]:
    """
    筛选活跃基金（开放申购 + 开放赎回）。

    Returns:
        [{"code": "000001", "name": "华夏成长", "type": "混合型-偏股"}, ...]
    """
    import akshare as ak

    # --- 1. 获取全量基金列表 ---
    logger.info("📋 获取全量基金列表（fund_name_em）...")
    try:
        df_list = ak.fund_name_em()
        if df_list is None or df_list.empty:
            logger.error("❌ 基金列表获取失败")
            return []
        logger.info(f"  ✅ 共 {len(df_list)} 只基金")
    except Exception as e:
        logger.error(f"❌ fund_name_em 失败: {e}")
        return []

    # 确保必要列存在
    if "基金代码" not in df_list.columns:
        logger.error("❌ 基金列表缺少'基金代码'列")
        return []
    if "基金类型" not in df_list.columns:
        for col in df_list.columns:
            if "类型" in str(col):
                df_list = df_list.rename(columns={col: "基金类型"})
                break
    name_col = "基金名称" if "基金名称" in df_list.columns else (
        df_list.columns[1] if len(df_list.columns) > 1 else "基金名称"
    )
    if name_col != "基金名称":
        df_list = df_list.rename(columns={name_col: "基金名称"})

    # --- 2. 筛选类型（去除不支持类型）---
    mask_valid = df_list["基金代码"].apply(
        lambda x: str(x).isdigit() and len(str(x)) == 6
    )
    if "基金类型" in df_list.columns:
        for kw in SKIP_TYPE_KEYWORDS:
            mask_valid &= ~df_list["基金类型"].astype(str).str.contains(kw, na=False)
    mask_valid &= ~df_list["基金名称"].astype(str).apply(
        lambda x: any(kw in x for kw in SKIP_NAME_KEYWORDS)
    )
    df_filtered = df_list[mask_valid].copy()
    logger.info(f"  📊 类型过滤后: {len(df_filtered)} 只（去除货币/QDII/REITs等）")

    # --- 3. 获取申购状态并筛选活跃基金 ---
    logger.info("📋 获取申购状态（fund_purchase_em）...")
    try:
        df_purchase = ak.fund_purchase_em()
        if df_purchase is not None and not df_purchase.empty:
            if "基金代码" not in df_purchase.columns:
                for col in df_purchase.columns:
                    if "代码" in str(col):
                        df_purchase = df_purchase.rename(columns={col: "基金代码"})
                        break

            purchase_col = None
            redeem_col = None
            for col in df_purchase.columns:
                if "申购" in str(col) and "状态" in str(col):
                    purchase_col = col
                elif "赎回" in str(col) and "状态" in str(col):
                    redeem_col = col

            if purchase_col and redeem_col:
                active_codes = set()
                for _, row in df_purchase.iterrows():
                    code = str(row.get("基金代码", "")).strip()
                    p_status = str(row.get(purchase_col, ""))
                    r_status = str(row.get(redeem_col, ""))
                    if ("开放" in p_status or "限大额" in p_status) and "开放" in r_status:
                        if code.isdigit() and len(code) == 6:
                            active_codes.add(code)

                df_filtered = df_filtered[
                    df_filtered["基金代码"].astype(str).isin(active_codes)
                ]
                logger.info(
                    f"  ✅ 申购状态过滤后: {len(df_filtered)} 只活跃基金"
                    f"（开放申购+开放赎回）"
                )
            else:
                logger.warning("  ⚠️ 申购状态列未找到，跳过申购状态过滤")
        else:
            logger.warning("  ⚠️ fund_purchase_em 返回空数据，跳过申购状态过滤")
    except Exception as e:
        logger.warning(f"  ⚠️ fund_purchase_em 失败: {e}，跳过申购状态过滤")

    # --- 4. 构建 result ---
    result = []
    type_col = "基金类型" if "基金类型" in df_filtered.columns else None
    for _, row in df_filtered.iterrows():
        code = str(row["基金代码"]).strip()
        name = str(row.get("基金名称", "")).strip()
        fund_type = str(row[type_col]).strip() if type_col else ""
        result.append({"code": code, "name": name, "type": fund_type})

    seen = set()
    unique = []
    for item in result:
        if item["code"] not in seen:
            seen.add(item["code"])
            unique.append(item)
    result = unique

    result.sort(key=lambda x: x["code"])

    if offset > 0:
        result = result[offset:]
        logger.info(f"  ⏩ 偏移 {offset}，剩余 {len(result)} 只")
    if limit > 0:
        result = result[:limit]
        logger.info(f"  ✂️ 限制 {limit} 只")

    return result


# ============================================================
# 2. 单只基金预热（进程内运行，独立 V8 实例）
# ============================================================

def _prewarm_single_fund(args: tuple) -> dict:
    """
    在独立进程中预热单只基金。
    每个进程有自己的 V8 引擎实例，不会冲突。

    Args:
        args: (code, name, skip_existing, delay)

    Returns:
        {"code", "name", "success", "nav_rows", "nav2_rows", "time", "skipped"}
    """
    code, name, skip_existing, delay = args

    import akshare as ak
    from data_loader.cache_layer import cache_get, cache_set

    t0 = time.time()
    nav_rows = 0
    nav2_rows = 0
    success = True
    skipped = False

    for indicator in ["单位净值走势", "累计净值走势"]:
        # 跳过已有缓存
        if skip_existing:
            try:
                cached = cache_get("fund_nav", ttl_seconds=86400, expect_df=True,
                                   symbol=code, indicator=indicator)
                if cached is not None and not cached.empty:
                    if indicator == "单位净值走势":
                        nav_rows = len(cached)
                    else:
                        nav2_rows = len(cached)
                    continue
            except Exception:
                pass

        try:
            df = ak.fund_open_fund_info_em(symbol=code, indicator=indicator)
            if df is not None and not df.empty:
                cache_set("fund_nav", df, expect_df=True, symbol=code, indicator=indicator)
                if indicator == "单位净值走势":
                    nav_rows = len(df)
                else:
                    nav2_rows = len(df)
            else:
                success = False
        except Exception as e:
            success = False

        # 指标间延迟
        time.sleep(0.3)

    elapsed = time.time() - t0

    # 如果两个指标都从缓存命中，标记为跳过
    if skip_existing and nav_rows > 0 and nav2_rows > 0 and elapsed < 0.5:
        skipped = True

    return {
        "code": code,
        "name": name,
        "success": success,
        "nav_rows": nav_rows,
        "nav2_rows": nav2_rows,
        "time": elapsed,
        "skipped": skipped,
    }


# ============================================================
# 3. 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="大规模活跃基金预热脚本（多进程版）")
    parser.add_argument("--limit", type=int, default=0,
                        help="最大预热基金数量（0=不限）")
    parser.add_argument("--offset", type=int, default=0,
                        help="偏移量（从第N只开始，用于断点续跑）")
    parser.add_argument("--nav-only", action="store_true",
                        help="只预热净值数据，跳过全局市场数据")
    parser.add_argument("--skip-existing", action="store_true",
                        help="跳过已有缓存的基金（增量更新）")
    parser.add_argument("--workers", type=int, default=5,
                        help="并发进程数（默认5，推荐3-5）")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="每只基金之间的延迟秒数（默认0.5）")
    parser.add_argument("--batch-size", type=int, default=20,
                        help="每批次的基金数量（默认20，控制 Supabase 连接压力）")
    args = parser.parse_args()

    # macOS 上多进程需要这个
    multiprocessing_start = "fork" if sys.platform != "darwin" else "spawn"
    os.environ.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("🚀 大规模活跃基金预热开始（多进程版）")
    logger.info(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   配置: workers={args.workers}, batch={args.batch_size}, "
                f"skip_existing={args.skip_existing}, delay={args.delay}s")
    logger.info("=" * 60)

    # ---- 检查 Supabase ----
    from data_loader.cache_layer import is_ready
    if not is_ready():
        logger.error("❌ Supabase 不可用")
        sys.exit(1)
    logger.info("✅ Supabase 连接正常")

    # ---- 筛选活跃基金 ----
    funds = fetch_active_funds(limit=args.limit, offset=args.offset)
    if not funds:
        logger.error("❌ 无活跃基金可预热")
        sys.exit(1)

    total = len(funds)
    logger.info(f"\n📊 共 {total} 只活跃基金待预热")
    logger.info(f"   预计耗时: ~{total / (args.workers * 6) * 60:.0f} 分钟"
                f"（{args.workers} 进程 × ~6只/分/进程）")

    # ---- 预热全局数据（可选）----
    if not args.nav_only:
        logger.info("\n📌 预热全局共享数据...")
        try:
            from scripts.prewarm_cache import prewarm_market_data
            mkt_result = prewarm_market_data()
            logger.info(f"  市场数据: 成功 {mkt_result['success']}，失败 {mkt_result['failed']}")
        except Exception as e:
            logger.warning(f"  ⚠️ 市场数据预热失败: {e}")

        import akshare as ak
        from data_loader.cache_layer import cache_set_large
        try:
            df = ak.fund_name_em()
            if df is not None and not df.empty:
                cache_set_large("fund_list_all", df)
                logger.info(f"  ✅ 基金列表: {len(df)} 条")
        except Exception as e:
            logger.warning(f"  ⚠️ 基金列表: {e}")

        try:
            df = ak.fund_purchase_em()
            if df is not None and not df.empty:
                cache_set_large("fund_purchase_all", df)
                logger.info(f"  ✅ 申购状态: {len(df)} 条")
        except Exception as e:
            logger.warning(f"  ⚠️ 申购状态: {e}")

    # ---- 分批并发预热 ----
    logger.info(f"\n🔥 开始预热 {total} 只基金的净值数据（{args.workers} 进程）...")

    ok_count = 0
    fail_count = 0
    skip_count = 0
    done_count = 0
    progress_file = Path("/tmp/prewarm_active_progress.txt")
    batch_size = args.batch_size

    # 使用 spawn 方式启动子进程（macOS 安全要求）
    from multiprocessing import get_context
    ctx = get_context("spawn")

    with ctx.Pool(processes=args.workers) as pool:
        # 分批提交，控制 Supabase 连接压力
        for batch_start in range(0, total, batch_size):
            batch_end = min(batch_start + batch_size, total)
            batch_funds = funds[batch_start:batch_end]

            # 构建任务参数
            tasks = [
                (f["code"], f["name"], args.skip_existing, args.delay)
                for f in batch_funds
            ]

            # 提交本批次
            results = pool.map(_prewarm_single_fund, tasks)

            # 统计本批次结果
            for r in results:
                done_count += 1
                if r.get("skipped"):
                    skip_count += 1
                elif r["success"]:
                    ok_count += 1
                else:
                    fail_count += 1

            # 进度显示
            elapsed_min = (time.time() - start_time) / 60
            rate = done_count / elapsed_min if elapsed_min > 0 else 0
            remain = (total - done_count) / rate if rate > 0 else 0

            logger.info(
                f"  [{done_count}/{total}] | "
                f"成功 {ok_count} 失败 {fail_count} 跳过 {skip_count} | "
                f"{rate:.0f} 只/分, 剩余 ~{remain:.0f} 分"
            )

            # 保存进度
            if done_count % 100 == 0 or batch_end >= total:
                try:
                    progress_file.write_text(f"{done_count}\n")
                except Exception:
                    pass

            # 批次间短暂休息，避免 Supabase 连接池被打满
            if batch_end < total:
                time.sleep(1.0)

    # ---- 清理进度文件 ----
    try:
        progress_file.unlink(missing_ok=True)
    except Exception:
        pass

    # ---- 汇总 ----
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ 预热完成！")
    logger.info(f"   总耗时: {elapsed:.0f} 秒 ({elapsed/60:.1f} 分钟)")
    logger.info(f"   成功: {ok_count}, 失败: {fail_count}, 跳过: {skip_count}")
    actual = ok_count + fail_count
    logger.info(f"   成功率: {ok_count/actual*100:.1f}%" if actual > 0 else "")
    logger.info(f"   平均速度: {total / (elapsed/60):.0f} 只/分钟")
    logger.info("=" * 60)

    # 缓存统计
    from data_loader.cache_layer import _get_client
    client = _get_client()
    if client:
        try:
            resp = client.table("data_cache").select("cache_key", count="exact").execute()
            logger.info(f"   Supabase 缓存总条目: {resp.count}")
        except Exception:
            pass


if __name__ == "__main__":
    main()
