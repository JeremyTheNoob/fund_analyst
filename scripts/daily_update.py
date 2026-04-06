"""
每日增量更新脚本 — fund_quant_v2
更新 Supabase 缓存中已缓存的数据，确保每日数据新鲜度。

更新策略：
1. 市场数据刷新（强制更新，忽略 TTL）：
   - 指数日线、国债收益率、中债综合指数、转债估值、港股指数
2. 基金类型列表刷新（7天缓存，仅工作日更新）
3. 热门基金净值刷新（从排名快照中读取）
4. 缓存清理（删除过期数据，释放 Supabase 空间）
5. [可选] 个股指标全量预热（stock_metrics_all，约 50 分钟）

运行方式：
    python -m scripts.daily_update
    python -m scripts.daily_update --market    # 只更新市场数据
    python -m scripts.daily_update --cleanup   # 只做清理
    python -m scripts.daily_update --stock-metrics  # 额外预热个股指标
    python -m scripts.daily_update --dry-run   # 模拟运行，不实际更新

通常由 GitHub Actions cron 每天定时触发。
"""

from __future__ import annotations

import argparse
import logging
import time
import sys
from datetime import datetime, timezone, timedelta

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# 辅助
# ============================================================

def _is_workday() -> bool:
    """简单判断今天是否是工作日（周一~周五）"""
    return datetime.now().weekday() < 5


# ============================================================
# 1. 市场数据刷新
# ============================================================

def refresh_market_data(dry_run: bool = False) -> dict:
    """
    刷新所有市场级共享数据（强制更新，绕过 TTL 检查）。

    Returns:
        {"success": int, "failed": int, "skipped": int}
    """
    from data_loader.base_api import (
        _ak_index_daily_main,
        _ak_bond_us_rate,
        _ak_bond_composite_index,
        load_cb_value_analysis,
        _ak_hk_index_daily,
    )
    from data_loader.cache_layer import cache_set

    results = {"success": 0, "failed": 0, "skipped": 0}
    today = datetime.now().strftime("%Y%m%d")

    if dry_run:
        logger.info("🔍 [DRY RUN] 市场数据刷新（不会实际写入）")

    # --- 指数日线 ---
    indices = {
        "sh000300": "沪深300",
        "sh000905": "中证500",
        "sh000852": "中证1000",
        "sh000050": "上证50",
        "sz399006": "创业板指",
        "sh000688": "科创50",
        "sh000985": "中证全指",
        "sz399370": "国证成长",
        "sz399371": "国证价值",
    }

    logger.info("\n📈 刷新指数日线...")
    for symbol, name in indices.items():
        try:
            df = _ak_index_daily_main(symbol)
            if df is not None and not df.empty:
                if not dry_run:
                    cache_set("index_daily", df, expect_df=True, symbol=symbol)
                logger.info(f"  ✅ {name} ({symbol}): {len(df)} 行")
                results["success"] += 1
            else:
                logger.warning(f"  ⚠️ {name}: 无数据")
                results["failed"] += 1
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"  ❌ {name} ({symbol}): {e}")
            results["failed"] += 1

    # --- 港股指数 ---
    hk_indices = {"HSI": "恒生指数", "HSCEI": "恒生国企", "HSTECH": "恒生科技"}
    logger.info("\n📈 刷新港股指数...")
    for symbol, name in hk_indices.items():
        try:
            df = _ak_hk_index_daily(symbol)
            if df is not None and not df.empty:
                if not dry_run:
                    cache_set("hk_index_daily", df, expect_df=True, symbol=symbol)
                logger.info(f"  ✅ {name}: {len(df)} 行")
                results["success"] += 1
            else:
                logger.warning(f"  ⚠️ {name}: 无数据")
                results["failed"] += 1
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"  ❌ {name}: {e}")
            results["failed"] += 1

    # --- 国债收益率 ---
    logger.info("\n📈 刷新国债收益率...")
    try:
        start_date = str(int(today[:4]) - 2) + today[4:]
        df = _ak_bond_us_rate(start_date=start_date)
        if df is not None and not df.empty:
            if not dry_run:
                cache_set("bond_us_rate", df, expect_df=True, start_date=start_date)
            logger.info(f"  ✅ 国债收益率: {len(df)} 行")
            results["success"] += 1
        else:
            logger.warning("  ⚠️ 国债收益率: 无数据")
            results["failed"] += 1
    except Exception as e:
        logger.warning(f"  ❌ 国债收益率: {e}")
        results["failed"] += 1

    # --- 中债综合指数 ---
    logger.info("\n📈 刷新中债综合指数...")
    for indicator in ["财富", "总值"]:
        try:
            df = _ak_bond_composite_index(indicator=indicator)
            if df is not None and not df.empty:
                if not dry_run:
                    cache_set("bond_composite", df, expect_df=True, indicator=indicator)
                logger.info(f"  ✅ 中债综合({indicator}): {len(df)} 行")
                results["success"] += 1
            else:
                logger.warning(f"  ⚠️ 中债综合({indicator}): 无数据")
                results["failed"] += 1
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"  ❌ 中债综合({indicator}): {e}")
            results["failed"] += 1

    # --- 全市场可转债估值 ---
    logger.info("\n📈 刷新转债估值...")
    try:
        df = load_cb_value_analysis()
        if df is not None and not df.empty:
            logger.info(f"  ✅ 转债估值: {len(df)} 行")
            results["success"] += 1
        else:
            logger.warning("  ⚠️ 转债估值: 无数据")
            results["failed"] += 1
    except Exception as e:
        logger.warning(f"  ❌ 转债估值: {e}")
        results["failed"] += 1

    return results


# ============================================================
# 2. 基金类型全量刷新
# ============================================================

def refresh_fund_type_index(dry_run: bool = False) -> dict:
    """
    刷新基金类型索引（fund_name_em 全量数据 → 逐个写入缓存）。

    fund_name_em 返回所有基金的代码和类型，写入 Supabase 后供所有 worker 共享。
    """
    from data_loader.cache_layer import cache_set
    import akshare as ak

    results = {"success": 0, "failed": 0}

    if dry_run:
        logger.info("🔍 [DRY RUN] 基金类型索引刷新（不会实际写入）")

    logger.info("\n📋 刷新基金类型索引（fund_name_em）...")
    try:
        df = ak.fund_name_em()
        if df is None or df.empty:
            logger.warning("  ⚠️ fund_name_em 返回空数据")
            return results

        if "基金代码" not in df.columns or "基金类型" not in df.columns:
            logger.warning(f"  ⚠️ fund_name_em 列名异常: {list(df.columns)}")
            return results

        total = len(df)
        logger.info(f"  📊 共 {total} 只基金，开始逐个写入缓存...")

        batch_size = 100
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            batch = df.iloc[start:end]

            for _, row in batch.iterrows():
                code = str(row["基金代码"]).strip()
                fund_type = str(row["基金类型"]).strip()

                if not code.isdigit() or len(code) != 6:
                    continue

                try:
                    if not dry_run:
                        cache_set("fund_type", fund_type, expect_df=False, symbol=code)
                    results["success"] += 1
                except Exception as e:
                    results["failed"] += 1

            logger.info(f"  ⏳ 进度: {end}/{total}")

            # 每 500 条休息一下
            if end % 500 == 0:
                time.sleep(2)

        logger.info(f"  ✅ 基金类型索引刷新完成: 成功 {results['success']}，失败 {results['failed']}")

    except Exception as e:
        logger.error(f"  ❌ fund_name_em 加载失败: {e}")
        results["failed"] += 1

    return results


# ============================================================
# 3. 缓存清理
# ============================================================

def cleanup_expired_cache(dry_run: bool = False) -> dict:
    """
    清理 Supabase 中过期/无用的缓存数据。

    清理策略：
    - 删除 30 天未更新的数据（可能是废弃的基金代码）
    - 保留常用的市场数据（即使超过 30 天）

    注意：Supabase anon key 无法执行 DELETE，需要通过 RPC 或 Dashboard。
    这里只做诊断和报告。
    """
    from config import SUPABASE_URL, SUPABASE_ANON_KEY
    from supabase import create_client

    results = {"checked": 0, "expired": 0, "kept": 0}

    try:
        client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    except Exception as e:
        logger.warning(f"⚠️ Supabase 连接失败，跳过清理: {e}")
        return results

    logger.info("\n🧹 缓存清理诊断...")

    # 扫描所有缓存条目
    try:
        # 分页查询（Supabase 默认限制 1000 行）
        all_keys = []
        offset = 0
        page_size = 1000

        while True:
            resp = (
                client.table("data_cache")
                .select("cache_key, updated_at")
                .range(offset, offset + page_size - 1)
                .order("updated_at", desc=False)
                .execute()
            )

            if not resp.data:
                break

            all_keys.extend(resp.data)
            offset += page_size

            if len(resp.data) < page_size:
                break

        logger.info(f"  📊 缓存总条目: {len(all_keys)}")

        # 分析过期数据
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        expired_keys = []
        kept_keys = []

        for item in all_keys:
            results["checked"] += 1
            try:
                updated_str = item["updated_at"]
                updated_at = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))

                if updated_at < cutoff:
                    # 检查是否为市场级数据（需要长期保留）
                    key = item["cache_key"]
                    is_market_data = any(
                        key.startswith(prefix) for prefix in [
                            "index_daily:", "bond_composite:", "bond_us_rate:",
                            "cb_value_analysis:", "hk_index_daily:",
                        ]
                    )
                    if is_market_data:
                        kept_keys.append(key)
                    else:
                        expired_keys.append(key)
                        results["expired"] += 1
                else:
                    kept_keys.append(key)
                    results["kept"] += 1
            except Exception:
                results["kept"] += 1

        # 报告
        if expired_keys:
            logger.info(f"  🗑️ 发现 {len(expired_keys)} 条过期数据（30天未更新）:")
            # 按前缀分组
            prefix_counts = {}
            for key in expired_keys:
                prefix = key.split(":")[0] if ":" in key else key
                prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1

            for prefix, count in sorted(prefix_counts.items(), key=lambda x: -x[1]):
                logger.info(f"     - {prefix}: {count} 条")

            if not dry_run:
                logger.info(f"  ⚠️ anon key 无 DELETE 权限，请在 Supabase Dashboard 执行清理")
                logger.info(f"  💡 可执行 SQL:")
                logger.info(f"     DELETE FROM data_cache WHERE updated_at < NOW() - INTERVAL '30 days'")
                logger.info(f"       AND cache_key NOT LIKE 'index_daily:%'")
                logger.info(f"       AND cache_key NOT LIKE 'bond_composite:%'")
                logger.info(f"       AND cache_key NOT LIKE 'bond_us_rate:%'")
                logger.info(f"       AND cache_key NOT LIKE 'cb_value_analysis:%'")
                logger.info(f"       AND cache_key NOT LIKE 'hk_index_daily:%';")
        else:
            logger.info("  ✅ 无过期数据")

    except Exception as e:
        logger.warning(f"  ⚠️ 缓存扫描失败: {e}")

    return results


# ============================================================
# 4. 热门基金数据刷新
# ============================================================

def refresh_hot_funds(dry_run: bool = False, top_n: int = 5) -> dict:
    """
    刷新上次排名快照中记录的热门基金净值数据。

    Returns:
        {"success": int, "failed": int}
    """
    from config import SUPABASE_URL, SUPABASE_ANON_KEY
    from supabase import create_client
    from data_loader.base_api import _ak_fund_nav
    from data_loader.cache_layer import cache_set

    results = {"success": 0, "failed": 0}

    try:
        client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    except Exception as e:
        logger.warning(f"⚠️ Supabase 连接失败，跳过热门基金刷新: {e}")
        return results

    logger.info(f"\n🔥 刷新热门基金净值（最近快照前 {top_n} 个类型）...")

    try:
        # 获取最近快照中的基金代码（去重）
        resp = (
            client.table("fund_rank_snapshot")
            .select("fund_code, fund_name")
            .order("snapshot_date", desc=True)
            .limit(500)
            .execute()
        )

        if not resp.data:
            logger.info("  ⚠️ 无排名快照数据，跳过")
            return results

        # 去重
        seen = set()
        funds = []
        for item in resp.data:
            code = item["fund_code"]
            if code not in seen:
                seen.add(code)
                funds.append({"code": code, "name": item.get("fund_name", "")})

        logger.info(f"  📊 共 {len(funds)} 只热门基金")

        for i, f in enumerate(funds, 1):
            for indicator in ["单位净值走势", "累计净值走势"]:
                try:
                    df = _ak_fund_nav(f["code"], indicator=indicator)
                    if df is not None and not df.empty:
                        if not dry_run:
                            cache_set("fund_nav", df, expect_df=True, symbol=f["code"], indicator=indicator)
                        results["success"] += 1
                    else:
                        results["failed"] += 1
                except Exception:
                    results["failed"] += 1

            if i % 20 == 0:
                logger.info(f"  ⏳ 进度: {i}/{len(funds)}")
                time.sleep(3)
            else:
                time.sleep(1)

        logger.info(f"  ✅ 热门基金刷新完成: 成功 {results['success']}，失败 {results['failed']}")

    except Exception as e:
        logger.error(f"  ❌ 热门基金刷新失败: {e}")

    return results


# ============================================================
# 5. 个股指标全量预热（可选）
# ============================================================

def refresh_stock_metrics(dry_run: bool = False) -> dict:
    """
    预热全量 A 股个股指标（PE/PB/PEG/成交额）到 Supabase。

    注意：此步骤耗时约 50 分钟（~5400 只股票），不建议每日自动执行。
    建议每周手动触发一次，或在需要时手动执行。

    Returns:
        {"success": int, "failed": int}
    """
    results = {"success": 0, "failed": 0}

    logger.info("\n📊 预热个股指标（stock_metrics_all）...")

    try:
        from scripts.prewarm_stock_metrics import (
            get_portfolio_a_share_codes,
            fetch_stock_combined,
            save_to_supabase,
        )
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        # 获取股票代码
        codes = get_portfolio_a_share_codes()
        if not codes:
            logger.warning("  ⚠️ 无法获取股票代码列表")
            return results

        total = len(codes)
        logger.info(f"  📊 共 {total} 只股票待拉取")
        logger.info(f"  ⏱️ 预估耗时: {total / 5 / 60:.0f} 分钟（5 线程）")

        if dry_run:
            logger.info("  🔍 [DRY RUN] 跳过实际拉取")
            return {"success": total, "failed": 0}

        # 批量拉取
        all_records = []
        success = 0
        fail = 0
        counter_lock = threading.Lock()
        start_t = time.time()

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_stock_combined, code): code for code in codes}

            for i, future in enumerate(as_completed(futures), 1):
                code = futures[future]
                try:
                    result = future.result(timeout=30)
                    if result is not None:
                        with counter_lock:
                            all_records.append(result)
                            success += 1
                    else:
                        with counter_lock:
                            fail += 1
                except Exception:
                    with counter_lock:
                        fail += 1

                if i % 100 == 0 or i == total:
                    elapsed = time.time() - start_t
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (total - i) / rate / 60 if rate > 0 else 0
                    logger.info(
                        f"  [{i}/{total}] ✅{success} ❌{fail} "
                        f"速率 {rate:.1f}/s ETA {eta:.0f}min"
                    )

        if all_records:
            import pandas as pd
            df = pd.DataFrame(all_records)
            save_to_supabase(df)
            results["success"] = len(df)
            results["failed"] = fail
        else:
            logger.warning("  ⚠️ 未获取到任何数据")
            results["failed"] = total

    except Exception as e:
        logger.error(f"  ❌ 个股指标预热失败: {e}")
        results["failed"] += 1

    return results


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="每日增量更新脚本")
    parser.add_argument("--market", action="store_true", help="只更新市场数据")
    parser.add_argument("--cleanup", action="store_true", help="只做缓存清理诊断")
    parser.add_argument("--stock-metrics", action="store_true", help="额外预热全量个股指标（约50分钟）")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，不实际写入")
    args = parser.parse_args()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("🔄 每日增量更新开始")
    logger.info(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   工作日: {'是' if _is_workday() else '否（周末）'}")
    if args.dry_run:
        logger.info("   模式: DRY RUN（不实际写入）")
    logger.info("=" * 60)

    # 检查 Supabase 连接
    from data_loader.cache_layer import is_ready
    if not is_ready():
        logger.error("❌ Supabase 不可用，请检查配置")
        sys.exit(1)
    logger.info("✅ Supabase 连接正常")

    # ---- 只做清理 ----
    if args.cleanup:
        cleanup_expired_cache(dry_run=args.dry_run)
        elapsed = time.time() - start_time
        logger.info(f"\n✅ 完成！耗时 {elapsed:.0f} 秒")
        return

    # ---- 只更新市场数据 ----
    if args.market:
        market_result = refresh_market_data(dry_run=args.dry_run)
        logger.info(f"\n📊 市场数据: 成功 {market_result['success']}，失败 {market_result['failed']}")
        elapsed = time.time() - start_time
        logger.info(f"\n✅ 完成！耗时 {elapsed:.0f} 秒")
        return

    # ---- 完整更新流程 ----

    # Step 1: 市场数据刷新（每天）
    logger.info("\n" + "=" * 60)
    logger.info("📌 Step 1/4: 刷新市场数据")
    logger.info("=" * 60)
    market_result = refresh_market_data(dry_run=args.dry_run)

    # Step 2: 基金类型索引刷新（仅工作日）
    fund_type_result = {"success": 0, "failed": 0}
    if _is_workday():
        logger.info("\n" + "=" * 60)
        logger.info("📌 Step 2/4: 刷新基金类型索引")
        logger.info("=" * 60)
        fund_type_result = refresh_fund_type_index(dry_run=args.dry_run)
    else:
        logger.info("\n📌 Step 2/4: 跳过基金类型索引刷新（非工作日）")

    # Step 3: 热门基金净值刷新
    logger.info("\n" + "=" * 60)
    logger.info("📌 Step 3/4: 刷新热门基金净值")
    logger.info("=" * 60)
    hot_result = refresh_hot_funds(dry_run=args.dry_run)

    # Step 4: 缓存清理诊断
    logger.info("\n" + "=" * 60)
    logger.info("📌 Step 4/4: 缓存清理诊断")
    logger.info("=" * 60)
    cleanup_result = cleanup_expired_cache(dry_run=args.dry_run)

    # Step 5: [可选] 个股指标全量预热
    stock_metrics_result = {"success": 0, "failed": 0}
    if args.stock_metrics:
        logger.info("\n" + "=" * 60)
        logger.info("📌 Step 5/5: 预热全量个股指标（可选）")
        logger.info("=" * 60)
        stock_metrics_result = refresh_stock_metrics(dry_run=args.dry_run)

    # ---- 总结 ----
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ 增量更新完成！耗时 {elapsed:.0f} 秒 ({elapsed/60:.1f} 分钟)")
    logger.info(f"   市场数据: {market_result['success']} 成功 / {market_result['failed']} 失败")
    logger.info(f"   基金类型: {fund_type_result['success']} 成功 / {fund_type_result['failed']} 失败")
    logger.info(f"   热门基金: {hot_result['success']} 成功 / {hot_result['failed']} 失败")
    logger.info(f"   缓存诊断: {cleanup_result['checked']} 条目, {cleanup_result['expired']} 条过期")
    if args.stock_metrics:
        logger.info(f"   个股指标: {stock_metrics_result['success']} 成功 / {stock_metrics_result['failed']} 失败")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
