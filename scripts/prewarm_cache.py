"""
缓存预热脚本 — fund_quant_v2
从东方财富基金排名中获取 Top20 基金，预热其基础数据到 Supabase。

预热策略：
1. 获取各类型业绩排名前 20 的基金
2. 对每只基金预热：基金类型(fund_type)、净值(fund_nav)、基本信息
3. 预热全局共享数据：指数日线、国债收益率、中债综合指数、转债估值

运行方式：
    python -m scripts.prewarm_cache
    python -m scripts.prewarm_cache --top 20 --types 股票型,混合型
    python -m scripts.prewarm_cache --market-only   # 只预热市场数据，跳过基金排名
"""

from __future__ import annotations

import argparse
import logging
import time
import sys
from datetime import datetime

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# 参数
# ============================================================
DEFAULT_TOP_N = 20
DEFAULT_TYPES = [
    "股票型", "混合型-偏股", "混合型-灵活配置",
    "混合型-偏债", "债券型-长债", "债券型-中短债",
    "指数型-股票",
]


# ============================================================
# 1. 基金排名获取
# ============================================================

def fetch_top_funds(fund_type: str, top_n: int = 20) -> list[dict]:
    """
    从东方财富获取某类型基金业绩排名前 N 的基金代码。

    Args:
        fund_type: 基金类型（如 "股票型"）
        top_n: 取前 N 名

    Returns:
        [{"code": "000001", "name": "华夏成长"}, ...]
    """
    import akshare as ak

    # 尝试多种排名周期：近一年为首选
    for indicator in ["近一年", "近三年"]:
        try:
            logger.info(f"  获取 {fund_type} 排名（{indicator}）...")
            df = ak.fund_open_fund_rank_em(
                symbol=fund_type,
                indicator=indicator,
            )
            if df is not None and not df.empty:
                # 确保有基金代码列
                code_col = None
                for c in df.columns:
                    if "基金代码" in str(c):
                        code_col = c
                        break
                name_col = None
                for c in df.columns:
                    if "基金名称" in str(c):
                        name_col = c
                        break

                if code_col is None:
                    logger.warning(f"  ⚠️ {fund_type} 排名数据缺少基金代码列，列名: {list(df.columns)}")
                    continue

                results = []
                for _, row in df.head(top_n).iterrows():
                    code = str(row[code_col]).strip()
                    name = str(row[name_col]).strip() if name_col else ""
                    # 确保是6位代码
                    if code.isdigit() and len(code) == 6:
                        results.append({"code": code, "name": name})

                if results:
                    logger.info(f"  ✅ {fund_type}({indicator}): 获取到 {len(results)} 只基金")
                    return results
                else:
                    logger.warning(f"  ⚠️ {fund_type}({indicator}): 无有效基金代码")

        except Exception as e:
            logger.warning(f"  ⚠️ {fund_type}({indicator}) 获取失败: {e}")
            continue

    logger.warning(f"  ❌ {fund_type}: 所有排名周期均失败")
    return []


def get_all_top_funds(types: list[str], top_n: int = 20) -> list[dict]:
    """
    获取所有类型的前 N 基金，去重。

    Returns:
        [{"code": "000001", "name": "华夏成长", "types": ["股票型"]}, ...]
    """
    all_funds = {}
    for ft in types:
        funds = fetch_top_funds(ft, top_n)
        for f in funds:
            code = f["code"]
            if code not in all_funds:
                all_funds[code] = {"code": code, "name": f["name"], "types": []}
            all_funds[code]["types"].append(ft)

    result = list(all_funds.values())
    logger.info(f"\n📊 共获取 {len(result)} 只不重复基金（来自 {len(types)} 个类型）")
    return result


# ============================================================
# 2. 基金级数据预热
# ============================================================

def prewarm_fund_data(code: str, name: str) -> bool:
    """
    预热单只基金的基础数据。

    预热内容：
    - fund_type（7天缓存）
    - fund_nav 单位净值走势（5分钟缓存）
    - fund_nav 累计净值走势（5分钟缓存）
    """
    from data_loader.base_api import get_fund_type_em, _ak_fund_nav
    from data_loader.cache_layer import cache_set

    success = True

    # 1) 基金类型
    try:
        ft = get_fund_type_em(code)
        if ft:
            logger.info(f"    ✅ {code} {name}: 类型={ft}")
        else:
            logger.warning(f"    ⚠️ {code} {name}: 类型获取失败")
            success = False
    except Exception as e:
        logger.warning(f"    ⚠️ {code}: 类型预热异常: {e}")
        success = False

    # 2) 净值数据（单位净值 + 累计净值）
    for indicator in ["单位净值走势", "累计净值走势"]:
        try:
            df = _ak_fund_nav(code, indicator=indicator)
            if df is not None and not df.empty:
                cache_set("fund_nav", df, expect_df=True, symbol=code, indicator=indicator)
                logger.info(f"    ✅ {code}: {indicator} ({len(df)} 行)")
            else:
                logger.warning(f"    ⚠️ {code}: {indicator} 无数据")
                success = False
        except Exception as e:
            logger.warning(f"    ⚠️ {code}: {indicator} 预热异常: {e}")
            success = False

    return success


# ============================================================
# 3. 市场级数据预热
# ============================================================

def prewarm_market_data() -> dict:
    """
    预热全局共享的市场数据。

    预热内容：
    - 主要指数日线（沪深300、中证500、中证1000、上证50、创业板指等）
    - 国债收益率
    - 中债综合指数
    - 全市场可转债估值
    - 恒生指数

    Returns:
        {"success": int, "failed": int}
    """
    from data_loader.base_api import (
        _ak_index_daily_main,
        _ak_bond_us_rate,
        _ak_bond_composite_index,
        load_cb_value_analysis,
        _ak_hk_index_daily,
        _ak_bond_china_yield,
        _ak_cb_info,
    )
    from data_loader.cache_layer import cache_set

    results = {"success": 0, "failed": 0}
    today = datetime.now().strftime("%Y%m%d")

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

    logger.info("\n📈 预热指数日线...")
    for symbol, name in indices.items():
        try:
            df = _ak_index_daily_main(symbol)
            if df is not None and not df.empty:
                cache_set("index_daily", df, expect_df=True, symbol=symbol)
                logger.info(f"  ✅ {name} ({symbol}): {len(df)} 行")
                results["success"] += 1
            else:
                logger.warning(f"  ⚠️ {name}: 无数据")
                results["failed"] += 1
            time.sleep(0.5)  # 礼貌延迟
        except Exception as e:
            logger.warning(f"  ❌ {name} ({symbol}): {e}")
            results["failed"] += 1

    # --- 港股指数 ---
    hk_indices = {
        "HSI": "恒生指数",
        "HSCEI": "恒生国企",
        "HSTECH": "恒生科技",
    }
    logger.info("\n📈 预热港股指数...")
    for symbol, name in hk_indices.items():
        try:
            df = _ak_hk_index_daily(symbol)
            if df is not None and not df.empty:
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
    logger.info("\n📈 预热国债收益率...")
    try:
        # 取最近 2 年数据
        start_date = str(int(today[:4]) - 2) + today[4:]
        df = _ak_bond_us_rate(start_date=start_date)
        if df is not None and not df.empty:
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
    logger.info("\n📈 预热中债综合指数...")
    for indicator in ["财富", "总值"]:
        try:
            df = _ak_bond_composite_index(indicator=indicator)
            if df is not None and not df.empty:
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
    logger.info("\n📈 预热转债估值...")
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

    # --- 中证转债指数历史 ---
    logger.info("\n📈 预热中证转债指数...")
    try:
        import akshare as ak
        start_date = str(int(today[:4]) - 3) + today[4:]
        df = ak.index_zh_a_hist(symbol="中证转债", period="daily",
                                 start_date=start_date, end_date=today)
        if df is not None and not df.empty:
            cache_set("cb_index_hist", df, expect_df=True, symbol="中证转债",
                      start_date=start_date, end_date=today)
            logger.info(f"  ✅ 中证转债指数: {len(df)} 行")
            results["success"] += 1
        else:
            logger.warning("  ⚠️ 中证转债指数: 无数据")
            results["failed"] += 1
    except Exception as e:
        logger.warning(f"  ❌ 中证转债指数: {e}")
        results["failed"] += 1

    # --- 中债收益率曲线（AAA/国债） ---
    logger.info("\n📈 预热中债收益率曲线...")
    try:
        start_date = str(int(today[:4]) - 3) + today[4:]
        df = _ak_bond_china_yield(start_date=start_date, end_date=today)
        if df is not None and not df.empty:
            logger.info(f"  ✅ 中债收益率曲线: {len(df)} 行")
            results["success"] += 1
        else:
            logger.warning("  ⚠️ 中债收益率曲线: 无数据")
            results["failed"] += 1
    except Exception as e:
        logger.warning(f"  ❌ 中债收益率曲线: {e}")
        results["failed"] += 1

    # --- 指数估值（PE/PB） ---
    logger.info("\n📈 预热指数估值...")
    from data_loader.index_stock_loader import load_index_valuation
    valuation_indices = {
        "000300": "沪深300",
        "000905": "中证500",
        "000852": "中证1000",
    }
    for code, name in valuation_indices.items():
        try:
            df = load_index_valuation(code)
            if df is not None and not df.empty:
                logger.info(f"  ✅ {name} PE/PB: {len(df)} 行")
                results["success"] += 1
            else:
                logger.warning(f"  ⚠️ {name} PE/PB: 无数据")
                results["failed"] += 1
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"  ❌ {name} PE/PB: {e}")
            results["failed"] += 1

    # --- 成份股权重（沪深300前10） ---
    logger.info("\n📈 预热成份股权重...")
    from data_loader.index_stock_loader import load_index_cons_weights
    try:
        df = load_index_cons_weights("000300")
        if df is not None and not df.empty:
            logger.info(f"  ✅ 沪深300成份股: {len(df)} 行")
            results["success"] += 1
        else:
            logger.warning("  ⚠️ 沪深300成份股: 无数据")
            results["failed"] += 1
    except Exception as e:
        logger.warning(f"  ❌ 沪深300成份股: {e}")
        results["failed"] += 1

    return results


# ============================================================
# 4. 排名快照保存到 Supabase
# ============================================================

def save_rank_snapshot(funds: list[dict]) -> int:
    """
    将基金排名快照保存到 Supabase fund_rank_snapshot 表。

    Returns:
        成功写入的记录数
    """
    from config import SUPABASE_URL, SUPABASE_ANON_KEY
    from supabase import create_client

    try:
        client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    except Exception as e:
        logger.warning(f"⚠️ Supabase 连接失败，跳过排名快照保存: {e}")
        return 0

    saved = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for f in funds:
        for ft in f["types"]:
            try:
                client.table("fund_rank_snapshot").upsert(
                    {
                        "fund_code": f["code"],
                        "fund_name": f["name"],
                        "fund_type": ft,
                        "rank_period": "近一年",
                        "rank_value": None,  # 排名值暂不记录
                        "snapshot_date": today,
                    },
                    on_conflict="fund_code,fund_type,rank_period,snapshot_date",
                ).execute()
                saved += 1
            except Exception as e:
                logger.warning(f"  ⚠️ 排名快照写入失败 {f['code']}/{ft}: {e}")

    if saved > 0:
        logger.info(f"📋 排名快照已保存 {saved} 条记录（{today}）")
    return saved


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="缓存预热脚本")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, help=f"每个类型取前 N 名（默认 {DEFAULT_TOP_N}）")
    parser.add_argument("--types", type=str, default=None, help="基金类型，逗号分隔（默认使用预设列表）")
    parser.add_argument("--market-only", action="store_true", help="只预热市场数据，跳过基金排名")
    parser.add_argument("--fund-only", action="store_true", help="只预热基金数据，跳过市场数据")
    args = parser.parse_args()

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("🚀 缓存预热开始")
    logger.info(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 检查 Supabase 连接
    from data_loader.cache_layer import is_ready
    if not is_ready():
        logger.error("❌ Supabase 不可用，请检查配置")
        sys.exit(1)
    logger.info("✅ Supabase 连接正常")

    # ---- 市场级数据预热 ----
    if not args.fund_only:
        logger.info("\n" + "=" * 60)
        logger.info("📌 第一步：预热市场级数据")
        logger.info("=" * 60)
        market_result = prewarm_market_data()
        logger.info(f"\n📊 市场数据预热完成：成功 {market_result['success']}，失败 {market_result['failed']}")

    # ---- 基金排名获取 + 数据预热 ----
    if not args.market_only:
        types = args.types.split(",") if args.types else DEFAULT_TYPES

        logger.info("\n" + "=" * 60)
        logger.info("📌 第二步：获取基金排名并预热")
        logger.info("=" * 60)
        funds = get_all_top_funds(types, args.top)

        # 保存排名快照
        save_rank_snapshot(funds)

        # 逐个预热
        logger.info(f"\n🔥 开始预热 {len(funds)} 只基金的数据...")
        ok_count = 0
        fail_count = 0

        for i, f in enumerate(funds, 1):
            logger.info(f"\n  [{i}/{len(funds)}] {f['code']} {f['name']} ({', '.join(f['types'])})")
            try:
                if prewarm_fund_data(f["code"], f["name"]):
                    ok_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                logger.error(f"  ❌ {f['code']}: 预热异常: {e}")
                fail_count += 1

            # 礼貌延迟，避免触发限流
            if i % 10 == 0:
                logger.info(f"  ⏳ 已处理 {i}/{len(funds)}，休息 3 秒...")
                time.sleep(3)
            else:
                time.sleep(1)

        logger.info(f"\n📊 基金数据预热完成：成功 {ok_count}，失败 {fail_count}")

    # ---- 全局缓存预热（fund_list / fund_purchase） ----
    logger.info("\n" + "=" * 60)
    logger.info("📌 第三步：预热全局共享数据")
    logger.info("=" * 60)

    import akshare as ak
    from data_loader.cache_layer import cache_set, cache_set_large

    # 全量基金列表（大表 → Storage）
    logger.info("\n📋 预热全量基金列表...")
    try:
        df_list = ak.fund_name_em()
        if df_list is not None and not df_list.empty:
            cache_set_large("fund_list_all", df_list)
            logger.info(f"  ✅ 基金列表: {len(df_list)} 条")
        else:
            logger.warning("  ⚠️ 基金列表: 无数据")
    except Exception as e:
        logger.warning(f"  ❌ 基金列表: {e}")

    # 申购赎回状态（大表 → Storage）
    logger.info("\n📋 预热申购赎回状态...")
    try:
        df_purchase = ak.fund_purchase_em()
        if df_purchase is not None and not df_purchase.empty:
            cache_set_large("fund_purchase_all", df_purchase)
            logger.info(f"  ✅ 申购状态: {len(df_purchase)} 条")
        else:
            logger.warning("  ⚠️ 申购状态: 无数据")
    except Exception as e:
        logger.warning(f"  ❌ 申购状态: {e}")

    # 基金费率表（典型基金）
    logger.info("\n📋 预热基金费率...")
    from data_loader.base_api import _ak_fund_fee_em
    fee_funds = ["000001", "000011", "110011", "161725", "005827"]
    for fc in fee_funds:
        try:
            df_fee = _ak_fund_fee_em(fc)
            if df_fee is not None:
                logger.info(f"  ✅ 费率 {fc}: 已缓存")
            time.sleep(0.3)
        except Exception as e:
            logger.warning(f"  ⚠️ 费率 {fc}: {e}")

    # ---- 总结 ----
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ 预热完成！总耗时 {elapsed:.0f} 秒 ({elapsed/60:.1f} 分钟)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
