"""
权益类数据加载器 — fund_quant_v2
负责：基金基本信息 / 净值历史 / 股票持仓 / FF因子 / 基准收益率
"""

from __future__ import annotations
import re
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from config import (
    DATA_CONFIG, CACHE_TTL, INDEX_MAP, INDEX_NAME_CODE,
    FUND_TYPE_THRESHOLDS, KNOWN_COMPANIES
)
from data_loader.base_api import (
    cached, retry, safe_api_call,
    _ak_fund_basic_xq, _ak_fund_name_em, _ak_fund_scale_sina, _ak_fund_list_em,
    _ak_fund_fee_em, _ak_fund_purchase_status,
    _ak_fund_holdings_stock, _ak_fund_asset_allocation,
    _ak_index_daily_main, _ak_index_daily_em, _ak_hk_index_daily,
    parse_pct,
)
from data_loader.index_sync import get_total_return_series
from models.schema import FundBasicInfo, NavData, HoldingsData, FactorData, BenchmarkData
from processor.data_cleaner import BenchmarkManager
from processor.benchmark_cache import benchmark_cache
from utils.common import audit_logger

logger = logging.getLogger(__name__)


# ============================================================
# 基金基本信息
# ============================================================

@cached(ttl=CACHE_TTL["medium"])
@audit_logger
def load_basic_info(symbol: str) -> FundBasicInfo:
    """
    加载基金基本信息。
    优先级：雪球 XQ → 天天 EM → 新浪 Sina
    """
    r = dict(
        symbol=symbol, name=symbol, type_raw="", type_category="equity",
        establish_date="", scale="", company="", manager="",
        purchase_status="", redeem_status="", min_purchase=0.0, benchmark_text="",
        benchmark_parsed={}, fee_manage=0.0, fee_sale=0.0,
        fee_redeem=0.0, fee_custody=0.0, fee_total=0.0,
    )

    # --- 1. 雪球基本信息 ---
    df_xq = _ak_fund_basic_xq(symbol)
    if df_xq is not None and not df_xq.empty:
        info = dict(zip(df_xq.iloc[:, 0], df_xq.iloc[:, 1]))
        r["name"]           = info.get("基金名称", symbol)
        r["type_raw"]       = info.get("基金类型", "")
        r["establish_date"] = info.get("成立时间", "")
        r["scale"]          = info.get("最新规模", "")
        r["company"]        = info.get("基金公司", "")
        r["manager"]        = info.get("基金经理", "")
        r["benchmark_text"] = info.get("业绩比较基准", "")
        r["fee_manage"]     = parse_pct(info.get("管理费率", ""))
        r["fee_custody"]    = parse_pct(info.get("托管费率", ""))
        r["fee_sale"]       = parse_pct(info.get("销售服务费率", ""))

    # --- 2. 获取申购赎回状态 ---
    purchase_status = _ak_fund_purchase_status(symbol)
    if purchase_status:
        r["purchase_status"] = purchase_status.get('purchase_status', '')
        r["redeem_status"] = purchase_status.get('redeem_status', '')
        r["min_purchase"] = purchase_status.get('min_purchase', 0.0)

    # --- 3. 天天 EM 基金名称列表补全 ---
    if r["name"] == symbol or not r["type_raw"]:
        # 先尝试从基金列表中获取信息
        df_list = _ak_fund_list_em()
        if df_list is not None and not df_list.empty:
            row = df_list[df_list["基金代码"] == symbol]
            if not row.empty:
                row = row.iloc[0]
                if r["name"] == symbol:
                    r["name"] = row.get("基金名称", symbol)
                if not r["type_raw"]:
                    # 从基金列表中无法获取类型信息，保留空值
                    pass
        # 如果仍然没有名称信息，尝试获取单个基金名称
        if r["name"] == symbol:
            fund_name = _ak_fund_name_em(symbol)
            if fund_name:
                r["name"] = fund_name

    # --- 3. 新浪 Sina 规模 / 基金经理 / 成立日期补全 ---
    need_scale   = not r["scale"]
    need_manager = not r["manager"]
    need_est     = not r["establish_date"]
    if need_scale or need_manager or need_est:
        df_sina = _ak_fund_scale_sina()
        if df_sina is not None and not df_sina.empty:
            row_s = df_sina[df_sina["基金代码"].astype(str).str.zfill(6) == str(symbol).zfill(6)]
            if not row_s.empty:
                rs = row_s.iloc[0]
                if need_scale:
                    shares = float(rs.get("最近总份额", 0) or 0)
                    nav_v  = float(rs.get("单位净值", 1) or 1)
                    if shares > 0:
                        yi = shares * nav_v / 1e8
                        r["scale"] = f"{yi:.1f}亿元" if yi >= 1 else f"{yi*100:.1f}百万元"
                if need_manager:
                    mgr = str(rs.get("基金经理", "") or "")
                    if mgr and mgr != "nan":
                        r["manager"] = mgr
                if need_est:
                    est = rs.get("成立日期")
                    if est is not None and str(est) not in ("NaT", "nan", ""):
                        try:
                            r["establish_date"] = pd.to_datetime(est).strftime("%Y-%m-%d")
                        except Exception:
                            pass

    # --- 4. ETF 兜底：从净值历史推断成立日 + 被动标记 ---
    is_etf = "ETF" in r.get("name", "") or "ETF" in r.get("type_raw", "")
    if is_etf and not r["establish_date"]:
        try:
            # ETF 成立日期兜底逻辑（暂不实现，直接跳过）
            pass
        except Exception:
            pass
    if is_etf and not r["manager"]:
        r["manager"] = "被动跟踪（指数型）"

    # --- 5. 从基金名称推断公司 ---
    if not r["company"] and r["name"] != symbol:
        for co in KNOWN_COMPANIES:
            if r["name"].startswith(co):
                r["company"] = co + "基金"
                break

    # --- 6. 天天 EM 费率补全 ---
    # 使用 AkShare 的 fund_fee_em 接口获取费率
    if not r["fee_manage"] or not r["fee_custody"] or not r["fee_sale"]:
        try:
            df_fee = _ak_fund_fee_em(symbol=symbol, indicator="运作费用")
            if df_fee is not None and not df_fee.empty:
                # 转置 DataFrame,第一行为列名,第二行为值
                if len(df_fee.columns) >= 6:
                    # 管理费率（第1列的值）
                    if not r["fee_manage"] and pd.notna(df_fee.iloc[0, 1]):
                        r["fee_manage"] = parse_pct(str(df_fee.iloc[0, 1]))
                    # 托管费率（第3列的值）
                    if not r["fee_custody"] and pd.notna(df_fee.iloc[0, 3]):
                        r["fee_custody"] = parse_pct(str(df_fee.iloc[0, 3]))
                    # 销售服务费率（第5列的值）
                    if not r["fee_sale"] and pd.notna(df_fee.iloc[0, 5]):
                        r["fee_sale"] = parse_pct(str(df_fee.iloc[0, 5]))
        except Exception as e:
            logger.warning(f"[load_basic_info] 获取{symbol}费率失败: {e}")

    # --- 7. 计算衍生字段 - 三级优先级基准调度 ---
    # 一级优先级：解析合同文本
    benchmark_manager = BenchmarkManager()
    parsed_contract = benchmark_manager.parse_contract(r["benchmark_text"])

    # 无论业绩基准是否解析成功，都需要根据基金原始类型设置 type_category
    fund_category = _classify_fund(r)
    r["type_category"] = fund_category

    if parsed_contract["components"]:
        # 一级优先级成功：使用合同解析结果
        r["benchmark_parsed"] = parsed_contract
        logger.info(f"[load_basic_info] {symbol} 使用合同解析的业绩比较基准")
    else:
        # 二级优先级：根据基金分类映射
        default_benchmark = benchmark_manager.get_default_benchmark(fund_category)

        # 转换为标准格式
        components = []
        if default_benchmark.get("equity_code") and default_benchmark["equity_weight"] > 0:
            components.append({
                "name": _get_index_name(default_benchmark["equity_code"]),
                "code": default_benchmark["equity_code"],
                "weight": default_benchmark["equity_weight"]
            })
        if default_benchmark.get("bond_code") and default_benchmark["bond_weight"] > 0:
            components.append({
                "name": _get_index_name(default_benchmark["bond_code"]),
                "code": default_benchmark["bond_code"],
                "weight": default_benchmark["bond_weight"]
            })

        # 判断基准类型
        if not components:
            btype = "unknown"
        elif all("债" in c["name"] for c in components):
            btype = "bond_index"
        elif all("可转债" in c["name"] or "000832" in c["code"] for c in components):
            btype = "cb_index"
        elif all("债" not in c["name"] for c in components):
            btype = "stock_index"
        else:
            btype = "mixed_index"

        r["benchmark_parsed"] = {
            "type": btype,
            "components": components,
            "source": default_benchmark.get("source", "category_mapping"),
            "warnings": parsed_contract.get("warnings", [])
        }
        logger.info(f"[load_basic_info] {symbol} 使用{default_benchmark['source']}基准")

    r["fee_total"] = r["fee_manage"] + r["fee_custody"] + r["fee_sale"]

    # --- 8. 获取最新单位净值 ---
    # 使用AkShare接口获取单位净值，取最新值
    r["latest_unit_nav"] = 0.0
    try:
        from data_loader.base_api import _ak_fund_nav
        df_unit_nav = _ak_fund_nav(symbol, indicator="单位净值走势")
        if df_unit_nav is not None and not df_unit_nav.empty:
            # 取最后一行第二列的值（净值）
            latest_val = df_unit_nav.iloc[-1, 1]
            if pd.notna(latest_val):
                r["latest_unit_nav"] = float(latest_val)
    except Exception as e:
        logger.warning(f"[load_basic_info] 获取{symbol}单位净值失败: {e}")

    return FundBasicInfo(**r)


def _get_index_name(code: str) -> str:
    """根据代码返回指数显示名称"""
    name_map = {
        "000300.SH": "沪深300",
        "000016.SH": "上证50",
        "000905.SH": "中证500",
        "000852.SH": "中证1000",
        "399006.SZ": "创业板指",
        "000688.SH": "科创50",
        "399370.SZ": "国证成长",
        "399371.SZ": "国证价值",
        "000985.SH": "中证全指",
        "881001.SH": "万得全A",
        "000903.SH": "中证100",
        "HSI.HI": "恒生指数",
        "HSCEI.HI": "恒生国企",
        "HSTECH.HI": "恒生科技",
        "H11001.CSI": "中债综合财富",
        "000832.CSI": "中证可转债",
    }
    return name_map.get(code, code)


def _classify_fund(info: dict) -> str:
    """根据 type_raw + 名称判断基金类型标准码"""
    t = info.get("type_raw", "")
    name = info.get("name", "")
    thresholds = FUND_TYPE_THRESHOLDS

    # 不支持类型（优先）
    if any(k in t for k in thresholds["money_keywords"]):
        return "money"
    if "QDII" in t or "QDII" in name:
        return "qdii"
    if any(k in t for k in thresholds["commodity_keywords"]):
        return "commodity"

    # 支持类型 - 返回适合 BenchmarkManager DEFAULT_BENCHMARK_WEIGHTS 的名称
    if any(k in t for k in thresholds["index_keywords"]) or any(k in name for k in ["ETF", "etf"]):
        if "增强" in t:
            return "增强指数"
        elif "标准指数" in t or "被动指数" in t:
            return "标准指数"
        else:
            return "股票型"
    if any(k in t for k in thresholds["sector_keywords"]):
        return "股票型"  # 行业/主题基金使用股票型基准
    if any(k in t for k in thresholds["bond_keywords"]):
        return "中短债" if "中短债" in t else ("长债" if "长债" in t else "纯债型")
    if any(k in t for k in ["混合", "配置", "平衡"]):
        if "偏股" in t:
            return "偏股混合型"
        elif "偏债" in t:
            return "偏债混合型"
        elif "平衡" in t:
            return "平衡混合型"
        else:
            return "混合型"
    if any(k in t for k in ["股票", "权益"]):
        return "股票型"
    if "可转债" in t or "转债" in name:
        return "可转债基金"
    return "股票型"


def _parse_benchmark(text: str) -> dict:
    """解析业绩基准文本 → {type, components:[{name, code, weight}]}"""
    if not text:
        return {"type": "unknown", "components": []}

    found = []
    for name, code in INDEX_NAME_CODE.items():
        if name in text:
            found.append((text.index(name), name, code))
    found.sort(key=lambda x: x[0])

    weights_raw = [float(m) / 100.0 for m in re.findall(r"(\d+)\s*%", text)]
    weights = [w for w in weights_raw if 0 < w <= 1]

    components = []
    if not weights:
        components = [{"name": n, "code": c, "weight": 1.0} for _, n, c in found]
    elif len(found) == len(weights):
        components = [{"name": n, "code": c, "weight": w}
                      for (_, n, c), w in zip(found, weights)]
    elif len(found) == 1 and weights:
        _, n, c = found[0]
        components = [{"name": n, "code": c, "weight": weights[0]}]
    else:
        components = [{"name": n, "code": c, "weight": weights[i] if i < len(weights) else 1.0}
                      for i, (_, n, c) in enumerate(found)]

    total = sum(x["weight"] for x in components)
    if total > 0 and abs(total - 1.0) > 0.05:
        for x in components:
            x["weight"] = round(x["weight"] / total, 4)

    if not components:
        return {"type": "unknown", "components": []}

    all_stock = all("债" not in c["name"] for c in components)
    all_bond  = all("债" in c["name"] for c in components)
    btype = "stock_index" if all_stock else ("bond_index" if all_bond else "mixed_index")
    return {"type": btype, "components": components}


# ============================================================
# 净值历史
# ============================================================

@cached(ttl=CACHE_TTL["short"])
def load_unit_nav(symbol: str) -> Optional[float]:
    """
    获取最新单位净值（用于基础信息显示）。
    """
    @retry()
    def _fetch():
        import akshare as ak
        return ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
    
    df = _fetch()
    if df is None or df.empty:
        logger.warning(f"[load_unit_nav] {symbol} 单位净值数据为空")
        return None
    
    try:
        df = df.iloc[:, :2].copy()
        df.columns = ["date", "nav"]
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        # 取最后一行的净值值
        latest_nav = df["nav"].dropna().iloc[-1]
        return float(latest_nav)
    except Exception as e:
        logger.warning(f"[load_unit_nav] {symbol} 提取单位净值失败: {e}")
        return None


@cached(ttl=CACHE_TTL["short"])
@cached(ttl=CACHE_TTL["medium"])
@audit_logger
def load_nav(
    symbol: str,
    years: int = None,
    since_inception: bool = False,
) -> NavData:
    """
    加载单位净值走势。
    返回 NavData（df 包含 date / nav / ret）。
    
    优化点：
    1. 添加缓存，避免重复加载相同基金的数据
    2. 使用safe_api_call支持超时控制
    3. 优化错误处理和日志记录
    """
    _years = years or DATA_CONFIG["nav_years"]

    def _fetch():
        import akshare as ak
        return ak.fund_open_fund_info_em(symbol=symbol, indicator="单位净值走势")
    
    try:
        # 使用safe_api_call，设置较短的超时时间，减少重试次数
        df = safe_api_call(_fetch, timeout_seconds=10.0, max_retries=1)
    except Exception as e:
        logger.warning(f"[load_nav] {symbol} 净值数据获取失败: {e}")
        # 返回空数据而不是抛出异常，避免影响用户体验
        empty = NavData(symbol=symbol, df=pd.DataFrame(columns=["date", "nav", "ret"]))
        return empty
    
    empty = NavData(symbol=symbol, df=pd.DataFrame(columns=["date", "nav", "ret"]))

    if df is None or df.empty:
        logger.warning(f"[load_nav] {symbol} 净值数据为空")
        return empty

    df = df.iloc[:, :2].copy()
    df.columns = ["date", "nav"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["nav"]  = pd.to_numeric(df["nav"], errors="coerce")
    df = df.dropna().sort_values("date").reset_index(drop=True)
    df = df[df["nav"] > 0]

    if df.empty:
        return empty

    # 时间范围过滤
    if since_inception:
        pass
    else:
        df = df[df["date"] >= datetime.now() - timedelta(days=_years * 365)]

    if df.empty:
        return empty

    df["ret"] = df["nav"].pct_change().fillna(0)
    return NavData(symbol=symbol, df=df.reset_index(drop=True))


# ============================================================
# 股票持仓
# ============================================================

@cached(ttl=CACHE_TTL["short"])
def load_stock_holdings(symbol: str) -> HoldingsData:
    """
    加载股票/资产配置持仓数据。
    按年份依次尝试 2024/2023，优先使用 asset_allocation 接口。
    """
    r = dict(
        symbol=symbol,
        stock_ratio=0.0, bond_ratio=0.0, cash_ratio=0.0, cb_ratio=0.0,
        top10_stocks=[], bond_details=[], asset_allocation={},
    )

    # --- 资产配置接口（最准确）---
    df_asset = _ak_fund_asset_allocation(symbol, date="2024")
    if df_asset is not None and not df_asset.empty:
        if "资产类别" in df_asset.columns:
            for _, row in df_asset.iterrows():
                asset = str(row.get("资产类别", ""))
                try:
                    ratio = float(row.get("占净值比例(%)", 0) or 0) / 100
                except Exception:
                    ratio = 0.0
                if "股票" in asset:
                    r["stock_ratio"] = ratio
                elif "债券" in asset:
                    r["bond_ratio"] = ratio
                elif "现金" in asset or "银行存款" in asset:
                    r["cash_ratio"] = ratio

    # --- 股票前十大持仓 ---
    for year in ["2024", "2023", "2022"]:
        df_top10 = _ak_fund_holdings_stock(symbol, year)
        if df_top10 is not None and not df_top10.empty and "占净值比例" in df_top10.columns:
            r["top10_stocks"] = df_top10.head(10).to_dict("records")
            # 若资产配置接口未返回股票仓位，用 top10 之和估算
            if r["stock_ratio"] == 0.0:
                total = df_top10["占净值比例"].sum()
                r["stock_ratio"] = min(total / 100, 1.0)
            break

    # --- 默认值（所有接口均失败时）---
    if r["stock_ratio"] == 0.0 and r["bond_ratio"] == 0.0:
        logger.warning(f"[load_stock_holdings] {symbol} 持仓数据全部失败，使用经验默认值")
        r["stock_ratio"] = 0.85
        r["bond_ratio"]  = 0.10
        r["cash_ratio"]  = 0.05

    return HoldingsData(**r)


# ============================================================
# FF 因子
# ============================================================

@cached(ttl=CACHE_TTL["long"])
def load_index_daily(symbol_code: str, start: str, end: str) -> pd.DataFrame:
    """
    加载 A股指数日行情（主力 + 备用双接口），返回 date / ret。
    """
    def _build(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "ret"])
        if "date" not in df.columns or "close" not in df.columns:
            return pd.DataFrame(columns=["date", "ret"])
        df = df[["date", "close"]].copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]
        df["ret"] = df["close"].pct_change().fillna(0)
        return df[["date", "ret"]].reset_index(drop=True)

    raw = _ak_index_daily_main(symbol_code)
    result = _build(raw)
    if not result.empty:
        return result

    raw = _ak_index_daily_em(symbol_code)
    return _build(raw)


@cached(ttl=CACHE_TTL["long"])
def load_hk_index_daily(sina_symbol: str, start: str, end: str) -> pd.DataFrame:
    """港股指数日行情，返回 date / ret"""
    raw = _ak_hk_index_daily(sina_symbol)
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "ret"])
    if "date" not in raw.columns or "close" not in raw.columns:
        return pd.DataFrame(columns=["date", "ret"])
    df = raw[["date", "close"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]
    df["ret"] = df["close"].pct_change().fillna(0)
    return df[["date", "ret"]].reset_index(drop=True)


@cached(ttl=CACHE_TTL["long"])
def load_ff_factors(start: str, end: str) -> FactorData:
    """
    构建 FF 因子代理序列。
    列：date / Mkt / SMB / HML / Short_MOM [/ RMW]
    SMB = 中证1000 - 沪深300
    HML = 国证价值 - 国证成长
    RMW = 300质量成长 - 沪深300（失败时降维）
    """
    mkt   = load_index_daily(INDEX_MAP["mkt"][0], start, end).rename(columns={"ret": "Mkt"})
    small = load_index_daily(INDEX_MAP["small"][0], start, end).rename(columns={"ret": "ret_small"})
    val   = load_index_daily(INDEX_MAP["value"][0], start, end).rename(columns={"ret": "ret_val"})
    grw   = load_index_daily(INDEX_MAP["growth"][0], start, end).rename(columns={"ret": "ret_grw"})

    df = mkt.copy()
    df = df.merge(small, on="date", how="left")
    df = df.merge(val,   on="date", how="left")
    df = df.merge(grw,   on="date", how="left")

    # 前向填充（防停牌截断）
    for col in ["ret_small", "ret_val", "ret_grw"]:
        if col in df.columns:
            df[col] = df[col].ffill(limit=DATA_CONFIG["ffill_limit"])

    df["SMB"] = df["ret_small"] - df["Mkt"]
    df["HML"] = df["ret_val"]   - df["ret_grw"]

    # Carhart 短期动量（21日滚动均值滞后1日）
    df["Short_MOM"] = df["Mkt"].rolling(21, min_periods=1).mean().shift(1)

    df.drop(columns=["ret_small", "ret_val", "ret_grw"], inplace=True, errors="ignore")

    # RMW（可选）
    try:
        qual = load_index_daily(INDEX_MAP["quality"][0], start, end).rename(columns={"ret": "ret_qual"})
        df = df.merge(qual[["date", "ret_qual"]], on="date", how="left")
        df["ret_qual"] = df["ret_qual"].ffill(limit=DATA_CONFIG["ffill_limit"])
        nan_ratio = df["ret_qual"].isna().mean()
        if nan_ratio < 0.50:
            df["RMW"] = df["ret_qual"] - df["Mkt"]
        df.drop(columns=["ret_qual"], inplace=True, errors="ignore")
    except Exception:
        pass

    df = df.dropna(subset=["Mkt", "SMB", "HML"]).reset_index(drop=True)
    return FactorData(df=df)


# ============================================================
# 基准收益率
# ============================================================

@cached(ttl=CACHE_TTL["long"])
def load_bond_index(start: str, end: str) -> pd.DataFrame:
    """中债综合财富指数，返回 date / ret"""
    import akshare as ak
    try:
        df = ak.bond_new_composite_index_cbond(indicator="财富")
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "ret"])
        # 兼容 '日期' 列名
        if "日期" in df.columns:
            df = df.rename(columns={"日期": "date"})
        elif "date" not in df.columns:
            df = df.rename(columns={df.columns[0]: "date"})
        # 价格列
        val_col = "value" if "value" in df.columns else ("指数" if "指数" in df.columns else df.columns[-1])
        df = df[["date", val_col]].copy()
        df.columns = ["date", "close"]
        df["date"] = pd.to_datetime(df["date"])
        df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]
        df = df.sort_values("date")
        df["ret"] = df["close"].pct_change().fillna(0)
        return df[["date", "ret"]].reset_index(drop=True)
    except Exception as e:
        logger.warning(f"[load_bond_index] 中债指数获取失败: {e}")
        return pd.DataFrame(columns=["date", "ret"])


def build_benchmark(basic: FundBasicInfo, start: str, end: str) -> BenchmarkData:
    """
    根据业绩基准解析结果构建加权基准收益率序列。
    支持：A股指数 / 港股指数（hk:前缀）/ 中债指数
    注意：现在优先使用全收益指数（包含分红再投资收益）
    
    优化：使用 benchmark_cache 避免重复加载
    """
    parsed = basic.benchmark_parsed
    if not parsed or not parsed.get("components"):
        # 使用沪深300全收益指数（默认）
        # P1-优化：检查缓存
        cache_key = f"sh000300_{start}_{end}"
        cached_result = benchmark_cache.get("sh000300", start, end)

        if cached_result is not None:
            cached_df, cached_desc = cached_result
            if not cached_df.empty:
                logger.info(f"[build_benchmark] 从缓存获取沪深300全收益指数（默认）")
                return BenchmarkData(df=cached_df[["date", "bm_ret"]], description="沪深300全收益（默认）")
        
        try:
            df = get_total_return_series("sh000300", start, end)
            if not df.empty:
                # 重命名tr_ret列为bm_ret
                df = df.rename(columns={"tr_ret": "bm_ret"})
                df["bm_ret"] = df["bm_ret"].fillna(0)
                # P1-优化：存入缓存
                benchmark_cache.set("sh000300", start, end, df, "沪深300全收益（默认）")
                logger.info("[build_benchmark] 使用沪深300全收益指数（默认）")
                return BenchmarkData(df=df[["date", "bm_ret"]], description="沪深300全收益（默认）")
        except Exception as e:
            logger.warning(f"[build_benchmark] 获取沪深300全收益指数失败，回退到价格指数: {e}")
            # 回退到价格指数
            df = load_index_daily("sh000300", start, end).rename(columns={"ret": "bm_ret"})
            df["bm_ret"] = df["bm_ret"].fillna(0)
            return BenchmarkData(df=df, description="沪深300价格（默认）")

    parts = []
    desc_parts = []
    for comp in parsed["components"]:
        w    = comp["weight"]
        code = comp["code"]
        name = comp["name"]

        if code is None and "中债" in name:
            # 债券指数使用价格指数（财富指数版本本身是全收益的）
            df_part = load_bond_index(start, end).rename(columns={"ret": "part_ret"})
            logger.info(f"[build_benchmark] 使用债券指数: {name}")
        elif code is not None and code.startswith("hk:"):
            # 港股指数不支持全收益计算，使用价格指数
            # 转换格式: HSI.HI → HSI（新浪格式）
            hk_sym = code[3:]
            if '.' in hk_sym:
                hk_sym = hk_sym.split('.')[0]
            df_part = load_hk_index_daily(hk_sym, start, end).rename(columns={"ret": "part_ret"})
            logger.info(f"[build_benchmark] 使用港股指数: {hk_sym}")
        elif code is None:
            # 未知基准组件
            df_part = pd.DataFrame({
                "date": pd.date_range(start, end, freq="B"),
                "part_ret": 0.0
            })
            logger.warning(f"[build_benchmark] 未知基准组件: {name}")
        elif code is not None and ("H11001" in code or ".CSI" in code):
            # 中债综合指数特殊处理，使用债券指数接口
            df_part = load_bond_index(start, end).rename(columns={"ret": "part_ret"})
            logger.info(f"[build_benchmark] 使用债券指数接口: {code} ({name})")
        else:
            # A股指数：优先使用全收益指数
            # P1-优化：检查缓存
            cached_result = benchmark_cache.get(code, start, end)

            if cached_result is not None:
                cached_df, cached_desc = cached_result
                if not cached_df.empty:
                    df_part = cached_df.rename(columns={"bm_ret": "part_ret"})
                    logger.info(f"[build_benchmark] 从缓存获取全收益指数: {code} ({name})")
            else:
                try:
                    df_part = get_total_return_series(code, start, end)
                    if not df_part.empty:
                        # 重命名tr_ret列为part_ret
                        df_part = df_part.rename(columns={"tr_ret": "part_ret"})
                        # P1-优化：存入缓存
                        benchmark_cache.set(code, start, end, df_part, f"{code}全收益")
                        logger.info(f"[build_benchmark] 使用全收益指数: {code} ({name})")
                    else:
                        raise ValueError("全收益数据为空")
                except Exception as e:
                    # 回退到价格指数
                    logger.warning(f"[build_benchmark] 获取{code}全收益指数失败，使用价格指数: {e}")
                    df_part = load_index_daily(code, start, end).rename(columns={"ret": "part_ret"})

        df_part = df_part[df_part["part_ret"].notna()].copy()
        df_part["weighted"] = df_part["part_ret"] * w
        parts.append(df_part[["date", "weighted"]])
        desc_parts.append(f"{name}×{int(w*100)}%")

    if not parts:
        # 回退到默认基准
        try:
            df = get_total_return_series("sh000300", start, end)
            if not df.empty:
                df = df.rename(columns={"tr_ret": "bm_ret"})
                df["bm_ret"] = df["bm_ret"].fillna(0)
                logger.info("[build_benchmark] 使用沪深300全收益指数（回退）")
                return BenchmarkData(df=df[["date", "bm_ret"]], description="沪深300全收益（回退）")
        except Exception as e:
            logger.warning(f"[build_benchmark] 获取沪深300全收益指数失败，回退到价格指数: {e}")
            df = load_index_daily("sh000300", start, end).rename(columns={"ret": "bm_ret"})
            df["bm_ret"] = df["bm_ret"].fillna(0)
            return BenchmarkData(df=df, description="沪深300价格（回退）")

    merged = parts[0].rename(columns={"weighted": "bm_ret"})
    for p in parts[1:]:
        merged = merged.merge(p, on="date", how="inner")
        merged["bm_ret"] = merged["bm_ret"] + merged["weighted"]
        merged.drop(columns=["weighted"], inplace=True)

    result = merged[["date", "bm_ret"]].dropna().reset_index(drop=True)
    result["bm_ret"] = result["bm_ret"].fillna(0)
    
    # 标记是否包含全收益指数
    desc_prefix = "全收益: " if "全收益" in " ".join(desc_parts) else ""
    description = desc_prefix + " + ".join(desc_parts)
    
    logger.info(f"[build_benchmark] 构建完成: {description}")
    return BenchmarkData(df=result, description=description)


# ============================================================
# 快速基金代码校验
# ============================================================

@cached(ttl=CACHE_TTL["long"])  # 长期缓存，基金代码很少变化
def validate_fund_code_fast(symbol: str) -> bool:
    """
    快速校验基金代码是否存在。
    使用本地基金目录优先，如果本地不存在则回退到API查询。
    
    注意：此函数用于需要严格验证的场景，可能会调用API。
    对于大多数用户交互场景，请使用validation_bypass模块中的函数。
    
    返回：
        True - 基金代码有效
        False - 基金代码无效
    """
    if not symbol or len(symbol) != 6:
        logger.debug(f"[validate_fund_code_fast] 基金代码格式无效: {symbol}")
        return False
    
    try:
        # 尝试从本地目录获取（最快）
        try:
            from fund_directory import validate_fund_code_local
            is_valid_local = validate_fund_code_local(symbol)
            if is_valid_local:
                logger.debug(f"[validate_fund_code_fast] 本地目录验证通过: {symbol}")
                return True
            logger.debug(f"[validate_fund_code_fast] 本地目录未找到: {symbol}")
        except ImportError:
            logger.debug("[validate_fund_code_fast] 无法导入本地目录模块，降级到API验证")
        except Exception as e:
            logger.debug(f"[validate_fund_code_fast] 本地目录验证异常: {e}")
        
        # 本地目录不存在或验证失败，回退到API验证
        # 使用天天基金网接口获取基金名称列表（带缓存）
        df_names = _ak_fund_list_em()
        if df_names is None or df_names.empty:
            logger.warning("[validate_fund_code_fast] 基金名称列表为空")
            return False
        
        # 检查基金代码是否存在
        code_exists = symbol in df_names["基金代码"].astype(str).values
        if code_exists:
            logger.debug(f"[validate_fund_code_fast] API验证通过: 基金代码 {symbol}")
        else:
            logger.warning(f"[validate_fund_code_fast] API验证失败: 基金代码 {symbol} 不存在于基金列表中")
        
        return code_exists
    except Exception as e:
        logger.warning(f"[validate_fund_code_fast] 基金代码校验异常: {e}")
        # 发生异常时返回 False，避免阻塞用户
        return False


def validate_fund_code_quick(symbol: str) -> bool:
    """
    快速基金代码验证（极简版本）
    
    方案2实现：仅进行最基本的格式检查，不进行API验证
    用于解决用户输入后校验时间过长的问题
    
    返回：
        True - 代码格式正确（但不保证真实存在）
        False - 代码格式明显错误
    """
    # 极简验证：只检查是否是6位数字
    if not symbol or len(symbol) != 6:
        logger.debug(f"[validate_fund_code_quick] 基金代码格式无效: {symbol}")
        return False
    
    # 检查是否全是数字
    if not symbol.isdigit():
        logger.debug(f"[validate_fund_code_quick] 基金代码包含非数字字符: {symbol}")
        return False
    
    # 格式检查通过
    logger.debug(f"[validate_fund_code_quick] 基金代码格式正确: {symbol}")
    return True


def get_validation_info(symbol: str) -> dict:
    """
    获取基金代码验证信息（推荐使用）
    
    返回详细的验证信息，供前端显示和决策
    
    返回：
        {
            'valid': True/False,
            'method': 'quick'/'local'/'api'/'none',
            'message': '验证消息',
            'warning': '警告信息（如有）',
            'recommended_action': '建议操作'
        }
    """
    from validation_bypass import validate_fund_code as validate_fund_code_strategic
    
    try:
        # 使用策略性验证
        result = validate_fund_code_strategic(symbol, strict=False)
        
        # 添加建议操作
        if not result['valid']:
            result['recommended_action'] = '请检查基金代码格式'
        elif result['method'] == 'quick':
            result['recommended_action'] = '将继续尝试加载数据'
        elif result['method'] == 'local':
            result['recommended_action'] = '本地验证通过，继续加载'
        elif result['method'] == 'api':
            result['recommended_action'] = 'API验证通过，继续加载'
        else:
            result['recommended_action'] = '继续处理'
        
        return result
        
    except Exception as e:
        logger.error(f"[get_validation_info] 验证信息获取失败: {e}")
        
        # 降级：使用快速验证
        is_valid_quick = validate_fund_code_quick(symbol)
        
        return {
            'valid': is_valid_quick,
            'method': 'quick' if is_valid_quick else 'none',
            'message': '快速验证' + ('通过' if is_valid_quick else '失败'),
            'warning': '验证系统异常，已降级到快速验证',
            'recommended_action': '将继续尝试处理' if is_valid_quick else '请检查输入'
        }
