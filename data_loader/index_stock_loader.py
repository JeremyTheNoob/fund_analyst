"""
指数型-股票基金 专用数据加载器 — fund_quant_v2
负责：PE/PB 估值 / 成份股权重 / 费率解析 / 增强型识别
"""

from __future__ import annotations
import logging
from typing import Optional, Tuple, Dict, Any, List

import akshare as ak
import pandas as pd
import numpy as np

from config import CACHE_TTL
from data_loader.base_api import cached, safe_api_call
from data_loader.akshare_timeout import call_with_timeout

logger = logging.getLogger(__name__)


# ============================================================
# 指数代码标准化
# ============================================================

def normalize_index_code(code: str) -> str:
    """
    将各种格式的指数代码标准化为6位纯数字（中证指数接口格式）。
    
    Examples:
        "sh000300" → "000300"
        "sz399006" → "399006"
        "000300"   → "000300"
        "sh000905" → "000905"
        "000300.SH" → "000300"
        "000300.SS" → "000300"
    """
    code = str(code).strip()
    # 移除后缀 (.SH, .SS, .SZ 等)
    for suffix in (".SH", ".SS", ".SZ", ".sh", ".ss", ".sz"):
        if code.upper().endswith(suffix):
            code = code[: -len(suffix)]
    # 移除前缀
    for prefix in ("sh", "sz", "SH", "SZ"):
        if code.startswith(prefix):
            code = code[2:]
    return code


def index_code_to_short(code: str) -> str:
    """
    从指数代码推断短代码（用于中证/国证接口）。
    
    Examples:
        "sh000300" → "000300"
        "000300"   → "000300"
    """
    return normalize_index_code(code)


# ============================================================
# 指数名称映射（用于显示）
# ============================================================

INDEX_NAME_MAP: Dict[str, str] = {
    "000300": "沪深300",
    "000905": "中证500",
    "000852": "中证1000",
    "000016": "上证50",
    "399006": "创业板指",
    "000688": "科创50",
    "000015": "红利指数",
    "000922": "中证红利",
    "399673": "创业板50",
    "000991": "全指医药",
    "399986": "中证银行",
    "399971": "中证传媒",
    "000832": "中证转债",
}


def get_index_name(code: str) -> str:
    """获取指数中文名称"""
    short = index_code_to_short(code)
    return INDEX_NAME_MAP.get(short, f"指数{short}")


# ============================================================
# PE/PB 估值数据
# ============================================================

@cached(ttl=CACHE_TTL["long"])
def load_index_valuation(index_code: str) -> Optional[pd.DataFrame]:
    """
    加载指数 PE/PB 估值历史（中证指数官方接口）。
    
    Args:
        index_code: 指数代码，如 "sh000300"、"000300" 等
    
    Returns:
        DataFrame 列：日期 / 指数代码 / 指数名称 / 当日收盘点位 / 滚动市盈率 / 市净率 / 股息率
        失败返回 None
    """
    short_code = index_code_to_short(index_code)
    
    try:
        df = call_with_timeout(
            ak.stock_zh_index_value_csindex, kwargs={"symbol": short_code},
            timeout=15.0
        )
        if df is not None and not df.empty:
            # 标准化列名
            col_map = {
                "日期": "date",
                "指数代码": "index_code",
                "指数名称": "index_name",
                "收盘点位": "close",
                "滚动市盈率": "pe_ttm",
                "市净率": "pb",
                "股息率": "dividend_yield",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            
            # 确保有 date 列
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            
            # 数值列转 float
            for col in ["close", "pe_ttm", "pb", "dividend_yield"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            logger.info(f"[load_index_valuation] {short_code} 估值数据 {len(df)} 行")
            return df
    except Exception as e:
        logger.warning(f"[load_index_valuation] {short_code} 估值数据加载失败: {e}")
    
    return None


def calc_valuation_percentile(
    valuation_df: Optional[pd.DataFrame],
    metric: str = "pe_ttm",
    window_years: int = 5,
) -> Dict[str, Any]:
    """
    计算估值历史分位数。
    
    Args:
        valuation_df: load_index_valuation 返回的 DataFrame
        metric: "pe_ttm" / "pb"
        window_years: 回看年限（默认5年）
    
    Returns:
        {
            "current": 当前值,
            "percentile": 当前分位（0-100）,
            "min": 最小值,
            "max": 最大值,
            "median": 中位数,
            "zone": "极度低估" / "低估" / "合理" / "高估" / "极度高估",
        }
    """
    result = {
        "current": None,
        "percentile": None,
        "min": None,
        "max": None,
        "median": None,
        "zone": "数据不足",
    }
    
    if valuation_df is None or valuation_df.empty or metric not in valuation_df.columns:
        return result
    
    # 过滤最近N年数据
    df = valuation_df.dropna(subset=[metric]).copy()
    if df.empty:
        return result
    
    if "date" in df.columns:
        cutoff = df["date"].max() - pd.Timedelta(days=window_years * 365)
        df = df[df["date"] >= cutoff]
    
    if len(df) < 60:  # 至少3个月数据
        return result
    
    current = float(df.iloc[-1][metric])
    series = df[metric].values
    
    result["current"] = round(current, 2)
    result["min"] = round(float(np.min(series)), 2)
    result["max"] = round(float(np.max(series)), 2)
    result["median"] = round(float(np.median(series)), 2)
    result["percentile"] = round(float(np.percentile(series, range(101)).searchsorted(current)), 1)
    
    # 分区判定
    p = result["percentile"]
    if p <= 10:
        result["zone"] = "极度低估"
    elif p <= 30:
        result["zone"] = "低估"
    elif p <= 70:
        result["zone"] = "合理"
    elif p <= 90:
        result["zone"] = "高估"
    else:
        result["zone"] = "极度高估"
    
    return result


# ============================================================
# 指数成份股权重
# ============================================================

@cached(ttl=CACHE_TTL["long"])
def load_index_cons_weights(index_code: str) -> Optional[pd.DataFrame]:
    """
    加载指数成份股及权重（中证指数官方接口）。
    
    Args:
        index_code: 指数代码，如 "sh000300"、"000300" 等
    
    Returns:
        DataFrame 列：成分券代码 / 成分券名称 / 权重(%)
        失败返回 None
    """
    short_code = index_code_to_short(index_code)
    
    try:
        df = call_with_timeout(
            ak.index_stock_cons_weight_csindex, kwargs={"symbol": short_code},
            timeout=15.0
        )
        if df is not None and not df.empty:
            # 标准化列名
            col_map = {
                "成分券代码": "code",
                "成分券名称": "name",
                "权重": "weight",
            }
            df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
            df["weight"] = pd.to_numeric(df.get("weight", 0), errors="coerce")
            
            logger.info(f"[load_index_cons_weights] {short_code} 成份股 {len(df)} 只")
            return df
    except Exception as e:
        logger.warning(f"[load_index_cons_weights] {short_code} 成份股加载失败: {e}")
    
    return None


def build_concentration_analysis(weights_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """
    构建持仓集中度分析数据。
    
    Args:
        weights_df: load_index_cons_weights 返回的 DataFrame
    
    Returns:
        {
            "total_count": 总成份股数,
            "top10": [{code, name, weight}, ...],  # 前十大
            "top10_sum": 前十大权重合计(%),
            "top5_sum": 前五大权重合计(%),
            "hhi": HHI 集中度指数,
            "giant_risk": "巨头风险：XXX 占比 X.XX%，若跌停直接拉低净值 X.XX%",
        }
    """
    result = {
        "total_count": 0,
        "top10": [],
        "top10_sum": 0.0,
        "top5_sum": 0.0,
        "hhi": 0.0,
        "giant_risk": "",
    }
    
    if weights_df is None or weights_df.empty:
        return result
    
    df = weights_df.dropna(subset=["weight"]).copy()
    if df.empty:
        return result
    
    # 按权重降序
    df = df.sort_values("weight", ascending=False).reset_index(drop=True)
    
    result["total_count"] = len(df)
    
    # 前十大
    top10 = df.head(10)
    result["top10"] = top10[["code", "name", "weight"]].to_dict("records")
    result["top10_sum"] = round(float(top10["weight"].sum()), 2)
    result["top5_sum"] = round(float(df.head(5)["weight"].sum()), 2)
    
    # HHI（赫芬达尔指数）
    weights = df["weight"].values / 100.0  # 转为小数
    hhi = float(np.sum(weights ** 2)) * 10000  # 通常乘以10000
    result["hhi"] = round(hhi, 0)
    
    # 巨头风险文案
    top1 = df.iloc[0]
    name = top1.get("name", "未知")
    w = top1.get("weight", 0)
    impact = round(w / 100 * 0.10 * 100, 3)  # 跌停10%的净值影响
    result["giant_risk"] = (
        f"巨头风险：{name} 占比 {w:.2f}%，"
        f"若跌停直接拉低净值约 {impact:.3f}%"
    )
    
    return result


# ============================================================
# 费率数据
# ============================================================

def load_fund_fee_detail(symbol: str) -> Dict[str, float]:
    """
    加载基金费率详情（管理费/托管费/销售服务费）。
    
    Args:
        symbol: 基金代码
    
    Returns:
        {
            "management_fee": 管理费率（小数，如0.005）,
            "custody_fee": 托管费率（小数）,
            "sales_service_fee": 销售服务费率（小数，C类）,
            "total_expense_ratio": 综合费率 TER（小数）,
        }
    """
    result = {
        "management_fee": 0.0,
        "custody_fee": 0.0,
        "sales_service_fee": 0.0,
        "total_expense_ratio": 0.0,
    }
    
    try:
        df = call_with_timeout(
            ak.fund_fee_em, kwargs={"symbol": symbol, "indicator": "运作费用"},
            timeout=10.0
        )
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                item = str(row.iloc[0]) if len(row) > 0 else ""
                value = str(row.iloc[1]) if len(row) > 1 else ""
                
                # 解析费率值（可能含 %）
                try:
                    val = float(str(value).replace("%", "").strip()) / 100
                except (ValueError, TypeError):
                    val = 0.0
                
                if "管理费" in item and "管理费率" not in item:
                    result["management_fee"] = val
                elif "托管费" in item:
                    result["custody_fee"] = val
                elif "销售服务" in item:
                    result["sales_service_fee"] = val
    except Exception as e:
        logger.warning(f"[load_fund_fee_detail] {symbol} 费率加载失败: {e}")
    
    result["total_expense_ratio"] = (
        result["management_fee"]
        + result["custody_fee"]
        + result["sales_service_fee"]
    )
    
    return result


def classify_fund_subtype(fund_name: str) -> str:
    """
    识别指数基金的子类型。
    
    Args:
        fund_name: 基金全称
    
    Returns:
        "passive"  — 被动跟踪型（纯指数基金）
        "enhanced" — 指数增强型
    """
    name = fund_name.upper() if fund_name else ""
    
    # 增强型关键词
    enhanced_keywords = ["增强", "增利", "优化"]
    for kw in enhanced_keywords:
        if kw in fund_name:
            return "enhanced"
    
    return "passive"


def is_etf(fund_name: str, symbol: str) -> bool:
    """
    判断是否为 ETF（交易所交易基金）。
    
    判断依据：
    1. 基金名称含 "ETF"
    2. 基金代码以 51 开头（上交所ETF）或 159 开头（深交所ETF）
    
    Args:
        fund_name: 基金全称
        symbol: 基金代码
    
    Returns:
        True = ETF, False = 场外指数基金
    """
    if "ETF" in (fund_name or "").upper():
        return True
    
    code = str(symbol).strip()
    if code.startswith("51") or code.startswith("159"):
        return True
    
    return False


# ============================================================
# ETF 流动性数据
# ============================================================

def load_etf_liquidity(symbol: str, years: int = 1) -> Dict[str, Any]:
    """
    加载 ETF 流动性指标。
    
    Args:
        symbol: 基金代码
        years: 回看年数
    
    Returns:
        {
            "daily_avg_amount": 日均成交额（万元）,
            "daily_avg_amount_str": 日均成交额（格式化字符串）,
            "amount_trend": 近3月/6月/1年日均成交额,
            "is_liquid": 是否流动性充足,
        }
    """
    from datetime import datetime, timedelta
    from data_loader.index_loader import load_etf_daily_trading
    
    result = {
        "daily_avg_amount": 0.0,
        "daily_avg_amount_str": "—",
        "amount_trend": {},
        "is_liquid": True,
    }
    
    try:
        df = load_etf_daily_trading(symbol, years=years)
        if df is None or df.empty or "amount" not in df.columns:
            return result
        
        df = df.dropna(subset=["amount"])
        if df.empty:
            return result
        
        # 全期日均
        avg_all = float(df["amount"].mean())
        result["daily_avg_amount"] = round(avg_all, 0)
        
        # 格式化
        if avg_all >= 10000:
            result["daily_avg_amount_str"] = f"{avg_all / 10000:.2f} 亿元"
        else:
            result["daily_avg_amount_str"] = f"{avg_all:.0f} 万元"
        
        # 分期限日均
        now = datetime.now()
        for label, days in [("近3月", 90), ("近6月", 180), ("近1年", 365)]:
            cutoff = now - timedelta(days=days)
            if "date" in df.columns:
                sub = df[df["date"] >= pd.Timestamp(cutoff)]
            else:
                sub = df.tail(days)
            if not sub.empty:
                avg = float(sub["amount"].mean())
                if avg >= 10000:
                    result["amount_trend"][label] = f"{avg / 10000:.2f} 亿"
                else:
                    result["amount_trend"][label] = f"{avg:.0f} 万"
        
        # 流动性判定：日均成交额低于 500 万预警
        result["is_liquid"] = avg_all >= 500
        
    except Exception as e:
        logger.warning(f"[load_etf_liquidity] {symbol} 流动性分析失败: {e}")
    
    return result


# ============================================================
# 调仓日历（6月/12月）
# ============================================================

def get_rebalance_info() -> Dict[str, Any]:
    """
    返回指数定期调仓信息（通用规则）。
    
    中证/国证指数通常每年6月和12月第二个周五收盘后生效。
    
    Returns:
        {
            "rebalance_months": [6, 12],
            "next_rebalance": "2026年6月第二个周五",
            "description": "中证/国证系列指数每年6月和12月定期调整成份股...",
        }
    """
    from datetime import datetime
    
    now = datetime.now()
    year = now.year
    
    # 计算6月第二个周五
    def _second_friday(year: int, month: int) -> str:
        import calendar
        cal = calendar.Calendar(firstweekday=calendar.MONDAY)
        fridays = [d for d in cal.itermonthdates(year, month) 
                   if d.weekday() == 4 and d.month == month]
        if len(fridays) >= 2:
            return fridays[1].strftime("%Y年%m月%d日")
        return f"{year}年{month}月"
    
    next_june = _second_friday(year, 6)
    next_dec = _second_friday(year, 12)
    
    if now.month < 6:
        next_rebalance = next_june
    elif now.month < 12:
        next_rebalance = next_dec
    else:
        next_rebalance = _second_friday(year + 1, 6)
    
    return {
        "rebalance_months": [6, 12],
        "next_rebalance": next_rebalance,
        "description": (
            "中证/国证系列指数每年6月和12月定期调整成份股，"
            "通常在第二个周五收盘后生效。规模较大的指数基金在调仓期可能产生冲击成本，"
            "导致短期净值波动。"
        ),
    }
