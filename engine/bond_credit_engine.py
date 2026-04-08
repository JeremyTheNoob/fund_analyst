"""
bond_credit_engine.py — 资产维度·信用债指标计算

指标清单：
  拟买入：YTM（组合加权到期收益率）、平均信用评级
  已持有：违约预警（城投/地产/弱资质检测）
  通用：信用利差（信用债 YTM − 同期限国债 YTM，插值法）

数据来源：
- 国债收益率曲线：bond_china_yield 表（中债国债收益率曲线）
- AAA 信用债收益率：bond_china_yield 表（中债中短期票据收益率曲线(AAA)）
- 债券剩余期限：bond_info 表（maturity_date 动态计算）
- 城投/地产识别：债券名称关键字匹配
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from models.schema import HoldingsData, BondYieldData
from models.schema_v2 import CreditBondMetrics

logger = logging.getLogger(__name__)


# ============================================================
# 国债收益率曲线插值锚点（年）
# ============================================================
_YIELD_CURVE_ANCHORS = [0.25, 0.5, 1.0, 3.0, 5.0, 7.0, 10.0, 30.0]
_YIELD_CURVE_COLS = ["3月", "6月", "1年", "3年", "5年", "7年", "10年", "30年"]


def run_credit_bond_analysis(
    holdings: HoldingsData,
    yield_data: Optional[BondYieldData] = None,
    mode: str = "buy",
    fund_code: str = "",
) -> CreditBondMetrics:
    """
    信用债维度分析。

    Args:
        holdings: 持仓数据
        yield_data: 国债收益率 + 信用利差
        mode: "buy" / "hold"
        fund_code: 基金代码

    Returns:
        CreditBondMetrics
    """
    result = CreditBondMetrics()

    # 获取信用债持仓（从 bond_classification 中提取）
    bond_class = holdings.bond_classification or {}
    credit_details = bond_class.get("credit_bond", {}).get("details", [])

    # 如果 bond_classification 为空，从全部债券中筛选信用债
    if not credit_details:
        credit_details = _filter_credit_bonds(holdings.bond_details)

    if not credit_details:
        return result

    ref_date = date.today()

    # === YTM（组合加权静态收益率） ===
    result.ytm = _calc_portfolio_ytm(credit_details, yield_data, ref_date)

    # === 平均信用评级 ===
    result.avg_rating = _calc_avg_rating(credit_details)

    # === 信用利差（信用债YTM − 同期限国债YTM） ===
    spread_result = _calc_credit_spread(credit_details, yield_data, ref_date)
    if spread_result is not None:
        result.credit_spread_latest = spread_result["spread"]
        result.credit_spread_trend = spread_result["trend"]
        result.credit_spread_df = spread_result["df"]

    # === 已持有模式额外指标 ===
    if mode == "hold":
        result.default_warning = _detect_sector_warning(credit_details)

    return result


# ============================================================
# 辅助函数
# ============================================================

def _filter_credit_bonds(bond_details: List[Dict]) -> List[Dict]:
    """筛选信用债（排除利率债、转债）"""
    if not bond_details:
        return []

    rate_keywords = ["国债", "国开", "进出口", "农发", "央票", "地方政府", "政金"]
    cb_keywords = ["可转债", "转债"]

    result = []
    for bond in bond_details:
        name = str(bond.get("债券名称", "")).upper()
        if any(kw in name for kw in rate_keywords):
            continue
        if any(kw in name for kw in cb_keywords):
            continue
        result.append(bond)
    return result


def _calc_portfolio_ytm(
    credit_details: List[Dict],
    yield_data: Optional[BondYieldData],
    ref_date: date,
) -> Optional[float]:
    """
    组合加权静态收益率 YTM。

    策略：
    1. 对每只信用债，根据剩余期限查 AAA 中短期票据收益率曲线（插值）得到该债 YTM
    2. 按占净值比例加权，得到组合 YTM
    3. 若 AAA 曲线不可用，退而用 3 年期 AAA 作为近似
    """
    # 获取 AAA 收益率曲线最新一日数据
    aaa_latest = _load_aaa_yield_latest()
    if aaa_latest is None:
        return None

    weighted_ytm = 0.0
    total_weight = 0.0

    for bond in credit_details:
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue

        bond_code = str(bond.get("债券代码", ""))
        bond_name = str(bond.get("债券名称", ""))

        # 获取剩余期限
        remaining = _get_bond_remaining_maturity(bond_code, bond_name, ref_date)
        if remaining is None:
            remaining = 3.0  # 默认 3 年

        # 插值 AAA 收益率
        ytm = _interpolate_yield(aaa_latest, remaining)
        if ytm is None:
            ytm = aaa_latest.get(3.0) or aaa_latest.get(5.0)  # 兜底 3Y/5Y

        if ytm is not None:
            weighted_ytm += ytm * ratio
            total_weight += ratio

    if total_weight > 0:
        return round(weighted_ytm / total_weight, 4)

    return None


def _calc_credit_spread(
    credit_details: List[Dict],
    yield_data: Optional[BondYieldData],
    ref_date: date,
) -> Optional[Dict[str, Any]]:
    """
    计算信用利差 = 信用债 YTM − 同期限国债 YTM。

    对组合中每只信用债：
    1. 获取剩余期限
    2. 从 AAA 中短期票据收益率曲线插值得到信用债 YTM
    3. 从国债收益率曲线插值得到同期限国债 YTM
    4. 两者之差 = 该债信用利差
    5. 按占净值比例加权，得到组合信用利差

    同时计算历史走势（3 年），判断趋势。
    """
    aaa_history, gov_history = _load_yield_curve_history()
    if aaa_history is None or gov_history is None:
        return None

    # 加权计算最新利差
    weighted_spread = 0.0
    total_weight = 0.0

    for bond in credit_details:
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue

        bond_code = str(bond.get("债券代码", ""))
        bond_name = str(bond.get("债券名称", ""))
        remaining = _get_bond_remaining_maturity(bond_code, bond_name, ref_date)
        if remaining is None:
            remaining = 3.0

        aaa_latest = aaa_history.iloc[-1] if not aaa_history.empty else None
        gov_latest = gov_history.iloc[-1] if not gov_history.empty else None
        if aaa_latest is None or gov_latest is None:
            continue

        aaa_ytm = _interpolate_yield(aaa_latest, remaining)
        gov_ytm = _interpolate_yield(gov_latest, remaining)
        if aaa_ytm is not None and gov_ytm is not None:
            spread = aaa_ytm - gov_ytm
            weighted_spread += spread * ratio
            total_weight += ratio

    if total_weight <= 0:
        return None

    portfolio_spread = weighted_spread / total_weight  # 单位：%（非 bp）

    # 构建历史时序：对每个历史日期，做同样的加权计算
    spread_series = _build_spread_timeseries(
        aaa_history, gov_history, credit_details, ref_date
    )

    if spread_series is None or spread_series.empty:
        return {
            "spread": round(portfolio_spread * 100, 1),  # 转 bp
            "trend": None,
            "df": None,
        }

    # 趋势判断
    trend = None
    if len(spread_series) >= 60:
        recent = spread_series.tail(60).mean()
        prev_end = len(spread_series) - 60
        prev = spread_series.iloc[:prev_end].tail(60).mean() if prev_end >= 60 else spread_series.head(prev_end).mean()
        diff_bp = (recent - prev) * 100
        if diff_bp > 10:
            trend = "走阔"
        elif diff_bp < -10:
            trend = "收窄"
        else:
            trend = "平稳"

    chart_df = spread_series.reset_index()
    chart_df.columns = ["date", "credit_spread"]

    return {
        "spread": round(portfolio_spread * 100, 1),  # 转 bp
        "trend": trend,
        "df": chart_df,
    }


def _build_spread_timeseries(
    aaa_history: pd.DataFrame,
    gov_history: pd.DataFrame,
    credit_details: List[Dict],
    ref_date: date,
) -> Optional[pd.Series]:
    """
    构建信用利差历史时序。

    使用月频采样（每月最后一个交易日）减少计算量。
    """
    # 统一日期索引
    aaa_idx = aaa_history.index if isinstance(aaa_history.index, pd.DatetimeIndex) else None
    gov_idx = gov_history.index if isinstance(gov_history.index, pd.DatetimeIndex) else None

    if aaa_idx is None or gov_idx is None:
        return None

    common_dates = aaa_idx.intersection(gov_idx)
    if len(common_dates) == 0:
        return None

    # 月频采样
    monthly_dates = common_dates.to_series().resample("ME").last().dropna().values
    if len(monthly_dates) == 0:
        return None

    spreads = []
    for d in monthly_dates:
        d = pd.Timestamp(d)
        aaa_row = aaa_history.loc[d] if d in aaa_history.index else None
        gov_row = gov_history.loc[d] if d in gov_history.index else None
        if aaa_row is None or gov_row is None:
            continue

        # 用持仓日期（而非今天）计算剩余期限
        bond_date = d.date() if hasattr(d, "date") else d
        weighted_spread = 0.0
        total_weight = 0.0

        for bond in credit_details:
            ratio = _safe_ratio(bond)
            if ratio <= 0:
                continue

            bond_code = str(bond.get("债券代码", ""))
            bond_name = str(bond.get("债券名称", ""))
            remaining = _get_bond_remaining_maturity(bond_code, bond_name, bond_date)
            if remaining is None:
                remaining = 3.0

            aaa_ytm = _interpolate_yield(aaa_row, remaining)
            gov_ytm = _interpolate_yield(gov_row, remaining)
            if aaa_ytm is not None and gov_ytm is not None:
                weighted_spread += (aaa_ytm - gov_ytm) * ratio
                total_weight += ratio

        if total_weight > 0:
            spreads.append((d, weighted_spread / total_weight * 100))  # bp

    if not spreads:
        return None

    series = pd.Series(
        [s[1] for s in spreads],
        index=[s[0] for s in spreads],
        name="credit_spread",
    )
    return series


def _detect_sector_warning(credit_details: List[Dict]) -> Optional[str]:
    """
    行业风险预警：检测城投债、地产债、弱资质债。

    规则：
    1. 城投债占比超过 40% → 预警（区域财政压力）
    2. 地产债占比超过 20% → 预警（行业周期性风险）
    3. AA- 及以下占比超过 30% → 弱资质预警
    """
    warnings = []
    total_ratio = 0.0

    # 分类统计
    urban_ratio = 0.0  # 城投债
    realty_ratio = 0.0  # 地产债
    low_rating_ratio = 0.0  # AA- 及以下

    urban_keywords = ["城投", "轨交", "交投", "建投", "水务", "交建", "城建",
                      "土地储备", "地方平台", "园区", "国资运营", "旅投", "水务集团",
                      "交通投资", "城市建设", "基础设施"]
    realty_keywords = ["地产", "房企", "置业", "房地产", "万科", "保利", "龙湖",
                       "碧桂园", "融创", "恒大", "中海", "华润置地", "招商蛇口"]

    low_ratings = ["AA-", "A+", "A", "A-", "BBB+"]

    for bond in credit_details:
        name = str(bond.get("债券名称", ""))
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue
        total_ratio += ratio

        # 城投检测
        if any(kw in name for kw in urban_keywords):
            urban_ratio += ratio

        # 地产检测
        if any(kw in name for kw in realty_keywords):
            realty_ratio += ratio

        # 弱评级检测
        for lr in low_ratings:
            if lr in name:
                low_rating_ratio += ratio
                break

    if total_ratio <= 0:
        return None

    if urban_ratio / total_ratio > 0.4:
        warnings.append(f"城投债占比 {round(urban_ratio/total_ratio*100, 1)}%，需关注区域财政压力")

    if realty_ratio / total_ratio > 0.2:
        warnings.append(f"地产债占比 {round(realty_ratio/total_ratio*100, 1)}%，行业周期性风险较高")

    if low_rating_ratio / total_ratio > 0.3:
        warnings.append(f"AA-及以下评级占比 {round(low_rating_ratio/total_ratio*100, 1)}%，弱资质敞口较大")

    return "；".join(warnings) if warnings else None


def _calc_avg_rating(credit_details: List[Dict]) -> Optional[str]:
    """
    计算平均信用评级。

    评级顺序：AAA > AA+ > AA > AA- > A+ > A > A- > BBB+
    从债券名称中推断评级，要求明确评级格式（防止误匹配）。
    """
    rating_map = {
        "AAA": 7, "AA+": 6, "AA": 5, "AA-": 4,
        "A+": 3, "A": 2, "A-": 1, "BBB+": 0,
    }
    # 优先匹配长评级（避免 "A" 匹配到 "AAA" 中的 A）
    # 精确匹配：AAA 独立出现 / AA+ / AA- / A+ / A- / BBB+
    # 单独的 "AA" 和 "A" 需要独立词或后面无字母
    rating_scores = []

    for bond in credit_details:
        name = str(bond.get("债券名称", ""))
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue

        detected_rating = None

        # 也检查债券明细中是否有单独的评级字段
        bond_rating = bond.get("信用评级", "")
        if bond_rating:
            detected_rating = str(bond_rating).strip()

        if not detected_rating:
            # 从名称中提取：AAA / AA+ / AA- / A+ / A- / BBB+
            # 注意：需避免 "A" 误匹配到中文字符间的字母
            import re
            m = re.search(r'(AAA|AA[+\-]|A[+\-]|BBB\+)', name)
            if m:
                detected_rating = m.group(1)

        if detected_rating and detected_rating in rating_map:
            rating_scores.append((rating_map[detected_rating], ratio))

    if not rating_scores:
        return None

    total_w = sum(w for _, w in rating_scores)
    if total_w <= 0:
        return None

    avg_score = sum(s * w for s, w in rating_scores) / total_w
    closest = min(rating_map.items(), key=lambda x: abs(x[1] - avg_score))
    return closest[0]


# ============================================================
# 收益率曲线数据加载
# ============================================================

def _load_aaa_yield_latest() -> Optional[Dict[float, float]]:
    """
    获取 AAA 中短期票据收益率曲线最新一个交易日数据。

    Returns:
        {期限(年): 收益率(%)}，如 {0.25: 1.48, 0.5: 1.51, 1.0: 1.57, ...}
    """
    try:
        from data_loader.db_accessor import DB
        df = DB.query_df(
            "SELECT \"3月\", \"6月\", \"1年\", \"3年\", \"5年\", \"7年\", \"10年\", \"30年\" "
            "FROM bond_china_yield "
            "WHERE \"曲线名称\" = '中债中短期票据收益率曲线(AAA)' "
            "ORDER BY date DESC LIMIT 1"
        )
        if df is None or df.empty:
            return None

        row = df.iloc[0]
        result = {}
        for anchor, col in zip(_YIELD_CURVE_ANCHORS, _YIELD_CURVE_COLS):
            val = _safe_float_val(row.get(col))
            if val is not None:
                result[anchor] = val
        return result if result else None

    except Exception as e:
        logger.debug(f"[credit_bond] AAA 曲线加载失败: {e}")
        return None


def _load_yield_curve_history() -> tuple:
    """
    获取 AAA + 国债收益率曲线的历史数据（3 年）。

    Returns:
        (aaa_df, gov_df)，每个 DataFrame 的 index 为日期，列为期限锚点。
        列名：3月/6月/1年/3年/5年/7年/10年/30年，值为收益率(%)
    """
    try:
        from data_loader.db_accessor import DB

        # AAA 中短期票据
        aaa_df = DB.query_df(
            "SELECT date, \"3月\" as \"3月\", \"6月\" as \"6月\", \"1年\" as \"1年\", "
            "\"3年\" as \"3年\", \"5年\" as \"5年\", \"7年\" as \"7年\", "
            "\"10年\" as \"10年\", \"30年\" as \"30年\" "
            "FROM bond_china_yield "
            "WHERE \"曲线名称\" = '中债中短期票据收益率曲线(AAA)' "
            "ORDER BY date ASC"
        )

        # 国债
        gov_df = DB.query_df(
            "SELECT date, \"3月\" as \"3月\", \"6月\" as \"6月\", \"1年\" as \"1年\", "
            "\"3年\" as \"3年\", \"5年\" as \"5年\", \"7年\" as \"7年\", "
            "\"10年\" as \"10年\", \"30年\" as \"30年\" "
            "FROM bond_china_yield "
            "WHERE \"曲线名称\" = '中债国债收益率曲线' "
            "ORDER BY date ASC"
        )

        if aaa_df is None or aaa_df.empty or gov_df is None or gov_df.empty:
            return None, None

        # 设置日期索引，转数值
        for df in [aaa_df, gov_df]:
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
            for col in _YIELD_CURVE_COLS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        return aaa_df, gov_df

    except Exception as e:
        logger.debug(f"[credit_bond] 收益率曲线历史加载失败: {e}")
        return None, None


# ============================================================
# 插值
# ============================================================

def _interpolate_yield(
    curve_data,
    target_years: float,
) -> Optional[float]:
    """
    线性插值获取目标期限的收益率。

    Args:
        curve_data: dict {期限: 收益率} 或 DataFrame/Series（列为期限名）
        target_years: 目标期限（年）

    Returns:
        收益率 (%)，None 表示无法插值
    """
    # 构建 (期限, 收益率) 点对
    points = []
    if isinstance(curve_data, dict):
        for year, val in curve_data.items():
            v = _safe_float_val(val)
            if v is not None:
                points.append((float(year), float(v)))
    elif hasattr(curve_data, "get"):  # Series / DataFrame row
        for anchor, col in zip(_YIELD_CURVE_ANCHORS, _YIELD_CURVE_COLS):
            val = curve_data.get(col)
            v = _safe_float_val(val)
            if v is not None:
                points.append((anchor, float(v)))

    if not points:
        return None

    points.sort(key=lambda x: x[0])

    # 精确匹配
    for yr, val in points:
        if abs(yr - target_years) < 0.01:
            return val

    # 外推（上限）
    if target_years > points[-1][0]:
        # 简单外推：用最长两个锚点线性外推
        if len(points) >= 2:
            y1, v1 = points[-2]
            y2, v2 = points[-1]
            slope = (v2 - v1) / (y2 - y1) if y2 != y1 else 0
            return v2 + slope * (target_years - y2)
        return points[-1][1]

    # 外推（下限）
    if target_years < points[0][0]:
        if len(points) >= 2:
            y1, v1 = points[0]
            y2, v2 = points[1]
            slope = (v2 - v1) / (y2 - y1) if y2 != y1 else 0
            return v1 + slope * (target_years - y1)
        return points[0][1]

    # 线性插值
    for i in range(len(points) - 1):
        y1, v1 = points[i]
        y2, v2 = points[i + 1]
        if y1 <= target_years <= y2:
            if y2 == y1:
                return v1
            t = (target_years - y1) / (y2 - y1)
            return v1 + t * (v2 - v1)

    return None


# ============================================================
# 期限计算
# ============================================================

def _get_bond_remaining_maturity(
    bond_code: str,
    bond_name: str,
    ref_date,
) -> Optional[float]:
    """
    获取债券剩余期限（年）。

    复用 bond_rate_engine 的逻辑，优先 bond_info.maturity_date。
    """
    try:
        from engine.bond_rate_engine import _get_remaining_maturity
        return _get_remaining_maturity(bond_code, bond_name, ref_date)
    except Exception as e:
        logger.debug(f"[credit_bond] 剩余期限获取失败({bond_code}): {e}")
        return None


# ============================================================
# 工具函数
# ============================================================

def _safe_ratio(bond: Dict) -> float:
    """
    安全提取债券占净值比例（小数，如 0.022 表示 2.2%）。

    数据源（fund_bond_holdings 表）的"占净值比例"列统一为百分比格式（如 2.2 = 2.2%），
    需要除以 100 转为小数。
    """
    ratio = float(bond.get("占净值比例", 0) or 0)
    if ratio > 0:
        ratio = ratio / 100.0
    return ratio


def _safe_float_val(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None
