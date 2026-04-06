"""
bond_credit_engine.py — 资产维度·信用债指标计算

指标清单：
  拟买入：YTM、平均信用评级、机构持有比例变化
  已持有：违约预警、再投资风险
  通用：信用利差走势（收窄/走阔/平稳）

数据来源：
- 信用利差：BondYieldData.credit_spread（真实数据）
- 评级：从债券持仓名称推断
- 机构持有比例：fund_holder 本地数据
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from models.schema import HoldingsData, BondYieldData
from models.schema_v2 import CreditBondMetrics

logger = logging.getLogger(__name__)


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
        fund_code: 基金代码（用于加载机构持有比例）

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

    # === YTM（静态收益率） ===
    # 从持仓数据中提取（如果有的话），或用中债 AA+ 收益率近似
    result.ytm = _estimate_portfolio_ytm(credit_details, yield_data)

    # === 平均信用评级 ===
    result.avg_rating = _calc_avg_rating(credit_details)

    # === 信用利差走势 ===
    if yield_data is not None and yield_data.df is not None:
        ydf = yield_data.df
        if "credit_spread" in ydf.columns:
            spread_series = ydf["credit_spread"].dropna()
            if not spread_series.empty:
                result.credit_spread_latest = round(float(spread_series.iloc[-1]), 2)

                # 趋势判断：比较近 3 个月均值 vs 前 3 个月均值
                if len(spread_series) >= 60:
                    recent = spread_series.tail(60).mean()
                    prev = spread_series.iloc[-120:-60].mean() if len(spread_series) >= 120 else spread_series.head(len(spread_series) - 60).mean()
                    diff = recent - prev
                    if diff > 10:  # 扩大超过 10bp
                        result.credit_spread_trend = "走阔"
                    elif diff < -10:
                        result.credit_spread_trend = "收窄"
                    else:
                        result.credit_spread_trend = "平稳"

                # 图表数据
                if "date" in ydf.columns:
                    chart_df = ydf[["date", "credit_spread"]].dropna()
                    chart_df["date"] = pd.to_datetime(chart_df["date"])
                    result.credit_spread_df = chart_df

    # === 机构持有比例变化 ===
    result.institution_ratio_change = _get_institution_ratio_change(fund_code)

    # === 已持有模式额外指标 ===
    if mode == "hold":
        # 违约预警
        result.default_warning = _detect_default_warning(credit_details, yield_data)

        # 再投资风险
        result.reinvestment_risk = _evaluate_reinvestment_risk(credit_details, yield_data)

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


def _estimate_portfolio_ytm(
    credit_details: List[Dict],
    yield_data: Optional[BondYieldData] = None,
) -> Optional[float]:
    """
    估算组合静态收益率 YTM。

    策略：
    1. 从持仓数据中提取（如果 API 返回了到期收益率）
    2. 否则用中债 AA+ 企业债收益率近似
    """
    # 尝试从持仓明细提取 YTM
    ytms = []
    weights = []
    for bond in credit_details:
        ytm = bond.get("到期收益率") or bond.get("YTM")
        ratio = _safe_ratio(bond)
        if ytm is not None and ratio > 0:
            try:
                ytms.append(float(ytm))
                weights.append(ratio)
            except (ValueError, TypeError):
                pass

    if ytms and weights:
        total_w = sum(weights)
        return round(sum(y * w / total_w for y, w in zip(ytms, weights)), 4)

    # 回退：用信用利差 + 10年国债近似
    if yield_data is not None and yield_data.df is not None:
        ydf = yield_data.df
        latest = ydf.iloc[-1]
        y10 = _safe_float_val(latest.get("yield_10y"))
        cs = _safe_float_val(latest.get("credit_spread"))
        if y10 is not None and cs is not None:
            return round(y10 + cs / 100, 4)  # cs 是 bp，需除以100

    return None


def _calc_avg_rating(credit_details: List[Dict]) -> Optional[str]:
    """
    计算平均信用评级。

    评级顺序：AAA > AA+ > AA > AA- > A+ > A > A- > BBB+
    从债券名称中推断评级
    """
    rating_map = {
        "AAA": 7, "AA+": 6, "AA": 5, "AA-": 4,
        "A+": 3, "A": 2, "A-": 1, "BBB+": 0,
    }
    rating_scores = []

    for bond in credit_details:
        name = str(bond.get("债券名称", "")).upper()
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue

        # 从债券名称中提取评级
        detected_rating = None
        for rating in rating_map:
            if rating in name:
                detected_rating = rating
                break

        # 也检查债券明细中是否有单独的评级字段
        if not detected_rating:
            bond_rating = bond.get("信用评级", "")
            if bond_rating:
                detected_rating = str(bond_rating).strip()

        if detected_rating and detected_rating in rating_map:
            rating_scores.append((rating_map[detected_rating], ratio))

    if not rating_scores:
        return None

    # 加权平均
    total_w = sum(w for _, w in rating_scores)
    if total_w <= 0:
        return None

    avg_score = sum(s * w for s, w in rating_scores) / total_w

    # 找到最接近的评级
    closest = min(rating_map.items(), key=lambda x: abs(x[1] - avg_score))
    return closest[0]


def _get_institution_ratio_change(fund_code: str) -> Optional[float]:
    """
    机构持有比例变化（最近两期差值）。

    数据来源：fund_holder_*.csv（本地缓存）
    """
    if not fund_code:
        return None

    try:
        from data_loader.cache_paths import DAILY_DIR, latest
        path = latest(DAILY_DIR, "fund_holder_*.csv")
        if path is None:
            return None

        df = pd.read_csv(path, dtype=str)
        # 查找该基金的机构持有比例
        fund_rows = df[df.iloc[:, 0] == fund_code] if len(df.columns) > 0 else pd.DataFrame()
        if fund_rows.empty or len(fund_rows) < 2:
            return None

        # 提取最近两期机构持有比例
        # fund_holder 表结构通常包含：基金代码、机构持有比例、日期
        ratios = []
        for _, row in fund_rows.iterrows():
            for col in df.columns:
                if "机构" in col or "institution" in col.lower():
                    val = _safe_float_val(row.get(col))
                    if val is not None:
                        ratios.append(val)

        if len(ratios) >= 2:
            return round(ratios[-1] - ratios[-2], 2)

    except Exception as e:
        logger.debug(f"[credit_bond] 机构持有比例加载失败: {e}")

    return None


def _detect_default_warning(
    credit_details: List[Dict],
    yield_data: Optional[BondYieldData] = None,
) -> Optional[str]:
    """
    违约预警：信用利差异常跳升 + 低评级占比高

    规则化预警（非精确模型）：
    1. 近期信用利差走阔超过 30bp
    2. AA- 及以下占比超过 30%
    """
    warnings = []

    # 1. 利差走阔检测
    if yield_data is not None and yield_data.df is not None:
        ydf = yield_data.df
        if "credit_spread" in ydf.columns:
            spread = ydf["credit_spread"].dropna()
            if len(spread) >= 20:
                recent = spread.tail(20).mean()
                prev = spread.head(len(spread) - 20).tail(20).mean()
                if recent - prev > 30:
                    warnings.append(f"近期信用利差走阔 {round(recent - prev, 1)}bp")

    # 2. 低评级检测
    low_rating_ratio = 0.0
    total_ratio = 0.0
    for bond in credit_details:
        name = str(bond.get("债券名称", "")).upper()
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue
        total_ratio += ratio
        # AA- 及以下
        for low_r in ["AA-", "A+", "A", "A-", "BBB+"]:
            if low_r in name:
                low_rating_ratio += ratio
                break

    if total_ratio > 0 and low_rating_ratio / total_ratio > 0.3:
        warnings.append(f"AA-及以下评级占比 {round(low_rating_ratio/total_ratio*100, 1)}%")

    return "；".join(warnings) if warnings else None


def _evaluate_reinvestment_risk(
    credit_details: List[Dict],
    yield_data: Optional[BondYieldData] = None,
) -> Optional[str]:
    """
    再投资风险评价。

    关注点：
    1. 短债占比高 + 利率下行 → 再投资收益下降
    2. 大量债券即将到期 → 再投资压力
    """
    # 简化版：检查短债占比
    short_keywords = ["1年", "1Y", "6M", "3M", "超短", "270天", "365天"]
    short_ratio = 0.0
    total_ratio = 0.0

    for bond in credit_details:
        name = str(bond.get("债券名称", "")).upper()
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue
        total_ratio += ratio
        if any(kw in name for kw in short_keywords):
            short_ratio += ratio

    if total_ratio > 0 and short_ratio / total_ratio > 0.5:
        return f"短债占比 {round(short_ratio/total_ratio*100, 1)}%，到期再投资压力较大"

    return None


def _safe_ratio(bond: Dict) -> float:
    ratio = float(bond.get("占净值比例", 0) or 0)
    if ratio > 1.5:
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
