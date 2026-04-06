"""
bond_rate_engine.py — 资产维度·利率债指标计算

指标清单：
  拟买入：TRI脱水、久期、期限利差、DV01、回撤修复天数
  已持有：最大回撤、底层机构占比
  通用：收益率曲线形态

数据来源：
- 久期/DV01：从债券持仓推算（复用 bond_engine 的 _infer_bond_duration）
- 期限利差：10Y国债 - 2Y国债
- 国债/中债指数：已有缓存接口
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from models.schema import HoldingsData, CleanNavData, BondYieldData
from models.schema_v2 import RateBondMetrics
from engine.bond_engine import (
    BOND_MATURITY_DURATION_MAP,
    _infer_bond_duration,
)

logger = logging.getLogger(__name__)


def run_rate_bond_analysis(
    nav: CleanNavData,
    holdings: HoldingsData,
    yield_data: Optional[BondYieldData] = None,
    mode: str = "buy",
) -> RateBondMetrics:
    """
    利率债维度分析。

    Args:
        nav: 清洗后净值
        holdings: 持仓数据（含 bond_details 和 bond_classification）
        yield_data: 国债收益率 + 信用利差
        mode: "buy" / "hold"

    Returns:
        RateBondMetrics
    """
    result = RateBondMetrics()

    # 获取利率债持仓（从 bond_classification 中提取）
    bond_class = holdings.bond_classification or {}
    rate_bond_details = bond_class.get("gov_bond", {}).get("details", [])
    rate_bond_ratio = bond_class.get("gov_bond", {}).get("ratio", 0)

    if not rate_bond_details and rate_bond_ratio == 0:
        # 尝试从全部债券持仓中筛选利率债
        rate_bond_details = _filter_rate_bonds(holdings.bond_details)
        if rate_bond_details:
            rate_bond_ratio = sum(
                _safe_ratio(b) for b in rate_bond_details
            )

    if not rate_bond_details and rate_bond_ratio <= 0:
        return result

    # === 久期（加权） ===
    durations = []
    weights = []
    for bond in rate_bond_details:
        name = str(bond.get("债券名称", "") or "")
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue
        dur = _infer_bond_duration(name)
        durations.append(dur)
        weights.append(ratio)

    if durations and weights:
        total_w = sum(weights)
        if total_w > 0:
            result.duration = round(
                sum(d * w / total_w for d, w in zip(durations, weights)), 2
            )

    # === DV01（万一价值） ===
    # DV01 ≈ Duration × 0.0001 × 基金利率债部分市值（简化为单位净值变化）
    if result.duration:
        # 基金净值对利率变化 1bp 的敏感度
        result.dv01 = round(result.duration * rate_bond_ratio * 0.0001 * 10000, 2)
        # 单位：bp（每 1bp 利率变动，净值变动多少 bp）

    # === 期限利差 ===
    if yield_data is not None and yield_data.df is not None:
        ydf = yield_data.df
        if "yield_10y" in ydf.columns and "yield_2y" in ydf.columns:
            latest = ydf.iloc[-1]
            y10 = _safe_float_val(latest.get("yield_10y"))
            y2 = _safe_float_val(latest.get("yield_2y"))
            if y10 is not None and y2 is not None:
                result.term_spread = round((y10 - y2) * 100, 1)  # bp

                # 收益率曲线形态
                if y10 > y2 + 0.5:  # > 50bp
                    result.yield_curve_shape = "陡峭"
                elif y10 < y2:
                    result.yield_curve_shape = "倒挂"
                elif y10 < y2 + 0.3:  # < 30bp
                    result.yield_curve_shape = "平坦"
                else:
                    result.yield_curve_shape = "正常"

    # === 回撤修复天数 ===
    if nav.df is not None and not nav.df.empty:
        result.drawdown_recovery_days = _calc_drawdown_recovery(nav)

    # === 已持有模式额外指标 ===
    if mode == "hold":
        # 最大回撤
        if nav.df is not None and not nav.df.empty:
            result.max_drawdown = _calc_max_drawdown(nav)

        # 底层机构占比
        result.institution_ratio = _get_institution_ratio(holdings)

    # === 全收益脱水 ===
    # 需要中债综合指数作为利率债基准
    result.tri_deviation = _calc_rate_bond_tri_deviation(nav)

    return result


# ============================================================
# 辅助函数
# ============================================================

def _filter_rate_bonds(bond_details: List[Dict]) -> List[Dict]:
    """从债券持仓中筛选利率债"""
    if not bond_details:
        return []

    rate_keywords = ["国债", "国开", "进出口", "农发", "央票", "地方政府", "政金"]
    result = []
    for bond in bond_details:
        name = str(bond.get("债券名称", "")).upper()
        if any(kw in name for kw in rate_keywords):
            result.append(bond)
    return result


def _safe_ratio(bond: Dict) -> float:
    """安全提取债券占净值比例"""
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


def _calc_drawdown_recovery(nav: CleanNavData) -> Optional[int]:
    """最大回撤修复天数"""
    try:
        from engine.common_metrics import recovery_days, max_drawdown
        rets = nav.df.set_index("date")["ret"]
        return recovery_days(rets.values)
    except Exception:
        return None


def _calc_max_drawdown(nav: CleanNavData) -> Optional[float]:
    """最大回撤"""
    try:
        from engine.common_metrics import max_drawdown
        rets = nav.df.set_index("date")["ret"]
        mdd = max_drawdown(rets.values)
        return round(mdd * 100, 2) if mdd else None
    except Exception:
        return None


def _get_institution_ratio(holdings: HoldingsData) -> Optional[float]:
    """获取机构持有比例（从本地数据或API）"""
    # 这个需要从 fund_holder 数据中获取
    # 暂时返回 None，后续接入
    return None


def _calc_rate_bond_tri_deviation(nav: CleanNavData) -> Optional[float]:
    """
    利率债全收益脱水（含权TRI偏离度）。

    基准：中债综合财富指数
    """
    try:
        from data_loader.bond_loader import load_bond_composite_index
        start = str(nav.df["date"].min())[:10]
        end = str(nav.df["date"].max())[:10]
        bm_df = load_bond_composite_index("财富", start, end)
        if bm_df is None or bm_df.empty:
            return None

        fund_ret = nav.df.set_index("date")["ret"]
        bm_s = bm_df.set_index("date")["ret"].reindex(fund_ret.index).fillna(0)
        fund_cum = (1 + fund_ret).cumprod()
        bm_cum = (1 + bm_s).cumprod()

        aligned = pd.DataFrame({"fund": fund_cum, "bm": bm_cum}).dropna()
        if len(aligned) < 20:
            return None

        dev = (aligned["fund"].iloc[-1] - aligned["bm"].iloc[-1]) / aligned["bm"].iloc[-1] * 100
        return round(float(dev), 2)
    except Exception as e:
        logger.debug(f"[rate_bond] TRI deviation 计算失败: {e}")
        return None
