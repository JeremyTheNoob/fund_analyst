"""
bond_rate_engine.py — 资产维度·利率债指标计算

指标清单：
  拟买入：TRI脱水、久期、期限利差、DV01、回撤修复天数
  已持有：最大回撤、底层机构占比
  通用：收益率曲线形态

数据来源：
- 久期/DV01：从债券持仓推算（优先使用 bond_info 表的到期日数据）
- 期限利差：10Y国债 - 2Y国债
- 国债/中债指数：已有缓存接口

久期计算策略（2026-04-08 升级）：
  1. 优先从 bond_info 表查到期日 → 动态计算剩余期限 → 转修正久期
  2. 退而使用 bond_info.bond_period_years（原始期限）→ 转修正久期
  3. 最终退回名称推断（_infer_bond_duration_fallback）
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from models.schema import HoldingsData, CleanNavData, BondYieldData
from models.schema_v2 import RateBondMetrics

logger = logging.getLogger(__name__)

# 期限 → 修正久期近似系数（Macaulay Duration → Modified Duration）
# 不同期限对应的久期占期限比例，考虑了凸性和付息频率
_DURATION_FACTOR = {
    # (min_years, max_years): factor
    (0, 0.25): 0.98,    # <3个月，接近 full duration
    (0.25, 0.5): 0.97,   # 3-6个月
    (0.5, 1): 0.96,      # 6个月-1年
    (1, 2): 0.93,        # 1-2年
    (2, 3): 0.90,        # 2-3年
    (3, 5): 0.87,        # 3-5年
    (5, 7): 0.84,        # 5-7年
    (7, 10): 0.82,       # 7-10年
    (10, 15): 0.78,      # 10-15年
    (15, 20): 0.74,      # 15-20年
    (20, 30): 0.68,      # 20-30年
    (30, 999): 0.62,     # 30年以上
}


def _duration_factor(years: float) -> float:
    """根据剩余期限获取久期修正系数"""
    for (lo, hi), factor in _DURATION_FACTOR.items():
        if lo <= years < hi:
            return factor
    return 0.70  # 兜底


def _duration_from_remaining_maturity(remaining_years: float) -> float:
    """
    根据剩余期限计算修正久期（Modified Duration）。

    修正久期 ≈ 剩余期限 × 久期系数
    系数随期限增长而递减（因为远端现金流对利率敏感度较低）。

    Args:
        remaining_years: 剩余期限（年）

    Returns:
        修正久期（年）
    """
    if remaining_years <= 0:
        return 0.0
    factor = _duration_factor(remaining_years)
    return round(remaining_years * factor, 2)


def _get_remaining_maturity(
    bond_code: str,
    bond_name: str,
    ref_date: Optional[date] = None,
) -> Optional[float]:
    """
    获取债券剩余期限（年）。

    查找优先级：
    1. bond_info.maturity_date → 动态计算（ref_date - maturity_date）
    2. bond_info.bond_period_years → 原始期限（不考虑已流逝时间）
    3. 返回 None（由调用方 fallback 到名称推断）

    Args:
        bond_code: 债券代码
        bond_name: 债券名称（仅日志用）
        ref_date: 参考日期（默认今天）

    Returns:
        剩余期限（年），或 None
    """
    if ref_date is None:
        ref_date = date.today()

    try:
        from data_loader.db_accessor import get_bond_info

        info = get_bond_info(bond_code)
        if not info:
            return None

        # 策略1：用到期日动态计算
        mat_str = info.get("maturity_date")
        if mat_str and mat_str != "---":
            try:
                mat_date = date.fromisoformat(str(mat_str)[:10])
                remaining = (mat_date - ref_date).days / 365.0
                if remaining > 0:
                    return round(remaining, 4)
                else:
                    # 已到期债券，剩余期限为 0
                    return 0.0
            except (ValueError, TypeError):
                pass

        # 策略2：用原始期限（不精确，但比名称推断好）
        period_years = info.get("bond_period_years")
        if period_years is not None:
            return float(period_years)

        return None

    except Exception as e:
        logger.debug(f"[bond_info] 查询失败 '{bond_name}': {e}")
        return None


def _infer_bond_duration_fallback(bond_name: str) -> float:
    """
    从债券名称推断久期（兜底方案，当 bond_info 表无数据时使用）。
    规则：关键词 + 期限匹配。
    （原 _infer_bond_duration 重命名）
    """
    bond_name = bond_name.lower()

    # 同业存单（极短期）
    if "同业存单" in bond_name or "ncd" in bond_name:
        if "6M" in bond_name or "半年" in bond_name:
            return 0.45
        if "3M" in bond_name or "季度" in bond_name:
            return 0.25
        return 0.8  # 1年期最常见

    # 可转债（含权，久期短）
    if "可转债" in bond_name or "转债" in bond_name:
        return 0.6

    # 国债/政金债：无法从名称判断期限，使用 6.5 年（加权平均近似值）
    if "国债" in bond_name or "国开" in bond_name or "进出口" in bond_name or "农发" in bond_name:
        return 6.5

    # 根据名称中的期限数字推断
    match = re.search(r"(\d+)(年|y)", bond_name)
    if match:
        years = int(match.group(1))
        if years <= 1:
            return 0.9
        elif years <= 3:
            return years * 0.87
        elif years <= 5:
            return years * 0.86
        elif years <= 10:
            return years * 0.84
        else:
            return years * 0.60

    # 默认：中短期信用债（最常见）
    return 2.5


def _calc_bond_duration(bond_code: str, bond_name: str, ref_date: Optional[date] = None) -> float:
    """
    计算单只债券的修正久期。

    优先级：
    1. bond_info 剩余期限 → _duration_from_remaining_maturity
    2. bond_info 原始期限 → _duration_from_remaining_maturity（不精确）
    3. 名称推断兜底

    Args:
        bond_code: 债券代码
        bond_name: 债券名称
        ref_date: 参考日期

    Returns:
        修正久期（年）
    """
    # 尝试从 bond_info 获取剩余期限
    remaining = _get_remaining_maturity(bond_code, bond_name, ref_date)

    if remaining is not None:
        if remaining == 0.0:
            return 0.0
        return _duration_from_remaining_maturity(remaining)

    # 兜底：名称推断
    return _infer_bond_duration_fallback(bond_name)


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

    if not rate_bond_details:
        # 尝试从全部债券持仓中筛选利率债
        rate_bond_details = _filter_rate_bonds(holdings.bond_details)
    
    if not rate_bond_details:
        return result
    
    # 利率债占净值比例：直接从利率债明细汇总
    rate_bond_ratio = sum(_safe_ratio(b) for b in rate_bond_details)

    # === 全收益脱水 ===
    # 仅当利率债占比较高时计算（≥10%），避免股票型基金对比纯债基准失真
    if rate_bond_ratio >= 0.10:
        result.tri_deviation = _calc_rate_bond_tri_deviation(nav)
    
    # === 回撤修复天数 ===
    if nav.df is not None and not nav.df.empty:
        result.drawdown_recovery_days = _calc_drawdown_recovery(nav)

    # === 久期（加权） ===
    durations = []
    weights = []
    for bond in rate_bond_details:
        name = str(bond.get("债券名称", "") or "")
        code = str(bond.get("债券代码", "") or "")
        ratio = _safe_ratio(bond)
        if ratio <= 0:
            continue
        dur = _calc_bond_duration(code, name)
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

    # === 已持有模式额外指标 ===
    if mode == "hold":
        # 最大回撤
        if nav.df is not None and not nav.df.empty:
            result.max_drawdown = _calc_max_drawdown(nav)

        # 底层机构占比
        result.institution_ratio = _get_institution_ratio(holdings)

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
    """
    安全提取债券占净值比例（小数，如 0.022 表示 2.2%）。

    数据源（fund_bond_holdings 表）的"占净值比例"列统一为百分比格式（如 2.2 = 2.2%），
    需要除以 100 转为小数。
    """
    ratio = float(bond.get("占净值比例", 0) or 0)
    # 统一从百分比转为小数
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
        bm_df = load_bond_composite_index(start, end)
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
