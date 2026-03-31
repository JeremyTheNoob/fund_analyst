"""
指数型-固收基金 专用数据加载器 — fund_quant_v2
负责：日偏离度分析 / 久期估算 / 信用等级对齐 / 调仓损耗监测 / 费率 / 利率环境
"""

from __future__ import annotations
import logging
from datetime import date, timedelta
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import numpy as np

from config import CACHE_TTL
from data_loader.base_api import cached, safe_api_call
from data_loader.akshare_timeout import call_with_timeout

logger = logging.getLogger(__name__)


# ============================================================
# 日偏离度分析（跟踪精度模型）
# ============================================================

def calc_daily_tracking_deviation(
    nav_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    计算每日跟踪偏离度分布（指数型-固收核心指标）。

    债券流动性不如股票，日偏离经常超过 0.05% 说明经理拿货能力或抽样模型待提高。

    Args:
        nav_df: 基金净值 DataFrame，需含 date / ret（或 tr_ret）
        benchmark_df: 基准指数 DataFrame，需含 date / bm_ret

    Returns:
        {
            "daily_deviation": pd.Series,  # 日偏离度序列
            "mean_abs_dev": 0.023,         # 日均偏离绝对值
            "std_dev": 0.035,              # 偏离标准差
            "p50": 0.018,                  # 中位数
            "p90": 0.052,                  # 90%分位
            "p95": 0.068,                  # 95%分位
            "over_threshold_pct": 35.2,    # 超过0.05%阈值的天数占比(%)
            "quality": "优良",             # 质量：极优/优良/一般/较差
            "recent_30d": {...},           # 近30天偏离统计
        }
    """
    result = {
        "daily_deviation": pd.Series(dtype=float),
        "mean_abs_dev": 0.0,
        "std_dev": 0.0,
        "p50": 0.0,
        "p90": 0.0,
        "p95": 0.0,
        "over_threshold_pct": 0.0,
        "quality": "数据不足",
        "recent_30d": {},
    }

    if nav_df is None or nav_df.empty or benchmark_df is None or benchmark_df.empty:
        return result

    # 提取基金收益率
    ret_col = "tr_ret" if "tr_ret" in nav_df.columns else "ret"
    if ret_col not in nav_df.columns:
        return result

    fund_ret = nav_df[["date", ret_col]].dropna().copy()
    fund_ret = fund_ret.rename(columns={ret_col: "fund_ret"})
    fund_ret["date"] = pd.to_datetime(fund_ret["date"])

    # 基准收益率：优先用 bm_ret，否则从 close 计算
    if "bm_ret" in benchmark_df.columns:
        bm_ret = benchmark_df[["date", "bm_ret"]].dropna().copy()
    elif "close" in benchmark_df.columns:
        bm_tmp = benchmark_df[["date", "close"]].dropna().copy()
        bm_tmp["date"] = pd.to_datetime(bm_tmp["date"])
        bm_tmp = bm_tmp.sort_values("date")
        bm_tmp["bm_ret"] = bm_tmp["close"].pct_change().fillna(0)
        bm_ret = bm_tmp[["date", "bm_ret"]].copy()
    else:
        return result

    bm_ret["date"] = pd.to_datetime(bm_ret["date"])

    # 对齐日期
    merged = pd.merge(fund_ret, bm_ret, on="date", how="inner").sort_values("date")

    if len(merged) < 30:
        return result

    # 日偏离 = 基金日收益 - 基准日收益
    merged["deviation"] = merged["fund_ret"] - merged["bm_ret"]
    dev = merged["deviation"]

    result["daily_deviation"] = dev

    abs_dev = dev.abs()
    result["mean_abs_dev"] = round(float(abs_dev.mean()), 6)
    result["std_dev"] = round(float(dev.std()), 6)
    result["p50"] = round(float(abs_dev.quantile(0.50)), 6)
    result["p90"] = round(float(abs_dev.quantile(0.90)), 6)
    result["p95"] = round(float(abs_dev.quantile(0.95)), 6)

    # 超阈值比例（日偏离绝对值 > 0.05% = 0.0005）
    threshold = 0.0005
    result["over_threshold_pct"] = round(float((abs_dev > threshold).mean() * 100), 1)

    # 质量评估
    mad = result["mean_abs_dev"]
    over_pct = result["over_threshold_pct"]
    if mad <= 0.0003 and over_pct <= 20:
        result["quality"] = "极优"
    elif mad <= 0.0005 and over_pct <= 40:
        result["quality"] = "优良"
    elif mad <= 0.001 and over_pct <= 60:
        result["quality"] = "一般"
    else:
        result["quality"] = "较差"

    # 近30天偏离统计
    recent = dev.tail(30)
    result["recent_30d"] = {
        "mean_abs": round(float(recent.abs().mean()), 6),
        "over_threshold_pct": round(float((recent.abs() > threshold).mean() * 100), 1),
        "quality": result["quality"] if len(recent) >= 20 else "数据不足",
    }

    return result


# ============================================================
# 久期估算（基于债券持仓信息）
# ============================================================

def estimate_duration_from_holdings(bond_details: list) -> Dict[str, Any]:
    """
    从债券持仓信息估算组合久期。

    通过债券名称关键词推断期限分布：
    - "1年/1Y/短期" → 1年
    - "3年/3Y/中期" → 3年
    - "5年/5Y/中长期" → 5年
    - "7年/7Y" → 7年
    - "10年/10Y/长期/30年" → 10年
    - "国债XX" → 根据期限推断

    Args:
        bond_details: 债券持仓明细列表

    Returns:
        {
            "estimated_duration": 3.5,
            "duration_range": "中长期",
            "top_holdings": [{"name": "XX", "ratio": 5.2, "est_dur": 10}, ...],
            "duration_distribution": {"短端(<=1Y)": 10%, "中端(1-3Y)": 30%, ...},
        }
    """
    result = {
        "estimated_duration": 0.0,
        "duration_range": "未知",
        "top_holdings": [],
        "duration_distribution": {},
    }

    if not bond_details:
        return result

    duration_map = {
        "短端(<=1Y)": 0.0,
        "中短端(1-3Y)": 0.0,
        "中端(3-5Y)": 0.0,
        "中长端(5-7Y)": 0.0,
        "长端(7-10Y)": 0.0,
        "超长端(>10Y)": 0.0,
    }

    total_weighted_dur = 0.0
    total_weight = 0.0
    analyzed = []

    for bond in bond_details[:30]:  # 取前30只
        name = str(bond.get("债券名称", ""))
        ratio = float(bond.get("占净值比例", 0))

        est_dur = _infer_bond_duration(name)

        total_weighted_dur += est_dur * ratio
        total_weight += ratio

        # 分类
        if est_dur <= 1:
            duration_map["短端(<=1Y)"] += ratio
        elif est_dur <= 3:
            duration_map["中短端(1-3Y)"] += ratio
        elif est_dur <= 5:
            duration_map["中端(3-5Y)"] += ratio
        elif est_dur <= 7:
            duration_map["中长端(5-7Y)"] += ratio
        elif est_dur <= 10:
            duration_map["长端(7-10Y)"] += ratio
        else:
            duration_map["超长端(>10Y)"] += ratio

        analyzed.append({"name": name, "ratio": ratio, "est_dur": est_dur})

    if total_weight > 0:
        result["estimated_duration"] = round(total_weighted_dur / total_weight, 2)

    # 久期范围
    d = result["estimated_duration"]
    if d <= 1.5:
        result["duration_range"] = "短久期"
    elif d <= 3.5:
        result["duration_range"] = "中短久期"
    elif d <= 5.5:
        result["duration_range"] = "中久期"
    elif d <= 8.0:
        result["duration_range"] = "中长久期"
    else:
        result["duration_range"] = "长久期"

    # 持仓按权重降序
    analyzed.sort(key=lambda x: x["ratio"], reverse=True)
    result["top_holdings"] = analyzed[:10]

    # 分布转为百分比
    if total_weight > 0:
        result["duration_distribution"] = {
            k: round(v / total_weight * 100, 1)
            for k, v in duration_map.items()
            if v > 0
        }

    return result


def _infer_bond_duration(bond_name: str) -> float:
    """从债券名称推断期限"""
    name = bond_name.upper()

    # 明确期限关键词
    import re
    m = re.search(r"(\d+)\s*[年Y]", bond_name)
    if m:
        return min(float(m.group(1)), 30.0)

    # 债券类型推断
    if any(kw in name for kw in ["存单", "CD", "同业"]):
        return 0.5
    if any(kw in name for kw in ["超短", "短融"]):
        return 0.8
    if any(kw in name for kw in ["短债", "1年"]):
        return 1.0
    if any(kw in name for kw in ["中短", "2年"]):
        return 2.0
    if any(kw in name for kw in ["3年"]):
        return 3.0
    if any(kw in name for kw in ["5年"]):
        return 5.0
    if any(kw in name for kw in ["7年"]):
        return 7.0
    if any(kw in name for kw in ["10年", "长债", "长期"]):
        return 10.0
    if any(kw in name for kw in ["30年", "超长期"]):
        return 20.0
    if any(kw in name for kw in ["国开", "进出口", "农发"]):
        return 5.0  # 政金债默认5年
    if any(kw in name for kw in ["国债"]):
        return 7.0  # 国债默认7年
    if any(kw in name for kw in ["城投", "企业"]):
        return 3.0  # 信用债默认3年

    return 3.0  # 默认


# ============================================================
# 信用等级对齐（信用下沉检测）
# ============================================================

def analyze_credit_alignment(
    bond_details: list,
    benchmark_name: str = "",
) -> Dict[str, Any]:
    """
    分析基金信用等级与目标指数的对齐程度。

    如果目标指数是国债/政金债，而基金偷偷买了信用债，
    这就是信用下沉套利，风险属性已改变。

    Args:
        bond_details: 债券持仓明细
        benchmark_name: 业绩比较基准名称

    Returns:
        {
            "gov_ratio": 65.2,     # 利率债占比
            "policy_ratio": 20.1,  # 政金债占比
            "credit_ratio": 12.5,  # 信用债占比
            "urban_ratio": 2.2,    # 城投债占比
            "is_credit_downgrade": False,
            "credit_downgrade_detail": "",
            "benchmark_type": "政金债",
        }
    """
    result = {
        "gov_ratio": 0.0,
        "policy_ratio": 0.0,
        "credit_ratio": 0.0,
        "urban_ratio": 0.0,
        "real_estate_ratio": 0.0,
        "is_credit_downgrade": False,
        "credit_downgrade_detail": "",
        "benchmark_type": "未知",
    }

    if not bond_details:
        return result

    total_ratio = sum(float(b.get("占净值比例", 0)) for b in bond_details)
    if total_ratio == 0:
        return result

    gov = 0.0
    policy = 0.0
    urban = 0.0
    real_estate = 0.0
    credit = 0.0
    ratio = 0.0

    for bond in bond_details:
        name = str(bond.get("债券名称", "")).upper()
        try:
            ratio = float(bond.get("占净值比例", 0))
        except (ValueError, TypeError):
            ratio = 0.0

        if any(kw in name for kw in ["国债"]):
            gov += ratio
        elif any(kw in name for kw in ["国开", "进出口", "农发", "政金", "央票"]):
            policy += ratio
        elif any(kw in name for kw in ["城投", "城建", "城控", "城发"]):
            urban += ratio
        elif any(kw in name for kw in ["地产", "置业", "房产"]):
            real_estate += ratio
        else:
            credit += ratio

    result["gov_ratio"] = round(gov / total_ratio * 100, 1)
    result["policy_ratio"] = round(policy / total_ratio * 100, 1)
    result["credit_ratio"] = round(credit / total_ratio * 100, 1)
    result["urban_ratio"] = round(urban / total_ratio * 100, 1)
    result["real_estate_ratio"] = round(real_estate / total_ratio * 100, 1)

    # 判断基准类型
    bm_lower = (benchmark_name or "").lower()
    if any(kw in bm_lower for kw in ["国债", "利率"]):
        result["benchmark_type"] = "国债"
    elif any(kw in bm_lower for kw in ["政金", "国开", "政策性"]):
        result["benchmark_type"] = "政金债"
    elif any(kw in bm_lower for kw in ["信用", "企业", "公司"]):
        result["benchmark_type"] = "信用债"
    elif any(kw in bm_lower for kw in ["综合", "财富", "总"]):
        result["benchmark_type"] = "综合指数"
    else:
        result["benchmark_type"] = "未知"

    # 信用下沉检测
    non_rate_ratio = result["credit_ratio"] + result["urban_ratio"] + result["real_estate_ratio"]

    if result["benchmark_type"] in ("国债", "政金债"):
        if non_rate_ratio > 10:
            result["is_credit_downgrade"] = True
            result["credit_downgrade_detail"] = (
                f"目标指数为{result['benchmark_type']}类，但基金持仓中非利率债占比达 {non_rate_ratio:.1f}%，"
                f"存在信用下沉套利嫌疑，风险属性已偏离指数定位。"
            )
    elif result["benchmark_type"] == "综合指数":
        # 综合指数允许一定信用债，但超过30%需预警
        if non_rate_ratio > 30:
            result["is_credit_downgrade"] = True
            result["credit_downgrade_detail"] = (
                f"作为跟踪综合指数的基金，信用债+城投债占比 {non_rate_ratio:.1f}% 偏高，"
                f"可能导致跟踪偏离加大。"
            )

    return result


# ============================================================
# 调仓损耗监测
# ============================================================

def monitor_rebalance_loss(
    nav_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    监测指数调仓期间的跟踪偏离放大（流动性冲击指标）。

    指数债券基金在6月和12月定期调仓时，如果偏离度异常放大，
    说明调仓面临流动性问题。

    Args:
        nav_df: 基金净值 DataFrame
        benchmark_df: 基准 DataFrame

    Returns:
        {
            "rebalance_windows": [...],
            "avg_dev_normal": 0.02,
            "avg_dev_rebalance": 0.08,
            "rebalance_penalty": 0.06,
            "liquidity_risk": "low",
        }
    """
    result = {
        "rebalance_windows": [],
        "avg_dev_normal": 0.0,
        "avg_dev_rebalance": 0.0,
        "rebalance_penalty": 0.0,
        "liquidity_risk": "unknown",
    }

    if nav_df is None or nav_df.empty or benchmark_df is None or benchmark_df.empty:
        return result

    ret_col = "tr_ret" if "tr_ret" in nav_df.columns else "ret"
    if ret_col not in nav_df.columns:
        return result

    fund_ret = nav_df[["date", ret_col]].dropna().copy()
    fund_ret = fund_ret.rename(columns={ret_col: "fund_ret"})
    fund_ret["date"] = pd.to_datetime(fund_ret["date"])

    # 基准收益率：优先用 bm_ret，否则从 close 计算
    if "bm_ret" in benchmark_df.columns:
        bm_ret = benchmark_df[["date", "bm_ret"]].dropna().copy()
    elif "close" in benchmark_df.columns:
        bm_tmp = benchmark_df[["date", "close"]].dropna().copy()
        bm_tmp["date"] = pd.to_datetime(bm_tmp["date"])
        bm_tmp = bm_tmp.sort_values("date")
        bm_tmp["bm_ret"] = bm_tmp["close"].pct_change().fillna(0)
        bm_ret = bm_tmp[["date", "bm_ret"]].copy()
    else:
        return result

    bm_ret["date"] = pd.to_datetime(bm_ret["date"])

    merged = pd.merge(fund_ret, bm_ret, on="date", how="inner").sort_values("date")
    if len(merged) < 60:
        return result

    merged["deviation"] = (merged["fund_ret"] - merged["bm_ret"]).abs()

    # 识别调仓窗口（6月和12月，前后各5个交易日）
    merged["month"] = merged["date"].dt.month
    rebalance_mask = merged["month"].isin([5, 6, 7, 11, 12, 1])
    normal_mask = ~rebalance_mask

    if normal_mask.any():
        result["avg_dev_normal"] = round(float(merged.loc[normal_mask, "deviation"].mean()) * 100, 4)

    if rebalance_mask.any():
        rebalance_dev = merged.loc[rebalance_mask, "deviation"]
        result["avg_dev_rebalance"] = round(float(rebalance_dev.mean()) * 100, 4)

        # 按调仓窗口分组
        merged["period"] = merged["date"].dt.to_period("M")
        for period, grp in merged.loc[rebalance_mask].groupby("period"):
            avg_dev = round(float(grp["deviation"].mean()) * 100, 4)
            result["rebalance_windows"].append({
                "period": str(period),
                "avg_deviation": avg_dev,
                "trading_days": len(grp),
            })

    # 调仓惩罚（调仓期偏离 - 正常期偏离）
    result["rebalance_penalty"] = round(
        max(result["avg_dev_rebalance"] - result["avg_dev_normal"], 0), 4
    )

    # 流动性风险评估
    penalty = result["rebalance_penalty"]
    if penalty >= 0.05:
        result["liquidity_risk"] = "high"
    elif penalty >= 0.02:
        result["liquidity_risk"] = "medium"
    elif result["avg_dev_normal"] > 0:
        result["liquidity_risk"] = "low"
    else:
        result["liquidity_risk"] = "unknown"

    return result


# ============================================================
# 费率加载（复用 index_stock_loader）
# ============================================================

def load_idx_bond_fee(symbol: str) -> Dict[str, float]:
    """加载指数型-固收基金费率"""
    from data_loader.index_stock_loader import load_fund_fee_detail
    return load_fund_fee_detail(symbol)


# ============================================================
# 票息覆盖率分析
# ============================================================

def analyze_coupon_coverage(
    ann_return: float,
    ter: float,
    y10y: float = None,
) -> Dict[str, Any]:
    """
    票息覆盖率分析：费率占收益的比重。

    债券收益本身就薄（如3%），如果费率占到了0.5%，
    相当于1/6的收益被吃掉。

    Args:
        ann_return: 基金年化收益率（小数，如0.03）
        ter: 综合费率（小数，如0.005）
        y10y: 当前10年期国债收益率（小数，如0.025）

    Returns:
        {
            "fee_to_return_ratio": 16.7,
            "fee_eat_pct": "约1/6的收益被费率吃掉",
            "coupon_income_estimate": 3.0,
            "net_yield": 2.5,
            "assessment": "费率侵蚀较轻",
        }
    """
    result = {
        "fee_to_return_ratio": 0.0,
        "fee_eat_pct": "",
        "coupon_income_estimate": ann_return * 100 if ann_return else 0,
        "net_yield": 0.0,
        "assessment": "数据不足",
    }

    if not ann_return or ann_return <= 0:
        return result

    # 费率占收益比
    ratio = 0.0
    if ter > 0 and ann_return > 0:
        ratio = ter / ann_return * 100
        result["fee_to_return_ratio"] = round(ratio, 1)

        # 直观描述
        if ratio >= 20:
            result["fee_eat_pct"] = f"约1/5的收益被费率吃掉"
        elif ratio >= 16.7:
            result["fee_eat_pct"] = f"约1/6的收益被费率吃掉"
        elif ratio >= 14.3:
            result["fee_eat_pct"] = f"约1/7的收益被费率吃掉"
        elif ratio >= 12.5:
            result["fee_eat_pct"] = f"约1/8的收益被费率吃掉"
        elif ratio >= 10:
            result["fee_eat_pct"] = f"约1/10的收益被费率吃掉"
        else:
            result["fee_eat_pct"] = f"费率侵蚀较小（{ratio:.1f}%）"

    result["net_yield"] = round((ann_return - ter) * 100, 2)

    # 评估
    if ratio <= 10:
        result["assessment"] = "费率侵蚀较轻，性价比高"
    elif ratio <= 16.7:
        result["assessment"] = "费率侵蚀中等，可接受"
    elif ratio <= 25:
        result["assessment"] = "费率侵蚀较重，长期持有需关注"
    else:
        result["assessment"] = "费率侵蚀严重，建议选择费率更低的产品"

    return result


# ============================================================
# 收益/费用比（费率侵蚀模型增强版）
# ============================================================

def build_fee_erosion_model(
    ann_return: float,
    ter: float,
    hold_years: int = 5,
) -> Dict[str, Any]:
    """
    费率侵蚀模型：不同持有年限下费率对收益的侵蚀。

    Args:
        ann_return: 年化收益率（小数）
        ter: 综合费率（小数）
        hold_years: 持有年限

    Returns:
        {
            "annual_return_gross": 3.0,
            "annual_return_net": 2.5,
            "total_gross": 15.9,
            "total_net": 13.1,
            "total_fee_drag": 2.8,
            "fee_drag_pct": 17.6,
        }
    """
    gross_ann = round(ann_return * 100, 2) if ann_return else 0
    net_ann = round((ann_return - ter) * 100, 2) if ann_return else 0
    gross_total = round(((1 + ann_return) ** hold_years - 1) * 100, 2) if ann_return else 0
    net_total = round(((1 + ann_return - ter) ** hold_years - 1) * 100, 2) if ann_return and ann_return > ter else 0
    fee_drag = round(gross_total - net_total, 2)
    fee_drag_pct = round(fee_drag / gross_total * 100, 1) if gross_total > 0 else 0

    return {
        "annual_return_gross": gross_ann,
        "annual_return_net": net_ann,
        "total_gross": gross_total,
        "total_net": net_total,
        "total_fee_drag": fee_drag,
        "fee_drag_pct": fee_drag_pct,
        "hold_years": hold_years,
    }


# ============================================================
# 利率环境 + 10年国债收益率技术分析
# ============================================================

def load_y10y_technical_analysis(lookback_years: int = 3) -> Dict[str, Any]:
    """
    加载10年国债收益率数据进行技术分析。

    Returns:
        {
            "current_y10y": 2.35,
            "y10y_percentile": 15.2,
            "y10y_trend": "down",
            "ma20": 2.38,
            "ma60": 2.42,
            "golden_cross": False,      # 金叉
            "death_cross": True,        # 死叉
            "higher_highs": False,      # 高点抬高
            "lower_lows": True,         # 低点降低
            "pattern": "下行通道",
            "chart_df": pd.DataFrame,
        }
    """
    result = {
        "current_y10y": None,
        "y10y_percentile": None,
        "y10y_trend": "unknown",
        "ma20": None,
        "ma60": None,
        "golden_cross": False,
        "death_cross": False,
        "higher_highs": False,
        "lower_lows": False,
        "pattern": "数据不足",
        "chart_df": pd.DataFrame(),
    }

    try:
        from data_loader.bond_loader import load_rate_environment, load_multi_tenor_yields
        from datetime import date as _date

        today = _date.today()
        end = today.strftime("%Y%m%d")
        start = (today - timedelta(days=365 * lookback_years)).strftime("%Y%m%d")

        # 多期限数据
        df = load_multi_tenor_yields(start, end)
        if df.empty or "y10y" not in df.columns:
            return result

        df = df.set_index("date").sort_index()
        y10y = df["y10y"].dropna()

        if len(y10y) < 60:
            return result

        result["current_y10y"] = round(float(y10y.iloc[-1]), 3)
        result["y10y_percentile"] = round(float((y10y < result["current_y10y"]).mean() * 100), 1)

        # 均线
        ma20 = y10y.rolling(20).mean().iloc[-1]
        ma60 = y10y.rolling(60).mean().iloc[-1]
        result["ma20"] = round(float(ma20), 3) if pd.notna(ma20) else None
        result["ma60"] = round(float(ma60), 3) if pd.notna(ma60) else None

        # 金叉/死叉（20日线上穿/下穿60日线）
        if len(y10y) >= 61:
            ma20_series = y10y.rolling(20).mean()
            ma60_series = y10y.rolling(60).mean()
            # 最近一个交叉
            diff = ma20_series - ma60_series
            if len(diff) >= 2:
                prev_diff = diff.iloc[-2]
                curr_diff = diff.iloc[-1]
                if pd.notna(prev_diff) and pd.notna(curr_diff):
                    if prev_diff <= 0 and curr_diff > 0:
                        result["golden_cross"] = True
                    elif prev_diff >= 0 and curr_diff < 0:
                        result["death_cross"] = True

        # 高低点分析
        if len(y10y) >= 120:
            recent = y10y.tail(120)
            # 找局部高点（前后10日最高）
            peaks = []
            troughs = []
            for i in range(10, len(recent) - 10):
                window = recent.iloc[i-10:i+11]
                if recent.iloc[i] == window.max():
                    peaks.append(float(recent.iloc[i]))
                if recent.iloc[i] == window.min():
                    troughs.append(float(recent.iloc[i]))

            if len(peaks) >= 2:
                result["higher_highs"] = peaks[-1] > peaks[-2]
            if len(troughs) >= 2:
                result["lower_lows"] = troughs[-1] < troughs[-2]

        # 图表形态判断
        cur = result["current_y10y"]
        m20 = result["ma20"]
        m60 = result["ma60"]

        if m20 is not None and m60 is not None:
            if cur < m20 < m60 and result["death_cross"]:
                result["pattern"] = "下行通道（空头排列，死叉确认）"
                result["y10y_trend"] = "down"
            elif cur > m20 > m60 and result["golden_cross"]:
                result["pattern"] = "上行通道（多头排列，金叉确认）"
                result["y10y_trend"] = "up"
            elif cur < m20 < m60:
                result["pattern"] = "下行通道（空头排列）"
                result["y10y_trend"] = "down"
            elif cur > m20 > m60:
                result["pattern"] = "上行通道（多头排列）"
                result["y10y_trend"] = "up"
            elif m20 > m60:
                result["pattern"] = "偏多震荡"
                result["y10y_trend"] = "up_flat"
            else:
                result["pattern"] = "偏空震荡"
                result["y10y_trend"] = "down_flat"

        # 高低点辅助判断
        if result["lower_lows"] and not result["higher_highs"]:
            result["pattern"] += " · Lower Highs & Lower Lows"
        elif result["higher_highs"] and not result["lower_lows"]:
            result["pattern"] += " · Higher Highs & Higher Lows"

        # 供图表使用的数据
        chart_df = y10y.reset_index()
        chart_df.columns = ["date", "y10y"]
        chart_df["ma20"] = y10y.rolling(20).mean().values
        chart_df["ma60"] = y10y.rolling(60).mean().values
        result["chart_df"] = chart_df

    except Exception as e:
        logger.warning(f"[load_y10y_technical_analysis] 失败: {e}")

    return result


# ============================================================
# YTM（到期收益率）估算
# ============================================================

def estimate_portfolio_ytm(
    duration_analysis: Dict[str, Any],
    credit_analysis: Dict[str, Any],
    y10y: float = None,
) -> Dict[str, Any]:
    """
    估算组合到期收益率（YTM）。

    基于久期和信用结构估算：YTM ≈ 无风险利率 + 信用利差

    Args:
        duration_analysis: estimate_duration_from_holdings 返回值
        credit_analysis: analyze_credit_alignment 返回值
        y10y: 当前10年期国债收益率

    Returns:
        {
            "estimated_ytm": 3.2,
            "risk_free_component": 2.5,
            "credit_spread_component": 0.5,
            "duration_premium": 0.2,
            "ytm_assessment": "中等水平",
        }
    """
    result = {
        "estimated_ytm": None,
        "risk_free_component": 0.0,
        "credit_spread_component": 0.0,
        "duration_premium": 0.0,
        "ytm_assessment": "数据不足",
    }

    if y10y is None:
        return result

    dur = duration_analysis.get("estimated_duration", 0)
    credit_ratio = credit_analysis.get("credit_ratio", 0) / 100

    # 无风险利率（根据久期调整：短久期用2Y，长久期用10Y）
    if dur <= 2:
        result["risk_free_component"] = round(y10y - 0.5, 3)  # 假设2Y比10Y低50bp
    elif dur <= 5:
        result["risk_free_component"] = round(y10y - 0.2, 3)
    else:
        result["risk_free_component"] = round(y10y, 3)

    # 信用利差（信用债占比 × 经验利差）
    credit_spread = credit_ratio * 0.8  # 信用债平均利差约80bp
    result["credit_spread_component"] = round(credit_spread, 3)

    # 久期溢价（长久期补偿）
    if dur > 5:
        dur_premium = (dur - 5) * 0.05
    else:
        dur_premium = 0
    result["duration_premium"] = round(dur_premium, 3)

    result["estimated_ytm"] = round(
        result["risk_free_component"]
        + result["credit_spread_component"]
        + result["duration_premium"],
        2,
    )

    # YTM 水位评估
    ytm = result["estimated_ytm"]
    if ytm >= 4.0:
        result["ytm_assessment"] = "收益水平较高，具有配置价值"
    elif ytm >= 3.0:
        result["ytm_assessment"] = "收益水平中等"
    elif ytm >= 2.0:
        result["ytm_assessment"] = "收益水平偏低"
    else:
        result["ytm_assessment"] = "收益水平极低，性价比不足"

    return result


# ============================================================
# 指数型-固收指数名称映射
# ============================================================

BOND_INDEX_NAME_MAP = {
    "H11001": "中债-综合指数",
    "CBA00101": "中债-国债总指数",
    "CBA00801": "中债-信用债总指数",
    "CBA02501": "中债-高信用等级债券指数",
    "000012": "中债综合指数",
    "000013": "中债总财富指数",
    "000015": "中债国债总指数",
    "000016": "中债信用债总指数",
}


def get_bond_index_name(code: str) -> str:
    """获取债券指数中文名称"""
    short = code.replace(".CSI", "").replace(".SH", "").replace(".SZ", "")
    return BOND_INDEX_NAME_MAP.get(short, f"债券指数{short}")


# ============================================================
# 共享：10年国债收益率专题报告（bond_long / idx_bond 复用）
# ============================================================

def generate_y10y_rate_topic(rate_analysis: Dict[str, Any]) -> str:
    """
    生成10年国债收益率分析专题文本（Markdown）。

    用于 bond_long（债券型-长债）和 idx_bond（指数型-固收）的利率专题章节。

    Args:
        rate_analysis: load_y10y_technical_analysis() 返回值

    Returns:
        Markdown 文本，包含 [INSERT_CHART: Y10Y_TREND] 图表标记
    """
    lines = []

    current_y10y = rate_analysis.get("current_y10y")
    y10y_pct = rate_analysis.get("y10y_percentile")
    ma20 = rate_analysis.get("ma20")
    ma60 = rate_analysis.get("ma60")
    pattern = rate_analysis.get("pattern", "数据不足")
    death_cross = rate_analysis.get("death_cross", False)
    golden_cross = rate_analysis.get("golden_cross", False)
    higher_highs = rate_analysis.get("higher_highs", False)
    lower_lows = rate_analysis.get("lower_lows", False)

    if current_y10y is None:
        return "10年国债收益率数据加载失败，无法进行技术分析。"

    lines.extend([
        "",
        "---",
        "",
        "### 📈 10年国债收益率分析",
        "",
        f"**当前 10Y 收益率**：**{current_y10y:.3f}%**（历史分位 {y10y_pct or 0:.1f}%）",
    ])

    if ma20 is not None:
        lines.append(f"20日均线：{ma20:.3f}%")
    if ma60 is not None:
        lines.append(f"60日均线：{ma60:.3f}%")

    lines.extend([
        "",
        f"**技术面形态**：{pattern}",
        "",
    ])

    # 均线系统
    if ma20 is not None and ma60 is not None:
        lines.extend([
            "**均线系统**：",
            "",
        ])
        if death_cross:
            lines.append(
                f"- 20日线（{ma20:.3f}%）**下穿** 60日线（{ma60:.3f}%），形成**死叉**，"
                f"通常确认下行趋势加剧。"
            )
        elif golden_cross:
            lines.append(
                f"- 20日线（{ma20:.3f}%）**上穿** 60日线（{ma60:.3f}%），形成**金叉**，"
                f"通常确认上行趋势启动。"
            )
        else:
            lines.append(
                f"- 20日线（{ma20:.3f}%）与 60日线（{ma60:.3f}%）暂无交叉信号。"
            )

    # 高低点分析
    if higher_highs or lower_lows:
        lines.extend(["", "**高低点转换（Price Action）**：", ""])
        if lower_lows and not higher_highs:
            lines.append(
                "- 收益率呈现 **Lower Highs & Lower Lows**，"
                "每一个新高点比前低、每一个新低点也更低，典型下行通道。"
            )
        elif higher_highs and not lower_lows:
            lines.append(
                "- 收益率呈现 **Higher Highs & Higher Lows**，"
                "高点不断抬升、低点也不断抬升，典型上行通道。"
            )

    lines.extend([
        "",
        "### 🏛️ 基本面判断：驱动力分析",
        "",
        "**货币政策（核心指标）**",
        "",
        "观察 MLF（中期借贷便利）和 LPR（贷款市场报价利率）：",
        "- 若央行处于降息周期或频繁下调存款准备金率（RRR），10年期国债收益率通常处于下行通道",
        "- 若 MLF/LPR 保持稳定或上调，收益率可能企稳或上行",
        "",
        "**通胀与经济数据**",
        "",
        "| 信号类型 | 指标 | 对收益率影响 |",
        "|----------|------|-------------|",
        "| 下行信号 | CPI/PPI 疲软，PMI < 50 | 收益率下行（债市牛） |",
        "| 上行信号 | 通胀抬头，社融超预期 | 收益率上行（债市熊） |",
        "",
        "### 💭 情绪面与资金面",
        "",
        "**存单利率（NCD）**",
        "- 观察 1 年期同业存单利率",
        "- 存单利率持续下行 = 银行体系资金充裕 = 向下引导 10Y 收益率",
        "",
        "**股债跷跷板**",
        "- 股市持续放量上涨时，10Y 收益率往往上行（资金从债市流向股市）",
        "- 股市阴跌时，债市通常是避风港（资金涌入债市，收益率下行）",
        "",
        "[INSERT_CHART: Y10Y_TREND]",
    ])

    return "\n".join(lines)
