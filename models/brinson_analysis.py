"""
Brinson归因分析模块
用于分析混合型基金/资产组合的收益来源（配置效应+选择效应+交互效应）
基于Brinson-Hood-Beebower模型
"""

import pandas as pd
import numpy as np


def run_brinson(
    fund_ret: pd.Series,
    stock_ratio: float,
    bond_ratio: float,
    benchmark_ret: pd.Series,
    benchmark_stock_weight: float = 0.6,
) -> dict:
    """
    Brinson归因（配置效应+选择效应+交互效应）

    Args:
        fund_ret: 基金日收益率序列（index为日期）
        stock_ratio: 基金平均股票仓位（0-1）
        bond_ratio: 基金平均债券仓位（0-1）
        benchmark_ret: 基准日收益率序列（index为日期）
        benchmark_stock_weight: 基准股票权重（默认0.6）

    Returns:
        归因结果字典：
        - allocation: 配置效应（年化收益率）
        - selection: 选择效应（年化收益率）
        - interaction: 交互效应（年化收益率）
        - excess_return: 总超额收益（年化）
        - fund_annual: 基金年化收益率
        - benchmark_annual: 基准年化收益率
        - interpretation: 文字解读
    """
    # ========== 1. 数据有效性检查 ==========
    if fund_ret.empty or benchmark_ret.empty:
        return {
            'allocation': 0.0,
            'selection': 0.0,
            'interaction': 0.0,
            'excess_return': 0.0,
            'fund_annual': 0.0,
            'benchmark_annual': 0.0,
            'interpretation': '数据不足，无法进行Brinson归因',
        }

    # 计算基准债券权重
    benchmark_bond_weight = 1.0 - benchmark_stock_weight

    # ========== 2. 数据对齐与年化收益率计算 ==========
    merged = pd.DataFrame({
        'fund_ret': fund_ret,
        'benchmark_ret': benchmark_ret
    }).dropna()

    if len(merged) < 60:  # 至少需要60天数据
        return {
            'allocation': 0.0,
            'selection': 0.0,
            'interaction': 0.0,
            'excess_return': 0.0,
            'fund_annual': 0.0,
            'benchmark_annual': 0.0,
            'interpretation': f'数据不足({len(merged)}天<60天)，无法进行Brinson归因',
        }

    # 计算年化收益率
    fund_daily_mean = merged['fund_ret'].mean()
    benchmark_daily_mean = merged['benchmark_ret'].mean()

    fund_annual = (1 + fund_daily_mean) ** 252 - 1
    benchmark_annual = (1 + benchmark_daily_mean) ** 252 - 1

    # 超额收益（年化）
    excess_return = fund_annual - benchmark_annual

    # ========== 3. 配置效应 ==========
    # 配置效应 = 基金与基准的资产配置差异导致的收益差异
    # Allocation = (基金股票权重 - 基准股票权重) * 股票收益率
    #           + (基金债券权重 - 基准债券权重) * 债券收益率

    # 简化假设：股票和债券的年化收益率分别等于基准年化收益率
    # 在完整实现中，应该用股票指数和债券指数的收益率

    # 假设股票年化收益率 = 基准年化收益率 / 股票权重（简化）
    stock_annual = benchmark_annual / benchmark_stock_weight if benchmark_stock_weight > 0 else benchmark_annual * 1.5
    bond_annual = 0.03  # 假设债券年化收益率为3%（中国长期国债收益率）

    allocation = ((stock_ratio - benchmark_stock_weight) * stock_annual +
                  (bond_ratio - benchmark_bond_weight) * bond_annual)

    # ========== 4. 选择效应 ==========
    # 选择效应 = 基金选股/择债能力带来的收益差异
    # Selection = 基金股票权重 * (基金股票收益 - 股票基准收益)
    #           + 基金债券权重 * (基金债券收益 - 债券基准收益)

    # 简化假设：基金股票收益 = 基金整体收益 / 股票权重
    if stock_ratio > 0:
        fund_stock_return = fund_annual / stock_ratio
        selection = stock_ratio * (fund_stock_return - stock_annual)
    else:
        selection = 0.0

    # ========== 5. 交互效应 ==========
    # 交互效应 = 配置效应与选择效应的交叉项
    # Interaction = (基金股票权重 - 基准股票权重) * (基金股票收益 - 股票基准收益)
    #            + (基金债券权重 - 基准债券权重) * (基金债券收益 - 债券基准收益)

    if stock_ratio > 0 and benchmark_stock_weight > 0:
        fund_stock_return = fund_annual / stock_ratio
        interaction = ((stock_ratio - benchmark_stock_weight) * (fund_stock_return - stock_annual) +
                       (bond_ratio - benchmark_bond_weight) * 0)  # 债券部分简化为0
    else:
        interaction = 0.0

    # ========== 6. 文字解读 ==========
    interpretation = _interpret_brinson(allocation, selection, interaction, excess_return)

    return {
        'allocation': allocation,
        'selection': selection,
        'interaction': interaction,
        'excess_return': excess_return,
        'fund_annual': fund_annual,
        'benchmark_annual': benchmark_annual,
        'interpretation': interpretation,
    }


def _interpret_brinson(
    allocation: float,
    selection: float,
    interaction: float,
    excess_return: float,
) -> str:
    """
    解读Brinson归因结果，生成大白话解释

    Args:
        allocation: 配置效应（年化）
        selection: 选择效应（年化）
        interaction: 交互效应（年化）
        excess_return: 总超额收益（年化）

    Returns:
        文字解读字符串
    """
    parts = []

    # 配置效应解读
    if abs(allocation) > 0.005:  # 大于0.5%
        if allocation > 0:
            if stock_ratio > benchmark_stock_weight:
                parts.append(f"✅ 配置效应+{allocation*100:+.1f}%：多配股票踩对了节奏")
            else:
                parts.append(f"✅ 配置效应+{allocation*100:+.1f}%：低配股票规避了风险")
        else:
            if stock_ratio > benchmark_stock_weight:
                parts.append(f"❌ 配置效应{allocation*100:+.1f}%：高位多配股票拖累收益")
            else:
                parts.append(f"❌ 配置效应{allocation*100:+.1f}%：低配股票错过了行情")

    # 选择效应解读
    if abs(selection) > 0.005:
        if selection > 0:
            parts.append(f"✅ 选股能力+{selection*100:+.1f}%：精选个股跑出超额收益")
        else:
            parts.append(f"❌ 选股能力{selection*100:+.1f}%：个股选择拖累整体表现")

    # 交互效应解读
    if abs(interaction) > 0.005:
        if interaction > 0:
            parts.append(f"✅ 交互效应+{interaction*100:+.1f}%：配置与选股形成正向共振")
        else:
            parts.append(f"⚠️ 交互效应{interaction*100:+.1f}%：配置与选股存在相互抵消")

    # 总体评价
    if excess_return > 0.05:  # 超额收益>5%
        parts.insert(0, "🎯 总体评价：显著跑赢基准，综合能力出色")
    elif excess_return > 0.02:  # 超额收益>2%
        parts.insert(0, "👍 总体评价：小幅跑赢基准，能力尚可")
    elif excess_return < -0.02:  # 超额收益<-2%
        parts.insert(0, "⚠️ 总体评价：跑输基准，需要改进投资策略")
    else:
        parts.insert(0, "📊 总体评价：与基准基本持平，缺乏超额收益")

    return '\n'.join(parts)


def calculate_radar_scores_brinson(
    allocation: float,
    selection: float,
    excess_return: float,
    fund_annual: float,
) -> dict:
    """
    基于Brinson归因结果计算雷达图评分

    Args:
        allocation: 配置效应（年化）
        selection: 选择效应（年化）
        excess_return: 总超额收益（年化）
        fund_annual: 基金年化收益率

    Returns:
        雷达图评分字典（0-100）
    """
    from utils.helpers import normalize_score

    # 1. 配置能力评分
    alloc_score = normalize_score(allocation, -0.10, 0.10)

    # 2. 选股能力评分
    select_score = normalize_score(selection, -0.10, 0.10)

    # 3. 超额能力评分
    excess_score = normalize_score(excess_return, -0.10, 0.15)

    # 4. 绝对收益评分
    abs_score = normalize_score(fund_annual, 0.0, 0.20)

    return {
        '配置能力': alloc_score,
        '选股能力': select_score,
        '超额能力': excess_score,
        '绝对收益': abs_score,
    }
