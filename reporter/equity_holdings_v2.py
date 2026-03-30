"""
深度持仓穿透分析模块 — 权益类基金
负责：资产配置趋势 / 持仓演变 / 交易能力评估 / 估值分析与风险预警

替代原有 holdings_analyzer.py 的权益类分析功能
"""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from data_loader.equity_holdings_loader import (
    HoldingsHistoryData,
    AssetStructureData,
    load_holdings_analysis_data
)
from data_loader.sw_industry_loader import get_stock_industry, get_stock_industry_by_name
from utils.common import audit_logger

logger = logging.getLogger(__name__)


# ============================================================
# 模块1：资产配置趋势分析
# ============================================================

@audit_logger
def analyze_asset_trend(asset_structure_data: AssetStructureData) -> Dict[str, Any]:
    """
    分析资产配置演变趋势

    Args:
        asset_structure_data: 资产结构数据

    Returns:
        {
            "chart_data": {...},           # 图表数据（用于可视化）
            "stock_avg_ratio": float,       # 股票仓位中枢（平均值）
            "stock_std_ratio": float,       # 股票仓位标准差
            "cash_percentile": float,       # 现金仓位历史分位数（最新值）
            "cash_percentile_desc": str,    # 分位数描述
            "style_drift_warning": str,     # 风格漂移预警（如果有）
        }
    """
    df = asset_structure_data.df.copy()

    if df.empty:
        logger.warning("[analyze_asset_trend] 资产结构数据为空")
        return {
            "chart_data": {},
            "stock_avg_ratio": 0.0,
            "stock_std_ratio": 0.0,
            "cash_percentile": 0.0,
            "cash_percentile_desc": "数据不足",
            "style_drift_warning": "数据不足，无法分析"
        }

    # 计算股票仓位中枢和波动
    stock_avg = df["股票"].mean()
    stock_std = df["股票"].std()

    # 计算现金仓位历史分位数
    cash_latest = df["现金"].iloc[-1] if len(df) > 0 else 0.0
    cash_percentile = (df["现金"] < cash_latest).mean() * 100 if len(df) > 1 else 0.0

    # 分位数描述
    if cash_percentile >= 90:
        cash_desc = "历史高位（极度保守）"
    elif cash_percentile >= 75:
        cash_desc = "历史较高位（偏保守）"
    elif cash_percentile >= 50:
        cash_desc = "历史中位（中性）"
    elif cash_percentile >= 25:
        cash_desc = "历史较低位（偏进攻）"
    else:
        cash_desc = "历史低位（极度进攻）"

    # 风格漂移预警（仓位突变检测）
    # 检查是否存在某季度股票仓位变化超过10个百分点
    style_drift_warning = ""
    if len(df) > 1:
        stock_change = df["股票"].diff().abs()
        max_change = stock_change.max()

        if max_change > 0.15:
            style_drift_warning = f"检测到风格漂移：股票仓位单季最大变化 {max_change*100:.1f}%，存在大幅调仓迹象"
        elif max_change > 0.10:
            style_drift_warning = f"检测到风格漂移：股票仓位单季最大变化 {max_change*100:.1f}%，存在调仓迹象"

    # 准备图表数据（百分比堆叠面积图）
    chart_data = {
        "x": df["日期"].dt.strftime("%Y-%m").tolist(),
        "series": [
            {
                "name": "股票",
                "data": (df["股票"] * 100).round(2).tolist(),
                "color": "#e74c3c"  # 红色
            },
            {
                "name": "债券",
                "data": (df["债券"] * 100).round(2).tolist(),
                "color": "#27ae60"  # 绿色
            },
            {
                "name": "现金",
                "data": (df["现金"] * 100).round(2).tolist(),
                "color": "#95a5a6"  # 灰色
            },
            {
                "name": "其他",
                "data": (df["其他"] * 100).round(2).tolist(),
                "color": "#3498db"  # 蓝色
            }
        ],
        "stock_avg_ratio": round(stock_avg * 100, 2),
        "stock_std_ratio": round(stock_std * 100, 2),
        "cash_percentile": round(cash_percentile, 1),
        "cash_percentile_desc": cash_desc,
        "style_drift_warning": style_drift_warning
    }

    return {
        "chart_data": chart_data,
        "stock_avg_ratio": round(stock_avg * 100, 2),
        "stock_std_ratio": round(stock_std * 100, 2),
        "cash_percentile": round(cash_percentile, 1),
        "cash_percentile_desc": cash_desc,
        "style_drift_warning": style_drift_warning
    }


# ============================================================
# 模块2：持仓演变分析
# ============================================================

@audit_logger
def analyze_holdings_evolution(holdings_data: HoldingsHistoryData) -> Dict[str, Any]:
    """
    分析前十大持仓股历史变化

    Args:
        holdings_data: 持仓历史数据

    Returns:
        {
            "heatmap_data": {...},          # 热力图数据
            "retention_rate": float,        # 持仓留存率（平均连续持有季度数 / 总季度数）
            "avg_holding_periods": float,  # 平均持仓周期（季度）
            "turnover_rate": float,        # 换手率（新进+退出 / 总持仓数）
            "style_tag": str,              # 风格标签
        }
    """
    df = holdings_data.df.copy()

    if df.empty:
        logger.warning("[analyze_holdings_evolution] 持仓数据为空")
        return {
            "heatmap_data": {},
            "retention_rate": 0.0,
            "avg_holding_periods": 0.0,
            "turnover_rate": 0.0,
            "style_tag": "数据不足"
        }

    # 提取前十大持仓（按最新一期）
    if '年份' not in df.columns:
        logger.error("[analyze_holdings_evolution] 缺少'年份'列，无法进行分析")
        return {
            "heatmap_data": {},
            "retention_rate": 0.0,
            "avg_holding_periods": 0.0,
            "turnover_rate": 0.0,
            "style_tag": "数据不足"
        }
    
    latest_period = df['年份'].iloc[0] + df['季度'].iloc[0] if '季度' in df.columns else df['年份'].iloc[0]
    df_latest = df[df['年份'] == df['年份'].iloc[0]].sort_values('占净值比例', ascending=False).head(10)

    if df_latest.empty:
        logger.warning("[analyze_holdings_evolution] 最新期持仓为空")
        return {
            "heatmap_data": {},
            "retention_rate": 0.0,
            "avg_holding_periods": 0.0,
            "turnover_rate": 0.0,
            "style_tag": "数据不足"
        }

    top10_stocks = df_latest['股票代码'].tolist()
    stock_names = df_latest.set_index('股票代码')['股票名称'].to_dict()

    # 构建热力图数据
    # X轴：季度时间线
    # Y轴：股票
    # 颜色：占净值比例
    if '年份' not in df.columns or '季度' not in df.columns:
        logger.error("[analyze_holdings_evolution] 缺少'年份'或'季度'列")
        return {
            "heatmap_data": {},
            "retention_rate": 0.0,
            "avg_holding_periods": 0.0,
            "turnover_rate": 0.0,
            "style_tag": "数据不足"
        }

    # 提取季度标识（从"2025年1季度股票投资明细"转换为"2025Q1"）
    def extract_quarter_label(quarter_str):
        """从季度字符串中提取季度标签，如'2025年1季度股票投资明细' -> '2025Q1'"""
        import re
        match = re.search(r'(\d{4})年(\d+)季度', quarter_str)
        if match:
            year = match.group(1)
            q = match.group(2)
            return f"{year}Q{q}"
        return quarter_str

    # 获取所有季度（按年份+季度排序）
    df_with_label = df.copy()
    df_with_label['季度标签'] = df_with_label['季度'].apply(extract_quarter_label)
    periods = sorted(df_with_label['季度标签'].unique(), reverse=True)  # 按时间降序

    # 构建季度到标签的映射
    quarter_to_label = dict(zip(df_with_label['季度'], df_with_label['季度标签']))

    heatmap_data = {
        "x": periods,  # 季度标签（如'2025Q1'）
        "y": [stock_names.get(code, code) for code in top10_stocks],
        "z": [],  # 占比矩阵
        "annotations": [],  # 标记矩阵（新进/退出/增持/减持）
        "colors": [[0, '#e74c3c'], [0.5, '#f39c12'], [1, '#27ae60']]  # 低->高：红->黄->绿
    }

    # 构建占比矩阵
    for stock_code in top10_stocks:
        row_data = []
        annotations_row = []

        for period_label in periods:
            # 获取该季度该股票的占比
            # 需要通过季度标签找到对应的原始季度值
            quarter_values = [q for q, label in quarter_to_label.items() if label == period_label]
            if not quarter_values:
                row_data.append(0.0)
                annotations_row.append("-")
                continue

            quarter_value = quarter_values[0]
            period_data = df[df['季度'] == quarter_value]
            stock_data = period_data[period_data['股票代码'] == stock_code]

            if not stock_data.empty:
                ratio = stock_data['占净值比例'].values[0] * 100  # 转换为百分比
                row_data.append(round(ratio, 2))

                # 判断标记
                annotation = ""
                if ratio > 0:
                    annotation = f"{ratio:.1f}%"
            else:
                row_data.append(0.0)
                annotation = "-"

            annotations_row.append(annotation)

        heatmap_data["z"].append(row_data)
        heatmap_data["annotations"].append(annotations_row)

    # 计算持仓留存率
    # 方法：统计每只股票出现在持仓的季度数，然后除以总季度数
    retention_rates = []
    for stock_code in top10_stocks:
        stock_quarters = df_with_label[df_with_label['股票代码'] == stock_code]['季度标签'].unique()
        if len(stock_quarters) > 0:
            retention_rates.append(len(stock_quarters) / len(periods))

    retention_rate = np.mean(retention_rates) if retention_rates else 0.0

    # 计算平均持仓周期（季度）
    avg_holding_periods = np.mean([len(df_with_label[df_with_label['股票代码'] == code]['季度标签'].unique()) for code in top10_stocks]) if top10_stocks else 0.0

    # 计算换手率
    # 方法：统计相邻两期的新进和退出股票数
    new_entries = 0
    exits = 0
    if '季度标签' in df_with_label.columns and '股票代码' in df_with_label.columns:
        for i in range(len(periods) - 1):
            period1 = periods[i]
            period2 = periods[i + 1]

            stocks1 = set(df_with_label[df_with_label['季度标签'] == period1]['股票代码'].unique())
            stocks2 = set(df_with_label[df_with_label['季度标签'] == period2]['股票代码'].unique())

            new_entries += len(stocks1 - stocks2)
            exits += len(stocks2 - stocks1)
    else:
        logger.error("[analyze_holdings_evolution] 缺少必要的列，跳过换手率计算")

    total_holdings = len(df_with_label[df_with_label['季度标签'].isin(periods)].groupby(['季度标签', '股票代码'])) if '季度标签' in df_with_label.columns and '股票代码' in df_with_label.columns else 0
    turnover_rate = (new_entries + exits) / total_holdings if total_holdings > 0 else 0.0

    # 风格标签
    if avg_holding_periods >= 6:
        style_tag = "长期价值投资型"
    elif avg_holding_periods >= 2:
        style_tag = "中期平衡型"
    else:
        style_tag = "高频交易型"

    return {
        "heatmap_data": heatmap_data,
        "retention_rate": round(retention_rate * 100, 1),
        "avg_holding_periods": round(avg_holding_periods, 1),
        "turnover_rate": round(turnover_rate * 100, 1),
        "style_tag": style_tag,
        "unique_years": df['年份'].unique().tolist()  # 添加年份数据
    }


# ============================================================
# 模块3：交易能力评估
# ============================================================

@audit_logger
def evaluate_trading_ability(
    holdings_data: HoldingsHistoryData,
    n_quarters: int = 4
) -> Dict[str, Any]:
    """
    评估最近N个季度的交易能力（新买/卖的30天走势）

    Args:
        holdings_data: 持仓历史数据
        n_quarters: 评估的季度数，默认4个季度

    Returns:
        {
            "chart_data": {...},            # 交易能力分布图数据
            "buy_success_rate": float,     # 抄底成功率（新买入股票30天正收益比例）
            "sell_success_rate": float,    # 逃顶成功率（卖出股票30天负收益比例）
            "ability_score": float,        # 能力评分（0-100）
            "ability_tag": str,            # 能力标签
            "trades_detail": List[Dict],   # 交易详情
        }
    """
    df = holdings_data.df.copy()

    if df.empty or '年份' not in df.columns or len(df['年份'].unique()) < 2:
        logger.warning(f"[evaluate_trading_ability] 持仓数据不足，需要至少{n_quarters}个季度")
        return {
            "chart_data": {},
            "buy_success_rate": 0.0,
            "sell_success_rate": 0.0,
            "ability_score": 0.0,
            "ability_tag": "数据不足",
            "trades_detail": []
        }

    # 获取最近n_quarters个季度
    if '年份' not in df.columns:
        logger.warning("[evaluate_trading_ability] 缺少'年份'列")
        return {
            "chart_data": {},
            "buy_success_rate": 0.0,
            "sell_success_rate": 0.0,
            "ability_score": 0.0,
            "ability_tag": "数据不足",
            "trades_detail": []
        }
    
    recent_periods = sorted(df['年份'].unique(), reverse=True)[:n_quarters]

    if len(recent_periods) < 2:
        logger.warning(f"[evaluate_trading_ability] 实际季度数 {len(recent_periods)} < 需要的 {n_quarters}")
        return {
            "chart_data": {},
            "buy_success_rate": 0.0,
            "sell_success_rate": 0.0,
            "ability_score": 0.0,
            "ability_tag": "数据不足",
            "trades_detail": []
        }

    # 识别交易
    trades = []
    for i in range(len(recent_periods) - 1):
        period_current = recent_periods[i]
        period_prev = recent_periods[i + 1]

        df_current = df[df['年份'] == period_current].head(10)  # 前十大
        df_prev = df[df['年份'] == period_prev].head(10)

        stocks_current = set(df_current['股票代码'].tolist())
        stocks_prev = set(df_prev['股票代码'].tolist())

        # 新买入：上期不在，本期在前十
        new_buys = stocks_current - stocks_prev
        # 卖出：上期在前十，本期不在
        sells = stocks_prev - stocks_current

        # 记录交易
        for stock_code in new_buys:
            stock_data = df_current[df_current['股票代码'] == stock_code]
            if not stock_data.empty:
                trades.append({
                    "period": period_current,
                    "type": "buy",
                    "code": stock_code,
                    "name": stock_data['股票名称'].values[0],
                    "ratio": stock_data['占净值比例'].values[0] * 100,
                    "return_30d": None  # 待获取
                })

        for stock_code in sells:
            stock_data = df_prev[df_prev['股票代码'] == stock_code]
            if not stock_data.empty:
                trades.append({
                    "period": period_prev,
                    "type": "sell",
                    "code": stock_code,
                    "name": stock_data['股票名称'].values[0],
                    "ratio": stock_data['占净值比例'].values[0] * 100,
                    "return_30d": None  # 待获取
                })

    if not trades:
        logger.warning("[evaluate_trading_ability] 没有检测到交易")
        return {
            "chart_data": {},
            "buy_success_rate": 0.0,
            "sell_success_rate": 0.0,
            "ability_score": 0.0,
            "ability_tag": "无交易数据",
            "trades_detail": []
        }

    # 模拟30天收益率（简化版：随机生成，实际应从AkShare获取）
    # TODO: 集成 ak.stock_zh_a_hist() 获取真实走势
    import random
    for trade in trades:
        # 模拟：买入的成功率较高，卖出的成功率略低
        if trade["type"] == "buy":
            # 买入：60%概率正收益，正收益区间0-20%，负收益区间-10%-0
            if random.random() < 0.6:
                trade["return_30d"] = round(random.uniform(0, 20), 2)
            else:
                trade["return_30d"] = round(random.uniform(-10, 0), 2)
        else:
            # 卖出：50%概率负收益（逃顶成功），负收益区间-20%-0，正收益区间0-10%
            if random.random() < 0.5:
                trade["return_30d"] = round(random.uniform(-20, 0), 2)
            else:
                trade["return_30d"] = round(random.uniform(0, 10), 2)

    # 计算成功率
    buy_trades = [t for t in trades if t["type"] == "buy"]
    sell_trades = [t for t in trades if t["type"] == "sell"]

    buy_success_rate = sum(1 for t in buy_trades if t["return_30d"] > 0) / len(buy_trades) * 100 if buy_trades else 0.0
    sell_success_rate = sum(1 for t in sell_trades if t["return_30d"] < 0) / len(sell_trades) * 100 if sell_trades else 0.0

    # 能力评分（抄底成功率 × 40% + 逃顶成功率 × 60%）
    ability_score = buy_success_rate * 0.4 + sell_success_rate * 0.6

    # 能力标签
    if buy_success_rate > 70 and sell_success_rate > 70:
        ability_tag = "神级交易能力"
    elif buy_success_rate > 60 and sell_success_rate > 60:
        ability_tag = "优秀交易能力"
    elif buy_success_rate < 40 and sell_success_rate < 40:
        ability_tag = "频繁接盘侠"
    else:
        ability_tag = "平庸交易"

    # 准备图表数据
    chart_data = {
        "x": ["新买入", "卖出"],
        "positive_returns": [
            round(sum(1 for t in buy_trades if t["return_30d"] > 0) / len(buy_trades) * 100, 1) if buy_trades else 0.0,
            round(sum(1 for t in sell_trades if t["return_30d"] > 0) / len(sell_trades) * 100, 1) if sell_trades else 0.0
        ],
        "negative_returns": [
            round(sum(1 for t in buy_trades if t["return_30d"] <= 0) / len(buy_trades) * 100, 1) if buy_trades else 0.0,
            round(sum(1 for t in sell_trades if t["return_30d"] <= 0) / len(sell_trades) * 100, 1) if sell_trades else 0.0
        ],
        "avg_return": [
            round(np.mean([t["return_30d"] for t in buy_trades]), 2) if buy_trades else 0.0,
            round(np.mean([t["return_30d"] for t in sell_trades]), 2) if sell_trades else 0.0
        ]
    }

    return {
        "chart_data": chart_data,
        "buy_success_rate": round(buy_success_rate, 1),
        "sell_success_rate": round(sell_success_rate, 1),
        "ability_score": round(ability_score, 1),
        "ability_tag": ability_tag,
        "trades_detail": trades
    }


# ============================================================
# 模块4.1：估值分析
# ============================================================

@audit_logger
def analyze_valuation(
    holdings_data: HoldingsHistoryData,
    top_n: int = 10
) -> Dict[str, Any]:
    """
    分析最新持仓的估值情况

    Args:
        holdings_data: 持仓历史数据
        top_n: 分析前N大持仓

    Returns:
        {
            "valuation_chart": {...},       # 估值散点图数据
            "overvalued_stocks": [...],    # 高估股票列表
            "fair_valued_stocks": [...],   # 估值合理股票
            "undervalued_stocks": [...],   # 低估股票
            "overvalued_count": int,        # 高估股票数量
            "risk_warning": str,            # 风险预警
        }
    """
    df = holdings_data.df.copy()

    if df.empty:
        logger.warning("[analyze_valuation] 持仓数据为空")
        return {
            "valuation_chart": {},
            "overvalued_stocks": [],
            "fair_valued_stocks": [],
            "undervalued_stocks": [],
            "overvalued_count": 0,
            "risk_warning": "数据不足"
        }

    if '年份' not in df.columns:
        logger.error("[analyze_valuation] 缺少'年份'列")
        return {
            "valuation_chart": {},
            "overvalued_stocks": [],
            "fair_valued_stocks": [],
            "undervalued_stocks": [],
            "overvalued_count": 0,
            "risk_warning": "数据不足"
        }

    # 获取最新一期前N大持仓
    latest_period = df['年份'].iloc[0]
    df_latest = df[df['年份'] == latest_period].sort_values('占净值比例', ascending=False).head(top_n)

    if df_latest.empty:
        logger.warning("[analyze_valuation] 最新期持仓为空")
        return {
            "valuation_chart": {},
            "overvalued_stocks": [],
            "fair_valued_stocks": [],
            "undervalued_stocks": [],
            "overvalued_count": 0,
            "risk_warning": "数据不足"
        }

    # 模拟估值数据（简化版）
    # TODO: 集成 ak.stock_zh_a_spot_em() 获取真实PE/PB
    import random
    valuations = []
    for _, row in df_latest.iterrows():
        code = row['股票代码']
        name = row['股票名称']
        ratio = row['占净值比例'] * 100

        # 模拟PE值（10-80）
        pe = round(random.uniform(10, 80), 2)

        # 模拟行业PE分位数（0-100）
        pe_percentile = round(random.uniform(0, 100), 1)

        # 估值评级
        if pe > 50 and pe_percentile > 80:
            rating = "高估"
        elif pe < 20 and pe_percentile < 20:
            rating = "低估"
        else:
            rating = "合理"

        valuations.append({
            "code": code,
            "name": name,
            "pe": pe,
            "pe_percentile": pe_percentile,
            "ratio": ratio,
            "rating": rating
        })

    # 分类
    overvalued_stocks = [v for v in valuations if v["rating"] == "高估"]
    fair_valued_stocks = [v for v in valuations if v["rating"] == "合理"]
    undervalued_stocks = [v for v in valuations if v["rating"] == "低估"]

    # 风险预警
    risk_warning = ""
    if len(overvalued_stocks) >= top_n * 0.5:
        risk_warning = f"警告：前{top_n}大持仓中有 {len(overvalued_stocks)} 只股票估值偏高（>行业80分位），存在回调风险"
    elif len(overvalued_stocks) >= top_n * 0.3:
        risk_warning = f"提示：前{top_n}大持仓中有 {len(overvalued_stocks)} 只股票估值偏高，需关注估值风险"

    # 准备图表数据（散点图）
    valuation_chart = {
        "x": [v["pe"] for v in valuations],
        "y": [v["ratio"] for v in valuations],
        "text": [v["name"] for v in valuations],
        "marker": {
            "size": [max(v["ratio"], 1) for v in valuations],  # 气泡大小基于占比
            "color": [
                "#e74c3c" if v["rating"] == "高估" else
                "#27ae60" if v["rating"] == "低估" else
                "#f39c12"
                for v in valuations
            ]
        },
        "color_map": {
            "高估": "#e74c3c",  # 红色
            "合理": "#f39c12",  # 黄色
            "低估": "#27ae60"   # 绿色
        }
    }

    return {
        "valuation_chart": valuation_chart,
        "overvalued_stocks": overvalued_stocks,
        "fair_valued_stocks": fair_valued_stocks,
        "undervalued_stocks": undervalued_stocks,
        "overvalued_count": len(overvalued_stocks),
        "risk_warning": risk_warning
    }


# ============================================================
# 模块4.2：压力测试
# ============================================================

@audit_logger
def stress_test_industry(
    holdings_data: HoldingsHistoryData,
    industry_params: Optional[Dict[str, float]] = None,
    top_n: int = 10
) -> Dict[str, Any]:
    """
    行业暴跌压力测试

    Args:
        holdings_data: 持仓历史数据
        industry_params: 行业跌幅参数，默认为None使用预设值
        top_n: 分析前N大持仓

    Returns:
        {
            "industry_stress_chart": {...},  # 行业影响图数据
            "industry_impact": {...},        # 各行业对净值的冲击
            "total_nav_decline": float,      # 预计净值跌幅
            "risk_level": str,               # 风险等级
        }
    """
    # 预设行业跌幅参数
    if industry_params is None:
        industry_params = {
            "银行": -0.08,
            "保险": -0.10,
            "证券": -0.15,
            "白酒": -0.20,
            "医药": -0.30,
            "新能源": -0.30,
            "半导体": -0.30,
            "房地产": -0.15,
            "煤炭": -0.10,
            "钢铁": -0.12,
            "基础化工": -0.15,
            "其他": -0.15  # 默认跌幅
        }

    df = holdings_data.df.copy()

    if df.empty:
        logger.warning("[stress_test_industry] 持仓数据为空")
        return {
            "industry_stress_chart": {},
            "industry_impact": {},
            "total_nav_decline": 0.0,
            "risk_level": "数据不足"
        }

    if '年份' not in df.columns:
        logger.error("[stress_test_industry] 缺少'年份'列")
        return {
            "industry_stress_chart": {},
            "industry_impact": {},
            "total_nav_decline": 0.0,
            "risk_level": "数据不足"
        }

    # 获取最新一期前N大持仓
    latest_period = df['年份'].iloc[0]
    df_latest = df[df['年份'] == latest_period].sort_values('占净值比例', ascending=False).head(top_n)

    if df_latest.empty:
        logger.warning("[stress_test_industry] 最新期持仓为空")
        return {
            "industry_stress_chart": {},
            "industry_impact": {},
            "total_nav_decline": 0.0,
            "risk_level": "数据不足"
        }

    # 计算行业冲击
    industry_impact = {}
    for _, row in df_latest.iterrows():
        code = row['股票代码']
        name = row['股票名称']
        ratio = row['占净值比例']

        # 获取股票行业
        industry = get_stock_industry(code)
        if not industry:
            industry = get_stock_industry_by_name(name)

        # 使用预设跌幅或默认跌幅
        decline = industry_params.get(industry, industry_params.get("其他", -0.15))

        # 计算冲击
        impact = ratio * decline

        if industry not in industry_impact:
            industry_impact[industry] = 0.0

        industry_impact[industry] += impact

    # 计算总冲击
    total_nav_decline = sum(industry_impact.values())

    # 风险等级
    if total_nav_decline < -0.15:
        risk_level = "极高"
    elif total_nav_decline < -0.10:
        risk_level = "高"
    elif total_nav_decline < -0.05:
        risk_level = "中等"
    else:
        risk_level = "低"

    # 准备图表数据（水平柱状图）
    industries_sorted = sorted(industry_impact.items(), key=lambda x: abs(x[1]), reverse=True)

    industry_stress_chart = {
        "y": [item[0] for item in industries_sorted],
        "x": [round(item[1] * 100, 2) for item in industries_sorted],
        "colors": [
            "#e74c3c" if item[1] < 0 else "#27ae60"  # 负向影响为红色
            for item in industries_sorted
        ]
    }

    return {
        "industry_stress_chart": industry_stress_chart,
        "industry_impact": {k: round(v * 100, 2) for k, v in industry_impact.items()},
        "total_nav_decline": round(total_nav_decline * 100, 2),
        "risk_level": risk_level
    }


@audit_logger
def stress_test_market(
    holdings_data: HoldingsHistoryData,
    market_decline: float = -0.10,
    beta: float = 1.0,
    top_n: int = 10
) -> Dict[str, Any]:
    """
    全市场暴跌压力测试

    Args:
        holdings_data: 持仓历史数据
        market_decline: 市场跌幅，默认-10%
        beta: 基金Beta，默认1.0
        top_n: 分析前N大持仓

    Returns:
        {
            "market_stress_chart": {...},    # 市场压力图数据
            "expected_nav_decline": float,  # 预计净值跌幅
            "vs_market": float,             # 相对市场跌幅
            "defensive_strength": str,      # 防御强度标签
        }
    """
    df = holdings_data.df.copy()

    if df.empty:
        logger.warning("[stress_test_market] 持仓数据为空")
        return {
            "market_stress_chart": {},
            "expected_nav_decline": 0.0,
            "vs_market": 0.0,
            "defensive_strength": "数据不足"
        }

    if '年份' not in df.columns:
        logger.error("[stress_test_market] 缺少'年份'列")
        return {
            "market_stress_chart": {},
            "expected_nav_decline": 0.0,
            "vs_market": 0.0,
            "defensive_strength": "数据不足"
        }

    # 获取最新一期前N大持仓
    latest_period = df['年份'].iloc[0]
    df_latest = df[df['年份'] == latest_period].sort_values('占净值比例', ascending=False).head(top_n)

    if df_latest.empty:
        logger.warning("[stress_test_market] 最新期持仓为空")
        return {
            "market_stress_chart": {},
            "expected_nav_decline": 0.0,
            "vs_market": 0.0,
            "defensive_strength": "数据不足"
        }

    # 计算预计净值跌幅
    # 简化版：预计净值跌幅 = 基金Beta × 市场跌幅
    expected_nav_decline = beta * market_decline

    # 相对市场跌幅
    vs_market = expected_nav_decline - market_decline

    # 防御强度标签
    if vs_market > 0.02:
        defensive_strength = "强防御（相对市场少跌2个百分点以上）"
    elif vs_market > 0:
        defensive_strength = "中等防御（相对市场少跌）"
    elif vs_market > -0.02:
        defensive_strength = "中性（接近市场）"
    else:
        defensive_strength = "弱防御（相对市场多跌）"

    # 准备图表数据（仪表盘）
    market_stress_chart = {
        "value": round(expected_nav_decline * 100, 2),
        "title": f"预计净值跌幅（市场下跌{abs(market_decline)*100:.0f}%）",
        "zones": [
            {"value": -5, "label": "安全区", "color": "#27ae60"},
            {"value": -15, "label": "警示区", "color": "#f39c12"},
            {"value": -100, "label": "危险区", "color": "#e74c3c"}
        ]
    }

    return {
        "market_stress_chart": market_stress_chart,
        "expected_nav_decline": round(expected_nav_decline * 100, 2),
        "vs_market": round(vs_market * 100, 2),
        "defensive_strength": defensive_strength
    }


# ============================================================
# 主入口：生成深度持仓分析
# ============================================================

@audit_logger
def generate_deep_holdings_analysis(
    symbol: str,
    analysis_period: str,
    establish_date: Optional[str] = None,
    manager_start_date: Optional[str] = None,
    max_years: int = 5,
    fund_beta: float = 1.0,
    n_quarters_for_trading: int = 4,
    top_n_for_valuation: int = 10
) -> Dict[str, Any]:
    """
    生成深度持仓分析报告

    Args:
        symbol: 基金代码
        analysis_period: 分析周期，"成立以来" 或 "现任经理"
        establish_date: 基金成立日期
        manager_start_date: 基金经理上任日期
        max_years: 最大年数，默认5年
        fund_beta: 基金Beta（用于市场压力测试）
        n_quarters_for_trading: 交易能力评估的季度数
        top_n_for_valuation: 估值分析的前N大持仓

    Returns:
        {
            "asset_trend": {...},              # 模块1：资产配置趋势
            "holdings_evolution": {...},      # 模块2：持仓演变
            "trading_ability": {...},          # 模块3：交易能力评估
            "valuation_stress": {...}         # 模块4：估值与压力测试
        }
    """
    logger.info(f"[generate_deep_holdings_analysis] 开始分析 {symbol}，分析周期：{analysis_period}")

    # 1. 加载数据
    holdings_data, asset_structure_data = load_holdings_analysis_data(
        symbol=symbol,
        analysis_period=analysis_period,
        establish_date=establish_date,
        manager_start_date=manager_start_date,
        max_years=max_years
    )

    # 2. 模块1：资产配置趋势分析
    asset_trend = analyze_asset_trend(asset_structure_data)

    # 3. 模块2：持仓演变分析
    holdings_evolution = analyze_holdings_evolution(holdings_data)

    # 4. 模块3：交易能力评估（条件渲染：如果数据不足4季度，显示"数据不足"）
    if '年份' in holdings_data.df.columns and len(holdings_data.df['年份'].unique()) >= n_quarters_for_trading:
        trading_ability = evaluate_trading_ability(holdings_data, n_quarters=n_quarters_for_trading)
    else:
        logger.warning(f"[generate_deep_holdings_analysis] 交易能力评估需要至少{n_quarters_for_trading}个季度数据")
        trading_ability = {
            "chart_data": {},
            "buy_success_rate": 0.0,
            "sell_success_rate": 0.0,
            "ability_score": 0.0,
            "ability_tag": f"数据不足（需要至少{n_quarters_for_trading}个季度）",
            "trades_detail": []
        }

    # 5. 模块4.1：估值分析
    valuation_analysis = analyze_valuation(holdings_data, top_n=top_n_for_valuation)

    # 6. 模块4.2：压力测试
    industry_stress = stress_test_industry(holdings_data, top_n=top_n_for_valuation)
    market_stress = stress_test_market(holdings_data, beta=fund_beta, top_n=top_n_for_valuation)

    # 合并模块4
    valuation_stress = {
        **valuation_analysis,
        "industry_stress": industry_stress,
        "market_stress": market_stress,
        "expected_decline": industry_stress["total_nav_decline"]
    }

    logger.info(f"[generate_deep_holdings_analysis] 分析完成")

    return {
        "asset_trend": asset_trend,
        "holdings_evolution": holdings_evolution,
        "trading_ability": trading_ability,
        "valuation_stress": valuation_stress
    }
