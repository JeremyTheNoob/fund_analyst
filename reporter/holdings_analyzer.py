"""
持仓分析器 — fund_quant_v2
负责：行业配置分析 / 集中度分析 / 个股留存率 / 资产配置穿透
"""

from __future__ import annotations
from typing import Any, Dict, List
from data_loader.sw_industry_loader import get_stock_industry, get_stock_industry_by_name
import logging

logger = logging.getLogger(__name__)


# ============================================================
# 权益类基金持仓分析
# ============================================================

def analyze_equity_holdings(report: Any) -> Dict[str, Any]:
    """
    分析权益类基金持仓（基于最新一期数据）
    
    Returns:
        {
            "top10_concentration": float,  # 前十大重仓股合计占比
            "concentration_level": str,     # 高/中/低
            "manager_style_tag": str,      # 赛道型/全市场/均衡型
            "stock_count": int,            # 重仓股数量
            "overweight_industry": str,     # 显著超配行业（需多期数据支持）
            "underweight_industry": str,    # 显著低配行业（需多期数据支持）
            "industry_allocation": List[Dict],  # 行业配置（需多期数据支持）
            "retained_stocks_count": int,   # 个股留存率（需多期数据支持）
            "holding_period_tag": str,      # 长期持股/高频换手（需多期数据支持）
            "profit_source": str,          # 长期复利/波段操作（需多期数据支持）
            "risk_industry_name": str,     # 风险集中行业
            "top_stocks": List[Dict],      # 前十大重仓股详情
        }
    """
    holdings = report.chart_data.get("holdings", {})
    top10_stocks = holdings.get("top10_stocks", [])
    
    # 计算前十大重仓股合计占比
    top10_concentration = 0.0
    for stock in top10_stocks[:10]:
        ratio = stock.get("占净值比例", 0)
        try:
            top10_concentration += float(ratio)
        except (ValueError, TypeError):
            pass
    
    top10_concentration = min(top10_concentration, 100.0)
    
    # 集中度等级
    if top10_concentration >= 60:
        concentration_level = "极高，倾向于押注个股 Alpha"
    elif top10_concentration >= 45:
        concentration_level = "中高，核心持股明确"
    else:
        concentration_level = "较低，靠分散配置减震"
    
    # 经理风格标签（基于集中度）
    if top10_concentration >= 55:
        manager_style_tag = "赛道型博弈选手（集中押注）"
    elif top10_concentration >= 40:
        manager_style_tag = "均衡配置型选手（适度集中）"
    else:
        manager_style_tag = "全市场价值发现者（广泛分散）"
    
    # 个股留存率（需多期数据，当前使用估算）
    # 这里用一个基于波动率的简化判断：如果基金波动率低，说明持仓稳定
    # 实际应对比前后两期的 top10 股票代码交集
    equity_metrics = report.equity_metrics
    if equity_metrics:
        volatility = equity_metrics.common.volatility
        # 波动率低 -> 持股稳定 -> 留存率高
        if volatility < 0.15:
            retained_stocks_count = 8
            holding_period_tag = "长周期持股"
            profit_source = "长期复利"
        elif volatility < 0.25:
            retained_stocks_count = 5
            holding_period_tag = "中等持股周期"
            profit_source = "波段操作差价"
        else:
            retained_stocks_count = 2
            holding_period_tag = "高频换手"
            profit_source = "短期估值波动"
    else:
        retained_stocks_count = 5
        holding_period_tag = "未知"
        profit_source = "未知"
    
    # 行业配置（使用申万行业数据库）
    industry_allocation = []
    for stock in top10_stocks[:5]:
        name = stock.get("股票名称", "")
        code = stock.get("股票代码", "")
        ratio = stock.get("占净值比例", 0)

        if name:
            # 优先通过股票代码查询申万行业
            industry = None
            if code:
                # 转换为6位代码
                code_6 = str(code).zfill(6)
                industry = get_stock_industry(code_6)

            # 如果代码查询失败，使用名称推断
            if not industry:
                industry = get_stock_industry_by_name(name)

            # 如果仍未找到，使用简化的关键词匹配（保底）
            if not industry:
                industry = _infer_industry_from_name(name)

            industry_allocation.append({
                "industry": industry or "未知行业",
                "ratio": ratio,
                "stock_name": name,
            })
    
    # 风险集中行业（占比最高的行业）
    risk_industry_name = ""
    if industry_allocation:
        risk_industry_name = max(industry_allocation, key=lambda x: x.get("ratio", 0))["industry"]
    
    return {
        "top10_concentration": top10_concentration,
        "concentration_level": concentration_level,
        "manager_style_tag": manager_style_tag,
        "stock_count": len(top10_stocks),
        "overweight_industry": "暂需多期持仓数据支持",  # 占位
        "underweight_industry": "暂需多期持仓数据支持",  # 占位
        "industry_allocation": industry_allocation,
        "retained_stocks_count": retained_stocks_count,
        "holding_period_tag": holding_period_tag,
        "profit_source": profit_source,
        "risk_industry_name": risk_industry_name,
        "top_stocks": top10_stocks,
    }


# ============================================================
# 固收+基金持仓分析
# ============================================================

def analyze_cb_holdings(report: Any) -> Dict[str, Any]:
    """
    分析可转债/固收+基金持仓
    
    Returns:
        {
            "base_bond_ratio": float,       # 纯债占比
            "equity_ratio": float,         # 权益占比
            "convertible_ratio": float,     # 转债占比
            "equity_plus_convertible_ratio": float,  # 权益+转债
            "cb_style": str,              # 偏股型/平衡型/偏债型
            "risk_level": str,            # 积极进攻/防御观望
            "manager_behavior": str,       # 前瞻性调仓/顺势加码
            "asset_allocation_history": List[Dict],  # 资产配置历史（需多期数据）
            "alpha_jump_period": str,      # 超额收益爆发期
            "quarter_market_up": str,     # 上涨季度
            "quarter_market_down": str,    # 下跌季度
            "old_ratio": float,           # 历史权益仓位
            "new_ratio": float,           # 当前权益仓位
            "alpha_boost": float,          # 超额收益贡献
            "percentile": float,           # 历史分位点
        }
    """
    holdings = report.chart_data.get("holdings", {})
    
    # 资产配置比例（小数格式转换为百分比）
    stock_ratio = holdings.get("stock_ratio", 0.0) * 100
    bond_ratio = holdings.get("bond_ratio", 0.0) * 100
    cash_ratio = holdings.get("cash_ratio", 0.0) * 100
    cb_ratio = holdings.get("cb_ratio", 0.0) * 100

    # 容错处理：如果资产配置数据不完整（如股票仓位为0但转债>0），从转债反推
    if stock_ratio == 0.0 and cb_ratio > 0.0:
        # 转债基金通常股票+转债合计为权益暴露，反推股票仓位
        estimated_equity = min(cb_ratio * 1.2, 30.0)  # 转债的1.2倍作为权益暴露，上限30%
        stock_ratio = estimated_equity
        logger.info(f"[analyze_cb_holdings] 股票仓位缺失，从转债反推: {stock_ratio:.1f}%")

    # 容错处理：如果债券仓位缺失，从持仓明细汇总
    if bond_ratio == 0.0:
        bond_details = holdings.get("bond_details", [])
        if bond_details:
            total_bond = sum(float(b.get("占净值比例", 0) or 0) for b in bond_details)
            # 检查数据格式（百分比 vs 小数）
            if total_bond > 1.5:  # 大于1.5说明是百分比格式
                bond_ratio = total_bond / 100.0 * 100  # 转换为百分比显示
            else:
                bond_ratio = total_bond * 100
            logger.info(f"[analyze_cb_holdings] 债券仓位缺失，从持仓明细汇总: {bond_ratio:.1f}%")
    
    # 纯债占比（总债券 - 转债）
    base_bond_ratio = max(0.0, bond_ratio - cb_ratio)
    
    # 权益+转债合计
    equity_plus_convertible_ratio = stock_ratio + cb_ratio
    
    # 转债风格判断（基于溢价率）
    cb_metrics = report.cb_metrics
    if cb_metrics:
        premium_avg = cb_metrics.premium_avg
        if premium_avg <= 20:
            cb_style = "偏股型（高弹性）"
        elif premium_avg >= 40:
            cb_style = "偏债型（防守型）"
        else:
            cb_style = "平衡型（攻守兼备）"
    else:
        cb_style = "未知"
    
    # 风险水平判断
    if equity_plus_convertible_ratio >= 40:
        risk_level = "积极进攻期"
    elif equity_plus_convertible_ratio >= 20:
        risk_level = "平衡配置期"
    else:
        risk_level = "防御观望期"
    
    # 经理行为判断（基于净值曲线斜率）
    # 实际应对比前后两期的仓位变化
    common = cb_metrics.common if cb_metrics else None
    if common:
        volatility = common.volatility
        annualized_return = common.annualized_return
        
        # 高收益+低波动 -> 前瞻性调仓
        if annualized_return > 0.08 and volatility < 0.10:
            manager_behavior = "前瞻性调仓（精准择时）"
        # 高收益+高波动 -> 顺势加码
        elif annualized_return > 0.08 and volatility >= 0.10:
            manager_behavior = "顺势加码（紧跟市场）"
        else:
            manager_behavior = "稳健操作（保守策略）"
    else:
        manager_behavior = "未知"
    
    # 超额收益爆发期（从图表数据推断）
    excess_info = report.chart_data.get("excess_return", {}).get("excess_info", {})
    curve_trend = excess_info.get("curve_trend", "平稳")
    if "上升" in curve_trend:
        alpha_jump_period = "统计期后半段"
    elif "下降" in curve_trend:
        alpha_jump_period = "统计期前半段"
    else:
        alpha_jump_period = "无显著爆发期"
    
    # 历史分位点（简化估算）
    # 实际应基于历史多期权益仓位计算分位数
    percentile = 70.0 if equity_plus_convertible_ratio >= 30 else 40.0
    
    return {
        "base_bond_ratio": base_bond_ratio,
        "equity_ratio": stock_ratio,
        "convertible_ratio": cb_ratio,
        "equity_plus_convertible_ratio": equity_plus_convertible_ratio,
        "cb_style": cb_style,
        "risk_level": risk_level,
        "manager_behavior": manager_behavior,
        "asset_allocation_history": [],  # 需多期数据
        "alpha_jump_period": alpha_jump_period,
        "quarter_market_up": "2024Q4",  # 占位，应从历史数据推断
        "quarter_market_down": "2024Q3",  # 占位
        "old_ratio": stock_ratio * 0.8,  # 简化估算
        "new_ratio": stock_ratio,
        "alpha_boost": 2.5,  # 简化估算
        "percentile": percentile,
    }


# ============================================================
# 辅助函数
# ============================================================

def _infer_industry_from_name(stock_name: str) -> str:
    """
    从股票名称推断行业（保底方案）

    注意：这是最后的保底方案，优先使用申万行业数据库
    """
    if not stock_name:
        return "未知"

    # 最简化的关键词匹配（仅覆盖最常见的关键词）
    industry_map = {
        "白酒": "白酒",
        "茅台": "白酒",
        "五粮液": "白酒",
        "宁德": "电池",
        "比亚迪": "乘用车",
        "平安": "保险",
        "招行": "股份制银行",
        "腾讯": "游戏",
        "美团": "互联网电商",
        "中芯": "半导体",
        "长江": "电力",
        "中石油": "油服工程",
        "煤炭": "煤炭开采",
        "钢铁": "普钢",
        "医药": "化学制药",
        "恒瑞": "化学制药",
        "万科": "房地产开发",
        "保利": "房地产开发",
        "美的": "白色家电",
        "格力": "白色家电",
        "海康": "计算机设备",
        "中车": "轨交设备",
        "三一": "工程机械",
        "航天": "航天装备",
        "中航": "航空装备",
        "移动": "通信服务",
        "电信": "通信服务",
        "联通": "通信服务",
    }

    for keyword, industry in industry_map.items():
        if keyword in stock_name:
            return industry

    # 默认返回 "其他"（而不是 "未知"，避免显示敏感词）
    return "其他"


def get_top_holdings_summary(holdings: Dict[str, Any], top_n: int = 5) -> List[Dict]:
    """获取前 N 大持仓摘要"""
    top_stocks = holdings.get("top10_stocks", [])
    return [
        {
            "name": stock.get("股票名称", ""),
            "ratio": stock.get("占净值比例", 0),
            "code": stock.get("股票代码", ""),
        }
        for stock in top_stocks[:top_n]
    ]
