"""
利率预测模块 — fund_quant_v2
功能：基于技术指标的短期/中期利率趋势预测
"""

from __future__ import annotations
import logging
from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd
import numpy as np

from config import CACHE_TTL
from data_loader.bond_loader import load_multi_tenor_yields, load_rate_environment

logger = logging.getLogger(__name__)


# ============================================================
# 利率预测模型
# ============================================================

def predict_rate_trend(horizon: str = "3m") -> dict:
    """
    基于技术指标的利率趋势预测

    Args:
        horizon: 预测周期 ("1m" / "3m" / "6m")

    Returns:
        {
            "direction": "up" / "down" / "sideways",
            "confidence": 0.75,  # 置信度 0-1
            "y10y_forecast": {
                "current": 2.5,
                "short_term": 2.4,   # 1个月后预测值
                "mid_term": 2.3,      # 3个月后预测值
                "long_term": 2.2,     # 6个月后预测值
            },
            "key_factors": [
                "10Y收益率处于历史75分位数，均值回归概率高",
                "期限利差平坦化，上行空间有限"
            ],
            "risk_signals": [
                "近期波动率上升，需警惕政策超预期",
                "信用利差走阔，反映市场风险偏好下降"
            ],
            "chart_data": {
                "history": pd.DataFrame,  # 历史数据（近3年）
                "forecast": pd.DataFrame,  # 预测数据（未来6个月）
                "upper_band": pd.DataFrame,  # 预测上界（置信区间）
                "lower_band": pd.DataFrame,  # 预测下界
            }
        }
    """
    # 获取历史数据（近3年）
    today = date.today()
    end = today.strftime("%Y%m%d")
    start = (today - timedelta(days=365 * 3)).strftime("%Y%m%d")

    # 获取多期限收益率
    df = load_multi_tenor_yields(start, end)
    if df.empty or "y10y" not in df.columns:
        return _fallback_prediction()

    # 获取利率环境快照
    rate_env = load_rate_environment(lookback_years=3)

    # 提取10Y收益率序列
    df = df.set_index("date").sort_index()
    y10y = df["y10y"].dropna()

    current_y10y = float(y10y.iloc[-1])
    percentile = rate_env.get("y10y_percentile", 50)
    term_spread = rate_env.get("term_spread", 0)
    spread_status = rate_env.get("term_spread_status", "normal")
    trend = rate_env.get("y10y_trend", "flat")

    # 预测逻辑（基于技术指标组合）
    direction, confidence, factors, risks = _analyze_trend_signals(
        current_y10y, percentile, term_spread, spread_status, trend, y10y
    )

    # 生成预测值（基于趋势外推 + 均值回归）
    forecast = _generate_forecast_values(
        current_y10y, direction, confidence, percentile, y10y
    )

    # 生成图表数据
    chart_data = _generate_chart_data(y10y, forecast)

    return {
        "direction": direction,
        "confidence": confidence,
        "y10y_forecast": forecast,
        "key_factors": factors,
        "risk_signals": risks,
        "chart_data": chart_data,
    }


def _analyze_trend_signals(
    current: float,
    percentile: float,
    term_spread: float,
    spread_status: str,
    trend: str,
    y10y: pd.Series,
) -> Tuple[str, float, list, list]:
    """
    分析趋势信号，返回 (方向, 置信度, 关键因素, 风险信号)
    """
    factors = []
    risks = []
    up_signals = 0
    down_signals = 0

    # 1. 均值回归信号
    if percentile >= 80:
        factors.append(f"10Y收益率处于历史{percentile:.0f}%分位数，均值回归概率高")
        down_signals += 2
    elif percentile <= 20:
        factors.append(f"10Y收益率处于历史{percentile:.0f}%分位数，均值反弹空间大")
        up_signals += 2
    else:
        factors.append(f"10Y收益率处于历史{percentile:.0f}%分位数，位于中性区间")

    # 2. 期限利差信号
    if spread_status == "flat":
        spread_str = f"{term_spread:.2f}%" if term_spread is not None else "N/A"
        factors.append(f"期限利差{spread_str}（平坦化），长端上行空间有限")
        down_signals += 1
        risks.append("期限利差极度平坦，可能预示经济下行风险")
    elif spread_status == "steep":
        spread_str = f"{term_spread:.2f}%" if term_spread is not None else "N/A"
        factors.append(f"期限利差{spread_str}（陡峭），长端有进一步上行空间")
        up_signals += 1
    else:
        spread_str = f"{term_spread:.2f}%" if term_spread is not None else "N/A"
        factors.append(f"期限利差{spread_str}（正常），信号中性")

    # 3. 趋势动量信号
    if trend == "up":
        factors.append("近3个月趋势上行，短期动量偏强")
        up_signals += 1
        risks.append("近期波动率上升，需警惕政策超预期")
    elif trend == "down":
        factors.append("近3个月趋势下行，短期动量偏弱")
        down_signals += 1
    else:
        factors.append("近3个月趋势震荡，方向不明")

    # 4. 波动率信号
    volatility = y10y.rolling(20).std().iloc[-1]
    if volatility > 0.15:  # 波动率 > 15bp
        risks.append(f"近20日波动率{volatility:.1f}bp，市场情绪不稳")

    # 综合判断方向和置信度
    net_signal = up_signals - down_signals

    if net_signal >= 2:
        direction = "up"
        confidence = min(0.7 + abs(net_signal) * 0.1, 0.85)
    elif net_signal <= -2:
        direction = "down"
        confidence = min(0.7 + abs(net_signal) * 0.1, 0.85)
    else:
        direction = "sideways"
        confidence = 0.55  # 震荡方向置信度较低

    # 调整置信度（波动率过大时降低）
    if volatility > 0.15:
        confidence -= 0.1
    confidence = max(confidence, 0.4)

    return direction, round(confidence, 2), factors, risks


def _generate_forecast_values(
    current: float,
    direction: str,
    confidence: float,
    percentile: float,
    y10y: pd.Series,
) -> dict:
    """
    生成预测值（基于趋势外推 + 均值回归）
    """
    # 历史均值（近3年）
    historical_mean = float(y10y.mean())

    # 趋势幅度（基于置信度）
    trend_magnitude = confidence * 0.3  # 最大30bp变动

    if direction == "up":
        short_term = current + trend_magnitude * 0.3
        mid_term = current + trend_magnitude * 0.6
        long_term = current + trend_magnitude * 1.0
    elif direction == "down":
        short_term = current - trend_magnitude * 0.3
        mid_term = current - trend_magnitude * 0.6
        long_term = current - trend_magnitude * 1.0
    else:
        # 震荡方向，向历史均值回归
        regression_strength = (current - historical_mean) * 0.5
        short_term = current - regression_strength * 0.3
        mid_term = current - regression_strength * 0.6
        long_term = current - regression_strength * 1.0

    return {
        "current": round(current, 2),
        "short_term": round(short_term, 2),   # 1个月后
        "mid_term": round(mid_term, 2),       # 3个月后
        "long_term": round(long_term, 2),      # 6个月后
        "historical_mean": round(historical_mean, 2),
    }


def _generate_chart_data(
    y10y_history: pd.Series,
    forecast: dict,
) -> dict:
    """
    生成图表数据（历史 + 预测）
    """
    # 历史数据（近3年）
    history_df = y10y_history.reset_index()
    history_df.columns = ["date", "value"]

    # 预测数据（未来6个月）
    current_date = y10y_history.index[-1]
    forecast_dates = pd.date_range(
        start=current_date + timedelta(days=30),
        periods=6,
        freq="30D"
    )

    forecast_values = [
        forecast["short_term"],
        forecast["short_term"] * 0.7 + forecast["mid_term"] * 0.3,
        forecast["mid_term"],
        forecast["mid_term"] * 0.7 + forecast["long_term"] * 0.3,
        forecast["long_term"] * 0.7 + forecast["long_term"] * 0.3,
        forecast["long_term"],
    ]

    forecast_df = pd.DataFrame({
        "date": forecast_dates,
        "value": forecast_values,
    })

    # 置信区间（基于历史波动率）
    volatility = y10y_history.rolling(20).std().iloc[-1]
    confidence_band = volatility * 1.96  # 95%置信区间

    forecast_df["upper"] = forecast_df["value"] + confidence_band
    forecast_df["lower"] = forecast_df["value"] - confidence_band

    return {
        "history": history_df,
        "forecast": forecast_df,
        "upper_band": forecast_df[["date", "upper"]].rename(columns={"upper": "value"}),
        "lower_band": forecast_df[["date", "lower"]].rename(columns={"lower": "value"}),
    }


def _fallback_prediction() -> dict:
    """数据不足时的兜底预测"""
    return {
        "direction": "sideways",
        "confidence": 0.3,
        "y10y_forecast": {
            "current": 2.5,
            "short_term": 2.5,
            "mid_term": 2.5,
            "long_term": 2.5,
            "historical_mean": 2.5,
        },
        "key_factors": ["数据不足，无法做出有效预测"],
        "risk_signals": [],
        "chart_data": {
            "history": pd.DataFrame(columns=["date", "value"]),
            "forecast": pd.DataFrame(columns=["date", "value"]),
            "upper_band": pd.DataFrame(columns=["date", "value"]),
            "lower_band": pd.DataFrame(columns=["date", "value"]),
        },
    }


# ============================================================
# 生成预测图表
# ============================================================

def generate_rate_prediction_chart(prediction: dict) -> dict:
    """
    生成利率预测图表数据（用于 Plotly 渲染）

    Returns:
        {
            "x": [日期列表],
            "series": [
                {"name": "历史10Y收益率", "data": [历史值], "color": "#3498db"},
                {"name": "预测10Y收益率", "data": [预测值], "color": "#e74c3c", "dash": "dash"},
                {"name": "95%置信区间上界", "data": [上界], "color": "#95a5a6", "dash": "dot"},
                {"name": "95%置信区间下界", "data": [下界], "color": "#95a5a6", "dash": "dot"},
            ],
            "prediction_info": {
                "direction": "上行",
                "confidence": "75%",
                "current": 2.5,
                "mid_term_forecast": 2.3,
            }
        }
    """
    history = prediction["chart_data"]["history"]
    forecast = prediction["chart_data"]["forecast"]
    upper = prediction["chart_data"]["upper_band"]
    lower = prediction["chart_data"]["lower_band"]

    # 合并所有日期
    all_dates = pd.concat([history["date"], forecast["date"]])
    x = [d.strftime("%Y-%m-%d") for d in all_dates]

    # 历史数据
    history_values = history["value"].tolist() + [None] * len(forecast)

    # 预测数据
    forecast_values = [None] * len(history) + forecast["value"].tolist()

    # 置信区间
    upper_values = [None] * len(history) + upper["value"].tolist()
    lower_values = [None] * len(history) + lower["value"].tolist()

    # 方向中文映射
    direction_map = {"up": "上行", "down": "下行", "sideways": "震荡"}
    direction_cn = direction_map.get(prediction["direction"], "震荡")

    return {
        "x": x,
        "series": [
            {
                "name": "历史10Y收益率",
                "data": history_values,
                "color": "#3498db",
            },
            {
                "name": "预测10Y收益率",
                "data": forecast_values,
                "color": "#e74c3c",
                "dash": "dash",
            },
            {
                "name": "95%置信区间上界",
                "data": upper_values,
                "color": "#95a5a6",
                "dash": "dot",
            },
            {
                "name": "95%置信区间下界",
                "data": lower_values,
                "color": "#95a5a6",
                "dash": "dot",
            },
        ],
        "prediction_info": {
            "direction": direction_cn,
            "confidence": f"{int(prediction['confidence'] * 100)}%",
            "current": prediction["y10y_forecast"]["current"],
            "mid_term_forecast": prediction["y10y_forecast"]["mid_term"],
        },
    }
