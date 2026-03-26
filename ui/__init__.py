"""
UI层模块
负责Streamlit界面展示（图表+组件）
依赖：config, utils, data, models
"""

from .charts import (
    plot_radar_chart,
    plot_cumulative_return,
    plot_holdings_pie,
)

from .components import (
    render_kpi_card,
    render_metric_card,
    render_risk_card,
    render_model_results,
    render_analysis_report,
)

__all__ = [
    'plot_radar_chart',
    'plot_cumulative_return',
    'plot_holdings_pie',
    'render_kpi_card',
    'render_metric_card',
    'render_risk_card',
    'render_model_results',
    'render_analysis_report',
]
