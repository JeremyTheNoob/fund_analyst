"""
基金穿透式诊断系统 - 主程序
基于模块化架构的Streamlit应用
"""

import streamlit as st
import pandas as pd

import config
from models import analyze_fund
from ui.charts import plot_radar_chart, plot_cumulative_return, plot_holdings_pie
from ui.components import render_analysis_report, render_css
from ui.holdings_components import render_holdings_penetration_dashboard
from ui.alpha_v2_components import render_alpha_v2_dashboard

# 设置Streamlit运行时标志，使缓存装饰器正常工作
import data.fetcher
data.fetcher._in_streamlit_runtime = True


def main():
    """主程序"""

    # 页面配置
    st.set_page_config(
        page_title="DeepInFund · 基金深度诊断",
        page_icon="🔬",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # CSS样式
    render_css()

    # Hero Banner
    st.markdown("""
    <div class="hero">
        <h1>🔬 DeepInFund</h1>
        <p>买基之前搜一搜</p>
    </div>
    """, unsafe_allow_html=True)

    # 输入区
    col_in, col_year, col_btn = st.columns([3, 2, 2])

    with col_in:
        fund_code = st.text_input(
            "基金代码",
            placeholder="如：000001",
            label_visibility='collapsed',
            key="fund_code_input",
        )

    with col_year:
        years = st.selectbox(
            "分析时长",
            [1, 3, 5, 10],
            index=2,
            label_visibility='visible',
        )

    with col_btn:
        analyze_btn = st.button(
            "开始分析",
            type="primary",
            use_container_width=True,
        )

    # 分析逻辑
    if analyze_btn and fund_code:
        with st.spinner(f"正在分析基金 {fund_code}..."):

            # 调用模型层分析
            result = analyze_fund(
                symbol=fund_code,
                years=years,
                since_inception=False,
            )

            # 检查错误
            if 'error' in result:
                st.error(f"分析失败：{result['error']}")
                return

            # 提取结果
            basic_info = result.get('basic_info', {})
            nav_data = result.get('nav_data', pd.DataFrame())
            model_type = result.get('model_type', '')
            model_results = result.get('model_results', {})
            performance = result.get('performance', {})
            holdings_data = result.get('holdings_data', {})

            # 获取雷达图评分
            if 'radar_scores' in model_results:
                radar_scores = model_results['radar_scores']
            elif 'radar_scores' in result:
                radar_scores = result['radar_scores']
            else:
                # 如果没有雷达图评分，创建一个默认的
                radar_scores = {
                    'scores': {k: 50 for k in config.RADAR_WEIGHTS['equity'].keys()},
                    'weights': config.RADAR_WEIGHTS.get(model_type, config.RADAR_WEIGHTS['others']),
                    'total_score': 50.0,
                }

            # 渲染完整分析报告
            render_analysis_report(
                symbol=fund_code,
                basic_info=basic_info,
                performance=performance,
                model_type=model_type,
                model_results=model_results,
                radar_scores=radar_scores,
            )

            # 附加图表（如果有数据）
            if not nav_data.empty:
                st.markdown("---")
                st.markdown("### 📊 收益曲线")

                # 累计收益曲线
                col_curve, col_holdings = st.columns([2, 1])

                with col_curve:
                    # 获取基准数据
                    benchmark_ret = model_results.get('benchmark_ret', pd.Series())
                    benchmark_df = None
                    if not benchmark_ret.empty:
                        benchmark_df = pd.DataFrame({
                            'date': benchmark_ret.index,
                            'nav': (1 + benchmark_ret).cumprod() * 100
                        })

                    fig = plot_cumulative_return(
                        fund_nav=nav_data,
                        benchmark_df=benchmark_df,
                        fund_name=basic_info.get('name', '基金'),
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # 持仓饼图
                with col_holdings:
                    top10 = holdings_data.get('top10', pd.DataFrame())
                    if not top10.empty:
                        fig = plot_holdings_pie(top10, top_n=10)
                        st.plotly_chart(fig, use_container_width=True)

            # ---------- Part 3: 持仓穿透分析（权益/混合/行业型） ----------
            holdings_penetration = model_results.get('holdings_penetration', {})
            if holdings_penetration and model_type in ('equity', 'mixed', 'sector'):
                st.markdown("---")
                render_holdings_penetration_dashboard(holdings_penetration)

            # ---------- Part 3b: Alpha v2.0 专业分层分析 ----------
            alpha_v2 = model_results.get('alpha_v2', {})
            if alpha_v2 and 'error' not in alpha_v2 and model_type in ('equity', 'mixed', 'sector'):
                st.markdown("---")
                render_alpha_v2_dashboard(alpha_v2)

            # 免责声明(移到报告最后)
            st.markdown("---")
            from ui.components import render_disclaimer
            render_disclaimer()

    # 使用示例
    if not fund_code:
        st.markdown("---")
        st.markdown("### 💡 使用示例")
        st.info("""
        **推荐测试基金：**
        - 000001 - 华夏成长（混合型）
        - 000069 - 华夏债券（债券型）
        - 510300 - 沪深300ETF（指数型）
        - 161725 - 招商中证白酒（行业型）

        **使用说明：**
        1. 输入6位基金代码
        2. 选择分析时长（1/3/5/10年）
        3. 点击"开始分析"按钮
        4. 查看完整的量化分析报告
        """)


if __name__ == '__main__':
    main()
