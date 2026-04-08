"""
main.py — 基金穿透式分析 v2

v2 重构核心变化：
- 按"拟买入"/"已持有"路由（不再按基金类型分路）
- 按资产维度（股票/利率债/信用债/可转债）分析
- 卡片式紧凑布局，渐进加载
- 不给出投资建议和倾向性表述

启动命令：streamlit run main.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# ============================================================
# 页面配置
# ============================================================

st.set_page_config(
    page_title="基金穿透式分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# 自定义样式
st.markdown("""<style>
/* 全局 */
.stApp { max-width: 960px; margin: 0 auto; }
/* 卡片 */
.card {
    background: #ffffff;
    border: 1px solid #e8e8e8;
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.card-title {
    font-size: 16px;
    font-weight: 600;
    color: #1a1a1a;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid #f0f0f0;
}
.metric-row {
    display: flex;
    gap: 24px;
    flex-wrap: wrap;
    margin-bottom: 8px;
}
.metric-item {
    flex: 1;
    min-width: 140px;
}
.metric-label {
    font-size: 12px;
    color: #888;
    margin-bottom: 2px;
}
.metric-value {
    font-size: 20px;
    font-weight: 600;
    color: #1a1a1a;
}
.metric-desc {
    font-size: 11px;
    color: #aaa;
    margin-top: 2px;
}
.metric-positive { color: #e74c3c; }  /* 涨/高 */
.metric-negative { color: #27ae60; }  /* 跌/低 */
.metric-neutral  { color: #888; }
/* 免责声明 */
.disclaimer {
    background: #fff8e6;
    border: 1px solid #f0d080;
    border-radius: 8px;
    padding: 12px 16px;
    font-size: 12px;
    color: #8a6d3b;
    margin-bottom: 16px;
}
/* 资产结构 */
.asset-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 12px;
    margin-right: 6px;
    margin-bottom: 4px;
}
/* 空板块 */
.empty-section {
    color: #ccc;
    font-size: 13px;
    padding: 8px 0;
}
/* 隐藏 Streamlit 默认元素 */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
[data-testid="stSidebar"] { display: none; }
</style>""", unsafe_allow_html=True)

# ============================================================
# 侧边栏（使用量 + 分析历史 + 反馈入口）
# ============================================================

def _render_sidebar():
    """渲染侧边栏"""
    from data_loader.user_tracker import get_visitor_id, check_usage_limit

    visitor_id = get_visitor_id()
    can_use, remaining = check_usage_limit(visitor_id)

    with st.sidebar:
        st.markdown("### 📊 基金穿透式分析")
        st.caption("内测版本 v2.0")

        # 使用量
        st.divider()
        usage_color = "#e74c3c" if remaining <= 0 else "#27ae60" if remaining > 3 else "#f39c12"
        st.markdown(
            f"**内测剩余次数** <span style='color:{usage_color};font-size:20px;font-weight:700'>"
            f"{remaining}</span> / 5",
            unsafe_allow_html=True,
        )

        if remaining <= 0:
            st.warning("⚠️ 内测次数已用完，公测时可以继续使用")
        else:
            st.progress(remaining / 5)

        # 分析历史
        st.divider()
        history = st.session_state.get("analysis_history", [])
        if history:
            st.markdown("**📋 分析历史**")
            # 最新的在前
            for item in reversed(history[-10:]):
                mode_icon = "🔍" if item.get("mode") == "buy" else "📋"
                label = f"{mode_icon} {item.get('code', '')} {item.get('name', '')}"
                ts = item.get("time", "")
                if ts:
                    label += f"  _{ts}_"
                col_text, col_btn = st.columns([4, 1])
                with col_text:
                    st.markdown(label)
                with col_btn:
                    if st.button("加载", key=f"hist_{item.get('code', '')}_{item.get('time', '')}", use_container_width=True):
                        _load_from_history(item)

            # 清空历史
            if st.button("🗑️ 清空历史", use_container_width=True):
                st.session_state.pop("analysis_history", None)
                st.rerun()
        else:
            st.markdown("*暂无分析记录*")

        # 反馈入口
        st.divider()
        if st.button("📝 提交内测反馈", use_container_width=True, type="secondary"):
            st.session_state["show_feedback"] = True
            st.rerun()

        st.caption("感谢您参与内测！")


def _load_from_history(item: Dict[str, Any]):
    """从历史记录重新加载"""
    code = item.get("code", "")
    mode = item.get("mode", "buy")
    if not code:
        return
    st.session_state["mode"] = mode
    if mode == "buy":
        st.session_state["buy_code_input"] = code
        st.session_state.pop("report", None)
    else:
        codes_str = ", ".join(item.get("codes", [code]))
        st.session_state["hold_codes_input"] = codes_str
        st.session_state.pop("report", None)
    st.rerun()


def _save_to_history(report: Dict[str, Any]):
    """保存分析记录到历史"""
    history = st.session_state.get("analysis_history", [])
    now = datetime.now().strftime("%m-%d %H:%M")

    if report.get("portfolio"):
        # 多基金组合
        codes = report.get("codes", [])
        history.append({
            "code": f"{len(codes)}只组合",
            "codes": codes,
            "name": "",
            "mode": "hold",
            "time": now,
        })
    else:
        overview = report.get("overview")
        history.append({
            "code": report.get("code", ""),
            "name": overview.fund_name if overview else "",
            "mode": report.get("mode", "buy"),
            "time": now,
        })

    # 最多保留 20 条
    st.session_state["analysis_history"] = history[-20:]


# ============================================================
# 日志
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main_v2")


# ============================================================
# 路由页面
# ============================================================

def show_home():
    """首页：拟买入 vs 已买入选择"""
    st.markdown("## 基金穿透式分析")
    st.markdown("")
    st.markdown("您想分析还没买入的基金，还是分析已经买入的一只或多只基金？")
    st.markdown("")

    # 使用量检查
    from data_loader.user_tracker import get_visitor_id, check_usage_limit
    visitor_id = get_visitor_id()
    can_use, remaining = check_usage_limit(visitor_id)

    if not can_use:
        st.error("⚠️ 内测次数已用完，公测时可以继续使用")
        if st.button("📝 提交内测反馈", type="secondary"):
            st.session_state["show_feedback"] = True
            st.rerun()
        _show_feedback_dialog()
        return

    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔍 还没买", use_container_width=True, type="primary"):
            st.session_state["mode"] = "buy"
            st.rerun()

    with col2:
        if st.button("📋 已买入", use_container_width=True):
            st.session_state["mode"] = "hold"
            st.rerun()

    st.markdown("")
    st.markdown("---")
    st.caption("⚠️ 输出结果基于用户主动设置的条件，由模型生成，无人工干预，"
               "历史数据仅供参考，不构成投资建议，用户应独立作出投资决策。")


def show_buy_page():
    """拟买入页面：单基金分析"""
    st.markdown("## 🔍 拟买入分析")
    if st.button("← 返回"):
        st.session_state.pop("mode", None)
        st.session_state.pop("report", None)
        st.rerun()

    st.markdown("")
    col_code, col_btn = st.columns([4, 1])
    with col_code:
        code = st.text_input(
            "基金代码",
            placeholder="输入6位基金代码，如 110011",
            key="buy_code_input",
        )
    with col_btn:
        st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
        clicked = st.button("开始解析", type="primary", use_container_width=True)

    if clicked and code:
        code = code.strip()
        if len(code) != 6 or not code.isdigit():
            st.error("请输入6位基金代码")
            return

        # 使用量检查
        from data_loader.user_tracker import get_visitor_id, check_usage_limit, increment_usage
        visitor_id = get_visitor_id()
        can_use, remaining = check_usage_limit(visitor_id)
        if not can_use:
            st.error("⚠️ 内测次数已用完，公测时可以继续使用")
            if st.button("📝 提交内测反馈", type="secondary"):
                st.session_state["show_feedback"] = True
                st.rerun()
            _show_feedback_dialog()
            return

        report = _run_single_analysis(code, mode="buy")
        st.session_state["report"] = report
        increment_usage(visitor_id)
        _save_to_history(report)

    # 展示报告
    report = st.session_state.get("report")
    if report:
        _render_report(report)


def show_hold_page():
    """已买入页面：1只或多只基金分析"""
    st.markdown("## 📋 已持有分析")
    if st.button("← 返回"):
        st.session_state.pop("mode", None)
        st.session_state.pop("report", None)
        st.rerun()

    st.markdown("")
    codes_input = st.text_area(
        "输入基金代码（多个代码用逗号或换行分隔）",
        placeholder="110011, 000001, 000029",
        height=80,
        key="hold_codes_input",
    )

    clicked = st.button("开始解析", type="primary")
    if clicked and codes_input:
        # 解析基金代码
        import re
        codes = re.findall(r"\d{6}", codes_input)
        codes = list(set(codes))  # 去重

        if not codes:
            st.error("未检测到有效的6位基金代码")
            return

        if len(codes) > 10:
            st.error("最多同时分析10只基金")
            return

        # 使用量检查
        from data_loader.user_tracker import get_visitor_id, check_usage_limit, increment_usage
        visitor_id = get_visitor_id()
        can_use, remaining = check_usage_limit(visitor_id)
        if not can_use:
            st.error("⚠️ 内测次数已用完，公测时可以继续使用")
            if st.button("📝 提交内测反馈", type="secondary"):
                st.session_state["show_feedback"] = True
                st.rerun()
            _show_feedback_dialog()
            return

        st.session_state["codes"] = codes

        if len(codes) == 1:
            report = _run_single_analysis(codes[0], mode="hold")
            st.session_state["report"] = report
        else:
            report = _run_portfolio_analysis(codes)
            st.session_state["report"] = report

        increment_usage(visitor_id)
        _save_to_history(report)

    # 展示报告
    report = st.session_state.get("report")
    if report:
        _render_report(report)


# ============================================================
# 分析调度
# ============================================================

def _run_single_analysis(code: str, mode: str = "buy") -> Dict[str, Any]:
    """
    单基金分析入口（带渐进加载状态）。

    Returns:
        dict 包含 overview / stock / rate_bond / credit_bond / cb
    """
    from models.schema_v2 import (
        FundAssetOverview, StockAssetMetrics,
        RateBondMetrics, CreditBondMetrics, CBMetrics,
    )
    from data_loader.equity_loader import (
        load_basic_info, load_nav,
        load_stock_holdings,
    )
    from data_loader.bond_loader import load_bond_holdings, load_treasury_yields
    from data_loader.equity_loader import build_benchmark
    from processor.data_cleaner import clean_nav_data
    from data_loader.fund_manager_loader import get_manager_info
    from engine.equity_engine_v2 import run_stock_analysis
    from engine.bond_rate_engine import run_rate_bond_analysis
    from engine.bond_credit_engine import run_credit_bond_analysis
    from engine.cb_engine_v2 import run_cb_analysis

    status = st.status("**正在分析...**", expanded=True)
    result = {"code": code, "mode": mode, "warnings": []}

    # === 1. 基础信息 ===
    status.markdown("📋 加载基金基础信息...")
    logger.info(f"[{_tag(code)}] 加载基础信息...")
    try:
        basic = load_basic_info(code)
    except Exception as e:
        logger.error(f"[{_tag(code)}] 基础信息加载失败: {e}")
        result["error"] = f"基金 {code} 信息加载失败"
        status.update(label="❌ 分析失败", state="error", expanded=False)
        return result

    # 检查成立是否满 1 年
    inception_date = basic.establish_date
    if inception_date:
        try:
            est = pd.to_datetime(inception_date)
            if (datetime.now() - est.to_pydatetime()).days < 365:
                result["fund_too_new"] = True
                # 查找同经理其他基金
                manager_info = get_manager_info(code)
                manager_names = manager_info.get("manager_names", [])
                alternatives = _find_manager_other_funds(code, manager_names)
                result["alternative_funds"] = alternatives
                result["manager_names"] = manager_names
                status.update(label="✅ 分析完成", state="complete", expanded=False)
                return result
        except Exception:
            pass

    # === 2. 净值 ===
    status.markdown("📈 加载净值数据...")
    logger.info(f"[{_tag(code)}] 加载净值...")
    try:
        nav_raw = load_nav(code)
        nav_clean = clean_nav_data(nav_raw)
    except Exception as e:
        logger.error(f"[{_tag(code)}] 净值加载失败: {e}")
        nav_clean = None

    # === 3. 持仓 ===
    status.markdown("📊 加载持仓数据...")
    logger.info(f"[{_tag(code)}] 加载持仓...")
    holdings = None
    try:
        # 股票持仓
        holdings = load_stock_holdings(code)
        # 债券持仓（含资产配置和债券分类）
        bond_holdings = load_bond_holdings(code)
        if bond_holdings:
            # 合并资产配置比例
            if bond_holdings.stock_ratio > 0:
                holdings.stock_ratio = bond_holdings.stock_ratio
            if bond_holdings.bond_ratio > 0:
                holdings.bond_ratio = bond_holdings.bond_ratio
            if bond_holdings.cash_ratio > 0:
                holdings.cash_ratio = bond_holdings.cash_ratio
            holdings.cb_ratio = bond_holdings.cb_ratio
            holdings.bond_details = bond_holdings.bond_details
            holdings.bond_classification = bond_holdings.bond_classification
            holdings.asset_allocation = bond_holdings.asset_allocation
    except Exception as e:
        logger.warning(f"[{_tag(code)}] 持仓加载部分失败: {e}")

    if holdings is None:
        from models.schema import HoldingsData
        holdings = HoldingsData(symbol=code)

    # === 4. 基准 + 国债收益率 ===
    status.markdown("📊 加载基准与利率数据...")
    logger.info(f"[{_tag(code)}] 加载基准数据...")
    benchmark = None
    yield_data = None
    try:
        benchmark = build_benchmark(basic, "20200101", datetime.now().strftime("%Y%m%d"))
    except Exception as e:
        logger.warning(f"[{_tag(code)}] 基准加载失败: {e}")

    try:
        yield_data = load_treasury_yields("20230101", datetime.now().strftime("%Y%m%d"))
    except Exception as e:
        logger.warning(f"[{_tag(code)}] 国债收益率加载失败: {e}")

    # === 5. 经理信息 ===
    try:
        manager_info = get_manager_info(code)
    except Exception as e:
        logger.warning(f"[{_tag(code)}] 经理信息加载失败: {e}")
        manager_info = {
            "manager_names": [], "manager_str": "未知",
            "manager_start_date": "", "tenure_years": None,
            "tenure_years_max": None, "cum_days": None,
            "is_multi_manager": False, "is_new_manager": False,
            "is_stable": False, "manager_risk_flag": "任职信息待补充",
        }

    # === 6. 构建 Overview === (show_buy_page)
    status.markdown("🔍 构建资产概览...")
    logger.info(f"[{_tag(code)}] 构建资产概览...")
    overview = _build_overview(code, basic, holdings, manager_info)
    result["overview"] = overview

    # === 7. 10年国债收益率（用于ERP） ===
    yield_10y = None
    if yield_data and yield_data.df is not None and not yield_data.df.empty:
        latest = yield_data.df.iloc[-1]
        from engine.bond_rate_engine import _safe_float_val
        yield_10y = _safe_float_val(latest.get("yield_10y"))

    # === 8. 各资产维度分析 ===

    # 股票
    if overview.has_stock and nav_clean:
        status.markdown("📈 分析股票维度...")
        logger.info(f"[{_tag(code)}] 股票维度分析...")
        try:
            result["stock"] = run_stock_analysis(
                nav=nav_clean,
                benchmark=benchmark,
                holdings=holdings,
                mode=mode,
                yield_10y=yield_10y,
                fund_code=code,
            )
        except Exception as e:
            logger.error(f"[{_tag(code)}] 股票分析失败: {e}")
            result["warnings"].append(f"股票分析失败: {e}")

    # 利率债
    if overview.has_rate_bond and nav_clean:
        status.markdown("🏦 分析利率债维度...")
        logger.info(f"[{_tag(code)}] 利率债维度分析...")
        try:
            result["rate_bond"] = run_rate_bond_analysis(
                nav=nav_clean,
                holdings=holdings,
                yield_data=yield_data,
                mode=mode,
            )
        except Exception as e:
            logger.error(f"[{_tag(code)}] 利率债分析失败: {e}")
            result["warnings"].append(f"利率债分析失败: {e}")

    # 信用债
    if overview.has_credit_bond:
        status.markdown("📜 分析信用债维度...")
        logger.info(f"[{_tag(code)}] 信用债维度分析...")
        try:
            result["credit_bond"] = run_credit_bond_analysis(
                holdings=holdings,
                yield_data=yield_data,
                mode=mode,
                fund_code=code,
            )
        except Exception as e:
            logger.error(f"[{_tag(code)}] 信用债分析失败: {e}")
            result["warnings"].append(f"信用债分析失败: {e}")

    # 可转债
    if overview.has_cb:
        status.markdown("🔄 分析可转债维度...")
        logger.info(f"[{_tag(code)}] 可转债维度分析...")
        try:
            result["cb"] = run_cb_analysis(
                holdings=holdings,
                nav=nav_clean,
                mode=mode,
            )
        except Exception as e:
            logger.error(f"[{_tag(code)}] 可转债分析失败: {e}")
            result["warnings"].append(f"可转债分析失败: {e}")

    logger.info(f"[{_tag(code)}] 分析完成")
    status.update(label="✅ 分析完成", state="complete", expanded=False)
    return result


def _run_portfolio_analysis(codes: List[str]) -> Dict[str, Any]:
    """多基金组合分析（带渐进加载）"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    status = st.status(f"**正在分析 {len(codes)} 只基金...**", expanded=True)
    results = {}

    # 并发分析每只基金
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_run_single_analysis_silent, code, "hold"): code
            for code in codes
        }
        done_count = 0
        for future in as_completed(futures, timeout=120):
            code = futures[future]
            try:
                results[code] = future.result()
            except Exception as e:
                results[code] = {"code": code, "error": str(e)}
            done_count += 1
            status.markdown(f"📊 已完成 {done_count}/{len(codes)} 只基金...")

    status.markdown("🔗 构建组合指标...")
    # 构建组合指标
    portfolio = _build_portfolio_metrics(results)

    status.update(label=f"✅ {len(codes)} 只基金分析完成", state="complete", expanded=False)

    return {
        "mode": "portfolio",
        "codes": codes,
        "sub_reports": results,
        "portfolio": portfolio,
    }


def _run_single_analysis_silent(code: str, mode: str = "buy") -> Dict[str, Any]:
    """单基金分析（静默模式，不显示 st.status，用于组合并发调用）"""
    from models.schema_v2 import (
        FundAssetOverview, StockAssetMetrics,
        RateBondMetrics, CreditBondMetrics, CBMetrics,
    )
    from data_loader.equity_loader import (
        load_basic_info, load_nav,
        load_stock_holdings,
    )
    from data_loader.bond_loader import load_bond_holdings, load_treasury_yields
    from data_loader.equity_loader import build_benchmark
    from processor.data_cleaner import clean_nav_data
    from data_loader.fund_manager_loader import get_manager_info
    from engine.equity_engine_v2 import run_stock_analysis
    from engine.bond_rate_engine import run_rate_bond_analysis
    from engine.bond_credit_engine import run_credit_bond_analysis
    from engine.cb_engine_v2 import run_cb_analysis

    result = {"code": code, "mode": mode, "warnings": []}

    try:
        basic = load_basic_info(code)
    except Exception as e:
        logger.error(f"[{code}] 基础信息加载失败: {e}")
        result["error"] = f"基金 {code} 信息加载失败"
        return result

    inception_date = basic.establish_date
    if inception_date:
        try:
            est = pd.to_datetime(inception_date)
            if (datetime.now() - est.to_pydatetime()).days < 365:
                result["fund_too_new"] = True
                return result
        except Exception:
            pass

    try:
        nav_raw = load_nav(code)
        nav_clean = clean_nav_data(nav_raw)
    except Exception:
        nav_clean = None

    holdings = None
    try:
        holdings = load_stock_holdings(code)
        bond_holdings = load_bond_holdings(code)
        if bond_holdings:
            if bond_holdings.stock_ratio > 0:
                holdings.stock_ratio = bond_holdings.stock_ratio
            if bond_holdings.bond_ratio > 0:
                holdings.bond_ratio = bond_holdings.bond_ratio
            if bond_holdings.cash_ratio > 0:
                holdings.cash_ratio = bond_holdings.cash_ratio
            holdings.cb_ratio = bond_holdings.cb_ratio
            holdings.bond_details = bond_holdings.bond_details
            holdings.bond_classification = bond_holdings.bond_classification
            holdings.asset_allocation = bond_holdings.asset_allocation
    except Exception:
        pass

    if holdings is None:
        from models.schema import HoldingsData
        holdings = HoldingsData(symbol=code)

    benchmark = None
    yield_data = None
    try:
        benchmark = build_benchmark(basic, "20200101", datetime.now().strftime("%Y%m%d"))
    except Exception:
        pass

    try:
        yield_data = load_treasury_yields("20230101", datetime.now().strftime("%Y%m%d"))
    except Exception:
        pass

    try:
        manager_info = get_manager_info(code)
    except Exception:
        manager_info = {
            "manager_names": [], "manager_str": "未知",
            "manager_start_date": "", "tenure_years": None,
            "tenure_years_max": None, "cum_days": None,
            "is_multi_manager": False, "is_new_manager": False,
            "is_stable": False, "manager_risk_flag": "任职信息待补充",
        }

    overview = _build_overview(code, basic, holdings, manager_info)
    result["overview"] = overview

    yield_10y = None
    if yield_data and yield_data.df is not None and not yield_data.df.empty:
        latest = yield_data.df.iloc[-1]
        from engine.bond_rate_engine import _safe_float_val
        yield_10y = _safe_float_val(latest.get("yield_10y"))

    if overview.has_stock and nav_clean:
        try:
            result["stock"] = run_stock_analysis(
                nav=nav_clean, benchmark=benchmark,
                holdings=holdings, mode=mode, yield_10y=yield_10y,
                fund_code=code,
            )
        except Exception as e:
            result["warnings"].append(f"股票分析失败: {e}")

    if overview.has_rate_bond and nav_clean:
        try:
            result["rate_bond"] = run_rate_bond_analysis(
                nav=nav_clean, holdings=holdings,
                yield_data=yield_data, mode=mode,
            )
        except Exception as e:
            result["warnings"].append(f"利率债分析失败: {e}")

    if overview.has_credit_bond:
        try:
            result["credit_bond"] = run_credit_bond_analysis(
                holdings=holdings, yield_data=yield_data,
                mode=mode, fund_code=code,
            )
        except Exception as e:
            result["warnings"].append(f"信用债分析失败: {e}")

    if overview.has_cb:
        try:
            result["cb"] = run_cb_analysis(
                holdings=holdings, nav=nav_clean, mode=mode,
            )
        except Exception as e:
            result["warnings"].append(f"可转债分析失败: {e}")

    return result


def _build_portfolio_metrics(results: Dict[str, Any]) -> Dict[str, Any]:
    """构建多基金组合指标（增强版）"""
    from data_loader.equity_loader import load_nav
    from processor.data_cleaner import clean_nav_data

    portfolio = {
        "fund_list": [],
        "corr_matrix": None,
        "overlap_heatmap": None,
        "top_overlap_stocks": [],
        "diversification_score": None,
        "asset_alloc_summary": {},
        "per_fund_summary": [],
    }

    valid_reports = []
    for code, report in results.items():
        if report.get("error") or report.get("fund_too_new"):
            continue
        overview = report.get("overview")
        if overview:
            portfolio["fund_list"].append({
                "code": code,
                "name": overview.fund_name,
                "fund_type": overview.fund_type,
                "stock_ratio": overview.asset_allocation.get("股票", 0),
                "bond_ratio": overview.asset_allocation.get("债券", 0),
                "cb_ratio": getattr(overview, "cb_ratio", 0),
                "cash_ratio": overview.asset_allocation.get("现金", 0),
            })
            valid_reports.append(report)

    if len(valid_reports) < 2:
        # 单基金也构建 per_fund_summary
        if len(valid_reports) == 1:
            portfolio["per_fund_summary"] = _build_per_fund_summary(valid_reports)
        return portfolio

    # =============================================
    # 1. 相关性矩阵（缓存优先）
    # =============================================
    try:
        nav_dict = {}
        for report in valid_reports:
            code = report["code"]
            try:
                nav_raw = load_nav(code)
                nav_clean = clean_nav_data(nav_raw)
                if nav_clean and nav_clean.df is not None and not nav_clean.df.empty:
                    nav_clean.df["date"] = pd.to_datetime(nav_clean.df["date"])
                    # 最近 1 年
                    cutoff = datetime.now() - pd.Timedelta(days=365)
                    df_1y = nav_clean.df[nav_clean.df["date"] >= cutoff]
                    if len(df_1y) > 60:  # 至少 60 个交易日
                        nav_dict[code] = df_1y.set_index("date")["nav"].rename(code)
            except Exception:
                pass

        if len(nav_dict) >= 2:
            nav_combined = pd.DataFrame(nav_dict).dropna()
            rets = nav_combined.pct_change().dropna()
            corr = rets.corr()
            portfolio["corr_matrix"] = corr

            # 分散化评分：1 - 平均相关性（归一化到 0~100）
            avg_corr = (corr.values.sum() - np.trace(corr.values)) / (corr.shape[0] * (corr.shape[0] - 1))
            portfolio["avg_corr"] = avg_corr
            portfolio["diversification_score"] = max(0, (1 - avg_corr) * 100)

            # 最小方差组合权重（解析解）
            try:
                cov = rets.cov().values
                n = cov.shape[0]
                inv_cov = np.linalg.pinv(cov)
                w = inv_cov @ np.ones(n)
                w = w / w.sum()
                # 组合波动率
                port_vol = np.sqrt(w @ cov @ w) * np.sqrt(252) * 100
                portfolio["min_var_vol"] = port_vol
                portfolio["min_var_weights"] = dict(zip(corr.columns.tolist(), w.round(4).tolist()))
            except Exception:
                pass

    except Exception as e:
        logger.warning(f"相关性矩阵计算失败: {e}")

    # =============================================
    # 2. 持仓重叠分析（增强版）
    # =============================================
    all_stocks = {}
    for report in valid_reports:
        code = report["code"]
        stock_metrics = report.get("stock")
        if stock_metrics and stock_metrics.top10_details:
            for s in stock_metrics.top10_details:
                sname = s.get("name", "")
                if not sname:
                    continue
                ratio = _safe_float(s.get("ratio", 0)) or 0
                if sname not in all_stocks:
                    all_stocks[sname] = {
                        "funds": [], "codes": [],
                        "total_ratio": 0, "details": [],
                        "max_ratio": 0,
                    }
                all_stocks[sname]["funds"].append(code)
                all_stocks[sname]["codes"].append(code)
                all_stocks[sname]["total_ratio"] += ratio
                all_stocks[sname]["max_ratio"] = max(all_stocks[sname]["max_ratio"], ratio)
                all_stocks[sname]["details"].append(s)

    # 重叠排序：先按基金数，再按总权重
    overlap_sorted = sorted(
        all_stocks.items(),
        key=lambda x: (len(x[1]["funds"]), x[1]["total_ratio"]),
        reverse=True,
    )
    portfolio["top_overlap_stocks"] = [
        {
            "name": name,
            "fund_count": len(info["funds"]),
            "funds": info["funds"],
            "total_ratio": info["total_ratio"],
            "max_ratio": info["max_ratio"],
            "details": info["details"],
        }
        for name, info in overlap_sorted[:15]
    ]

    # 重叠度矩阵（基金×基金，共享股票数）
    fund_codes = [r["code"] for r in valid_reports]
    code_to_idx = {c: i for i, c in enumerate(fund_codes)}
    overlap_matrix = np.zeros((len(fund_codes), len(fund_codes)), dtype=int)
    for name, info in all_stocks.items():
        if len(info["funds"]) >= 2:
            for i_idx, c1 in enumerate(info["funds"]):
                for c2 in info["funds"][i_idx + 1:]:
                    if c1 in code_to_idx and c2 in code_to_idx:
                        overlap_matrix[code_to_idx[c1]][code_to_idx[c2]] += 1
                        overlap_matrix[code_to_idx[c2]][code_to_idx[c1]] += 1
    portfolio["overlap_matrix"] = overlap_matrix
    portfolio["overlap_matrix_labels"] = fund_codes

    # =============================================
    # 3. 资产配置汇总
    # =============================================
    n_funds = len(valid_reports)
    alloc = {"股票": 0, "债券": 0, "可转债": 0, "现金": 0}
    for fl in portfolio["fund_list"]:
        alloc["股票"] += fl.get("stock_ratio", 0)
        alloc["债券"] += fl.get("bond_ratio", 0)
        alloc["可转债"] += fl.get("cb_ratio", 0)
        alloc["现金"] += fl.get("cash_ratio", 0)
    for k in alloc:
        alloc[k] = round(alloc[k] / n_funds * 100, 1) if n_funds else 0
    portfolio["asset_alloc_summary"] = alloc

    # =============================================
    # 4. 单基金摘要（用于雷达图等）
    # =============================================
    portfolio["per_fund_summary"] = _build_per_fund_summary(valid_reports)

    return portfolio


def _build_per_fund_summary(valid_reports: List[Dict]) -> List[Dict]:
    """构建单基金摘要列表"""
    summaries = []
    for report in valid_reports:
        code = report["code"]
        overview = report.get("overview")
        stock = report.get("stock")
        rb = report.get("rate_bond")
        cb = report.get("cb")
        credit = report.get("credit_bond")

        s = {
            "code": code,
            "name": overview.fund_name if overview else code,
            "type": overview.fund_type if overview else "",
            "stock_ratio": (overview.asset_allocation.get("股票", 0) if overview else 0),
            "bond_ratio": (overview.asset_allocation.get("债券", 0) if overview else 0),
            "cb_ratio": getattr(overview, "cb_ratio", 0) if overview else 0,
            "ldays": getattr(stock, "ldays", None),
            "alpha_60d": getattr(stock, "alpha_60d", None),
            "max_drawdown_stock": getattr(stock, "max_drawdown", None),
            "dv01": getattr(rb, "dv01", None),
            "max_drawdown_rate": getattr(rb, "max_drawdown", None),
            "double_high_count": len(cb.double_high_list) if cb and cb.double_high_list else 0,
            "cb_delta_avg": getattr(cb, "delta_avg", None),
            "default_warning": bool(getattr(credit, "default_warning", "")),
        }
        summaries.append(s)
    return summaries


# ============================================================
# 构建辅助
# ============================================================

def _build_overview(code: str, basic, holdings, manager_info: dict) -> "FundAssetOverview":
    """构建资产概览"""
    from models.schema_v2 import FundAssetOverview

    # 资产配置
    asset_alloc = holdings.asset_allocation or {}
    if not asset_alloc:
        asset_alloc = {
            "股票": holdings.stock_ratio,
            "债券": holdings.bond_ratio,
            "现金": holdings.cash_ratio,
        }

    # 持有标记
    has_stock = holdings.stock_ratio > 0.05
    bond_class = holdings.bond_classification or {}
    has_rate_bond = bond_class.get("gov_bond", {}).get("ratio", 0) > 0.05
    has_credit_bond = (
        bond_class.get("credit_bond", {}).get("ratio", 0) > 0.05
        or bond_class.get("urban_construction", {}).get("ratio", 0) > 0.05
    )
    has_cb = holdings.cb_ratio > 0.05

    # 如果没有 bond_classification，从债券明细推断
    if not has_rate_bond and not has_credit_bond and holdings.bond_details:
        from engine.bond_rate_engine import _filter_rate_bonds
        rate_bonds = _filter_rate_bonds(holdings.bond_details)
        has_rate_bond = len(rate_bonds) > 0
        has_credit_bond = len(holdings.bond_details) - len(rate_bonds) > 0

    # 规模
    import re as _re
    aum = 0.0
    if basic.scale:
        try:
            # 兼容 "4.74亿"、"4.74亿元"、"2.02亿"、"100.50亿元"
            aum_str = _re.sub(r'[亿元]*', '', basic.scale).strip()
            aum = float(aum_str)
        except Exception:
            pass

    # 经理任职天数
    manager_days = manager_info.get("tenure_days", 0) or 0

    return FundAssetOverview(
        fund_name=basic.name,
        fund_code=code,
        fund_type=basic.type_raw,
        manager_name=manager_info.get("manager_str", basic.manager),
        manager_start_date=manager_info.get("manager_start_date", ""),
        manager_days=int(manager_days),
        aum=aum,
        inception_date=basic.establish_date,
        fee_total=basic.fee_total,
        benchmark_text=basic.benchmark_text,
        asset_allocation=asset_alloc,
        has_stock=has_stock,
        has_rate_bond=has_rate_bond,
        has_credit_bond=has_credit_bond,
        has_cb=has_cb,
        rate_bond_ratio=bond_class.get("gov_bond", {}).get("ratio", 0),
        credit_bond_ratio=bond_class.get("credit_bond", {}).get("ratio", 0),
        cb_ratio=holdings.cb_ratio,
    )


def _find_manager_other_funds(
    current_code: str, manager_names: List[str]
) -> List[Dict[str, str]]:
    """查找同经理管理的其他基金"""
    if not manager_names:
        return []

    try:
        from data_loader.fund_manager_loader import load_current_table
        df = load_current_table()
        if df.empty:
            return []

        others = []
        for name in manager_names:
            rows = df[(df["经理姓名"] == name) & (df["基金代码"] != current_code)]
            for _, row in rows.iterrows():
                fcode = str(row.get("基金代码", ""))
                fname = str(row.get("基金名称", ""))
                if fcode and fname:
                    others.append({"code": fcode, "name": fname, "manager": name})

        # 去重
        seen = set()
        unique = []
        for item in others:
            if item["code"] not in seen:
                seen.add(item["code"])
                unique.append(item)

        return unique[:10]
    except Exception:
        return []


# ============================================================
# UI 渲染
# ============================================================

def _render_report(report: Dict[str, Any]):
    """渲染完整报告"""

    mode = report.get("mode", "buy")

    # 免责声明
    _render_disclaimer()

    # 基金不满一年
    if report.get("fund_too_new"):
        _render_too_new(report)
        return

    # 组合模式
    if mode == "portfolio":
        _render_portfolio(report)
        return

    # 单基金模式（buy / hold）
    overview = report.get("overview")
    if not overview:
        st.error("分析失败，请检查基金代码")
        return

    # 板块 1：资产概览
    _render_overview(overview, mode)

    # 板块 2-5：按资产维度
    _render_stock_section(report.get("stock"), overview, mode)
    _render_rate_bond_section(report.get("rate_bond"), overview, mode)
    _render_credit_bond_section(report.get("credit_bond"), overview, mode)
    _render_cb_section(report.get("cb"), overview, mode)

    # 警告
    if report.get("warnings"):
        for w in report["warnings"]:
            st.warning(w)


def _render_disclaimer():
    """免责声明"""
    st.markdown(
        '<div class="disclaimer">'
        '⚠️ 输出结果基于用户主动设置的条件，由模型生成，无人工干预，'
        '历史数据仅供参考，不构成投资建议，用户应独立作出投资决策。'
        '</div>',
        unsafe_allow_html=True,
    )


def _render_too_new(report: Dict[str, Any]):
    """基金不满一年的提示"""
    code = report.get("code", "")
    alternatives = report.get("alternative_funds", [])
    manager_names = report.get("manager_names", [])

    st.warning(f"基金 {code} 成立不满一年，数据不具有参考性。")

    if alternatives:
        st.markdown(f"该基金经理（{' / '.join(manager_names)}）同时管理的其他基金：")
        for alt in alternatives:
            st.markdown(f"- **{alt['name']}**（{alt['code']}）")
        st.markdown("建议选择其中一只进行解析。")
    else:
        if manager_names:
            st.warning(
                f"该基金经理（{' / '.join(manager_names)}）仅负责这一只基金，"
                "数据不具有参考性。"
            )
        else:
            st.warning("该基金成立不满一年，且无法获取基金经理信息，数据不具有参考性。")


def _render_overview(overview: "FundAssetOverview", mode: str):
    """板块 1：基金资产概览"""
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown(f'<div class="card-title">📊 基金资产概览</div>', unsafe_allow_html=True)

        # 基金基本信息
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"**{overview.fund_name}**（{overview.fund_code}）")
            st.caption(overview.fund_type)
        with col2:
            st.markdown(f"**经理**：{overview.manager_name or '未知'}")
            if overview.manager_days > 0:
                years = overview.manager_days // 365
                days = overview.manager_days % 365
                st.caption(f"任职 {years}年{days}天")
        with col3:
            st.markdown(f"**规模**：{overview.aum:.2f} 亿元" if overview.aum > 0 else "**规模**：未知")
            st.caption(f"成立日期：{overview.inception_date}")

        # 业绩比较基准
        if overview.benchmark_text:
            st.caption(f"业绩比较基准：{overview.benchmark_text[:80]}{'...' if len(overview.benchmark_text) > 80 else ''}")

        # 资产结构
        st.markdown("")
        _render_asset_allocation(overview)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_asset_allocation(overview: "FundAssetOverview"):
    """渲染资产结构"""
    alloc = overview.asset_allocation
    if not alloc:
        return

    # 简洁展示：用彩色标签
    total = sum(alloc.values())
    if total <= 0:
        return

    color_map = {
        "股票": ("#e74c3c", "#fdeaea"),
        "债券": ("#3498db", "#ebf5fb"),
        "现金": ("#95a5a6", "#f2f3f4"),
        "其他": ("#f39c12", "#fef9e7"),
    }

    badges = []
    for name, ratio in alloc.items():
        pct = ratio / total * 100 if total > 0 else 0
        if pct < 1:
            continue
        fg, bg = color_map.get(name, ("#333", "#f5f5f5"))
        badges.append(
            f'<span class="asset-badge" style="color:{fg};background:{bg}">'
            f'{name} {pct:.1f}%</span>'
        )

    if badges:
        st.markdown(" ".join(badges), unsafe_allow_html=True)


def _render_stock_section(stock_metrics, overview, mode: str):
    """板块：股票"""
    if not overview.has_stock:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        label = "📈 股票"
        st.markdown(f'<div class="card-title">{label}</div>', unsafe_allow_html=True)

        if stock_metrics is None:
            st.markdown('<span class="empty-section">股票数据加载失败</span>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            return

        metrics_to_show = _get_stock_metrics_display(stock_metrics, mode)

        # 分两行展示（每行 3 个）
        _render_metrics_grid(metrics_to_show[:6])
        if len(metrics_to_show) > 6:
            _render_metrics_grid(metrics_to_show[6:])

        # ---- Top10 持仓股估值气泡图 ----
        if stock_metrics.top10_details:
            _render_top10_valuation_bubble(stock_metrics.top10_details)

        # ---- 已持有模式增强 ----
        if mode == "hold":
            _render_stock_hold_enhanced(stock_metrics)

        st.markdown('</div>', unsafe_allow_html=True)


def _get_stock_metrics_display(stock: Any, mode: str) -> List[Dict]:
    """获取股票指标展示列表"""
    items = []

    # 通用
    if stock.alpha_annual is not None:
        items.append({
            "label": "年化 Alpha",
            "value": f"{stock.alpha_annual:.2%}",
            "desc": "相对业绩基准的超额收益（年化）",
            "trend": "positive" if stock.alpha_annual > 0 else "negative",
        })

    if stock.r_squared is not None:
        items.append({
            "label": "风格 R²",
            "value": f"{stock.r_squared:.2%}",
            "desc": "与基准的相关性，越高越稳定",
            "trend": "positive" if stock.r_squared > 0.7 else "neutral",
        })

    if stock.tri_deviation is not None:
        items.append({
            "label": "全收益脱水",
            "value": f"{stock.tri_deviation:+.2f}%",
            "desc": "相对基准累计超额收益（含权TRI偏离度）",
            "trend": "positive" if stock.tri_deviation > 0 else "negative",
        })

    if stock.pe_percentile is not None:
        items.append({
            "label": "估值水位 (PE分位)",
            "value": f"{stock.pe_percentile:.1f}%",
            "desc": "Top10 持仓股加权 PE 历史分位",
            "trend": "negative" if stock.pe_percentile > 70 else "positive" if stock.pe_percentile < 30 else "neutral",
        })

    if stock.weighted_peg is not None:
        items.append({
            "label": "加权 PEG",
            "value": f"{stock.weighted_peg:.2f}",
            "desc": "PE/G，<1 为低估，>2 为高估",
            "trend": "positive" if stock.weighted_peg < 1 else "negative" if stock.weighted_peg > 2 else "neutral",
        })

    if stock.erp is not None:
        items.append({
            "label": "股权溢价 (ERP)",
            "value": f"{stock.erp:+.2f}%",
            "desc": "1/PE - 10年债收益率，正值表示股票相对债券有吸引力",
            "trend": "positive" if stock.erp > 0 else "negative",
        })

    if stock.ldays is not None:
        items.append({
            "label": "流动性穿透 (Ldays)",
            "value": f"{stock.ldays:.1f} 天",
            "desc": "Top10 持仓股变现天数，越短流动性越好",
            "trend": "positive" if stock.ldays < 5 else "negative" if stock.ldays > 15 else "neutral",
        })

    if stock.blackswan_loss is not None:
        items.append({
            "label": "黑天鹅压测",
            "value": f"{stock.blackswan_loss:.1f}%",
            "desc": "PE 回归历史低位时的预期跌幅",
            "trend": "negative",
        })

    # 已持有额外
    if mode == "hold":
        if stock.excess_drawdown is not None:
            items.append({
                "label": "超额回撤",
                "value": f"{stock.excess_drawdown:.2f}%",
                "desc": "几何超额收益的最大回撤",
                "trend": "negative",
            })

        if stock.stop_profit_signal:
            items.append({
                "label": "Alpha 信号",
                "value": stock.stop_profit_signal,
                "desc": "",
                "trend": "negative",
            })

    return items


def _render_top10_valuation_bubble(top10_details: List[Dict[str, Any]]):
    """
    渲染 Top10 持仓股 PE 分位气泡图。

    X轴: PE(TTM)   Y轴: PE历史分位(%)
    气泡大小: 持仓占比   颜色: PEG (<1绿, 1-2黄, >2红)
    """
    # 过滤有效数据：必须有 PE 和 PE分位
    valid = []
    for s in top10_details:
        pe = _safe_float(s.get("pe_ttm"))
        pct = _safe_float(s.get("pe_percentile"))
        name = s.get("name", "")
        ratio = _safe_float(s.get("ratio") or s.get("占净值比例", 0))
        peg = _safe_float(s.get("peg"))
        if pe is not None and pe > 0 and pct is not None and name:
            valid.append({
                "name": name,
                "pe": pe,
                "pct": pct,
                "ratio": ratio or 0.5,  # 默认最小气泡
                "peg": peg,
            })

    if not valid:
        return

    # 气泡大小：持仓占比映射到 15~55px 范围
    ratios = [v["ratio"] for v in valid]
    r_min, r_max = min(ratios), max(ratios)
    r_range = r_max - r_min if r_max > r_min else 1
    sizes = [15 + (v["ratio"] - r_min) / r_range * 40 for v in valid]

    # 颜色：PEG 分段
    colors = []
    for v in valid:
        peg = v["peg"]
        if peg is not None:
            if peg < 1:
                colors.append("#27ae60")   # 绿色：低估成长
            elif peg <= 2:
                colors.append("#f39c12")   # 黄色：合理
            else:
                colors.append("#e74c3c")   # 红色：高估
        else:
            colors.append("#95a5a6")       # 灰色：数据缺失

    # 气泡边框颜色（稍深）
    border_colors = [
        "#1e8449" if c == "#27ae60"
        else "#d68910" if c == "#f39c12"
        else "#c0392b" if c == "#e74c3c"
        else "#7f8c8d"
        for c in colors
    ]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=[v["pe"] for v in valid],
        y=[v["pct"] for v in valid],
        mode="markers+text",
        marker=dict(
            size=sizes,
            color=colors,
            line=dict(width=1.5, color=border_colors),
            opacity=0.85,
        ),
        text=[v["name"] for v in valid],
        textposition="top center",
        textfont=dict(size=10, color="#333"),
        hovertemplate=(
            "<b>%{text}</b><br>"
            "PE(TTM): %{x:.1f}<br>"
            "PE分位: %{y:.1f}%<br>"
            "PEG: %{customdata[0]}<br>"
            "持仓占比: %{customdata[1]:.2f}%<extra></extra>"
        ),
        customdata=[
            [v["peg"] if v["peg"] else "N/A", v["ratio"]]
            for v in valid
        ],
    ))

    # 基准线：PE分位 30% / 70%
    x_range = [v["pe"] for v in valid]
    x_pad = (max(x_range) - min(x_range)) * 0.05

    fig.add_hline(y=30, line_dash="dash", line_color="#27ae60", opacity=0.5,
                  annotation_text="低估线 30%", annotation_position="top left",
                  annotation_font=dict(size=9, color="#27ae60"))
    fig.add_hline(y=70, line_dash="dash", line_color="#e74c3c", opacity=0.5,
                  annotation_text="高估线 70%", annotation_position="bottom left",
                  annotation_font=dict(size=9, color="#e74c3c"))

    fig.update_layout(
        xaxis_title="PE（市盈率 TTM）",
        yaxis_title="PE 历史分位 (%)",
        xaxis=dict(range=[0, max(x_range) + x_pad + 5]),
        yaxis=dict(range=[0, 105]),
        height=380,
        margin=dict(t=30, b=40, l=55, r=20),
        hovermode="closest",
        plot_bgcolor="#fafafa",
    )

    # 颜色图例
    legend_html = (
        '<div style="display:flex;gap:16px;font-size:12px;color:#555;'
        'margin-top:-8px;margin-bottom:8px;padding:0 4px;">'
        '<span><span style="color:#27ae60">●</span> PEG &lt; 1 低估</span>'
        '<span><span style="color:#f39c12">●</span> PEG 1~2 合理</span>'
        '<span><span style="color:#e74c3c">●</span> PEG &gt; 2 高估</span>'
        '<span><span style="color:#95a5a6">●</span> PEG 缺失</span>'
        '<span style="color:#aaa">⬤ 气泡大小 = 持仓占比</span>'
        '</div>'
    )
    st.markdown(legend_html, unsafe_allow_html=True)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _render_stock_hold_enhanced(stock: Any):
    """已持有模式·股票板块增强展示"""

    # 1. 超额回撤高亮条
    if stock.excess_drawdown is not None:
        dd = stock.excess_drawdown
        bar_color = "#e74c3c" if dd < -15 else "#f39c12" if dd < -5 else "#27ae60"
        bar_width = min(abs(dd) / 30 * 100, 100)  # 30% 映射到满格
        st.markdown(
            f'<div style="margin:8px 0;">'
            f'<div style="font-size:12px;color:#888;margin-bottom:4px;">超额回撤（几何超额最大回撤）</div>'
            f'<div style="background:#f0f0f0;border-radius:6px;height:24px;position:relative;overflow:hidden;">'
            f'<div style="background:{bar_color};width:{bar_width}%;height:100%;border-radius:6px;"></div>'
            f'<span style="position:absolute;left:8px;top:2px;font-size:13px;font-weight:600;color:#fff;">{dd:.2f}%</span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # 2. Alpha 信号标签
    if stock.stop_profit_signal:
        signal = stock.stop_profit_signal
        if "由正转负" in signal:
            tag_color, tag_bg = "#e74c3c", "#fdeaea"
        elif "衰减" in signal:
            tag_color, tag_bg = "#f39c12", "#fef9e7"
        else:
            tag_color, tag_bg = "#e67e22", "#fef5e7"
        st.markdown(
            f'<div style="margin:6px 0;">'
            f'<span style="display:inline-block;padding:4px 12px;border-radius:6px;'
            f'background:{tag_bg};color:{tag_color};font-size:13px;font-weight:600;">'
            f'⚠️ {signal}</span></div>',
            unsafe_allow_html=True,
        )

    # 3. PE 极端值警告
    if stock.pe_extreme:
        st.markdown(
            '<span style="display:inline-block;padding:4px 12px;border-radius:6px;'
            'background:#fff3cd;color:#856404;font-size:12px;font-weight:500;">'
            '⚡ Top10 持仓 PE 处于历史极端区间</span>',
            unsafe_allow_html=True,
        )

    # 4. 滚动 Alpha 趋势图
    if stock.alpha_trend_df is not None and not stock.alpha_trend_df.empty:
        import plotly.graph_objects as go

        df = stock.alpha_trend_df.tail(250)  # 最近 1 年
        fig = go.Figure()

        # Alpha 折线
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["alpha"],
            mode="lines", name="滚动 Alpha（60日）",
            line=dict(color="#e74c3c", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(231,76,60,0.08)",
        ))

        # 零线
        fig.add_hline(y=0, line_dash="dash", line_color="#95a5a6", line_width=1)

        fig.update_layout(
            height=200, margin=dict(l=40, r=20, t=10, b=30),
            xaxis_title="", yaxis_title="Alpha（年化）",
            template="simple_white",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    # 5. 风格 R² 热力图
    if stock.r2_matrix_df is not None and not stock.r2_matrix_df.empty:
        import plotly.graph_objects as go

        r2_df = stock.r2_matrix_df.sort_values("R²", ascending=False)
        fig = go.Figure(go.Bar(
            x=r2_df["R²"].values,
            y=r2_df.index.tolist(),
            orientation="h",
            marker_color=[
                "#27ae60" if v > 0.7 else "#f39c12" if v > 0.4 else "#e74c3c"
                for v in r2_df["R²"].values
            ],
        ))

        # 添加 R² = 0.7 参考线
        fig.add_vline(x=0.7, line_dash="dash", line_color="#95a5a6")
        fig.add_annotation(
            x=0.7, y=len(r2_df) - 0.5,
            text="R²=0.7", showarrow=False,
            font=dict(size=10, color="#95a5a6"),
        )

        fig.update_layout(
            height=max(160, len(r2_df) * 35),
            margin=dict(l=80, r=20, t=10, b=30),
            xaxis_title="R²", yaxis_title="",
            template="simple_white",
            showlegend=False,
            xaxis_range=[0, 1],
        )
        st.plotly_chart(fig, use_container_width=True)

        # 风格一致性文字说明
        if stock.style_consistency_r2 is not None:
            r2_val = stock.style_consistency_r2
            if r2_val > 0.8:
                desc = f"风格稳定，R²={r2_val:.2f}"
                c = "#27ae60"
            elif r2_val > 0.5:
                desc = f"风格较稳定，R²={r2_val:.2f}"
                c = "#f39c12"
            else:
                desc = f"风格漂移风险较高，R²={r2_val:.2f}"
                c = "#e74c3c"
            st.markdown(
                f'<span style="font-size:12px;color:{c};">📊 风格一致性：{desc}</span>',
                unsafe_allow_html=True,
            )


def _render_rate_bond_section(rate_bond: Any, overview, mode: str):
    """板块：利率债"""
    if not overview.has_rate_bond:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">🏦 利率债</div>', unsafe_allow_html=True)

        if rate_bond is None:
            st.markdown('<span class="empty-section">利率债数据加载失败</span>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            return

        items = []

        if rate_bond.duration is not None:
            items.append({
                "label": "加权久期",
                "value": f"{rate_bond.duration:.2f} 年",
                "desc": "利率变动 1% → 净值变动约 1%×久期",
            })

        if rate_bond.dv01 is not None:
            items.append({
                "label": "DV01（利率敏感度）",
                "value": f"{rate_bond.dv01:.2f} bp",
                "desc": "利率变动 1bp → 净值变动（基点）",
            })

        if rate_bond.term_spread is not None:
            items.append({
                "label": "期限利差 (10Y-2Y)",
                "value": f"{rate_bond.term_spread:.1f} bp",
                "desc": f"曲线形态：{rate_bond.yield_curve_shape or '未知'}",
            })

        if rate_bond.drawdown_recovery_days is not None:
            items.append({
                "label": "回撤修复天数",
                "value": f"{rate_bond.drawdown_recovery_days} 天",
                "desc": "最大回撤后恢复到前高所需天数",
            })

        if mode == "hold" and rate_bond.max_drawdown is not None:
            items.append({
                "label": "最大回撤",
                "value": f"{rate_bond.max_drawdown:.2f}%",
                "desc": "",
            })

        _render_metrics_grid(items)

        # ---- 已持有模式增强 ----
        if mode == "hold":
            _render_rate_bond_hold_enhanced(rate_bond)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_credit_bond_section(credit_bond: Any, overview, mode: str):
    """板块：信用债"""
    if not overview.has_credit_bond:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">📜 信用债</div>', unsafe_allow_html=True)

        if credit_bond is None:
            st.markdown('<span class="empty-section">信用债数据加载失败</span>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            return

        items = []

        if credit_bond.ytm is not None:
            items.append({
                "label": "静态收益率 (YTM)",
                "value": f"{credit_bond.ytm:.2%}",
                "desc": "组合加权到期收益率",
            })

        if credit_bond.avg_rating:
            items.append({
                "label": "平均信用评级",
                "value": credit_bond.avg_rating,
                "desc": "Top持仓加权信用评级",
            })

        if credit_bond.credit_spread_latest is not None:
            trend_emoji = "↑" if credit_bond.credit_spread_trend == "走阔" else "↓" if credit_bond.credit_spread_trend == "收窄" else "→"
            items.append({
                "label": f"信用利差 {trend_emoji}",
                "value": f"{credit_bond.credit_spread_latest:.0f} bp",
                "desc": f"走势：{credit_bond.credit_spread_trend or '未知'}",
            })

        if mode == "hold":
            if credit_bond.default_warning:
                items.append({
                    "label": "行业风险预警",
                    "value": credit_bond.default_warning,
                    "desc": "城投/地产/弱资质敞口检测",
                    "trend": "negative",
                })

        _render_metrics_grid(items)

        # ---- 已持有模式增强 ----
        if mode == "hold":
            _render_credit_bond_hold_enhanced(credit_bond)

        # 信用利差走势图（通用）
        if credit_bond.credit_spread_df is not None and not credit_bond.credit_spread_df.empty:
            import plotly.graph_objects as go
            df = credit_bond.credit_spread_df.tail(250)  # 最近 1 年
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["credit_spread"],
                mode="lines", name="信用利差",
                line=dict(color="#3498db", width=1.5),
            ))
            fig.update_layout(
                height=200, margin=dict(l=40, r=20, t=10, b=30),
                xaxis_title="", yaxis_title="bp",
                template="simple_white",
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_cb_section(cb: Any, overview, mode: str):
    """板块：可转债"""
    if not overview.has_cb:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">🔄 可转债</div>', unsafe_allow_html=True)

        if cb is None:
            st.markdown('<span class="empty-section">可转债数据加载失败</span>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            return

        items = []

        if cb.avg_conv_price is not None:
            items.append({
                "label": "加权价格",
                "value": f"¥{cb.avg_conv_price:.1f}",
                "desc": "持仓转债加权均价",
            })

        if cb.conv_premium_rate is not None:
            items.append({
                "label": "转股溢价率",
                "value": f"{cb.conv_premium_rate:.1f}%",
                "desc": "溢价率越低，股债平衡性越好",
            })

        if cb.bond_floor_premium is not None:
            items.append({
                "label": "纯债溢价率",
                "value": f"{cb.bond_floor_premium:.1f}%",
                "desc": "高于债底的部分，越低越安全",
            })

        if cb.ytm is not None:
            items.append({
                "label": "加权 YTM",
                "value": f"{cb.ytm:.2%}",
                "desc": "到期收益率",
            })

        _render_metrics_grid(items)

        # ---- 已持有模式增强 ----
        if mode == "hold":
            _render_cb_hold_enhanced(cb)

        # 类股化提示（浅色折叠）
        if mode == "buy" and cb.stock_like_warning:
            with st.expander("查看提示"):
                st.markdown(cb.stock_like_warning)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio(report: Dict[str, Any]):
    """多基金组合报告（增强版）"""
    portfolio = report.get("portfolio", {})
    sub_reports = report.get("sub_reports", {})

    _render_disclaimer()

    # 板块 0：组合总览仪表盘
    _render_portfolio_dashboard(portfolio)

    # 板块 1：资产配置总览
    _render_portfolio_asset_alloc(portfolio)

    # 板块 2：相关性矩阵（增强）
    _render_portfolio_correlation(portfolio)

    # 板块 3：持仓重叠分析（增强）
    _render_portfolio_overlap(portfolio, sub_reports)

    # 板块 4：综合流动性
    _render_portfolio_liquidity(sub_reports)

    # 板块 5：组合久期
    _render_portfolio_duration(sub_reports)

    # 板块 6：单基金雷达图对比
    _render_portfolio_radar(portfolio)

    # 板块 7：单基金入口
    _render_portfolio_fund_list(portfolio)


def _render_portfolio_dashboard(portfolio: Dict[str, Any]):
    """组合总览仪表盘"""
    fund_list = portfolio.get("fund_list", [])
    if not fund_list:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">📊 组合总览</div>', unsafe_allow_html=True)

        # 基金数量和分散化评分
        n = len(fund_list)
        div_score = portfolio.get("diversification_score")
        avg_corr = portfolio.get("avg_corr")
        min_var_vol = portfolio.get("min_var_vol")

        cols = st.columns(3)
        with cols[0]:
            st.markdown(
                f'<div style="text-align:center;padding:12px;">'
                f'<div style="font-size:28px;font-weight:700;color:#2c3e50;">{n}</div>'
                f'<div style="font-size:12px;color:#888;">基金数量</div></div>',
                unsafe_allow_html=True,
            )
        with cols[1]:
            if div_score is not None:
                score_color = "#27ae60" if div_score > 60 else "#f39c12" if div_score > 30 else "#e74c3c"
                score_label = "优秀" if div_score > 60 else "良好" if div_score > 30 else "集中度高"
                st.markdown(
                    f'<div style="text-align:center;padding:12px;">'
                    f'<div style="font-size:28px;font-weight:700;color:{score_color};">{div_score:.0f}</div>'
                    f'<div style="font-size:12px;color:#888;">分散化评分 · {score_label}</div></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown('<div style="text-align:center;padding:12px;"><div style="font-size:12px;color:#aaa;">需 ≥2 只基金</div></div>', unsafe_allow_html=True)
        with cols[2]:
            if min_var_vol is not None:
                st.markdown(
                    f'<div style="text-align:center;padding:12px;">'
                    f'<div style="font-size:28px;font-weight:700;color:#2c3e50;">{min_var_vol:.1f}%</div>'
                    f'<div style="font-size:12px;color:#888;">最小方差组合年化波动</div></div>',
                    unsafe_allow_html=True,
                )
            elif avg_corr is not None:
                st.markdown(
                    f'<div style="text-align:center;padding:12px;">'
                    f'<div style="font-size:28px;font-weight:700;color:#2c3e50;">{avg_corr:.2f}</div>'
                    f'<div style="font-size:12px;color:#888;">平均相关性</div></div>',
                    unsafe_allow_html=True,
                )

        # 最小方差权重建议
        mv_w = portfolio.get("min_var_weights")
        if mv_w:
            st.markdown('<div style="margin-top:8px;padding:8px 12px;border-radius:6px;background:#ebf5fb;font-size:12px;color:#2c3e50;">', unsafe_allow_html=True)
            st.markdown(f'**💡 最优配置权重**（最小方差）')
            for code, w in sorted(mv_w.items(), key=lambda x: x[1], reverse=True):
                # 查找基金名称
                fname = code
                for f in fund_list:
                    if f["code"] == code:
                        fname = f["name"]
                        break
                bar_w = w * 100
                st.markdown(
                    f'<div style="margin:3px 0;display:flex;align-items:center;gap:8px;">'
                    f'<span style="min-width:100px;font-size:11px;">{fname}</span>'
                    f'<div style="flex:1;background:#e8e8e8;border-radius:4px;height:16px;position:relative;">'
                    f'<div style="background:#3498db;width:{bar_w}%;height:100%;border-radius:4px;"></div>'
                    f'<span style="position:absolute;left:6px;top:0;font-size:10px;color:#fff;">{w*100:.1f}%</span>'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio_asset_alloc(portfolio: Dict[str, Any]):
    """资产配置总览"""
    alloc = portfolio.get("asset_alloc_summary", {})
    if not alloc:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">🥧 资产配置总览</div>', unsafe_allow_html=True)

        import plotly.graph_objects as go

        labels = []
        values = []
        colors = []
        for name, val in alloc.items():
            if val > 0:
                labels.append(f"{name} {val:.1f}%")
                values.append(val)
        color_map = {"股票": "#e74c3c", "债券": "#3498db", "可转债": "#f39c12", "现金": "#95a5a6"}
        colors = [color_map.get(l.split()[0], "#bdc3c7") for l in labels]

        fig = go.Figure(go.Pie(
            labels=labels,
            values=values,
            marker_colors=colors,
            hole=0.5,
            textinfo="label",
            textposition="outside",
            textfont=dict(size=12),
        ))
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        # 集中度评估
        stock_val = alloc.get("股票", 0)
        bond_val = alloc.get("债券", 0) + alloc.get("可转债", 0)
        if stock_val > 70:
            level, lc = "偏股型，权益暴露较高", "#e74c3c"
        elif stock_val > 40:
            level, lc = "均衡型，股债搭配", "#27ae60"
        elif bond_val > 70:
            level, lc = "偏债型，风险较低", "#3498db"
        else:
            level, lc = "灵活配置型", "#f39c12"
        st.markdown(
            f'<span style="font-size:12px;color:{lc};">📐 组合风格：{level}</span>',
            unsafe_allow_html=True,
        )

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio_correlation(portfolio: Dict[str, Any]):
    """相关性矩阵（增强版）"""
    corr = portfolio.get("corr_matrix")
    if corr is None or corr.empty:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">🔗 基金相关性矩阵</div>', unsafe_allow_html=True)

        # 查找基金名称映射
        fund_list = portfolio.get("fund_list", [])
        name_map = {f["code"]: f["name"] for f in fund_list}

        import plotly.figure_factory as ff

        # 用名称做标签
        labels = []
        for c in corr.columns.tolist():
            labels.append(name_map.get(c, c)[:8])  # 截断长名称

        fig = ff.create_annotated_heatmap(
            z=corr.values.round(3).tolist(),
            x=labels, y=labels,
            annotation_text=corr.values.round(2).tolist(),
            colorscale=[
                [0, "#e74c3c"],
                [0.5, "#ffffff"],
                [1, "#3498db"],
            ],
            showscale=True,
            zmin=-1, zmax=1,
        )
        fig.update_layout(height=max(250, len(labels) * 50 + 80), margin=dict(l=80, r=30, t=10, b=80))
        fig.update_xaxes(side="bottom")
        st.plotly_chart(fig, use_container_width=True)

        # 高相关预警
        codes = corr.columns.tolist()
        high_corr_pairs = []
        for i in range(len(codes)):
            for j in range(i + 1, len(codes)):
                v = corr.values[i][j]
                if v > 0.85:
                    n1 = name_map.get(codes[i], codes[i])
                    n2 = name_map.get(codes[j], codes[j])
                    high_corr_pairs.append((n1, n2, v))

        if high_corr_pairs:
            st.markdown(
                '<div style="margin-top:8px;padding:8px 12px;border-radius:6px;background:#fdeaea;">'
                '<span style="font-weight:600;color:#e74c3c;font-size:13px;">⚠️ 高相关预警</span>',
                unsafe_allow_html=True,
            )
            for n1, n2, v in high_corr_pairs[:5]:
                st.markdown(
                    f'<span style="font-size:12px;color:#666;">'
                    f'{n1} ↔ {n2}：r = {v:.2f}（分散化效果有限）</span>',
                    unsafe_allow_html=True,
                )
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio_overlap(portfolio: Dict[str, Any], sub_reports: Dict[str, Any]):
    """持仓重叠分析（增强版）"""
    top_overlap = portfolio.get("top_overlap_stocks", [])
    overlap_matrix = portfolio.get("overlap_matrix")

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">🔲 持仓重叠分析</div>', unsafe_allow_html=True)

        # 1. 重叠度矩阵热力图
        if overlap_matrix is not None:
            labels_raw = portfolio.get("overlap_matrix_labels", [])
            fund_list = portfolio.get("fund_list", [])
            name_map = {f["code"]: f["name"] for f in fund_list}
            labels = [name_map.get(c, c)[:8] for c in labels_raw]

            if len(labels) >= 2:
                import plotly.figure_factory as ff
                fig = ff.create_annotated_heatmap(
                    z=overlap_matrix.tolist(),
                    x=labels, y=labels,
                    annotation_text=overlap_matrix.tolist(),
                    colorscale=[
                        [0, "#eafaf1"],
                        [0.5, "#fef9e7"],
                        [1, "#e74c3c"],
                    ],
                    showscale=True,
                    zmin=0,
                )
                fig.update_layout(
                    height=max(220, len(labels) * 50 + 60),
                    margin=dict(l=80, r=30, t=10, b=80),
                )
                fig.update_xaxes(side="bottom")
                st.plotly_chart(fig, use_container_width=True)

        # 2. 重叠股票列表（仅显示出现在≥2只基金的）
        if top_overlap:
            multi_overlap = [s for s in top_overlap if s["fund_count"] >= 2]
            if not multi_overlap:
                st.markdown('<span style="color:#27ae60;font-size:13px;">✅ 持仓无重叠，分散化良好</span>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
                return

            st.markdown('<div style="font-size:13px;font-weight:600;margin:8px 0 4px;">重叠最高的股票</div>', unsafe_allow_html=True)

            name_map = {f["code"]: f["name"] for f in portfolio.get("fund_list", [])}
            for item in multi_overlap[:8]:
                fc = item["fund_count"]
                total_r = item.get("total_ratio", 0)
                # 颜色按严重程度
                if fc >= 3:
                    row_bg = "#fdeaea"
                    border_c = "#e74c3c"
                elif fc == 2:
                    row_bg = "#fef9e7"
                    border_c = "#f39c12"
                else:
                    row_bg = "#f5f5f5"
                    border_c = "#ddd"

                fund_names = [name_map.get(c, c) for c in item["funds"]]
                st.markdown(
                    f'<div style="margin:4px 0;padding:8px 12px;border-radius:6px;'
                    f'background:{row_bg};border-left:3px solid {border_c};">'
                    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                    f'<span style="font-weight:600;font-size:13px;">{item["name"]}</span>'
                    f'<span style="font-size:11px;color:#888;">'
                    f'出现在 {fc} 只基金 · 累计权重 {total_r:.1f}%</span></div>'
                    f'<div style="font-size:11px;color:#888;margin-top:2px;">'
                    f'{"、".join(fund_names)}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            # 集中度评级
            multi_overlap = sum(1 for s in top_overlap if s["fund_count"] >= 2)
            total_stocks = len(top_overlap)
            if total_stocks > 0:
                overlap_ratio = multi_overlap / total_stocks
                if overlap_ratio > 0.5:
                    rating, rc = "重叠度高，建议调整", "#e74c3c"
                elif overlap_ratio > 0.2:
                    rating, rc = "重叠度中等", "#f39c12"
                else:
                    rating, rc = "重叠度低，分散良好", "#27ae60"
                st.markdown(
                    f'<span style="font-size:12px;color:{rc};font-weight:500;">'
                    f'📐 重叠度评估：{rating}（{multi_overlap}/{total_stocks} 只股票出现在多只基金中）</span>',
                    unsafe_allow_html=True,
                )

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio_liquidity(sub_reports: Dict[str, Any]):
    """组合流动性板块（增强版）"""
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">💧 综合流动性与变现评估</div>', unsafe_allow_html=True)

        ldays_items = []
        for code, report in sub_reports.items():
            overview = report.get("overview")
            stock = report.get("stock")
            if stock and stock.ldays is not None:
                fname = overview.fund_name if overview else code
                ldays_items.append({
                    "code": code,
                    "name": fname,
                    "ldays": stock.ldays,
                })

        if ldays_items:
            avg_ldays = np.mean([x["ldays"] for x in ldays_items])
            max_ldays = max(x["ldays"] for x in ldays_items)
            color = "#27ae60" if avg_ldays < 10 else "#f39c12" if avg_ldays < 30 else "#e74c3c"

            st.markdown(
                f'<div style="display:flex;gap:16px;margin-bottom:8px;">'
                f'<div style="text-align:center;flex:1;padding:8px;background:#f8f9fa;border-radius:6px;">'
                f'<div style="font-size:22px;font-weight:700;color:{color};">{avg_ldays:.1f}</div>'
                f'<div style="font-size:11px;color:#888;">加权 Ldays</div></div>'
                f'<div style="text-align:center;flex:1;padding:8px;background:#f8f9fa;border-radius:6px;">'
                f'<div style="font-size:22px;font-weight:700;color:#2c3e50;">{max_ldays:.1f}</div>'
                f'<div style="font-size:11px;color:#888;">最大 Ldays</div></div>'
                f'<div style="text-align:center;flex:1;padding:8px;background:#f8f9fa;border-radius:6px;">'
                f'<div style="font-size:22px;font-weight:700;color:#2c3e50;">{len(ldays_items)}</div>'
                f'<div style="font-size:11px;color:#888;">统计基金数</div></div>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # 按基金分列条形图
            sorted_items = sorted(ldays_items, key=lambda x: x["ldays"], reverse=True)
            import plotly.graph_objects as go
            fig = go.Figure(go.Bar(
                x=[x["ldays"] for x in sorted_items],
                y=[x["name"][:10] for x in sorted_items],
                orientation="h",
                marker_color=[
                    "#e74c3c" if x["ldays"] > 30 else "#f39c12" if x["ldays"] > 10 else "#27ae60"
                    for x in sorted_items
                ],
            ))
            fig.update_layout(
                height=max(160, len(sorted_items) * 35),
                margin=dict(l=80, r=20, t=10, b=30),
                xaxis_title="Ldays（天）", yaxis_title="",
                template="simple_white", showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown("暂无流动性数据")

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio_duration(sub_reports: Dict[str, Any]):
    """组合久期板块（增强版）"""
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">📏 组合久期与利率敏感度</div>', unsafe_allow_html=True)

        dv01_items = []
        dd_items = []
        recovery_items = []
        for code, report in sub_reports.items():
            overview = report.get("overview")
            rb = report.get("rate_bond")
            fname = overview.fund_name if overview else code
            if rb:
                if rb.dv01 is not None:
                    dv01_items.append({"code": code, "name": fname, "dv01": rb.dv01})
                if rb.max_drawdown is not None:
                    dd_items.append({"code": code, "name": fname, "dd": rb.max_drawdown})
                if rb.drawdown_recovery_days is not None:
                    recovery_items.append({"code": code, "name": fname, "days": rb.drawdown_recovery_days})

        if not dv01_items and not dd_items and not recovery_items:
            st.markdown("暂无债券相关数据")
            st.markdown('</div>', unsafe_allow_html=True)
            return

        # 汇总数字
        if dv01_items:
            avg_dv01 = np.mean([x["dv01"] for x in dv01_items])
            dv_color = "#e74c3c" if avg_dv01 > 5 else "#f39c12" if avg_dv01 > 2 else "#27ae60"
            st.markdown(
                f'<div style="display:flex;gap:16px;margin-bottom:8px;">'
                f'<div style="text-align:center;flex:1;padding:8px;background:#f8f9fa;border-radius:6px;">'
                f'<div style="font-size:22px;font-weight:700;color:{dv_color};">{avg_dv01:.2f}</div>'
                f'<div style="font-size:11px;color:#888;">加权 DV01（bp）</div></div>',
                unsafe_allow_html=True,
            )
            if recovery_items:
                avg_rec = np.mean([x["days"] for x in recovery_items])
                st.markdown(
                    f'<div style="text-align:center;flex:1;padding:8px;background:#f8f9fa;border-radius:6px;">'
                    f'<div style="font-size:22px;font-weight:700;color:#2c3e50;">{avg_rec:.0f}</div>'
                    f'<div style="font-size:11px;color:#888;">加权回撤修复（天）</div></div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        # DV01 按基金分列
        if dv01_items:
            import plotly.graph_objects as go
            sorted_dv = sorted(dv01_items, key=lambda x: x["dv01"], reverse=True)
            fig = go.Figure(go.Bar(
                x=[x["dv01"] for x in sorted_dv],
                y=[x["name"][:10] for x in sorted_dv],
                orientation="h",
                marker_color=[
                    "#e74c3c" if x["dv01"] > 5 else "#f39c12" if x["dv01"] > 2 else "#27ae60"
                    for x in sorted_dv
                ],
            ))
            fig.update_layout(
                height=max(120, len(sorted_dv) * 30),
                margin=dict(l=80, r=20, t=10, b=30),
                xaxis_title="DV01（bp）", yaxis_title="",
                template="simple_white", showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio_radar(portfolio: Dict[str, Any]):
    """单基金多维度雷达图对比"""
    summaries = portfolio.get("per_fund_summary", [])
    if len(summaries) < 2:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">🎯 基金多维对比</div>', unsafe_allow_html=True)

        import plotly.graph_objects as go

        # 选取可比较的维度（标准化到 0~100）
        categories = ["权益暴露", "流动性", "利率敏感", "转债风险", "违约风险"]
        fig = go.Figure()

        color_palette = ["#e74c3c", "#3498db", "#f39c12", "#27ae60", "#9b59b6", "#1abc9c"]

        for i, s in enumerate(summaries[:6]):  # 最多 6 只
            # 权益暴露：股票比例 × 100
            equity = s["stock_ratio"] * 100
            # 流动性：Ldays 归一化（越大越差 → 100 - 归一化）
            if s["ldays"] is not None:
                liquidity = max(0, 100 - s["ldays"] / 50 * 100)
            else:
                liquidity = 50  # 缺失默认
            # 利率敏感：DV01 归一化
            if s["dv01"] is not None:
                rate_sens = min(100, s["dv01"] / 10 * 100)
            else:
                rate_sens = 0
            # 转债风险：双高数量
            cb_risk = min(100, s["double_high_count"] / 5 * 100)
            # 违约风险
            default_risk = 80 if s["default_warning"] else 10

            values = [equity, liquidity, rate_sens, cb_risk, default_risk]
            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=categories,
                fill="toself",
                opacity=0.15,
                name=s["name"][:10],
                line=dict(color=color_palette[i % len(color_palette)], width=1.5),
            ))

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            height=350,
            margin=dict(l=40, r=40, t=30, b=10),
            showlegend=True,
            legend=dict(orientation="h", y=-0.05, font=dict(size=10)),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown('</div>', unsafe_allow_html=True)


def _render_portfolio_fund_list(portfolio: Dict[str, Any]):
    """单基金入口"""
    fund_list = portfolio.get("fund_list", [])
    if not fund_list:
        return

    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="card-title">📋 基金列表</div>', unsafe_allow_html=True)

        cols = st.columns(min(len(fund_list), 3))
        for i, fund in enumerate(fund_list):
            with cols[i % 3]:
                st.markdown(f"**{fund['name']}**（{fund['code']}）")
                st.caption(fund.get("fund_type", ""))

        st.markdown('</div>', unsafe_allow_html=True)


def _render_rate_bond_hold_enhanced(rate_bond: Any):
    """已持有模式·利率债板块增强展示"""

    # 1. 最大回撤进度条
    if rate_bond.max_drawdown is not None:
        dd = rate_bond.max_drawdown
        bar_color = "#e74c3c" if dd < -3 else "#f39c12" if dd < -1 else "#27ae60"
        bar_width = min(abs(dd) / 10 * 100, 100)
        st.markdown(
            f'<div style="margin:8px 0;">'
            f'<div style="font-size:12px;color:#888;margin-bottom:4px;">最大回撤</div>'
            f'<div style="background:#f0f0f0;border-radius:6px;height:24px;position:relative;overflow:hidden;">'
            f'<div style="background:{bar_color};width:{bar_width}%;height:100%;border-radius:6px;"></div>'
            f'<span style="position:absolute;left:8px;top:2px;font-size:13px;font-weight:600;color:#fff;">{dd:.2f}%</span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # 2. DV01 风格标签
    if rate_bond.dv01 is not None:
        dv = rate_bond.dv01
        if dv > 5:
            label, color = "高敏感", "#e74c3c"
        elif dv > 2:
            label, color = "中敏感", "#f39c12"
        else:
            label, color = "低敏感", "#27ae60"
        st.markdown(
            f'<span style="display:inline-block;padding:3px 10px;border-radius:4px;'
            f'font-size:12px;color:{color};background:{"#fdeaea" if color=="#e74c3c" else "#fef9e7" if color=="#f39c12" else "#eafaf1"};'
            f'">利率敏感度：{label}（DV01={dv:.2f}bp）</span>',
            unsafe_allow_html=True,
        )

    # 3. 收益率曲线形态
    if rate_bond.yield_curve_shape:
        shape = rate_bond.yield_curve_shape
        shape_map = {
            "陡峭": ("📈 曲线陡峭", "#3498db", "#ebf5fb", "长端利率较高，适合哑铃型配置"),
            "平坦": ("➡️ 曲线平坦", "#f39c12", "#fef9e7", "期限利差收窄，注意利率方向"),
            "倒挂": ("⚠️ 曲线倒挂", "#e74c3c", "#fdeaea", "衰退信号，警惕利率下行风险"),
            "正常": ("📊 曲线正常", "#27ae60", "#eafaf1", "期限利差正常"),
        }
        if shape in shape_map:
            title, fg, bg, hint = shape_map[shape]
            st.markdown(
                f'<div style="margin:8px 0;padding:10px 14px;border-radius:8px;background:{bg};border-left:3px solid {fg};">'
                f'<span style="font-weight:600;color:{fg};font-size:13px;">{title}</span>'
                f'<br><span style="font-size:11px;color:#888;">{hint}</span></div>',
                unsafe_allow_html=True,
            )




def _render_credit_bond_hold_enhanced(credit_bond: Any):
    """已持有模式·信用债板块增强展示"""

    # 1. 行业风险预警高亮卡
    if credit_bond.default_warning:
        warnings = credit_bond.default_warning.split("；")
        for w in warnings:
            st.markdown(
                '<div style="margin:6px 0;padding:10px 14px;border-radius:8px;'
                'background:#fdeaea;border-left:3px solid #e74c3c;">'
                '<span style="font-weight:600;color:#e74c3c;font-size:13px;">'
                f'⚠️ 行业风险预警</span>'
                f'<br><span style="font-size:12px;color:#666;">{w}</span></div>',
                unsafe_allow_html=True,
            )

    # 2. 信用评级色条
    if credit_bond.avg_rating:
        rating = credit_bond.avg_rating
        rating_map = {
            "AAA": ("#27ae60", "#eafaf1"),
            "AA+": ("#27ae60", "#eafaf1"),
            "AA": ("#3498db", "#ebf5fb"),
            "AA-": ("#f39c12", "#fef9e7"),
            "A+": ("#e67e22", "#fef5e7"),
            "A": ("#e74c3c", "#fdeaea"),
        }
        fg, bg = rating_map.get(rating, ("#888", "#f5f5f5"))
        st.markdown(
            f'<span style="display:inline-block;padding:4px 12px;border-radius:6px;'
            f'font-size:13px;font-weight:600;color:{fg};background:{bg};">'
            f'评级：{rating}</span>',
            unsafe_allow_html=True,
        )


def _render_cb_hold_enhanced(cb: Any):
    """已持有模式·可转债板块增强展示"""

    # 1. 双高检测卡片
    if cb.is_double_high and cb.double_high_list:
        st.markdown(
            '<div style="margin:8px 0;padding:10px 14px;border-radius:8px;'
            'background:#fdeaea;border-left:3px solid #e74c3c;">'
            '<span style="font-weight:600;color:#e74c3c;font-size:13px;">'
            f'⚠️ 双高转债检测（价格>130 + 溢价率>30%）</span></div>',
            unsafe_allow_html=True,
        )
        # 列表
        for item in cb.double_high_list[:5]:
            p = item["price"]
            prem = item["premium"]
            # 颜色按严重程度
            if p > 150 and prem > 40:
                row_bg = "#fdeaea"
            elif p > 130 and prem > 30:
                row_bg = "#fef9e7"
            else:
                row_bg = "#f5f5f5"
            st.markdown(
                f'<div style="margin:3px 0;padding:6px 12px;border-radius:4px;background:{row_bg};'
                f'font-size:12px;display:flex;justify-content:space-between;">'
                f'<span>{item["name"]}</span>'
                f'<span>¥{p:.0f}　溢价率 {prem:.1f}%</span></div>',
                unsafe_allow_html=True,
            )

    # 2. 债底保护失效
    if cb.bond_floor_failed:
        st.markdown(
            '<div style="margin:6px 0;padding:10px 14px;border-radius:8px;'
            'background:#fdeaea;border-left:3px solid #e74c3c;">'
            '<span style="font-weight:600;color:#e74c3c;font-size:13px;">'
            '🛡️ 债底保护失效</span>'
            '<br><span style="font-size:12px;color:#666;">'
            '加权 YTM 为负，转债价格已脱离债底保护，下跌空间被打开。</span></div>',
            unsafe_allow_html=True,
        )

    # 3. 股债双杀模拟可视化
    if cb.blackswan_cb_loss is not None:
        loss = cb.blackswan_cb_loss
        bar_color = "#e74c3c" if loss < -15 else "#f39c12" if loss < -8 else "#e67e22"
        bar_width = min(abs(loss) / 25 * 100, 100)
        st.markdown(
            f'<div style="margin:8px 0;padding:10px 14px;border-radius:8px;'
            f'background:#fef9e7;border-left:3px solid #f39c12;">'
            f'<span style="font-weight:600;color:#f39c12;font-size:13px;">'
            f'📊 股债双杀模拟</span>'
            f'<br><span style="font-size:11px;color:#888;">假设：股票-20% + 利率+50bp</span>'
            f'<div style="margin-top:6px;background:#f0f0f0;border-radius:6px;height:24px;position:relative;overflow:hidden;">'
            f'<div style="background:{bar_color};width:{bar_width}%;height:100%;border-radius:6px;"></div>'
            f'<span style="position:absolute;left:8px;top:2px;font-size:13px;font-weight:600;color:#fff;">{loss:.1f}%</span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )


def _render_metrics_grid(items: List[Dict]):
    """渲染指标网格（每行 3 个）"""
    if not items:
        return

    cols_per_row = 3
    for i in range(0, len(items), cols_per_row):
        row = items[i:i + cols_per_row]
        cols = st.columns(len(row))
        for j, item in enumerate(row):
            with cols[j]:
                trend_cls = item.get("trend", "")
                if item.get("desc"):
                    st.markdown(
                        f'<div class="metric-item">'
                        f'<div class="metric-label">{item["label"]}</div>'
                        f'<div class="metric-value metric-{trend_cls}">{item["value"]}</div>'
                        f'<div class="metric-desc">{item["desc"]}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="metric-item">'
                        f'<div class="metric-label">{item["label"]}</div>'
                        f'<div class="metric-value metric-{trend_cls}">{item["value"]}</div>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )


# ============================================================
# 内测反馈问卷
# ============================================================

def _show_feedback_dialog():
    """显示内测反馈问卷弹窗"""
    if not st.session_state.get("show_feedback"):
        return

    # st.dialog 需要 Streamlit 1.33+
    try:
        dialog_func = st.dialog("📝 内测反馈问卷")
        dialog_func(_feedback_form)
    except (AttributeError, TypeError):
        # 回退：直接渲染表单（非弹窗）
        st.divider()
        _feedback_form()


def _feedback_form():
    """反馈问卷表单内容"""
    st.markdown("感谢参与内测！您的反馈将帮助我们在公测前改进产品。")
    st.markdown("")

    # Q1: 投资经验
    st.markdown("**1. 您的投资经验**")
    q1 = st.radio(
        "",
        ["不到 1 年（基金新手）", "1-3 年", "3-5 年", "5 年以上"],
        index=None, horizontal=True, label_visibility="collapsed",
    )

    # Q2: 了解渠道
    st.markdown("**2. 您平时主要通过什么渠道了解基金？**（可多选）")
    q2 = st.multiselect(
        "",
        ["小红书 / 抖音", "天天基金 / 蚂蚁财富", "银行/券商 App", "雪球 / 集思录等论坛"],
        label_visibility="collapsed",
    )
    q2_other = st.text_input("其他渠道（选填）", placeholder="如：微信群、公众号等")

    # Q3: 常用功能
    st.markdown("**3. 您最常使用的分析功能是？**（可多选）")
    q3 = st.multiselect(
        "",
        ["拟买入分析（选基参考）", "已持有分析（单只）", "多基金组合分析", "第一次体验"],
        label_visibility="collapsed",
    )

    # Q4: 有价值指标
    st.markdown("**4. 以下哪些指标对您最有价值？**（可多选，最多 5 项）")
    q4_options = [
        "PE 分位 / 估值水位", "PEG / 业绩匹配度", "Alpha / 超额收益",
        "流动性穿透（Ldays）", "黑天鹅压测", "资产配置分析",
        "持仓重叠分析", "风格漂移检测", "持仓股估值气泡图",
    ]
    q4 = st.multiselect("", q4_options, max_selections=5, label_visibility="collapsed")

    # Q5: 报告复杂度
    st.markdown("**5. 您觉得当前的分析报告：**")
    q5 = st.radio(
        "",
        ["太简单，希望更深入", "刚好合适", "太复杂，看不太懂", "专业术语太多"],
        index=None, horizontal=True, label_visibility="collapsed",
    )

    # Q6: 付费意愿
    st.markdown("**6. 如果正式版定价 9.9 元/月，您会付费吗？**")
    q6 = st.radio(
        "",
        ["一定会", "大概率会", "看具体功能再决定", "不太会，倾向用免费工具", "更倾向一次性买断（如 49 元/年）"],
        index=None, horizontal=False, label_visibility="collapsed",
    )

    # Q7: 开放反馈
    st.markdown("**7. 还有什么想吐槽或建议的？**（选填）")
    q7 = st.text_area("", placeholder="任何想法都可以写下来...", height=100)

    # 提交
    st.markdown("")
    submitted = st.button("✅ 提交反馈", type="primary", use_container_width=True)

    if submitted:
        # 校验必填
        if not q1 or not q5 or not q6:
            st.warning("请完成第 1、5、6 题（必填）")
            return

        # 组装数据
        channels = list(q2)
        if q2_other.strip():
            channels.append(q2_other.strip())

        from data_loader.user_tracker import get_visitor_id, submit_feedback
        visitor_id = get_visitor_id()
        ok = submit_feedback(
            visitor_id=visitor_id,
            q1_experience=q1,
            q2_channels=channels,
            q3_features=list(q3),
            q4_valuable=list(q4),
            q5_complexity=q5,
            q6_pricing=q6,
            q7_open_feedback=q7,
        )

        if ok:
            st.session_state.pop("show_feedback", None)
            st.success("感谢您的反馈！🙏")
            st.rerun()
        else:
            st.error("提交失败，请稍后重试")


# ============================================================
# 工具
# ============================================================

def _tag(code: str) -> str:
    return code


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except (ValueError, TypeError):
        return None


# ============================================================
# 入口
# ============================================================

if __name__ == "__main__":
    # 侧边栏（每次都渲染）
    _render_sidebar()

    # 反馈弹窗（从侧边栏触发）
    _show_feedback_dialog()

    mode = st.session_state.get("mode")

    if mode is None:
        show_home()
    elif mode == "buy":
        show_buy_page()
    elif mode == "hold":
        show_hold_page()
