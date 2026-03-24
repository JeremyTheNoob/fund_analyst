# 基金穿透式分析 - 长期记忆

## 项目目标
- 产品：基金穿透式分析工具，对外运营，含付费功能
- 当前状态：Streamlit MVP 已可运行，正在规划正式版

## 技术栈（当前）
- 前端/后端：Python + Streamlit
- 数据源：AkShare
- 正确 API：
  - `fund_open_fund_info_em(symbol=xxx, indicator="单位净值走势")` - 历史净值
  - `fund_manager_em()` - 基金经理（全量）
  - `fund_portfolio_hold_em(symbol=xxx, date="2024")` - 持仓
  - `fund_open_fund_daily_em()` - 基金日报（含申购/赎回状态、手续费）
  - `fund_individual_basic_info_xq(symbol=xxx)` - 雪球基础信息（主力接口）
  - `stock_zh_a_spot_em()` - 股票估值快照（PE/PB）
  - `index_zh_a_hist(symbol=xxx, period="daily", ...)` - 指数历史行情（基准）

## 当前版本
- **v7.0**（2026-03-24 重写）：全新架构，彻底解决v6崩溃问题
- 文件：`fund_analysis.py`（主部署文件）= `fund_analysis_v7.py`
- 五阶段框架：类型识别→量化看板→持仓穿透→可视化→诊断报告
- DataFrame构建规范：全部用字典方式，杜绝列名重复Bug

## 商业模式共识
- 免费3次/天 + 付费会员（无限 + 高级功能）
- 主要获客渠道：微信公众号/视频号、小红书、雪球、SEO

## 正式版技术选项
- A：升级 Streamlit（2周，快速验证）
- B：前后端分离（React + FastAPI + PostgreSQL，2个月）
- C：微信小程序（6周，移动端优先）

## 基金筛选阈值参数（待实现）
- 卓越：综合分 ≥80，优选 ≥60，问题 ≤25
- 偏科/问题条件：费率偏高、规模过小、经理任期 < 1年
- 不同类型基金用不同权重（货币/指数被动特殊处理）
