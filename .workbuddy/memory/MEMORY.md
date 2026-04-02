# 基金穿透式分析 - 长期记忆

## 项目基本信息
- **产品**：基金穿透式分析工具，对外运营，含付费功能
- **部署地址**：https://fundanalyst.streamlit.app/
- **GitHub**：https://github.com/JeremyTheNoob/fund_analyst（Public）
- **获客渠道**：小红书 + 抖音（图文 + AI配音）
- **技术栈**：Python + Streamlit，数据源 AkShare
- **启动命令**：`cd /Users/liuweihua/WorkBuddy/基金穿透式分析 && streamlit run main.py`

## 架构
```
data_loader/ → processor/ → engine/ → reporter/ → main.py
数据获取    →  标准化清洗 → 模块化计算 → 统一报告分发
```
- `pipeline.py`：核心流水线，`analyze_fund()` 入口
- `models/schema.py`：FundReport / FundBasicInfo / EquityMetrics 等数据模型
- `reporter/chart_gen.py`：生成 chart_data
- `reporter/translator.py`：生成 text_report（返回 dict：headline/body/advice/risk_warning）

## 基金类型识别系统（2026-03-31 重构）
- **权威数据源**：`fund_name_em` AkShare 接口，30 种 `主类型-子类型`
- **类型识别**：`pipeline.py` → `resolve_framework(symbol) → (framework_id, raw_type)`
- **两层路由**：框架层 13 种（FUND_TYPE_MAP）→ 模型层 4 种（equity/bond/index/cb）
- **特殊路由**：bond 类 cb_ratio > 30% → `cb` 模型；QDII/货币/FOF/商品/REITs → 友好提示
- **显示层**：统一用 `report.equity_metrics` / `report.bond_metrics` / `report.index_metrics` / `report.cb_metrics` 判断

## 报告生成器（reporter/）
| 类型 | 文件 | 板块数 |
|---|---|---|
| 权益类（纯股） | `equity_report_writer.py` | 5 |
| 混合型偏股 | `hybrid_equity_report_writer.py` | 6 |
| 混合型偏债 | `hybrid_bond_report_writer.py` | 5 |
| 混合型灵活配置 | `hybrid_flexible_report_writer.py` | 5 |
| 混合型绝对收益 | `hybrid_absreturn_report_writer.py` | 4 |
| 债券型-中短债 | `bond_short_report_writer.py` | 5 |
| 债券型-混合二级 | `bond_mixed2_report_writer.py` | 5 |
| 债券型-混合一级 | `bond_mixed1_report_writer.py` | 5 |
| 债券类 | `bond_report_writer.py` | 5+1 |
| 指数类（旧） | `index_report_writer.py` | 3+1 |
| 指数型-股票 | `idx_stock_report_writer.py` | 5 |
| 指数型-固收 | `idx_bond_report_writer.py` | 5+利率 |
| 转债类 | `cb_report_writer.py` | 4+1 |

### 图表内联标记
- 通用：`CUM_RET` / `DRAWDOWN` / `HEATMAP` / `EXCESS_ALPH` / `ASSET_ALLOCATION` / `RBSA_DRIFT` / `RATE_PREDICTION`
- 权益类：`BRINSON`
- 混合型：`ASSET_ALLOCATION_PIE` / `BOND_HOLDINGS_PIE` / `TIMING_COMPARISON` / `ROLLING_BETA` / `VOLATILITY_BAND`
- 指数型：`PE_PERCENTILE` / `PB_PERCENTILE` / `TOP10_WEIGHTS` / `EXCESS_ALPH` / `Y10Y_TREND`
- 持仓穿透：`DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER`
- 颜色方案：基金（红 #e74c3c）、基准（灰 #95a5a6）

## Supabase 缓存体系（2026-04-02 完整实施）

### 缓存层
- `data_loader/cache_layer.py`：cache_get/cache_set/cached_api 装饰器
- `config.py`：SUPABASE_URL / SUPABASE_ANON_KEY / SUPABASE_ENABLED
- Supabase URL：`https://fwksdeyzgwliivghszaa.supabase.co`
- 表：`data_cache`（缓存）+ `fund_rank_snapshot`（排名快照）
- **所有 AkShare API 已 100% 接入缓存**（核心分析路径）

### 缓存通道一览
| 前缀 | API | TTL |
|---|---|---|
| `fund_type` | fund_name_em 基金类型 | 7d |
| `fund_nav` | fund_open_fund_info_em 净值（含名称兜底） | 5m |
| `fund_basic_xq` | fund_individual_basic_info_xq 雪球基础信息 | 24h |
| `fund_holdings_stock` | fund_portfolio_hold_em 股票持仓 | 24h |
| `fund_holdings_bond` | fund_portfolio_bond_hold_em 债券持仓 | 24h |
| `fund_asset_alloc` | fund_individual_detail_hold_xq 资产配置 | 7d |
| `fund_fee` / `fund_fee_detail` | fund_fee_em 费率 | 7d |
| `fund_list_all` | fund_name_em 全量基金列表 | 1h |
| `fund_purchase_all` | fund_purchase_em 申购赎回状态 | 24h |
| `index_daily` | stock_zh_index_daily A股指数 | 24h |
| `hk_index_daily` | stock_hk_index_daily_sina 港股指数 | 24h |
| `index_valuation` | stock_zh_index_value_csindex PE/PB | 24h |
| `index_cons_weights` | index_stock_cons_weight_csindex 成份股 | 7d |
| `bond_composite` | bond_new_composite_index_cbond 中债指数 | 24h |
| `bond_us_rate` | bond_zh_us_rate 国债收益率 | 24h |
| `bond_china_yield` | bond_china_yield 中债收益率曲线 | 24h |
| `cb_value_analysis` | bond_zh_cov_value_analysis 转债估值 | 24h |
| `cb_index_hist` | index_zh_a_hist 中证转债指数 | 24h |
| `cb_info` | bond_zh_cov_info 可转债基本信息 | 24h |
| `etf_price` | fund_etf_hist_em ETF二级市场行情 | 24h |
| `etf_trading` | fund_etf_hist_em ETF成交数据 | 24h |

### 定时任务
- **scripts/prewarm_cache.py**：预热各类型 Top20 + 市场数据
- **scripts/prewarm_active_funds.py**：大规模活跃基金预热（~3000只，~250MB）
- **scripts/daily_update.py**：每日增量更新（市场/基金类型/热门净值/清理）
- **.github/workflows/daily_cache_update.yml**：每天北京时间 6:00 自动执行
- GitHub Secrets 已配置 SUPABASE_URL + SUPABASE_ANON_KEY

### 缓存序列化
- 小表（≤1000行）：JSON records 格式
- 大表（>1000行）：CSV 格式（`__CSV__:` 前缀标记），体积小 3-5 倍
- 缓存大小上限：20MB

## 关键 Bug 记录（勿重踩）
1. `_align_bm()` 必须取 `ret` 列，取 `close` 导致跟踪误差 669740%
2. 累计收益曲线必须优先全收益 `tr_ret`；强制零点对齐
3. 超额收益必须几何算法 `(fund_nav / bm_nav) - 1`；优先全收益基准
4. 基准数据列名统一为 `bm_ret`
5. `FundBasicInfo` 使用 `symbol` 不是 `code`
6. `beta` 在 `EquityMetrics`，不在 `CommonMetrics` → `hasattr()` 保护
7. 全收益指数代码需格式转换：`000300.SH` → `sh000300`
8. 债券权重和异常：API 返回百分比格式但代码当小数 → `bond_engine.py` 兼容
9. `call_with_timeout` 必须使用命名参数
10. 图表基准数据空值保护：永远不返回 `None`，返回空 DataFrame
11. `_remove_outliers_mad()` 必须先检查 `col not in df.columns`
12. `FundReport` 要有 `warnings: List[str] = []` 字段

## 待处理事项
- ⚠️ 基金 000297 数据加载超时问题
