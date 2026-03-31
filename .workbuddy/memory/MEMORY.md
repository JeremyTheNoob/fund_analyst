# 基金穿透式分析 - 长期记忆

## 项目基本信息
- **产品**：基金穿透式分析工具，对外运营，含付费功能
- **部署地址**：https://fundanalyst.streamlit.app/
- **GitHub**：https://github.com/JeremyTheNoob/fund_analyst（Public）
- **获客渠道**：小红书 + 抖音（图文 + AI配音）
- **技术栈**：Python + Streamlit，数据源 AkShare
- **启动命令**：`cd /Users/liuweihua/WorkBuddy/基金穿透式分析 && streamlit run main.py`

## 架构说明
```
data_loader/ → processor/ → engine/ → reporter/ → main.py
数据获取    →  标准化清洗 → 模块化计算 → 统一报告分发
```
- `pipeline.py`：核心流水线，`analyze_fund()` 入口
- `models/schema.py`：FundReport / FundBasicInfo / EquityMetrics 等数据模型
- `reporter/chart_gen.py`：生成 chart_data
- `reporter/translator.py`：生成 text_report（返回 dict：headline/body/advice/risk_warning）

## 基金类型识别系统（2026-03-31 重构）

### 核心设计
- **权威数据源**：`fund_name_em` AkShare 接口，30 种 `主类型-子类型` 格式
- **类型识别函数**：`pipeline.py` → `resolve_framework(symbol) → (framework_id, raw_type)`
- **缓存机制**：`data_loader/base_api.py` → `get_fund_type_em(symbol)` 模块级缓存

### 两层路由
1. **框架层（13 种）**：`FUND_TYPE_MAP` 映射 fund_name_em → framework_id
2. **模型层（4 种引擎）**：`FRAMEWORK_ROUTING[framework_id] → (model_type, holdings_load, cn_name)`
   - model_type: `equity` / `bond` / `index` / `cb`
   - holdings_load: `stock` / `bond`

### 支持的框架
| 框架 ID | 中文名 | 模型 | 持仓加载 |
|---|---|---|---|
| `stock` | 股票型 | equity | stock |
| `hybrid_equity` | 混合型-偏股 | equity | stock |
| `hybrid_balanced` | 混合型-平衡 | equity | stock |
| `hybrid_flexible` | 混合型-灵活 | equity | stock |
| `hybrid_absreturn` | 混合型-绝对收益 | equity | stock |
| `hybrid_bond` | 混合型-偏债 | bond | bond |
| `bond_long` | 债券型-长债 | bond | bond |
| `bond_short` | 债券型-中短债 | bond | bond |
| `bond_mixed2` | 债券型-混合二级 | bond | bond |
| `bond_mixed1` | 债券型-混合一级 | bond | bond |
| `idx_stock` | 指数型-股票 | index | stock |
| `idx_bond` | 指数型-固收 | index | bond |

### 特殊路由
- **转债自动识别**：bond 类基金 cb_ratio > 30% → 自动切换为 `cb` 模型
- **不支持类型**：QDII、货币、FOF、商品、REITs → `UNSUPPORTED_MESSAGES` 友好提示

### 显示层路由（main.py / translator.py / chart_gen.py）
- 统一使用 metrics 字段判断：`report.equity_metrics` / `report.bond_metrics` / `report.index_metrics` / `report.cb_metrics`
- **不再**依赖 `fund_type` 字符串匹配
- **混合型偏股路由**：`fund_type in (hybrid_equity/balanced)` → `_render_hybrid_equity_report()`，6板块
- **混合型绝对收益路由**：`fund_type == hybrid_absreturn` → `_render_hybrid_absreturn_report()`，4板块（波动率区间为核心）
- **灵活配置型独立路由**：`fund_type == hybrid_flexible` → `_render_hybrid_flexible_report()`，5板块（择时风格为核心）
- **混合二级债基路由**：`fund_type == bond_mixed2` → `_render_bond_mixed2_report()`，5板块（股票增强+转债配置）
- **混合一级债基路由**：`fund_type == bond_mixed1` → `_render_bond_mixed1_report()`，5板块（转债弹性+溢价率审计+全市场估值预警）
- **指数型-股票路由**：`fund_type == idx_stock` → `_render_idx_stock_report()`，5板块（基本信息+费率排名+PE/PB估值+成份股穿透+投资建议）
- **指数型-其他路由**：`_render_legacy_index_report()`，旧版 3+1 板块

## 深度报告生成器（reporter/）
| 类型 | 文件 | 章节结构 |
|---|---|---|
| **权益类（纯股）** | `equity_report_writer.py` | **5板块**：①收益曲线 / ②持仓穿透 / ③深度分析(Brinson) / ④风险预警(压力测试) / ⑤投资建议(买入/持有/离场) |
| **混合型偏股** | `hybrid_equity_report_writer.py` | **6板块**：①资产结构模型(股债动态比例) / ②收益表现(同类分位数) / ③持仓穿透(深度持仓UI) / ④非股票分析(风险对冲/转债灰色地带/现金择时) / ⑤风险预警(RBSA风格漂移) / ⑥投资建议(买入/持有/离场) |
| **混合型偏债** | `hybrid_bond_report_writer.py` | **5板块**：①收益情况展示(累计收益曲线) / ②收益归因(资产配置饼图+久期) / ③债券持仓深度(券种饼图+WACS) / ④风险预警(回撤图+利率预测) / ⑤投资建议(买入/持有/离场) |
| **混合型灵活配置** | `hybrid_flexible_report_writer.py` | **5板块**：①收益情况(收益归因+Alpha/Beta) / ②择时风格(仓位区间+暴跌复盘) / ③深度分析(股债穿透+决策复盘) / ④风险预警(Beta黑盒+择时失败+风格切换) / ⑤投资建议(三重监控) |
| **混合型绝对收益** | `hybrid_absreturn_report_writer.py` | **4板块**：①收益情况展示(年度收益表) / ②收益解析(净值直线度+避风港概率) / ③深度分析(策略稳定性+波动率区间) / ④风险预警(日波动异常+基差风险) |
| **债券型-中短债** | `bond_short_report_writer.py` | **5板块**：①信用垫层(政金债/信用债穿透+期限分布) / ②收益表现(滚动持有胜率+累计收益) / ③深度分析(杠杆利用率+压力测试) / ④风险预警(规模变动+流动性+利率) / ⑤投资建议(拟买入/已持有/离场信号) |
| **债券型-混合二级** | `bond_mixed2_report_writer.py` | **5板块**：①基本信息(资产结构+股票仓位红线监控) / ②收益表现(债券类型分布+股票增强质量) / ③深度分析(转债配置策略+溢价率监控) / ④风险预警(股债双杀复盘+压力测试) / ⑤投资建议(拟购入/已持有) |
| **债券型-混合一级** | `bond_mixed1_report_writer.py` | **5板块**：①基本信息(资产结构+信用/转债配比) / ②收益表现(收益展示+转债弹性模型Beta) / ③深度分析(转债归因+溢价率审计) / ④风险预警(违约/赎回风险+全市场估值预警) / ⑤投资建议(拟购入/已持有) |
| 债券类 | `bond_report_writer.py` | 5+1（收益逻辑/风险边际/波动率管理+结论+利率专题(bond_long)，含利率预测） |
| 指数类 | `index_report_writer.py` | 3+1（净值拟合/跟踪偏离度/场内流动性+结论） |
| **指数型-股票** | `idx_stock_report_writer.py` | **5板块**（基本信息+费率排名+深度分析+风险预警+投资建议） |
| **指数型-固收** | `idx_bond_report_writer.py` | **5板块+利率专题**（跟踪精度+收益表现+久期信用+调仓损耗+YTM投资建议+10年国债分析） |
| 转债类 | `cb_report_writer.py` | 4+1（资产配置/非对称捕获/回撤防御/大类穿透+结论） |

### 图表内联标记
- `[INSERT_CHART: CUM_RET]` / `DRAWDOWN` / `HEATMAP` / `EXCESS_ALPH` — 通用
- `[INSERT_CHART: BRINSON]` — 权益类 Brinson 归因柱状图（2026-03-31 新增）
- `[INSERT_CHART: ASSET_ALLOCATION_PIE]` — 混合型偏债资产配置饼图（2026-03-31 新增）
- `[INSERT_CHART: BOND_HOLDINGS_PIE]` — 混合型偏债券种分布饼图（2026-03-31 新增）
- `[INSERT_CHART: ASSET_ALLOCATION]` — 混合型资产配置面积图
- `[INSERT_CHART: TIMING_COMPARISON]` — 灵活配置型仓位/市场对照图（2026-03-31 新增）
- `[INSERT_CHART: ROLLING_BETA]` — 灵活配置型滚动Beta曲线（2026-03-31 新增）
- `[INSERT_CHART: VOLATILITY_BAND]` — 绝对收益型波动率区间监控图（2026-03-31 新增）
- `[INSERT_CHART: RBSA_DRIFT]` — 混合型 RBSA 风格漂移图
- `[INSERT_CHART: RATE_PREDICTION]` — 债券类利率预测
- `[INSERT_CHART: CAPTURE_RATIO]` / `TRACKING_ERROR_SCATTER` / `PREMIUM_DISCOUNT` — 特定类型
- `[INSERT_CHART: PE_PERCENTILE]` / `PB_PERCENTILE` — 指数型-股票 PE/PB 估值分位图
- `[INSERT_CHART: TOP10_WEIGHTS]` — 指数型-股票 前十大成份股权重柱状图
- `[INSERT_CHART: EXCESS_ALPH]` — 指数增强型 超额收益曲线
- `[INSERT_CHART: Y10Y_TREND]` — 10年国债收益率走势与均线（指数型-固收）
- `[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]` — 权益类/混合型 section3 持仓穿透（UI 组件渲染）

## 核心模块

### 申万行业全收益指数系统
- `data_loader/index_cache_config.py` / `index_cache_manager.py` / `index_integration.py`
- 本地缓存：`data/index_cache/`
- 全收益合成：`(1 + 价格收益率) × (1 + 股息收益率) - 1`

### 指数型-固收专用加载器
- `data_loader/idx_bond_loader.py`
- 日偏离度分析 / 久期估算 / 信用等级对齐 / 调仓损耗 / 10年国债技术分析 / YTM估算

### 利率预测模块
- `data_loader/rate_prediction.py`：`predict_rate_trend(horizon)`
- 技术指标组合（均值回归 + 期限利差 + 趋势动量 + 波动率调整）
- 置信度：基础 0.7 + abs(net_signal)*0.1（最高0.85）/ 震荡 0.55

### 权益类深度持仓分析
- `data_loader/equity_holdings_loader.py` / `reporter/equity_holdings_v2.py` / `ui/equity_holdings_v2_components.py`
- 四大模块：资产配置演变/持仓历史热力图/交易能力评估/估值分析与风险预警
- 数据范围：最多 5 年（20 个季度）

### 图表数据结构（chart_data）
- `cumulative_return`: x, series, benchmark_info
- `drawdown`: x, series, drawdown_info
- `monthly_heatmap`: data, x, y, heatmap_info
- `excess_return`: x, series, excess_info
- 颜色方案：基金（红色 #e74c3c）、基准（灰色 #95a5a6）

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

## 验证过的 AkShare 接口
- `fund_name_em()` — 基金类型（权威，模块级缓存）
- `fund_open_fund_info_em()` — 历史净值
- `fund_individual_basic_info_xq()` — 雪球基础信息
- `fund_portfolio_hold_em()` — 股票前十大持仓（多季度数据，需按最新季度过滤）
- `fund_portfolio_bond_hold_em()` — 债券全部持仓
- `fund_individual_detail_hold_xq(symbol, date)` — 基金大类资产配置（雪球，列：资产类型/仓位占比）
  - 日期格式 YYYYMMDD（如 "20240930"），需从近到远回退尝试
- `fund_portfolio_bond_hold_em()` — 债券全部持仓
- `stock_zh_index_daily()` — 指数日行情
- `bond_new_composite_index_cbond(indicator="财富")` — 中债综合指数
- `bond_zh_us_rate()` — 国债收益率（利率预测模块）
- `bond_zh_cov_value_analysis()` — 全市场可转债估值数据（纯债溢价率/转股溢价率，混合一级债基溢价率预警）
- `index_zh_a_hist(symbol="000832")` — 中证转债指数历史日线（混合一级债基 Beta 回归，需网络稳定）
- `stock_zh_index_value_csindex(symbol)` — 中证指数估值（PE-TTM/PB/股息率，指数型-股票估值锚点，symbol 传6位代码如"000300"）
- `index_stock_cons_weight_csindex(symbol)` — 中证指数成份股权重（指数型-股票持仓穿透，symbol 传6位代码）
- `fund_fee_em(symbol, indicator="运作费用")` — 基金费率（管理费/托管费/销售服务费）

## 待处理事项
- ⏳ 调研云数据库方案（PostgreSQL 增量更新）
- ⏳ 设计增量更新机制
- ⚠️ 基金 000297 数据加载超时问题
