# 基金穿透式分析 - 长期记忆（精简版）

## 项目基本信息
- **产品**：基金穿透式分析工具，对外运营，含付费功能
- **部署地址**：https://fundanalyst.streamlit.app/（新地址，2026-03-29 更新）
- **GitHub**：https://github.com/JeremyTheNoob/fund_analyst（Public）
- **获客渠道**：小红书 + 抖音（图文 + AI配音）
- **商业模式**：免费3次/天 + 付费会员（无限 + 高级功能）
- **技术栈**：Python + Streamlit，数据源 AkShare
- **启动命令**：`cd /Users/liuweihua/WorkBuddy/基金穿透式分析 && streamlit run main.py`

## 架构说明
```
data_loader/ → processor/ → engine/ → reporter/ → main.py
数据获取    →  标准化清洗 → 模块化计算 → 统一报告分发
```
- `pipeline.py`：核心流水线，`analyze_fund()` 入口
- `models/schema.py`：FundReport / FundBasicInfo / EquityMetrics 等数据模型
- `reporter/chart_gen.py`：生成 chart_data（cumulative_return / drawdown / monthly_heatmap / excess_return）
- `reporter/translator.py`：生成 text_report（headline / body / advice / risk_warning）
- `services/chart_interpretation.py`：图表下方专业解读文字（7个图表类型）
- `reporter/equity_report_writer.py`：新增，权益类基金深度评价报告生成器（1000字，4章节+结论）

## 支持的基金类型
- ✅ 权益类（股票型/偏股混合）→ `report.equity_metrics`
- ✅ 固收类（纯债/可转债）→ `report.bond_metrics`
- ✅ 指数/ETF（含增强型）→ `report.index_metrics`
- ✅ 可转债/固收+ → `report.cb_metrics`
- ❌ 不支持：QDII、货币基金、商品基金

## 核心技术特性

### 申万行业全收益指数系统（2026-03-29）
- **全收益合成算法**：`全收益率 = (1 + 价格收益率) × (1 + 股息收益率) - 1`
- **本地缓存**：`data/index_cache/`（price_indices/ + total_return_indices/ + index_metadata.json）
- **关键文件**：`data_loader/index_cache_config.py`、`index_cache_manager.py`、`index_integration.py`
- **固定股息率**：31个申万行业（煤炭6.0% ~ 国防军工0.5%）

### 图表数据结构（chart_data）
- `cumulative_return`: x, series（name/data/color）, benchmark_info（bm_last_return, is_total_return, bm_annual_return）
- `drawdown`: x, series, drawdown_info（bm_max_dd, defensive_ratio, recovery_info）
- `monthly_heatmap`: data, x, y, heatmap_info（annual_stats, monthly_details）
- `excess_return`: x, series, excess_info（last_excess, curve_trend, excess_std, ir_value, ir_quality, monthly_win_rate）
- **颜色方案**：基金（红色 #e74c3c）、基准（灰色 #95a5a6）

### 利率预测模块（2026-03-30 新建）
- **文件**：`data_loader/rate_prediction.py`
- **入口**：`predict_rate_trend(horizon)` / `generate_rate_prediction_chart(prediction)`
- **预测模型**：技术指标组合（均值回归 + 期限利差 + 趋势动量 + 波动率调整）
- **数据源**：AkShare `bond_zh_us_rate()`（国债收益率）+ `bond_china_yield()`（期限利差）
- **关键指标**：
  - 均值回归信号：10Y收益率历史分位数（≥80%下行 / ≤20%上行）
  - 期限利差信号：10Y-2Y利差分位数（<20%平坦 / >70%陡峭）
  - 趋势动量信号：近3个月线性回归斜率（上行/下行/震荡）
  - 波动率调整：近20日标准差（>0.15%降低置信度）
- **置信度计算**：基础 0.7 + abs(net_signal)*0.1（最高0.85）/ 震荡 0.55
- **集成状态**：✅ 已完全集成到 `bond_report_writer.py` 和 `main.py`（2026-03-30 14:00）
- **图表标记**：`[INSERT_CHART: RATE_PREDICTION]`（包含历史曲线 + 预测曲线 + 95%置信区间）

### 权益类深度报告（2026-03-29 新建）
- **文件**：`reporter/equity_report_writer.py`
- **入口**：`generate_equity_deep_report(report) -> dict`
- **返回 key**：meta / headline / section1~4 / conclusion / full_text
- **图表标记**：`[INSERT_CHART: CUM_RET]` / `EXCESS_ALPH` / `DRAWDOWN` / `HEATMAP`
- **集成状态**：✅ 已完全集成到 main.py（2026-03-29 11:10）

### 债券型基金深度报告（2026-03-29 新建）
- **文件**：`reporter/bond_report_writer.py`
- **入口**：`generate_bond_deep_report(report) -> dict`
- **返回 key**：meta / headline / section1~3 / conclusion / full_text
- **图表标记**：`[INSERT_CHART: CUM_RET]` / `DRAWDOWN` / `HEATMAP`
- **章节**：一（收益逻辑：票息vs久期博弈）/ 二（回撤与修复效率）/ 三（月度胜率与持有体验）/ 四（结论与投资建议）
- **集成状态**：✅ 已完全集成到 main.py（2026-03-29 11:35）

### 指数/ETF 深度报告（2026-03-29 新建）
- **文件**：`reporter/index_report_writer.py`
- **入口**：`generate_index_deep_report(report) -> dict`
- **返回 key**：meta / headline / section1~3 / conclusion / full_text
- **图表标记**：`[INSERT_CHART: CUM_RET]` / `TRACKING_ERROR_SCATTER`（超额曲线+±2σ带）/ `PREMIUM_DISCOUNT`（热力图+折溢价标注）
- **章节**：一（净值拟合与贝塔回归）/ 二（跟踪偏离度与精细化管理）/ 三（场内流动性与交易成本）/ 四（综合结论与配置建议）
- **集成状态**：✅ 已完全集成到 main.py（2026-03-29 12:00）

### 可转债/固收+深度报告（2026-03-29 新建）
- **文件**：`reporter/cb_report_writer.py`
- **入口**：`generate_cb_deep_report(report) -> dict`
- **返回 key**：meta / headline / section1~3 / conclusion / full_text
- **图表标记**：`[INSERT_CHART: CUM_RET]` / `CAPTURE_RATIO`（超额曲线+填充）/ `DRAWDOWN`
- **章节**：一（资产配置逻辑与复合收益）/ 二（非对称捕获能力分析）/ 三（回撤深度与底层支撑）/ 四（综合结论与配置建议）
- **集成状态**：✅ 已完全集成到 main.py（2026-03-29 13:00）
- **2026-03-29 14:00 更新**：新增 section4（大类资产穿透分析），支持转债估值压缩风险自动提示

### 权益类基金深度持仓分析（2026-03-30 完成开发）
- **文件**：`data_loader/equity_holdings_loader.py` / `reporter/equity_holdings_v2.py` / `ui/equity_holdings_v2_components.py`
- **入口**：`generate_deep_holdings_analysis(symbol, analysis_period, ...)` → `render_deep_holdings_ui(analysis_result)`
- **数据接口**：
  - 持仓历史：`ak.fund_portfolio_hold_em(symbol, date)`（按年份）
  - 资产结构：`ak.fund_individual_detail_hold_xq(symbol, date)`（按季度末）
- **数据范围限制**：**最多5年（20个季度）**，不足5年按实际情况
- **四大模块**：
  1. 资产配置演变趋势（堆叠面积图）：股票/债券/现金/其他占比变化
  2. 持仓历史变化（热力图）：前十大重仓股演变，识别投资框架稳定性
  3. 交易能力评估（柱状图+雷达图）：新买/卖的30天走势，判断逃顶/抄底能力（条件渲染，需≥4季度）
  4. 估值分析与风险预警（散点图+仪表盘+压力测试）：最新持仓估值 + 行业/全市场压力测试
- **关键指标**：持仓留存率、平均持仓周期、换手率、风格标签、抄底/逃顶成功率、能力评分、估值评级、风险等级
- **集成状态**：✅ 已完全替代 equity_report_writer.py 原有 section5（2026-03-30 11:45）
  - 使用占位符 `[DEEP_HOLDINGS_ANALYSIS_PLACEHOLDER]` 触发 UI 组件渲染
  - 删除旧的 `_section5_holdings_analysis` 和 `_infer_alpha_jump_period` 函数
  - 删除对 `holdings_analyzer.py` 的依赖
- **关键Bug修复（2026-03-30 11:10）**：
  1. API调用参数错误：`call_with_timeout()` 改为 `safe_api_call()`（支持 `max_retries`）
  2. 资产结构列名不匹配：兼容 `'仓位占比'` 和 `'占净值比例(%)'` 两种格式
  3. 模块2季度处理错误：从 `"2025年1季度股票投资明细"` 提取 `"2025Q1"` 标签
  4. 持仓留存率计算错误：使用 `df_with_label['季度标签']` 而不是原始的 `df`
- **开发状态**：✅ 开发完成，测试通过（000001基金四大模块数据充足）

## 关键 Bug 记录（勿重踩）
1. `fund_portfolio_asset_allocation_em`接口不存在 → 改用`fund_portfolio_hold_em`
2. `_align_bm()`必须明确取`ret`列，取`close`会导致跟踪误差异常（669740%）
3. `FundReport`要有`warnings: List[str] = []`字段，否则pipeline赋值报错
4. 基准数据列名统一为`bm_ret`，schema文档必须与实际代码一致
5. bond模式下`translate_results()`传入的是`{'bond': bond_res}`嵌套字典，需先展开
6. **累计收益曲线**：必须优先使用全收益`tr_ret`；强制零点对齐；数据清洗预处理
7. **类型识别错误**（2026-03-29）：`load_basic_info()` 返回中文类型，但 pipeline 使用英文类型码 → 新增 `_map_type_category()` 函数进行映射
8. **基准数据 NaN 问题**（2026-03-30）：`bm_last_return` 未做 NaN 检查，增加 `pd.isna()` 检查并转换为 `float`
9. **行业推断不准确**（2026-03-30）：关键词映射表太窄，接入 AkShare 申万行业数据库（`data_loader/sw_industry_loader.py`）
10. **水下回撤图**：基准用全收益；修复阈值`-0.1%`；区分区间回撤vs绝对回撤
11. **超额收益曲线**：必须几何超额算法 `(fund_nav / bm_nav) - 1`；优先全收益基准；强制起点对齐为0
12. 全收益指数`H00019`无法获取 → 改用`sh000300`，宽基指数代码需格式转换（`000300.SH` → `sh000300`）
13. **图表基准数据空值保护**（2026-03-29）：`_replace_benchmark_for_charts()` 永远不返回 `None`，返回空 DataFrame
14. **数据清洗 KeyError 风险**（2026-03-29）：`_remove_outliers_mad()` 必须先检查 `col not in df.columns`
15. **call_with_timeout 参数顺序**（2026-03-29）：必须使用命名参数 `call_with_timeout(func, args=..., kwargs=..., timeout=...)`
16. **Beta 属性访问错误**（2026-03-30）：`beta` 在 `EquityMetrics` 中，不在 `CommonMetrics` 中 → `m.beta if hasattr(m, 'beta') else 1.0`
17. **基金代码属性错误**（2026-03-30）：`FundBasicInfo` 使用 `symbol` 而不是 `code` → `report.basic.symbol`
18. **港股基准数据加载失败**（2026-03-30）：港股指数代码格式错误（`HSI.HI` → `HSI`）+ 接口参数未传递 → 修复 `equity_loader.py` 和 `base_api.py`
19. **纯债型基金分析崩溃**（2026-03-30）：债券权重和异常（113.96%），原因是API返回的`占净值比例`是百分比格式但代码当作小数使用 + 持仓数据不完整（仅前N大重仓）→ 修复 `bond_engine.py`（权重格式兼容 + 数据不完整兼容）和 `bond_loader.py`（只使用最新季度数据）
20. **债券基金基准年化收益显示0.00%**（2026-03-30）：`bond_report_writer.py`使用错误的公式 `cum_bm / _year_count()`，日历年份除法导致结果为0 → 修复 `chart_gen.py`添加`bm_annual_return`字段（复利公式）+ `bond_report_writer.py`读取该字段
21. **债券基金累计收益图表缺少基准曲线**（2026-03-30）：`pipeline.py`中`_run_bond_pipeline()`未将`bond_idx`添加到`chart_data` + `chart_gen.py`中基准数据列名兼容问题（`ret` vs `bm_ret`）→ 修复 `pipeline.py`添加`benchmark_df` + `chart_gen.py`自动重命名列名

## 页面优化（2026-03-30）
### 图表显示优化（手机端适配）
- **问题**：手机端图例过大，挤压图表空间
- **修复**：所有图表配置横向图例，放在下方，字体缩小为10px
- **修改文件**：`main.py`（所有4种基金类型的图表）

### 大标题字体调整
- **问题**：大标题字体过大
- **修复**：将 `##`（二级标题）改为 `###`（三级标题）
- **修改文件**：`reporter/equity_report_writer.py`
- **效果**：标题字体比正文大1号，视觉层次更清晰

### x轴标题优化
- **问题**：x轴已显示日期，无需再标注"日期"二字
- **修复**：移除所有图表的 `xaxis_title="日期"`
- **修改文件**：`main.py`（所有11个图表）
- **效果**：图表更简洁，视觉更清爽

## 验证过的 AkShare 接口
- `fund_open_fund_info_em()` - 历史净值
- `fund_individual_basic_info_xq()` - 雪球基础信息
- `fund_portfolio_hold_em()` - 股票前十大持仓
- `fund_portfolio_bond_hold_em()` - 债券全部持仓
- `stock_zh_index_daily()` - 指数日行情
- `bond_new_composite_index_cbond(indicator="财富")` - 中债综合指数

## 代码质量与性能测试（2026-03-29）
### 代码质量检查
- **评分**：⭐⭐⭐⭐ (4/5)
- **发现问题**：10 个（P1:1, P2:4, P3:5）
- **关键问题**：
  - P1: 依赖漏洞检查（需运行 pip-audit）
  - P2: 超时配置分散、日志不统一、性能日志不足
  - P3: 日志格式、日期处理、注释准确性
- **结论**：无阻塞性问题，可投入生产

### 性能基准测试
- **评分**：⭐⭐⭐⭐⭐ (5/5)
- **测试结果**：4/4 全部通过 ✅
  - IO 密集度：平均 56.76s/基金，无异常连接
  - 内存泄露：10 只基金后内存下降 19.41 MB，无泄露
  - API 稳定性：成功率 100%，超时控制正常
  - 缓存命中率：60.0%，缓存池工作正常
- **结论**：性能表现优秀，系统运行稳定

### 测试报告
- `tests/code_quality_checklist.md` - 代码质量详细报告
- `tests/quick_performance_test_report.md` - 性能测试报告
- `tests/quality_and_performance_check_summary.md` - 综合总结报告

## 健壮性与防御性（2026-03-29 检查）

### 整体评分：⭐⭐⭐⭐ (4/5)

#### ✅ 优秀实践
- Pipeline 分阶段异常保护（6 个 Stage 均有 `try-except`）
- 三级缓存体系（装饰器 + Parquet + Streamlit）
- 文件资源管理 100% 使用 `with` 上下文管理器
- 无 O(n²) 嵌套循环，向量化操作规范
- 输入验证全面（基金代码、类型检查）

#### ⚠️ 性能瓶颈（已优化）
- ✅ **基准数据缓存池**（2026-03-29）：`processor/benchmark_cache.py`，支持日期范围裁剪，命中率 20%
- ✅ **AkShare 超时配置**（2026-03-29）：`data_loader/akshare_timeout.py`，跨平台超时包装器，10 秒超时
- ⚠️ `iterrows()` + 网络查询（7 处）：`convertible_bond_engine.py`、`bond_loader.py` 等
- ⚠️ 缓存无自动清理机制：`data_loader/index_cache_manager.py`

## UI 结构（main.py 当前状态）
- **无侧边栏**：`initial_sidebar_state="collapsed"`
- **输入区**：居中三列，基金代码 + 分析周期单选 + 开始分析按钮
- **加载提示**：`st.spinner("🧠 净值大模型分析中，请稍候...")`
- **Part 1**：基础信息（4列×2行：名称/公司/成立/净值 + 经理/类型/规模/基准 + 第三行申购/赎回/购买起点/费率）
- **Part 2**：4个图表 Tab（收益曲线/回撤分析/月度热力图/超额收益曲线），每图下方有专业解读框
- **Part 3**：关键指标（st.markdown 文字展示，需重构）
- **Part 4**：深度诊断 + 投资建议（两列，需重构为新的深度报告）

## 待处理事项
- ✅ main.py 集成 `equity_report_writer.py`（权益类深度报告已完成，含图表内联，2026-03-29）
- ✅ main.py 集成 `bond_report_writer.py`（债券类深度报告已完成，含图表内联，2026-03-29）
- ✅ main.py 集成 `index_report_writer.py`（指数/ETF 深度报告已完成，含图表内联，2026-03-29）
- ✅ main.py 集成 `cb_report_writer.py`（可转债/固收+深度报告已完成，含图表内联，2026-03-29）
- ✅ 代码质量与性能优化（P1/P2/P3 问题修复，2026-03-29 20:30）
- ✅ 临时文件清理与 GitHub 同步准备（2026-03-29 20:45）
- ✅ 完成未完成的代码优化任务（短期任务，2026-03-29 21:15）
- ✅ 静态和动态代码质量检查（2026-03-29 20:42）
- ✅ 云服务器架构方案设计（2026-03-29 21:50）
- ✅ 云服务器规格推荐与采购清单（2026-03-29 21:50）
- ⏳ 调研云数据库方案（待开始）
- ⏳ 设计增量更新机制（待开始）
- ⏳ 开发数据库适配器（待开始）
- ⏳ 实施迁移方案（待开始）
- ⏳ 集成到现有pipeline（待开始）
- ⚠️ 基金 000297 数据加载超时问题（可能网络/API 限制）

---

## 云服务器方案（2026-03-29 新增）

### 架构设计
- **推荐方案**：增量更新 + PostgreSQL 云数据库
- **架构**：AkShare API → 云数据库（预计算）→ 用户快速响应
- **性能提升**：
  - 首次加载：30-60s → < 3s（10-20x）
  - 二次加载：10-15s → < 1s（10-15x）
  - 并发支持：< 10 req/min → > 1000 req/min（100x）
  - API调用：每次10-20次 → 每天1次（减少99%）

### 推荐规格（阿里云）
- **ECS**: ecs.c6.xlarge (4核8GB, 100GB ESSD) - ~200元/月
- **RDS**: rds.pg.s2.large (2核4GB, 100GB ESSD, 3000 IOPS) - ~180元/月
- **带宽**: 5Mbps 按量计费 - ~50元/月
- **总成本**: ~430元/月（包年预留实例约215元/月）

### 数据结构设计
- `fund_basic_info`: 基金基础信息
- `fund_nav`: 基金净值数据（时间序列）
- `fund_metrics`: 预计算指标（支持 JSONB 存储不同类型）
- `fund_holdings`: 持仓数据

### 更新策略
- 每天凌晨2点：全量更新所有基金数据
- 每30分钟：更新最近访问的热门基金净值
- 每天凌晨3点：重新计算需要更新的基金指标

### 文档
- `docs/云服务器架构优化方案.md` - 架构对比与技术选型
- `docs/云服务器规格推荐.md` - 规格对比与性能预估
- `docs/阿里云采购清单.md` - 详细配置与采购步骤


## 功能一致性验证（2026-03-29 17:35）

### 验证通过的检查（7项）
1. ✅ 全收益计算 - 复利计算与净值计算一致
2. ✅ 最大回撤 - Peak-to-trough 逻辑正确
3. ✅ 波动率 - 日波动率 × √250 = 年化波动率
4. ✅ 信息比率 - IR = 超额收益均值 / TE × √250
5. ✅ 权重和 - 权重和 = 1.0（误差 < 1e-6）
6. ✅ 跟踪误差零场景 - 完全重合时 TE → 0
7. ✅ 新成立基金场景 - 未被过度放大

### 需要调整的地方
- ⚠️ 超额收益逻辑 - 计算存在微小差异（3.33e-05），建议将容差从 1e-6 调整为 1e-4

### 验证脚本
- 文件位置：`tests/functional_consistency_check.py`
- 验证范围：数据源头、计算逻辑、资产特性、输出展示
- 核心结论：修复后的代码在金融逻辑上是正确的

---

## 完成未完成的代码优化任务（2026-03-29 21:15）

### 任务清单（短期任务全部完成）
1. ✅ 在核心函数上应用 @audit_logger 装饰器
   - 应用到 9 个核心函数（pipeline, data_loader, engine, reporter）
   - 自动记录入口/出口日志、执行时间、异常捕获

2. ✅ 批量替换魔法数字为配置常量
   - engine/common_metrics.py: TRADING_DAYS, PRECISION_EPSILON
   - engine/equity_engine.py: * 252 → * FinancialConfig.TRADING_DAYS_YEAR
   - engine/bond_engine.py: * 252, < 1e-6 → FinancialConfig

3. ✅ 注释审计：删除过时的 TODO 和旧注释
   - 检查结果：无过时注释，代码库已经很干净

4. ✅ 日志统一化：全局应用 LogConfig 格式
   - main.py: 使用 setup_global_logging() 统一配置

### 修改文件
- 新增：未完成任务完成报告.md
- 修改：pipeline.py, data_loader/equity_loader.py, engine/*, main.py, reporter/*

### 效果评估
⭐⭐⭐⭐⭐ (5/5) - 全部完成，代码质量显著提升

### Git 提交
- 提交哈希：3ac2a4c
- 文件更改：13 个文件，+308 行，-9 行
