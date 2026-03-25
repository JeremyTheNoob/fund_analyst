# 基金穿透式分析 - 长期记忆

## 项目目标
- 产品：基金穿透式分析工具，对外运营，含付费功能
- 当前状态：v8.0 专业量化版已完成，待推送到GitHub部署

## 技术栈（当前）
- 前端/后端：Python + Streamlit
- 数据源：AkShare
- 正确 API（已验证）：
  - `fund_open_fund_info_em(symbol, indicator="单位净值走势")` - 历史净值，列名：净值日期/单位净值/日增长率（取前两列）
  - `fund_individual_basic_info_xq(symbol)` - 雪球基础信息（基准文本）
  - `fund_portfolio_hold_em(symbol, date="2024")` - 持仓
  - `stock_zh_index_daily_em(symbol="sh000300")` - 指数日行情（沪深300/中证1000等）
  - `bond_new_composite_index_cbond(indicator="财富")` - 中债综合指数（注意参数是"财富"不是"总值"）
  - `bond_zh_us_rate(start_date=xxx)` - 10年国债收益率（列名：日期/中国国债收益率10年）
  - `bond_china_yield(start_date, end_date)` - 国债收益率曲线（行数可能为空，有备用方案）

## 当前版本
- **v9.5**（2026-03-25）：全面代码审查，8项潜在Bug修复：
  - **致命Bug**：bond模式下translate_results/calc_radar_scores传入的是`{'bond': bond_res}`嵌套字典，直接`results.get('duration')`取不到值→修复为调用前展开子字典
  - **高风险**：fetch_ff_factors内部inner join（small/val/grw），某个指数停牌一天截断全部因子数据→改为left join+ffill(3天)
  - **中风险**：_empty_ff_result缺少r_squared_recent/residual_insight字段→补全
  - **中风险**：estimate_hidden_cost未接收type_category参数，所有基金永远用equity隐性费率0.15%→修复函数签名+调用传参
  - **中风险**：run_brinson用`dir()`判断变量存在（Python反模式）→改为直接引用
  - **中风险**：fetch_bond_index首行NaN未填0（与fetch_index_daily不一致）→补fillna(0)
  - **低风险**：sector基金在bond_ratio≤0.10时右列不渲染，估值预警不显示→修复判断条件
  - **Bug修复（09:26）**：显性费率永远为0→根因是原用的雪球接口和天天`fund_open_fund_info_em(indicator='基金概况')`均不返回费率字段；改用 `fund_fee_em(indicator='运作费用')` 主方案 + `fund_individual_detail_info_xq`兜底，实测正确
  - 文件：`fund_analysis.py` = `fund_analysis_v9.5.py`
- **v9.3**（2026-03-24）：四项实战优化 + 累计收益图三项修复：
  - **缓存升级**：`fetch_index_daily/fetch_ff_factors/fetch_treasury_10y/fetch_bond_index/fetch_sw_industry_ret` 全部升级为 `ttl=86400`（公共因子全天缓存），基金特有净值/持仓仍实时拉取
  - **前视偏差标注**：业绩基准小字追加「基于当前公开基准回溯，历史基准变更期间数据仅供参考」
  - **残差分析**：`run_ff_model()` 新增 `r_squared_recent` + `residual_insight`；全期-近期R²>0.25→橙色预警「疑有非标资产收益」；R²<0.4→蓝色独立风格说明；展示于Part 2权益解读卡下方
  - **合规补丁**：Part 4 末尾完整免责卡片（不构成投资建议/模型局限/前视偏差/数据来源/风险提示）
  - **累计收益图修复**：①inner join对齐日期防起点偏移；②sector类型改用申万行业指数作基准；③bm_text赋值顺序修复
  - 文件：`fund_analysis.py` = `fund_analysis_v9.3.py`
  - **合规补丁**：Part 4 末尾完整免责卡片（不构成投资建议/模型局限/前视偏差/数据来源/风险提示）
  - 文件：`fund_analysis.py` = `fund_analysis_v9.3.py`
- **v9.2**（2026-03-24）：数据层全面修复，7项关键修复：
  - **F1 致命修复**：fetch_nav 从「单位净值」→「累计净值」，彻底修复分红跳空假信号（原代码分红日会误算为单日暴跌33%）
  - **F2 致命修复**：FF因子 RMW 改为列级降维：NaN>50%直接 drop 列，不再用 NaN 列污染全表导致样本清零；MOM→Short_MOM 避免与经典 Carhart 12月定义混淆
  - **F3 高风险修复**：fetch_treasury_10y 将 bond_zh_us_rate 升为主力方案，原 bond_china_yield 降为备用；两方案均加 ffill() 填充非交易日
  - **F4 高风险修复**：fetch_stock_valuation_alert 加新股（<250条）/亏损股（PE≤0）专项拦截，加 note 字段提示，单只失败不影响整批
  - **F5 逻辑修复**：fetch_holdings 新增第0层 fund_portfolio_asset_allocation_em（历史季度资产配置表）；弃用粗暴1.4系数，改为按前十大集中度自适应（高集中×1.1/均衡×1.6/高分散×2.2）
  - **F6 逻辑修复**：隐性费率废弃基于净值负自相关的玄学换手率估算（Roll模型不适用于基金净值），改为行业经验中位数兜底（权益0.15%/债券0.03%）
  - **F7 工程修复**：全局 retry_on_failure 装饰器，所有 fetch_ 函数支持3次自动重试
  - 文件：`fund_analysis.py` = `fund_analysis_v9.2.py`
- **v9.1**（2026-03-24）：UI 全面重构，8项优化：
  - Part 3 收益曲线提前至 Part 1 后（先看结果再看拆解）+ 去掉 expander
  - Part 2.5 动态列宽兜底（预判左右列内容，避免空白块）
  - 动态标题：「这 +X.XX% 是怎么赚/亏的？」涨红跌绿
  - 雷达图&进度条颜色语义化：≥80绿/60-79橙/<60红（统一阈值）
  - Part 4 risk 字段定性/定量分离（只下结论，不重复 Part 2.5 的数字）
  - 权益基金触发久期模型时加「跨界扫描发现」微文案提示
  - 压测卡片 CSS: Grid → Flexbox+min-width，移动端自动折行
  - 文件：`fund_analysis.py` = `fund_analysis_v9.1.py`
- **v9.0**（2026-03-24）：新增 Part 2.5「风险提示」板块，三大功能：
  - `performance_decomposition()` 收益拆解功劳簿（Brinson配置+行业Alpha+残差）
  - `fetch_stock_valuation_alert()` 前十大重仓股PE历史分位预警（百度财经API）
  - `bond_stress_test()` 债券久期压力测试（3场景10/50/100BP）
  - 股债双杀联合预警（混合型专属）
- **v8.9**（2026-03-24）：报告开头新增5维综合实力雷达图（Plotly scatterpolar）
  - 五维：超额能力/风险控制/性价比/风格稳定/业绩持续，各归一化0-100
  - 函数：`calc_radar_scores()` + `plot_fund_radar()`
  - 展示：Part 0（雷达图+评分卡+形状识别），位于 Part 1 基本信息之前
- **v8.8**（2026-03-24）：翻译层全面升级（性格标签+情绪指标+一致性判定+四象限分类）
- 文件：`fund_analysis.py`（主部署文件）= `fund_analysis_v9.0.py`

## v8.0 核心架构（5层）
1. **数据层**：基本信息 / 净值 / FF因子 / 国债利率 / 持仓季报
2. **模型层**：
   - 权益类：FF三/五因子 + Carhart四因子（自动选模型）
   - 债券类：T-Model久期归因（从净值反推Duration + Convexity）
   - 混合类：Brinson归因（配置效应+选择效应+交互效应）+ 20日滚动Beta监控 + 风格漂移预警
   - 行业/主题型：中性化Alpha + 跟踪误差
3. **逻辑网关**：股票仓位>80%→权益，<20%→债券，中间→混合；行业/主题单独处理
4. **翻译层**：大白话四维诊断（性格/实力/风险/建议）+ 综合评分(0-100)
5. **展示层**：Streamlit四部分布局

## FF因子构建方案（A+C混合）
- SMB = 中证1000(sh000852) - 沪深300(sh000300)
- HML = 国证价值(sz399371) - 国证成长(sz399370)
- MOM = 21日滚动均值收益率
- 说明：方案B（全市场截面）数据量巨大且无历史快照接口，暂不实现

## 基准构建规则
- 从招募说明书解析（正则匹配×XX%格式）
- 动态加权：先算各成分日收益率，再加权（不能直接加指数点数）
- 未解析到基准时按类型使用默认（权益→沪深300，债券→中债综合，混合→60/40）

## 债券仓位判断
- 静态法：季报`fund_portfolio_hold_em`（最准但滞后）
- 动态法：20日滚动双因子回归（股票+债券指数），偏差>15%触发风格漂移预警

## 商业模式共识
- 免费3次/天 + 付费会员（无限 + 高级功能）
- 获客渠道：小红书+抖音（图文+AI配音）
- 部署地址：https://fundanalyst-supq2s7qdckbk9468a83bc.streamlit.app/
- GitHub：https://github.com/JeremyTheNoob/fund_analyst
