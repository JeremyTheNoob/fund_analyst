# 新图表实现计划

## 概述
为基金穿透式分析系统添加4个新图表，提升分析深度和用户体验。

## 实现步骤

### 步骤1：更新数据模型
**文件**：`fund_quant_v2/models/schema.py`

**修改**：
1. 在 `BondMetrics` 中添加 `credit_spread_history: pd.DataFrame` 字段
2. 在 `EquityMetrics` 中添加 `style_history: List[Dict]` 字段（用于风格箱轨迹）

### 步骤2：在引擎中添加计算逻辑
**文件**：`fund_quant_v2/engine/common_metrics.py`
**添加**：几何超额收益计算函数 `geometric_excess_return()`

**文件**：`fund_quant_v2/engine/equity_engine.py`
**添加**：风格箱坐标计算和风格漂移轨迹

**文件**：`fund_quant_v2/engine/bond_engine.py`
**添加**：信用利差历史计算（期限匹配 + SMA平滑）

**文件**：`fund_quant_v2/engine/index_engine.py`
**添加**：跟踪偏离度统计（直方图分布 + 偏度/峰度）

### 步骤3：在图表生成器中添加图表函数
**文件**：`fund_quant_v2/reporter/chart_gen.py`

**添加4个新函数**：
1. `_excess_return_chart()` - 超额收益动态曲线（通用）
2. `_style_box_chart()` - 风格箱（权益类）
3. `_credit_spread_chart()` - 信用利差走势（固收类）
4. `_tracking_diff_histogram()` - 跟踪偏离度分布图（指数/ETF）

**更新 `generate_chart_data()`**：
- 在通用图表部分调用 `_excess_return_chart()`
- 在权益类图表部分调用 `_style_box_chart()`
- 在固收类图表部分调用 `_credit_spread_chart()`
- 在指数类图表部分调用 `_tracking_diff_histogram()`

### 步骤4：测试验证
**测试用例**：
1. 测试超额收益曲线的零点重置和异常处理
2. 测试风格箱坐标归一化（1-3离散坐标）
3. 测试信用利差的期限匹配和SMA平滑
4. 测试跟踪偏离度的直方图bin设置和正态分布拟合

## 关键技术点

### 1. 超额收益动态曲线
- 几何超额：`excess_ret = (1 + fund_ret) / (1 + bm_ret) - 1`
- 内连接对齐日期
- 处理 `bm_ret = -1` 的极端情况
- 零点重置：从区间首日开始重新计算

### 2. 风格箱
- SMB因子 → 规模坐标：>0.5→小盘(3), <-0.5→大盘(1)
- HML因子 → 价值坐标：>0.5→价值(1), <-0.5→成长(3)
- 3×3网格可视化
- 轨迹：最近4个季度的风格位置

### 3. 信用利差走势
- 期限匹配：同期限信用债 - 同期限国债
- 5日SMA平滑处理毛刺
- 监测信用风险扩大

### 4. 跟踪偏离度分布
- 动态bin设置：`bin_width = max(0.01, std/5)`
- 正态分布曲线叠加
- 统计量：偏度、峰度
- 判断：负偏移 → 持续隐形损耗

## 预期效果

- **超额收益曲线**：直观展示基金跑赢/跑输基准的时点
- **风格箱**：量化展示基金风格特征和漂移
- **信用利差走势**：监测债基信用风险变化
- **跟踪偏离度分布**：评估指数/ETF工具精密程度
