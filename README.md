# 基金穿透式分析工具

基于 AkShare 数据源的基金穿透式分析工具，提供 4 个维度的深度分析：

## 📊 分析维度

1. **风险与收益平衡**
   - 夏普比率
   - 最大回撤
   - 年化波动率
   - 累计收益率

2. **基金经理及其风格**
   - 任职回报与年限
   - 风格偏好
   - 机构持仓比例

3. **持仓底牌**
   - 前十大重仓股占比
   - 重仓股的透视分析
   - 换手率

4. **成本与规则**
   - 费率结构
   - 分红方式

## 🚀 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 运行应用
```bash
streamlit run fund_analysis.py
```

### 3. 浏览器访问
打开 http://localhost:8501

## 📦 数据来源
- [AkShare](https://akshare.akfamily.xyz/) - 开源财经数据接口

## 🌐 部署到 Hugging Face (免费)
1. 创建 Hugging Face 账号
2. 新建 Space，选择 "Streamlit"
3. 上传所有文件
4. 自动生成访问链接
