# 统计模块使用说明

## 功能介绍
`fund_tracker.py` 提供独立的数据统计功能，包括：
- PV（页面访问量）
- UV（独立访客数）
- 搜索分析次数
- 独立基金数

## 使用方法

### 1. 在 fund_analysis.py 开头添加导入

```python
# 添加统计模块导入
from fund_tracker import init_db, get_or_create_session, record_visit, record_search, get_statistics
```

### 2. 在程序开始处初始化数据库

```python
# 在主函数开始处调用
def main():
    # 初始化统计数据库
    init_db()
    
    # 记录页面访问
    record_visit()
    
    # 原有的代码...
    render_css()
```

### 3. 在搜索按钮点击时记录搜索

```python
# 在搜索按钮点击事件中
if col_btn.button('🚀 开始分析'):
    # 记录搜索
    record_search(fund_code, fund_name)
    
    # 原有的分析逻辑...
```

### 4. 在页面底部显示统计信息

```python
# 在页面底部添加
pv, uv, search_count, unique_funds = get_statistics()
st.markdown('---')
st.info(f"📊 **统计数据** | 访问量: {pv} | 独立访客: {uv} | 搜索分析: {search_count} 次 | 独立基金: {unique_funds} 只")
```

## 数据库文件
统计数据存储在 `fund_tracker.db` 文件中，包含以下表：

### visits 表（访问记录）
- id: 主键
- visit_time: 访问时间
- session_id: 会话ID
- user_ip: 用户IP地址

### searches 表（搜索记录）
- id: 主键
- search_time: 搜索时间
- fund_code: 基金代码
- fund_name: 基金名称
- session_id: 会话ID
- user_ip: 用户IP地址

## 注意事项
1. 数据库文件会自动创建，无需手动配置
2. Session ID 基于时间戳生成，每次刷新页面会变化
3. 统计信息实时更新，无需手动刷新
4. 如果不需要统计功能，可以移除相关导入和调用
