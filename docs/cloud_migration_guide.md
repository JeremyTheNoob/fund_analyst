# 基金穿透式分析 - 上云迁移指南

## 本地演示已完成 ✅

本地演示版本已就绪，使用 SQLite 数据库运行：

```bash
# 运行测试
python3 test_local_demo.py

# 启动 Streamlit
python3 run_local_demo.py
```

---

## 云端架构

```
┌─────────────────────────────────────────────────────────────┐
│                      用户访问层                              │
│         https://xxx.gz.apigw.tencentcs.com                  │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                   腾讯云 Serverless                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │   云函数 SCF    │  │  TDSQL-C        │  │    COS      │ │
│  │   Streamlit    │  │  Serverless     │  │  对象存储    │ │
│  │   (Docker)     │  │  (MySQL)        │  │  (Parquet)  │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
┌─────────────────────────────────────────────────────────────┐
│                   GitHub Actions (定时任务)                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  daily_update   │  │  prewarm_cache  │  │   数据校验   │ │
│  │  (每日6:00)     │  │  (预热缓存)      │  │  (完整性检查)│ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## 迁移步骤

### Phase 1: 创建腾讯云资源

#### 1.1 创建 TDSQL-C Serverless (MySQL)

1. 登录 [腾讯云控制台](https://console.cloud.tencent.com/)
2. 进入 **云数据库 TDSQL-C**
3. 创建实例：
   - 计费模式：Serverless
   - 数据库版本：MySQL 8.0
   - 地域：广州/上海（选择离用户近的）
   - 算力配置：0.5-2 CCU（自动扩缩容）
   - 存储：50GB

4. 创建数据库和用户：
```sql
CREATE DATABASE fund_analyst CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'fund_user'@'%' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON fund_analyst.* TO 'fund_user'@'%';
FLUSH PRIVILEGES;
```

#### 1.2 创建 COS 存储桶

1. 进入 **对象存储 COS**
2. 创建存储桶：
   - 名称：`fund-analyst-data`
   - 地域：与数据库相同
   - 访问权限：私有读写

#### 1.3 创建云函数 SCF

1. 进入 **云函数 SCF**
2. 创建函数：
   - 函数名称：`fund-analyst-app`
   - 运行环境：Python 3.9
   - 内存：512MB
   - 超时：60秒

---

### Phase 2: 数据迁移

#### 2.1 导出 SQLite 到 MySQL

```bash
# 安装依赖
pip install sqlite3-to-mysql

# 导出数据
sqlite3mysql \
    -f data/fund_data.db \
    -d fund_analyst \
    -u fund_user \
    -p your_password \
    -h your-tdsql-host.mysql.tencentcloud.com \
    -P 3306
```

#### 2.2 或使用 Python 脚本迁移

```bash
python scripts/migrate_sqlite_to_mysql.py
```

脚本会自动：
- 读取 SQLite 数据库
- 创建 MySQL 表结构
- 分批导入数据（避免内存溢出）

---

### Phase 3: 部署应用

#### 3.1 配置环境变量

在腾讯云 SCF 中设置环境变量：

```bash
FUND_DB_TYPE=mysql
MYSQL_HOST=your-tdsql-host.mysql.tencentcloud.com
MYSQL_PORT=3306
MYSQL_USER=fund_user
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=fund_analyst
SUPABASE_ENABLED=false
```

#### 3.2 部署 Streamlit

```bash
# 打包应用
cd /Users/liuweihua/WorkBuddy/基金穿透式分析
zip -r deploy.zip . -x "*.git*" -x "data/local_cache/*" -x "*.pyc"

# 上传到 SCF 或使用 CLI
tencentcloud scf deploy --function-name fund-analyst-app --zip-file deploy.zip
```

#### 3.3 Dockerfile（可选，用于容器部署）

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

---

### Phase 4: 配置定时任务

#### 4.1 GitHub Actions 更新数据库

修改 `.github/workflows/daily_cache_update.yml`：

```yaml
name: Daily Data Update
on:
  schedule:
    - cron: '0 22 * * *'  # UTC 22:00 = 北京时间 6:00

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Update MySQL Database
        run: python scripts/daily_update_mysql.py
        env:
          MYSQL_HOST: ${{ secrets.MYSQL_HOST }}
          MYSQL_USER: ${{ secrets.MYSQL_USER }}
          MYSQL_PASSWORD: ${{ secrets.MYSQL_PASSWORD }}
          MYSQL_DATABASE: fund_analyst
```

---

## 成本预估

| 组件 | 配置 | 月费用 |
|------|------|--------|
| **云函数 SCF** | 100万次调用 | ¥0（免费额度） |
| **TDSQL-C** | 0.5-2 CCU, 50GB | ¥30-50 |
| **COS** | 10GB 存储 | ¥0-5 |
| **总计** | | **¥30-55/月** |

---

## 一键部署脚本

创建 `scripts/deploy_to_tencent_cloud.py`：

```python
#!/usr/bin/env python3
"""
一键部署到腾讯云
"""
import os
import subprocess

def main():
    print("=" * 60)
    print("基金穿透式分析 - 腾讯云一键部署")
    print("=" * 60)
    
    # 1. 检查环境变量
    required_env = [
        "TENCENT_SECRET_ID",
        "TENCENT_SECRET_KEY",
        "MYSQL_HOST",
        "MYSQL_PASSWORD",
    ]
    
    for env in required_env:
        if not os.environ.get(env):
            print(f"❌ 缺少环境变量: {env}")
            return 1
    
    # 2. 数据迁移
    print("\n📦 步骤 1: 迁移数据到 TDSQL-C...")
    subprocess.run(["python", "scripts/migrate_sqlite_to_mysql.py"], check=True)
    
    # 3. 部署云函数
    print("\n🚀 步骤 2: 部署云函数 SCF...")
    subprocess.run(["python", "scripts/deploy_scf.py"], check=True)
    
    # 4. 配置定时任务
    print("\n⏰ 步骤 3: 配置 GitHub Actions...")
    print("请确保 GitHub Secrets 已配置:")
    print("  - MYSQL_HOST")
    print("  - MYSQL_USER")
    print("  - MYSQL_PASSWORD")
    
    print("\n✅ 部署完成！")
    print(f"访问地址: https://{os.environ.get('SCF_DOMAIN', 'xxx')}")
    
    return 0

if __name__ == "__main__":
    exit(main())
```

---

## 回滚方案

如果云端部署出现问题，可以立即回滚到本地：

```bash
# 切换回本地 SQLite
export FUND_DB_TYPE=sqlite
export SUPABASE_ENABLED=false
python3 run_local_demo.py
```

---

## 下一步

1. **注册腾讯云账号**（如果还没有）
2. **创建 TDSQL-C 实例**
3. **运行数据迁移脚本**
4. **部署云函数**

要我帮你开始实施吗？
