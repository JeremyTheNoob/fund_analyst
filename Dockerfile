FROM python:3.11-slim

# 安装系统依赖（部分 Python 包需要编译）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先只复制依赖文件（利用 Docker layer 缓存，代码改动不会重装依赖）
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# 复制所有代码
COPY . .

# Streamlit 监听端口
EXPOSE 8501

# 健康检查
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# 启动命令
CMD ["streamlit", "run", "main.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
