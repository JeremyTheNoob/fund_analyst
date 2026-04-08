#!/bin/bash
# init_server.sh — 腾讯云轻量服务器初始化脚本
# ============================================
#
# 使用方式（在服务器上执行）：
#   curl -sL https://raw.githubusercontent.com/JeremyTheNoob/fund_analyst/main/scripts/init_server.sh | bash
#
# 或者手动下载后执行：
#   chmod +x scripts/init_server.sh
#   ./scripts/init_server.sh
#
# 该脚本会：
# 1. 安装 Python 3.11 + pip
# 2. 克隆项目代码
# 3. 安装 Python 依赖
# 4. 配置环境变量
# 5. 首次从 COS 同步 DB
# 6. 配置 systemd 服务（开机自启）
# 7. 启动 Streamlit 服务

set -e

# ═══════════════════════════════════════════════
# 配置区域（按需修改）
# ═══════════════════════════════════════════════

APP_DIR="/opt/fund_analyst"
REPO_URL="https://github.com/JeremyTheNoob/fund_analyst.git"
BRANCH="main"
PORT=8501
PYTHON_VERSION="3.11"

# COS 配置（首次部署时填入，之后可改为从环境变量读取）
# COS_SECRET_ID=""
# COS_SECRET_KEY=""
# COS_BUCKET=""
# COS_REGION=""

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ═══════════════════════════════════════════════
# 1. 系统依赖
# ═══════════════════════════════════════════════

log_info "安装系统依赖..."
if command -v apt-get &> /dev/null; then
    # Ubuntu/Debian
    sudo apt-get update -qq
    sudo apt-get install -y -qq python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python3-pip git tar curl > /dev/null 2>&1
    sudo ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python3 2>/dev/null || true
elif command -v yum &> /dev/null; then
    # CentOS
    sudo yum install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-pip git tar curl > /dev/null 2>&1
fi
log_info "Python 版本: $(python3 --version)"

# ═══════════════════════════════════════════════
# 2. 克隆代码
# ═══════════════════════════════════════════════

if [ -d "$APP_DIR" ]; then
    log_info "更新代码..."
    cd "$APP_DIR" && git pull origin "$BRANCH"
else
    log_info "克隆代码到 $APP_DIR..."
    sudo mkdir -p "$APP_DIR"
    sudo chown $(whoami):$(whoami) "$APP_DIR"
    git clone -b "$BRANCH" "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"

# ═══════════════════════════════════════════════
# 3. Python 虚拟环境 + 依赖
# ═══════════════════════════════════════════════

log_info "创建 Python 虚拟环境..."
python3 -m venv venv
source venv/bin/activate

log_info "安装 Python 依赖..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
pip install cos-python-sdk-v5 -q  # COS SDK

# ═══════════════════════════════════════════════
# 4. 环境变量
# ═══════════════════════════════════════════════

ENV_FILE="$APP_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
    log_warn "创建 .env 文件，请稍后填入 COS 配置"
    cat > "$ENV_FILE" << 'EOF'
# 腾讯云 COS 配置（必填，用于 DB 同步）
COS_SECRET_ID=your-secret-id
COS_SECRET_KEY=your-secret-key
COS_BUCKET=your-bucket-name
COS_REGION=ap-guangzhou

# 可选：自定义 DB 路径
# FUND_DB_PATH=/opt/fund_analyst/data/fund_data.db
EOF
fi

# 加载环境变量
set -a; source "$ENV_FILE"; set +a
export PYTHONPATH="$APP_DIR:$PYTHONPATH"

# ═══════════════════════════════════════════════
# 5. 首次同步 DB
# ═══════════════════════════════════════════════

if [ ! -f "$APP_DIR/data/fund_data.db" ]; then
    if [ "$COS_SECRET_ID" = "your-secret-id" ] || [ -z "$COS_SECRET_ID" ]; then
        log_warn "COS 未配置，跳过 DB 同步。请编辑 $ENV_FILE 后手动运行："
        log_warn "  cd $APP_DIR && source venv/bin/activate && python3 scripts/sync_from_cos.py"
    else
        log_info "从 COS 同步 DB..."
        python3 scripts/sync_from_cos.py
    fi
else
    log_info "本地 DB 已存在，跳过同步"
fi

# ═══════════════════════════════════════════════
# 6. systemd 服务
# ═══════════════════════════════════════════════

SERVICE_NAME="fund-analyst"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

log_info "配置 systemd 服务..."
sudo tee "$SERVICE_FILE" > /dev/null << EOF
[Unit]
Description=Fund Analyst (Streamlit)
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$APP_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$APP_DIR/venv/bin/streamlit run main.py --server.port $PORT --server.address 0.0.0.0 --server.headless true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ${SERVICE_NAME}
sudo systemctl restart ${SERVICE_NAME}

# ═══════════════════════════════════════════════
# 7. 防火墙
# ═══════════════════════════════════════════════

log_info "开放端口 $PORT..."
if command -v ufw &> /dev/null; then
    sudo ufw allow $PORT/tcp 2>/dev/null || true
elif command -v firewall-cmd &> /dev/null; then
    sudo firewall-cmd --permanent --add-port=$PORT/tcp 2>/dev/null || true
    sudo firewall-cmd --reload 2>/dev/null || true
fi

# ═══════════════════════════════════════════════
# 完成
# ═══════════════════════════════════════════════

sleep 3
if sudo systemctl is-active --quiet ${SERVICE_NAME}; then
    SERVER_IP=$(curl -s ifconfig.me 2>/dev/null || echo "<服务器IP>")
    echo ""
    echo "═══════════════════════════════════════"
    echo -e "${GREEN}✅ 部署成功！${NC}"
    echo ""
    echo "  访问地址: http://${SERVER_IP}:${PORT}"
    echo "  项目目录: $APP_DIR"
    echo "  日志查看: sudo journalctl -u ${SERVICE_NAME} -f"
    echo "  重启服务: sudo systemctl restart ${SERVICE_NAME}"
    echo ""
    echo "  ⚠️  请记得编辑 $ENV_FILE 填入 COS 配置"
    echo "     然后运行: cd $APP_DIR && source venv/bin/activate && python3 scripts/sync_from_cos.py"
    echo "═══════════════════════════════════════"
else
    echo ""
    log_error "服务启动失败，请查看日志："
    echo "  sudo journalctl -u ${SERVICE_NAME} -n 50"
fi
