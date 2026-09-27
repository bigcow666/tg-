#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/root/tg_media_bot"
APP="$APP_DIR/telegram_media_bot.py"
VENV="$APP_DIR/venv"
SERVICE="/etc/systemd/system/tg-media-bot.service"

echo "=========================================="
echo " Telegram Media Bot - 一键部署脚本"
echo "=========================================="

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: 请使用 root 执行"
    exit 1
fi

echo "[1/10] 从 GitHub 克隆仓库..."
if [ -d "$APP_DIR" ]; then
    echo "目录已存在，更新中..."
    cd "$APP_DIR"
    git pull
else
    git clone https://github.com/bigcow666/tg-.git "$APP_DIR"
    cd "$APP_DIR"
fi

echo "[2/10] 更新系统软件源..."
apt-get update

echo "[3/10] 安装系统依赖..."
apt-get install -y \
    python3 \
    python3-venv \
    python3-pip \
    python3-dev \
    build-essential \
    pkg-config \
    ffmpeg \
    ca-certificates \
    curl \
    wget \
    git

echo "[4/10] 创建 Python 虚拟环境..."
if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
fi

"$VENV/bin/python" -m pip install --upgrade pip setuptools wheel

echo "[5/10] 安装 Python 依赖..."
"$VENV/bin/pip" install --upgrade \
    "telethon==1.45.0" \
    cryptg \
    pillow \
    hachoir \
    aiohttp

echo "[6/10] 检查环境..."
"$VENV/bin/python" - <<'PY'
import sys
import telethon
import cryptg
import PIL
import hachoir
import aiohttp

print("Python   :", sys.version.split()[0])
print("Telethon :", telethon.__version__)
print("cryptg   : OK")
print("Pillow   :", PIL.__version__)
print("hachoir  : OK")
print("aiohttp  :", aiohttp.__version__)
PY

ffmpeg -version | head -n 1
ffprobe -version | head -n 1

echo "[7/10] 交互式配置机器人参数..."
echo ""
echo "=========================================="
echo "请输入必填参数"
echo "=========================================="
echo ""

read -p "API_ID (Telegram 开发者平台): " API_ID
read -p "API_HASH: " API_HASH
read -p "BOT_TOKEN (@BotFather 获取): " BOT_TOKEN
read -p "授权账号 User ID (多个用逗号分隔，如: 123456789,987654321): " ALLOWED_USERS_INPUT
read -p "目标群 ID (例如: -1001234567890): " TARGET_CHAT_ID
read -p "缓存大小限制 (GB, 默认10): " MAX_CACHE_GB
MAX_CACHE_GB=${MAX_CACHE_GB:-10}

echo ""
echo "[8/10] 更新配置文件..."

# 使用 Python 修改配置文件
"$VENV/bin/python" << PYSCRIPT
import re

# 读取配置文件
with open('$APP', 'r', encoding='utf-8') as f:
    content = f.read()

# 修改 API_ID
content = re.sub(r'^API_ID = .*', 'API_ID = $API_ID', content, flags=re.MULTILINE)

# 修改 API_HASH
content = re.sub(r'^API_HASH = .*', 'API_HASH = "$API_HASH"', content, flags=re.MULTILINE)

# 修改 BOT_TOKEN
content = re.sub(r'^BOT_TOKEN = .*', 'BOT_TOKEN = "$BOT_TOKEN"', content, flags=re.MULTILINE)

# 修改 DEFAULT_TARGET_CHAT_ID
content = re.sub(r'^DEFAULT_TARGET_CHAT_ID = .*', 'DEFAULT_TARGET_CHAT_ID = $TARGET_CHAT_ID', content, flags=re.MULTILINE)

# 修改 MAX_CACHE_GB
content = re.sub(r'^MAX_CACHE_GB = .*', 'MAX_CACHE_GB = $MAX_CACHE_GB', content, flags=re.MULTILINE)

# 修改 ALLOWED_USERS
users_str = "$ALLOWED_USERS_INPUT"
users = [int(u.strip()) for u in users_str.split(',') if u.strip()]

if users:
    allowed_users_str = "ALLOWED_USERS = {\n"
    for i, user in enumerate(users):
        if i < len(users) - 1:
            allowed_users_str += f"    {user},\n"
        else:
            allowed_users_str += f"    {user}\n"
    allowed_users_str += "}"
    
    pattern = r'ALLOWED_USERS = \{[^}]*\}'
    content = re.sub(pattern, allowed_users_str, content, flags=re.DOTALL)

# 写回文件
with open('$APP', 'w', encoding='utf-8') as f:
    f.write(content)

print("✓ 配置已更新")
PYSCRIPT

echo "[9/10] 检查机器人代码..."
"$VENV/bin/python" -m py_compile "$APP"
echo "✓ Python 语法检查：OK"

echo "[10/10] 创建 systemd 服务..."
cat > "$SERVICE" <<EOF
[Unit]
Description=Telegram Media Transfer Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$VENV/bin/python -u $APP
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable tg-media-bot
systemctl restart tg-media-bot
sleep 3

echo ""
echo "=========================================="
echo " ✅ 部署完成！"
echo "=========================================="
echo ""
echo "📋 配置信息："
echo "  安装目录: $APP_DIR"
echo "  API_ID: $API_ID"
echo "  API_HASH: ${API_HASH:0:16}..."
echo "  BOT_TOKEN: ${BOT_TOKEN:0:20}..."
echo "  授权账号: $ALLOWED_USERS_INPUT"
echo "  目标群ID: $TARGET_CHAT_ID"
echo "  缓存限制: ${MAX_CACHE_GB} GB"
echo ""
echo "🚀 常用命令："
echo "  查看状态: systemctl status tg-media-bot"
echo "  实时日志: journalctl -u tg-media-bot -f"
echo "  重启服务: systemctl restart tg-media-bot"
echo ""

systemctl --no-pager --full status tg-media-bot
