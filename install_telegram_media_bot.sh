#!/usr/bin/env bash
set -Eeuo pipefail

# 上传目录：
# /root/telegram_media_bot.py/
# ├── telegram_media_bot.py
# └── install_telegram_media_bot.sh
#
# 最终运行目录：
# /root/tg_media_bot/
# └── telegram_media_bot.py

UPLOAD_DIR="/root/telegram_media_bot.py"
UPLOAD_PY="$UPLOAD_DIR/telegram_media_bot.py"

APP_DIR="/root/tg_media_bot"
APP="$APP_DIR/telegram_media_bot.py"
VENV="$APP_DIR/venv"
SERVICE="/etc/systemd/system/tg-media-bot.service"

echo "=========================================="
echo " Telegram Media Bot - Full Bootstrap"
echo "=========================================="

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: 请使用 root 执行"
    exit 1
fi

if [ ! -f "$UPLOAD_PY" ]; then
    echo "ERROR: 找不到程序：$UPLOAD_PY"
    echo "请确认两个文件位于：$UPLOAD_DIR/"
    exit 1
fi

echo "[1/10] 更新系统软件源..."
apt-get update

echo "[2/10] 安装系统依赖..."
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
    wget

echo "[3/10] 创建运行目录..."
mkdir -p "$APP_DIR"
mkdir -p "$APP_DIR/downloads"
mkdir -p "$APP_DIR/cache"
mkdir -p "$APP_DIR/logs"

echo "[4/10] 部署程序..."
if [ -f "$APP" ]; then
    BACKUP="$APP.bak.$(date +%Y%m%d-%H%M%S)"
    cp "$APP" "$BACKUP"
    echo "旧程序已备份：$BACKUP"
fi

cp "$UPLOAD_PY" "$APP"
chmod 755 "$APP"

echo "程序已部署到：$APP"

echo "[5/10] 创建 Python 虚拟环境..."
if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
fi

"$VENV/bin/python" -m pip install --upgrade pip setuptools wheel

echo "[6/10] 安装 Python 依赖..."
"$VENV/bin/pip" install --upgrade \
    "telethon==1.45.0" \
    cryptg \
    pillow \
    hachoir \
    aiohttp

echo "[7/10] 检查环境..."
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

echo "[8/10] 检查机器人代码..."
"$VENV/bin/python" -m py_compile "$APP"
echo "Python 语法检查：OK"

echo "[9/10] 创建 systemd 保活服务..."
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

echo "[10/10] 启动机器人..."
systemctl restart tg-media-bot
sleep 3

echo
echo "=========================================="
echo " 部署完成"
echo "=========================================="

systemctl --no-pager --full status tg-media-bot

echo
echo "运行程序：$APP"
echo "Python 环境：$VENV"
echo
echo "实时日志："
echo "journalctl -u tg-media-bot -f"
echo
echo "重启："
echo "systemctl restart tg-media-bot"
