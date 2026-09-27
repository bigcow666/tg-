#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/root/tg_media_bot"
ROOT_PY="/root/telegram_media_bot.py"
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

# 1. 检查上传到 /root 的程序
if [ ! -f "$ROOT_PY" ] && [ ! -f "$APP" ]; then
    echo "ERROR: 找不到 telegram_media_bot.py"
    echo "请先把 telegram_media_bot.py 上传到 /root/"
    exit 1
fi

echo
echo "[1/10] 更新系统软件源..."
apt-get update

echo
echo "[2/10] 安装系统依赖..."
apt-get install -y \
    python3 \
    python3-venv \
    python3-pip \
    python3-dev \
    build-essential \
    gcc \
    g++ \
    make \
    pkg-config \
    ffmpeg \
    ca-certificates \
    curl \
    wget

echo
echo "[3/10] 创建程序目录..."
mkdir -p "$APP_DIR"
mkdir -p "$APP_DIR/downloads"
mkdir -p "$APP_DIR/cache"
mkdir -p "$APP_DIR/logs"

echo
echo "[4/10] 移动程序..."

# 新机器：/root/telegram_media_bot.py -> /root/tg_media_bot/telegram_media_bot.py
if [ -f "$ROOT_PY" ]; then
    if [ -f "$APP" ]; then
        cp "$APP" "$APP.bak.$(date +%Y%m%d-%H%M%S)"
    fi
    mv "$ROOT_PY" "$APP"
fi

if [ ! -f "$APP" ]; then
    echo "ERROR: 程序移动失败：$APP"
    exit 1
fi

echo "程序位置：$APP"

echo
echo "[5/10] 创建 Python venv..."

if [ ! -d "$VENV" ]; then
    python3 -m venv "$VENV"
fi

"$VENV/bin/python" -m pip install --upgrade \
    pip \
    setuptools \
    wheel

echo
echo "[6/10] 安装 Python 依赖..."

"$VENV/bin/pip" install --upgrade \
    "telethon==1.45.0" \
    cryptg \
    pillow \
    hachoir \
    aiohttp

echo
echo "[7/10] 检查环境..."

echo "Python:"
"$VENV/bin/python" --version

echo
echo "Telethon:"
"$VENV/bin/python" -c 'import telethon; print(telethon.__version__)'

echo
echo "cryptg:"
"$VENV/bin/python" -c 'import cryptg; print("OK")'

echo
echo "Pillow:"
"$VENV/bin/python" -c 'import PIL; print(PIL.__version__)'

echo
echo "hachoir:"
"$VENV/bin/python" -c 'import hachoir; print("OK")'

echo
echo "aiohttp:"
"$VENV/bin/python" -c 'import aiohttp; print(aiohttp.__version__)'

echo
echo "FFmpeg:"
ffmpeg -version | head -n 1

echo
echo "FFprobe:"
ffprobe -version | head -n 1

echo
echo "[8/10] 检查机器人代码..."

"$VENV/bin/python" -m py_compile "$APP"

echo "Python 语法检查：OK"

echo
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

echo
echo "[10/10] 启动机器人..."

systemctl restart tg-media-bot

sleep 3

echo
echo "=========================================="
echo " 部署完成"
echo "=========================================="

systemctl --no-pager --full status tg-media-bot

echo
echo "程序："
echo "$APP"

echo
echo "Python 环境："
echo "$VENV"

echo
echo "实时日志："
echo "journalctl -u tg-media-bot -f"

echo
echo "重启："
echo "systemctl restart tg-media-bot"

echo
echo "停止："
echo "systemctl stop tg-media-bot"
