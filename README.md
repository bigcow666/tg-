# Telegram Media Bot

基于 Telethon 的 Telegram 机器人，用于读取消息链接中的媒体，并转存到指定目标群/频道。

## 快速部署

### 一键下载和部署

```bash
git clone https://github.com/bigcow666/tg-.git /root/tg_media_bot
cd /root/tg_media_bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 配置参数

编辑 `telegram_media_bot.py`，修改以下必填参数：

```python
API_ID = 12345678
API_HASH = "0123456789abcdef0123456789abcdef"
BOT_TOKEN = "1234567890:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
ALLOWED_USERS = {123456789}
DEFAULT_TARGET_CHAT_ID = -1001234567890
```

### 启动

```bash
python telegram_media_bot.py
```

---

## 必填参数说明

### API_ID
Telegram 开发者平台申请的 API ID。

```python
API_ID = 12345678
```

### API_HASH
Telegram 开发者平台申请的 API Hash。

```python
API_HASH = "0123456789abcdef0123456789abcdef"
```

### BOT_TOKEN
由 @BotFather 创建机器人后获取的 Token。

```python
BOT_TOKEN = "1234567890:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

### 授权账号 / ALLOWED_USERS
只有这些 Telegram User ID 才能使用这个机器人。

```python
ALLOWED_USERS = {
    123456789,
    987654321,
}
```

说明：
- API_ID / API_HASH：Telegram API 凭证
- BOT_TOKEN：机器人身份凭证
- ALLOWED_USERS：白名单，只有这里列出的账号可以使用机器人

如何获取自己的 User ID：
- 先给机器人发一条消息
- 查看日志或机器人返回信息
- 或者使用 @userinfobot 查询

---

## 机器人权限

如果机器人需要读取/转存群组里的媒体，必须保证：

- 机器人已加入源群/频道
- 能看到消息内容和媒体
- 已加入目标群/频道
- 目标群/频道允许机器人发送消息
- 如需发送媒体，需具备对应权限

目标群 ID 通常是数字形式，例如：

```python
DEFAULT_TARGET_CHAT_ID = -1001234567890
```

注意：
- 不要使用 `@用户名`
- 必须使用数字 ID

---

## BotFather 基础设置

```bash
@BotFather
/newbot
```

常用命令：

```bash
/setname
/setusername
/setdescription
/setuserpic
/token
```

---

## systemd 服务管理（可选）

### 创建服务文件

```bash
sudo nano /etc/systemd/system/tg-media-bot.service
```

写入以下内容：

```ini
[Unit]
Description=Telegram Media Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/tg_media_bot
ExecStart=/root/tg_media_bot/venv/bin/python -u /root/tg_media_bot/telegram_media_bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 管理命令

```bash
# 启动
systemctl start tg-media-bot

# 停止
systemctl stop tg-media-bot

# 重启
systemctl restart tg-media-bot

# 查看状态
systemctl status tg-media-bot --no-pager -l

# 开机自启
systemctl enable tg-media-bot

# 查看日志
journalctl -u tg-media-bot -n 100 --no-pager

# 实时日志
journalctl -u tg-media-bot -f
```

---

## 修改代码后

```bash
# 检查语法
/root/tg_media_bot/venv/bin/python -m py_compile /root/tg_media_bot/telegram_media_bot.py

# 重启服务
systemctl restart tg-media-bot

# 查看日志
journalctl -u tg-media-bot -n 100 --no-pager
```

---

## 常用排查命令

```bash
systemctl status tg-media-bot --no-pager -l; journalctl -u tg-media-bot -n 100 --no-pager
```

---

## 注意事项

- 不要把 API_ID / API_HASH 和 BOT_TOKEN 混用
- 配置 ALLOWED_USERS 时必须填入正确的 User ID
- 目标群必须使用数字 ID
- 机器人必须加入源群和目标群
- 修改代码后必须先检查语法，再重启
