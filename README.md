# Telegram Media Bot

基于 Telethon 的 Telegram 机器人，用于读取消息链接中的媒体，并转存到指定目标群/频道。

---

## 快速开始

### 一键部署

```bash
git clone https://github.com/bigcow666/tg-.git /root/tg_media_bot
cd /root/tg_media_bot
sudo bash install_telegram_media_bot.sh
```

脚本会自动执行：
- ✅ GitHub 克隆（如果目录已存在则更新）
- ✅ 系统依赖安装
- ✅ Python 虚拟环境创建
- ✅ Python 依赖安装
- ✅ 交互式参数配置
- ✅ systemd 服务创建
- ✅ 自动启动并设置开机自启

### 部署过程中需要输入

```
API_ID: 12345678
API_HASH: 0123456789abcdef0123456789abcdef
BOT_TOKEN: 1234567890:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
授权账号 User ID: 123456789,987654321
目标群 ID: -1001234567890
缓存大小限制 (GB): 10
```

---

## 必填参数说明

### API_ID
Telegram 开发者平台申请的 API ID。

```python
API_ID = 12345678
```

获取位置：https://my.telegram.org/apps

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

### ALLOWED_USERS
只允许这些 Telegram User ID 使用此机器人。

```python
ALLOWED_USERS = {
    123456789,
    987654321
}
```

如何获取自己的 User ID：
- 给机器人发送一条消息
- 查看日志或机器人返回信息
- 或使用 @userinfobot 查询

### DEFAULT_TARGET_CHAT_ID
默认转存目标群 ID。

```python
DEFAULT_TARGET_CHAT_ID = -1001234567890
```

注意：
- 不要使用 `@用户名`
- 必须使用数字 ID

### MAX_CACHE_GB
缓存大小限制（单位：GB）。

```python
MAX_CACHE_GB = 10
```

---

## 机器人权限要求

如果机器人需要读取/转存群组里的媒体，必须保证：

- ✅ 机器人已加入源群/频道
- ✅ 能看到消息内容和媒体
- ✅ 已加入目标群/频道
- ✅ 目标群/频道允许机器人发送消息
- ✅ 如需发送媒体，需具备对应权限

---

## BotFather 基础设置

创建新机器人：

```bash
@BotFather
/newbot
```

常用命令：

```bash
/setname           # 设置机器人名称
/setusername       # 设置机器人用户名
/setdescription    # 设置机器人描述
/setuserpic        # 设置机器人头像
/token             # 查看或重置 Token
```

---

## 部署后的管理

### systemd 命令

```bash
# 查看状态
systemctl status tg-media-bot

# 启动
systemctl start tg-media-bot

# 停止
systemctl stop tg-media-bot

# 重启
systemctl restart tg-media-bot

# 开机自启（已自动设置）
systemctl enable tg-media-bot

# 取消开机自启
systemctl disable tg-media-bot
```

### 日志查看

```bash
# 实时日志
journalctl -u tg-media-bot -f

# 查看最近 50 条
journalctl -u tg-media-bot -n 50 --no-pager

# 查看最近 100 条
journalctl -u tg-media-bot -n 100 --no-pager

# 查看今天的日志
journalctl -u tg-media-bot --since today --no-pager
```

---

## 修改配置后

如果需要修改 `telegram_media_bot.py` 中的参数：

```bash
# 1. 编辑配置文件
nano /root/tg_media_bot/telegram_media_bot.py

# 2. 检查语法
/root/tg_media_bot/venv/bin/python -m py_compile /root/tg_media_bot/telegram_media_bot.py

# 3. 重启服务
systemctl restart tg-media-bot

# 4. 查看日志
journalctl -u tg-media-bot -n 100 --no-pager
```

---

## 常见排查

### 查看完整状态和日志

```bash
systemctl status tg-media-bot --no-pager -l; journalctl -u tg-media-bot -n 100 --no-pager
```

### 检查机器人是否运行

```bash
systemctl is-active tg-media-bot
```

### 查看 Python 进程

```bash
ps aux | grep '[t]elegram_media_bot.py'
```

### 检查虚拟环境

```bash
ls -la /root/tg_media_bot/venv/bin/python
```

---

## 常用命令速记

```bash
# 启动/停止/重启
systemctl start tg-media-bot
systemctl stop tg-media-bot
systemctl restart tg-media-bot

# 查看状态和日志
systemctl status tg-media-bot --no-pager -l
journalctl -u tg-media-bot -f

# 日志查询
journalctl -u tg-media-bot -n 100 --no-pager
journalctl -u tg-media-bot --since today --no-pager

# 开机自启
systemctl enable tg-media-bot
systemctl disable tg-media-bot
```

---

## 注意事项

- ⚠️ 不要把 `API_ID` / `API_HASH` 和 `BOT_TOKEN` 混用
- ⚠️ `ALLOWED_USERS` 必须填写正确的 Telegram User ID
- ⚠️ 目标群必须使用数字 ID，不能使用 `@用户名`
- ⚠️ 机器人必须加入源群和目标群
- ⚠️ 如需发送媒体，必须给机器人相应权限
- ⚠️ 修改代码后务必先检查语法，再重启服务

---

## 项目说明

- 本项目默认使用 `MemorySession()`，不依赖本地 session 文件
- `telegram_media_bot.py` 的业务逻辑保持不变
- 安装脚本仅负责部署、配置和服务管理
- GitHub 仓库：https://github.com/bigcow666/tg-
