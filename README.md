install安装环境，另一个文件输入参数

Telegram Bot 必填参数
你的这个 Telethon 机器人至少涉及下面几个核心参数。
1. API_ID
这是 Telegram 开发者 API 的 ID。
格式类似：
12345678
不是 BotFather 给你的 Bot Token。
2. API_HASH
Telegram API Hash。
格式类似：
0123456789abcdef0123456789abcdef
它和 API_ID 是一组。
即：
API_ID
API_HASH
获取位置：
Telegram API Development Tools⁠�
三、BOT_TOKEN
通过 @BotFather 创建机器人后获得。
格式类似：
1234567890:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
代码配置一般类似：
BOT_TOKEN = "你的 Bot Token"
BOT_TOKEN、API_ID、API_HASH 三个不要混淆。
API_ID       → Telegram API 身份
API_HASH     → Telegram API 密钥
BOT_TOKEN    → 机器人身份
四、SESSION
你的程序如果使用 Telethon Bot 登录，通常会产生 session。
例如：
telegram_media_bot.session
或者代码中可能指定：
SESSION_NAME = "telegram_media_bot"
第一次启动时可能需要登录/授权。
如果程序使用：
TelegramClient(...).start(bot_token=BOT_TOKEN)
那么通常不需要人工输入手机号登录。
五、机器人本身需要的 Telegram 权限
如果这个机器人负责读取/转存群组里的媒体，需要特别注意：
来源群
机器人必须能够看到需要处理的消息。
如果是群组/超级群，通常需要把机器人加入进去。
目标群
如果机器人要把媒体组发送到目标群：
机器人必须加入目标群
并且需要有发送媒体的权限。
如果目标是频道，还需要把机器人加入频道并赋予相应管理员权限。
六、目标群 ID
超级群通常类似：
-1001234567890
频道也通常是：
-100xxxxxxxxxx
代码里可能类似：
TARGET_CHAT_ID = -1001234567890
不要把 @用户名 和数字 ID 混着用。
七、Telegram BotFather 基础设置
创建机器人：
@BotFather
常用：
/newbot
修改名称：
/setname
修改用户名：
/setusername
设置简介：
/setdescription
设置头像：
/setuserpic
查看 Token：
/token



🤖 机器人基础操作
查看运行状态
systemctl status tg-media-bot --no-pager -l
启动机器人
systemctl start tg-media-bot
停止机器人
systemctl stop tg-media-bot
重启机器人
systemctl restart tg-media-bot
查看是否正在运行
systemctl is-active tg-media-bot
设置开机自动启动
systemctl enable tg-media-bot
取消开机自动启动
systemctl disable tg-media-bot
📋 日志
实时日志
journalctl -u tg-media-bot -f
最近 50 条
journalctl -u tg-media-bot -n 50 --no-pager
最近 100 条
journalctl -u tg-media-bot -n 100 --no-pager
查看今天的日志
journalctl -u tg-media-bot --since today --no-pager
实时日志退出
Ctrl + C
🔍 查看当前运行的程序
查看 systemd 实际运行哪个文件
systemctl show tg-media-bot -p ExecStart --value
应该是：
/root/tg_media_bot/venv/bin/python -u /root/tg_media_bot/telegram_media_bot.py
查看实际 Python 进程
ps aux | grep '[t]elegram_media_bot.py'
🛠️ 修改程序后
先检查语法：
/root/tg_media_bot/venv/bin/python -m py_compile /root/tg_media_bot/telegram_media_bot.py
没输出 = 语法正常。
然后重启：
systemctl restart tg-media-bot
马上看日志：
journalctl -u tg-media-bot -n 100 --no-pager
🔄 systemd 配置修改后
如果改的是：
/etc/systemd/system/tg-media-bot.service
执行：
systemctl daemon-reload
然后：
systemctl restart tg-media-bot
🚨 出问题时最常用的一条
systemctl status tg-media-bot --no-pager -l; journalctl -u tg-media-bot -n 100 --no-pager
你以后换 VPS，基本就记这 8 个：
systemctl start tg-media-bot
systemctl stop tg-media-bot
systemctl restart tg-media-bot
systemctl status tg-media-bot --no-pager -l
systemctl is-active tg-media-bot
journalctl -u tg-media-bot -f
journalctl -u tg-media-bot -n 100 --no-pager
systemctl enable tg-media-bot
