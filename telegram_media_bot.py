import asyncio
import fcntl
from dataclasses import dataclass, field
import json
import logging
import os
import re
import shutil
import secrets
import subprocess
import sys
import time
import uuid
from contextvars import ContextVar
from pathlib import Path
from types import SimpleNamespace

from telethon import TelegramClient, events, functions, types, utils
from telethon.sessions import MemorySession
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.errors.rpcerrorlist import FileReferenceExpiredError

# ============================================================
# 配置
# ============================================================
API_ID = 12345678
API_HASH = "PUT_YOUR_API_HASH_HERE"
BOT_TOKEN = "PUT_YOUR_BOT_TOKEN_HERE"

# >>> 修改开始：普通用户登录为可选项，默认关闭
ENABLE_USER_LOGIN = False
USER_SESSION_STRING = ""
# <<< 修改结束

# 只允许这些 Telegram User ID 使用
ALLOWED_USERS = {
    123456789,
}

# 默认目标群；用户也可以用 /target 修改自己的目标群
DEFAULT_TARGET_CHAT_ID = -1001234567890

# 缓存总上限
MAX_CACHE_GB = 30
SPLIT_SIZE_MB = 1900
ALBUM_DIAGNOSTIC_MODE = True
GROUP_PROBE_RADIUS = 30

# 【核心优化】：并行任务数设为 1，保护硬盘 I/O；开启 2 个物理 TCP 连接池
MAX_CONCURRENT_TASKS = 1  
BOT_POOL_SIZE = 2

# 下载配置 (动态并发控制在单连接安全范围内)
DOWNLOAD_CONNECTIONS_MIN = 4
DOWNLOAD_CONNECTIONS_MAX = 8
DOWNLOAD_CONNECTIONS_STEP = 2
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
DOWNLOAD_RANGE_PER_CONNECTION = 32 * 1024 * 1024
DOWNLOAD_PARALLEL_THRESHOLD_MB = 100
DOWNLOAD_SPEED_UP_RATIO = 1.05
DOWNLOAD_SPEED_DOWN_RATIO = 0.85

# 上传配置 (配合双 TCP 连接池，逻辑并发控制在 8 路)
UPLOAD_CONNECTIONS_MIN = 4
UPLOAD_CONNECTIONS_MAX = 8
UPLOAD_CONNECTIONS_STEP = 2
UPLOAD_PART_SIZE = 512 * 1024
UPLOAD_PARALLEL_THRESHOLD_MB = 10
UPLOAD_PART_RETRIES = 3
UPLOAD_RANGE_PER_CONNECTION = 32 * 1024 * 1024
UPLOAD_SPEED_UP_RATIO = 1.05
UPLOAD_SPEED_DOWN_RATIO = 0.85

# 【修复】：使用绝对路径锚定，防止系统服务启动导致相对路径漂移
BASE_DIR = Path(__file__).parent.resolve()
CACHE_DIR = BASE_DIR / "cache"
CONFIG_FILE = BASE_DIR / "bot_users.json"
LOG_FILE = BASE_DIR / "bot.log"
LOCK_FILE = BASE_DIR / "bot.lock"
TASKS_DIR = BASE_DIR / "tasks"

CACHE_UNUSED_TASKS = 3
CACHE_MANIFEST = "cache_manifest.json"
CACHE_RESERVED = set()
CACHE_IN_USE = set()
CACHE_STATE_LOCK = asyncio.Lock()

# ============================================================
# 【修复】：同步 I/O 后台线程执行器 (彻底防止阻塞事件循环导致分片丢失)
# ============================================================
def sync_read_file(path, offset, expected):
    with open(path, "rb") as fh:
        fh.seek(offset)
        return fh.read(expected)

def sync_write_append(path, data):
    with open(path, "ab") as fp:
        fp.write(data)

# ============================================================
# 日志 / 单实例锁
# ============================================================
lock_handle = None

def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    root.addHandler(fh)
    root.addHandler(sh)

def acquire_single_instance():
    global lock_handle
    lock_handle = open(LOCK_FILE, "w", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.error("已有一个 telegram_media_bot.py 正在运行。")
        raise SystemExit(1)
    lock_handle.write(str(os.getpid()))
    lock_handle.flush()

# ============================================================
# Telegram 客户端池初始化
# ============================================================
# 【核心优化】：创建多个物理 TCP 连接池
bot_clients = []
for _ in range(BOT_POOL_SIZE):
    bot_clients.append(TelegramClient(MemorySession(), API_ID, API_HASH))

# 主客户端，用于常规指令交互和媒体组探测
client = bot_clients[0]

user_client = None
if ENABLE_USER_LOGIN and USER_SESSION_STRING.strip():
    user_client = TelegramClient(StringSession(USER_SESSION_STRING.strip()), API_ID, API_HASH)

def get_source_client():
    if ENABLE_USER_LOGIN:
        if user_client is None:
            raise RuntimeError("已启用用户登录，但 USER_SESSION_STRING 为空。")
        return user_client
    return client

# ============================================================
# 用户配置与持久化任务队列
# ============================================================
USERS = {}

class RetryableTransferError(Exception): pass

@dataclass
class TransferJob:
    job_id: str
    user_id: int
    link: str
    event: object
    created_at: float = field(default_factory=time.time)
    status: str = "排队中"
    task: asyncio.Task | None = None
    status_message: object = None
    cache_dir: str | None = None
    cancelled: bool = False

JOB_QUEUE = asyncio.Queue()
JOBS = {}
USER_JOBS = {}
QUEUE_WORKERS = []

TASKS_FILE = TASKS_DIR / "queue.json"
STATUS_STATE_FILE = TASKS_DIR / "status.json"

GLOBAL_STATUS_MESSAGE = None
GLOBAL_STATUS_CHAT_ID = None
GLOBAL_STATUS_MESSAGE_ID = None
GLOBAL_LAST_USER_MESSAGE_ID = None

BATCH_TOTAL = 0
BATCH_COMPLETED = 0
BATCH_FAILED = 0
BATCH_CANCELLED = 0
FAILED_TASKS = []
JOB_STATUS_TEXTS = {}
CURRENT_JOB_ID = ContextVar("current_job_id", default=None)
PERSISTENCE_LOCK = asyncio.Lock()

def ensure_task_dirs():
    TASKS_DIR.mkdir(parents=True, exist_ok=True)

def atomic_write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        try: tmp.unlink(missing_ok=True)
        except Exception: pass

def serialize_job(job):
    return {
        "job_id": job.job_id,
        "user_id": int(job.user_id),
        "link": job.link,
        "created_at": float(job.created_at),
        "status": job.status,
        "cache_dir": job.cache_dir,
        "cancelled": bool(job.cancelled),
    }

def save_persistent_queue():
    jobs = []
    for job in JOBS.values():
        if job.status in ("排队中", "运行中") and not job.cancelled:
            jobs.append(serialize_job(job))
    data = {
        "version": 1,
        "jobs": jobs,
        "batch": {"total": BATCH_TOTAL, "completed": BATCH_COMPLETED, "failed": BATCH_FAILED, "cancelled": BATCH_CANCELLED},
        "failed_tasks": FAILED_TASKS,
        "status": {"chat_id": GLOBAL_STATUS_CHAT_ID, "message_id": GLOBAL_STATUS_MESSAGE_ID, "last_user_message_id": GLOBAL_LAST_USER_MESSAGE_ID},
    }
    atomic_write_json(TASKS_FILE, data)

def load_persistent_queue():
    global BATCH_TOTAL, BATCH_COMPLETED, BATCH_FAILED, BATCH_CANCELLED
    global FAILED_TASKS, GLOBAL_STATUS_CHAT_ID, GLOBAL_STATUS_MESSAGE_ID, GLOBAL_LAST_USER_MESSAGE_ID

    if not TASKS_FILE.exists(): return []
    try: data = json.loads(TASKS_FILE.read_text(encoding="utf-8"))
    except Exception: return []

    batch = data.get("batch") or {}
    BATCH_TOTAL = int(batch.get("total", 0) or 0)
    BATCH_COMPLETED = int(batch.get("completed", 0) or 0)
    BATCH_FAILED = int(batch.get("failed", 0) or 0)
    BATCH_CANCELLED = int(batch.get("cancelled", 0) or 0)
    FAILED_TASKS = list(data.get("failed_tasks") or [])

    status = data.get("status") or {}
    GLOBAL_STATUS_CHAT_ID = status.get("chat_id")
    GLOBAL_STATUS_MESSAGE_ID = status.get("message_id")
    GLOBAL_LAST_USER_MESSAGE_ID = status.get("last_user_message_id")

    restored, seen_job_ids = [], set()
    for item in data.get("jobs", []):
        try:
            status = item.get("status", "排队中")
            if status not in ("排队中", "运行中"): continue
            job_id = str(item["job_id"])
            if not job_id or job_id in seen_job_ids: continue
            link = str(item["link"]).strip()
            if not parse_message_link(link): continue
            seen_job_ids.add(job_id)
            job = TransferJob(
                job_id=job_id, user_id=int(item["user_id"]), link=link,
                event=SimpleNamespace(sender_id=int(item["user_id"])),
                created_at=float(item.get("created_at", time.time())),
                status="排队中", cache_dir=item.get("cache_dir"), cancelled=False,
            )
            restored.append(job)
        except Exception: pass
    return restored

def rebuild_job_indexes(jobs):
    JOBS.clear(); USER_JOBS.clear(); JOB_STATUS_TEXTS.clear()
    for job in jobs:
        JOBS[job.job_id] = job
        USER_JOBS.setdefault(job.user_id, []).append(job.job_id)

def active_job_count():
    return sum(1 for job in JOBS.values() if job.status in ("运行中", "排队中") and not job.cancelled)

def queue_status_text():
    running = sum(1 for job in JOBS.values() if job.status == "运行中")
    queued = sum(1 for job in JOBS.values() if job.status == "排队中" and not job.cancelled)

    lines = [
        "📋 转存任务",
        "━━━━━━━━━━━━",
        f"总任务：{BATCH_TOTAL}",
        f"完成：{BATCH_COMPLETED}",
        f"运行中：{running}",
        f"排队：{queued}",
    ]
    if BATCH_FAILED: lines.append(f"失败：{BATCH_FAILED}")
    if BATCH_CANCELLED: lines.append(f"取消：{BATCH_CANCELLED}")

    running_jobs = [job for job in JOBS.values() if job.status == "运行中"]
    for job in running_jobs[:MAX_CONCURRENT_TASKS]:
        detail = JOB_STATUS_TEXTS.get(job.job_id)
        # 【新增】：注入快捷取消命令
        cancel_cmd = f"/cancel_{job.job_id[:8]}"
        if detail:
            detail = detail.strip()
            if len(detail) > 500: detail = detail[:497] + "..."
            lines.extend(["", f"▶️ [{job.job_id[:8]}] {cancel_cmd}", detail])
        else:
            link = job.link.strip().replace("\n", " ")
            if len(link) > 120: link = link[:117] + "..."
            lines.extend(["", f"▶️ [{job.job_id[:8]}] {cancel_cmd} {link}"])

    if queued: lines.extend(["", f"⏳ 还有 {queued} 个任务排队中"])
    return "\n".join(lines)

def ensure_dirs():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ensure_task_dirs()
    if not CONFIG_FILE.exists(): CONFIG_FILE.write_text("{}", encoding="utf-8")

def load_users():
    try: return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception: return {}

def save_users(data):
    tmp = CONFIG_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CONFIG_FILE)

def is_allowed(user_id):
    return int(user_id) in ALLOWED_USERS

def get_user(user_id):
    if not is_allowed(user_id): raise PermissionError("User is not allowed")
    key = str(user_id)
    if key not in USERS:
        USERS[key] = {"target": DEFAULT_TARGET_CHAT_ID}
        save_users(USERS)
    return USERS[key]

# ============================================================
# 缓存处理
# ============================================================
def dir_size(path):
    if not path.exists(): return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())

def cache_size_gb():
    return dir_size(CACHE_DIR) / (1024 ** 3)

def safe_remove(path):
    try:
        path = Path(path)
        if path.is_dir(): shutil.rmtree(path, ignore_errors=True)
        elif path.exists(): path.unlink()
    except OSError: pass

def _manifest_path(task_dir): return Path(task_dir) / CACHE_MANIFEST

def _read_manifest(task_dir):
    path = _manifest_path(task_dir)
    try:
        if not path.is_file(): return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception: return None

def _write_manifest(task_dir, data):
    path = _manifest_path(task_dir)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def _manifest_matches(data, peer, message_id):
    if not data or not data.get("ready"): return False
    if str(data.get("peer")) != str(peer): return False
    ids = {int(x) for x in data.get("message_ids", [])}
    return int(message_id) in ids

def _valid_cached_files(task_dir, data):
    if not data or not data.get("ready"): return []
    result = []
    for rel in data.get("output_files", []):
        path = (Path(task_dir) / rel).resolve()
        root = Path(task_dir).resolve()
        if root not in path.parents or not path.is_file() or path.stat().st_size <= 0: return []
        result.append(path)
    return result

def find_cached_task(peer, message_id):
    if not CACHE_DIR.exists(): return None
    for task_dir in sorted(CACHE_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not task_dir.is_dir(): continue
        data = _read_manifest(task_dir)
        if not _manifest_matches(data, peer, message_id): continue
        files = _valid_cached_files(task_dir, data)
        if not files: continue
        return task_dir, data, files
    return None

def cleanup_unused_caches(exclude=None):
    exclude_set = {Path(x).resolve() for x in (exclude or [])}
    if not CACHE_DIR.exists(): return 0
    removed = 0
    for p in list(CACHE_DIR.iterdir()):
        if not p.is_dir(): continue
        rp = p.resolve()
        if rp in exclude_set or rp in {Path(x).resolve() for x in CACHE_RESERVED | CACHE_IN_USE}: continue
        data = _read_manifest(p)
        if not data or not data.get("ready"): continue
        if int(data.get("miss_count", 0)) >= CACHE_UNUSED_TASKS:
            safe_remove(p)
            removed += 1
    return removed

def note_new_cache_task(peer, message_id):
    matched = None
    if not CACHE_DIR.exists(): return None
    protected = {Path(x).resolve() for x in CACHE_RESERVED | CACHE_IN_USE}
    for p in CACHE_DIR.iterdir():
        if not p.is_dir(): continue
        data = _read_manifest(p)
        if _manifest_matches(data, peer, message_id) and _valid_cached_files(p, data):
            data["miss_count"] = 0
            data["use_count"] = int(data.get("use_count", 0))
            data["last_used"] = time.time()
            _write_manifest(p, data)
            matched = p
        elif data and data.get("ready"):
            # 【修复】：防误杀机制，只惩罚未被保护的闲置缓存
            if p.resolve() not in protected:
                data["miss_count"] = int(data.get("miss_count", 0)) + 1
                _write_manifest(p, data)
    cleanup_unused_caches(exclude=[matched] if matched else None)
    return matched

def reserve_cache(task_dir):
    if task_dir: CACHE_RESERVED.add(str(Path(task_dir).resolve()))
def release_cache_reservation(task_dir):
    if task_dir: CACHE_RESERVED.discard(str(Path(task_dir).resolve()))
def mark_cache_in_use(task_dir):
    if task_dir: CACHE_IN_USE.add(str(Path(task_dir).resolve()))
def unmark_cache_in_use(task_dir):
    if task_dir: CACHE_IN_USE.discard(str(Path(task_dir).resolve()))

def cleanup_old_tasks(exclude=None):
    exclude = Path(exclude).resolve() if exclude else None
    if not CACHE_DIR.exists(): return
    for p in list(CACHE_DIR.iterdir()):
        if not p.is_dir(): continue
        if exclude and p.resolve() == exclude: continue
        safe_remove(p)

def ensure_cache_room(exclude=None):
    limit = MAX_CACHE_GB * (1024 ** 3)
    if dir_size(CACHE_DIR) <= limit: return True

    protected = {Path(x).resolve() for x in CACHE_RESERVED | CACHE_IN_USE}
    if exclude: protected.add(Path(exclude).resolve())

    candidates = []
    for p in CACHE_DIR.iterdir():
        if not p.is_dir() or p.resolve() in protected: continue
        data = _read_manifest(p)
        if data and data.get("ready"):
            candidates.append((float(data.get("last_used", 0)), p))

    for _, p in sorted(candidates):
        safe_remove(p)
        if dir_size(CACHE_DIR) <= limit: break
    return dir_size(CACHE_DIR) <= limit

# ============================================================
# Telegram 链接
# ============================================================
TG_LINK_RE = re.compile(
    r"(?:https?://)?t\.me/(?:c/)?([^/\s?]+)/(\d+)(?:\?[^\s]*)?",
    re.IGNORECASE,
)

def parse_message_link(text):
    m = TG_LINK_RE.search(text.strip())
    if not m: return None
    raw_peer, message_id = m.group(1), int(m.group(2))
    if raw_peer.isdigit(): peer = int("-100" + raw_peer)
    else: peer = raw_peer.lstrip("@")
    return peer, message_id

def parse_comment_id(link):
    match = re.search(r"[?&]comment=(\d+)", link, re.IGNORECASE)
    return int(match.group(1)) if match else None

# ============================================================
# 提取消息
# ============================================================
async def get_linked_message(peer, message_id, source_client=None):
    source_client = source_client or get_source_client()
    message = await source_client.get_messages(peer, ids=message_id)
    if isinstance(message, list): return message[0] if message else None
    return message

async def get_comment_message(channel_peer, post_id, comment_id):
    if not ENABLE_USER_LOGIN or user_client is None:
        raise RuntimeError("评论链接需要先启用普通用户登录。")
    discussion = await user_client(functions.messages.GetDiscussionMessageRequest(peer=channel_peer, msg_id=post_id))
    source_entity = await user_client.get_entity(channel_peer)
    source_peer_id = utils.get_peer_id(source_entity)
    candidates = []
    for chat in getattr(discussion, "chats", []) or []:
        if not isinstance(chat, (types.Channel, types.Chat)): continue
        try: chat_peer_id = utils.get_peer_id(chat)
        except Exception: continue
        if chat_peer_id != source_peer_id: candidates.append(chat)

    discussion_chat = next(
        (chat for chat in candidates if isinstance(chat, types.Channel) and getattr(chat, "megagroup", False)),
        candidates[0] if candidates else None,
    )
    if discussion_chat is None: return None, None
    discussion_peer = utils.get_peer_id(discussion_chat)
    comment_message = await get_linked_message(discussion_peer, comment_id, source_client=user_client)
    return discussion_peer, comment_message

async def get_album_messages(peer, linked_message, source_client=None):
    if not linked_message: return []
    source_client = source_client or getattr(linked_message, "_client", None) or get_source_client()
    if not linked_message.grouped_id: return [linked_message]

    start_id = linked_message.id
    begin = max(1, start_id - GROUP_PROBE_RADIUS)
    end = start_id + GROUP_PROBE_RADIUS
    candidate_ids = list(range(begin, end + 1))

    messages = await source_client.get_messages(peer, ids=candidate_ids)
    if not isinstance(messages, list): messages = [messages] if messages else []

    grouped_id = linked_message.grouped_id
    members = [m for m in messages if m and m.grouped_id == grouped_id and m.media]
    if linked_message.media and linked_message.id not in {m.id for m in members}:
        members.append(linked_message)
    members.sort(key=lambda m: m.id)
    return members

# ============================================================
# 进度显示
# ============================================================
STATUS_LOCKS = {}
STATUS_EDIT_MIN_INTERVAL = 5.0
STATUS_EDIT_NEXT_ALLOWED = {}
STATUS_EDIT_FLOOD_UNTIL = {}
STATUS_EDIT_LAST_TEXT = {}
STATUS_EDIT_RETRY_TASKS = {}

def get_status_lock(message_id):
    lock = STATUS_LOCKS.get(message_id)
    if lock is None:
        lock = asyncio.Lock()
        STATUS_LOCKS[message_id] = lock
    return lock

async def _scheduled_status_edit(message, message_id):
    try:
        while True:
            next_allowed = STATUS_EDIT_NEXT_ALLOWED.get(message_id, 0.0)
            delay = max(0.0, next_allowed - time.monotonic())
            if delay > 0: await asyncio.sleep(delay)

            lock = get_status_lock(message_id)
            async with lock:
                next_allowed = STATUS_EDIT_NEXT_ALLOWED.get(message_id, 0.0)
                if next_allowed - time.monotonic() > 0: continue
                latest_text = STATUS_EDIT_LAST_TEXT.get(message_id)
                if not latest_text: return
                try:
                    await message.edit(latest_text)
                    STATUS_EDIT_NEXT_ALLOWED[message_id] = time.monotonic() + STATUS_EDIT_MIN_INTERVAL
                    STATUS_EDIT_FLOOD_UNTIL.pop(message_id, None)
                    return
                except Exception as e:
                    if type(e).__name__ == "FloodWaitError":
                        # 【修复】：严格遵守 Telegram 限流下发的等待时间
                        wait_time = getattr(e, 'seconds', 5)
                        STATUS_EDIT_NEXT_ALLOWED[message_id] = time.monotonic() + wait_time + 1
                        continue
                    return
    except asyncio.CancelledError: raise
    except Exception: pass
    finally:
        current = asyncio.current_task()
        if STATUS_EDIT_RETRY_TASKS.get(message_id) is current:
            STATUS_EDIT_RETRY_TASKS.pop(message_id, None)

async def edit_status(message, text):
    global GLOBAL_STATUS_MESSAGE
    if GLOBAL_STATUS_MESSAGE is not None: message = GLOBAL_STATUS_MESSAGE
    if not message: return
    message_id = getattr(message, "id", None)
    if message_id is None: return

    job_id = CURRENT_JOB_ID.get()
    if job_id: JOB_STATUS_TEXTS[job_id] = text

    rendered = queue_status_text() if GLOBAL_STATUS_MESSAGE is not None else text
    STATUS_EDIT_LAST_TEXT[message_id] = rendered

    lock = get_status_lock(message_id)
    async with lock:
        now = time.monotonic()
        next_allowed = STATUS_EDIT_NEXT_ALLOWED.get(message_id, 0.0)
        if now < next_allowed:
            if message_id not in STATUS_EDIT_RETRY_TASKS:
                task = asyncio.create_task(_scheduled_status_edit(message, message_id))
                STATUS_EDIT_RETRY_TASKS[message_id] = task
            return
        try:
            await message.edit(rendered)
            STATUS_EDIT_NEXT_ALLOWED[message_id] = time.monotonic() + STATUS_EDIT_MIN_INTERVAL
        except Exception as e:
            if type(e).__name__ == "FloodWaitError":
                wait_time = getattr(e, 'seconds', 5)
                STATUS_EDIT_NEXT_ALLOWED[message_id] = time.monotonic() + wait_time + 1
                if message_id not in STATUS_EDIT_RETRY_TASKS:
                    task = asyncio.create_task(_scheduled_status_edit(message, message_id))
                    STATUS_EDIT_RETRY_TASKS[message_id] = task

def release_status_lock(message_id):
    STATUS_LOCKS.pop(message_id, None)

async def cleanup_status_message_state(message_id):
    if not message_id: return
    retry = STATUS_EDIT_RETRY_TASKS.pop(message_id, None)
    if retry and retry is not asyncio.current_task() and not retry.done(): retry.cancel()
    STATUS_EDIT_FLOOD_UNTIL.pop(message_id, None)
    STATUS_EDIT_NEXT_ALLOWED.pop(message_id, None)
    STATUS_EDIT_LAST_TEXT.pop(message_id, None)
    STATUS_LOCKS.pop(message_id, None)

async def create_or_move_global_status(event=None, force_new=False):
    global GLOBAL_STATUS_MESSAGE, GLOBAL_STATUS_CHAT_ID, GLOBAL_STATUS_MESSAGE_ID
    global GLOBAL_LAST_USER_MESSAGE_ID

    if event is not None:
        GLOBAL_LAST_USER_MESSAGE_ID = getattr(event, "id", None)
        GLOBAL_STATUS_CHAT_ID = int(event.chat_id or event.sender_id)

    old = GLOBAL_STATUS_MESSAGE
    if old is not None and force_new:
        old_id = getattr(old, "id", None)
        try: await old.delete()
        except Exception: pass
        await cleanup_status_message_state(old_id)
        GLOBAL_STATUS_MESSAGE = None
        GLOBAL_STATUS_MESSAGE_ID = None

    if GLOBAL_STATUS_MESSAGE is None and GLOBAL_STATUS_CHAT_ID and GLOBAL_STATUS_MESSAGE_ID:
        try:
            GLOBAL_STATUS_MESSAGE = await client.get_messages(GLOBAL_STATUS_CHAT_ID, ids=int(GLOBAL_STATUS_MESSAGE_ID))
            if isinstance(GLOBAL_STATUS_MESSAGE, list):
                GLOBAL_STATUS_MESSAGE = GLOBAL_STATUS_MESSAGE[0] if GLOBAL_STATUS_MESSAGE else None
        except Exception:
            GLOBAL_STATUS_MESSAGE = None

    if GLOBAL_STATUS_MESSAGE is None and GLOBAL_STATUS_CHAT_ID:
        reply_to = GLOBAL_LAST_USER_MESSAGE_ID if GLOBAL_LAST_USER_MESSAGE_ID else None
        GLOBAL_STATUS_MESSAGE = await client.send_message(GLOBAL_STATUS_CHAT_ID, queue_status_text(), reply_to=reply_to)
        GLOBAL_STATUS_MESSAGE_ID = GLOBAL_STATUS_MESSAGE.id
        STATUS_EDIT_NEXT_ALLOWED[GLOBAL_STATUS_MESSAGE.id] = time.monotonic() + STATUS_EDIT_MIN_INTERVAL

    save_persistent_queue()
    return GLOBAL_STATUS_MESSAGE

async def refresh_global_status():
    if GLOBAL_STATUS_MESSAGE is None: return
    await edit_status(GLOBAL_STATUS_MESSAGE, queue_status_text())
    save_persistent_queue()

def format_bytes(value):
    value = float(value or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if value < 1024 or unit == units[-1]: return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"

def progress_bar(percent, width=12):
    percent = max(0.0, min(100.0, float(percent)))
    filled = int(width * percent / 100)
    return "█" * filled + "░" * (width - filled)

def progress_text(prefix, current, total):
    if not total: return f"{prefix}：{format_bytes(current)}"
    percent = max(0.0, min(100.0, current * 100 / total))
    return f"{prefix}：{progress_bar(percent)} {percent:5.1f}% ({format_bytes(current)}/{format_bytes(total)})"

# ============================================================
# 文件处理与切片
# ============================================================
def file_size(path):
    try: return Path(path).stat().st_size
    except OSError: return 0

def is_video(path):
    return Path(path).suffix.lower() in {".mp4", ".m4v", ".mov", ".mkv", ".webm", ".avi", ".ts"}

def is_telegram_video(message):
    if getattr(message, "video", False): return True
    media_file = getattr(message, "file", None)
    mime_type = getattr(media_file, "mime_type", None)
    if isinstance(mime_type, str) and mime_type.lower().startswith("video/"): return True
    document = getattr(message, "document", None)
    for attribute in getattr(document, "attributes", None) or []:
        if isinstance(attribute, types.DocumentAttributeVideo): return True
    return False

def ensure_video_extension(message, path):
    path = Path(path)
    if not is_telegram_video(message) or path.suffix.lower() not in ("", ".bin"): return path
    new_path = path.with_suffix(".mp4")
    if new_path.exists(): new_path.unlink()
    path.rename(new_path)
    return new_path

async def ensure_video_extension_ffprobe(message, path):
    path = Path(path)
    if not path.is_file() or not is_telegram_video(message) or path.suffix.lower() not in ("", ".bin"): return path
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_type",
        "-of", "default=noprint_wrappers=1:nokey=1", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0 or stdout.decode("utf-8", errors="ignore").strip() != "video": return path
    new_path = path.with_suffix(".mp4")
    if new_path.exists(): new_path.unlink()
    path.rename(new_path)
    return new_path

async def split_video_ffmpeg(path, output_dir, status_message=None, file_index=1, file_total=1):
    path = Path(path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if file_size(path) <= SPLIT_SIZE_MB * 1024 * 1024: return [path]

    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration,bit_rate", "-of", "default=noprint_wrappers=1", str(path)],
        capture_output=True, text=True, check=True,
    )

    duration = bitrate = None
    for line in probe.stdout.splitlines():
        if line.startswith("duration="):
            try: duration = float(line.split("=", 1)[1])
            except ValueError: pass
        elif line.startswith("bit_rate="):
            try: bitrate = float(line.split("=", 1)[1])
            except ValueError: pass

    if not duration or not bitrate or bitrate <= 0:
        raise RuntimeError(f"无法获取 {path.name} 的 duration/bit_rate。")

    target_bytes = SPLIT_SIZE_MB * 1024 * 1024 * 0.97
    segment_seconds = max(10.0, target_bytes * 8 / bitrate)
    pattern = output_dir / f"{path.stem}.part%03d.mp4"

    if status_message:
        await edit_status(status_message, f"✂️ 正在切片 {file_index}/{file_total}\n\n🎬 {path.name}\n{progress_bar(0)} 0.0%\n模式：无重新编码（stream copy）")

    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-progress", "pipe:1", "-nostats", "-i", str(path),
        "-map", "0", "-c", "copy", "-f", "segment", "-segment_time", f"{segment_seconds:.3f}", "-reset_timestamps", "1",
        "-segment_format", "mp4", "-movflags", "+faststart", str(pattern),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    last_update = 0.0
    stderr_task = asyncio.create_task(proc.stderr.read())

    try:
        while True:
            raw = await proc.stdout.readline()
            if not raw: break
            line = raw.decode("utf-8", errors="replace").strip()
            if not line.startswith("out_time_ms="): continue
            try: out_time = int(line.split("=", 1)[1]) / 1_000_000.0
            except ValueError: continue

            percent = max(0.0, min(100.0, out_time * 100.0 / duration))
            now = time.monotonic()
            if status_message and (now - last_update >= 1.0 or percent >= 100.0):
                last_update = now
                try: await edit_status(status_message, f"✂️ 正在切片 {file_index}/{file_total}\n\n🎬 {path.name}\n{progress_bar(percent)} {percent:5.1f}%\n模式：无重新编码（stream copy）")
                except Exception: pass

        return_code = await proc.wait()
        stderr_bytes = await stderr_task
        stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()

        if return_code != 0:
            raise RuntimeError(f"FFmpeg 切片失败（退出码 {return_code}）：\n{stderr_text[-1500:] if stderr_text else '无错误输出'}")

    except asyncio.CancelledError:
        if proc.returncode is None: proc.kill(); await proc.wait()
        if not stderr_task.done(): stderr_task.cancel()
        raise

    parts = sorted(output_dir.glob(f"{path.stem}.part*.mp4"))
    if not parts: raise RuntimeError(f"FFmpeg 没有生成 {path.name} 的分片。")
    for part in parts:
        if not part.is_file() or part.stat().st_size <= 0: raise RuntimeError(f"FFmpeg 生成了无效分片：{part.name}")
    path.unlink(missing_ok=True)
    if status_message:
        try: await edit_status(status_message, f"✂️ 切片完成 {file_index}/{file_total}\n生成：{len(parts)} 个分片")
        except Exception: pass
    return parts

def get_display_name(message, fallback=None):
    media_file = getattr(message, "file", None)
    name = getattr(media_file, "name", None) if media_file else None
    if name: return Path(str(name)).name
    document = getattr(message, "document", None)
    if document:
        for attr in getattr(document, "attributes", None) or []:
            name = getattr(attr, "file_name", None)
            if name: return Path(str(name)).name
    caption = getattr(message, "message", None) or getattr(message, "text", None)
    if caption:
        caption = " ".join(str(caption).split())
        return caption[:77] + "..." if len(caption) > 80 else caption
    return fallback or f"媒体_{getattr(message, 'id', 'unknown')}"

# ============================================================
# 下载核心逻辑
# ============================================================
async def download_message_parallel(message, task_dir, status_message=None, index=1, total_files=1):
    if not message.media: return None
    source_client = getattr(message, "_client", None) or get_source_client()
    current_media = message.media
    media_file = getattr(message, "file", None)
    total_size = getattr(media_file, "size", None)

    if not total_size or total_size < DOWNLOAD_PARALLEL_THRESHOLD_MB * 1024 * 1024:
        started = time.monotonic()
        last_update = [0.0]

        async def progress(current, total):
            if not status_message or not total: return
            now = time.monotonic()
            if current < total and now - last_update[0] < 1.0: return
            elapsed = max(now - started, 0.001)
            speed = current / elapsed
            percent = max(0.0, min(100.0, current * 100 / total))
            remaining = max(total - current, 0)
            eta = remaining / speed if speed > 0 else 0
            eta_text = f"{int(eta // 60)}分{int(eta % 60)}秒" if eta >= 60 else f"{int(eta)}秒"
            icon = "🎬" if getattr(message, "video", False) else "🖼"
            last_update[0] = now
            try: await edit_status(status_message, f"📥 下载 {index}/{total_files}\n\n{icon} {get_display_name(message)}\n{progress_bar(percent)} {percent:5.1f}%\n{format_bytes(current)} / {format_bytes(total)}\n速度：{format_bytes(speed)}/s\n预计剩余：{eta_text}")
            except Exception: pass

        path = await message.download_media(file=str(task_dir), progress_callback=progress)
        return await ensure_video_extension_ffprobe(message, ensure_video_extension(message, path)) if path else None

    chunk_size = DOWNLOAD_CHUNK_SIZE
    output_name = getattr(media_file, "name", None) or f"media_{getattr(message, 'id', index)}"
    if is_telegram_video(message) and Path(output_name).suffix.lower() in ("", ".bin"):
        output_name = f"{Path(output_name).stem}.mp4"
    output_path = Path(task_dir) / output_name
    parts_dir = Path(task_dir) / f".parts_{getattr(message, 'id', index)}"
    parts_dir.mkdir(parents=True, exist_ok=True)

    connections = DOWNLOAD_CONNECTIONS_MIN
    best_speed = last_adjust_speed = downloaded = 0.0
    last_update = 0.0
    started = time.monotonic()
    part_no = offset = 0
    progress_lock = asyncio.Lock()

    async def update_status(force=False):
        nonlocal last_update
        if not status_message: return
        now = time.monotonic()
        if not force and downloaded < total_size and now - last_update < 1.0: return
        elapsed = max(now - started, 0.001)
        speed = downloaded / elapsed
        percent = downloaded * 100 / total_size
        remaining = max(total_size - downloaded, 0)
        eta = remaining / speed if speed > 0 else 0
        eta_text = f"{int(eta // 60)}分{int(eta % 60)}秒" if eta >= 60 else f"{int(eta)}秒"
        last_update = now
        try: await edit_status(status_message, f"📥 下载 {index}/{total_files}\n\n🎬 {Path(output_name).name}\n{progress_bar(percent)} {percent:5.1f}%\n{format_bytes(downloaded)} / {format_bytes(total_size)}\n速度：{format_bytes(speed)}/s\n预计剩余：{eta_text}")
        except Exception: pass

    await update_status(force=True)

    async def download_range(part_no, start_offset, end_offset, batch_received):
        nonlocal downloaded
        part_path = parts_dir / f"part_{part_no:06d}"
        expected = end_offset - start_offset
        received = 0

        try:
            if part_path.exists():
                part_path.unlink()
            async for data in source_client.iter_download(current_media, offset=start_offset, limit=expected, request_size=chunk_size, chunk_size=chunk_size):
                if not data: continue
                remaining = expected - received
                if remaining <= 0: break
                if len(data) > remaining: data = data[:remaining]

                # 【修复】：下放至异步线程执行写入，防止阻塞导致并发断流
                await asyncio.to_thread(sync_write_append, part_path, data)
                n = len(data)
                received += n
                await asyncio.sleep(0.05)

                async with progress_lock:
                    downloaded += n
                    batch_received[0] += n
                    await update_status()

                if received >= expected: break

            if received != expected: raise IOError(f"分块长度错误: part={part_no} expected={expected} received={received}")
            return part_path
        except asyncio.CancelledError: raise
        except Exception:
            part_path.unlink(missing_ok=True)
            raise

    all_parts = []
    try:
        while offset < total_size:
            batch_start = time.monotonic()
            batch_received = [0]
            batch_parts = []
            per_connection = max(chunk_size, DOWNLOAD_RANGE_PER_CONNECTION)
            per_connection = ((per_connection + chunk_size - 1) // chunk_size) * chunk_size
            ranges = []
            batch_offset = offset
            batch_part_no = part_no

            for _ in range(connections):
                if batch_offset >= total_size: break
                end_offset = min(batch_offset + per_connection, total_size)
                ranges.append((batch_part_no, batch_offset, end_offset))
                batch_part_no += 1
                batch_offset = end_offset
            if not ranges: break

            batch_success = False
            batch_attempt = 0
            reference_refreshes = 0

            class _ReferenceRefreshFailed(RuntimeError): pass

            while not batch_success:
                batch_attempt += 1
                try:
                    batch_results = await asyncio.gather(*(download_range(part, start, end, batch_received) for part, start, end in ranges), return_exceptions=True)
                    errors = [item for item in batch_results if isinstance(item, BaseException)]
                    if errors:
                        expired_error = next((item for item in errors if isinstance(item, FileReferenceExpiredError)), None)
                        if expired_error is not None:
                            async with progress_lock: downloaded = max(0, downloaded - batch_received[0])
                            batch_received[0] = 0
                            for part, _, _ in ranges: (parts_dir / f"part_{part:06d}").unlink(missing_ok=True)
                            reference_refreshes += 1
                            if reference_refreshes > 2: raise _ReferenceRefreshFailed(f"刷新文件引用 2 次仍然失效：{output_name}") from expired_error
                            peer = getattr(message, "input_chat", None) or getattr(message, "chat_id", None)
                            try: fresh_message = await source_client.get_messages(peer, ids=message.id)
                            except Exception as refresh_error: raise _ReferenceRefreshFailed(f"重新获取源消息失败：{output_name}") from refresh_error
                            if not fresh_message or not fresh_message.media: raise _ReferenceRefreshFailed(f"无法重新获取源消息或媒体：{output_name}") from expired_error
                            current_media = fresh_message.media
                            message.media = current_media
                            await asyncio.sleep(1)
                            continue
                        raise errors[0]
                    batch_parts = batch_results
                    batch_success = True
                except asyncio.CancelledError: raise
                except _ReferenceRefreshFailed: raise
                except Exception:
                    async with progress_lock: downloaded = max(0, downloaded - batch_received[0])
                    batch_received[0] = 0
                    for part, _, _ in ranges: (parts_dir / f"part_{part:06d}").unlink(missing_ok=True)
                    connections = max(DOWNLOAD_CONNECTIONS_MIN, connections - DOWNLOAD_CONNECTIONS_STEP)
                    await asyncio.sleep(min(batch_attempt, 3))
                    if batch_attempt >= 5: raise RuntimeError(f"分块下载连续失败 {batch_attempt} 次")

            all_parts.extend(batch_parts)
            offset = batch_offset
            part_no = batch_part_no

            batch_elapsed = max(time.monotonic() - batch_start, 0.001)
            batch_bytes = batch_received[0]
            batch_speed = batch_bytes / batch_elapsed

            connections_changed = False
            if batch_speed > 0:
                if best_speed <= 0:
                    best_speed = batch_speed
                    last_adjust_speed = batch_speed
                else:
                    reference_speed = last_adjust_speed or best_speed or batch_speed
                    if batch_speed >= reference_speed * DOWNLOAD_SPEED_UP_RATIO:
                        if connections < DOWNLOAD_CONNECTIONS_MAX:
                            connections = min(DOWNLOAD_CONNECTIONS_MAX, connections + DOWNLOAD_CONNECTIONS_STEP)
                            connections_changed = True
                    elif batch_speed <= reference_speed * DOWNLOAD_SPEED_DOWN_RATIO:
                        if connections > DOWNLOAD_CONNECTIONS_MIN:
                            connections = max(DOWNLOAD_CONNECTIONS_MIN, connections - DOWNLOAD_CONNECTIONS_STEP)
                            connections_changed = True
                    last_adjust_speed = batch_speed
                    best_speed = max(best_speed, batch_speed)
            await update_status(force=connections_changed)

        all_parts.sort(key=lambda x: int(x.stem.split("_")[-1]))
        with open(output_path, "wb") as out:
            for part_path in all_parts:
                with open(part_path, "rb") as fp:
                    while True:
                        data = fp.read(1024 * 1024)
                        if not data: break
                        out.write(data)

        if output_path.stat().st_size != total_size: raise IOError(f"合并后文件大小错误: expected={total_size} actual={output_path.stat().st_size}")
        for part_path in all_parts: part_path.unlink(missing_ok=True)
        try: parts_dir.rmdir()
        except OSError: pass

        return await ensure_video_extension_ffprobe(message, ensure_video_extension(message, output_path))

    except asyncio.CancelledError: raise
    finally:
        if parts_dir.exists():
            for part_path in parts_dir.glob("part_*"): part_path.unlink(missing_ok=True)
            try: parts_dir.rmdir()
            except OSError: pass

async def download_message(message, task_dir, status_message=None, index=1, total_files=1):
    return await download_message_parallel(message, task_dir, status_message=status_message, index=index, total_files=total_files)

# ============================================================
# 上传核心逻辑 (使用多 TCP 连接池)
# ============================================================
async def _parallel_big_upload(file_path, status_message=None, file_index=1, file_total=1):
    file_path = Path(file_path)
    total_size = file_size(file_path)

    if total_size <= 0: raise RuntimeError(f"文件大小无效: {file_path}")

    part_size = UPLOAD_PART_SIZE
    total_parts = (total_size + part_size - 1) // part_size
    file_id = secrets.randbits(63)

    connections = UPLOAD_CONNECTIONS_MIN
    reference_speed = None
    offset_part = 0
    uploaded_bytes = 0
    started = time.monotonic()
    last_progress = [0.0]

    progress_lock = asyncio.Lock()

    async def update_progress(force=False):
        if not status_message: return
        now = time.monotonic()
        if not force and now - last_progress[0] < 1.0: return
        async with progress_lock:
            now = time.monotonic()
            if not force and now - last_progress[0] < 1.0: return
            current = min(uploaded_bytes, total_size)
            elapsed = max(now - started, 0.001)
            speed = current / elapsed
            percent = max(0.0, min(100.0, current * 100 / total_size))
            remaining = max(total_size - current, 0)
            eta = remaining / speed if speed > 0 else 0
            eta_text = f"{int(eta // 60)}分{int(eta % 60)}秒" if eta >= 60 else f"{int(eta)}秒"
            icon = "🎬" if is_video(file_path) else "🖼"

            await edit_status(
                status_message,
                f"📤 上传媒体：{file_index}/{file_total}\n\n"
                f"{icon} {file_path.name}\n"
                f"{progress_bar(percent)} {percent:5.1f}%\n"
                f"{format_bytes(current)} / {format_bytes(total_size)}\n"
                f"速度：{format_bytes(speed)}/s\n"
                f"预计剩余：{eta_text}\n"
                f"并行：{connections} 路（多 TCP）"
            )
            last_progress[0] = now

    await update_progress(force=True)

    async def upload_part(part_no):
        nonlocal uploaded_bytes
        offset = part_no * part_size
        expected = min(part_size, total_size - offset)

        # 【修复】：通过后台线程读取大块数据，避免卡死主进程导致 Timeout
        data = await asyncio.to_thread(sync_read_file, file_path, offset, expected)

        if len(data) != expected:
            raise IOError(f"上传分块读取长度错误: part={part_no} expected={expected} received={len(data)}")

        for attempt in range(1, UPLOAD_PART_RETRIES + 1):
            try:
                # 【核心优化】：按 part_no 将任务分配给池子中的不同物理 TCP Socket 连接
                pool_client = bot_clients[part_no % BOT_POOL_SIZE]
                
                result = await pool_client(
                    functions.upload.SaveBigFilePartRequest(
                        file_id=file_id,
                        file_part=part_no,
                        file_total_parts=total_parts,
                        bytes=data,
                    )
                )

                if not result: raise RuntimeError(f"Telegram 拒绝上传分块: part={part_no}")
                return len(data)

            except FloodWaitError as e:
                if attempt >= UPLOAD_PART_RETRIES: raise
                await asyncio.sleep(e.seconds + 1)
            except asyncio.CancelledError: raise
            except Exception:
                if attempt >= UPLOAD_PART_RETRIES: raise
                await asyncio.sleep(min(2 ** (attempt - 1), 10))

        raise RuntimeError(f"上传分块最终失败: part={part_no}")

    while offset_part < total_parts:
        batch_start_part = offset_part
        batch_count = min(connections, total_parts - offset_part)
        parts_per_connection = max(1, UPLOAD_RANGE_PER_CONNECTION // part_size)
        batch_count = min(connections * parts_per_connection, total_parts - offset_part)
        batch_parts = list(range(offset_part, offset_part + batch_count))
        batch_started = time.monotonic()

        queue = asyncio.Queue()
        for part_no in batch_parts: queue.put_nowait(part_no)

        async def worker():
            nonlocal uploaded_bytes
            while True:
                try: part_no = queue.get_nowait()
                except asyncio.QueueEmpty: return
                try:
                    part_bytes = await upload_part(part_no)
                    async with progress_lock: uploaded_bytes += part_bytes
                    await asyncio.sleep(0.05)
                finally: queue.task_done()
                await update_progress()

        workers = [asyncio.create_task(worker()) for _ in range(min(connections, batch_count))]

        try: await asyncio.gather(*workers)
        except asyncio.CancelledError:
            for task in workers: task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        except Exception:
            for task in workers:
                if not task.done(): task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

        batch_elapsed = max(time.monotonic() - batch_started, 0.001)
        batch_bytes = sum(min(part_size, total_size - p * part_size) for p in batch_parts)
        batch_speed = batch_bytes / batch_elapsed
        offset_part += batch_count

        if reference_speed is None: reference_speed = batch_speed
        else:
            if batch_speed >= reference_speed * UPLOAD_SPEED_UP_RATIO:
                new_connections = min(UPLOAD_CONNECTIONS_MAX, connections + UPLOAD_CONNECTIONS_STEP)
                if new_connections != connections: connections = new_connections
                reference_speed = batch_speed
            elif batch_speed <= reference_speed * UPLOAD_SPEED_DOWN_RATIO:
                new_connections = max(UPLOAD_CONNECTIONS_MIN, connections - UPLOAD_CONNECTIONS_STEP)
                if new_connections != connections: connections = new_connections
                reference_speed = batch_speed
        await update_progress(force=True)

    if uploaded_bytes != total_size: raise RuntimeError(f"上传总长度错误: expected={total_size} uploaded={uploaded_bytes}")
    uploaded_bytes = total_size
    await update_progress(force=True)
    return types.InputFileBig(id=file_id, parts=total_parts, name=file_path.name)

async def upload_file_with_retry(file_path, status_message=None, file_index=1, file_total=1, max_retries=3):
    file_path = Path(file_path)
    total_size = file_size(file_path)

    if total_size >= UPLOAD_PARALLEL_THRESHOLD_MB * 1024 * 1024:
        for attempt in range(1, max_retries + 1):
            try:
                return await _parallel_big_upload(file_path, status_message=status_message, file_index=file_index, file_total=file_total)
            except asyncio.CancelledError: raise
            except FloodWaitError as e:
                if attempt >= max_retries: break
                await asyncio.sleep(e.seconds + 1)
            except Exception:
                if attempt >= max_retries: break
                await asyncio.sleep(min(2 ** (attempt - 1), 30))
        try: await edit_status(status_message, f"❌ 文件上传失败\n\n{file_path.name}\n连续失败 {max_retries} 次\n任务结束，不再继续发送媒体组")
        except Exception: pass
        return None

    for attempt in range(1, max_retries + 1):
        started = time.monotonic()
        last_update = [0.0]

        async def progress(current, total):
            if not status_message or not total: return
            now = time.monotonic()
            if current < total and now - last_update[0] < 1.0: return
            elapsed = max(now - started, 0.001)
            speed = current / elapsed
            percent = max(0.0, min(100.0, current * 100 / total))
            remaining = max(total - current, 0)
            eta = remaining / speed if speed > 0 else 0
            eta_text = f"{int(eta // 60)}分{int(eta % 60)}秒" if eta >= 60 else f"{int(eta)}秒"
            icon = "🎬" if is_video(file_path) else "🖼"
            retry_text = f"\n重试：第 {attempt}/{max_retries} 次" if attempt > 1 else ""
            status_text = f"📤 准备媒体组：{file_index}/{file_total}\n\n{icon} {file_path.name}\n{progress_bar(percent)} {percent:5.1f}%\n{format_bytes(current)} / {format_bytes(total)}\n速度：{format_bytes(speed)}/s\n预计剩余：{eta_text}\n并行：单路（小文件）{retry_text}"
            asyncio.create_task(edit_status(status_message, status_text))
            last_update[0] = now

        try:
            return await client.upload_file(str(file_path), file_size=total_size, progress_callback=progress)
        except FloodWaitError as e:
            if attempt >= max_retries: break
            try: await edit_status(status_message, f"⏳ Telegram 限流\n{file_path.name}\n等待 {e.seconds} 秒后重试 ({attempt + 1}/{max_retries})")
            except Exception: pass
            await asyncio.sleep(e.seconds + 2)
        except asyncio.CancelledError: raise
        except Exception as e:
            if attempt >= max_retries:
                try: await edit_status(status_message, f"❌ 文件上传失败\n\n{file_path.name}\n连续失败 {max_retries} 次\n任务结束，不再继续发送媒体组")
                except Exception: pass
                return None
            wait_seconds = min(2 ** (attempt - 1), 30)
            try: await edit_status(status_message, f"⚠️ 上传连接中断\n\n{file_path.name}\n{type(e).__name__}\n{wait_seconds} 秒后重试 ({attempt + 1}/{max_retries})")
            except Exception: pass
            await asyncio.sleep(wait_seconds)
    return None

async def _get_video_metadata(file_path):
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height:format=duration",
            "-of", "json", str(file_path), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode != 0: return 0, 1, 1
        data=json.loads(stdout.decode("utf-8", errors="ignore"))
        stream=(data.get("streams") or [{}])[0]
        fmt=data.get("format") or {}
        return float(fmt.get("duration") or 0), int(stream.get("width") or 1), int(stream.get("height") or 1)
    except Exception: return 0, 1, 1

async def _generate_video_thumbnail(file_path):
    file_path = Path(file_path)
    thumb_path = file_path.with_name(f"{file_path.stem}.thumb.jpg")
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", str(file_path),
            "-frames:v", "1", "-vf", "scale=320:320:force_original_aspect_ratio=decrease", "-q:v", "3", str(thumb_path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0 or not thumb_path.exists() or thumb_path.stat().st_size == 0: return None
        return thumb_path
    except Exception: return None

async def send_album_from_handles(handles, files, target, caption=None, status_message=None):
    if len(handles) != len(files): raise RuntimeError("媒体组上传句柄数量不一致")
    if not handles: raise RuntimeError("媒体组没有可发送的媒体")
    entity = await client.get_input_entity(target)
    media = []

    for handle, file_path in zip(handles, files):
        path = Path(file_path)
        suffix = path.suffix.lower()
        if suffix in (".jpg", ".jpeg", ".png", ".webp"):
            uploaded = types.InputMediaUploadedPhoto(file=handle)
            uploaded_result = await client(functions.messages.UploadMediaRequest(peer=entity, media=uploaded))
            fm = utils.get_input_media(uploaded_result.photo)
        elif suffix in (".mp4", ".m4v", ".mov", ".webm"):
            duration, width, height = await _get_video_metadata(path)
            attributes = [
                types.DocumentAttributeFilename(file_name=path.name),
                types.DocumentAttributeVideo(duration=duration, w=width, h=height, supports_streaming=True),
            ]
            thumb_path = await _generate_video_thumbnail(path)
            thumb_handle = None
            try:
                if thumb_path: thumb_handle = await client.upload_file(str(thumb_path))
                uploaded = types.InputMediaUploadedDocument(file=handle, thumb=thumb_handle, mime_type="video/mp4", attributes=attributes, force_file=False, nosound_video=True)
                uploaded_result = await client(functions.messages.UploadMediaRequest(peer=entity, media=uploaded))
            finally:
                if thumb_path:
                    try: thumb_path.unlink(missing_ok=True)
                    except Exception: pass
            fm = utils.get_input_media(uploaded_result.document, supports_streaming=True)
        else:
            uploaded = types.InputMediaUploadedDocument(file=handle, mime_type="application/octet-stream", attributes=[types.DocumentAttributeFilename(file_name=path.name)], force_file=True)
            uploaded_result = await client(functions.messages.UploadMediaRequest(peer=entity, media=uploaded))
            fm = utils.get_input_media(uploaded_result.document)
        media.append(fm)

    for chunk_start in range(0, len(media), 10):
        chunk_media = media[chunk_start:chunk_start + 10]
        multi_media = []
        for index, media_item in enumerate(chunk_media):
            msg = caption if caption and index == 0 else ""
            multi_media.append(types.InputSingleMedia(media=media_item, random_id=secrets.randbits(63), message=msg))
        try: await client(functions.messages.SendMultiMediaRequest(peer=entity, multi_media=multi_media))
        except Exception as album_error:
            if ALBUM_DIAGNOSTIC_MODE:
                single_errors = []
                for index, media_item in enumerate(chunk_media):
                    file_name = Path(files[chunk_start + index]).name
                    try: await client(functions.messages.SendMediaRequest(peer=entity, media=media_item, message=(caption if caption and index == 0 else "")))
                    except Exception as single_error: single_errors.append((file_name, single_error))
                if single_errors: raise album_error
                continue
            raise

    if status_message: asyncio.create_task(edit_status(status_message, f"📦 媒体组发送完成\n{len(files)} 个媒体"))

async def upload_media_group(files, target, caption=None, status_message=None):
    handles = []
    total = len(files)
    for index, file_path in enumerate(files, 1):
        handle = await upload_file_with_retry(file_path, status_message=status_message, file_index=index, file_total=total, max_retries=3)
        if handle is None: raise RuntimeError(f"上传失败：{Path(file_path).name}")
        handles.append(handle)

    if status_message:
        try: await edit_status(status_message, f"📦 {total} 个媒体已上传到 Telegram\n正在组装为一个媒体组...")
        except Exception: pass

    await send_album_from_handles(handles, files, target, caption=caption, status_message=status_message)
    return True

# ============================================================
# 媒体组处理
# ============================================================
async def process_album(messages, target, task_dir, status_message=None, source_peer=None):
    task_dir = Path(task_dir)
    task_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    messages = sorted(messages, key=lambda m: m.id)

    for message in messages:
        path = await download_message(message, task_dir, status_message=status_message, index=len(downloaded) + 1, total_files=len(messages))
        if path: downloaded.append((message, path))

    if not downloaded: return 0

    caption = None
    for message, _ in downloaded:
        if text := getattr(message, "message", None):
            caption = text
            break

    output_files = []
    for index, (_, path) in enumerate(downloaded):
        if file_size(path) <= SPLIT_SIZE_MB * 1024 * 1024:
            output_files.append(path)
            continue
        if not is_video(path): raise RuntimeError(f"{path.name} 超过 {SPLIT_SIZE_MB} MB，但不是视频。")
        parts = await split_video_ffmpeg(path, task_dir / f"parts_{index}", status_message=status_message, file_index=index + 1, file_total=len(messages))
        output_files.extend(parts)

    rel_files = [str(Path(x).resolve().relative_to(task_dir.resolve())) for x in output_files]
    _write_manifest(task_dir, {
        "version": 1, "ready": True, "peer": str(source_peer), "message_ids": [int(m.id) for m in messages],
        "output_files": rel_files, "created_at": time.time(), "last_used": time.time(), "use_count": 0, "miss_count": 0,
    })

    await upload_media_group(output_files, target, caption=caption, status_message=status_message)
    return len(downloaded)

# ============================================================
# 命令
# ============================================================
@client.on(events.NewMessage(pattern=r"^/start$"))
async def start_handler(event):
    if not is_allowed(event.sender_id): return
    get_user(event.sender_id)
    await event.reply("媒体转存 Bot\n\n直接发送 Telegram 消息链接即可转存。\n\n/target - 设置你的目标群\n/status - 查看设置\n/queue - 查看队列\n/cancel - 取消当前任务\n/cancelall - 取消你的全部任务\n/clearqueue - 清空任务队列\n/clearcache - 手动清理缓存\n\n链接格式：\nhttps://t.me/channel/123")

@client.on(events.NewMessage(pattern=r"^/target(?:\s+(-100\d+))?$"))
async def target_handler(event):
    if not is_allowed(event.sender_id): return
    user = get_user(event.sender_id)
    target = event.pattern_match.group(1)
    if not target: return await event.reply("用法：\n/target -1001234567890")
    user["target"] = int(target)
    save_users(USERS)
    await event.reply(f"已设置目标群：{target}")

@client.on(events.NewMessage(pattern=r"^/status$"))
async def status_handler(event):
    if not is_allowed(event.sender_id): return
    user = get_user(event.sender_id)
    jobs = USER_JOBS.get(event.sender_id, [])
    running, queued = 0, 0
    for jid in jobs:
        job = JOBS.get(jid)
        if not job: continue
        if job.status == "运行中": running += 1
        elif job.status == "排队中": queued += 1
    await event.reply(f"目标群：{user['target']}\n缓存：{cache_size_gb():.2f}/{MAX_CACHE_GB} GB\n视频分片阈值：{SPLIT_SIZE_MB} MB\n媒体组探测范围：前后各 {GROUP_PROBE_RADIUS} 个消息 ID\n并发上限：{MAX_CONCURRENT_TASKS}\n你的任务：运行中 {running} 个，排队中 {queued} 个\n全局队列等待：{JOB_QUEUE.qsize()} 个")

@client.on(events.NewMessage(pattern=r"^/queue$"))
async def queue_handler(event):
    if not is_allowed(event.sender_id): return
    try: await create_or_move_global_status(event=event, force_new=True)
    except Exception: pass
    jobs = USER_JOBS.get(event.sender_id, [])
    lines = []
    for index, jid in enumerate(jobs, 1):
        job = JOBS.get(jid)
        if not job or job.status not in ("运行中", "排队中"): continue
        short_id = job.job_id[:8]
        link = job.link.strip()
        if len(link) > 55: link = link[:52] + "..."
        state = "▶️ 运行中" if job.status == "运行中" else "⏳ 排队中"
        lines.append(f"{index}. {state} [{short_id}]\n{link}")
    if not lines: return await event.reply("你当前没有运行中或排队中的任务。")
    await event.reply(f"📋 你的任务队列\n并发上限：{MAX_CONCURRENT_TASKS}\n\n" + "\n\n".join(lines))

@client.on(events.NewMessage(pattern=r"^/cancel$"))
async def cancel_handler(event):
    if not is_allowed(event.sender_id): return
    await event.reply("请在状态列表直接点击 /cancel_xxxxxxxx 的快捷命令以精准取消对应任务。")

# 【新增】：单个任务点对点精准取消
@client.on(events.NewMessage(pattern=r"^/cancel_([a-f0-9]{8})$"))
async def cancel_specific_handler(event):
    if not is_allowed(event.sender_id): return
    short_id = event.pattern_match.group(1)
    jobs = USER_JOBS.get(event.sender_id, [])
    for jid in jobs:
        if jid.startswith(short_id):
            job = JOBS.get(jid)
            if not job: continue
            if job.status in ("运行中", "排队中") and not job.cancelled:
                job.cancelled = True
                if job.status == "运行中":
                    if job.task and not job.task.done(): job.task.cancel()
                    await event.reply(f"🛑 已取消运行中的任务 [{short_id}]\n已完成的切片将保留以便后续复用。")
                else:
                    job.status = "已取消"
                    release_cache_reservation(job.cache_dir)
                    JOBS.pop(job.job_id, None)
                    if jid in jobs: jobs.remove(jid)
                    if not jobs: USER_JOBS.pop(event.sender_id, None)
                    global BATCH_CANCELLED
                    BATCH_CANCELLED += 1
                    await event.reply(f"🛑 已取消排队任务 [{short_id}]")
                save_persistent_queue()
                await refresh_global_status()
                return
    await event.reply(f"⚠️ 找不到可以取消的任务 [{short_id}]。它可能已经完成或被清除。")

@client.on(events.NewMessage(pattern=r"^/cancelall$"))
async def cancelall_handler(event):
    if not is_allowed(event.sender_id): return
    jobs = USER_JOBS.get(event.sender_id, [])
    cancelled_running = cancelled_queued = 0
    for jid in jobs:
        job = JOBS.get(jid)
        if not job: continue
        if job.status == "运行中":
            job.cancelled = True
            cancelled_running += 1
            if job.task and not job.task.done(): job.task.cancel()
        elif job.status == "排队中":
            job.cancelled = True
            job.status = "已取消"
            release_cache_reservation(job.cache_dir)
            cancelled_queued += 1
    total = cancelled_running + cancelled_queued
    save_persistent_queue()
    await refresh_global_status()
    if total: await event.reply(f"🛑 已取消全部任务：{total} 个\n运行中：{cancelled_running} 个\n排队中：{cancelled_queued} 个\n运行中任务取消后，已完成的缓存会保留以便后续复用。")
    else: await event.reply("你当前没有运行中或排队中的任务。")

@client.on(events.NewMessage(pattern=r"^/clearqueue$"))
async def clearqueue_handler(event):
    if not is_allowed(event.sender_id): return
    running_tasks = []
    for job in list(JOBS.values()):
        job.cancelled = True
        if job.task and not job.task.done():
            job.task.cancel()
            running_tasks.append(job.task)
        release_cache_reservation(job.cache_dir)
    if running_tasks: await asyncio.gather(*running_tasks, return_exceptions=True)
    JOBS.clear()
    USER_JOBS.clear()
    JOB_STATUS_TEXTS.clear()
    while not JOB_QUEUE.empty():
        try: JOB_QUEUE.get_nowait(); JOB_QUEUE.task_done()
        except asyncio.QueueEmpty: break
    global BATCH_TOTAL, BATCH_COMPLETED, BATCH_FAILED, BATCH_CANCELLED, FAILED_TASKS
    BATCH_TOTAL = BATCH_COMPLETED = BATCH_FAILED = BATCH_CANCELLED = 0
    FAILED_TASKS.clear()
    save_persistent_queue()
    await refresh_global_status()
    await event.reply("🧹 任务队列已全部清空。\n缓存不会删除。")

@client.on(events.NewMessage(pattern=r"^/failed$"))
async def failed_handler(event):
    if not is_allowed(event.sender_id): return
    user_id = int(event.sender_id)
    links = [item.get("link", "") for item in FAILED_TASKS if int(item.get("user_id", 0) or 0) == user_id and item.get("link")]
    if not links: return await event.reply("当前没有失败任务链接。")
    lines = [f"❌ 失败任务：{len(links)} 个", ""]
    for index, link in enumerate(links, 1): lines.append(f"{index}. {link}")
    text = "\n".join(lines)
    for start in range(0, len(text), 3800): await event.reply(text[start:start + 3800])

@client.on(events.NewMessage(pattern=r"^/clearcache$"))
async def clearcache_handler(event):
    if not is_allowed(event.sender_id): return
    active = [job for job in JOBS.values() if job.status in ("运行中", "排队中") and not job.cancelled]
    if active: return await event.reply(f"⚠️ 当前还有 {len(active)} 个任务正在运行/排队。\n请先使用 /cancelall，再执行 /clearcache。")
    before = cache_size_gb()
    cleanup_old_tasks()
    after = cache_size_gb()
    await event.reply(f"🧹 缓存清理完成。\n清理前：{before:.2f} GB\n清理后：{after:.2f} GB")

# ============================================================
# 链接转存
# ============================================================
async def run_transfer(event, link, status_message=None, cache_dir=None):
    if not is_allowed(event.sender_id): return False
    user = get_user(event.sender_id)
    parsed = parse_message_link(link)
    if not parsed: return False
    peer, message_id = parsed
    target = user["target"]

    comment_id = parse_comment_id(link)
    if comment_id is not None:
        if not ENABLE_USER_LOGIN or user_client is None:
            await edit_status(status_message, "❌ 这是评论区链接，请先启用普通用户登录并配置 USER_SESSION_STRING。")
            return False
        source_client = user_client
    else:
        source_client = get_source_client()

    if cache_dir:
        task_dir = Path(cache_dir)
        mark_cache_in_use(task_dir)
    else:
        task_dir = CACHE_DIR / f"{event.sender_id}_{int(time.time())}_{int(time.time_ns() % 1000000):06d}"
        task_dir.mkdir(parents=True, exist_ok=True)
        mark_cache_in_use(task_dir)

    try:
        if status_message is not None: await edit_status(status_message, "🔎 正在读取链接消息...")

        if comment_id is not None:
            source_peer, linked_message = await get_comment_message(peer, message_id, comment_id)
            source_message_id = comment_id
            if source_peer is None or linked_message is None:
                await edit_status(status_message, "❌ 找不到评论消息。请确认普通账号能访问关联讨论群，且评论 ID 正确。")
                return False
        else:
            source_peer = peer
            source_message_id = message_id
            linked_message = await get_linked_message(peer, message_id, source_client=source_client)

        if not linked_message:
            await edit_status(status_message, "❌ 找不到这个消息。请确认当前来源账号有权限访问源群/频道。")
            return False

        if not linked_message.media:
            await edit_status(status_message, "❌ 这个链接指向的消息没有媒体。")
            return False

        messages = await get_album_messages(source_peer, linked_message, source_client=source_client)
        if not messages:
            await edit_status(status_message, "❌ 没有找到可转存的媒体。")
            return False

        cached = find_cached_task(source_peer, source_message_id)
        if cached:
            cached_dir, cached_data, cached_files = cached
            if task_dir.resolve() != cached_dir.resolve():
                safe_remove(task_dir)
                unmark_cache_in_use(task_dir)
            task_dir = cached_dir
            mark_cache_in_use(task_dir)
            cached_data["miss_count"] = 0
            cached_data["use_count"] = int(cached_data.get("use_count", 0)) + 1
            cached_data["last_used"] = time.time()
            _write_manifest(task_dir, cached_data)
            await edit_status(status_message, f"♻️ 命中缓存：{len(cached_files)} 个文件\n开始上传...")
            count = await upload_media_group(cached_files, target, caption=next((getattr(m, "message", None) for m in messages if getattr(m, "message", None)), None), status_message=status_message)
            return len(messages) > 0

        if not ensure_cache_room(exclude=task_dir):
            await edit_status(status_message, f"❌ 缓存已达到 {MAX_CACHE_GB} GB，无法开始任务。")
            return False

        logging.info("Transfer: user=%s peer=%s message=%s album=%s count=%s target=%s", event.sender_id, source_peer, source_message_id, linked_message.grouped_id, len(messages), target)
        await edit_status(status_message, f"找到媒体：{len(messages)} 个\n开始下载...")
        count = await process_album(messages, target, task_dir, status_message=status_message, source_peer=source_peer)
        return True

    except asyncio.CancelledError: raise
    except (ConnectionError, asyncio.TimeoutError) as e:
        logging.warning("Telegram/network interruption; keeping job for retry: %s: %s", type(e).__name__, e)
        raise RetryableTransferError(str(e)) from e
    except FloodWaitError as e:
        await edit_status(status_message, f"⏳ Telegram 限流：等待 {e.seconds} 秒。")
        return False
    except Exception as e:
        logging.exception("Transfer failed")
        await edit_status(status_message, f"❌ 任务失败：{type(e).__name__}: {e}\n\n详细日志：bot.log")
        return False
    finally:
        unmark_cache_in_use(task_dir)
        release_cache_reservation(task_dir)
        try:
            manifest = _read_manifest(task_dir)
            if not manifest or not manifest.get("ready"):
                safe_remove(task_dir)
                logging.info("Removed incomplete cache: %s", task_dir)
        except Exception:
            logging.exception("Failed to clean incomplete cache: %s", task_dir)
            safe_remove(task_dir)

@client.on(events.NewMessage)
async def link_handler(event):
    global BATCH_TOTAL, BATCH_COMPLETED, BATCH_FAILED, BATCH_CANCELLED
    global GLOBAL_STATUS_MESSAGE, GLOBAL_STATUS_CHAT_ID, GLOBAL_STATUS_MESSAGE_ID, GLOBAL_LAST_USER_MESSAGE_ID

    if not is_allowed(event.sender_id): return
    if event.raw_text.startswith("/"): return

    links = []
    seen = set()
    for match in TG_LINK_RE.finditer(event.raw_text):
        link_text = match.group(0)
        parsed = parse_message_link(link_text)
        if not parsed: continue
        normalized = link_text.strip()
        if normalized not in seen:
            seen.add(normalized)
            links.append((normalized, parsed))

    if not links: return

    if active_job_count() == 0:
        BATCH_TOTAL = BATCH_COMPLETED = BATCH_FAILED = BATCH_CANCELLED = 0
        FAILED_TASKS.clear()
        JOB_STATUS_TEXTS.clear()

    GLOBAL_LAST_USER_MESSAGE_ID = event.id
    GLOBAL_STATUS_CHAT_ID = int(event.chat_id or event.sender_id)

    await create_or_move_global_status(event, force_new=True)

    for link_text, (peer, message_id) in links:
        job_id = uuid.uuid4().hex
        cached_dir = None if parse_comment_id(link_text) is not None else note_new_cache_task(peer, message_id)
        job = TransferJob(
            job_id=job_id,
            user_id=event.sender_id,
            link=link_text,
            event=event,
            cache_dir=str(cached_dir) if cached_dir else None,
        )
        if cached_dir: reserve_cache(cached_dir)

        JOBS[job_id] = job
        USER_JOBS.setdefault(event.sender_id, []).append(job_id)
        BATCH_TOTAL += 1
        await JOB_QUEUE.put(job)

    save_persistent_queue()
    await refresh_global_status()

async def transfer_worker(worker_id):
    logging.info("Queue worker %s started.", worker_id)

    while True:
        job = await JOB_QUEUE.get()
        try:
            if job.cancelled or job.status == "已取消":
                release_cache_reservation(job.cache_dir)
                JOBS.pop(job.job_id, None)
                save_persistent_queue()
                continue

            job.status = "运行中"
            save_persistent_queue()
            await refresh_global_status()

            logging.info("Queue worker %s starting job=%s user=%s running=%s/%s", worker_id, job.job_id, job.user_id, sum(1 for item in JOBS.values() if item.status == "运行中"), MAX_CONCURRENT_TASKS)

            job.status_message = GLOBAL_STATUS_MESSAGE
            token = CURRENT_JOB_ID.set(job.job_id)
            try:
                job.task = asyncio.create_task(run_transfer(job.event, job.link, GLOBAL_STATUS_MESSAGE, job.cache_dir))
            finally:
                CURRENT_JOB_ID.reset(token)

            result = False
            interrupted = False
            retryable = False
            try:
                result = await job.task
            except RetryableTransferError as e:
                retryable = True
                job.status = "排队中"
                logging.warning("Job kept for retry after Telegram/network interruption: %s (%s)", job.job_id, e)
            except asyncio.CancelledError:
                interrupted = True
                logging.warning("Job interrupted by bot shutdown/restart; preserving: %s", job.job_id)
            except Exception:
                logging.exception("Unhandled job exception: %s", job.job_id)

            global BATCH_COMPLETED, BATCH_FAILED, BATCH_CANCELLED
            if job.cancelled:
                job.status = "已取消"
                BATCH_CANCELLED += 1
            elif interrupted:
                job.status = "运行中"
            elif retryable:
                job.status = "排队中"
            elif result is True:
                job.status = "已完成"
                BATCH_COMPLETED += 1
            else:
                job.status = "失败"
                BATCH_FAILED += 1
                FAILED_TASKS.append({"job_id": job.job_id, "user_id": int(job.user_id), "link": job.link})
                logging.error("Job failed: %s", job.job_id)

        finally:
            job.task = None
            JOB_QUEUE.task_done()
            release_cache_reservation(job.cache_dir)

            if job.status in ("已完成", "已取消", "失败"):
                JOB_STATUS_TEXTS.pop(job.job_id, None)
                JOBS.pop(job.job_id, None)
                user_list = USER_JOBS.get(job.user_id, [])
                if job.job_id in user_list: user_list.remove(job.job_id)
                if not user_list: USER_JOBS.pop(job.user_id, None)

            save_persistent_queue()
            await refresh_global_status()

            logging.info("Queue worker %s finished job=%s status=%s", worker_id, job.job_id, job.status)

            if job.status == "排队中" and not job.cancelled:
                await asyncio.sleep(5)
                await JOB_QUEUE.put(job)

async def register_bot_commands():
    commands = [
        types.BotCommand("start", "开始使用"),
        types.BotCommand("target", "设置转存目标"),
        types.BotCommand("status", "查看状态"),
        types.BotCommand("queue", "查看任务队列"),
        types.BotCommand("cancel", "取消一个任务"),
        types.BotCommand("cancelall", "取消全部任务"),
        types.BotCommand("clearqueue", "清空任务队列"),
        types.BotCommand("failed", "查看失败任务链接"),
        types.BotCommand("clearcache", "清理残留缓存"),
    ]
    try:
        await client(functions.bots.SetBotCommandsRequest(scope=types.BotCommandScopeDefault(), lang_code="", commands=commands))
        logging.info("Telegram bot command menu registered.")
    except Exception:
        logging.exception("Failed to register Telegram bot command menu.")

async def generate_user_session():
    if API_ID == 12345678 or API_HASH == "PUT_YOUR_API_HASH_HERE":
        raise RuntimeError("请先在配置区填写真实 API_ID 和 API_HASH，再运行 --login-user。")
    login_client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await login_client.start()
        me = await login_client.get_me()
        session_string = login_client.session.save()
        print("\n登录成功。以下是 USER_SESSION_STRING，请妥善保管：\n")
        print(session_string)
        print("\n账号：@%s（ID：%s）" % (me.username or "无用户名", me.id))
        print("将字符串填入配置区 USER_SESSION_STRING，并设置 ENABLE_USER_LOGIN = True。")
    finally:
        await login_client.disconnect()

async def main():
    setup_logging()
    acquire_single_instance()
    ensure_dirs()
    USERS.update(load_users())

    logging.info("Starting Telegram media transfer bot...")
    logging.info("Cache limit: %s GB", MAX_CACHE_GB)
    logging.info("Video split size: %s MB", SPLIT_SIZE_MB)
    logging.info("Album probe radius: %s", GROUP_PROBE_RADIUS)
    logging.info("Max concurrent tasks: %s", MAX_CONCURRENT_TASKS)

    # 【核心优化】：批量启动多物理 TCP 连接池
    logging.info(f"Starting bot with {BOT_POOL_SIZE} physical TCP connections...")
    for c in bot_clients:
        await c.start(bot_token=BOT_TOKEN)


    if ENABLE_USER_LOGIN:
        if user_client is None: raise RuntimeError("已启用用户登录，但 USER_SESSION_STRING 为空。")
        await user_client.connect()
        if not await user_client.is_user_authorized():
            await user_client.disconnect()
            raise RuntimeError("USER_SESSION_STRING 未授权或已失效，请重新生成 Session String。")
        user_me = await user_client.get_me()
        logging.info("Optional user session enabled: user_id=%s username=%s", user_me.id, user_me.username or "(no username)")
    else:
        logging.info("Optional user session disabled; using bot client for source reads.")

    await register_bot_commands()
    restored_jobs = load_persistent_queue()
    rebuild_job_indexes(restored_jobs)

    if restored_jobs:
        for job in restored_jobs: await JOB_QUEUE.put(job)
        try:
            await create_or_move_global_status(force_new=False)
            await refresh_global_status()
        except Exception:
            logging.exception("Failed to restore global status message.")

    logging.info("Restored persistent jobs: %s", len(restored_jobs))
    logging.info("Starting FIFO queue workers: %s", MAX_CONCURRENT_TASKS)

    for worker_id in range(1, MAX_CONCURRENT_TASKS + 1):
        QUEUE_WORKERS.append(asyncio.create_task(transfer_worker(worker_id)))

    logging.info("Waiting for Telegram message links...")

    try:
        await client.run_until_disconnected()
    finally:
        logging.info("Stopping queue workers...")
        for worker in QUEUE_WORKERS: worker.cancel()
        await asyncio.gather(*QUEUE_WORKERS, return_exceptions=True)
        QUEUE_WORKERS.clear()
        
        if user_client is not None and user_client.is_connected():
            await user_client.disconnect()
            
        # 【核心优化】：进程结束时优雅断开物理连接池中的所有 Socket
        for c in bot_clients:
            if c.is_connected():
                await c.disconnect()

if __name__ == "__main__":
    try:
        if "--login-user" in sys.argv:
            asyncio.run(generate_user_session())
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")
