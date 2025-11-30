import os
import asyncio
import requests
import yt_dlp
import sqlite3
from datetime import datetime
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    FSInputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.enums import ChatAction

# ============ إعدادات البوت ============

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN غير موجود في متغيرات البيئة! تأكد من إضافته في Replit أو السيرفر.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

VIDEO_EXTS = (".mp4", ".webm", ".mov", ".mkv")

# منصات محظورة ثابتة
BLOCKED_DOMAINS_BASE = [
    "netflix.com",
    "shahid.net",
    "shahed4u",
    "osn.com",
    "disneyplus.com",
    "amazon.com",
    "hbomax.com",
]

# مجموعات تعبّأ من قاعدة البيانات
EXTRA_BLOCKED_DOMAINS: set[str] = set()
BANNED_USERS: set[int] = set()

# عدّل هذا لِـ User ID تبعك في تيليجرام
ADMIN_IDS = {123456789}

# حالات المستخدمين (لاختيار النوع والجودة)
USER_STATE: dict[int, dict] = {}

# إعدادات yt-dlp
ydl_opts = {
    "format": "best[height<=720][filesize<50M]/best[height<=480]/best[height<=360]",
    "quiet": True,
    "no_warnings": True,
    "socket_timeout": 30,
    "retries": 5,
    "fragment_retries": 5,
    "extract_flat": False,
    "noplaylist": True,
}

# ============ إعدادات قاعدة البيانات ============

DB_FILE = "bot.db"


def get_conn():
    return sqlite3.connect(DB_FILE)


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")

    # جدول المستخدمين
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TEXT,
            last_seen_at TEXT,
            total_requests INTEGER DEFAULT 0
        );
    """)

    # جدول الطلبات
    c.execute("""
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            url TEXT,
            domain TEXT,
            action_type TEXT,
            quality TEXT,
            status TEXT,
            error TEXT,
            created_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
        );
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_requests_domain ON requests(domain);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status);")
    c.execute("CREATE INDEX IF NOT EXISTS idx_requests_user ON requests(user_id);")

    # جدول المستخدمين المحظورين
    c.execute("""
        CREATE TABLE IF NOT EXISTS banned_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE,
            reason TEXT,
            banned_at TEXT
        );
    """)

    # جدول الفيديوهات
    c.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            url TEXT UNIQUE,
            domain TEXT,
            first_seen_at TEXT,
            last_used_at TEXT,
            times_used INTEGER DEFAULT 0
        );
    """)

    # جدول المواقع المحظورة الإضافية
    c.execute("""
        CREATE TABLE IF NOT EXISTS blocked_domains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE,
            reason TEXT,
            added_at TEXT
        );
    """)

    conn.commit()
    conn.close()


def load_banned_users():
    global BANNED_USERS
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT telegram_id FROM banned_users;")
    rows = c.fetchall()
    conn.close()
    BANNED_USERS = {r[0] for r in rows if r[0] is not None}


def load_blocked_domains():
    global EXTRA_BLOCKED_DOMAINS
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT domain FROM blocked_domains;")
    rows = c.fetchall()
    conn.close()
    EXTRA_BLOCKED_DOMAINS = {r[0].lower() for r in rows if r[0]}


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def get_or_create_user(tg_user) -> int:
    """
    يرجع id الداخلي من جدول users
    """
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT id, total_requests FROM users WHERE telegram_id = ?;", (tg_user.id,))
    row = c.fetchone()
    now = datetime.utcnow().isoformat()

    if row:
        user_id, total_requests = row
        c.execute(
            "UPDATE users SET username=?, first_name=?, last_name=?, last_seen_at=?, total_requests=? WHERE id=?;",
            (
                tg_user.username,
                tg_user.first_name,
                tg_user.last_name,
                now,
                (total_requests or 0) + 1,
                user_id,
            ),
        )
    else:
        c.execute(
            """
            INSERT INTO users (telegram_id, username, first_name, last_name, created_at, last_seen_at, total_requests)
            VALUES (?, ?, ?, ?, ?, ?, 1);
            """,
            (
                tg_user.id,
                tg_user.username,
                tg_user.first_name,
                tg_user.last_name,
                now,
                now,
            ),
        )
        user_id = c.lastrowid

    conn.commit()
    conn.close()
    return user_id


def log_request_db(
    user_id: int | None,
    url: str,
    domain: str,
    action_type: str,
    quality: str,
    status: str,
    error: str | None = None,
):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO requests (user_id, url, domain, action_type, quality, status, error, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            user_id,
            url,
            domain,
            action_type,
            quality,
            status,
            error or "",
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def log_video_usage(title: str, url: str, domain: str):
    conn = get_conn()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    c.execute("SELECT id, times_used FROM videos WHERE url = ?;", (url,))
    row = c.fetchone()
    if row:
        vid_id, times_used = row
        c.execute(
            "UPDATE videos SET title=?, domain=?, last_used_at=?, times_used=? WHERE id=?;",
            (
                title,
                domain,
                now,
                (times_used or 0) + 1,
                vid_id,
            ),
        )
    else:
        c.execute(
            """
            INSERT INTO videos (title, url, domain, first_seen_at, last_used_at, times_used)
            VALUES (?, ?, ?, ?, ?, 1);
            """,
            (title, url, domain, now, now),
        )
    conn.commit()
    conn.close()


def ban_user_in_db(telegram_id: int, reason: str | None = None):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO banned_users (telegram_id, reason, banned_at)
        VALUES (?, ?, ?);
        """,
        (telegram_id, reason or "", datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()
    BANNED_USERS.add(telegram_id)


def unban_user_in_db(telegram_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM banned_users WHERE telegram_id = ?;", (telegram_id,))
    conn.commit()
    conn.close()
    BANNED_USERS.discard(telegram_id)


def add_blocked_domain_in_db(domain: str, reason: str | None = None):
    domain = domain.lower()
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO blocked_domains (domain, reason, added_at)
        VALUES (?, ?, ?);
        """,
        (domain, reason or "", datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()
    EXTRA_BLOCKED_DOMAINS.add(domain)


def remove_blocked_domain_in_db(domain: str):
    domain = domain.lower()
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM blocked_domains WHERE domain = ?;", (domain,))
    conn.commit()
    conn.close()
    EXTRA_BLOCKED_DOMAINS.discard(domain)


# ================== HELPERs للفيديو ==================


def looks_like_direct_video(url: str) -> bool:
    base = url.split("?", 1)[0].lower()
    return base.endswith(VIDEO_EXTS)


def is_blocked_domain(url: str) -> bool:
    try:
        hostname = (urlparse(url).hostname or "").lower()
        all_blocked = list(BLOCKED_DOMAINS_BASE) + list(EXTRA_BLOCKED_DOMAINS)
        return any(b in hostname for b in all_blocked)
    except Exception:
        return False


def get_video_info(url: str) -> dict:
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

            formats_raw = info.get("formats", []) or []
            qualities = []
            seen_heights = set()
            for f in formats_raw:
                h = f.get("height")
                fid = f.get("format_id")
                if not h or not fid:
                    continue
                if h in seen_heights:
                    continue
                seen_heights.add(h)
                qualities.append(
                    {
                        "format_id": fid,
                        "height": h,
                        "ext": f.get("ext", "mp4"),
                        "filesize": f.get("filesize"),
                    }
                )

            qualities.sort(key=lambda x: x["height"], reverse=True)

            return {
                "success": True,
                "title": info.get("title", "فيديو"),
                "duration": info.get("duration", 0),
                "uploader": info.get("uploader", "غير معروف"),
                "view_count": info.get("view_count", 0),
                "thumbnail": info.get("thumbnail", ""),
                "url": info.get("url"),
                "ext": info.get("ext", "mp4"),
                "filesize": info.get("filesize"),
                "webpage_url": info.get("webpage_url", url),
                "qualities": qualities,
            }
    except Exception as e:
        print(f"Video extract error: {e}")
        return {"success": False, "error": str(e)}


def get_direct_video_url(url: str) -> dict:
    if looks_like_direct_video(url):
        return {
            "success": True,
            "type": "direct",
            "url": url,
            "title": "فيديو مباشر",
            "duration": 0,
            "uploader": "غير معروف",
            "ext": url.split("?")[0].split(".")[-1],
            "qualities": [],
            "webpage_url": url,
        }

    info = get_video_info(url)
    if info.get("success") and info.get("url"):
        try:
            hostname = (urlparse(info.get("webpage_url", url)).hostname or "").lower()
            parts = hostname.split(".")
            platform = "link"
            if len(parts) >= 2:
                platform = parts[-2]
        except Exception:
            platform = "link"

        info["type"] = platform
        return info

    return {
        "success": False,
        "error": "تعذر استخراج رابط الفيديو من هذا الرابط.",
    }


def download_with_ytdlp(url: str, save_path: str, format_id: str | None = None) -> dict:
    try:
        opts = ydl_opts.copy()
        if format_id:
            opts["format"] = format_id
        opts["outtmpl"] = save_path.replace(".mp4", ".%(ext)s")

        print(f"[yt-dlp] بدء التحميل من: {url} | format={opts.get('format')}")
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])

        base = save_path.replace(".mp4", "")
        for ext in ["mp4", "webm", "mkv", "mov"]:
            possible = f"{base}.{ext}"
            if os.path.exists(possible):
                size = os.path.getsize(possible)
                print(f"[yt-dlp] تم العثور على الملف: {possible} (الحجم: {size} bytes)")
                if size > 0:
                    if possible != save_path:
                        os.rename(possible, save_path)
                    return {"success": True, "file_path": save_path, "file_size": size}

        return {"success": False, "error": "لم يتم إنشاء الملف بعد التحميل"}
    except Exception as e:
        print(f"download_with_ytdlp error: {e}")
        return {"success": False, "error": str(e)}


def download_audio_with_ytdlp(url: str, save_path: str) -> dict:
    try:
        opts = ydl_opts.copy()
        opts["format"] = "bestaudio[filesize<50M]/bestaudio"
        opts["outtmpl"] = save_path.replace(".mp3", ".%(ext)s")

        print(f"[yt-dlp] بدء تحميل الصوت فقط من: {url}")
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])

        base = save_path.replace(".mp3", "")
        for ext in ["mp3", "m4a", "webm", "opus"]:
            possible = f"{base}.{ext}"
            if os.path.exists(possible):
                size = os.path.getsize(possible)
                print(f"[yt-dlp] تم العثور على ملف الصوت: {possible} (الحجم: {size} bytes)")
                if size > 0:
                    if possible != save_path:
                        os.rename(possible, save_path)
                    return {"success": True, "file_path": save_path, "file_size": size}

        return {"success": False, "error": "لم يتم إنشاء ملف الصوت بعد التحميل"}
    except Exception as e:
        print(f"download_audio_with_ytdlp error: {e}")
        return {"success": False, "error": str(e)}


def download_video_fallback(direct_url: str, save_path: str) -> dict:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        print(f"[fallback] محاولة التحميل المباشر من: {direct_url}")

        with requests.get(direct_url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(save_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        size = os.path.getsize(save_path)
        print(f"[fallback] تم التحميل: {size} bytes")
        if size > 0:
            return {"success": True, "file_path": save_path, "file_size": size}
        return {"success": False, "error": "الملف الملتقط فارغ"}
    except Exception as e:
        print(f"download_video_fallback error: {e}")
        return {"success": False, "error": str(e)}


async def send_video_direct(message: Message, direct_url: str, caption: str, duration: int | None):
    try:
        await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
        await message.answer_video(
            video=direct_url,
            caption=caption,
            duration=duration or None,
            supports_streaming=True,
        )
        return {"success": True}
    except Exception as e:
        print(f"send_video_direct error: {e}")
        return {"success": False, "error": str(e)}


# ================== أوامر الإدارة (حظر / مواقع / تقارير) ==================


@router.message(Command("banuser"))
async def cmd_ban_user(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("استخدم الأمر بهذا الشكل:\n/banuser <user_id>")
        return

    try:
        uid = int(parts[1].strip())
        ban_user_in_db(uid, reason="manual ban")
        await message.answer(f"✅ تم حظر المستخدم ID={uid}")
    except ValueError:
        await message.answer("❌ ID غير صالح.")


@router.message(Command("unbanuser"))
async def cmd_unban_user(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("استخدم الأمر بهذا الشكل:\n/unbanuser <user_id>")
        return

    try:
        uid = int(parts[1].strip())
        unban_user_in_db(uid)
        await message.answer(f"✅ تم فك الحظر عن المستخدم ID={uid}")
    except ValueError:
        await message.answer("❌ ID غير صالح.")


@router.message(Command("banurl"))
async def cmd_ban_domain(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("استخدم الأمر بهذا الشكل:\n/banurl example.com")
        return

    domain = parts[1].strip().lower()
    add_blocked_domain_in_db(domain, reason="manual block")
    await message.answer(f"✅ تم إضافة الدومين إلى القائمة المحظورة:\n{domain}")


@router.message(Command("unbanurl"))
async def cmd_unban_domain(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("استخدم الأمر بهذا الشكل:\n/unbanurl example.com")
        return

    domain = parts[1].strip().lower()
    remove_blocked_domain_in_db(domain)
    await message.answer(f"✅ تم إزالة الدومين من القائمة المحظورة (إن وجد):\n{domain}")


@router.message(Command("banlist"))
async def cmd_ban_list(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT telegram_id, reason, banned_at FROM banned_users;")
    users_rows = c.fetchall()
    c.execute("SELECT domain, reason, added_at FROM blocked_domains;")
    dom_rows = c.fetchall()
    conn.close()

    if users_rows:
        users_text = "\n".join(
            f"👤 {r[0]} | سبب: {r[1] or '-'} | وقت: {r[2] or '-'}"
            for r in users_rows
        )
    else:
        users_text = "لا يوجد مستخدمون محظورون."

    if dom_rows:
        dom_text = "\n".join(
            f"🌐 {r[0]} | سبب: {r[1] or '-'} | وقت: {r[2] or '-'}"
            for r in dom_rows
        )
    else:
        dom_text = "لا توجد دومينات محظورة إضافيًا."

    await message.answer(
        "📋 قائمة الحظر:\n\n"
        f"👥 المستخدمون المحظورون:\n{users_text}\n\n"
        f"🌐 الدومينات الإضافية المحظورة:\n{dom_text}"
    )


@router.message(Command("statsdb"))
async def cmd_stats_db(message: Message):
    """إحصائيات عامة من جدول requests و users"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    conn = get_conn()
    c = conn.cursor()

    # إجمالي الطلبات
    c.execute("SELECT COUNT(*) FROM requests;")
    total = c.fetchone()[0] or 0

    # حسب نوع الطلب
    c.execute("""
        SELECT action_type, COUNT(*)
        FROM requests
        GROUP BY action_type;
    """)
    rows_type = c.fetchall()
    by_type = {r[0] or "unknown": r[1] for r in rows_type}

    # حسب الحالة (نجاح / فشل)
    c.execute("""
        SELECT status, COUNT(*)
        FROM requests
        GROUP BY status;
    """)
    rows_status = c.fetchall()
    by_status = {r[0] or "unknown": r[1] for r in rows_status}

    # عدد المستخدمين
    c.execute("SELECT COUNT(*) FROM users;")
    users_count = c.fetchone()[0] or 0

    conn.close()

    text = (
        "📊 إحصائيات عامة من قاعدة البيانات:\n\n"
        f"👥 عدد المستخدمين المسجلين: {users_count}\n"
        f"🔢 إجمالي الطلبات: {total}\n\n"
        "🎬 حسب نوع الطلب:\n"
    )

    for t, cnt in by_type.items():
        text += f"  • {t}: {cnt}\n"

    text += "\n✅ حسب حالة الطلب:\n"
    for st, cnt in by_status.items():
        text += f"  • {st}: {cnt}\n"

    await message.answer(text)


@router.message(Command("topdomains")))
async def cmd_top_domains(message: Message):
    """أكثر الدومينات استخدامًا"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT domain, COUNT(*) AS cnt
        FROM requests
        WHERE domain IS NOT NULL AND domain <> ''
        GROUP BY domain
        ORDER BY cnt DESC
        LIMIT 10;
    """)
    rows = c.fetchall()
    conn.close()

    if not rows:
        await message.answer("ℹ️ لا توجد بيانات كافية عن الدومينات حتى الآن.")
        return

    text = "🌐 أعلى الدومينات استخدامًا:\n\n"
    for domain, cnt in rows:
        text += f"  • {domain}: {cnt} طلب\n"

    await message.answer(text)


@router.message(Command("topvideos"))
async def cmd_top_videos(message: Message):
    """أشهر الفيديوهات المطلوبة"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ هذا الأمر للأدمن فقط.")
        return

    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT title, url, domain, times_used
        FROM videos
        ORDER BY times_used DESC
        LIMIT 10;
    """)
    rows = c.fetchall()
    conn.close()

    if not rows:
        await message.answer("ℹ️ لا توجد فيديوهات مسجلة حتى الآن.")
        return

    text = "🎥 أعلى الفيديوهات استخدامًا:\n\n"
    for title, url, domain, times_used in rows:
        short_title = (title or "بدون عنوان")[:40]
        text += (
            f"• {short_title}\n"
            f"  🌐 {domain or '-'} | 🔁 مرات الاستخدام: {times_used}\n"
            f"  🔗 {url}\n\n"
        )

    await message.answer(text)


# ================== أوامر البوت الأساسية ==================


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 أهلاً بك في بوت الفيديو.\n\n"
        "أرسل رابط من أي موقع يدعم الفيديو (YouTube, TikTok, Facebook, X, Vimeo, ...)\n"
        "أو رابط فيديو مباشر (.mp4 / .webm / .mov / .mkv).\n\n"
        "سيتم تحليل الرابط، ثم سيُطلب منك:\n"
        "1️⃣ اختيار الإرسال كـ 🎬 فيديو أو 🎧 صوت.\n"
        "2️⃣ لو اخترت فيديو سيتم إظهار جودات مختلفة (إن وُجدت) لتختار منها.\n\n"
        "📌 بعض المنصات المحمية (مثل Netflix, Shahid...) محظورة.\n"
    )


@router.message(F.text)
async def handle_link(message: Message):
    # حظر المستخدم
    if message.from_user.id in BANNED_USERS:
        await message.answer("🚫 تم حظرك من استخدام هذا البوت.")
        return

    url = (message.text or "").strip()

    # تجهيز user في قاعدة البيانات
    user_db_id = get_or_create_user(message.from_user)
    domain = (urlparse(url).hostname or "").lower() if url.startswith("http") else ""

    if not url.startswith("http"):
        await message.answer("❌ الرجاء إرسال رابط صحيح يبدأ بـ http أو https.")
        log_request_db(
            user_id=user_db_id,
            url=url,
            domain=domain,
            action_type="invalid",
            quality="",
            status="fail",
            error="invalid_url",
        )
        return

    if is_blocked_domain(url):
        await message.answer(
            "⛔ هذا الموقع محمي أو غير مدعوم (مثل منصات الأفلام المدفوعة)، لا يمكن التعامل معه."
        )
        log_request_db(
            user_id=user_db_id,
            url=url,
            domain=domain,
            action_type="blocked",
            quality="",
            status="fail",
            error="blocked_domain",
        )
        return

    wait_msg = await message.answer("🔍 جاري تحليل الرابط...")

    try:
        video_info = get_direct_video_url(url)

        if not video_info.get("success"):
            await wait_msg.edit_text(f"❌ {video_info.get('error', 'تعذر التعامل مع الرابط.')}")
            log_request_db(
                user_id=user_db_id,
                url=url,
                domain=domain,
                action_type="unknown",
                quality="",
                status="fail",
                error=video_info.get("error", "extract_error"),
            )
            return

        vtype = video_info.get("type", "unknown")

        if vtype == "direct":
            platform_name = "رابط مباشر"
        elif vtype in ["link", "unknown"]:
            platform_name = "منصة غير معروفة"
        else:
            platform_name = vtype.capitalize()

        info_text = f"✅ تم العثور على فيديو من: {platform_name}\n"

        if video_info.get("title"):
            title = video_info["title"]
            if len(title) > 50:
                title = title[:50] + "..."
            info_text += f"📹 {title}\n"

        if video_info.get("uploader"):
            info_text += f"👤 {video_info['uploader']}\n"

        if video_info.get("duration"):
            minutes = video_info["duration"] // 60
            seconds = video_info["duration"] % 60
            info_text += f"⏱️ {minutes}:{seconds:02d}\n"

        USER_STATE[message.from_user.id] = {
            "url": url,
            "video_info": video_info,
            "platform_name": platform_name,
            "user_db_id": user_db_id,
        }

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🎬 فيديو", callback_data="type_video"),
                    InlineKeyboardButton(text="🎧 صوت", callback_data="type_audio"),
                ]
            ]
        )

        await wait_msg.edit_text(
            info_text + "\n\nاختر نوع الإرسال الذي تريده:", reply_markup=kb
        )

    except Exception as e:
        print(f"Unexpected error: {e}")
        try:
            await wait_msg.edit_text(f"❌ حدث خطأ غير متوقع أثناء معالجة الرابط:\n{e}")
        except Exception:
            pass
        log_request_db(
            user_id=user_db_id,
            url=url,
            domain=domain,
            action_type="unknown",
            quality="",
            status="fail",
            error=str(e),
        )


# ================== التعامل مع أزرار الإرسال (فيديو / صوت) ==================


@router.callback_query(F.data.in_(["type_video", "type_audio"]))
async def cb_choose_type(call: CallbackQuery):
    state = USER_STATE.get(call.from_user.id)
    if not state:
        await call.answer("⏳ انتهت الجلسة، أرسل الرابط مرة أخرى.", show_alert=True)
        return

    url = state["url"]
    video_info = state["video_info"]
    platform_name = state["platform_name"]
    user_db_id = state["user_db_id"]

    await call.answer()

    if call.data == "type_audio":
        await call.message.edit_text("🎧 جاري تجهيز الصوت وإرساله، انتظر قليلاً...")
        await send_audio_from_url(call.message, url, video_info, platform_name, user_db_id)
    else:
        qualities = video_info.get("qualities") or []
        if not qualities:
            await call.message.edit_text(
                "🎬 لا توجد عدة جودات متاحة، سيتم الإرسال بأفضل جودة تلقائيًا..."
            )
            await send_video_with_quality(
                call.message, url, video_info, platform_name, None, user_db_id
            )
            return

        rows = []
        row = []
        for q in qualities[:4]:
            h = q["height"]
            btn = InlineKeyboardButton(text=f"{h}p", callback_data=f"q_{h}")
            row.append(btn)
        if row:
            rows.append(row)

        rows.append(
            [
                InlineKeyboardButton(
                    text="⭐ أفضل جودة تلقائيًا", callback_data="q_auto"
                )
            ]
        )

        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        await call.message.edit_text("🎬 اختر جودة الفيديو التي تريدها:", reply_markup=kb)


@router.callback_query(F.data.startswith("q_"))
async def cb_choose_quality(call: CallbackQuery):
    state = USER_STATE.get(call.from_user.id)
    if not state:
        await call.answer("⏳ انتهت الجلسة، أرسل الرابط مرة أخرى.", show_alert=True)
        return

    url = state["url"]
    video_info = state["video_info"]
    platform_name = state["platform_name"]
    user_db_id = state["user_db_id"]

    await call.answer()

    data = call.data
    if data == "q_auto":
        height = None
        await call.message.edit_text("⬇️ جاري التحميل بأفضل جودة متاحة...")
    else:
        try:
            height = int(data.split("_", 1)[1])
        except ValueError:
            height = None
        if height:
            await call.message.edit_text(f"⬇️ جاري التحميل بالجودة {height}p...")

    await send_video_with_quality(
        call.message, url, video_info, platform_name, height, user_db_id
    )


# ================== دوال الإرسال (فيديو / صوت) مع التسجيل في DB ==================


async def send_video_with_quality(
    message: Message,
    url: str,
    video_info: dict,
    platform_name: str,
    height: int | None,
    user_db_id: int,
):
    domain = (urlparse(url).hostname or "").lower()
    quality_str = f"{height}p" if height else "auto"
    status = "fail"
    error_msg = None

    try:
        vtype = video_info.get("type", "unknown")
        duration = video_info.get("duration", 0)
        caption = f"✅ {platform_name}"
        title = video_info.get("title") or "فيديو"
        if title:
            caption += f" | {title[:30]}"

        webpage_url = video_info.get("webpage_url", url)
        log_video_usage(title=title, url=webpage_url, domain=domain)

        if vtype == "direct":
            direct_url = video_info.get("url") or url
            await message.answer("📤 محاولة إرسال مباشر بدون تحميل...")
            send_result = await send_video_direct(message, direct_url, caption, duration)
            if send_result["success"]:
                status = "success"
                log_request_db(
                    user_id=user_db_id,
                    url=url,
                    domain=domain,
                    action_type="video",
                    quality=quality_str,
                    status=status,
                    error=None,
                )
                print("✅ أُرسل الفيديو مباشرة بدون تحميل.")
                return
            await message.answer("⚠️ فشل الإرسال المباشر، سيتم التحميل المؤقت ثم الإرسال...")

            ext = video_info.get("ext", "mp4")
            tmp_path = f"video_temp.{ext}"
            dl = download_video_fallback(direct_url, tmp_path)

        else:
            ext = video_info.get("ext", "mp4")
            tmp_path = f"video_temp.{ext}"

            format_id = None
            if height is not None:
                for q in video_info.get("qualities") or []:
                    if q["height"] == height:
                        format_id = q["format_id"]
                        break

            if format_id:
                dl = download_with_ytdlp(url, tmp_path, format_id=format_id)
            else:
                dl = download_with_ytdlp(url, tmp_path, format_id=None)

            if (not dl["success"]) and video_info.get("url"):
                dl = download_video_fallback(video_info["url"], tmp_path)

        if not dl["success"]:
            error_msg = dl["error"]
            await message.answer(f"❌ فشل تحميل الفيديو:\n{dl['error']}")
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
            return

        if dl["file_size"] > 50 * 1024 * 1024:
            error_msg = "file_too_large"
            await message.answer("❌ حجم الفيديو أكبر من 50MB، لا يمكن إرساله.")
            os.remove(tmp_path)
            return

        await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)

        video_file = FSInputFile(tmp_path)
        await message.answer_video(
            video=video_file,
            caption=caption,
            duration=duration or None,
            supports_streaming=True,
        )

        print("✅ تم تحميل الفيديو مؤقتاً وإرساله.")
        status = "success"

    except Exception as e:
        print(f"send_video_with_quality error: {e}")
        await message.answer(f"❌ حدث خطأ أثناء إرسال الفيديو:\n{e}")
        error_msg = str(e)
    finally:
        for fname in os.listdir("."):
            if fname.startswith("video_temp.") and os.path.isfile(fname):
                try:
                    os.remove(fname)
                    print(f"🧹 تم حذف الملف المؤقت: {fname}")
                except Exception as ee:
                    print(f"خطأ أثناء حذف الملف المؤقت {fname}: {ee}")

        log_request_db(
            user_id=user_db_id,
            url=url,
            domain=domain,
            action_type="video",
            quality=quality_str,
            status=status,
            error=error_msg,
        )


async def send_audio_from_url(
    message: Message,
    url: str,
    video_info: dict,
    platform_name: str,
    user_db_id: int,
):
    tmp_path = "audio_temp.mp3"
    domain = (urlparse(url).hostname or "").lower()
    status = "fail"
    error_msg = None

    try:
        title = video_info.get("title") or "فيديو"
        webpage_url = video_info.get("webpage_url", url)
        log_video_usage(title=title, url=webpage_url, domain=domain)

        dl = download_audio_with_ytdlp(url, tmp_path)
        if not dl["success"]:
            error_msg = dl["error"]
            await message.answer(f"❌ فشل تحميل الصوت:\n{dl['error']}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return

        if dl["file_size"] > 50 * 1024 * 1024:
            error_msg = "file_too_large"
            await message.answer("❌ حجم ملف الصوت أكبر من 50MB، لا يمكن إرساله.")
            os.remove(tmp_path)
            return

        await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_VOICE)

        caption = f"🎧 من: {platform_name}"
        if title:
            caption += f" | {title[:30]}"

        audio_file = FSInputFile(tmp_path)
        await message.answer_audio(
            audio=audio_file,
            caption=caption,
        )

        print("✅ تم تحميل الصوت مؤقتاً وإرساله.")
        status = "success"

    except Exception as e:
        print(f"send_audio_from_url error: {e}")
        await message.answer(f"❌ حدث خطأ أثناء إرسال الصوت:\n{e}")
        error_msg = str(e)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
                print("🧹 تم حذف ملف الصوت المؤقت.")
            except Exception as ee:
                print(f"خطأ أثناء حذف ملف الصوت المؤقت: {ee}")

        log_request_db(
            user_id=user_db_id,
            url=url,
            domain=domain,
            action_type="audio",
            quality="audio",
            status=status,
            error=error_msg,
        )


# ================== run ==================


async def main():
    print("📂 تهيئة قاعدة البيانات...")
    init_db()
    load_banned_users()
    load_blocked_domains()
    print("🚀 Bot is running...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
