"""BingoLink File Store: Telegram bot + MongoDB + BingoLink API.

Run a SINGLE long-polling instance. Never put credentials in source control.
"""
from __future__ import annotations

import asyncio
import hashlib
import html
import logging
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import aiohttp
from mongo_store import MongoStore
from dotenv import load_dotenv

load_dotenv()
LOG = logging.getLogger("bingolink_filebot")
MAX_BULK = 30
TOKEN_HOURS = 3
PRIVATE_REQUEST_HOURS = 24
DELETE_MAX_MINUTES = 47 * 60  # Telegram normally refuses to delete messages after 48h.


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def fresh_id(size: int = 9) -> str:
    return secrets.token_urlsafe(size)


def row_button(text: str, *, url: str | None = None, data: str | None = None) -> dict:
    item = {"text": text}
    if url is not None:
        item["url"] = url
    else:
        item["callback_data"] = data
    return item


def kb(*rows: list[dict]) -> dict:
    return {"inline_keyboard": list(rows)}


class TelegramError(Exception):
    pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS blbot_settings (
  key TEXT PRIMARY KEY, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS blbot_sudo (
  user_id BIGINT PRIMARY KEY, added_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS blbot_packages (
  id TEXT PRIMARY KEY, uploader_id BIGINT NOT NULL,
  published BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS blbot_items (
  package_id TEXT NOT NULL REFERENCES blbot_packages(id) ON DELETE CASCADE,
  position INT NOT NULL, backup_message_id BIGINT NOT NULL,
  backup_chat_id TEXT NOT NULL,
  PRIMARY KEY (package_id, position)
);
CREATE TABLE IF NOT EXISTS blbot_ingested (
  source_chat_id BIGINT NOT NULL, source_message_id BIGINT NOT NULL,
  package_id TEXT NOT NULL REFERENCES blbot_packages(id) ON DELETE CASCADE,
  PRIMARY KEY (source_chat_id, source_message_id)
);
CREATE TABLE IF NOT EXISTS blbot_drafts (
  uploader_id BIGINT PRIMARY KEY,
  package_id TEXT NOT NULL REFERENCES blbot_packages(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS blbot_force_chats (
  chat_id TEXT PRIMARY KEY, name TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('public', 'private')),
  join_url TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS blbot_join_requests (
  chat_id TEXT NOT NULL, user_id BIGINT NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (chat_id, user_id)
);
CREATE TABLE IF NOT EXISTS blbot_unlocks (
  token_hash CHAR(64) PRIMARY KEY,
  package_id TEXT NOT NULL REFERENCES blbot_packages(id) ON DELETE CASCADE,
  user_id BIGINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'issued'
    CHECK (status IN ('issued', 'delivering', 'used')),
  delivery_cursor INT NOT NULL DEFAULT 0,
  short_url TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  claimed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS blbot_unlock_user_idx ON blbot_unlocks (user_id, package_id, created_at DESC);
CREATE INDEX IF NOT EXISTS blbot_unlock_expiry_idx ON blbot_unlocks (expires_at);
CREATE TABLE IF NOT EXISTS blbot_pending (
  user_id BIGINT PRIMARY KEY,
  package_id TEXT NOT NULL,
  claim_hash CHAR(64),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS blbot_admin_input (
  user_id BIGINT PRIMARY KEY, action TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS blbot_sent_media (
  chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
  delete_at TIMESTAMPTZ NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  next_try_at TIMESTAMPTZ,
  PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS blbot_media_delete_idx ON blbot_sent_media (delete_at, next_try_at);
"""


class FileBot:
    def __init__(self):
        self.token = os.environ["BOT_TOKEN"].strip()
        self.owner = int(os.environ["BOT_OWNER_ID"])
        self.shortener_key = os.environ["BINGOLINK_API_KEY"].strip()
        self.shortener = os.getenv("BINGOLINK_BASE_URL", "https://www.bingolink.site").rstrip("/")
        parsed = urlsplit(self.shortener)
        if parsed.scheme != "https" or not parsed.netloc or parsed.path:
            raise ValueError("BINGOLINK_BASE_URL must be an HTTPS origin, e.g. https://www.bingolink.site")
        if not self.shortener_key.startswith("bl_live_"):
            raise ValueError("BINGOLINK_API_KEY must be a BingoLink developer API key")
        self.mongo_uri = os.environ["MONGODB_URI"].strip()
        self.mongo_db = os.getenv("MONGODB_DB", "bingolink_filebot").strip()
        self.http: aiohttp.ClientSession | None = None
        self.db: MongoStore | None = None
        self.bot_id: int = 0
        self.bot_username: str = ""
        self.locks: dict[str, asyncio.Lock] = {}

    def lock(self, key: str) -> asyncio.Lock:
        return self.locks.setdefault(key, asyncio.Lock())

    async def tg(self, method: str, **payload):
        assert self.http is not None
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        for attempt in range(2):
            try:
                async with self.http.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                    data = await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == 0:
                    await asyncio.sleep(2)
                    continue
                raise TelegramError(f"Telegram {method}: network error") from exc
            if data.get("ok"):
                return data["result"]
            if data.get("error_code") == 429 and attempt == 0:
                await asyncio.sleep(min(30, int(data.get("parameters", {}).get("retry_after", 3))))
                continue
            raise TelegramError(f"Telegram {method}: {data.get('description', 'unknown error')}")
        raise TelegramError(f"Telegram {method}: failed")

    async def say(self, chat: int, text: str, buttons: dict | None = None):
        payload = {"chat_id": chat, "text": text, "disable_web_page_preview": True, "parse_mode": "HTML"}
        if buttons:
            payload["reply_markup"] = buttons
        return await self.tg("sendMessage", **payload)

    async def config(self, name: str, default: str = "") -> str:
        val = await self.db.fetchval("SELECT value FROM blbot_settings WHERE key=$1", name)
        return default if val is None else val

    async def set_config(self, name: str, value: str):
        await self.db.execute("""INSERT INTO blbot_settings(key,value) VALUES($1,$2)
             ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""", name, value)

    async def is_uploader(self, user_id: int) -> bool:
        return user_id == self.owner or bool(await self.db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM blbot_sudo WHERE user_id=$1)", user_id))

    async def setup(self):
        self.db = MongoStore(self.mongo_uri, self.mongo_db)
        await self.db.setup()
        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45))
        me = await self.tg("getMe")
        self.bot_id = int(me["id"])
        self.bot_username = me["username"]
        LOG.info("Ready as @%s (MongoDB collections initialized)", self.bot_username)

    async def close(self):
        if self.http:
            await self.http.close()
        if self.db:
            await self.db.close()

    def share_url(self, package_id: str) -> str:
        return f"https://t.me/{self.bot_username}?start=s_{package_id}"

    async def backup_id(self) -> str | None:
        return (await self.config("backup_chat_id", os.getenv("BACKUP_CHANNEL_ID", ""))).strip() or None

    async def join_missing(self, user_id: int) -> list[dict]:
        if await self.config("force_join", "on") == "off":
            return []
        missing = []
        chats = await self.db.fetch("SELECT * FROM blbot_force_chats ORDER BY name")
        for c in chats:
            try:
                member = await self.tg("getChatMember", chat_id=c["chat_id"], user_id=user_id)
                status = member.get("status")
                if status in ("creator", "administrator", "member") or (
                    status == "restricted" and member.get("is_member") is True
                ):
                    continue
            except TelegramError as exc:
                LOG.warning("Join check failed in %s: %s", c["chat_id"], exc)
                # Fail closed. Telegram only guarantees getChatMember for admins.
            if c["kind"] == "private":
                recent = await self.db.fetchval("""SELECT EXISTS(
                  SELECT 1 FROM blbot_join_requests
                  WHERE chat_id=$1 AND user_id=$2 AND requested_at > NOW() - INTERVAL '24 hours'
                )""", c["chat_id"], user_id)
                if recent:
                    continue
            missing.append(dict(c))
        return missing

    async def join_prompt(self, user_id: int, missing: list[dict]):
        rows = [[row_button("Join: " + html.unescape(c["name"])[:48], url=c["join_url"])] for c in missing]
        rows.append([row_button("✅ Try Again", data="checkjoin")])
        await self.say(user_id, "🔒 <b>Join required</b>\nJoin the channels/groups below, then tap <b>Try Again</b>. "
                              "For private channels, a join request received in the past 24 hours also counts. "
                              "You won't need to open the original link again.", kb(*rows))

    async def pending_set(self, user_id: int, package_id: str, claim_hash: str | None = None):
        await self.db.execute("""INSERT INTO blbot_pending(user_id,package_id,claim_hash)
            VALUES($1,$2,$3) ON CONFLICT(user_id)
            DO UPDATE SET package_id=EXCLUDED.package_id,
              claim_hash=EXCLUDED.claim_hash, updated_at=NOW()""",
                              user_id, package_id, claim_hash)

    async def pending_clear(self, user_id: int):
        await self.db.execute("DELETE FROM blbot_pending WHERE user_id=$1", user_id)

    async def package(self, package_id: str):
        return await self.db.fetchrow("SELECT * FROM blbot_packages WHERE id=$1 AND published=TRUE", package_id)

    async def start_share(self, user_id: int, package_id: str):
        if not await self.package(package_id):
            await self.say(user_id, "This file link is invalid or no longer available.")
            return
        await self.pending_set(user_id, package_id)
        missing = await self.join_missing(user_id)
        if missing:
            await self.join_prompt(user_id, missing)
            return
        await self.issue_shortlink(user_id, package_id)
        await self.pending_clear(user_id)

    async def issue_shortlink(self, user_id: int, package_id: str):
        async with self.lock(f"issue:{user_id}:{package_id}"):
            previous = await self.db.fetchrow("""SELECT short_url FROM blbot_unlocks
               WHERE user_id=$1 AND package_id=$2 AND status='issued'
                 AND expires_at>NOW() AND short_url IS NOT NULL
               ORDER BY created_at DESC LIMIT 1""", user_id, package_id)
            if previous:
                short = previous["short_url"]
            else:
                token = fresh_id(24)
                token_hash = digest(token)
                deep_link = f"https://t.me/{self.bot_username}?start=r_{token}"
                await self.db.execute("""INSERT INTO blbot_unlocks
                  (token_hash,package_id,user_id,expires_at)
                  VALUES($1,$2,$3,NOW()+INTERVAL '3 hours')""", token_hash, package_id, user_id)
                try:
                    async with self.http.post(
                        self.shortener + "/api/v1/links",
                        json={"destination": deep_link, "title": "Telegram file unlock"},
                        headers={"Authorization": "Bearer " + self.shortener_key},
                        timeout=aiohttp.ClientTimeout(total=20)
                    ) as response:
                        result = await response.json(content_type=None)
                        if response.status != 201 or "link" not in result:
                            raise ValueError(f"BingoLink API HTTP {response.status}: {result.get('error', 'unknown error')}")
                        slug = result["link"].get("slug")
                        if not isinstance(slug, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,39}", slug):
                            raise ValueError("BingoLink API returned an invalid slug")
                        short = f"{self.shortener}/s/{slug}"
                    await self.db.execute("UPDATE blbot_unlocks SET short_url=$2 WHERE token_hash=$1", token_hash, short)
                except Exception:
                    await self.db.execute("DELETE FROM blbot_unlocks WHERE token_hash=$1", token_hash)
                    LOG.exception("Shortener API failure for package %s", package_id)
                    await self.say(user_id, "The shortener is temporarily unavailable. Tap Try Again in a moment.",
                                   kb([row_button("🔄 Try Again", data="retryshare:" + package_id)]))
                    return
            await self.say(user_id, "🔗 <b>Your unlock link</b>\nOpen BingoLink, finish its article pages, "
                                    "then tap <b>Get Link</b> to return here and receive your videos. "
                                    "This unlock is tied to your Telegram account and can be used once.",
                           kb([row_button("🌐 Open BingoLink", url=short)]))

    async def start_redeem(self, user_id: int, raw_token: str):
        if not re.fullmatch(r"[A-Za-z0-9_-]{25,48}", raw_token):
            await self.say(user_id, "Invalid unlock link.")
            return
        token_hash = digest(raw_token)
        item = await self.db.fetchrow("SELECT * FROM blbot_unlocks WHERE token_hash=$1 AND user_id=$2",
                                       token_hash, user_id)
        if not item:
            await self.say(user_id, "This unlock is invalid or belongs to another Telegram account.")
            return
        if item["status"] == "used":
            await self.say(user_id, "This unlock has already been used. Open the original file-share link to start again.")
            return
        if item["expires_at"] <= datetime.now(timezone.utc) and item["status"] == "issued":
            await self.say(user_id, "This unlock expired. Open the original file-share link for a new one.")
            return
        missing = await self.join_missing(user_id)
        if missing:
            await self.pending_set(user_id, item["package_id"], token_hash)
            await self.join_prompt(user_id, missing)
            return
        await self.pending_clear(user_id)
        await self.deliver(user_id, token_hash)

    async def deliver(self, user_id: int, token_hash: str):
        async with self.lock("deliver:" + token_hash):
            item = await self.db.fetchrow("""UPDATE blbot_unlocks
               SET status='delivering', claimed_at=COALESCE(claimed_at,NOW())
               WHERE token_hash=$1 AND user_id=$2 AND status='issued' AND expires_at>NOW()
               RETURNING *""", token_hash, user_id)
            if not item:
                item = await self.db.fetchrow("SELECT * FROM blbot_unlocks WHERE token_hash=$1 AND user_id=$2",
                                              token_hash, user_id)
                if not item or item["status"] != "delivering":
                    await self.say(user_id, "This unlock is already used or expired.")
                    return
            package_id = item["package_id"]
            files = await self.db.fetch("""SELECT position,backup_message_id,backup_chat_id FROM blbot_items
                                            WHERE package_id=$1 ORDER BY position""", package_id)
            if not files:
                await self.say(user_id, "These files are no longer available.")
                return
            protect = await self.config("forward_allowed", "off") != "on"
            minutes = int(await self.config("auto_delete_minutes", "60"))
            cursor = item["delivery_cursor"]
            await self.say(user_id, f"✅ <b>Unlocked!</b> Sending {len(files) - cursor} video(s) now…")
            for file in files[cursor:]:
                try:
                    sent = await self.tg("copyMessage", chat_id=user_id,
                                         from_chat_id=file["backup_chat_id"],
                                         message_id=int(file["backup_message_id"]),
                                         protect_content=protect)
                    if minutes:
                        await self.db.execute("""INSERT INTO blbot_sent_media(chat_id,message_id,delete_at)
                             VALUES($1,$2,NOW()+($3 * INTERVAL '1 minute'))
                             ON CONFLICT DO NOTHING""", user_id, int(sent["message_id"]), minutes)
                    await self.db.execute("""UPDATE blbot_unlocks SET delivery_cursor=$2
                        WHERE token_hash=$1 AND status='delivering'""", token_hash, int(file["position"]) + 1)
                except Exception:
                    LOG.exception("Media delivery failed: package=%s position=%s", package_id, file["position"])
                    await self.say(user_id, "One video couldn't be sent. Tap Retry to get the remaining files; "
                                            "you do not need to watch the articles again.",
                                   kb([row_button("🔄 Retry remaining", data="resume:" + token_hash[:24])]))
                    return
            await self.db.execute("UPDATE blbot_unlocks SET status='used' WHERE token_hash=$1", token_hash)
            if minutes:
                await self.say(user_id, f"✅ All videos sent. The bot will try to delete its video messages after {minutes} minutes.")
            else:
                await self.say(user_id, "✅ All videos sent.")

    async def upload_video(self, message: dict, user_id: int):
        source_chat = int(message["chat"]["id"])
        source_message = int(message["message_id"])
        previous = await self.db.fetchrow("""SELECT p.id,p.published FROM blbot_ingested i
           JOIN blbot_packages p ON p.id=i.package_id
           WHERE i.source_chat_id=$1 AND i.source_message_id=$2""", source_chat, source_message)
        if previous:
            note = ("Previously saved: " + self.share_url(previous["id"])) if previous["published"] else                    "This video is already in your current bundle. Continue uploading and send /done."
            await self.say(user_id, html.escape(note))
            return
        backup = await self.backup_id()
        if not backup:
            await self.say(user_id, "Set a backup channel first with /setbackup -100CHANNEL_ID")
            return
        draft = await self.db.fetchrow("SELECT package_id FROM blbot_drafts WHERE uploader_id=$1", user_id)
        pkg = draft["package_id"] if draft else fresh_id()
        if draft:
            count = await self.db.fetchval("SELECT COUNT(*) FROM blbot_items WHERE package_id=$1", pkg)
            if count >= MAX_BULK:
                await self.say(user_id, f"Bulk limit: {MAX_BULK} videos. Send /done.")
                return
        try:
            saved = await self.tg("forwardMessage", chat_id=backup,
                                  from_chat_id=message["chat"]["id"], message_id=message["message_id"],
                                  disable_notification=True)
        except TelegramError:
            # For content that cannot be forwarded, Telegram may permit copying.
            try:
                saved = await self.tg("copyMessage", chat_id=backup,
                                      from_chat_id=message["chat"]["id"], message_id=message["message_id"])
            except TelegramError:
                await self.say(user_id, "Telegram couldn't save this video to the backup channel. "
                                        "Check bot admin permissions and source restrictions.")
                return
        async with self.db.acquire() as conn:
            async with conn.transaction():
                if not draft:
                    await conn.execute("INSERT INTO blbot_packages(id,uploader_id,published) VALUES($1,$2,TRUE)", pkg, user_id)
                position = await conn.fetchval("SELECT COUNT(*) FROM blbot_items WHERE package_id=$1", pkg)
                await conn.execute("""INSERT INTO blbot_items(package_id,position,backup_message_id,backup_chat_id)
                                      VALUES($1,$2,$3,$4)""", pkg, position, int(saved["message_id"]), str(backup))
                await conn.execute("""INSERT INTO blbot_ingested(source_chat_id,source_message_id,package_id)
                                      VALUES($1,$2,$3)""", source_chat, source_message, pkg)
        if draft:
            await self.say(user_id, f"✅ Saved video #{position + 1}. Send more, then /done to get one share link.")
        else:
            await self.say(user_id, "✅ Video saved to your backup channel.\n<b>Share link:</b>\n" +
                                     html.escape(self.share_url(pkg)))

    async def start_bulk(self, user_id: int):
        if not await self.backup_id():
            await self.say(user_id, "Set the backup channel first: /setbackup -100CHANNEL_ID")
            return
        existing = await self.db.fetchval("SELECT package_id FROM blbot_drafts WHERE uploader_id=$1", user_id)
        if existing:
            count = await self.db.fetchval("SELECT COUNT(*) FROM blbot_items WHERE package_id=$1", existing)
            await self.say(user_id, f"You already have a bulk upload with {count} videos. Send /done or /cancel.")
            return
        package_id = fresh_id()
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("INSERT INTO blbot_packages(id,uploader_id) VALUES($1,$2)", package_id, user_id)
                await conn.execute("INSERT INTO blbot_drafts(uploader_id,package_id) VALUES($1,$2)", user_id, package_id)
        await self.say(user_id, f"📁 <b>Bulk upload started.</b>\nSend 1–{MAX_BULK} videos (albums also work). "
                                "When finished, send /done. All videos will share ONE link.")

    async def finish_bulk(self, user_id: int):
        draft = await self.db.fetchrow("SELECT package_id FROM blbot_drafts WHERE uploader_id=$1", user_id)
        if not draft:
            await self.say(user_id, "No active bulk upload. Start with /bulk.")
            return
        pkg = draft["package_id"]
        count = await self.db.fetchval("SELECT COUNT(*) FROM blbot_items WHERE package_id=$1", pkg)
        if not count:
            await self.say(user_id, "No videos added yet. Send video(s) or /cancel.")
            return
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("UPDATE blbot_packages SET published=TRUE WHERE id=$1 AND uploader_id=$2", pkg, user_id)
                await conn.execute("DELETE FROM blbot_drafts WHERE uploader_id=$1", user_id)
        await self.say(user_id, f"✅ <b>Bundle ready:</b> {count} videos in one link.\n" +
                                 html.escape(self.share_url(pkg)))

    async def cancel_bulk(self, user_id: int):
        draft = await self.db.fetchrow("SELECT package_id FROM blbot_drafts WHERE uploader_id=$1", user_id)
        if not draft:
            await self.say(user_id, "No active bulk upload.")
            return
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM blbot_drafts WHERE uploader_id=$1", user_id)
                await conn.execute("DELETE FROM blbot_packages WHERE id=$1 AND published=FALSE", draft["package_id"])
        await self.say(user_id, "Bulk draft discarded. Already forwarded backup posts stay in the backup channel.")

    async def settings_menu(self, user_id: int):
        backup = await self.backup_id() or "Not configured"
        minutes = await self.config("auto_delete_minutes", "60")
        forward = await self.config("forward_allowed", "off")
        force = await self.config("force_join", "on")
        count = await self.db.fetchval("SELECT COUNT(*) FROM blbot_force_chats")
        sudo = await self.db.fetchval("SELECT COUNT(*) FROM blbot_sudo")
        await self.say(user_id,
            "⚙️ <b>File Bot Settings</b>\n"
            f"Backup: <code>{html.escape(str(backup))}</code>\n"
            f"Auto-delete DM media: <b>{html.escape(minutes)} min</b> (0=off)\n"
            f"Forward/save allowed: <b>{html.escape(forward)}</b>\n"
            f"Force join: <b>{html.escape(force)}</b> ({count} chats)\n"
            f"Sudo uploaders: <b>{sudo}</b>\n\n"
            "For backup, force-join and sudo management, tap Commands.",
            kb(
                [row_button("Forward ON/OFF", data="settings:forward"), row_button("Force Join ON/OFF", data="settings:force")],
                [row_button("Delete OFF", data="settings:delete:0"), row_button("5m", data="settings:delete:5"), row_button("30m", data="settings:delete:30")],
                [row_button("1h", data="settings:delete:60"), row_button("6h", data="settings:delete:360"), row_button("24h", data="settings:delete:1440")],
                [row_button("➕ Add Join", data="settings:prompt:addjoin"), row_button("➖ Remove Join", data="settings:prompt:rmjoin")],
                [row_button("➕ Add Sudo", data="settings:prompt:addsudo"), row_button("➖ Remove Sudo", data="settings:prompt:remsudo")],
                [row_button("Set Backup", data="settings:prompt:setbackup"), row_button("Other Commands", data="settings:help")],
                [row_button("Channels & Sudo List", data="settings:details")]
            ))

    async def settings_details(self, user_id: int):
        channels = await self.db.fetch("SELECT * FROM blbot_force_chats ORDER BY name")
        sudo = await self.db.fetch("SELECT user_id FROM blbot_sudo ORDER BY added_at")
        force_text = "\n".join(f"• {html.escape(c['name'])} — <code>{c['chat_id']}</code> ({c['kind']})" for c in channels) or "None"
        sudo_text = ", ".join(str(x["user_id"]) for x in sudo) or "None"
        await self.say(user_id, "<b>Force-join chats</b>\n" + force_text + "\n\n<b>Sudo user IDs</b>\n" + sudo_text +
                       "\n\nUse /addjoin, /rmjoin, /addsudo or /remsudo to manage.")

    async def settings_help(self, user_id: int):
        await self.say(user_id, "<b>Owner commands</b>\n"
          "<code>/setbackup -1001234567890</code>\n"
          "<code>/addjoin @PublicChannel public</code>\n"
          "<code>/addjoin -1001234567890 private</code> (bot creates join-request invite)\n"
          "<code>/addjoin -1001234567890 private https://t.me/+INVITE</code>\n"
          "<code>/rmjoin -1001234567890</code>\n"
          "<code>/addsudo 123456789</code>\n"
          "<code>/remsudo 123456789</code>\n"
          "<code>/setdelete 60</code> (0–2820 min)\n"
          "<code>/forward on</code> or <code>/forward off</code>\n"
          "<code>/forcejoin on</code> or <code>/forcejoin off</code>\n"
          "<code>/settings</code> • <code>/stats</code>\n\n"
          "Only the OWNER can change these settings. Sudo users can upload files and create bundles. "
          "Bot must be channel admin; for private pending requests it needs Invite Users rights.")

    async def manage_command(self, user_id: int, command: str, args: list[str]):
        if user_id != self.owner:
            await self.say(user_id, "Only the bot owner can change settings.")
            return
        if command == "/settings":
            return await self.settings_menu(user_id)
        if command == "/stats":
            stats = await self.db.fetchrow("""SELECT
               (SELECT COUNT(*) FROM blbot_packages WHERE published) packages,
               (SELECT COUNT(*) FROM blbot_items) videos,
               (SELECT COUNT(*) FROM blbot_unlocks WHERE status='used') completed_unlocks""")
            return await self.say(user_id, "📊 <b>Bot statistics</b>\n" +
                                  "\n".join(f"{k}: {v}" for k, v in dict(stats).items()))
        if command == "/setbackup":
            if len(args) != 1 or not re.fullmatch(r"-?\d+", args[0]):
                return await self.say(user_id, "Usage: /setbackup -100CHANNEL_ID")
            try:
                me = await self.tg("getChatMember", chat_id=args[0], user_id=self.bot_id)
                chat = await self.tg("getChat", chat_id=args[0])
                if chat.get("type") != "channel" or me.get("status") not in ("administrator", "creator") or not me.get("can_post_messages", False):
                    raise ValueError("Bot needs channel administrator permission to post messages")
                await self.set_config("backup_chat_id", str(chat["id"]))
            except (TelegramError, ValueError) as exc:
                return await self.say(user_id, "Couldn't set backup: " + html.escape(str(exc)))
            return await self.say(user_id, "✅ Backup channel set to " + html.escape(chat.get("title", str(chat["id"]))))
        if command == "/addjoin":
            if len(args) < 2 or args[1].lower() not in ("public", "private"):
                return await self.say(user_id, "Usage: /addjoin @publicchannel public OR /addjoin -100CHANNEL_ID private [invite-url]")
            ref, kind = args[0], args[1].lower()
            try:
                chat = await self.tg("getChat", chat_id=ref)
                chat_id = str(chat["id"])
                member = await self.tg("getChatMember", chat_id=chat_id, user_id=self.bot_id)
                if member.get("status") not in ("administrator", "creator"):
                    raise ValueError("Add the bot as admin to this chat first")
                if kind == "private":
                    if not member.get("can_invite_users") and member.get("status") != "creator":
                        raise ValueError("Bot needs Invite Users admin permission for private join requests")
                    if len(args) >= 3:
                        invite = args[2]
                        if not (invite.startswith("https://t.me/+") or invite.startswith("https://t.me/joinchat/")):
                            raise ValueError("Invalid private invite link")
                    else:
                        link = await self.tg("createChatInviteLink", chat_id=chat_id,
                                             name="BingoLink force join", creates_join_request=True)
                        invite = link["invite_link"]
                else:
                    username = chat.get("username")
                    if not username:
                        raise ValueError("Public chat needs a public @username")
                    invite = f"https://t.me/{username}"
                await self.db.execute("""INSERT INTO blbot_force_chats(chat_id,name,kind,join_url)
                  VALUES($1,$2,$3,$4) ON CONFLICT(chat_id) DO UPDATE
                  SET name=EXCLUDED.name,kind=EXCLUDED.kind,join_url=EXCLUDED.join_url""",
                                      chat_id, chat.get("title", chat_id), kind, invite)
            except (TelegramError, ValueError) as exc:
                return await self.say(user_id, "Couldn't add force join: " + html.escape(str(exc)))
            return await self.say(user_id, "✅ Force join added: " + html.escape(chat.get("title", chat_id)))
        if command == "/rmjoin":
            if len(args) != 1:
                return await self.say(user_id, "Usage: /rmjoin @username or /rmjoin -100CHAT_ID")
            ref = args[0]
            try:
                chat = await self.tg("getChat", chat_id=ref)
                ref = str(chat["id"])
            except TelegramError:
                pass
            done = await self.db.execute("DELETE FROM blbot_force_chats WHERE chat_id=$1", ref)
            return await self.say(user_id, "Removed." if done.endswith("1") else "Chat wasn't in the force-join list.")
        if command in ("/addsudo", "/remsudo"):
            if len(args) != 1 or not args[0].isdigit():
                return await self.say(user_id, f"Usage: {command} TELEGRAM_NUMERIC_ID")
            target = int(args[0])
            if target == self.owner:
                return await self.say(user_id, "The owner can't be added or removed as sudo.")
            if command == "/addsudo":
                await self.db.execute("INSERT INTO blbot_sudo(user_id) VALUES($1) ON CONFLICT DO NOTHING", target)
            else:
                await self.db.execute("DELETE FROM blbot_sudo WHERE user_id=$1", target)
            return await self.say(user_id, "✅ Sudo list updated.")
        if command == "/setdelete":
            if len(args) != 1 or not args[0].isdigit() or not 0 <= int(args[0]) <= DELETE_MAX_MINUTES:
                return await self.say(user_id, "Usage: /setdelete MINUTES (0–2820, 0=off)")
            await self.set_config("auto_delete_minutes", args[0])
            return await self.settings_menu(user_id)
        if command in ("/forward", "/forcejoin"):
            if len(args) != 1 or args[0] not in ("on", "off"):
                return await self.say(user_id, f"Usage: {command} on|off")
            await self.set_config("forward_allowed" if command == "/forward" else "force_join", args[0])
            return await self.settings_menu(user_id)

    async def handle_message(self, message: dict):
        chat = message["chat"]
        if chat.get("type") != "private" or "from" not in message:
            return
        uid = int(message["from"]["id"])
        txt = message.get("text", "")
        if txt.startswith("/"):
            parts = txt.split()
            command = parts[0].split("@")[0].lower()
            args = parts[1:]
            if command == "/start":
                if args and args[0].startswith("s_"):
                    return await self.start_share(uid, args[0][2:])
                if args and args[0].startswith("r_"):
                    return await self.start_redeem(uid, args[0][2:])
                return await self.say(uid, "👋 <b>BingoLink File Store</b>\nOpen a file-share link to unlock videos "
                                            "through BingoLink.\n" +
                                      ("As an uploader: send a video for a single link, or /bulk then /done "
                                       "for several videos under one link." if await self.is_uploader(uid) else
                                       "If you have a file link, open it to begin."))
            if command == "/id":
                return await self.say(uid, f"Your Telegram ID: <code>{uid}</code>")
            if command == "/files":
                if not await self.is_uploader(uid):
                    return await self.say(uid, "Only the owner/sudo can list stored bundles.")
                if uid == self.owner:
                    files = await self.db.fetch("""SELECT p.id,COUNT(i.position)::int AS total FROM blbot_packages p
                        JOIN blbot_items i ON i.package_id=p.id
                        WHERE p.published=TRUE GROUP BY p.id,p.created_at
                        ORDER BY p.created_at DESC LIMIT 10""")
                else:
                    files = await self.db.fetch("""SELECT p.id,COUNT(i.position)::int AS total FROM blbot_packages p
                        JOIN blbot_items i ON i.package_id=p.id
                        WHERE p.published=TRUE AND p.uploader_id=$1
                        GROUP BY p.id,p.created_at ORDER BY p.created_at DESC LIMIT 10""", uid)
                lines = [f"• {x['total']} video(s): {html.escape(self.share_url(x['id']))}" for x in files]
                return await self.say(uid, "<b>Your recent share links</b>\n" +
                                      ("\n".join(lines) if lines else "No bundles yet."))
            if command in ("/settings", "/stats", "/setbackup", "/addjoin", "/rmjoin",
                           "/addsudo", "/remsudo", "/setdelete", "/forward", "/forcejoin"):
                return await self.manage_command(uid, command, args)
            if command in ("/bulk", "/done", "/cancel"):
                if not await self.is_uploader(uid):
                    return await self.say(uid, "Only the owner and sudo uploaders can store videos.")
                if command == "/bulk":
                    return await self.start_bulk(uid)
                if command == "/done":
                    return await self.finish_bulk(uid)
                return await self.cancel_bulk(uid)
            if command == "/help":
                return await self.say(uid, "Share link → join checks → BingoLink article steps → one-time unlock → videos. "
                                           "Uploaders: /bulk, send videos, /done, /cancel, /files. Owner: /settings. /id shows your ID.")
            return await self.say(uid, "Unknown command. Send /help.")
        if uid == self.owner and txt.strip():
            admin_input = await self.db.fetchrow("""SELECT action FROM blbot_admin_input
                 WHERE user_id=$1 AND created_at>NOW()-INTERVAL '10 minutes'""", uid)
            if admin_input:
                await self.db.execute("DELETE FROM blbot_admin_input WHERE user_id=$1", uid)
                return await self.manage_command(uid, "/" + admin_input["action"], txt.strip().split())
        is_video = bool(message.get("video") or (message.get("document") or {}).get("mime_type", "").startswith("video/"))
        if is_video:
            if not await self.is_uploader(uid):
                return await self.say(uid, "Only the owner/sudo can upload videos.")
            return await self.upload_video(message, uid)
        if await self.is_uploader(uid):
            await self.say(uid, "Send a video to create a link, or /bulk for multiple videos. /settings for owner controls.")
        else:
            await self.say(uid, "Open the file-share link you received to get started.")

    async def handle_callback(self, call: dict):
        user_id = int(call["from"]["id"])
        cid = call["id"]
        value = call.get("data", "")
        try:
            await self.tg("answerCallbackQuery", callback_query_id=cid)
        except TelegramError:
            pass
        if value.startswith("settings:"):
            if user_id != self.owner:
                return await self.say(user_id, "Only the owner can change settings.")
            action = value.split(":")
            if action[1] == "forward":
                old = await self.config("forward_allowed", "off")
                await self.set_config("forward_allowed", "on" if old == "off" else "off")
            elif action[1] == "force":
                old = await self.config("force_join", "on")
                await self.set_config("force_join", "on" if old == "off" else "off")
            elif action[1] == "delete" and len(action) == 3 and action[2].isdigit():
                await self.set_config("auto_delete_minutes", action[2])
            elif action[1] == "prompt" and len(action) == 3:
                prompts = {
                    "addjoin": "Send: <code>@PublicChannel public</code> OR <code>-100CHANNEL_ID private</code>. "
                               "For private chats the bot can create a request invite automatically.",
                    "rmjoin": "Send the chat ID to remove, e.g. <code>-1001234567890</code>.",
                    "addsudo": "Send the uploader's numeric Telegram ID, e.g. <code>123456789</code>.",
                    "remsudo": "Send the sudo user's numeric Telegram ID to remove.",
                    "setbackup": "Send the numeric backup-channel ID, e.g. <code>-1001234567890</code>. "
                                 "The bot must already be channel admin with Post Messages permission.",
                }
                chosen = action[2]
                if chosen in prompts:
                    await self.db.execute("""INSERT INTO blbot_admin_input(user_id,action)
                         VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE
                         SET action=EXCLUDED.action,created_at=NOW()""", user_id, chosen)
                    return await self.say(user_id, prompts[chosen] + "\n\nYou have 10 minutes to reply.")
            elif action[1] == "details":
                return await self.settings_details(user_id)
            elif action[1] == "help":
                return await self.settings_help(user_id)
            return await self.settings_menu(user_id)
        if value == "checkjoin":
            pending = await self.db.fetchrow("SELECT * FROM blbot_pending WHERE user_id=$1", user_id)
            if not pending:
                return await self.say(user_id, "No pending file request. Open your original file-share link.")
            missing = await self.join_missing(user_id)
            if missing:
                return await self.join_prompt(user_id, missing)
            if pending["claim_hash"]:
                await self.pending_clear(user_id)
                return await self.deliver(user_id, pending["claim_hash"])
            pkg = pending["package_id"]
            await self.pending_clear(user_id)
            return await self.issue_shortlink(user_id, pkg)
        if value.startswith("retryshare:"):
            return await self.start_share(user_id, value.split(":", 1)[1])
        if value.startswith("resume:"):
            prefix = value.split(":", 1)[1]
            # Telegram callback_data cannot exceed 64 bytes: use a 96-bit hash prefix.
            if re.fullmatch(r"[a-f0-9]{24}", prefix):
                item = await self.db.fetchrow("""SELECT token_hash,package_id FROM blbot_unlocks
                      WHERE token_hash LIKE $1 AND user_id=$2 AND status='delivering'
                      ORDER BY claimed_at DESC LIMIT 1""", prefix + "%", user_id)
                if not item:
                    return await self.say(user_id, "No unfinished delivery found.")
                missing = await self.join_missing(user_id)
                if missing:
                    await self.pending_set(user_id, item["package_id"], item["token_hash"])
                    return await self.join_prompt(user_id, missing)
                return await self.deliver(user_id, item["token_hash"])

    async def handle_join_request(self, req: dict):
        chat_id = str(req["chat"]["id"])
        if not await self.db.fetchval("SELECT EXISTS(SELECT 1 FROM blbot_force_chats WHERE chat_id=$1 AND kind='private')", chat_id):
            return
        dt = datetime.fromtimestamp(int(req["date"]), tz=timezone.utc)
        await self.db.execute("""INSERT INTO blbot_join_requests(chat_id,user_id,requested_at)
           VALUES($1,$2,$3) ON CONFLICT(chat_id,user_id)
           DO UPDATE SET requested_at=EXCLUDED.requested_at""", chat_id, int(req["from"]["id"]), dt)

    async def handle_chat_member(self, update: dict):
        """Remove stored join request upon a definite ban/leave update, if Telegram supplies one."""
        status = update.get("new_chat_member", {}).get("status")
        if status in ("left", "kicked"):
            await self.db.execute("DELETE FROM blbot_join_requests WHERE chat_id=$1 AND user_id=$2",
                str(update["chat"]["id"]), int(update["new_chat_member"]["user"]["id"]))

    async def cleanup(self):
        while True:
            try:
                rows = await self.db.fetch("""SELECT chat_id,message_id,attempts FROM blbot_sent_media
                    WHERE delete_at<=NOW() AND (next_try_at IS NULL OR next_try_at<=NOW())
                    ORDER BY delete_at LIMIT 50""")
                for item in rows:
                    try:
                        await self.tg("deleteMessage", chat_id=item["chat_id"], message_id=item["message_id"])
                        await self.db.execute("DELETE FROM blbot_sent_media WHERE chat_id=$1 AND message_id=$2",
                                              item["chat_id"], item["message_id"])
                    except TelegramError as exc:
                        LOG.warning("DM deletion failed for %s: %s", item["chat_id"], exc)
                        if item["attempts"] >= 3:
                            await self.db.execute("DELETE FROM blbot_sent_media WHERE chat_id=$1 AND message_id=$2",
                                                  item["chat_id"], item["message_id"])
                        else:
                            await self.db.execute("""UPDATE blbot_sent_media SET attempts=attempts+1,
                                next_try_at=NOW()+INTERVAL '5 minutes' WHERE chat_id=$1 AND message_id=$2""",
                                                  item["chat_id"], item["message_id"])
                # Keep old tokens and join events from accumulating forever.
                await self.db.execute("DELETE FROM blbot_unlocks WHERE created_at<NOW()-INTERVAL '30 days'")
                await self.db.execute("DELETE FROM blbot_join_requests WHERE requested_at<NOW()-INTERVAL '25 hours'")
            except Exception:
                LOG.exception("Cleanup worker error")
            await asyncio.sleep(45)

    async def run(self):
        await self.setup()
        cleanup_task = asyncio.create_task(self.cleanup())
        saved_offset = await self.config("tg_update_offset", "0")
        offset = int(saved_offset) if saved_offset.isdigit() and int(saved_offset) > 0 else None
        # Persist Telegram's polling cursor so restarts do not replay old file uploads.
        # Deployment must run only one instance.
        try:
            while True:
                try:
                    payload = {"timeout": 30, "limit": 50,
                               "allowed_updates": ["message", "callback_query", "chat_join_request", "chat_member"]}
                    if offset is not None:
                        payload["offset"] = offset
                    updates = await self.tg("getUpdates", **payload)
                    for update in updates:
                        offset = int(update["update_id"]) + 1
                        try:
                            if "chat_join_request" in update:
                                await self.handle_join_request(update["chat_join_request"])
                            elif "chat_member" in update:
                                await self.handle_chat_member(update["chat_member"])
                            elif "callback_query" in update:
                                await self.handle_callback(update["callback_query"])
                            elif "message" in update:
                                await self.handle_message(update["message"])
                        except Exception:
                            LOG.exception("An update failed; continuing with next update")
                            # Do not print update bodies; they can contain one-time tokens or user media metadata.
                        finally:
                            await self.set_config("tg_update_offset", str(offset))
                except (TelegramError, aiohttp.ClientError, asyncio.TimeoutError):
                    LOG.exception("Polling error; retrying")
                    await asyncio.sleep(4)
        finally:
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)
            await self.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(FileBot().run())
    except KeyboardInterrupt:
        LOG.info("Stopped")
