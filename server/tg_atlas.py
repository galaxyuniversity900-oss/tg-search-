#!/usr/bin/env python3
"""
TG-Atlas - a Telegram discovery engine (Termux / Linux / macOS / Windows).

What it does
  search   Live global search (channels, groups, bots). Telegram has no
           "next page" for search, so the engine builds an adaptive query
           tree: whenever a query is "saturated" (Telegram returned its
           maximum), it is automatically split into narrower sub-queries
           (Latin, Arabic, Persian, Cyrillic, Turkish, Hindi, digits, emoji).
  crawl    Snowball crawler. From seed usernames it follows channel
           recommendations, @mentions / t.me links inside recent posts and
           forwarded-from channels. Resumable (queue stored in SQLite).
  refresh  Re-fetch stale entries: members, about, linked chat, growth history.
  posts    Search messages: globally, inside one chat, or across all of your
           own dialogs. Filters: media type, date range, channels only.
  serve    Local web interface (recommended). Run it, open the printed link,
           sign in with your phone number + the code Telegram sends you, then
           search, run auto-discovery and click results. Nothing leaves your
           device except normal Telegram traffic; the server listens on
           127.0.0.1 only and requires a random access token.
  connectors  (inside the serve UI) Add your own MCP servers by name + URL. The app
           discovers the server's sign-in, sends you to the provider's page to
           log in, receives the redirect on 127.0.0.1 and stores the token
           locally (tg_atlas_connectors.json, owner-only). Then list tools, run
           them, or search every connector at once.
  auto     Autonomous mode. Mines new keywords from what it already found,
           searches them (with adaptive splitting), crawls recommendations,
           and keeps results/links.html + results/links.txt updated with
           clickable t.me links. Runs until Ctrl-C or --minutes.
  export   Write the clickable links files from the local index on demand.
  find     Instant offline full-text search over everything indexed so far
           (FTS5), with filters, sorting by size / creation date / growth.
  stats    Summary of the local index.

Honest expectations
  Telegram caps every search at ~100 results and rate-limits username
  resolution hard. There is no way to fetch 1.7M entries instantly: coverage
  grows over days of polite crawling. Flood waits are handled automatically;
  if Telegram asks for a very long wait the crawl stops cleanly and resumes
  later from the saved queue. Use a secondary account and keep the default
  delays - aggressive crawling can get an account restricted.

Setup
  pip install telethon
  python tg_atlas.py serve --open      # easiest: everything from the browser UI
  python tg_atlas.py auto --queries crypto ai --seeds durov --open
  python tg_atlas.py search "crypto"
  python tg_atlas.py crawl --seeds durov telegram --depth 3
  python tg_atlas.py find "machine learning" --kind channel --min-members 1000
Login uses your phone number and the code Telegram sends you. api_id and
api_hash (from https://my.telegram.org) are required by Telegram for every
client and are asked for once, then stored in tg_atlas_config.json.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import csv
import json
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from html import escape
from urllib.parse import parse_qs, quote, urlencode, urlparse

try:
    from telethon import TelegramClient, errors, functions, types
except ImportError:
    sys.exit("Telethon is not installed.\n"
             "  Termux / PC : pip install telethon\n"
             "  Pydroid 3   : menu (top-left) > Pip > search 'telethon' > Install, then run again.")

CONFIG_FILE = "tg_atlas_config.json"
SESSION_FILE = "tg_atlas_session"
SATURATION = 80  # a search returning >= this many hits is split further

ALPHABETS = {
    "latin": "abcdefghijklmnopqrstuvwxyz",
    "arabic": "ابتثجحخدذرزسشصضطظعغفقكلمنهوي",
    "persian": "پچژگ",
    "cyrillic": "абвгдежзийклмнопрстуфхцчшщъыьэюя",
    "turkish": "çğıöşü",
    "hindi": "अआइईउऊएऐओऔकखगघचछजझटठडढणतथदधनपफबभमयरलवशषसह",
    "digits": "0123456789",
    "emoji": "🔥💰📈🎮🎵🎬📚⚽🤖💻🌍📰🎓🛒❤",
}
DEFAULT_ALPHA = "latin,arabic,cyrillic,digits"

SKIP_NAMES = {
    "joinchat", "addstickers", "addemoji", "proxy", "socks", "share", "c", "s",
    "iv", "login", "setlanguage", "boost", "addtheme", "addlist", "giftcode",
    "invoice", "bg", "confirmphone", "contact", "m", "web", "k", "a", "username",
}
MENTION_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?!\+)([A-Za-z][A-Za-z0-9_]{3,31})"
    r"|(?<![\w/])@([A-Za-z][A-Za-z0-9_]{3,31})"
)
SCRIPTS = [
    ("arabic", 0x0600, 0x06FF), ("cyrillic", 0x0400, 0x04FF),
    ("hebrew", 0x0590, 0x05FF), ("devanagari", 0x0900, 0x097F),
    ("thai", 0x0E00, 0x0E7F), ("cjk", 0x4E00, 0x9FFF),
    ("hangul", 0xAC00, 0xD7AF), ("latin", 0x0041, 0x024F),
]
MEDIA_FILTERS = {
    "photo": types.InputMessagesFilterPhotos, "video": types.InputMessagesFilterVideo,
    "document": types.InputMessagesFilterDocument, "url": types.InputMessagesFilterUrl,
    "voice": types.InputMessagesFilterVoice, "music": types.InputMessagesFilterMusic,
    "gif": types.InputMessagesFilterGif, "pinned": types.InputMessagesFilterPinned,
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS entities(
  id INTEGER PRIMARY KEY, kind TEXT, title TEXT, username TEXT, about TEXT,
  members INTEGER, created TEXT, linked_chat_id INTEGER, script TEXT,
  verified INTEGER, scam INTEGER, fake INTEGER, restricted INTEGER,
  source TEXT, first_seen TEXT, last_checked TEXT, detailed INTEGER DEFAULT 0);
CREATE INDEX IF NOT EXISTS ix_user ON entities(lower(username));
CREATE INDEX IF NOT EXISTS ix_members ON entities(members);
CREATE INDEX IF NOT EXISTS ix_created ON entities(created);
CREATE INDEX IF NOT EXISTS ix_first ON entities(first_seen);
CREATE TABLE IF NOT EXISTS history(entity_id INTEGER, ts TEXT, members INTEGER);
CREATE INDEX IF NOT EXISTS ix_hist ON history(entity_id, ts);
CREATE TABLE IF NOT EXISTS queue(
  username TEXT PRIMARY KEY, depth INTEGER, status TEXT DEFAULT 'pending',
  source TEXT, added TEXT);
CREATE TABLE IF NOT EXISTS queries(q TEXT PRIMARY KEY, hits INTEGER, done_at TEXT);
"""
FIELDS = ["kind", "title", "username", "about", "members", "created", "linked_chat_id",
          "script", "verified", "scam", "fake", "restricted", "source"]


LOGBUF = deque(maxlen=200)


def log(*a):
    LOGBUF.append(" ".join(str(x) for x in a))
    print(*a, file=sys.stderr, flush=True)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fmt_n(n):
    n = n or 0
    return f"{n / 1e6:.1f}M" if n >= 1e6 else f"{n / 1e3:.1f}K" if n >= 1e3 else str(n)


def detect_script(text):
    counts = {}
    for ch in text or "":
        if not ch.isalpha():
            continue
        o = ord(ch)
        for name, lo, hi in SCRIPTS:
            if lo <= o <= hi:
                counts[name] = counts.get(name, 0) + 1
                break
    return max(counts, key=counts.get) if counts else None


class StopCrawl(Exception):
    pass


# --------------------------------------------------------------------------- DB
class DB:
    def __init__(self, path):
        self.c = sqlite3.connect(path)
        self.c.row_factory = sqlite3.Row
        self.c.executescript(SCHEMA)
        try:
            self.c.execute("CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(title, about, username)")
            self.fts = True
        except sqlite3.OperationalError:
            self.fts = False

    def upsert(self, e, detailed=False):
        e = dict(e)
        e["script"] = detect_script(f"{e.get('title') or ''} {e.get('about') or ''}")
        cols = ["id"] + FIELDS
        upd = ",".join(f"{k}=COALESCE(excluded.{k},entities.{k})" for k in FIELDS)
        ts = now()
        self.c.execute(
            f"INSERT INTO entities({','.join(cols)},first_seen,last_checked,detailed) "
            f"VALUES({','.join('?' * len(cols))},?,?,?) ON CONFLICT(id) DO UPDATE SET {upd},"
            "last_checked=COALESCE(excluded.last_checked,entities.last_checked),"
            "detailed=MAX(entities.detailed,excluded.detailed)",
            [e.get(k) for k in cols] + [ts, ts if detailed else None, int(detailed)],
        )
        if self.fts:
            r = self.c.execute("SELECT title,about,username FROM entities WHERE id=?", (e["id"],)).fetchone()
            self.c.execute("DELETE FROM fts WHERE rowid=?", (e["id"],))
            self.c.execute("INSERT INTO fts(rowid,title,about,username) VALUES(?,?,?,?)",
                           (e["id"], r["title"] or "", r["about"] or "", r["username"] or ""))

    def add_history(self, eid, members):
        if members is None:
            return
        last = self.c.execute("SELECT ts FROM history WHERE entity_id=? ORDER BY ts DESC LIMIT 1", (eid,)).fetchone()
        if last and last["ts"] > (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat():
            return
        self.c.execute("INSERT INTO history VALUES(?,?,?)", (eid, now(), members))

    def enqueue(self, username, depth, source):
        u = (username or "").lower()
        if not u or u in SKIP_NAMES:
            return False
        if self.c.execute("SELECT 1 FROM entities WHERE lower(username)=? AND detailed=1", (u,)).fetchone():
            return False
        cur = self.c.execute("INSERT OR IGNORE INTO queue(username,depth,source,added) VALUES(?,?,?,?)",
                             (u, depth, source, now()))
        return cur.rowcount > 0

    def next_job(self):
        r = self.c.execute("SELECT username,depth FROM queue WHERE status='pending' ORDER BY depth,rowid LIMIT 1").fetchone()
        if r:
            self.c.execute("UPDATE queue SET status='running' WHERE username=?", (r["username"],))
        return r

    def finish(self, username, status):
        self.c.execute("UPDATE queue SET status=? WHERE username=?", (status, username))


# ------------------------------------------------------------------- Telegram
def to_row(ent, source):
    if isinstance(ent, types.Channel):
        uname = ent.username
        if not uname and getattr(ent, "usernames", None):
            uname = next((u.username for u in ent.usernames if u.active), None)
        return dict(id=ent.id, kind="channel" if ent.broadcast else "group", title=ent.title,
                    username=uname, members=getattr(ent, "participants_count", None),
                    created=ent.date.isoformat() if ent.date else None,
                    verified=int(bool(ent.verified)), scam=int(bool(ent.scam)),
                    fake=int(bool(ent.fake)), restricted=int(bool(ent.restricted)), source=source)
    if isinstance(ent, types.User) and ent.bot:
        return dict(id=-ent.id, kind="bot", title=ent.first_name or ent.username, username=ent.username,
                    members=getattr(ent, "bot_active_users", None), source=source)
    return None


class Ctx:
    """Shared client + polite, adaptive rate limiter."""

    def __init__(self, client, db, delay, max_flood):
        self.client, self.db = client, db
        self.base = self.delay = delay
        self.max_flood = max_flood
        self.stop = False
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def call(self, req):
        for _ in range(4):
            if self.stop:
                raise StopCrawl()
            async with self._lock:
                gap = self.delay - (time.monotonic() - self._last)
                if gap > 0:
                    await asyncio.sleep(gap)
                self._last = time.monotonic()
            try:
                res = await self.client(req)
                self.delay = max(self.base, self.delay * 0.97)  # speed back up slowly
                return res
            except errors.FloodWaitError as e:
                self.delay = min(self.delay * 1.5 + 0.5, 30)  # slow down permanently-ish
                if e.seconds > self.max_flood:
                    self.stop = True
                    log(f"Flood wait of {e.seconds}s exceeds --max-flood; stopping. Re-run later to resume.")
                    raise StopCrawl()
                log(f"Flood wait {e.seconds}s ...")
                await asyncio.sleep(e.seconds + 1)
        raise StopCrawl()


async def detail(ctx, ent, source):
    row = to_row(ent, source)
    if not row:
        return None
    if isinstance(ent, types.Channel):
        try:
            full = (await ctx.call(functions.channels.GetFullChannelRequest(channel=ent))).full_chat
            row.update(about=full.about or None, members=full.participants_count,
                       linked_chat_id=getattr(full, "linked_chat_id", None))
        except (errors.ChannelPrivateError, errors.ChannelInvalidError):
            pass
    ctx.db.upsert(row, detailed=True)
    ctx.db.add_history(row["id"], row.get("members"))
    return row


async def resolve(ctx, username):
    r = await ctx.call(functions.contacts.ResolveUsernameRequest(username=username))
    if isinstance(r.peer, types.PeerChannel):
        return next((c for c in r.chats if c.id == r.peer.channel_id), None)
    if isinstance(r.peer, types.PeerUser):
        return next((u for u in r.users if u.id == r.peer.user_id), None)
    return None


async def recommendations(ctx, ent):
    try:
        r = await ctx.call(functions.channels.GetChannelRecommendationsRequest(channel=ent))
    except (errors.RPCError, AttributeError, TypeError):
        return []
    return [c for c in r.chats if isinstance(c, types.Channel)]


async def scan_posts(ctx, ent, n, depth, source):
    found = 0
    try:
        async for m in ctx.client.iter_messages(ent, limit=n):
            for mm in MENTION_RE.finditer(m.message or ""):
                found += ctx.db.enqueue(mm.group(1) or mm.group(2), depth + 1, source)
            ch = getattr(m.forward, "chat", None) if m.forward else None
            if isinstance(ch, types.Channel):
                row = to_row(ch, "forward")
                ctx.db.upsert(row)
                found += ctx.db.enqueue(row["username"], depth + 1, source)
        await asyncio.sleep(ctx.delay)
    except errors.RPCError:
        pass
    return found


# --------------------------------------------------------------------- commands
def build_alphabet(spec):
    return [ch for name in spec.split(",") if name.strip() in ALPHABETS for ch in ALPHABETS[name.strip()]]


def show(rows):
    for r in rows:
        print(f"{(r['kind'] or '?')[:7]:7} {fmt_n(r['members']):>7}  @{(r['username'] or '-'):<28.28} "
              f"{(r['created'] or '')[:10]}  {(r['title'] or '')[:40]}")


def emit(rows, out):
    rows = [dict(r) for r in rows]
    if not out:
        return
    if out.endswith(".json"):
        json.dump(rows, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    else:
        with open(out, "w", newline="", encoding="utf-8-sig") as f:
            if rows:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader()
                w.writerows(rows)
    log(f"Saved {len(rows)} rows to {out}")


async def search_tree(ctx, root, alphabet, depth, budget, shallow=False):
    """Run `root`; split every saturated query into narrower ones (Telegram has no next page)."""
    todo, seen, hits, ran = deque([(root, 0)]), set(), {}, 0
    while todo and not ctx.stop:
        q, d = todo.popleft()
        if q in seen:
            continue
        seen.add(q)
        cached = ctx.db.c.execute("SELECT hits FROM queries WHERE q=?", (q,)).fetchone() if d > 0 else None
        if cached:
            n = cached["hits"]  # resume: skip the API call but keep expanding saturated queries
        else:
            if ran >= budget:
                log(f"Query budget ({budget}) reached for '{root}'. Re-run to continue.")
                break
            ran += 1
            res = await ctx.call(functions.contacts.SearchRequest(q=q, limit=100))
            n = 0
            for ent in list(res.chats) + list(res.users):
                row = to_row(ent, "search")
                if row:
                    ctx.db.upsert(row)
                    hits[row["id"]] = row
                    ctx.db.enqueue(row.get("username"), 0, "search")
                    n += 1
            ctx.db.c.execute("INSERT OR REPLACE INTO queries VALUES(?,?,?)", (q, n, now()))
            ctx.db.c.commit()
            log(f"[depth {d}] '{q}' -> {n} hits | unique this run: {len(hits)} | queries: {ran}")
        if not shallow and n >= SATURATION and d < depth:
            todo.extend(((q + c) if len(q) <= 2 else f"{q} {c}", d + 1) for c in alphabet)
    return hits, ran


async def cmd_search(ctx, args):
    hits, _ = await search_tree(ctx, args.query, build_alphabet(args.alpha), args.depth,
                                args.max_queries, args.shallow)
    rows = sorted(hits.values(), key=lambda r: r.get("members") or 0, reverse=True)
    show(rows[: args.limit or None])
    log(f"{len(hits)} unique results this run. Use 'find' or 'export' for everything in the index.")
    emit(rows, args.out)


async def cmd_crawl(ctx, args):
    db = ctx.db
    db.c.execute("UPDATE queue SET status='pending' WHERE status='running'")
    for s in args.seeds or []:
        db.enqueue(s.lstrip("@"), 0, "seed")
    if args.from_db:
        for r in db.c.execute("SELECT username FROM entities WHERE username IS NOT NULL AND detailed=0"):
            db.enqueue(r["username"], 0, "db")
    db.c.commit()
    stats = {"done": 0, "active": 0}

    async def process(u, depth):
        try:
            ent = await resolve(ctx, u)
            row = await detail(ctx, ent, "crawl") if ent else None
            if not row:
                db.finish(u, "skipped")
                return
            if depth < args.depth and isinstance(ent, types.Channel):
                for c in await recommendations(ctx, ent):
                    r = to_row(c, "recommended")
                    db.upsert(r)
                    db.enqueue(r["username"], depth + 1, "recommended")
                if args.scan_posts:
                    await scan_posts(ctx, ent, args.scan_posts, depth, "post-link")
            db.finish(u, "done")
        except (errors.UsernameNotOccupiedError, errors.UsernameInvalidError):
            db.finish(u, "missing")
        except StopCrawl:
            db.finish(u, "pending")
            raise
        except errors.RPCError:
            db.finish(u, "error")

    async def worker():
        while not ctx.stop and stats["done"] < args.max:
            job = db.next_job()
            if not job:
                if stats["active"] == 0:
                    return
                await asyncio.sleep(1)
                continue
            stats["active"] += 1
            try:
                await process(job["username"], job["depth"])
            except StopCrawl:
                return
            finally:
                stats["active"] -= 1
            stats["done"] += 1
            if stats["done"] % 20 == 0:
                db.c.commit()
                pend = db.c.execute("SELECT COUNT(*) FROM queue WHERE status='pending'").fetchone()[0]
                log(f"crawled {stats['done']} | queue {pend} | delay {ctx.delay:.1f}s")

    await asyncio.gather(*[worker() for _ in range(args.workers)])
    db.c.commit()
    log(f"Crawl finished: {stats['done']} processed. Run again to continue from the saved queue.")


async def cmd_refresh(ctx, args):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=args.older_than)).isoformat()
    rows = ctx.db.c.execute(
        "SELECT username FROM entities WHERE username IS NOT NULL AND kind IN('channel','group') "
        "AND (last_checked IS NULL OR last_checked<?) ORDER BY last_checked LIMIT ?", (cutoff, args.limit)).fetchall()
    sem, n = asyncio.Semaphore(args.workers), 0

    async def one(u):
        nonlocal n
        async with sem:
            try:
                ent = await resolve(ctx, u)
                if ent:
                    await detail(ctx, ent, "refresh")
                n += 1
                if n % 20 == 0:
                    ctx.db.c.commit()
                    log(f"refreshed {n}/{len(rows)}")
            except (errors.RPCError, StopCrawl):
                pass

    await asyncio.gather(*[one(r["username"]) for r in rows])
    ctx.db.c.commit()
    log(f"Refreshed {n} entries.")


async def posts_core(ctx, query, chat=None, in_dialogs=False, per_chat=20, limit=100, days=None,
                     media=None, channels_only=False):
    """Search messages globally, inside one chat, or across your own dialogs. Returns a list of dicts."""
    since = datetime.now(timezone.utc) - timedelta(days=days) if days else None
    flt = MEDIA_FILTERS.get(media) if media else None
    targets = [None]
    if chat:
        targets = [chat]
    elif in_dialogs:
        targets = [d.entity async for d in ctx.client.iter_dialogs()]
    out = []
    for tgt in targets:
        kw = {"search": query, "limit": limit if tgt is None or chat else per_chat}
        if flt:
            kw["filter"] = flt
        try:
            async for m in ctx.client.iter_messages(tgt, **kw):
                if since and m.date < since:
                    break
                c = m.chat
                if channels_only and not (isinstance(c, types.Channel) and c.broadcast):
                    continue
                u = getattr(c, "username", None)
                link = f"https://t.me/{u}/{m.id}" if u else (f"https://t.me/c/{c.id}/{m.id}" if c else None)
                out.append(dict(date=m.date.isoformat(), chat=getattr(c, "title", None) or getattr(c, "first_name", "") or "",
                                username=u, views=m.views, forwards=m.forwards, link=link,
                                text=(m.message or "").replace("\n", " ")[:300]))
                for mm in MENTION_RE.finditer(m.message or ""):
                    ctx.db.enqueue(mm.group(1) or mm.group(2), 0, "post-link")
        except errors.FloodWaitError as e:
            log(f"Flood wait {e.seconds}s - stopping early.")
            break
        except errors.RPCError as e:
            log(f"Skipped a chat: {type(e).__name__}")
    ctx.db.c.commit()
    return out


async def cmd_posts(ctx, args):
    out = await posts_core(ctx, args.query, args.chat, args.in_dialogs, args.per_chat, args.limit,
                           args.days, args.media, args.channels_only)
    for p in out[: args.show]:
        print(f"{p['date'][:16]}  {p['chat'][:25]:25}  {p['text'][:90]}\n    {p['link'] or ''}")
    log(f"{len(out)} messages found.")
    emit(out, args.out)


def find_rows(db, args):
    where, params = [], []
    sql = "SELECT e.* FROM entities e"
    if args.query:
        if db.fts:
            toks = re.findall(r"\w+", args.query, re.UNICODE)
            sql = "SELECT e.* FROM fts JOIN entities e ON e.id=fts.rowid"
            where.append("fts MATCH ?")
            params.append(" ".join(f'"{t}"*' for t in toks) or '""')
        else:
            where.append("(e.title LIKE ? OR e.about LIKE ? OR e.username LIKE ?)")
            params += [f"%{args.query}%"] * 3
    if args.kind:
        where.append("e.kind=?")
        params.append(args.kind)
    if args.min_members is not None:
        where.append("e.members>=?")
        params.append(args.min_members)
    if args.max_members is not None:
        where.append("e.members<=?")
        params.append(args.max_members)
    if args.script:
        where.append("e.script=?")
        params.append(args.script)
    if args.verified:
        where.append("e.verified=1")
    if args.hide_scam:
        where.append("COALESCE(e.scam,0)=0 AND COALESCE(e.fake,0)=0")
    if args.days:
        where.append("e.created>=?")
        params.append((datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat())
    if where:
        sql += " WHERE " + " AND ".join(where)
    if args.sort == "growth":
        since = (datetime.now(timezone.utc) - timedelta(days=args.growth_days)).isoformat()
        sql = sql.replace("SELECT e.*", (
            "SELECT e.*, (SELECT members FROM history h WHERE h.entity_id=e.id ORDER BY ts DESC LIMIT 1)"
            "-(SELECT members FROM history h WHERE h.entity_id=e.id AND ts>=? ORDER BY ts LIMIT 1) AS growth"), 1)
        params.insert(0, since)
        order = "growth DESC"
    else:
        order = {"members": "e.members DESC", "created": "e.created DESC",
                 "relevance": "rank" if args.query and db.fts else "e.members DESC"}[args.sort]
    return db.c.execute(f"{sql} ORDER BY {order} LIMIT ?", params + [args.limit]).fetchall()


def cmd_find(db, args):
    rows = find_rows(db, args)
    show(rows)
    log(f"{len(rows)} results.")
    emit(rows, args.out)


WORD_RE = re.compile(r"[^\W\d_]{3,24}")
STOP_WORDS = set("the and for with from you your this that are our all new".split())
USER_RE = re.compile(r"^[A-Za-z0-9_]{4,32}$")

HTML_TEMPLATE = """<!DOCTYPE html><html lang="en" dir="auto"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>TG-Atlas links</title>
<style>:root{--bg:#f6f7f9;--card:#fff;--tx:#1b1f24;--mut:#667085;--ac:#229ed9;--bd:#e3e6ea}
@media(prefers-color-scheme:dark){:root{--bg:#14171a;--card:#1e2328;--tx:#eceff2;--mut:#98a2b3;--bd:#2c333a}}
body{margin:0;background:var(--bg);color:var(--tx);font:16px/1.5 system-ui,sans-serif}main{max-width:760px;margin:auto;padding:14px}
.bar{display:flex;gap:8px;flex-wrap:wrap;position:sticky;top:0;background:var(--bg);padding:8px 0}
input,select,button{font:inherit;color:var(--tx);background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:8px 10px}
input{flex:1;min-width:140px}.c{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:10px 12px;margin:8px 0}
.c a{color:var(--ac);font-weight:600;text-decoration:none;overflow-wrap:anywhere}.m{color:var(--mut);font-size:13px}.d{font-size:14px;overflow-wrap:anywhere}</style></head>
<body><main><h1>TG-Atlas: __COUNT__ links</h1><div class="m">Generated __DATE__</div>
<div class="bar"><input id="q" placeholder="Filter..." dir="auto"><select id="k"><option value="">All</option><option>channel</option><option>group</option><option>bot</option></select>
<select id="s"><option value="m">Most members</option><option value="c">Newest</option></select></div>
<div id="n" class="m"></div><div id="o"></div><button id="more">Show more</button></main>
<script>const D=__DATA__;let shown=100;
const o=document.getElementById('o'),q=document.getElementById('q'),k=document.getElementById('k'),s=document.getElementById('s'),n=document.getElementById('n'),more=document.getElementById('more');
function f(x){return x>=1e6?(x/1e6).toFixed(1)+'M':x>=1e3?(x/1e3).toFixed(1)+'K':String(x||0)}
function render(){const t=q.value.trim().toLowerCase();
let a=D.filter(r=>(!k.value||r[2]===k.value)&&(!t||(r[0]+' '+r[1]+' '+r[5]).toLowerCase().includes(t)));
a.sort(s.value==='c'?(x,y)=>(y[4]||'').localeCompare(x[4]||''):(x,y)=>(y[3]||0)-(x[3]||0));
n.textContent=a.length+' results';o.textContent='';
for(const r of a.slice(0,shown)){const c=document.createElement('div');c.className='c';
const l=document.createElement('a');l.href='https://t.me/'+r[1];l.target='_blank';l.rel='noopener';l.textContent=r[0]||('@'+r[1]);
const m=document.createElement('div');m.className='m';m.textContent=[r[2],f(r[3])+' members','@'+r[1],(r[4]||'').slice(0,10)].filter(Boolean).join(' | ');
c.append(l,m);if(r[5]){const d=document.createElement('div');d.className='d';d.textContent=r[5];c.append(d)}o.append(c)}
more.style.display=a.length>shown?'':'none'}
more.onclick=()=>{shown+=200;render()};[q,k,s].forEach(e=>e.oninput=()=>{shown=100;render()});render();</script></body></html>"""


def export_args(a=None, **over):
    d = dict(query=None, kind=None, min_members=None, max_members=None, script=None, verified=False,
             hide_scam=True, days=None, growth_days=7, export_limit=50000, out_dir="results", open=False)
    if a is not None:
        d.update({k: v for k, v in vars(a).items() if k in d})
    d.update(over)
    return argparse.Namespace(**d)


def collect_export(db, a):
    big = 10 ** 9
    rows = {r["id"]: r for r in find_rows(db, argparse.Namespace(**vars(a), sort="members", limit=a.export_limit or big))}
    if a.export_limit:  # also include the newest entries so fresh channels are not cut off
        extra = argparse.Namespace(**vars(a), sort="created", limit=max(1000, a.export_limit // 4))
        for r in find_rows(db, extra):
            rows.setdefault(r["id"], r)
    return [r for r in rows.values() if r["username"] and USER_RE.match(r["username"])]


def write_exports(db, a):
    """Write clickable results: links.html (searchable page) and links.txt (title | url | members)."""
    rows = collect_export(db, a)
    os.makedirs(a.out_dir, exist_ok=True)
    data = [[r["title"] or "", r["username"], r["kind"], r["members"] or 0, (r["created"] or "")[:10],
             (r["about"] or "")[:160].replace("\n", " ")] for r in rows]
    blob = json.dumps(data, ensure_ascii=False).replace("</", "<\\/").replace("\u2028", " ").replace("\u2029", " ")
    page = HTML_TEMPLATE.replace("__COUNT__", str(len(data))).replace("__DATE__", now()).replace("__DATA__", blob)
    p_html, p_txt = os.path.join(a.out_dir, "links.html"), os.path.join(a.out_dir, "links.txt")
    with open(p_html, "w", encoding="utf-8") as f:
        f.write(page)
    with open(p_txt, "w", encoding="utf-8") as f:
        for r in sorted(rows, key=lambda r: r["members"] or 0, reverse=True):
            f.write(f"{(r['title'] or r['username']).strip()} | https://t.me/{r['username']} | {fmt_n(r['members'])}\n")
    return p_html, p_txt


def open_file(path):
    for cmd in ("termux-open", "xdg-open", "open"):
        if shutil.which(cmd):
            subprocess.Popen([cmd, path])
            return
    if hasattr(os, "startfile"):
        os.startfile(path)


def cmd_export(db, args):
    paths = write_exports(db, args)
    log("Links ready: " + ", ".join(paths))
    if args.open:
        open_file(paths[0])


def mine_keywords(db, state, k):
    """Self-feeding vocabulary: frequent words from titles/descriptions already found become new
    queries, interleaved across scripts so one language does not dominate."""
    if state["cursor"] is None:
        rows = db.c.execute("SELECT title,about,first_seen FROM entities ORDER BY first_seen DESC LIMIT ?",
                            (state["window"],)).fetchall()
    else:
        rows = db.c.execute("SELECT title,about,first_seen FROM entities WHERE first_seen>? "
                            "ORDER BY first_seen LIMIT ?", (state["cursor"], state["window"])).fetchall()
    for r in rows:
        state["words"].update(set(WORD_RE.findall(f"{r['title'] or ''} {r['about'] or ''}".lower())))
    if rows:
        state["cursor"] = max(r["first_seen"] for r in rows)
    if len(state["words"]) > 200000:
        state["words"] = Counter(dict(state["words"].most_common(50000)))
    buckets = {}
    for w, _ in state["words"].most_common(5000):
        if w in STOP_WORDS or w in state["tried"]:
            continue
        if db.c.execute("SELECT 1 FROM queries WHERE q=?", (w,)).fetchone():
            state["tried"].add(w)
            continue
        buckets.setdefault(detect_script(w) or "other", []).append(w)
    out = []
    while len(out) < k and any(buckets.values()):
        for b in list(buckets.values()):
            if b and len(out) < k:
                out.append(b.pop(0))
    state["tried"].update(out)
    return out


async def cmd_auto(ctx, args):
    db, alphabet = ctx.db, build_alphabet(args.alpha)
    for sd in args.seeds or []:
        db.enqueue(sd.lstrip("@"), 0, "seed")
    todo = deque(args.queries or [])
    state = {"cursor": None, "words": Counter(), "tried": set(), "window": 50000}
    started, last_export, cycle, opened = time.monotonic(), 0.0, 0, False
    ea = export_args(args, hide_scam=not args.keep_scam)
    total = lambda: db.c.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    try:
        while not ctx.stop and not (args.minutes and time.monotonic() - started > args.minutes * 60) \
                and not (args.max_cycles and cycle >= args.max_cycles):
            cycle += 1
            if len(todo) < args.queries_per_cycle:
                todo.extend(mine_keywords(db, state, args.queries_per_cycle))
            batch = [todo.popleft() for _ in range(min(args.queries_per_cycle, len(todo)))]
            before = total()
            for q in batch:
                await search_tree(ctx, q, alphabet, args.depth, args.tree_budget)
            await cmd_crawl(ctx, argparse.Namespace(seeds=None, from_db=False, depth=args.crawl_depth,
                                                    max=args.crawl_per_cycle, workers=args.workers,
                                                    scan_posts=args.scan_posts))
            pend = db.c.execute("SELECT COUNT(*) FROM queue WHERE status='pending'").fetchone()[0]
            log(f"== cycle {cycle}: +{total() - before} new | index {total()} | queue {pend} | "
                f"queries this cycle: {', '.join(batch[:5])}{'...' if len(batch) > 5 else ''}")
            if time.monotonic() - last_export >= args.export_every:
                paths = write_exports(db, ea)
                last_export = time.monotonic()
                log("Links updated: " + ", ".join(paths))
                if args.open and not opened:
                    open_file(paths[0])
                    opened = True
            if not batch and pend == 0:
                log("Nothing left to explore. Add --queries or --seeds, or re-run later.")
                break
    finally:
        db.c.commit()
        log("Final links: " + ", ".join(write_exports(db, ea)))


UI_HTML = r"""<!DOCTYPE html><html lang="en" dir="auto"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>TG-Atlas</title>
<style>:root{--bg:#f6f7f9;--card:#fff;--tx:#1b1f24;--mut:#667085;--ac:#229ed9;--bd:#e3e6ea}
@media(prefers-color-scheme:dark){:root{--bg:#14171a;--card:#1e2328;--tx:#eceff2;--mut:#98a2b3;--bd:#2c333a}}
[hidden]{display:none!important}
body{margin:0;background:var(--bg);color:var(--tx);font:16px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}
main{max-width:720px;margin:auto;padding:14px}h1{font-size:20px;margin:4px 0 10px}h2,h3{margin:0 0 8px;font-size:16px}
.row{display:flex;gap:8px;margin:8px 0;flex-wrap:wrap}.tabs{overflow-x:auto;flex-wrap:nowrap}
input,select,button,pre{font:inherit;color:var(--tx);background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:9px 12px}
input{flex:1;min-width:120px}button{cursor:pointer}button.p,.chip.on{background:var(--ac);color:#fff;border-color:var(--ac)}
button:disabled{opacity:.5}.chip{border-radius:99px;padding:6px 12px;font-size:14px;white-space:nowrap}
.card{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:12px;margin:8px 0}
.card a{color:var(--ac);font-weight:600;text-decoration:none;overflow-wrap:anywhere}.m{color:var(--mut);font-size:13px}
.d{font-size:14px;margin:4px 0;overflow-wrap:anywhere}pre{font-size:12px;max-height:180px;overflow:auto;white-space:pre-wrap;margin:8px 0 0}</style></head>
<body><main><h1>TG-Atlas <span class="m" id="who"></span></h1>
<section id="setup" hidden><div class="card"><h2>Step 1: Telegram API keys</h2>
<p class="m">Create them once at my.telegram.org (API development tools). They stay on this device.</p>
<div class="row"><input id="aid" inputmode="numeric" placeholder="api_id"><input id="ahash" placeholder="api_hash"></div>
<button class="p" id="saveCfg">Save</button></div></section>
<section id="login" hidden><div class="card"><h2>Sign in to your Telegram</h2>
<div id="stPhone" class="row"><input id="phone" type="tel" dir="ltr" placeholder="+201234567890"><button class="p" id="sendCode">Send code</button></div>
<div id="stCode" class="row" hidden><input id="code" inputmode="numeric" dir="ltr" placeholder="Code from Telegram"><button class="p" id="signIn">Sign in</button></div>
<div id="stPass" class="row" hidden><input id="pass" type="password" placeholder="Two-step verification password"><button class="p" id="sendPass">Continue</button></div>
<div id="loginMsg" class="m"></div></div></section>
<section id="app" hidden>
<div class="row"><input id="q" dir="auto" placeholder="Any language or emoji. Type to search your index instantly"><button class="p" id="live">Search Telegram</button></div>
<div class="row tabs" id="tabs"></div>
<div class="row"><select id="sort"><option value="relevance">Most relevant</option><option value="members">Most members</option><option value="created">Newest</option><option value="growth">Fastest growing</option></select>
<input id="minm" type="number" placeholder="Min members"><select id="days"><option value="">Any age</option><option value="7">Created last 7 days</option><option value="30">Last 30 days</option><option value="365">Last year</option></select></div>
<div id="info" class="m"></div><div id="out"></div><div class="row"><button id="more" hidden>Show more</button></div>
<div class="card"><h3>Auto discovery</h3><p class="m" id="stats"></p>
<div class="row"><input id="akw" dir="auto" placeholder="Start keywords, comma separated (optional)"><input id="amin" type="number" value="60" style="max-width:90px" title="minutes"></div>
<div class="row"><button class="p" id="astart">Start</button><button id="astop">Stop</button></div><pre id="log"></pre></div>
<div class="card"><h3>Custom connectors</h3>
<p class="m">Add any MCP server by name and URL. You sign in on the provider's own page and come back here automatically.</p>
<div class="row"><input id="cnm" placeholder="Name, e.g. My MCP server"><input id="cur" type="url" dir="ltr" placeholder="https://example.com/mcp"></div>
<details><summary class="m">Advanced (optional)</summary><div class="row"><input id="ccid" placeholder="OAuth client ID"><input id="ccs" type="password" placeholder="OAuth client secret"></div></details>
<div class="row"><button class="p" id="cadd">Add connector</button></div><div id="clist"></div>
<div class="row"><input id="cq" dir="auto" placeholder="Search all connectors"><input id="cfn" value="search" style="max-width:100px" title="tool name contains"><button id="cgo">Search all</button></div>
<div id="cout"></div></div>
<div class="row"><button id="exp">Update links page</button><button id="logout">Log out</button></div></section></main>
<script>const T=new URLSearchParams(location.search).get('t')||'';const $=id=>document.getElementById(id);
async function api(p,b){try{const r=await fetch(p,{method:b?'POST':'GET',headers:{'X-Token':T,'Content-Type':'application/json'},body:b?JSON.stringify(b):undefined});return await r.json()}catch(e){return{error:'Cannot reach the local server'}}}
const vis=(id,on)=>{$(id).hidden=!on};let st=null,tab='all',offset=0,deb=null;
const fmt=n=>{n=n||0;return n>=1e6?(n/1e6).toFixed(1)+'M':n>=1e3?(n/1e3).toFixed(1)+'K':''+n};
const safe=u=>/^https:\/\/t\.me\//.test(u||'')?u:null;
for(const [k,v] of [['all','All'],['channel','Channels'],['group','Groups'],['bot','Bots'],['post','Posts']]){const b=document.createElement('button');b.className='chip'+(k===tab?' on':'');b.textContent=v;
b.onclick=()=>{tab=k;[...$('tabs').children].forEach(c=>c.classList.toggle('on',c===b));load(true)};$('tabs').append(b)}
async function refresh(){st=await api('/api/status');if(st.error){$('loginMsg').textContent=st.error;return}
vis('setup',!st.configured);vis('login',st.configured&&!st.authorized);vis('app',st.authorized);
vis('stPhone',st.stage==='phone');vis('stCode',st.stage==='code');vis('stPass',st.stage==='password');
if(!st.authorized)return;$('who').textContent=st.me||'';
$('stats').textContent=st.counts.entities+' indexed | '+st.counts.queue+' queued | '+(st.job.running?'running: '+st.job.name:'idle');
$('log').textContent=st.log.join('\n');$('log').scrollTop=$('log').scrollHeight;$('astart').disabled=st.job.running;$('astop').disabled=!st.job.running}
function card(it){const c=document.createElement('div');c.className='card';const l=document.createElement('a');const u=safe(it.link);
if(u){l.href=u;l.target='_blank';l.rel='noopener'}l.textContent=it.title||it.chat||it.username||'(untitled)';
const m=document.createElement('div');m.className='m';
m.textContent=it.text!==undefined?[it.chat,(it.date||'').slice(0,10),it.views?fmt(it.views)+' views':''].filter(Boolean).join(' | '):[it.kind,fmt(it.members)+' members',it.username?'@'+it.username:'',(it.created||'').slice(0,10)].filter(Boolean).join(' | ');
c.append(l,m);const t=it.text!==undefined?it.text:it.about;if(t){const d=document.createElement('div');d.className='d';d.dir='auto';d.textContent=t;c.append(d)}return c}
async function load(reset){if(reset){offset=0;$('out').textContent=''}
if(tab==='post'){const q=$('q').value.trim();vis('more',false);if(!q){$('info').textContent='Type a query, then press Search Telegram';return}
$('info').textContent='Searching posts...';const r=await api('/api/posts',{query:q,limit:60});if(r.error){$('info').textContent=r.error;return}
$('out').textContent='';r.items.forEach(i=>$('out').append(card(i)));$('info').textContent=r.items.length+' posts';return}
const p=new URLSearchParams({q:$('q').value.trim(),sort:$('sort').value,offset:offset,limit:30});
if(tab!=='all')p.set('kind',tab);if($('minm').value)p.set('min',$('minm').value);if($('days').value)p.set('days',$('days').value);
const r=await api('/api/results?'+p);if(r.error){$('info').textContent=r.error;return}
r.items.forEach(i=>$('out').append(card(i)));offset+=r.items.length;vis('more',r.more);$('info').textContent=offset+' shown (local index)'}
const msg=t=>{$('loginMsg').textContent=t};
$('saveCfg').onclick=async()=>{const r=await api('/api/config',{api_id:$('aid').value,api_hash:$('ahash').value});if(r.error)alert(r.error);refresh()};
$('sendCode').onclick=async()=>{msg('Sending...');const r=await api('/api/send_code',{phone:$('phone').value});msg(r.error||'Code sent. Check your Telegram app.');refresh()};
$('signIn').onclick=async()=>{const r=await api('/api/sign_in',{code:$('code').value});msg(r.error||'');await refresh();if(st.authorized)load(true)};
$('sendPass').onclick=async()=>{const r=await api('/api/password',{password:$('pass').value});msg(r.error||'');await refresh();if(st.authorized)load(true)};
$('live').onclick=async()=>{const q=$('q').value.trim();if(!q)return;if(tab==='post'){load(true);return}
const r=await api('/api/search',{query:q});$('info').textContent=r.error||'Searching Telegram. Results appear below as they arrive.';refresh()};
$('astart').onclick=async()=>{const r=await api('/api/auto',{queries:$('akw').value,minutes:+$('amin').value||60});if(r.error)alert(r.error);refresh()};
$('astop').onclick=async()=>{await api('/api/stop',{});refresh()};
$('exp').onclick=async()=>{const r=await api('/api/export',{});if(r.error){alert(r.error);return}window.open('/results/links.html?t='+encodeURIComponent(T),'_blank')};
$('logout').onclick=async()=>{if(confirm('Log out of Telegram on this device?')){await api('/api/logout',{});refresh()}};
$('q').oninput=()=>{clearTimeout(deb);deb=setTimeout(()=>{if(tab!=='post')load(true)},250)};
$('q').onkeydown=e=>{if(e.key==='Enter')$('live').click()};
$('more').onclick=()=>load(false);$('sort').onchange=$('minm').oninput=$('days').onchange=()=>load(true);
const E=t=>document.createElement(t);
function conShow(el,x,title){el.textContent='';if(title){const h=E('div');h.className='m';h.textContent=title;el.append(h)}
 if(x.error){const d=E('div');d.textContent=x.error;el.append(d);return}
 for(const u of x.links||[]){if(!/^https?:\/\//.test(u))continue;const a=E('a');a.href=u;a.target='_blank';a.rel='noopener noreferrer';a.textContent=u.slice(0,80);a.style.display='block';el.append(a)}
 const p=E('pre');p.textContent=x.text||'';el.append(p)}
async function conLoad(){const r=await api('/api/connectors');const box=$('clist');box.textContent='';
 for(const c of r.items||[]){const d=E('div');d.className='m';const s=E('span');s.textContent=c.name+' | '+(c.connected?'connected':'needs sign-in')+' ';d.append(s);
  const mk=(t,f)=>{const b=E('button');b.className='chip';b.textContent=t;b.onclick=f;return b};
  if(!c.connected)d.append(mk('Sign in',()=>conAdd(c.name,c.url)));
  d.append(mk('Tools',()=>conTools(c.name)),mk('Remove',async()=>{if(confirm('Remove '+c.name+'?')){await api('/api/connectors/remove',{name:c.name});conLoad()}}));box.append(d)}}
async function conAdd(name,url){name=name||$('cnm').value.trim();url=url||$('cur').value.trim();
 const r=await api('/api/connectors/add',{name:name,url:url,client_id:$('ccid').value.trim(),client_secret:$('ccs').value});
 if(r.error){alert(r.error);return}
 if(r.auth_url&&/^https?:\/\//.test(r.auth_url)){location.href=r.auth_url;return}
 alert('Connected: '+name);conLoad()}
async function conTools(name){const r=await api('/api/connectors/tools',{name:name}),o=$('cout');o.textContent='';
 if(r.error){o.textContent=r.error;return}
 const sel=E('select');for(const t of r.tools){const op=E('option');op.value=t.name;op.textContent=t.name+(t.readOnly?'':' (may change data)');sel.append(op)}
 const ta=E('input');ta.value='{}';ta.placeholder='Arguments as JSON';const go=E('button');go.className='p';go.textContent='Run';const out=E('div');
 go.onclick=async()=>{const t=r.tools.find(x=>x.name===sel.value);let a;try{a=JSON.parse(ta.value||'{}')}catch(e){out.textContent='Arguments must be valid JSON.';return}
  if(!t.readOnly&&!confirm('This tool may change data on '+name+'. Run it?'))return;out.textContent='Running...';
  conShow(out,await api('/api/connectors/call',{name:name,tool:sel.value,args:a}))};
 const row=E('div');row.className='row';row.append(sel,ta,go);o.append(row,out)}
$('cadd').onclick=()=>conAdd();
$('cgo').onclick=async()=>{const q=$('cq').value.trim();if(!q)return;const o=$('cout');o.textContent='Searching...';
 const r=await api('/api/connectors/search',{query:q,contains:$('cfn').value});o.textContent='';
 if(r.error){o.textContent=r.error;return}for(const it of r.items||[]){const c=E('div');c.className='card';o.append(c);conShow(c,it,it.name+(it.tool?' | '+it.tool:''))}};
const cp=new URLSearchParams(location.search);if(cp.get('connected')){alert('Connected: '+cp.get('connected'));history.replaceState(null,'','/?t='+encodeURIComponent(T))}
conLoad();
refresh().then(()=>{if(st&&st.authorized)load(true)});
setInterval(async()=>{await refresh();if(st&&st.authorized&&st.job.running&&tab!=='post'&&offset<=30)load(true)},3000);</script></body></html>"""

LOGIN_ERRORS = {
    "PhoneNumberInvalidError": "That phone number is not valid. Use the international format, e.g. +201234567890.",
    "PhoneNumberBannedError": "Telegram reports this number as banned.",
    "PhoneCodeInvalidError": "Wrong code. Check it and try again.",
    "PhoneCodeExpiredError": "The code expired. Press Send code again.",
    "PhoneCodeEmptyError": "Enter the code first.",
    "PasswordHashInvalidError": "Wrong two-step verification password.",
    "ApiIdInvalidError": "api_id / api_hash are not valid. Check them at my.telegram.org.",
}


def read_config():
    """API keys from env vars or the external file tg_atlas_config.json (placeholders are ignored)."""
    try:
        if os.environ.get("TG_API_ID") and os.environ.get("TG_API_HASH"):
            api_id, api_hash = int(os.environ["TG_API_ID"]), os.environ["TG_API_HASH"].strip()
        elif os.path.exists(CONFIG_FILE):
            c = json.load(open(CONFIG_FILE))
            api_id, api_hash = int(c["api_id"]), str(c["api_hash"]).strip()
        else:
            return None
    except (ValueError, KeyError, TypeError, OSError):
        return None
    if api_id > 0 and re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
        return api_id, api_hash
    return None


def save_config(api_id, api_hash):
    json.dump({"api_id": api_id, "api_hash": api_hash}, open(CONFIG_FILE, "w"))
    os.chmod(CONFIG_FILE, 0o600)


def auto_args(**over):
    d = dict(seeds=[], queries=[], minutes=60, max_cycles=0, queries_per_cycle=20, depth=2, tree_budget=60,
             crawl_per_cycle=40, crawl_depth=3, workers=2, scan_posts=0, alpha=DEFAULT_ALPHA, export_every=300,
             out_dir="results", export_limit=50000, kind=None, min_members=None, keep_scam=False, open=False)
    d.update(over)
    return argparse.Namespace(**d)


def num(v, default, lo, hi):
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return default


CONN_FILE = "tg_atlas_connectors.json"
MCP_VERSION = "2025-06-18"
INIT_PARAMS = {"protocolVersion": MCP_VERSION, "capabilities": {},
               "clientInfo": {"name": "tg-atlas", "version": "1.0"}}
READ_RE = re.compile(r"^(search|list|get|find|read|query|fetch|browse)", re.I)
URL_RE = re.compile(r"https?://[^\s\"'<>\\)\]]+")


class McpError(Exception):
    pass


class AuthNeeded(Exception):
    def __init__(self, headers=None):
        super().__init__("login required")
        self.headers = headers or {}


def hget(h, name):
    for k, v in (h or {}).items():
        if k.lower() == name:
            return v
    return None


def safe_url(u):
    p = urlparse(u or "")
    return bool(p.netloc) and (p.scheme == "https" or (p.scheme == "http" and p.hostname in ("127.0.0.1", "localhost")))


def http(method, url, data=None, headers=None, timeout=30):
    """Blocking HTTP helper (run it with asyncio.to_thread). Returns (status, headers, body)."""
    if not safe_url(url):
        raise McpError("Blocked URL: only https:// addresses are allowed.")
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read(5_000_000)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read(1_000_000)
    except (urllib.error.URLError, OSError) as e:
        raise McpError(f"Cannot reach the server: {getattr(e, 'reason', e)}")


def b64url(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def parse_sse(body):
    out = []
    for block in body.decode("utf-8", "replace").replace("\r\n", "\n").split("\n\n"):
        data = "\n".join(ln[5:].lstrip() for ln in block.split("\n") if ln.startswith("data:"))
        if data:
            try:
                out.append(json.loads(data))
            except ValueError:
                pass
    return out


def discover_oauth(url, www_auth):
    """Find the sign-in (OAuth) endpoints of an MCP server, following the MCP authorization flow."""
    u = urlparse(url)
    origin = f"{u.scheme}://{u.netloc}"
    meta_url = (re.search(r'resource_metadata="([^"]+)"', www_auth or "") or [None, None])[1]
    as_url = origin
    for cand in (meta_url, origin + "/.well-known/oauth-protected-resource"):
        if cand and safe_url(cand):
            st, _, b = http("GET", cand)
            if st == 200:
                try:
                    servers = json.loads(b).get("authorization_servers") or []
                except ValueError:
                    servers = []
                if servers:
                    as_url = servers[0]
                    break
    for path in ("/.well-known/oauth-authorization-server", "/.well-known/openid-configuration"):
        st, _, b = http("GET", as_url.rstrip("/") + path)
        if st == 200:
            try:
                meta = json.loads(b)
            except ValueError:
                continue
            if meta.get("authorization_endpoint") and meta.get("token_endpoint"):
                return meta
    raise McpError("Could not find this server's sign-in (OAuth) settings.")


def result_view(res):
    if isinstance(res, dict) and isinstance(res.get("content"), list):
        text = "\n".join(b.get("text", "") for b in res["content"] if isinstance(b, dict) and b.get("type") == "text")
        if not text:
            text = json.dumps(res, ensure_ascii=False, indent=1)
    else:
        text = json.dumps(res, ensure_ascii=False, indent=1)
    return {"text": text[:8000], "links": list(dict.fromkeys(URL_RE.findall(text)))[:30],
            "is_error": bool(isinstance(res, dict) and res.get("isError"))}


class ConnectorStore:
    """Custom MCP connectors: add by name + URL, sign in through the provider's own page, then use."""

    def __init__(self, port):
        self.port, self.pending, self.ready, self.sessions = port, {}, set(), {}
        self.data = json.load(open(CONN_FILE)) if os.path.exists(CONN_FILE) else {}

    def save(self):
        with open(CONN_FILE, "w") as f:
            json.dump(self.data, f)
        os.chmod(CONN_FILE, 0o600)

    @property
    def redirect(self):
        return f"http://127.0.0.1:{self.port}/oauth/callback"

    def status(self):
        return [{"name": n, "url": c["url"], "connected": bool(c.get("tokens") or c.get("no_auth"))}
                for n, c in self.data.items()]

    def remove(self, name):
        self.data.pop(name, None)
        self.ready.discard(name)
        self.sessions.pop(name, None)
        self.save()

    # -- JSON-RPC over streamable HTTP ------------------------------------
    def _headers(self, name):
        h = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
             "MCP-Protocol-Version": MCP_VERSION}
        tok = (self.data[name].get("tokens") or {}).get("access_token")
        if tok:
            h["Authorization"] = "Bearer " + tok
        if self.sessions.get(name):
            h["Mcp-Session-Id"] = self.sessions[name]
        return h

    def rpc(self, name, method, params=None, notify=False):
        for attempt in range(3):
            if method not in ("initialize", "notifications/initialized") and name not in self.ready:
                self._initialize(name)
            body = {"jsonrpc": "2.0", "method": method}
            if params is not None:
                body["params"] = params
            if not notify:
                body["id"] = secrets.randbelow(10 ** 9)
            st, h, b = http("POST", self.data[name]["url"], json.dumps(body).encode(), self._headers(name))
            if st == 401:
                if attempt == 0 and self._refresh(name):
                    continue
                raise AuthNeeded(h)
            if st == 404 and name in self.ready and method != "initialize":
                self.ready.discard(name)
                self.sessions.pop(name, None)
                continue
            if st >= 400:
                raise McpError(f"The server answered HTTP {st}.")
            if hget(h, "mcp-session-id"):
                self.sessions[name] = hget(h, "mcp-session-id")
            if notify:
                return None
            msgs = parse_sse(b) if "text/event-stream" in (hget(h, "content-type") or "") else [json.loads(b or b"null")]
            for m in msgs:
                if isinstance(m, dict) and m.get("id") == body["id"]:
                    if "error" in m:
                        raise McpError(str((m["error"] or {}).get("message", "Server error")))
                    return m.get("result")
            raise McpError("The server sent no answer.")
        raise McpError("Could not complete the request.")

    def _initialize(self, name):
        self.rpc(name, "initialize", INIT_PARAMS)
        self.ready.add(name)
        self.rpc(name, "notifications/initialized", notify=True)

    def tools(self, name):
        out, cursor = [], None
        for _ in range(10):
            res = self.rpc(name, "tools/list", {"cursor": cursor} if cursor else None) or {}
            for t in res.get("tools", []):
                ro = bool((t.get("annotations") or {}).get("readOnlyHint")) or bool(READ_RE.match(t.get("name", "")))
                out.append({"name": t.get("name", ""), "description": (t.get("description") or "")[:300],
                            "readOnly": ro, "schema": t.get("inputSchema") or {}})
            cursor = res.get("nextCursor")
            if not cursor:
                break
        return out

    def call(self, name, tool, args):
        return self.rpc(name, "tools/call", {"name": tool, "arguments": args})

    # -- sign-in (OAuth 2.1 with PKCE, dynamic client registration) --------
    def begin(self, name, url, client_id=None, client_secret=None):
        self.data[name] = {"url": url, "tokens": None,
                           "client": {"id": client_id, "secret": client_secret} if client_id else None}
        self.ready.discard(name)
        self.sessions.pop(name, None)
        try:
            self._initialize(name)  # works without login?
            self.data[name]["no_auth"] = True
            self.save()
            return {"connected": True}
        except AuthNeeded as e:
            www = hget(e.headers, "www-authenticate")
        except McpError:
            self.data.pop(name, None)
            raise
        try:
            meta = discover_oauth(url, www)
            c = self.data[name]
            if not c["client"]:
                reg = meta.get("registration_endpoint")
                if not reg:
                    raise McpError("This server needs a pre-registered OAuth client. Open Advanced and enter a client ID.")
                st, _, b = http("POST", reg, json.dumps({
                    "client_name": "TG-Atlas", "redirect_uris": [self.redirect], "grant_types": ["authorization_code", "refresh_token"],
                    "response_types": ["code"], "token_endpoint_auth_method": "none"}).encode(), {"Content-Type": "application/json"})
                info = json.loads(b or b"{}")
                if st >= 400 or not info.get("client_id"):
                    raise McpError("The server refused automatic client registration.")
                c["client"] = {"id": info["client_id"], "secret": info.get("client_secret")}
            c["token_endpoint"] = meta["token_endpoint"]
            verifier, state = secrets.token_urlsafe(48), secrets.token_urlsafe(16)
            params = {"response_type": "code", "client_id": c["client"]["id"], "redirect_uri": self.redirect,
                      "code_challenge": b64url(hashlib.sha256(verifier.encode()).digest()), "code_challenge_method": "S256",
                      "state": state, "resource": url}
            if not safe_url(meta["authorization_endpoint"]):
                raise McpError("The sign-in address is not a safe https URL.")
            self.pending[state] = {"name": name, "verifier": verifier, "t": time.time()}
            self.save()
            sep = "&" if "?" in meta["authorization_endpoint"] else "?"
            return {"auth_url": meta["authorization_endpoint"] + sep + urlencode(params)}
        except (McpError, ValueError, KeyError):
            self.data.pop(name, None)
            raise

    def _token_request(self, name, form):
        c = self.data[name]
        form = dict(form, client_id=c["client"]["id"], resource=c["url"])
        if c["client"].get("secret"):
            form["client_secret"] = c["client"]["secret"]
        st, _, b = http("POST", c["token_endpoint"], urlencode(form).encode(),
                        {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
        try:
            j = json.loads(b or b"{}")
        except ValueError:
            j = {}
        if st >= 400 or not j.get("access_token"):
            return None
        old = c.get("tokens") or {}
        c["tokens"] = {"access_token": j["access_token"], "refresh_token": j.get("refresh_token") or old.get("refresh_token"),
                       "expires_at": time.time() + float(j.get("expires_in") or 3600)}
        self.save()
        return c["tokens"]

    def finish(self, state, code):
        p = self.pending.pop(state, None)
        if not p or time.time() - p["t"] > 600:
            raise McpError("This sign-in link expired. Start again from the connector list.")
        if not self._token_request(p["name"], {"grant_type": "authorization_code", "code": code,
                                                "redirect_uri": self.redirect, "code_verifier": p["verifier"]}):
            raise McpError("The server did not accept the sign-in. Try again.")
        self.ready.discard(p["name"])
        return p["name"]

    def _refresh(self, name):
        c = self.data[name]
        rt = (c.get("tokens") or {}).get("refresh_token")
        if not rt or not c.get("token_endpoint") or not c.get("client"):
            return False
        return bool(self._token_request(name, {"grant_type": "refresh_token", "refresh_token": rt}))


TOKEN_FILE = "tg_atlas_token"


def load_token():
    """Keep the same access link across restarts so it can be bookmarked."""
    if os.path.exists(TOKEN_FILE):
        t = open(TOKEN_FILE).read().strip()
        if re.fullmatch(r"[A-Za-z0-9_-]{16,}", t):
            return t
    t = secrets.token_urlsafe(16)
    with open(TOKEN_FILE, "w") as f:
        f.write(t)
    os.chmod(TOKEN_FILE, 0o600)
    return t


class App:
    """Local web app: login with phone + code, search, auto-discovery, clickable results."""

    def __init__(self, db, args):
        self.db, self.args = db, args
        self.token = load_token()
        self.conns = ConnectorStore(args.port)
        self.client = self.ctx = None
        self.authorized, self.stage, self.phone, self.hash, self.me = False, "phone", None, None, None
        self.job, self.job_name = None, None

    async def init_client(self):
        cfg = read_config()
        if not cfg:
            return
        if self.client:
            await self.client.disconnect()
        self.client = TelegramClient(SESSION_FILE, cfg[0], cfg[1])
        await self.client.connect()
        self.authorized = await self.client.is_user_authorized()
        self.ctx = Ctx(self.client, self.db, self.args.delay, self.args.max_flood)

    def running(self):
        return bool(self.job and not self.job.done())

    def start_job(self, name, factory):
        if self.running():
            return {"error": "A job is already running. Stop it first."}
        self.ctx.stop = False
        self.job_name = name

        async def runner():
            try:
                await factory()
            except StopCrawl:
                log("Stopped.")
            except asyncio.CancelledError:
                log("Stopped by you.")
            except Exception as e:  # keep the server alive whatever happens
                log(f"Job error: {type(e).__name__}: {e}")
            finally:
                self.db.c.commit()
        self.job = asyncio.create_task(runner())
        return {"ok": True}

    def friendly(self, e):
        if isinstance(e, errors.FloodWaitError):
            return f"Telegram asks you to wait {e.seconds} seconds before trying again."
        return LOGIN_ERRORS.get(type(e).__name__, f"Telegram error: {type(e).__name__}")

    async def status(self):
        if self.authorized and not self.me:
            try:
                u = await self.client.get_me()
                self.me = f"{u.first_name or ''} {'@' + u.username if u.username else ''}".strip()
            except errors.RPCError:
                pass
        q = lambda sql: self.db.c.execute(sql).fetchone()[0]
        return {"configured": read_config() is not None, "authorized": self.authorized, "stage": self.stage,
                "me": self.me, "job": {"running": self.running(), "name": self.job_name if self.running() else None},
                "counts": {"entities": q("SELECT COUNT(*) FROM entities"),
                           "queue": q("SELECT COUNT(*) FROM queue WHERE status='pending'")},
                "log": list(LOGBUF)[-60:]}

    def results(self, qs):
        g = lambda k, d="": (qs.get(k) or [d])[0]
        limit, offset = num(g("limit", 30), 30, 1, 200), num(g("offset", 0), 0, 0, 10 ** 6)
        sort = g("sort", "relevance")
        sort = sort if sort in ("relevance", "members", "created", "growth") else "members"
        a = argparse.Namespace(
            query=g("q").strip() or None, kind=g("kind") if g("kind") in ("channel", "group", "bot") else None,
            min_members=num(g("min"), None, 0, 10 ** 9) if g("min") else None, max_members=None, script=None,
            verified=False, hide_scam=g("scam", "1") == "1", days=num(g("days"), None, 1, 10000) if g("days") else None,
            growth_days=7, sort=sort, limit=offset + limit)
        if a.sort == "relevance" and not a.query:
            a.sort = "members"
        try:
            rows = find_rows(self.db, a)[offset:]
        except sqlite3.OperationalError:
            rows = []
        items = [dict(id=r["id"], kind=r["kind"], title=r["title"], username=r["username"], members=r["members"],
                      created=r["created"], about=(r["about"] or "")[:200],
                      link=f"https://t.me/{r['username']}" if r["username"] and USER_RE.match(r["username"]) else None)
                 for r in rows]
        return {"items": items, "more": len(rows) >= limit}

    async def connectors_api(self, method, path, body):
        cs = self.conns
        try:
            if path == "/api/connectors":
                return {"items": cs.status()}
            if method != "POST":
                return {"error": "POST required"}
            name = str(body.get("name", "")).strip()
            if path == "/api/connectors/search":
                return {"items": await self.fed_search(str(body.get("query", "")).strip(), str(body.get("contains", "search")).strip().lower())}
            if path == "/api/connectors/add":
                url = str(body.get("url", "")).strip()
                if not re.fullmatch(r"[\w .\-]{1,40}", name):
                    return {"error": "Name: 1 to 40 letters, digits, spaces, dots or dashes."}
                if not safe_url(url):
                    return {"error": "Server URL must start with https://"}
                if name not in cs.data and len(cs.data) >= 200:
                    return {"error": "Connector limit (200) reached."}
                return await asyncio.to_thread(cs.begin, name, url, str(body.get("client_id") or "").strip() or None,
                                               str(body.get("client_secret") or "") or None)
            if name not in cs.data:
                return {"error": "Unknown connector."}
            if path == "/api/connectors/remove":
                cs.remove(name)
                return {"ok": True}
            if path == "/api/connectors/tools":
                return {"tools": await asyncio.to_thread(cs.tools, name)}
            if path == "/api/connectors/call":
                args = body.get("args") if isinstance(body.get("args"), dict) else {}
                return result_view(await asyncio.to_thread(cs.call, name, str(body.get("tool", "")), args))
        except AuthNeeded:
            return {"error": "Sign-in required. Press Sign in on this connector.", "needs_login": True}
        except McpError as e:
            return {"error": str(e)}
        return {"error": "Unknown endpoint."}

    async def fed_search(self, query, pat):
        if not query:
            return []
        sem = asyncio.Semaphore(4)

        async def one(name):
            async with sem:
                try:
                    tools = await asyncio.to_thread(self.conns.tools, name)
                    t = next((x for x in tools if pat in x["name"].lower() and x["readOnly"]), None)
                    if not t:
                        return {"name": name, "error": f"No read-only tool matching '{pat}'."}
                    props = (t["schema"] or {}).get("properties") or {}
                    key = next((k for k in ("query", "q", "search", "text", "keywords", "term") if k in props), None) or \
                        next((k for k, v in props.items() if isinstance(v, dict) and v.get("type") == "string"), None)
                    if not key:
                        return {"name": name, "tool": t["name"], "error": "This tool has no text input."}
                    res = await asyncio.to_thread(self.conns.call, name, t["name"], {key: query})
                    return {"name": name, "tool": t["name"], **result_view(res)}
                except AuthNeeded:
                    return {"name": name, "error": "Sign-in required."}
                except McpError as e:
                    return {"name": name, "error": str(e)}
        return await asyncio.gather(*[one(n) for n in list(self.conns.data)])

    async def api(self, method, path, qs, body):
        if path == "/api/status":
            return await self.status()
        if path == "/api/results":
            return self.results(qs)
        if path.startswith("/api/connectors"):
            return await self.connectors_api(method, path, body)
        if method != "POST":
            return {"error": "POST required"}
        if path == "/api/config":
            try:
                api_id = int(str(body.get("api_id", "")).strip())
            except ValueError:
                return {"error": "api_id must be a number."}
            api_hash = str(body.get("api_hash", "")).strip()
            if not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
                return {"error": "api_hash must be 32 hexadecimal characters."}
            save_config(api_id, api_hash)
            await self.init_client()
            return {"ok": True}
        if not self.client:
            return {"error": "Save your API keys first."}
        try:
            if path == "/api/send_code":
                phone = re.sub(r"[\s\-()]", "", str(body.get("phone", "")))
                if not re.fullmatch(r"\+?\d{7,15}", phone):
                    return {"error": LOGIN_ERRORS["PhoneNumberInvalidError"]}
                phone = phone if phone.startswith("+") else "+" + phone
                sent = await self.client.send_code_request(phone)
                self.phone, self.hash, self.stage = phone, sent.phone_code_hash, "code"
                return {"stage": "code"}
            if path == "/api/sign_in":
                code = re.sub(r"\D", "", str(body.get("code", "")))
                try:
                    await self.client.sign_in(phone=self.phone, code=code, phone_code_hash=self.hash)
                except errors.SessionPasswordNeededError:
                    self.stage = "password"
                    return {"stage": "password"}
                self.authorized, self.stage = True, "done"
                return {"ok": True}
            if path == "/api/password":
                await self.client.sign_in(password=str(body.get("password", "")))
                self.authorized, self.stage = True, "done"
                return {"ok": True}
            if not self.authorized:
                return {"error": "Sign in first."}
            if path == "/api/logout":
                await self.client.log_out()
                self.authorized, self.stage, self.me = False, "phone", None
                await self.init_client()
                return {"ok": True}
            if path == "/api/search":
                q = str(body.get("query", "")).strip()
                if not q:
                    return {"error": "Enter a query."}
                depth, budget = num(body.get("depth"), 2, 0, 4), num(body.get("budget"), 300, 1, 5000)
                return self.start_job("search", lambda: search_tree(
                    self.ctx, q, build_alphabet(DEFAULT_ALPHA), depth, budget, bool(body.get("shallow"))))
            if path == "/api/auto":
                qs_ = [w.strip() for w in re.split(r"[,\n]", str(body.get("queries", ""))) if w.strip()]
                minutes = float(num(body.get("minutes"), 60, 1, 100000))
                return self.start_job("auto", lambda: cmd_auto(self.ctx, auto_args(queries=qs_, minutes=minutes)))
            if path == "/api/stop":
                if self.running():
                    self.job.cancel()
                return {"ok": True}
            if path == "/api/posts":
                q = str(body.get("query", "")).strip()
                if not q:
                    return {"error": "Enter a query."}
                media = body.get("media") if body.get("media") in MEDIA_FILTERS else None
                items = await posts_core(self.ctx, q, chat=body.get("chat") or None, in_dialogs=bool(body.get("in_dialogs")),
                                         limit=num(body.get("limit"), 50, 1, 200), days=num(body.get("days"), None, 1, 10000) if body.get("days") else None,
                                         media=media, channels_only=bool(body.get("channels_only")))
                return {"items": items}
            if path == "/api/tg/search":  # direct Telegram API search for the standalone HTML page
                q = str(body.get("query", "")).strip()
                if not q:
                    return {"items": [], "total": 0, "more": False}
                want = [x for x in (body.get("types") or []) if x in ("channel", "chat", "bot")] or ["channel", "chat", "bot"]
                res = await self.ctx.call(functions.contacts.SearchRequest(q=q, limit=100))
                found, items = list(res.chats) + list(res.users), []
                for ent in found:
                    row = to_row(ent, "search")
                    if not row:
                        continue
                    self.db.upsert(row)
                    kind = "chat" if row["kind"] == "group" else row["kind"]
                    if kind in want:
                        items.append({"id": row["id"], "title": row.get("title"), "username": row.get("username"),
                                      "entity_type": kind, "subscribers_count": row.get("members"),
                                      "created_at": row.get("created"), "verified": bool(row.get("verified"))})
                self.db.c.commit()
                return {"items": items, "total": len(items), "more": len(found) >= SATURATION}
            if path == "/api/tg/posts":
                q = str(body.get("query", "")).strip()
                if not q:
                    return {"items": [], "total": 0}
                got = await posts_core(self.ctx, q, limit=num(body.get("limit"), 50, 1, 200))
                return {"items": [{"snippet": p["text"], "channel_username": p["username"], "views": p["views"],
                                   "posted_at": int(datetime.fromisoformat(p["date"]).timestamp()),
                                   "is_market": False, "tme_url": p["link"]} for p in got], "total": len(got)}
            if path == "/api/export":
                write_exports(self.db, export_args(None, out_dir="results"))
                return {"ok": True}
        except errors.RPCError as e:
            return {"error": self.friendly(e)}
        except StopCrawl:
            return {"error": "Telegram asked to wait too long. Try again later."}
        return {"error": "Unknown endpoint."}

    async def _send(self, writer, status, ctype, payload, extra=""):
        reason = {204: "No Content", 302: "Found", 200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 413: "Too Large", 500: "Error"}[status]
        writer.write((f"HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {len(payload)}\r\n"
                      f"{extra}Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: X-Token, Content-Type\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Private-Network: true\r\nAccess-Control-Max-Age: 600\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n").encode() + payload)
        await writer.drain()

    async def handle(self, reader, writer):
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 15)
            lines = head.decode("latin-1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            headers = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
            n = int(headers.get("content-length") or 0)
            if n > 100000:
                return await self._send(writer, 413, "text/plain", b"too large")
            body = await reader.readexactly(n) if n else b""
            if headers.get("host", "").split(":")[0] not in ("127.0.0.1", "localhost"):
                return await self._send(writer, 403, "text/plain", b"forbidden host")
            if method == "OPTIONS":  # CORS preflight; every real request still needs the secret token
                return await self._send(writer, 204, "text/plain", b"")
            u = urlparse(target)
            qs = parse_qs(u.query)
            if u.path == "/oauth/callback":  # the provider redirects the browser here after sign-in
                code, state = (qs.get("code") or [""])[0], (qs.get("state") or [""])[0]
                if not code:
                    msg = (qs.get("error_description") or qs.get("error") or ["Sign-in was cancelled."])[0]
                    return await self._send(writer, 400, "text/html; charset=utf-8", f"<p>{escape(msg)}</p>".encode())
                try:
                    cname = await asyncio.to_thread(self.conns.finish, state, code)
                except (McpError, AuthNeeded) as e:
                    return await self._send(writer, 400, "text/html; charset=utf-8", f"<p>{escape(str(e))}</p>".encode())
                return await self._send(writer, 302, "text/plain", b"",
                                        f"Location: /?t={self.token}&connected={quote(cname)}\r\n")
            tok = headers.get("x-token") or (qs.get("t") or [""])[0]
            if not secrets.compare_digest(tok, self.token):
                return await self._send(writer, 403, "text/plain",
                                        b"Open the full URL printed in the terminal (it contains the access token).")
            if u.path == "/":
                return await self._send(writer, 200, "text/html; charset=utf-8", UI_HTML.encode())
            if u.path in ("/results/links.html", "/results/links.txt"):
                fp = os.path.join("results", os.path.basename(u.path))
                if not os.path.exists(fp):
                    return await self._send(writer, 404, "text/plain", b"Press 'Update links page' first.")
                ctype = "text/html; charset=utf-8" if fp.endswith(".html") else "text/plain; charset=utf-8"
                return await self._send(writer, 200, ctype, open(fp, "rb").read())
            if u.path.startswith("/api/"):
                try:
                    data = json.loads(body or b"{}") if method == "POST" else {}
                except ValueError:
                    return await self._send(writer, 400, "application/json", b'{"error":"bad json"}')
                res = await self.api(method, u.path, qs, data)
                return await self._send(writer, 200, "application/json; charset=utf-8",
                                        json.dumps(res, ensure_ascii=False).encode())
            await self._send(writer, 404, "text/plain", b"not found")
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, ValueError):
            pass
        except Exception as e:
            try:
                await self._send(writer, 500, "application/json", json.dumps({"error": str(e)}).encode())
            except Exception:
                pass
        finally:
            writer.close()


async def cmd_serve(db, args):
    app = App(db, args)
    await app.init_client()
    server = await asyncio.start_server(app.handle, "127.0.0.1", args.port)  # loopback only
    app.conns.port = args.port = server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{args.port}/?t={app.token}"
    print(f"\nTG-Atlas is running. Open this link in your browser:\n\n  {url}\n\n(Ctrl-C to stop)\n", flush=True)
    if args.open:
        open_file(url)
    try:
        async with server:
            await server.serve_forever()
    finally:
        if app.running():
            app.job.cancel()
        if app.client:
            await app.client.disconnect()


def cmd_stats(db):
    q = lambda s: db.c.execute(s).fetchall()
    print("Total entities:", q("SELECT COUNT(*) FROM entities")[0][0])
    print("With full details:", q("SELECT COUNT(*) FROM entities WHERE detailed=1")[0][0])
    for r in q("SELECT kind,COUNT(*) n FROM entities GROUP BY kind ORDER BY n DESC"):
        print(f"  {r['kind']}: {r['n']}")
    for r in q("SELECT script,COUNT(*) n FROM entities WHERE script IS NOT NULL GROUP BY script ORDER BY n DESC LIMIT 8"):
        print(f"  script {r['script']}: {r['n']}")
    for r in q("SELECT status,COUNT(*) n FROM queue GROUP BY status"):
        print(f"  queue {r['status']}: {r['n']}")
    print("Queries run:", q("SELECT COUNT(*) FROM queries")[0][0])


# ------------------------------------------------------------------------- main
def load_config():
    cfg = read_config()
    if cfg:
        return cfg
    print("One-time setup: get api_id and api_hash at https://my.telegram.org")
    api_id, api_hash = int(input("api_id: ")), input("api_hash: ").strip()
    json.dump({"api_id": api_id, "api_hash": api_hash}, open(CONFIG_FILE, "w"))
    os.chmod(CONFIG_FILE, 0o600)
    return api_id, api_hash


def add_filters(f):
    f.add_argument("query", nargs="?")
    f.add_argument("--kind", choices=["channel", "group", "bot"])
    f.add_argument("--min-members", type=int)
    f.add_argument("--max-members", type=int)
    f.add_argument("--script", choices=[s[0] for s in SCRIPTS])
    f.add_argument("--verified", action="store_true")
    f.add_argument("--hide-scam", action="store_true")
    f.add_argument("--days", type=int, help="created within the last N days")
    f.add_argument("--growth-days", type=int, default=7)


def parser():
    p = argparse.ArgumentParser(description="TG-Atlas Telegram discovery engine")
    p.add_argument("--db", default="tg_atlas.db")
    p.add_argument("--delay", type=float, default=1.2, help="seconds between API calls")
    p.add_argument("--max-flood", type=int, default=300, help="stop if Telegram asks to wait longer (s)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("search", help="live global search with adaptive deep expansion")
    s.add_argument("query")
    s.add_argument("--deep", action="store_true", help="(default behaviour, kept for compatibility)")
    s.add_argument("--shallow", action="store_true", help="single query only, no automatic splitting")
    s.add_argument("--depth", type=int, default=3, help="max splitting levels")
    s.add_argument("--max-queries", type=int, default=5000, help="API searches per run (resumable)")
    s.add_argument("--alpha", default=DEFAULT_ALPHA, help="comma list: " + ",".join(ALPHABETS))
    s.add_argument("--limit", type=int, default=100, help="rows to print (0 = all)")
    s.add_argument("--out")

    c = sub.add_parser("crawl", help="snowball crawler (resumable)")
    c.add_argument("--seeds", nargs="*")
    c.add_argument("--from-db", action="store_true", help="queue every known username without details")
    c.add_argument("--depth", type=int, default=3)
    c.add_argument("--max", type=int, default=2000000, help="max entries to process this run")
    c.add_argument("--workers", type=int, default=2)
    c.add_argument("--scan-posts", type=int, default=0, help="posts per channel to scan for links/forwards")

    r = sub.add_parser("refresh", help="re-fetch stale entries")
    r.add_argument("--older-than", type=int, default=7, help="days")
    r.add_argument("--limit", type=int, default=500)
    r.add_argument("--workers", type=int, default=2)

    m = sub.add_parser("posts", help="search messages")
    m.add_argument("query")
    m.add_argument("--chat", help="search inside one chat (@username or link)")
    m.add_argument("--in-dialogs", action="store_true", help="search all of your own chats")
    m.add_argument("--per-chat", type=int, default=20)
    m.add_argument("--limit", type=int, default=100)
    m.add_argument("--days", type=int, help="only the last N days")
    m.add_argument("--media", choices=list(MEDIA_FILTERS))
    m.add_argument("--channels-only", action="store_true")
    m.add_argument("--show", type=int, default=20)
    m.add_argument("--out")

    f = sub.add_parser("find", help="instant offline search in the local index")
    add_filters(f)
    f.add_argument("--sort", choices=["relevance", "members", "created", "growth"], default="relevance")
    f.add_argument("--limit", type=int, default=30)
    f.add_argument("--out")

    x = sub.add_parser("export", help="write clickable links (links.html + links.txt) from the index")
    add_filters(x)
    x.add_argument("--out-dir", default="results")
    x.add_argument("--export-limit", type=int, default=50000, help="0 = everything")
    x.add_argument("--open", action="store_true", help="open links.html when done")

    u = sub.add_parser("auto", help="autonomous loop: mine keywords -> search -> crawl -> export links")
    u.add_argument("--queries", nargs="*", help="optional starting keywords")
    u.add_argument("--seeds", nargs="*", help="optional starting usernames for the crawler")
    u.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = until Ctrl-C)")
    u.add_argument("--max-cycles", type=int, default=0)
    u.add_argument("--queries-per-cycle", type=int, default=20)
    u.add_argument("--depth", type=int, default=2, help="query splitting levels")
    u.add_argument("--tree-budget", type=int, default=60, help="max API searches per keyword")
    u.add_argument("--crawl-per-cycle", type=int, default=40)
    u.add_argument("--crawl-depth", type=int, default=3)
    u.add_argument("--workers", type=int, default=2)
    u.add_argument("--scan-posts", type=int, default=0)
    u.add_argument("--alpha", default=DEFAULT_ALPHA)
    u.add_argument("--export-every", type=int, default=300, help="seconds between link-file updates")
    u.add_argument("--out-dir", default="results")
    u.add_argument("--export-limit", type=int, default=50000)
    u.add_argument("--kind", choices=["channel", "group", "bot"])
    u.add_argument("--min-members", type=int)
    u.add_argument("--keep-scam", action="store_true", help="do not hide scam/fake flagged entries")
    u.add_argument("--open", action="store_true", help="open links.html after the first export")

    v = sub.add_parser("serve", help="local web UI: sign in with phone + code, search, auto-discover, click links")
    v.add_argument("--port", type=int, default=8765)
    v.add_argument("--open", action="store_true", help="open the UI in your browser")

    sub.add_parser("stats", help="index summary")
    return p


async def run(args, db):
    api_id, api_hash = load_config()
    client = TelegramClient(SESSION_FILE, api_id, api_hash)
    await client.start()  # asks for phone number, then the code Telegram sends you
    try:
        ctx = Ctx(client, db, args.delay, args.max_flood)
        handler = {"search": cmd_search, "crawl": cmd_crawl, "refresh": cmd_refresh, "posts": cmd_posts,
                   "auto": cmd_auto}[args.cmd]
        try:
            await handler(ctx, args)
        except StopCrawl:
            log("Stopped early; progress saved.")
    finally:
        db.c.commit()
        await client.disconnect()


def main():
    if len(sys.argv) == 1:  # started without arguments (e.g. from Pydroid): open the web interface
        sys.argv.append("serve")
    args = parser().parse_args()
    db = DB(args.db)
    try:
        if args.cmd == "find":
            cmd_find(db, args)
        elif args.cmd == "stats":
            cmd_stats(db)
        elif args.cmd == "serve":
            asyncio.run(cmd_serve(db, args))
        elif args.cmd == "export":
            cmd_export(db, export_args(args, hide_scam=args.hide_scam))
        else:
            asyncio.run(run(args, db))
    except KeyboardInterrupt:
        log("Interrupted; progress saved.")
    finally:
        db.c.commit()


if __name__ == "__main__":
    main()
