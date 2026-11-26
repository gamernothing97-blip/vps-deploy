#!/usr/bin/env python3
"""Docker VPS Deploy Discord Bot  -  run: docker compose up -d --build  (every VPS is a Docker container)"""
import asyncio
import datetime as dt
import json
import math
import os
import re
import secrets
import sqlite3
import sys
import time
from pathlib import Path

if __name__ == "__main__":
    import startup
    startup.main_entry(sys.argv)  # checks + auto-fix, then starts itself in the background

import aiohttp
import discord
from aiohttp import web, WSMsgType
from discord import app_commands
from discord.ext import commands, tasks

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import agent as nodeapi

CREATOR = os.getenv("CREATOR_NAME", "Diablo")

# ------------------------------------------------------------------ "online on mobile" status
# Discord shows the phone icon when the gateway login says the client is a mobile app.
# (Unofficial trick: if a future discord.py changes its internals this is skipped and the bot still works.)
try:
    from discord.gateway import DiscordWebSocket as _DWS
    _orig_identify = _DWS.identify

    async def _mobile_identify(self):
        try:
            payload = {"op": self.IDENTIFY, "d": {
                "token": self.token,
                "properties": {"os": "Android", "browser": "Discord Android", "device": "Discord Android",
                               "$os": "Android", "$browser": "Discord Android", "$device": "Discord Android"},
                "compress": True, "large_threshold": 250}}
            if self.shard_id is not None and self.shard_count is not None:
                payload["d"]["shard"] = [self.shard_id, self.shard_count]
            state = self._connection
            if state._activity is not None or state._status is not None:
                payload["d"]["presence"] = {"status": state._status, "game": state._activity, "since": 0, "afk": False}
            if state._intents is not None:
                payload["d"]["intents"] = state._intents.value
            await self.call_hooks("before_identify", self.shard_id, initial=self._initial_identify)
            await self.send_as_json(payload)
        except AttributeError:
            await _orig_identify(self)
    _DWS.identify = _mobile_identify
except Exception as _e:
    print("mobile status trick not applied:", _e)

TOKEN = os.environ["DISCORD_TOKEN"]
MIN_AGE_DAYS = int(os.getenv("MIN_ACCOUNT_AGE_DAYS", "213"))  # ~7 months
VPS_DAYS = int(os.getenv("VPS_DAYS", "30"))
GRACE_DAYS = 3
# bot owners (full admin + can add/remove other admins) - set ADMIN_IDS=123,456 in .env
OWNER_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_IDS", "")) if x.strip().isdigit()}
LOCAL_MAX = 25  # node1 hard cap
BT = "```"

# invites: (ram GB, cpu, disk GB)
DEFAULT_PLANS = {2: (2, 1, 30), 4: (5, 2, 40), 6: (8, 2, 50), 8: (16, 3, 70), 10: (32, 4, 90)}
PLANS = dict(DEFAULT_PLANS)  # replaced by the database (see /plan-add, /plan-remove)
IMAGES = nodeapi.IMAGES

# ------------------------------------------------------------------ database
DB_PATH = os.path.abspath(os.getenv("DB_PATH", "bot.db"))  # absolute, so every start uses the SAME file
INSTANCE_ID = secrets.token_hex(3)  # shown on "/" and in the log: tells two running bots apart
try:  # refuse to run twice on one machine/volume (two bots = two states: random 401 / "not connected")
    import fcntl
    _LOCK_FH = open(DB_PATH + ".lock", "w")
    fcntl.flock(_LOCK_FH, fcntl.LOCK_EX | fcntl.LOCK_NB)
    _LOCK_FH.write(str(os.getpid()))
    _LOCK_FH.flush()
except ImportError:
    pass
except OSError:
    raise SystemExit(f"ANOTHER BOT IS ALREADY RUNNING with {DB_PATH} - stop it first (docker ps / ps aux | grep bot.py)")
print(f"bot instance {INSTANCE_ID} pid {os.getpid()} db {DB_PATH}", flush=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False)
try:
    db.execute("pragma journal_mode=wal")
    db.execute("pragma synchronous=normal")
except Exception:
    pass
db.executescript("""
create table if not exists joins(id integer primary key autoincrement, user_id int, inviter_id int,
    kind text, left int default 0, ts int);
create table if not exists spent(user_id int primary key, used int default 0);
create table if not exists nodes(name text primary key, token text, host text, port int,
    max_vps int, is_local int default 0, online int default 0);
create table if not exists vps(name text primary key, owner_id int, node text, invites int,
    os text, expires int, status text);
create table if not exists settings(k text primary key, v text);
create table if not exists admins(user_id int primary key);
""")
db.commit()
try:  # node type (docker / lxd / lxc) - added later, so migrate old databases
    db.execute("alter table nodes add column ntype text default 'auto'")
    db.commit()
except Exception:
    pass


def q(sql, *a):
    cur = db.execute(sql, a)
    db.commit()
    return cur.fetchall()


def get(k):
    r = q("select v from settings where k=?", k)
    return r[0][0] if r else None


def put(k, v):
    q("insert into settings values(?,?) on conflict(k) do update set v=excluded.v", k, str(v))


db.execute("create table if not exists plans(invites int primary key, ram int, cpu int, disk int, active int default 1)")
db.execute("create table if not exists old_nodes(name text primary key, token text, ntype text, max_vps int)")
db.execute("delete from old_nodes")  # node recovery was removed: forget every old token
db.commit()
ALL_PLANS = {}  # every plan ever created (also removed ones) - so old VPS of a removed plan keep working


def load_plans():
    """PLANS (active, shown in /buy and /deploy) comes from the database; first start seeds the defaults."""
    if not q("select 1 from plans"):
        for k, (r, c, d) in DEFAULT_PLANS.items():
            q("insert into plans values(?,?,?,?,1)", k, r, c, d)
    PLANS.clear()
    ALL_PLANS.clear()
    for k, r, c, d, a in q("select invites,ram,cpu,disk,active from plans order by invites"):
        ALL_PLANS[k] = (r, c, d)
        if a:
            PLANS[k] = (r, c, d)


def plan_of(inv):
    return ALL_PLANS.get(inv) or PLANS.get(inv) or (0, 0, 0)


def stats(uid):
    valid = fake = rejoin = left = 0
    for kind, l, c in q("select kind,left,count(*) from joins where inviter_id=? group by kind,left", uid):
        if kind == "valid":
            if l:
                left += c
            else:
                valid += c
        elif kind == "fake":
            fake += c
        else:
            rejoin += c
    used = (q("select used from spent where user_id=?", uid) or [[0]])[0][0]
    return dict(valid=valid, fake=fake, rejoin=rejoin, left=left, used=used, avail=max(valid - used, 0))


def spend(uid, n):
    q("insert into spent values(?,?) on conflict(user_id) do update set used=max(used+?,0)", uid, max(n, 0), n)


# ------------------------------------------------------------------ helpers
def is_owner(u):
    return u.id in OWNER_IDS


def is_admin(u):
    if u.id in OWNER_IDS or q("select 1 from admins where user_id=?", u.id):
        return True
    return isinstance(u, discord.Member) and u.guild_permissions.administrator


def channel_ok(user, channel_id):
    ch = get("invite_channel")
    return not ch or str(channel_id) == ch or is_admin(user)


class Tree(app_commands.CommandTree):
    async def interaction_check(self, i):
        if channel_ok(i.user, i.channel_id):
            return True
        await i.response.send_message(f"Use this command in <#{get('invite_channel')}> only.", ephemeral=True)
        return False


intents = discord.Intents.default()
intents.members = True
intents.message_content = True


load_plans()


class Bot(commands.Bot):
    async def setup_hook(self):
        if get("node1_removed") != "1":  # admin can remove node1 with /node-remove (then it stays removed)
            if not q("select 1 from nodes where name='node1'"):
                q("insert into nodes(name,max_vps,is_local,online) values('node1',?,1,0)", LOCAL_MAX)
            q("update nodes set max_vps=? where name='node1'", LOCAL_MAX)
        q("update nodes set online=0 where is_local=0")  # remote nodes become online when they connect
        await start_panel_server()
        asyncio.create_task(bootstrap_local())  # check Docker + prepare node1 in the background
        housekeeping.start()
        poll_nodes.start()
        presence_loop.start()
        # old global registrations are removed; commands are synced per-guild (appear instantly)
        try:
            await self.http.bulk_upsert_global_commands(self.application_id, [])
        except Exception as e:
            print("global clear failed:", e)


bot = Bot(command_prefix="-", intents=intents, tree_cls=Tree, help_command=None,
          status=discord.Status.online,
          activity=discord.Activity(type=discord.ActivityType.watching, name="☁️ Legacy Cloud V1"))


@bot.check
async def _global_check(ctx):
    return channel_ok(ctx.author, ctx.channel.id)


@bot.event
async def on_command_error(ctx, err):
    if isinstance(err, commands.CheckFailure):
        await ctx.send(f"Use this command in <#{get('invite_channel')}> only.", delete_after=5)
    elif not isinstance(err, commands.CommandNotFound):
        print("cmd error:", err)


async def dm(uid, text):
    try:
        u = bot.get_user(uid) or await bot.fetch_user(uid)
        await u.send(text)
    except Exception:
        pass


async def log(text):
    ch = get("log_channel")
    if ch:
        c = bot.get_channel(int(ch))
        if c:
            try:
                await c.send(text)
            except Exception:
                pass


# ------------------------------------------------------------------ admin command audit log
ADMIN_CMDS = {
    "deploy", "vps-list", "vps-delete", "vps-suspend", "vps-unsuspend", "admin-renew", "vps-expire",
    "vps-expire-all", "vps-stats", "reset-ssh", "invites-give", "invites-remove", "health", "diag", "fix",
    "node-install", "node-domain", "node-add", "node-connect", "node-list", "node-remove", "node-max", "setup",
    "admin-add", "admin-remove", "admin-list",
}
SECRET_ARGS = {"token"}


def _fmt_args(i):
    parts = []
    try:
        for k, v in i.namespace:
            if k in SECRET_ARGS:
                v = "***hidden***"
            elif hasattr(v, "mention"):
                v = v.mention
            parts.append(f"{k}={str(v)[:60]}")
    except Exception:
        pass
    return " ".join(parts)


@bot.event
async def on_app_command_completion(i: discord.Interaction, command):
    if command.qualified_name in ADMIN_CMDS:
        await log(f"🛡️ <@{i.user.id}> used `/{command.qualified_name}`")


async def bootstrap_local():
    """node1 = the machine this bot runs on. Checks Docker and creates the VPS network."""
    try:
        note = await nodeapi.ensure_lxd()
        await nodeapi.apply_protection()
        q("update nodes set online=1 where name='node1'")
        asyncio.create_task(nodeapi.watchdog_loop())
        async def _prewarm():
            async def _np(*a):
                pass
            for k in ("ubuntu22",):
                try:
                    await nodeapi._ensure_image(k, _np)
                    print(f"image {k} cached")
                except Exception as e:
                    print(f"image {k} pre-download failed: {e}")
                    await log(f"⚠️ OS image pre-download failed: `{str(e)[:700]}`")
        async def _quota():
            await _prewarm()
            note = await nodeapi.ensure_quota_pool()
            print("storage:", note)
            await bot.wait_until_ready()
            await log(("✅ " if "enforced" in note else "⚠️ ") + f"Disk limits: {note}")
        asyncio.create_task(_quota())
        print(f"node1 ready [{nodeapi.KIND}]" + (f" ({note})" if note else ""))
        await bot.wait_until_ready()
        await log(f"🖧 node1 runtime auto-detected: **{nodeapi.KIND.upper()}**\n```\n{nodeapi.REPORT_TEXT}\n```")
        if note:
            await bot.wait_until_ready()
            await log(f"✅ node1 set up automatically: {note}")
    except Exception as e:
        print("node1 auto-setup failed:", e)
        await bot.wait_until_ready()
        await log(f"⚠️ node1 auto-setup failed: {e}")


# ------------------------------------------------------------------ invite tracking
invite_cache = {}


async def snapshot(g):
    try:
        return {i.code: (i.uses or 0, i.inviter.id if i.inviter else 0) for i in await g.invites()}
    except discord.HTTPException:
        return None


async def sync_guild(g):
    bot.tree.copy_global_to(guild=g)
    try:
        n = await bot.tree.sync(guild=g)
        print(f"synced {len(n)} slash commands to guild {g.name} ({g.id})")
        return len(n)
    except Exception as e:
        print(f"SLASH SYNC FAILED in {g.name} ({g.id}): {e}  -> re-invite the bot with the applications.commands scope")
        return -1


_LAST_SYNC = [0.0]


async def sync_all(force=False):
    """Register the slash commands in every server AT THE SAME TIME (fast). Skips a repeat within 5 min
    (Discord reconnects fire on_ready again) unless forced."""
    if not force and time.time() - _LAST_SYNC[0] < 300:
        return
    _LAST_SYNC[0] = time.time()
    res = await asyncio.gather(*[sync_guild(g) for g in bot.guilds], return_exceptions=True)
    return res


@bot.event
async def on_ready():
    await sync_all()  # commands first - no waiting behind the slow invite snapshots
    for g in bot.guilds:
        s = await snapshot(g)
        if s is not None:
            invite_cache[g.id] = s
    print("ready as", bot.user)


@bot.tree.command(name="sync", description="Admin: re-register all slash commands right now (use if commands are missing/slow)")
@app_commands.guild_only()
async def sync_cmd(i: discord.Interaction):
    if not is_admin(i.user):
        return await i.response.send_message("Admins only.", ephemeral=True)
    await i.response.defer(ephemeral=True)
    n = await sync_guild(i.guild)
    await i.followup.send(f"✅ {n} slash commands registered. If they do not show, press Ctrl+R in Discord." if n >= 0
                          else "❌ Sync failed - re-invite the bot with the `applications.commands` scope.", ephemeral=True)


@bot.event
async def on_guild_join(g):
    invite_cache[g.id] = await snapshot(g) or {}
    await sync_guild(g)


@bot.event
async def on_invite_create(inv):
    invite_cache.setdefault(inv.guild.id, {})[inv.code] = (inv.uses or 0, inv.inviter.id if inv.inviter else 0)


@bot.event
async def on_invite_delete(inv):
    invite_cache.get(inv.guild.id, {}).pop(inv.code, None)


@bot.event
async def on_member_join(m):
    if m.bot:
        return
    old = invite_cache.get(m.guild.id, {})
    new = await snapshot(m.guild)
    if new is None:
        return
    invite_cache[m.guild.id] = new
    inviter = next((inv for code, (uses, inv) in new.items() if uses > old.get(code, (0, 0))[0]), None)
    if not inviter:
        return
    seen = q("select 1 from joins where user_id=?", m.id)
    age = (dt.datetime.now(dt.timezone.utc) - m.created_at).days
    if seen:
        kind = "rejoin"
    elif age < MIN_AGE_DAYS or inviter == m.id:
        kind = "fake"
    else:
        kind = "valid"
    q("insert into joins(user_id,inviter_id,kind,ts) values(?,?,?,?)", m.id, inviter, kind, int(time.time()))


@bot.event
async def on_member_remove(m):
    q("update joins set left=1 where id=(select max(id) from joins where user_id=?)", m.id)


# ------------------------------------------------------------------ nodes
HEAL_LOCK = asyncio.Lock()
START_TS = time.time()


async def heal_local():
    """Self-repair for node1: re-check Docker + network, re-apply firewall. True if docker works after."""
    async with HEAL_LOCK:
        try:
            await nodeapi.ensure_lxd()
            await nodeapi.apply_protection()
        except Exception as e:
            await log(f"❌ Auto-fix of node1 failed: {e}")
            return False
        try:
            await nodeapi.OPS["status"]()
            ok = True
        except Exception:
            ok = False
        if ok:
            q("update nodes set online=1 where name='node1'")
        return ok


CREATE_TIMEOUT = 840  # seconds (< Discord's 15-minute interaction limit)
LXD_DOWN_RE = re.compile(r"docker\.sock|cannot connect|cannot talk to docker|is the docker daemon running|"
                         r"failed to connect to the hypervisor|libvirt|cannot reach libvirt|lxd.*not (running|ready)|"
                         r"connection refused|daemon|failed to connect", re.I)


# ------------------------------------------------------------------ node links (node -> bot, outbound WebSocket)
# Remote nodes dial IN to this bot through the Cloudflare tunnel, so they need no IP / port of their own.
# Point the tunnel at http://localhost:PANEL_PORT  (cloudflared: `cloudflared tunnel --url http://localhost:8765`).
PANEL_PORT = int(os.getenv("PANEL_PORT", "8765"))
PANEL_BIND = os.getenv("PANEL_BIND", "127.0.0.1")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
if "PASTE" in PUBLIC_URL:
    PUBLIC_URL = ""
NODE_LINKS = {}  # node name -> {"ws": WebSocketResponse, "pending": {request id: Future}}


async def ws_handler(request):
    auth = request.headers.get("Authorization", "")
    tok = auth[7:] if auth.startswith("Bearer ") else ""
    name = None
    for n, t in q("select name,token from nodes where is_local=0 and token is not null"):
        if tok and t and secrets.compare_digest(t.encode(), tok.encode()):
            name = n
            break
    if not name:
        known = [n for n, in q("select name from nodes where is_local=0")]
        print(f"[{INSTANCE_ID}] node link REJECTED (401): token {tok[:4]}...({len(tok)} chars) matches no node; "
              f"nodes in this db: {known}", flush=True)
        await asyncio.sleep(1)  # slow down token guessing
        return web.Response(status=401, text="unauthorized")
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=0)
    await ws.prepare(request)
    old = NODE_LINKS.get(name)
    if old:  # same node reconnecting: drop the stale link
        try:
            await old["ws"].close()
        except Exception:
            pass
    link = {"ws": ws, "pending": {}}
    NODE_LINKS[name] = link
    q("update nodes set online=1 where name=?", name)
    nt = (request.headers.get("X-Node-Type") or "").lower()
    if nt in ("kvm", "lxd", "docker", "lxc"):
        q("update nodes set ntype=? where name=?", nt, name)
    await log(f"🟢 Node `{name}` connected" + (f" ({nt})." if nt else "."))
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                d = json.loads(msg.data)
            except ValueError:
                continue
            fut = link["pending"].pop(d.get("id"), None)
            if fut and not fut.done():
                if d.get("error"):
                    fut.set_exception(RuntimeError(d["error"]))
                else:
                    fut.set_result(d.get("data"))
    finally:
        for fut in link["pending"].values():
            if not fut.done():
                fut.set_exception(RuntimeError(f"node {name} disconnected"))
        link["pending"].clear()
        if NODE_LINKS.get(name) is link:
            del NODE_LINKS[name]
            q("update nodes set online=0 where name=?", name)
            await log(f"🔴 Node `{name}` disconnected.")
    return ws


async def start_panel_server():
    try:
        app = web.Application()
        app.router.add_get("/ws", ws_handler)
        app.router.add_get("/", lambda r: web.Response(
            text=f"ok instance={INSTANCE_ID} pid={os.getpid()} nodes={q('select count(*) from nodes where is_local=0')[0][0]}"))
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, PANEL_BIND, PANEL_PORT).start()
        print(f"node link server on {PANEL_BIND}:{PANEL_PORT} (expose it with Cloudflare Tunnel)")
    except Exception as e:
        print("node link server failed to start:", e)


async def call_remote(node, op, kw):
    link = NODE_LINKS.get(node)
    if not link or link["ws"].closed:
        raise RuntimeError(f"node {node} is offline (not connected to the bot)")
    rid = secrets.token_hex(8)
    fut = asyncio.get_running_loop().create_future()
    link["pending"][rid] = fut
    try:
        await link["ws"].send_json({"id": rid, "op": op, "kw": kw})
        return await asyncio.wait_for(fut, CREATE_TIMEOUT if op == "create" else 200)
    except asyncio.TimeoutError:
        raise RuntimeError(f"node {node} did not answer in time")
    except ConnectionError as e:
        raise RuntimeError(f"node {node} unreachable: {e}")
    finally:
        link["pending"].pop(rid, None)


async def call(node, op, _notify=None, _progress=None, **kw):
    r = q("select is_local from nodes where name=?", node)
    if not r:
        raise RuntimeError(f"node {node} does not exist (it was removed)")
    if r[0][0]:
        if _progress and op == "create":
            kw["progress"] = _progress
        try:
            if op == "create":
                try:
                    return await asyncio.wait_for(nodeapi.OPS[op](**kw), CREATE_TIMEOUT)
                except asyncio.TimeoutError:
                    try:
                        await nodeapi.OPS["action"](name=kw["name"], action="delete")
                    except Exception:
                        pass
                    raise RuntimeError(f"timed out after {CREATE_TIMEOUT // 60} min - VPS removed")
            return await nodeapi.OPS[op](**kw)  # local node1
        except ValueError:
            raise
        except Exception as e:
            # Only auto-heal (and retry) when Docker itself looks broken. A normal failure (no network, timeout,
            # quota ...) is reported straight away instead of silently running the whole create a 2nd time.
            if op == "create" and not LXD_DOWN_RE.search(str(e)):
                raise
            if _notify:
                try:
                    await _notify("⚠️ A problem came up on the server. **Fixing it automatically - please wait ~2 minutes...**")
                except Exception:
                    pass
            await log(f"⚠️ `{op}` failed on node1 ({str(e)[:200]}) - auto-fixing")
            if await heal_local():
                return await nodeapi.OPS[op](**kw)
            raise RuntimeError(f"auto-fix failed: {e}")
    return await call_remote(node, op, kw)


def pick_node():
    best = None
    for name, mx in q("select name,max_vps from nodes where online=1"):
        c = q("select count(*) from vps where node=? and status!='deleted'", name)[0][0]
        if c < mx and (best is None or c / mx < best[0]):
            best = (c / mx, name)
    return best[1] if best else None


# ------------------------------------------------------------------ progress bar + credentials
def pbar(p, w=18):
    f = int(w * min(max(p, 0), 100) / 100)
    return "[" + "█" * f + "░" * (w - f) + f"] {int(p)}%"


def prog_embed(title, pct, user, cid, node, ok_text, done=False, fail=None, dm_ok=True, stage=None, elapsed=0):
    e = discord.Embed(title=title, color=0x57F287 if done else (0xED4245 if fail else 0x5865F2))
    if fail:
        e.description = f"❌ **Failed:** {str(fail)[:900]}"
    else:
        working = f"⏳ **{stage or 'Working... please wait'}** · {int(elapsed)}s"
        e.description = (f"✅ **{ok_text}**" if done else working) + f"\n`{pbar(pct)}`"
    e.add_field(name="👤 Owner Tag", value=user.mention)
    e.add_field(name="📦 Container ID", value=f"`{cid}`")
    e.add_field(name="🌐 Cluster Node", value=f"`{str(node).upper()}`")
    if done:
        e.set_footer(text="🔒 SSHX access link sent directly to your DMs." if dm_ok
                     else "⚠️ Your DMs are closed - open them and use /myvps -> Reset SSH.")
    return e


async def run_with_progress(coro, channel, user, cid, node, title, ok_text, state=None):
    """Runs coro while a public embed shows the REAL stage (state = {"pct":..,"text":..} updated by the worker).
    Without a real stage (remote node) it falls back to a slow time-based bar. On error the embed turns red."""
    msg = None
    if channel is not None:
        try:
            msg = await channel.send(embed=prog_embed(title, 0, user, cid, node, ok_text, stage="Starting"))
        except Exception:
            msg = None
    task = asyncio.ensure_future(coro)
    t0 = time.time()
    while not task.done():
        if msg:
            el = time.time() - t0
            if state and state.get("text"):
                p, stage = state["pct"], state["text"]
            else:
                p, stage = 95 * (1 - math.exp(-el / 45)), None
            try:
                await msg.edit(embed=prog_embed(title, p, user, cid, node, ok_text, stage=stage, elapsed=el))
            except Exception:
                pass
        await asyncio.wait({task}, timeout=3)
    try:
        res = task.result()
    except BaseException as e:
        if msg:
            try:
                where = f" (stopped at: {state['text']})" if state and state.get("text") else ""
                await msg.edit(embed=prog_embed(title, 0, user, cid, node, ok_text, fail=f"{e}{where}"))
            except Exception:
                pass
        raise
    return res, msg


async def finish_progress(msg, title, user, cid, node, ok_text, dm_ok=True):
    if msg:
        try:
            await msg.edit(embed=prog_embed(title, 100, user, cid, node, ok_text, done=True, dm_ok=dm_ok))
        except Exception:
            pass


def creds_text(res):
    link = res.get("sshx")
    if not link:
        return "(SSHX link was not available - use Reset SSH in /myvps)"
    return (f"**SSHX (web terminal link - root access of your VPS):**\n{link}\n"
            f"No password needed - just open the link.")


# ------------------------------------------------------------------ deploy / renew
async def do_deploy(user, plan, os_key, free=False, node=None, notify=None, channel=None):
    ram, cpu, disk = PLANS[plan]
    if not free and stats(user.id)["avail"] < plan:
        return "❌ Not enough invites."
    if node:
        r = q("select online,max_vps from nodes where name=?", node)
        if not r:
            return f"❌ Node `{node}` not found."
        if not r[0][0]:
            return f"❌ Node `{node}` is offline."
        used = q("select count(*) from vps where node=? and status!='deleted'", node)[0][0]
        if used >= r[0][1]:
            return f"❌ Node `{node}` is full ({used}/{r[0][1]})."
    else:
        node = pick_node()
        if not node:
            return "❌ All nodes are full right now. Try again later."
    name = f"vps-{secrets.token_hex(4)}"
    if not free:
        spend(user.id, plan)
    q("insert into vps values(?,?,?,?,?,?,?)", name, user.id, node, plan, os_key, 0, "creating")
    T = "▶️ Starting VPS Instance Engine"
    OK = "VPS Boot Sequence Completed Successfully!"
    state = {"pct": 0, "text": ""}

    def on_prog(pct, text):
        state["pct"], state["text"] = pct, text
    try:
        res, pmsg = await run_with_progress(
            call(node, "create", _notify=notify, _progress=on_prog,
                 name=name, os_key=os_key, cpu=cpu, ram=ram, disk=disk),
            channel, user, name, node, T, OK, state=state)
    except Exception as e:
        q("delete from vps where name=?", name)
        if not free:
            spend(user.id, -plan)
        await log(f"❌ Deploy failed for <@{user.id}> on `{node}`: `{str(e)[:200]}`")
        return f"❌ Deploy failed: {e}"
    exp = int(time.time() + VPS_DAYS * 86400)
    q("update vps set expires=?, status='running' where name=?", exp, name)
    bump_created()
    text = (f"✅ **VPS deployed!**\nName: `{name}` · Node: {node}\nOS: {IMAGES[os_key][0]}\n"
            f"Specs: {ram}GB RAM · {cpu} CPU · {disk}GB disk\nExpires: <t:{exp}:R>\n\n"
            f"{creds_text(res)}\n\nManage with `/myvps`")
    await log(f"🚀 VPS `{name}` deployed for <@{user.id}> · node `{node}` · {IMAGES[os_key][0]} · "
              f"{ram}GB/{cpu}CPU/{disk}GB · {'free (admin)' if free else f'{plan} invites used'}")
    try:
        await (await user.create_dm()).send(text)
        await finish_progress(pmsg, T, user, name, node, OK, dm_ok=True)
        return "✅ VPS created! SSH details sent to your DMs."
    except discord.HTTPException:
        await finish_progress(pmsg, T, user, name, node, OK, dm_ok=False)
        return text + "\n⚠️ Your DMs are closed, so details are shown here (only you can see this)."


async def do_renew(uid, name, admin=False, days=None):
    r = q("select owner_id,node,invites,expires,status from vps where name=?", name)
    if not r:
        return "❌ VPS not found."
    owner, node, cost, exp, status = r[0]
    if owner != uid and not admin:
        return "❌ Not your VPS."
    if status not in ("running", "suspended"):
        return "❌ This VPS cannot be renewed."
    left_txt = ""
    if not admin:
        have = stats(uid)["avail"]
        if have < cost:
            return f"❌ Renewal needs **{cost}** invites, you only have **{have}**."
        spend(uid, cost)
        left_txt = f" (🎟️ {cost} invites cut, {have - cost} left)"
    new = max(exp, int(time.time())) + (days or VPS_DAYS) * 86400
    q("update vps set expires=?, status='running' where name=?", new, name)
    await log(f"🔁 VPS `{name}` renewed by <@{uid}>{' (admin)' if admin else ''} until <t:{new}:F>")
    if status == "suspended":
        try:
            await call(node, "action", name=name, action="start")
        except Exception as e:
            return f"⚠️ Renewed but start failed: {e}"
    return f"✅ Renewed until <t:{new}:F>.{left_txt}"


# ------------------------------------------------------------------ views
class SafeView(discord.ui.View):
    """Base for all menus/buttons: an error is logged and the user gets a clear message
    (instead of Discord's vague 'did not respond in time')."""
    async def on_error(self, i, error, item):
        import traceback
        print("view error:", "".join(traceback.format_exception(type(error), error, error.__traceback__)), flush=True)
        try:
            await log(f"⚠️ Menu/button error for <@{i.user.id}>: `{str(error)[:300]}`")
        except Exception:
            pass
        try:
            msg = "⚠️ Something went wrong. Please run the command again."
            if i.response.is_done():
                await i.followup.send(msg, ephemeral=True)
            else:
