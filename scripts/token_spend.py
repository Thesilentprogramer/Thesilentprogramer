#!/usr/bin/env python3
"""Compute lifetime AI token spend from local Claude Code, Codex, Cursor, and Antigravity logs.

Keeps a ledger in ~/Library/Application Support/token-spend/ledger.json that
only ever grows, and publishes aggregate totals (no log content) to
data/tokens.json. `--install` registers a launchd job that uploads every 6h;
`--uninstall` removes it. `--write PATH` writes the JSON locally instead.
The job runs a copy of this file, so rerun `--install` after editing it.
"""

import argparse
import base64
import glob
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

REPO = "Thesilentprogramer/Thesilentprogramer"
BRANCH = "main"
REMOTE_PATH = "data/tokens.json"
ENDPOINT = f"repos/{REPO}/contents/{REMOTE_PATH}"
LABEL = "com.thesilentprogramer.token-spend"
GH_FALLBACKS = ["/opt/homebrew/bin/gh", "/usr/local/bin/gh"]
SYSTEM_PYTHON = "/usr/bin/python3"
LEDGER_VERSION = 1
CODEX_FIELDS = ("total_tokens", "cached_input_tokens")
CURSOR_USAGE_START_MS = "1761350400000"  # 2025-10-25, matches the first ledger day
CURSOR_USAGE_PAGE = 200
# ponytail: dashboard API stops at 50 pages (~10k events). Raise if lifetime history truncates.
MAX_CURSOR_USAGE_PAGES = 50


def support_dir(home):
    return os.path.join(home, "Library", "Application Support", "token-spend")


def read_json_lines(path, needle):
    """Yield parsed objects from the lines of path that contain needle."""
    with open(path, encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if needle not in line:
                continue
            try:
                yield json.loads(line)
            except ValueError:
                continue


def scan_claude(home):
    """Per UTC day {all, fresh} from Claude Code transcripts, deduped on (message id, requestId)."""
    days = {}
    seen = set()
    pattern = os.path.join(home, ".claude", "projects", "**", "*.jsonl")
    for path in sorted(glob.glob(pattern, recursive=True)):
        for o in read_json_lines(path, '"usage"'):
            m = o.get("message")
            u = m.get("usage") if isinstance(m, dict) else None
            if not u:
                continue
            key = (m.get("id"), o.get("requestId"))
            if key in seen:
                continue
            seen.add(key)
            ts = o.get("timestamp")
            if not ts:
                continue
            cache_read = u.get("cache_read_input_tokens") or 0
            total = (
                (u.get("input_tokens") or 0)
                + (u.get("output_tokens") or 0)
                + (u.get("cache_creation_input_tokens") or 0)
                + cache_read
            )
            day = days.setdefault(ts[:10], {"all": 0, "fresh": 0})
            day["all"] += total
            day["fresh"] += total - cache_read
    return days


def codex_day(rel_path):
    m = re.search(r"(\d{4})/(\d{2})/(\d{2})/", rel_path)
    if m:
        return "-".join(m.groups())
    m = re.search(r"rollout-(\d{4}-\d{2}-\d{2})", os.path.basename(rel_path))
    return m.group(1) if m else None


def scan_codex_file(path):
    """(all, fresh) this file adds beyond the cumulative total it started from, or None."""
    first = last = None
    for o in read_json_lines(path, "total_token_usage"):
        info = (o.get("payload") or {}).get("info") or {}
        t = info.get("total_token_usage")
        last_usage = info.get("last_token_usage") or {}
        if not t:
            continue
        if first is None:
            first = {k: (t.get(k) or 0) - (last_usage.get(k) or 0) for k in CODEX_FIELDS}
        last = t
    if last is None:
        return None
    delta = {k: (last.get(k) or 0) - first[k] for k in CODEX_FIELDS}
    return delta["total_tokens"], delta["total_tokens"] - delta["cached_input_tokens"]


def scan_codex(home):
    """Per session file {all, fresh, day}, keyed by file name.

    The name carries a UUID and survives the move from sessions/ to
    archived_sessions/, so an archived session keeps its single ledger entry.
    """
    root = os.path.join(home, ".codex")
    files = {}
    for sub in ("sessions", "archived_sessions"):
        pattern = os.path.join(root, sub, "**", "*.jsonl")
        for path in sorted(glob.glob(pattern, recursive=True)):
            counts = scan_codex_file(path)
            if counts is None:
                continue
            rel = os.path.relpath(path, root)
            files[os.path.basename(rel)] = {"all": counts[0], "fresh": counts[1], "day": codex_day(rel)}
    return files


def _token_pair(inp, out, cache_read=0, cache_write=0):
    """(all, fresh). fresh skips cache reads, same split Codex uses."""
    fresh = inp + out + cache_write
    return fresh + cache_read, fresh


def _bubble_usage(raw):
    """(all, fresh, day) from a Cursor bubble, or None. Skips rows that are not token records."""
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", "replace")
    else:
        text = raw or ""
    block = re.search(r'"tokenCount"\s*:\s*\{([^}]*)\}', text)
    if not block:
        return None
    body = block.group(1)

    def num(name):
        match = re.search(rf'"{name}"\s*:\s*(-?\d+)', body)
        return int(match.group(1)) if match else 0

    total, fresh = _token_pair(
        num("inputTokens"), num("outputTokens"), num("cacheReadTokens"), num("cacheWriteTokens")
    )
    if total <= 0:
        return None
    created = re.search(r'"createdAt"\s*:\s*"(\d{4}-\d{2}-\d{2})', text)
    return total, fresh, created.group(1) if created else None


def _cursor_user_id(token):
    decoded = urllib.parse.unquote(token)
    if "::" in decoded:
        return decoded.split("::", 1)[0]
    parts = decoded.split(".")
    if len(parts) != 3:
        return None
    pad = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(pad.encode()))
    except (ValueError, json.JSONDecodeError):
        return None
    match = re.search(r"user_[A-Za-z0-9]+", payload.get("sub") or "")
    return match.group(0) if match else None


def _ms_day(ts):
    try:
        return datetime.utcfromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _http_json_post(url, headers, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode())


def _cursor_env_token():
    return os.environ.get("CURSOR_TOKEN") or os.environ.get("CURSOR_SESSION_TOKEN") or ""


def _iso_to_ms(stamp):
    if not stamp:
        return None
    try:
        return int(datetime.strptime(stamp[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        return None


def _scan_cursor_dashboard(token, start_ms=None):
    """Per conversation {all, fresh, day} from Cursor's usage dashboard.

    Current Cursor builds leave bubble tokenCount at 0. The billed events still
    exist on the account dashboard. Failures return {} so the bubble scan stands.
    """
    if not token:
        return {}
    user_id = _cursor_user_id(token)
    cookie = token
    if "::" in cookie and "%3A%3A" not in cookie:
        cookie = cookie.replace("::", "%3A%3A")
    elif user_id and "::" not in token and "%3A%3A" not in token:
        cookie = user_id + "%3A%3A" + token
    end_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    dashboard_payload = {
        "startDate": start_ms or CURSOR_USAGE_START_MS,
        "endDate": end_ms,
        "pageSize": CURSOR_USAGE_PAGE,
    }
    api2_payload = dict(dashboard_payload)
    if user_id:
        api2_payload["userId"] = user_id
    attempts = (
        (
            "https://cursor.com/api/dashboard/get-filtered-usage-events",
            {
                "Cookie": "WorkosCursorSessionToken=" + cookie,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://cursor.com",
                "Referer": "https://cursor.com/dashboard",
            },
            dashboard_payload,
        ),
        (
            "https://api2.cursor.sh/aiserver.v1.DashboardService/GetFilteredUsageEvents",
            {
                "Authorization": "Bearer " + token.split("::")[-1].split("%3A%3A")[-1],
                "Content-Type": "application/json",
                "connect-protocol-version": "1",
            },
            api2_payload,
        ),
    )
    last_error = None
    for url, headers, payload in attempts:
        sessions = {}
        seen = set()
        try:
            for page in range(1, MAX_CURSOR_USAGE_PAGES + 1):
                body = dict(payload)
                body["page"] = page
                data = _http_json_post(url, headers, body)
                rows = data.get("usageEventsDisplay") or data.get("usage_events") or []
                if not rows:
                    break
                new_on_page = 0
                for index, event in enumerate(rows):
                    kind = event.get("kind") or ""
                    if kind.endswith("_NOT_CHARGED"):
                        continue
                    usage = event.get("tokenUsage") or event.get("token_usage") or {}
                    inp = int(usage.get("inputTokens") or usage.get("input_tokens") or 0)
                    out = int(usage.get("outputTokens") or usage.get("output_tokens") or 0)
                    cache_read = int(usage.get("cacheReadTokens") or usage.get("cache_read_tokens") or 0)
                    cache_write = int(usage.get("cacheWriteTokens") or usage.get("cache_write_tokens") or 0)
                    # Card total historically ignored cache reads (old bubbles had none).
                    total, fresh = _token_pair(inp, out, 0, cache_write)
                    if total <= 0:
                        continue
                    stamp = event.get("timestamp")
                    cid = event.get("conversationId") or event.get("conversation_id") or "api:%s:%s:%s" % (
                        stamp, event.get("model") or "auto", index
                    )
                    event_key = (stamp, cid, kind, inp, out, cache_read, cache_write)
                    if event_key in seen:
                        continue
                    seen.add(event_key)
                    new_on_page += 1
                    day = _ms_day(stamp)
                    slot = sessions.setdefault(cid, {"all": 0, "fresh": 0, "day": day})
                    slot["all"] += total
                    slot["fresh"] += fresh
                    if day and (not slot["day"] or day < slot["day"]):
                        slot["day"] = day
                if new_on_page == 0 or len(rows) < CURSOR_USAGE_PAGE:
                    break
            if sessions:
                return sessions
            last_error = "empty"
        except urllib.error.HTTPError as exc:
            last_error = "HTTP %s" % exc.code
        except (urllib.error.URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
            last_error = str(exc.reason) if isinstance(exc, urllib.error.URLError) else str(exc)
    if last_error:
        print("cursor dashboard skipped: %s" % last_error)
    return {}


def _open_sqlite_ro(path):
    """Read-only connection. Copies path to a temp file only if the direct open fails."""
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        con.execute("SELECT 1")
        return con, None
    except sqlite3.Error:
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        shutil.copyfile(path, tmp)
        con = sqlite3.connect(f"file:{tmp}?mode=ro", uri=True)
        return con, tmp


def _merge_session(sessions, cid, total, fresh, day):
    slot = sessions.get(cid)
    if slot is None:
        sessions[cid] = {"all": total, "fresh": fresh, "day": day}
        return
    if total > slot["all"]:
        slot["all"] = total
        slot["fresh"] = max(slot["fresh"], fresh)
        if day and (not slot["day"] or day < slot["day"]):
            slot["day"] = day
    else:
        slot["fresh"] = max(slot["fresh"], fresh)


def scan_cursor(home):
    """Per conversation {all, fresh, day} from bubbles, then the usage dashboard.

    Newer Cursor builds leave bubble tokenCount at 0. Dashboard events fill that gap.
    GitHub Actions has no local DB; CURSOR_TOKEN then drives the dashboard alone.
    A locked database yields {} so the upload job still runs.
    """
    path = os.path.join(home, "Library", "Application Support", "Cursor", "User", "globalStorage", "state.vscdb")
    sessions = {}
    token = _cursor_env_token()
    if os.path.isfile(path):
        tmp = None
        try:
            con, tmp = _open_sqlite_ro(path)
        except (OSError, sqlite3.Error):
            con = None
            tmp = None
        if con is not None:
            try:
                try:
                    row = con.execute(
                        "SELECT value FROM ItemTable WHERE key = ?", ("cursorAuth/accessToken",)
                    ).fetchone()
                    if row and row[0]:
                        token = row[0].decode("utf-8", "replace") if isinstance(row[0], bytes) else row[0]
                except sqlite3.Error:
                    pass
                try:
                    rows = con.execute("SELECT key, value FROM cursorDiskKV WHERE key LIKE 'bubbleId:%'")
                except sqlite3.Error:
                    rows = []
                for key, raw in rows:
                    parts = key.split(":")
                    if len(parts) < 3:
                        continue
                    usage = _bubble_usage(raw)
                    if not usage:
                        continue
                    total, fresh, day = usage
                    slot = sessions.setdefault(parts[1], {"all": 0, "fresh": 0, "day": day})
                    slot["all"] += total
                    slot["fresh"] += fresh
                    if day and (not slot["day"] or day < slot["day"]):
                        slot["day"] = day
            except (sqlite3.Error, OSError, ValueError):
                sessions = {}
            finally:
                con.close()
                if tmp:
                    os.remove(tmp)
    if token:
        for cid, counts in _scan_cursor_dashboard(token).items():
            _merge_session(sessions, cid, counts["all"], counts["fresh"], counts.get("day"))
    return sessions


def bump_tokens_from_dashboard(path, token, now=None):
    """Add Cursor events newer than path's `updated` stamp. Never lowers the file.

    Used on GitHub Actions, which has no Antigravity/Codex logs. A full rescan
    there would drop those totals. Incremental dashboard events keep the floor.
    """
    with open(path, encoding="utf-8") as fh:
        existing = json.load(fh)
    start = _iso_to_ms(existing.get("updated"))
    extra = _scan_cursor_dashboard(token, str(start + 1) if start is not None else None)
    add_all = sum(item["all"] for item in extra.values())
    add_fresh = sum(item["fresh"] for item in extra.values())
    if add_all <= 0:
        return existing, False
    now = now or datetime.now(timezone.utc)
    return {
        "total": int(existing["total"]) + add_all,
        "fresh": int(existing["fresh"]) + add_fresh,
        "since": existing.get("since"),
        "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }, True


def _proto_fields(data, depth=0):
    """Protobuf wire fields as (number, wire, varint, nested-or-bytes), or None if the blob is junk."""
    if depth > 8 or not data:
        return None
    out = []
    i = 0
    n = len(data)

    def varint(at):
        val = shift = 0
        while at < n:
            byte = data[at]
            at += 1
            val |= (byte & 0x7F) << shift
            if byte < 128:
                return val, at
            shift += 7
            if shift > 63:
                return None
        return None

    while i < n:
        got = varint(i)
        if not got:
            return None
        tag, i = got
        number, wire = tag >> 3, tag & 7
        if number == 0:
            return None
        if wire == 0:
            got = varint(i)
            if not got:
                return None
            val, i = got
            out.append((number, 0, val, None))
        elif wire == 2:
            got = varint(i)
            if not got:
                return None
            length, i = got
            if i + length > n:
                return None
            blob = data[i:i + length]
            i += length
            nested = _proto_fields(blob, depth + 1)
            out.append((number, 2, None, nested if nested else blob))
        elif wire == 1:
            if i + 8 > n:
                return None
            i += 8
        elif wire == 5:
            if i + 4 > n:
                return None
            i += 4
        else:
            return None
    return out


def _antigravity_usage(blob):
    """(input, output) from a gen_metadata blob. Field 3 is reasoning inside field 2; field 1 is a flat prefix."""
    fields = _proto_fields(blob)
    if not fields:
        return None
    envelope = next((item for item in fields if item[0] == 1 and isinstance(item[3], list)), None)
    if not envelope:
        return None
    usage = next((item for item in envelope[3] if item[0] == 4 and isinstance(item[3], list)), None)
    if not usage:
        return None
    counts = {num: val for num, wire, val, _ in usage[3] if wire == 0}
    return counts.get(5, 0), counts.get(2, 0)


def _file_day(path):
    st = os.stat(path)
    birth = getattr(st, "st_birthtime", None) or st.st_mtime
    return datetime.fromtimestamp(birth, timezone.utc).strftime("%Y-%m-%d")


def scan_antigravity(home):
    """Per session db {all, fresh, day} from Antigravity's gen_metadata table.

    fresh equals all: these blobs have no cache-read field. Unreadable files are skipped.
    """
    roots = (
        ".gemini/antigravity-ide/conversations",
        ".gemini/antigravity/conversations",
        ".gemini/antigravity-cli/conversations",
    )
    sessions = {}
    for rel in roots:
        root = os.path.join(home, rel)
        if not os.path.isdir(root):
            continue
        for path in sorted(glob.glob(os.path.join(root, "*.db"))):
            try:
                con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            except sqlite3.Error:
                continue
            try:
                tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if "gen_metadata" not in tables:
                    continue
                total = 0
                for (blob,) in con.execute("SELECT data FROM gen_metadata"):
                    usage = _antigravity_usage(blob or b"")
                    if not usage:
                        continue
                    total += usage[0] + usage[1]
                if total <= 0:
                    continue
                sessions[os.path.splitext(os.path.basename(path))[0]] = {
                    "all": total, "fresh": total, "day": _file_day(path),
                }
            except (sqlite3.Error, OSError, ValueError):
                continue
            finally:
                con.close()
    return sessions


def scan_stats_cache(home):
    """Lump totals from ~/.claude/stats-cache.json, or None if it is missing."""
    path = os.path.join(home, ".claude", "stats-cache.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    total = cache_read = 0
    for counters in (data.get("modelUsage") or {}).values():
        for key, value in counters.items():
            # maxOutputTokens is a limit, not usage
            if key.endswith("Tokens") and not key.startswith("max") and isinstance(value, (int, float)) and not isinstance(value, bool):
                total += int(value)
                if key == "cacheReadInputTokens":
                    cache_read += int(value)
    return {
        "through": (data.get("lastComputedDate") or "")[:10],
        "since": (data.get("firstSessionDate") or "")[:10],
        "all": total,
        "fresh": total - cache_read,
    }


def load_ledger(path):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    return {"version": LEDGER_VERSION, "stats_cache": None, "codex": {}, "claude": {}, "cursor": {}, "antigravity": {}}


def merge_counts(stored, fresh):
    """Per-field max so a source that shrank (deleted transcripts) never lowers the ledger."""
    for key, counts in fresh.items():
        current = stored.setdefault(key, {})
        for field, value in counts.items():
            if isinstance(value, (int, float)):
                current[field] = max(current.get(field, value), value)
            elif field not in current:
                current[field] = value


def update_ledger(ledger, claude, codex, stats_cache, cursor=None, antigravity=None):
    # The stats cache is a frozen baseline: record it once and never touch it again.
    if ledger.get("stats_cache") is None and stats_cache is not None:
        ledger["stats_cache"] = stats_cache
    merge_counts(ledger.setdefault("claude", {}), claude)
    merge_counts(ledger.setdefault("codex", {}), codex)
    merge_counts(ledger.setdefault("cursor", {}), cursor or {})
    merge_counts(ledger.setdefault("antigravity", {}), antigravity or {})
    return ledger


def save_ledger(ledger, path):
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix="ledger.", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(ledger, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, path)


def seed_from(published):
    """Frozen baseline from the published totals, or None if they are unusable."""
    total, fresh, updated = published.get("total"), published.get("fresh"), published.get("updated")
    if not (isinstance(total, int) and isinstance(fresh, int) and isinstance(updated, str) and len(updated) >= 10):
        return None
    return {"through": updated[:10], "since": published.get("since"), "all": total, "fresh": fresh}


def summarize(ledger):
    """Return (per-source {all, fresh}, overall {all, fresh}, earliest day)."""
    seed = ledger.get("published")
    # A published seed already covers every source up to its day, the stats cache included
    base = seed or ledger.get("stats_cache") or {}
    through = base.get("through") or ""
    sources = {
        "published" if seed else "stats_cache": {"all": base.get("all", 0), "fresh": base.get("fresh", 0)},
        "claude": {"all": 0, "fresh": 0},
        "codex": {"all": 0, "fresh": 0},
        "cursor": {"all": 0, "fresh": 0},
        "antigravity": {"all": 0, "fresh": 0},
    }
    days = [base["since"]] if base.get("since") else []
    for day, counts in ledger.get("claude", {}).items():
        days.append(day)
        if day <= through:
            continue
        sources["claude"]["all"] += counts["all"]
        sources["claude"]["fresh"] += counts["fresh"]
    for counts in ledger.get("codex", {}).values():
        if counts.get("day"):
            days.append(counts["day"])
        if seed and (counts.get("day") or "") <= through:
            continue
        sources["codex"]["all"] += counts["all"]
        sources["codex"]["fresh"] += counts["fresh"]
    for name in ("cursor", "antigravity"):
        for counts in ledger.get(name, {}).values():
            if counts.get("day"):
                days.append(counts["day"])
            if seed and (counts.get("day") or "") <= through:
                continue
            sources[name]["all"] += counts["all"]
            sources[name]["fresh"] += counts["fresh"]
    totals = {
        "all": sum(s["all"] for s in sources.values()),
        "fresh": sum(s["fresh"] for s in sources.values()),
    }
    return sources, totals, min(days) if days else None


def compute(home, ledger_path, published=None):
    ledger = load_ledger(ledger_path)
    update_ledger(
        ledger,
        scan_claude(home),
        scan_codex(home),
        scan_stats_cache(home),
        scan_cursor(home),
        scan_antigravity(home),
    )
    result = summarize(ledger)
    # Counting less than what is published means the ledger was lost and rebuilt from
    # fewer logs. Start from the published total so uploads resume with the next day's usage.
    seed = seed_from(published) if published and not ledger.get("published") else None
    if seed and seed["all"] > result[1]["all"]:
        ledger["published"] = seed
        result = summarize(ledger)
    save_ledger(ledger, ledger_path)
    return result


def build_output(totals, since, now=None):
    now = now or datetime.now(timezone.utc)
    return {
        "total": int(totals["all"]),
        "fresh": int(totals["fresh"]),
        "since": since,
        "updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def render(output):
    return json.dumps(output, indent=2) + "\n"


def write_output(output, path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(render(output))


def summary_line(sources, totals, since):
    parts = [f"{name} {c['all']}/{c['fresh']}" for name, c in sources.items()]
    parts.append(f"total {totals['all']}/{totals['fresh']} since {since}")
    return "  ".join(parts)


def find_gh():
    gh = shutil.which("gh")
    if gh:
        return gh
    for candidate in GH_FALLBACKS:
        if os.path.exists(candidate):
            return candidate
    raise SystemExit("gh CLI not found")


def gh_api(gh, args, body=None):
    cmd = [gh, "api"] + args
    if body is not None:
        cmd += ["--input", "-"]
    return subprocess.run(cmd, input=body, capture_output=True, text=True)


def fetch_published(gh):
    """(blob sha, parsed JSON) of the published file, or None if it is not on the branch yet."""
    got = gh_api(gh, [f"{ENDPOINT}?ref={BRANCH}"])
    if got.returncode != 0:
        if "404" in got.stderr:
            return None
        raise SystemExit(f"gh api GET failed: {got.stderr.strip()}")
    remote = json.loads(got.stdout)
    try:
        current = json.loads(base64.b64decode(remote.get("content") or "").decode())
    except ValueError:
        current = {}
    return remote["sha"], current


def upload(gh, published, output):
    if published is None:
        print(f"{REMOTE_PATH} is not on {BRANCH} yet; skipping upload")
        return
    sha, current = published
    # Never make the card go down
    if output["total"] <= current.get("total", 0):
        print(f"remote total {current.get('total')} is not below {output['total']}; skipping upload")
        return
    body = json.dumps({
        "message": "Update token spend",
        "content": base64.b64encode(render(output).encode()).decode(),
        "sha": sha,
        "branch": BRANCH,
    })
    put = gh_api(gh, ["-X", "PUT", ENDPOINT], body=body)
    if put.returncode != 0:
        raise SystemExit(f"gh api PUT failed: {put.stderr.strip()}")
    print(f"uploaded {REMOTE_PATH} to {BRANCH}")


def plist_path(home):
    return os.path.join(home, "Library", "LaunchAgents", f"{LABEL}.plist")


def install(home):
    import plistlib  # only needed here; the homebrew python3 on this Mac fails to import it

    dest_dir = support_dir(home)
    os.makedirs(dest_dir, exist_ok=True)
    script = os.path.join(dest_dir, "token_spend.py")
    if os.path.abspath(__file__) != os.path.abspath(script):
        shutil.copyfile(os.path.abspath(__file__), script)
    log = os.path.join(home, "Library", "Logs", "token-spend.log")
    plist = plist_path(home)
    os.makedirs(os.path.dirname(plist), exist_ok=True)
    with open(plist, "wb") as fh:
        plistlib.dump({
            "Label": LABEL,
            # /usr/bin/python3 survives Homebrew and Xcode upgrades
            "ProgramArguments": [SYSTEM_PYTHON if os.path.exists(SYSTEM_PYTHON) else os.path.abspath(sys.executable), script, "--upload"],
            "StartInterval": 21600,
            "RunAtLoad": True,
            "StandardOutPath": log,
            "StandardErrorPath": log,
            "EnvironmentVariables": {"PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"},
        }, fh)
    uid = os.getuid()
    subprocess.run(["launchctl", "bootout", f"gui/{uid}/{LABEL}"], capture_output=True)
    subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", plist], check=True)
    print(f"installed {plist}")


def uninstall(home):
    plist = plist_path(home)
    subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}/{LABEL}"], capture_output=True)
    if os.path.exists(plist):
        os.remove(plist)
    print(f"removed {plist}")


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--write", metavar="PATH", help="write tokens.json to PATH")
    parser.add_argument("--upload", action="store_true", help="publish tokens.json to GitHub via gh")
    parser.add_argument("--install", action="store_true", help="install the 6-hourly launchd job")
    parser.add_argument("--uninstall", action="store_true", help="remove the launchd job")
    parser.add_argument("--ledger", metavar="PATH", help="ledger file (default: Application Support)")
    parser.add_argument("--home", metavar="PATH", help="home directory to scan (default: ~)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    home = os.path.abspath(os.path.expanduser(args.home)) if args.home else os.path.expanduser("~")
    if args.install:
        return install(home)
    if args.uninstall:
        return uninstall(home)
    if args.write and os.environ.get("GITHUB_ACTIONS") and os.path.isfile(args.write):
        token = _cursor_env_token()
        if not token:
            print("CURSOR_TOKEN unset; leaving %s unchanged" % args.write)
            return
        output, changed = bump_tokens_from_dashboard(args.write, token)
        print("tokens file %s -> %s" % (changed, output.get("total")))
        if changed:
            write_output(output, args.write)
        return
    ledger_path = args.ledger or os.path.join(support_dir(home), "ledger.json")
    gh = published = None
    if args.upload:
        gh = find_gh()
        published = fetch_published(gh)
    sources, totals, since = compute(home, ledger_path, published and published[1])
    print(summary_line(sources, totals, since))
    output = build_output(totals, since)
    if args.write:
        write_output(output, args.write)
    if args.upload:
        upload(gh, published, output)


if __name__ == "__main__":
    main()
