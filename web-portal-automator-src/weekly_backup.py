#!/usr/bin/env python3
"""
Weekly Google‑Drive backup  –  fast, resilient, self‑cleaning
──────────────────────────────────────────────────────────────
• Copies every folder ID in FOLDER_SOURCES (recursively) and every
  single file ID in FILE_SOURCES into
      WEEKLY_DEST_FOLDER / <DD.MM.YYYY>
• Deletes snapshots older than KEEP_SNAPSHOTS to stay below the
  15 GB free quota (or any quota you have).
• Skips copies that hit Drive 'storageQuotaExceeded' instead of retrying
  forever; other transient errors are retried with back‑off.
"""

from __future__ import annotations
import os, sys, time, json, logging, threading, queue, functools, concurrent.futures as cf
from datetime import datetime
from pathlib import Path
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─── configuration ─────────────────────────────────────────────
SCOPES          = ["https://www.googleapis.com/auth/drive"]
CREDS_FILE      = Path(__file__).with_name("credentials.json")

DEST_PARENT     = os.getenv("WEEKLY_DEST_FOLDER")      # required
MAX_WORKERS     = int(os.getenv("BACKUP_THREADS" , "8"))
KEEP_SNAPSHOTS  = int(os.getenv("KEEP_SNAPSHOTS", "3"))
PAGE_SIZE       = 1000

FOLDER_SOURCES = [
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP",
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
]
FILE_SOURCES = [
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",
    "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
    "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
    "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
]
# ───────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(threadName)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("backup")

# ─── thread‑local Drive client ─────────────────────────────────
_tls = threading.local()
def _drive():
    if not hasattr(_tls, "d"):
        creds = service_account.Credentials.from_service_account_file(
            CREDS_FILE, scopes=SCOPES
        )
        _tls.d = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _tls.d

# ─── helpers ───────────────────────────────────────────────────
def _error_reason(err: HttpError) -> str | None:
    try:
        reason = json.loads(err.content.decode())["error"]["errors"][0]["reason"]
        return reason
    except Exception:
        return None

def retry(fn):
    @functools.wraps(fn)
    def _wrap(*a, **kw):
        delay = 1
        for attempt in range(7):
            try:
                return fn(*a, **kw)
            except HttpError as e:
                r = _error_reason(e)
                if r in ("storageQuotaExceeded", "fileNotDownloadable"):   # don't retry
                    raise
                if e.resp.status in (403, 429, 500, 503):
                    time.sleep(delay)
                    delay = min(delay * 2, 64)
                    continue
                raise
            except Exception as e:
                if attempt == 6: raise
                log.warning("Transport error: %s – retrying …", e)
                _tls.d = None
                time.sleep(delay)
                delay = min(delay * 2, 32)
    return _wrap

# ─── thin Drive wrappers ───────────────────────────────────────
@retry
def gcopy(fid: str, *, name: str, parent: str):
    _drive().files().copy(
        fileId=fid,
        body={"name": name, "parents": [parent]},
        supportsAllDrives=True,
    ).execute()

@retry
def gcreate_folder(name: str, parent: str) -> str:
    return (
        _drive()
        .files()
        .create(body={"name": name,
                      "mimeType": "application/vnd.google-apps.folder",
                      "parents": [parent]},
                fields="id",
                supportsAllDrives=True)
        .execute()["id"]
    )

@retry
def glist(folder_id: str, page_token: str | None):
    return (
        _drive()
        .files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken,files(id,name,mimeType,shortcutDetails/targetId,"
                   "shortcutDetails/targetMimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=PAGE_SIZE,
            pageToken=page_token,
        )
        .execute()
    )

@retry
def get_name(fid: str) -> str:
    return _drive().files().get(fileId=fid, fields="name",
                                supportsAllDrives=True).execute()["name"]

# ─── snapshot clean‑up ─────────────────────────────────────────
def purge_old_snapshots(parent: str, keep: int):
    res = (
        _drive()
        .files()
        .list(
            q=f"'{parent}' in parents and mimeType='application/vnd.google-apps.folder' "
              f"and trashed=false",
            fields="files(id,name,createdTime)",
            supportsAllDrives=True,
        )
        .execute()["files"]
    )
    # dated names sort naturally dd.mm.yyyy
    to_delete = sorted(res, key=lambda f: f["createdTime"], reverse=True)[keep:]
    for f in to_delete:
        log.info("Deleting old snapshot %s", f["name"])
        _drive().files().delete(fileId=f["id"]).execute()

# ─── folder walk + scheduling copies ───────────────────────────
def walk_and_schedule(src: str, dst: str, pool: cf.Executor, skipped: list[str]):
    q: queue.Queue[tuple[str, str]] = queue.Queue()
    q.put((src, dst))
    while not q.empty():
        cur_src, cur_dst = q.get()
        page = None
        while True:
            resp = glist(cur_src, page)
            for f in resp.get("files", []):
                mt = f["mimeType"]
                if mt == "application/vnd.google-apps.folder":
                    q.put((f["id"], gcreate_folder(f["name"], cur_dst)))
                elif mt == "application/vnd.google-apps.shortcut":
                    tgt, tgt_mt = f["shortcutDetails"]["targetId"], f["shortcutDetails"]["targetMimeType"]
                    alias = f["name"] + " (shortcut)"
                    if tgt_mt == "application/vnd.google-apps.folder":
                        q.put((tgt, gcreate_folder(alias, cur_dst)))
                    else:
                        pool.submit(_safe_copy, tgt, alias, cur_dst, skipped)
                else:
                    pool.submit(_safe_copy, f["id"], f["name"], cur_dst, skipped)
            page = resp.get("nextPageToken")
            if not page: break

def _safe_copy(fid: str, name: str, parent: str, skipped: list[str]):
    try:
        gcopy(fid, name=name, parent=parent)
    except HttpError as e:
        reason = _error_reason(e)
        if reason == "storageQuotaExceeded":
            log.warning("%s – skipped (quota full)", name)
            skipped.append(f"{name} – quota full")
        else:
            log.warning("%s – skipped (%s)", name, reason or e)
            skipped.append(f"{name} – {reason or e}")

# ─── main ──────────────────────────────────────────────────────
def main():
    if not DEST_PARENT:
        log.error("WEEKLY_DEST_FOLDER env var not set"); sys.exit(1)

    # purge old snapshots to free space
    purge_old_snapshots(DEST_PARENT, KEEP_SNAPSHOTS)

    today = datetime.utcnow().strftime("%d.%m.%Y")
    backup_root = gcreate_folder(today, DEST_PARENT)
    log.info("Backup destination: %s (id=%s)", today, backup_root)

    skipped: List[str] = []
    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        # folders
        for fid in FOLDER_SOURCES:
            try:
                name = get_name(fid)
                log.info("Begin folder %s", name)
                walk_and_schedule(fid, gcreate_folder(name, backup_root), pool, skipped)
                log.info("Done  folder %s", name)
            except HttpError as e:
                log.warning("Skip folder %s – %s", fid, e)
                skipped.append(f"folder {fid} – {e}")

        # single files
        futures = []
        for fid in FILE_SOURCES:
            try:
                name = get_name(fid)
                futures.append(pool.submit(_safe_copy, fid, name, backup_root, skipped))
            except HttpError as e:
                skipped.append(f"file {fid} – {e}")
        for _ in cf.as_completed(futures): pass

    log.info("Skipped items (%d):", len(skipped))
    for s in skipped: log.info(" • %s", s)
    log.info("✅ Backup complete (%d worker threads).", MAX_WORKERS)

if __name__ == "__main__":
    main()
