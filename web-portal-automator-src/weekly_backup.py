#!/usr/bin/env python3
"""
Fast, thread‑safe Google‑Drive backup
─────────────────────────────────────
• Recursively clones every folder ID in FOLDER_SOURCES (all sub‑folders
  and files) and every single file ID in FILE_SOURCES.
• Works entirely server‑side (files().copy) – no download/upload.
• Uses a thread‑pool; each thread owns its own Drive client to avoid SSL
  crashes.  MAX_WORKERS=8 is a good speed/quotas balance.  Raise if you
  want more throughput and can tolerate the occasional automatic retry.
"""

from __future__ import annotations
import os, sys, time, logging, threading, functools, concurrent.futures as cf
from datetime import datetime
from pathlib import Path
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── configuration ──────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

DEST_PARENT = os.getenv("WEEKLY_DEST_FOLDER")         # required
CLEAN_DEST  = os.getenv("CLEAN_DEST", "0") == "1"     # optional

FOLDER_SOURCES = [   # full‑tree copies
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    "1-sVtj8AdMB7pQAadjB9_CUmQ67gOXswi",
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP",
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
]

FILE_SOURCES = [     # stand‑alone spreadsheets
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",
    "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
    "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
    "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
]

MAX_WORKERS = 8   # <-- raise to go faster, lower to reduce Drive 403s
# ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("backup")

# ── thread‑local Drive client ──────────────────────────────────
_tls = threading.local()


def _build_drive():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def drv():
    if not hasattr(_tls, "drv"):
        _tls.drv = _build_drive()
    return _tls.drv


# ── retry decorator (403/429 + SSL) ────────────────────────────
def retry(fn):
    @functools.wraps(fn)
    def _wrap(*a, **kw):
        delay = 1
        for attempt in range(8):
            try:
                return fn(*a, **kw)
            except HttpError as e:
                if e.resp.status in (403, 429):
                    time.sleep(delay)
                    delay = min(delay * 2, 64)
                    continue
                raise
            except Exception as e:
                if attempt == 7:
                    raise
                log.warning("Transient error (%s) – retrying …", e)
                _tls.drv = _build_drive()
                time.sleep(delay)
                delay = min(delay * 2, 32)
    return _wrap


# ── thin API helpers ───────────────────────────────────────────
@retry
def gcopy(fid: str, *, name: str, parent: str):
    drv().files().copy(
        fileId=fid,
        body={"name": name, "parents": [parent]},
        supportsAllDrives=True,
    ).execute()


@retry
def gcreate_folder(name: str, parent: str) -> str:
    return (
        drv()
        .files()
        .create(
            body={"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]},
            fields="id",
            supportsAllDrives=True,
        )
        .execute()["id"]
    )


@retry
def glist(folder_id: str, page_token: str | None):
    return (
        drv()
        .files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields=(
                "nextPageToken,files(id,name,mimeType,shortcutDetails/targetId,"
                "shortcutDetails/targetMimeType)"
            ),
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=page_token,
        )
        .execute()
    )


# ── recursive copy ─────────────────────────────────────────────
def copy_tree(src_id: str, dst_parent: str):
    name = drv().files().get(fileId=src_id, fields="name", supportsAllDrives=True).execute()["name"]
    dst_id = gcreate_folder(name, dst_parent)
    log.info("↪ %s → %s", name, dst_id)

    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        _copy_contents(pool, src_id, dst_id)


def _copy_contents(pool: cf.ThreadPoolExecutor, src: str, dst: str):
    page = None
    while True:
        resp = glist(src, page)
        for f in resp.get("files", []):
            mt = f["mimeType"]
            if mt == "application/vnd.google-apps.folder":
                _copy_contents(pool, f["id"], gcreate_folder(f["name"], dst))
            elif mt == "application/vnd.google-apps.shortcut":
                tgt_id = f["shortcutDetails"]["targetId"]
                tgt_mt = f["shortcutDetails"]["targetMimeType"]
                alias = f["name"] + " (shortcut)"
                if tgt_mt == "application/vnd.google-apps.folder":
                    _copy_contents(pool, tgt_id, gcreate_folder(alias, dst))
                else:
                    pool.submit(gcopy, tgt_id, name=alias, parent=dst)
            else:
                pool.submit(gcopy, f["id"], name=f["name"], parent=dst)
        page = resp.get("nextPageToken")
        if not page:
            break


# ── misc helpers ───────────────────────────────────────────────
def delete_recursive(folder_id: str):
    page = None
    while True:
        resp = glist(folder_id, page)
        for f in resp.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                delete_recursive(f["id"])
            else:
                drv().files().delete(fileId=f["id"]).execute()
        page = resp.get("nextPageToken")
        if not page:
            break
    drv().files().delete(fileId=folder_id).execute()


def ensure_dated(parent: str, today: str) -> str:
    q = (
        f"'{parent}' in parents and name='{today}' and mimeType='application/vnd.google-apps.folder' "
        f"and trashed=false"
    )
    r = drv().files().list(q=q, fields="files(id)", supportsAllDrives=True).execute()
    return r["files"][0]["id"] if r.get("files") else gcreate_folder(today, parent)


def access_ok(fid: str):
    try:
        name = drv().files().get(fileId=fid, fields="name", supportsAllDrives=True).execute()["name"]
        return True, name
    except HttpError as e:
        return False, ("not found" if e.resp.status == 404 else "forbidden")


# ── entry‑point ────────────────────────────────────────────────
def main():
    if not DEST_PARENT:
        log.error("WEEKLY_DEST_FOLDER env var not set"); sys.exit(1)

    today = datetime.utcnow().strftime("%d.%m.%Y")

    if CLEAN_DEST:
        q = f"'{DEST_PARENT}' in parents and name='{today}' and trashed=false"
        r = drv().files().list(q=q, fields="files(id)", supportsAllDrives=True).execute()
        if r.get("files"):
            log.info("Purging previous snapshot …")
            delete_recursive(r["files"][0]["id"])

    backup_root = ensure_dated(DEST_PARENT, today)
    log.info("Backup destination : %s (id=%s)", today, backup_root)

    skipped: List[str] = []

    # folders
    for fid in FOLDER_SOURCES:
        ok, info = access_ok(fid)
        if not ok:
            skipped.append(f"{fid} – {info}"); log.warning("SKIP folder %s : %s", fid, info); continue
        try: copy_tree(fid, backup_root)
        except Exception as e:
            skipped.append(f"{info} – {e}"); log.warning("Error copying %s : %s", info, e)

    # files
    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        futures=[]
        for fid in FILE_SOURCES:
            ok, info = access_ok(fid)
            if not ok:
                skipped.append(f"{fid} – {info}"); log.warning("SKIP file %s : %s", fid, info); continue
            futures.append(pool.submit(gcopy, fid, name=info, parent=backup_root))
        for _ in cf.as_completed(futures): pass

    log.info("✅ Backup complete.")
    if skipped:
        log.warning("Items skipped (%d):", len(skipped))
        for s in skipped: log.warning(" • %s", s)


if __name__ == "__main__":
    main()
