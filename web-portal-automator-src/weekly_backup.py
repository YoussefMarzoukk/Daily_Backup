#!/usr/bin/env python3
"""
Server‑side Google‑Drive backup
───────────────────────────────
• Recursively clones all folder IDs in FOLDER_SOURCES and the single
  files in FILE_SOURCES into  WEEKLY_DEST_FOLDER / <DD.MM.YYYY>.
• Uses only the Drive API (files().copy) – zero download/upload traffic.
• Thread‑safe: every worker keeps its own Drive service → no SSL crashes.
• Automatic retries on 403/429 with exponential back‑off.
"""

from __future__ import annotations
import os, sys, time, logging, threading, functools, concurrent.futures as cf
from datetime import datetime
from pathlib import Path
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─── configuration ─────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

DEST_PARENT = os.getenv("WEEKLY_DEST_FOLDER")       # required
CLEAN_DEST  = os.getenv("CLEAN_DEST", "0") == "1"   # optional purge

# six folder trees to back up (recursively)
FOLDER_SOURCES = [
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    "1-sVtj8AdMB7pQAadjB9_CUmQ67gOXswi",
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP",
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
]

# seven stand‑alone spreadsheets (or any files)
FILE_SOURCES = [
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",
    "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
    "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
    "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
]

MAX_WORKERS = 4            # <─ 4 keeps 403 warnings almost at zero
# ───────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("backup")

# ─── thread‑local Drive service ────────────────────────────────
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


# ─── retry decorator (403/429 + SSL) ───────────────────────────
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
                # OpenSSL or connection reset
                if attempt == 7:
                    raise
                log.warning("Transport error (%s) – retrying …", e)
                _tls.drv = _build_drive()
                time.sleep(delay)
                delay = min(delay * 2, 32)
    return _wrap


# ─── thin API wrappers ─────────────────────────────────────────
@retry
def gcopy(file_id: str, *, name: str, parent: str):
    drv().files().copy(
        fileId=file_id,
        body={"name": name, "parents": [parent]},
        supportsAllDrives=True,
    ).execute()


@retry
def gcreate_folder(name: str, parent: str) -> str:
    return (
        drv()
        .files()
        .create(
            body={
                "name": name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent],
            },
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


# ─── recursive copy logic ──────────────────────────────────────
def copy_tree(src_id: str, dst_parent: str):
    meta = drv().files().get(fileId=src_id, fields="name",
                             supportsAllDrives=True).execute()
    dst_id = gcreate_folder(meta["name"], dst_parent)
    log.info("↪ %s  →  %s", meta["name"], dst_id)

    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        _copy_contents(pool, src_id, dst_id)


def _copy_contents(pool: cf.ThreadPoolExecutor, src: str, dst: str):
    page = None
    while True:
        resp = glist(src, page)
        for f in resp.get("files", []):
            mt = f["mimeType"]
            if mt == "application/vnd.google-apps.folder":
                new_dst = gcreate_folder(f["name"], dst)
                _copy_contents(pool, f["id"], new_dst)
            elif mt == "application/vnd.google-apps.shortcut":
                tgt_id = f["shortcutDetails"]["targetId"]
                tgt_mt = f["shortcutDetails"]["targetMimeType"]
                name   = f["name"] + " (shortcut)"
                if tgt_mt == "application/vnd.google-apps.folder":
                    new_dst = gcreate_folder(name, dst)
                    _copy_contents(pool, tgt_id, new_dst)
                else:
                    pool.submit(gcopy, tgt_id, name=name, parent=dst)
            else:
                pool.submit(gcopy, f["id"], name=f["name"], parent=dst)
        page = resp.get("nextPageToken")
        if not page:
            break


# ─── utilities ────────────────────────────────────────────────
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
        f"'{parent}' in parents and mimeType='application/vnd.google-apps.folder' "
        f"and name='{today}' and trashed=false"
    )
    r = drv().files().list(q=q, fields="files(id)", supportsAllDrives=True).execute()
    return r["files"][0]["id"] if r.get("files") else gcreate_folder(today, parent)


def access_ok(file_id: str):
    try:
        m = drv().files().get(fileId=file_id, fields="name",
                              supportsAllDrives=True).execute()
        return True, m["name"]
    except HttpError as e:
        return False, ("not found" if e.resp.status == 404 else "forbidden")


# ─── main ──────────────────────────────────────────────────────
def main():
    if not DEST_PARENT:
        log.error("WEEKLY_DEST_FOLDER env var is missing.")
        sys.exit(1)

    today = datetime.utcnow().strftime("%d.%m.%Y")

    if CLEAN_DEST:
        q = f"'{DEST_PARENT}' in parents and name='{today}' and trashed=false"
        r = drv().files().list(q=q, fields="files(id)",
                               supportsAllDrives=True).execute()
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
            skipped.append(f"folder {fid} – {info}")
            log.warning("SKIP folder %s : %s", fid, info)
            continue
        try:
            copy_tree(fid, backup_root)
        except Exception as e:
            skipped.append(f"{info} – {e}")
            log.warning("Error copying folder %s : %s", info, e)

    # single files
    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        futures = []
        for fid in FILE_SOURCES:
            ok, info = access_ok(fid)
            if not ok:
                skipped.append(f"file {fid} – {info}")
                log.warning("SKIP file %s : %s", fid, info)
                continue
            futures.append(pool.submit(gcopy, fid, name=info, parent=backup_root))
        for _ in cf.as_completed(futures):
            pass

    log.info("Backup finished.")
    if skipped:
        log.warning("Items skipped (%d):", len(skipped))
        for s in skipped:
            log.warning(" • %s", s)


if __name__ == "__main__":
    main()
