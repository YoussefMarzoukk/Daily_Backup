#!/usr/bin/env python3
"""
Robust Google‑Drive backup
──────────────────────────
• Copies the folder IDs in FOLDER_SOURCES and the file IDs in FILE_SOURCES
  into  DEST_PARENT / <DD.MM.YYYY>
• Pure Drive API (server‑side copies) – no rclone needed.
• Shared‑Drives supported, shortcuts resolved, rate‑limit retries.
"""

from __future__ import annotations
import os, sys, time, logging, functools, concurrent.futures as cf
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─────── configuration ──────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

DEST_PARENT   = os.getenv("WEEKLY_DEST_FOLDER")          # required
CLEAN_DEST    = os.getenv("CLEAN_DEST", "0") == "1"      # optional

# six large folders
FOLDER_SOURCES: list[str] = [
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    "1-sVtj8AdMB7pQAadjB9_CUmQ67gOXswi",
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP",
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
]

# seven standalone spreadsheets
FILE_SOURCES: list[str] = [
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",
    "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
    "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
    "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
]

MAX_WORKERS = 10                    # concurrent Drive requests
# ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("backup")


# ─────── Drive helpers ──────────────────────────────────────────
def drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def retry(fn):
    """Retry decorator for Drive quota errors."""
    @functools.wraps(fn)
    def _wrap(*a, **kw):
        delay = 1
        for attempt in range(6):
            try:
                return fn(*a, **kw)
            except HttpError as e:
                if e.resp.status in (403, 429):
                    time.sleep(delay)
                    delay = min(delay * 2, 64)
                    continue
                raise
    return _wrap


@retry
def gcopy(drv, file_id: str, *, name: str, parent: str):
    return drv.files().copy(
        fileId=file_id,
        body={"name": name, "parents": [parent]},
        supportsAllDrives=True,
    ).execute()


@retry
def gcreate_folder(drv, name: str, parent: str) -> str:
    return drv.files().create(
        body={
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent],
        },
        fields="id",
        supportsAllDrives=True,
    ).execute()["id"]


@retry
def glist_children(drv, folder_id: str, page_token: str | None):
    return drv.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields=(
            "nextPageToken,files(id,name,mimeType,shortcutDetails/targetId,"
            "shortcutDetails/targetMimeType)"
        ),
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=1000,
        pageToken=page_token,
    ).execute()


# ─────── recursive folder copy ─────────────────────────────────
def copy_folder_tree(drv, src_id: str, dst_parent: str):
    meta = drv.files().get(
        fileId=src_id, fields="name", supportsAllDrives=True
    ).execute()
    dst_id = gcreate_folder(drv, meta["name"], dst_parent)
    log.info("↪ %s  →  %s", meta["name"], dst_id)

    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        _copy_folder_contents(pool, drv, src_id, dst_id)


def _copy_folder_contents(pool, drv, src_folder: str, dst_folder: str):
    page = None
    while True:
        resp = glist_children(drv, src_folder, page)
        for f in resp.get("files", []):
            mt = f["mimeType"]
            if mt == "application/vnd.google-apps.folder":
                _copy_folder_contents(pool, drv, f["id"],
                                      gcreate_folder(drv, f["name"], dst_folder))
            elif mt == "application/vnd.google-apps.shortcut":
                tgt_mt = f["shortcutDetails"]["targetMimeType"]
                tgt_id = f["shortcutDetails"]["targetId"]
                name   = f["name"]
                if tgt_mt == "application/vnd.google-apps.folder":
                    # recurse into shortcut target folder
                    _copy_folder_contents(
                        pool, drv, tgt_id,
                        gcreate_folder(drv, name + " (shortcut)", dst_folder)
                    )
                else:
                    pool.submit(gcopy, drv, tgt_id, name=name, parent=dst_folder)
            else:
                pool.submit(gcopy, drv, f["id"], name=f["name"], parent=dst_folder)
        page = resp.get("nextPageToken")
        if not page:
            break


# ─────── utilities ────────────────────────────────────────────
def delete_folder_recursive(drv, folder_id: str):
    page = None
    while True:
        resp = glist_children(drv, folder_id, page)
        for f in resp.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                delete_folder_recursive(drv, f["id"])
            else:
                drv.files().delete(fileId=f["id"]).execute()
        page = resp.get("nextPageToken")
        if not page:
            break
    drv.files().delete(fileId=folder_id).execute()


def ensure_dated_folder(drv, parent_id: str, today: str) -> str:
    q = (
        f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' "
        f"and name='{today}' and trashed=false"
    )
    res = drv.files().list(q=q, fields="files(id)", supportsAllDrives=True).execute()
    if res.get("files"):
        return res["files"][0]["id"]
    return gcreate_folder(drv, today, parent_id)


def access_ok(drv, file_id: str) -> tuple[bool, str]:
    try:
        m = drv.files().get(fileId=file_id, fields="name", supportsAllDrives=True).execute()
        return True, m["name"]
    except HttpError as e:
        reason = "not found" if e.resp.status == 404 else "forbidden"
        return False, reason


# ─────── main ─────────────────────────────────────────────────
def main() -> None:
    if not DEST_PARENT:
        log.error("Env var WEEKLY_DEST_FOLDER not set.")
        sys.exit(1)

    drv = drive_service()

    today = datetime.utcnow().strftime("%d.%m.%Y")
    if CLEAN_DEST:
        q = f"'{DEST_PARENT}' in parents and name='{today}' and trashed=false"
        res = drv.files().list(q=q, fields="files(id)", supportsAllDrives=True).execute()
        if res.get("files"):
            log.info("Purging existing folder %s …", today)
            delete_folder_recursive(drv, res["files"][0]["id"])

    backup_root = ensure_dated_folder(drv, DEST_PARENT, today)
    log.info("Backup destination : %s (id=%s)", today, backup_root)

    skipped: List[str] = []

    # ── copy folder trees ──────────────────────────────
    for fid in FOLDER_SOURCES:
        ok, info = access_ok(drv, fid)
        if not ok:
            skipped.append(f"folder {fid} – {info}")
            log.warning("SKIP folder %s : %s", fid, info)
            continue
        try:
            copy_folder_tree(drv, fid, backup_root)
        except Exception as e:
            skipped.append(f"folder {info} – {e}")
            log.warning("Error copying %s : %s", info, e)

    # ── copy individual files ─────────────────────────
    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        futures = []
        for fid in FILE_SOURCES:
            ok, info = access_ok(drv, fid)
            if not ok:
                skipped.append(f"file {fid} – {info}")
                log.warning("SKIP file   %s : %s", fid, info)
                continue
            futures.append(pool.submit(gcopy, drv, fid, name=info, parent=backup_root))
        for f in cf.as_completed(futures):
            pass

    # ── summary ───────────────────────────────────────
    log.info("Backup completed.")
    if skipped:
        log.warning("Items not copied (%d):", len(skipped))
        for s in skipped:
            log.warning(" • %s", s)


if __name__ == "__main__":
    main()
