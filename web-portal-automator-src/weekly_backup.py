#!/usr/bin/env python3
"""
Weekly Google‑Drive backup (thread‑safe, fast)

• Copies every folder ID in FOLDER_SOURCES (all sub‑folders & files) and
  every single file ID in FILE_SOURCES into
     WEEKLY_DEST_FOLDER / <DD.MM.YYYY>
• Uses server‑side `files().copy` – no data leaves Google’s backbone.
• Up to MAX_WORKERS parallel file copies; folder traversal is sequential
  to avoid giant thread storms.
"""

from __future__ import annotations
import os, sys, time, logging, threading, queue, functools, concurrent.futures as cf
from datetime import datetime
from pathlib import Path
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─── configuration ─────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

DEST_PARENT = os.getenv("WEEKLY_DEST_FOLDER")          # required
MAX_WORKERS = int(os.getenv("BACKUP_THREADS", "8"))    # tweak freely
PAGE_SIZE    = 1000                                    # Drive list page

FOLDER_SOURCES = [
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
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


def _build_drive():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def drv():
    if not hasattr(_tls, "drv"):
        _tls.drv = _build_drive()
    return _tls.drv


# ─── retry decorator (rate‑limit & transient errors) ───────────
def retry(fn):
    @functools.wraps(fn)
    def _wrap(*a, **kw):
        delay = 1
        for attempt in range(7):
            try:
                return fn(*a, **kw)
            except HttpError as e:
                if e.resp.status in (403, 429, 500, 503):
                    time.sleep(delay)
                    delay = min(delay * 2, 64)
                    continue
                raise
            except Exception as e:
                if attempt == 6:
                    raise
                log.warning("Transport error: %s – retrying …", e)
                _tls.drv = _build_drive()
                time.sleep(delay)
                delay = min(delay * 2, 32)
    return _wrap


# ─── Drive wrappers ────────────────────────────────────────────
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
            body={"name": name,
                  "mimeType": "application/vnd.google-apps.folder",
                  "parents": [parent]},
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
            fields="nextPageToken,files(id,name,mimeType,shortcutDetails/targetId,"
                   "shortcutDetails/targetMimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=PAGE_SIZE,
            pageToken=page_token,
        )
        .execute()
    )


# ─── sequential folder walk + parallel file copies ─────────────
def walk_and_schedule(src_folder: str, dst_folder: str,
                      pool: cf.ThreadPoolExecutor, skipped: list[str]):
    q: queue.Queue[tuple[str, str]] = queue.Queue()
    q.put((src_folder, dst_folder))

    while not q.empty():
        src, dst = q.get()
        page = None
        while True:
            resp = glist(src, page)
            for f in resp.get("files", []):
                mt = f["mimeType"]
                if mt == "application/vnd.google-apps.folder":
                    new_dst = gcreate_folder(f["name"], dst)
                    q.put((f["id"], new_dst))
                elif mt == "application/vnd.google-apps.shortcut":
                    tgt = f["shortcutDetails"]["targetId"]
                    tgt_mt = f["shortcutDetails"]["targetMimeType"]
                    alias = f["name"] + " (shortcut)"
                    if tgt_mt == "application/vnd.google-apps.folder":
                        new_dst = gcreate_folder(alias, dst)
                        q.put((tgt, new_dst))
                    else:
                        pool.submit(gcopy, tgt, name=alias, parent=dst)
                else:
                    pool.submit(gcopy, f["id"], name=f["name"], parent=dst)
            page = resp.get("nextPageToken")
            if not page:
                break


# ─── helpers ───────────────────────────────────────────────────
@retry
def get_name(fid: str):
    return drv().files().get(fileId=fid, fields="name",
                             supportsAllDrives=True).execute()["name"]


def ensure_dated_folder(parent: str, date_str: str) -> str:
    q = (f"'{parent}' in parents and name='{date_str}' "
         "and mimeType='application/vnd.google-apps.folder' and trashed=false")
    res = drv().files().list(q=q, fields="files(id)",
                             supportsAllDrives=True).execute()
    return res["files"][0]["id"] if res.get("files") else gcreate_folder(date_str, parent)


# ─── main ──────────────────────────────────────────────────────
def main():
    if not DEST_PARENT:
        log.error("WEEKLY_DEST_FOLDER env var not set"); sys.exit(1)

    today = datetime.utcnow().strftime("%d.%m.%Y")
    backup_root = ensure_dated_folder(DEST_PARENT, today)
    log.info("Backup destination: %s (id=%s)", today, backup_root)

    skipped: List[str] = []

    with cf.ThreadPoolExecutor(MAX_WORKERS) as pool:
        # folders first (schedule their file copies)
        for fid in FOLDER_SOURCES:
            try:
                name = get_name(fid)
                log.info("Begin folder %s", name)
                walk_and_schedule(fid, gcreate_folder(name, backup_root),
                                  pool, skipped)
                log.info("Done  folder %s", name)
            except HttpError as e:
                log.warning("Skip folder %s – %s", fid, e)
                skipped.append(f"folder {fid} – {e}")
        # single files
        futures = []
        for fid in FILE_SOURCES:
            try:
                name = get_name(fid)
                futures.append(pool.submit(gcopy, fid, name=name, parent=backup_root))
            except HttpError as e:
                skipped.append(f"file {fid} – {e}")
        for _ in cf.as_completed(futures):
            pass  # wait

    # summary
    log.info("Skipped items (%d):", len(skipped))
    for s in skipped:
        log.info(" • %s", s)
    log.info("✅ Backup complete with %d worker threads.", MAX_WORKERS)


if __name__ == "__main__":
    main()
