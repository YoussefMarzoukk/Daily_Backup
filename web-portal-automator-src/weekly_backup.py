#!/usr/bin/env python3
"""
Simple weekly Google‑Drive backup
────────────────────────────────
• Keeps only the newest KEEP_SNAPSHOTS dated folders to free space.
• Copies FOLDER_SOURCES (recursively) and FILE_SOURCES into
        WEEKLY_DEST_FOLDER/<DD.MM.YYYY>
• If Drive reports 'storageQuotaExceeded' once, the run stops gracefully.
"""

import os, sys, json, logging, time
from datetime import datetime
from pathlib import Path
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─── basic configuration ───────────────────────────────────────
SCOPES         = ["https://www.googleapis.com/auth/drive"]
CREDS_FILE     = Path(__file__).with_name("credentials.json")

DEST_PARENT    = os.getenv("WEEKLY_DEST_FOLDER")           # required
KEEP_SNAPSHOTS = int(os.getenv("KEEP_SNAPSHOTS", "3"))

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
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("backup")

creds = service_account.Credentials.from_service_account_file(
    CREDS_FILE, scopes=SCOPES
)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

# ─── helpers ───────────────────────────────────────────────────
def reason(err: HttpError) -> str:
    try:
        return json.loads(err.content.decode())["error"]["errors"][0]["reason"]
    except Exception:
        return "unknown"

def list_children(folder_id: str):
    page = None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken,files(id,name,mimeType,shortcutDetails/targetId,"
                   "shortcutDetails/targetMimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=page,
        ).execute()
        for f in resp.get("files", []):
            yield f
        page = resp.get("nextPageToken")
        if not page:
            break

def create_folder(name: str, parent: str) -> str:
    return drive.files().create(
        body={"name": name,
              "mimeType": "application/vnd.google-apps.folder",
              "parents": [parent]},
        fields="id",
        supportsAllDrives=True,
    ).execute()["id"]

def copy_file(fid: str, new_name: str, parent: str):
    drive.files().copy(
        fileId=fid,
        body={"name": new_name, "parents": [parent]},
        supportsAllDrives=True,
    ).execute()

def trash(file_id: str):
    drive.files().update(
        fileId=file_id,
        body={"trashed": True},
        supportsAllDrives=True,
    ).execute()

# ─── purge old snapshots ───────────────────────────────────────
def purge(parent: str, keep: int):
    snaps = drive.files().list(
        q=f"'{parent}' in parents and mimeType='application/vnd.google-apps.folder' "
          f"and trashed=false",
        fields="files(id,name,createdTime)",
        supportsAllDrives=True,
    ).execute()["files"]
    old = sorted(snaps, key=lambda f: f["createdTime"], reverse=True)[keep:]
    for f in old:
        try:
            log.info("Trashing old snapshot %s", f["name"])
            trash(f["id"])
        except HttpError as e:
            log.warning("Cannot trash %s – %s", f["name"], reason(e))

# ─── recursive copy (sequential) ───────────────────────────────
def copy_tree(src: str, dst_parent: str) -> bool:
    """Returns False if storage quota exceeded (abort signal)."""
    src_meta = drive.files().get(
        fileId=src, fields="name", supportsAllDrives=True
    ).execute()
    dst_id = create_folder(src_meta["name"], dst_parent)

    stack = [(src, dst_id)]
    while stack:
        cur_src, cur_dst = stack.pop()
        for it in list_children(cur_src):
            mt = it["mimeType"]
            try:
                if mt == "application/vnd.google-apps.folder":
                    new_dst = create_folder(it["name"], cur_dst)
                    stack.append((it["id"], new_dst))
                elif mt == "application/vnd.google-apps.shortcut":
                    tgt = it["shortcutDetails"]["targetId"]
                    tgt_mt = it["shortcutDetails"]["targetMimeType"]
                    alias = it["name"] + " (shortcut)"
                    if tgt_mt == "application/vnd.google-apps.folder":
                        new_dst = create_folder(alias, cur_dst)
                        stack.append((tgt, new_dst))
                    else:
                        copy_file(tgt, alias, cur_dst)
                else:
                    copy_file(it["id"], it["name"], cur_dst)
            except HttpError as e:
                if reason(e) == "storageQuotaExceeded":
                    log.warning("Drive quota full – aborting further copies.")
                    return False
                log.warning("Skip %s – %s", it["name"], reason(e))
    return True

# ─── main ──────────────────────────────────────────────────────
def main():
    if not DEST_PARENT:
        log.error("WEEKLY_DEST_FOLDER env var not set")
        sys.exit(1)

    purge(DEST_PARENT, KEEP_SNAPSHOTS)

    today = datetime.utcnow().strftime("%d.%m.%Y")
    backup_root = create_folder(today, DEST_PARENT)
    log.info("Created snapshot folder %s", today)

    # folders
    for fid in FOLDER_SOURCES:
        try:
            if not copy_tree(fid, backup_root):
                sys.exit(0)
        except HttpError as e:
            log.warning("Skip folder %s – %s", fid, reason(e))

    # single files
    for fid in FILE_SOURCES:
        try:
            meta = drive.files().get(
                fileId=fid, fields="name", supportsAllDrives=True
            ).execute()
            copy_file(fid, meta["name"], backup_root)
        except HttpError as e:
            if reason(e) == "storageQuotaExceeded":
                log.warning("Drive quota full – aborting further copies.")
                break
            log.warning("Skip file %s – %s", fid, reason(e))

    log.info("✅ Backup finished.")

if __name__ == "__main__":
    main()
