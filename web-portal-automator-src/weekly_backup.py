#!/usr/bin/env python3
"""
Weekly Drive backup.

Copies the contents of each ID in SOURCE_FOLDER_IDS into a dated sub‑folder
(DD.MM.YYYY) under DEST_PARENT_ID.

Environment variables
---------------------
• GDRIVE_SERVICE_ACCOUNT_JSON : entire service‑account key (JSON string)
• WEEKLY_DEST_FOLDER          : override DEST_PARENT_ID (optional)
• CLEAN_DEST                  : "1" → delete dated folder before copy (optional)
"""
from __future__ import annotations
import os, json, sys
from datetime import datetime
from collections import deque
from typing import List, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ──────────────────────────────────────────────────────────────────────────────
DEST_PARENT_ID = "1q9spRw8OX_V9OXghNbmZ2a2PHSb07cgF"  # backup root (shared‑drive)
SOURCE_FOLDER_IDS = [
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    # add more …
]
# ──────────────────────────────────────────────────────────────────────────────

# ---------- authentication ----------
creds_json = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON")
if not creds_json:
    sys.exit("❌  GDRIVE_SERVICE_ACCOUNT_JSON secret not set")
creds = service_account.Credentials.from_service_account_info(
    json.loads(creds_json),
    scopes=["https://www.googleapis.com/auth/drive"],
)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)

# ---------- Drive call presets ----------
READ_FLAGS  = dict(supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1000)
WRITE_FLAGS = dict(supportsAllDrives=True)

# ---------- helpers ----------
def list_children(folder_id: str) -> List[Dict]:
    """Immediate (non‑trashed) children of a folder."""
    q = f"'{folder_id}' in parents and trashed=false"
    out, token = [], None
    while True:
        resp = (
            drive.files()
            .list(q=q, fields="nextPageToken,files(id,name,mimeType)", pageToken=token, **READ_FLAGS)
            .execute()
        )
        out.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            return out

def delete_tree(file_id: str):
    """Delete file/folder recursively."""
    meta = drive.files().get(fileId=file_id, fields="id,mimeType", **READ_FLAGS).execute()
    if meta["mimeType"] == "application/vnd.google-apps.folder":
        for ch in list_children(file_id):
            delete_tree(ch["id"])
    drive.files().delete(fileId=file_id, **WRITE_FLAGS).execute()

def find_existing(parent_id: str, name: str, mime: str) -> str | None:
    """Return ID of an item with same name & type under parent, else None."""
    safe_name = name.replace("'", r"\'")
    q = (
        f"'{parent_id}' in parents and name='{safe_name}' "
        f"and mimeType='{mime}' and trashed=false"
    )
    resp = drive.files().list(q=q, fields="files(id)", pageSize=1, **READ_FLAGS).execute()
    items = resp.get("files", [])
    return items[0]["id"] if items else None

def ensure_folder(parent_id: str, name: str) -> str:
    """Create (or return existing) folder with given name under parent."""
    mime_folder = "application/vnd.google-apps.folder"
    existing = find_existing(parent_id, name, mime_folder)
    if existing:
        return existing
    body = {"name": name, "parents": [parent_id], "mimeType": mime_folder}
    return drive.files().create(body=body, fields="id", **WRITE_FLAGS).execute()["id"]

def copy_regular(file_id: str, parent_id: str, name: str):
    body = {"name": name, "parents": [parent_id]}
    drive.files().copy(fileId=file_id, body=body, fields="id", **WRITE_FLAGS).execute()

def recurse_copy(src_id: str, dest_parent: str):
    """Recursively copy src tree into dest_parent (skip duplicates)."""
    meta = drive.files().get(fileId=src_id, fields="id,name,mimeType", **READ_FLAGS).execute()
    name, mime = meta["name"], meta["mimeType"]

    if mime == "application/vnd.google-apps.folder":
        dest_id = ensure_folder(dest_parent, name)
        for child in list_children(src_id):
            recurse_copy(child["id"], dest_id)
    else:
        if not find_existing(dest_parent, name, mime):
            copy_regular(src_id, dest_parent, name)

# ---------- main ----------
def main():
    dest_root = os.getenv("WEEKLY_DEST_FOLDER", DEST_PARENT_ID)
    today     = datetime.utcnow().strftime("%d.%m.%Y")

    # Optionally wipe previous snapshot
    dated_folder_id = find_existing(dest_root, today, "application/vnd.google-apps.folder")
    if dated_folder_id and os.getenv("CLEAN_DEST") == "1":
        print(f"🗑  Removing existing '{today}' tree …")
        delete_tree(dated_folder_id)
        dated_folder_id = None

    if not dated_folder_id:
        dated_folder_id = ensure_folder(dest_root, today)
        print(f"📂  Created destination folder '{today}' (id={dated_folder_id})")

    # Copy each source folder
    for src in SOURCE_FOLDER_IDS:
        try:
            recurse_copy(src, dated_folder_id)
        except HttpError as e:
            print(f"⚠️   Skipped {src} — {e}")
    print("✅  Weekly Drive backup complete.")

if __name__ == "__main__":
    main()
