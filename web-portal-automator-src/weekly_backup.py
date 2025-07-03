#!/usr/bin/env python3
"""
Backup selected Google‑Drive folders into a destination folder.

Prerequisites
-------------
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib

GitHub secret
-------------
Save your entire service‑account JSON *as a single secret string*:

    name:  GDRIVE_SERVICE_ACCOUNT_JSON
    value: { "type": "service_account", ... }

Workflow example (runs daily at 02:00 UTC)
------------------------------------------
name: Drive‑Backup
on:
  schedule: [{cron: '0 2 * * *'}]
  workflow_dispatch:
jobs:
  backup:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.12'
    - name: Install deps
      run: pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
    - name: Run backup
      env:
        GDRIVE_SERVICE_ACCOUNT_JSON: ${{ secrets.GDRIVE_SERVICE_ACCOUNT_JSON }}
      run: python backup_drive_folders.py
"""
import os
import io
import json
import pathlib
from typing import List, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# ------------------------------------------------------------------------
# ✏️  Edit these
DESTINATION_FOLDER_ID = "1aoSe5seTsCGSn_2gDjU3SiFxlTwAFMmw"
SOURCE_FOLDER_IDS = [
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    # Add more folder IDs here …
]
# ------------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDS_JSON = os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON")
if not CREDS_JSON:
    raise SystemExit("❌  GDRIVE_SERVICE_ACCOUNT_JSON secret is not set")

creds = service_account.Credentials.from_service_account_info(
    json.loads(CREDS_JSON), scopes=SCOPES
)
drive = build("drive", "v3", credentials=creds, cache_discovery=False)


def list_children(folder_id: str) -> List[Dict]:
    """Return metadata for immediate children of a folder."""
    q = f"'{folder_id}' in parents and trashed = false"
    children = []
    page_token = None
    while True:
        resp = (
            drive.files()
            .list(q=q, fields="nextPageToken, files(id, name, mimeType)", pageToken=page_token)
            .execute()
        )
        children.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return children


def find_existing(dest_parent: str, name: str, mime: str) -> str | None:
    """Return ID of a file/folder with same name & mimeType in destination, or None."""
    q = (
        f"'{dest_parent}' in parents and "
        f"name = '{name.replace(\"'\", \"\\'\")}' and "
        f"mimeType = '{mime}' and trashed = false"
    )
    resp = drive.files().list(q=q, fields="files(id)", pageSize=1).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None


def copy_file(file_id: str, dest_parent: str, name: str) -> str:
    """Copy regular file into destination and return new file ID."""
    body = {"name": name, "parents": [dest_parent]}
    new_file = drive.files().copy(fileId=file_id, body=body).execute()
    return new_file["id"]


def copy_gsheet(file_id: str, dest_parent: str, name: str) -> str:
    """Duplicate a Google Sheet (needs `files.copy` as well)."""
    return copy_file(file_id, dest_parent, name)


def ensure_folder(dest_parent: str, name: str) -> str:
    """Create (or return existing) sub‑folder under dest_parent."""
    existing_id = find_existing(dest_parent, name, "application/vnd.google-apps.folder")
    if existing_id:
        return existing_id
    meta = {
        "name": name,
        "parents": [dest_parent],
        "mimeType": "application/vnd.google-apps.folder",
    }
    new_folder = drive.files().create(body=meta, fields="id").execute()
    return new_folder["id"]


def recurse_copy(src_id: str, dest_parent: str):
    """Recursively copy src_id into dest_parent."""
    src_meta = drive.files().get(fileId=src_id, fields="id, name, mimeType").execute()
    name = src_meta["name"]
    mime = src_meta["mimeType"]

    if mime == "application/vnd.google-apps.folder":
        dest_folder_id = ensure_folder(dest_parent, name)
        for child in list_children(src_id):
            recurse_copy(child["id"], dest_folder_id)
    elif mime == "application/vnd.google-apps.spreadsheet":
        if not find_existing(dest_parent, name, mime):
            copy_gsheet(src_id, dest_parent, name)
    else:
        if not find_existing(dest_parent, name, mime):
            copy_file(src_id, dest_parent, name)


def main():
    for src in SOURCE_FOLDER_IDS:
        recurse_copy(src, DESTINATION_FOLDER_ID)
    print("✅  Backup complete.")


if __name__ == "__main__":
    main()
