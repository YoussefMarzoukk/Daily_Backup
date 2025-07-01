"""
Robust weekly Google‑Drive backup
────────────────────────────────

* Verifies destination folder is reachable; exits with a clear error if not.
* Logs reachability of every source folder/file before copying.
* Recursively copies folder trees + individual Sheets to a dated sub‑folder.
* Skips items the service‑account can’t read, but lists them in the summary.

Author: ChatGPT (2025‑07‑01)
"""
from __future__ import annotations
import os, sys, logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, List

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ───────────────────────── configuration ──────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

DEST_ENV_VAR = "WEEKLY_DEST_FOLDER"   # Provided by the workflow
CHECK_PAGE_SIZE = 1000               # big page size to reduce API calls

FOLDER_SOURCES: set[str] = {
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP",
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
}

FILE_SOURCES: set[str] = {
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",
    "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
    "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
    "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
}
# ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ───────────── helper functions ─────────────
def drive_service() :
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)

def check_access(drive, file_id: str) -> Tuple[bool, str]:
    """Return (accessible?, name_or_reason)."""
    try:
        meta = (
            drive.files()
            .get(fileId=file_id, fields="name,mimeType", supportsAllDrives=True)
            .execute()
        )
        return True, meta["name"]
    except HttpError as e:
        if e.resp.status in (404, 403):
            return False, f"{e.resp.status} {'not found' if e.resp.status==404 else 'forbidden'}"
        raise

def create_folder(drive, name: str, parent: str) -> str:
    return (
        drive.files()
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

def copy_file(drive, fid: str, new_name: str, parent: str):
    drive.files().copy(
        fileId=fid,
        body={"name": new_name, "parents": [parent]},
        supportsAllDrives=True,
    ).execute()

def copy_folder_recursive(drive, src_id: str, dst_parent: str):
    src_meta = (
        drive.files()
        .get(fileId=src_id, fields="name", supportsAllDrives=True)
        .execute()
    )
    dst_id = create_folder(drive, src_meta["name"], dst_parent)

    page_token = None
    while True:
        resp = (
            drive.files()
            .list(
                q=f"'{src_id}' in parents and trashed=false",
                fields="nextPageToken,files(id,name,mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=CHECK_PAGE_SIZE,
                pageToken=page_token,
            )
            .execute()
        )
        for f in resp.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                copy_folder_recursive(drive, f["id"], dst_id)
            else:
                copy_file(drive, f["id"], f["name"], dst_id)
                log.info("      copied %s", f["name"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
# ────────────────────────────────────────────


def main():
    dest_root = os.environ.get(DEST_ENV_VAR)
    if not dest_root:
        log.error("Env var %s is not set.", DEST_ENV_VAR)
        sys.exit(1)

    drv = drive_service()

    # 1) verify destination path
    ok, info = check_access(drv, dest_root)
    if not ok:
        log.error("Destination folder %s is not accessible: %s", dest_root, info)
        log.error("👉  Share that folder (or its Shared Drive) with the service‑account "
                  "or double‑check the ID.")
        sys.exit(1)
    log.info("Destination folder OK → %s", info)

    # 2) create dated backup folder
    today = datetime.utcnow().strftime("%d.%m.%Y")
    backup_root_id = create_folder(drv, today, dest_root)
    log.info("Created backup folder %s (id=%s)", today, backup_root_id)

    skipped: List[str] = []

    # 3) folders
    for fid in FOLDER_SOURCES:
        ok, meta = check_access(drv, fid)
        if not ok:
            log.warning("SKIP folder %-30s : %s", fid, meta)
            skipped.append(f"folder {fid} – {meta}")
            continue
        log.info("Copying folder tree: %s", meta)
        try:
            copy_folder_recursive(drv, fid, backup_root_id)
        except HttpError as e:
            log.warning("Skipped folder %s — %s", meta, e)
            skipped.append(f"folder {meta} – {e}")

    # 4) single files
    for fid in FILE_SOURCES:
        ok, meta = check_access(drv, fid)
        if not ok:
            log.warning("SKIP file   %-30s : %s", fid, meta)
            skipped.append(f"file {fid} – {meta}")
            continue
        try:
            copy_file(drv, fid, meta, backup_root_id)
            log.info("Copied file: %s", meta)
        except HttpError as e:
            log.warning("Skipped file %s — %s", meta, e)
            skipped.append(f"file {meta} – {e}")

    # 5) summary
    log.info("──────────────── backup summary ────────────────")
    log.info("Destination : %s / %s", dest_root, today)
    log.info("Total skipped: %d", len(skipped))
    if skipped:
        for s in skipped:
            log.info("   • %s", s)
    log.info("Done.")
# ────────────────────────────────────────────


if __name__ == "__main__":
    main()
