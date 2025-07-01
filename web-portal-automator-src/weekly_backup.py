"""
Weekly Drive backup

Copies:
* Four source folders (and everything inside them)          – FOLDER_SOURCES
* Seven individual Google Sheets                            – FILE_SOURCES

into a dated folder (DD.MM.YYYY) that lives under the destination folder
whose ID is supplied in the environment variable WEEKLY_DEST_FOLDER.

The script:
* Works with My Drive and Shared Drives (supportsAllDrives=True everywhere)
* Preserves names and hierarchy
* Logs each item copied and any items skipped for lack of permission
* Exits 0 even if some items are skipped; non‑zero on unexpected errors
"""
from __future__ import annotations
import os, sys, logging
from datetime import datetime
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")
# -------------------------------------------------------------------------

# ---- what to back up -----------------------------------------------------
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
# -------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────
def create_folder(drive, name: str, parent: str) -> str:
    """Create a folder under *parent* and return its ID (works in Shared Drives)."""
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


def copy_file(drive, file_id: str, new_name: str, parent_id: str):
    drive.files().copy(
        fileId=file_id,
        body={"name": new_name, "parents": [parent_id]},
        supportsAllDrives=True,
    ).execute()


def copy_folder_recursive(drive, src_id: str, dst_parent: str):
    """Replicate *src_id* (folder) and all its contents under *dst_parent*."""
    src_name = (
        drive.files()
        .get(fileId=src_id, fields="name", supportsAllDrives=True)
        .execute()["name"]
    )
    dst_id = create_folder(drive, src_name, dst_parent)

    page_token = None
    while True:
        resp = (
            drive.files()
            .list(
                q=f"'{src_id}' in parents and trashed=false",
                fields="nextPageToken, files(id,name,mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=1000,
                pageToken=page_token,
            )
            .execute()
        )
        for itm in resp.get("files", []):
            if itm["mimeType"] == "application/vnd.google-apps.folder":
                copy_folder_recursive(drive, itm["id"], dst_id)
            else:
                copy_file(drive, itm["id"], itm["name"], dst_id)
                log.info("  └ copied %s", itm["name"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break


# ──────────────────────────────────────────────────────────────────────────
def main():
    try:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
        drive = build("drive", "v3", credentials=creds)

        dest_root = os.environ["WEEKLY_DEST_FOLDER"]  # must be shared with service‑account
        today = datetime.utcnow().strftime("%d.%m.%Y")
        backup_root = create_folder(drive, today, dest_root)
        log.info("Created weekly backup folder %s (id=%s)", today, backup_root)

        # copy folders
        for fid in FOLDER_SOURCES:
            try:
                log.info("Copying folder %s …", fid)
                copy_folder_recursive(drive, fid, backup_root)
            except HttpError as e:
                log.warning("Skipped folder %s — %s", fid, e)

        # copy individual files
        for fid in FILE_SOURCES:
            try:
                name = (
                    drive.files()
                    .get(fileId=fid, fields="name", supportsAllDrives=True)
                    .execute()["name"]
                )
                copy_file(drive, fid, name, backup_root)
                log.info("Copied file %s", name)
            except HttpError as e:
                log.warning("Skipped file %s — %s", fid, e)

        log.info("Weekly backup finished.")
    except Exception:
        log.exception("Weekly backup failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
