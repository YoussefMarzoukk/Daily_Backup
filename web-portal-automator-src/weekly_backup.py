"""
Weekly backup:
* Recursively copies five source folders and eight individual Sheets
  into a dated sub‑folder under DEST_PARENT.
* All MIME types are copied as‑is.
"""
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

# ---------- SOURCES ----------
FOLDER_SOURCES = {
    # 1‑A  “two folders inside”   (id = 16VQx…)
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    # 1‑B  sub‑folder #2 inside same parent – include because you said “both”
    #      (will be discovered automatically because we traverse recursively)
    # 2
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    # 3
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP",
    # 4
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
}

FILE_SOURCES = {
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",  # sheet
    "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
    "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
    "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
}
# ------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


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
        )
        .execute()["id"]
    )


def copy_file(drive, file_id: str, new_name: str, parent_id: str) -> None:
    drive.files().copy(
        fileId=file_id, body={"name": new_name, "parents": [parent_id]}
    ).execute()


def copy_folder_recursive(drive, src_id: str, dst_parent: str) -> None:
    """
    Recursively replicate src_id (folder) into dst_parent.
    Folder hierarchy and file names are preserved.
    """
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
                fields="nextPageToken, files(id,name,mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=1000,
                pageToken=page_token,
            )
            .execute()
        )
        for item in resp.get("files", []):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                copy_folder_recursive(drive, item["id"], dst_id)
            else:
                copy_file(drive, item["id"], item["name"], dst_id)
                log.info("  └ copied %s", item["name"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break


def main() -> None:
    try:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
        drive = build("drive", "v3", credentials=creds)

        dst_root = os.environ["WEEKLY_DEST_FOLDER"]  # 1aoSe5s…

        today = datetime.utcnow().strftime("%d.%m.%Y")
        backup_root = create_folder(drive, today, dst_root)
        log.info("Created weekly backup folder %s (id=%s)", today, backup_root)

        # ---------- copy folders ----------
        for fid in FOLDER_SOURCES:
            try:
                log.info("Copying folder %s …", fid)
                copy_folder_recursive(drive, fid, backup_root)
            except HttpError as e:
                log.warning("Skipped folder %s — %s", fid, e)
        # ---------- copy individual files ----------
        for fid in FILE_SOURCES:
            try:
                meta = (
                    drive.files()
                    .get(fileId=fid, fields="name", supportsAllDrives=True)
                    .execute()
                )
                copy_file(drive, fid, meta["name"], backup_root)
                log.info("Copied file %s", meta["name"])
            except HttpError as e:
                log.warning("Skipped file %s — %s", fid, e)

        log.info("Weekly backup finished.")
    except Exception:
        log.exception("Weekly backup failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
