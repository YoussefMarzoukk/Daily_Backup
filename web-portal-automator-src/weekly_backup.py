#!/usr/bin/env python3
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── Configuration ─────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")
DEST_PARENT_ID = os.getenv("WEEKLY_DEST_FOLDER")  # must be set in workflow

# The seven individual spreadsheet IDs:
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
    format="[%(asctime)s] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


def find_or_create_folder(drive, parent_id: str, name: str) -> str:
    """Look for a folder named `name` under `parent_id`, else create it."""
    q = (
        f"'{parent_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder' "
        f"and name='{name}' and trashed=false"
    )
    resp = drive.files().list(q=q, fields="files(id)").execute().get("files", [])
    if resp:
        return resp[0]["id"]
    created = (
        drive.files()
        .create(
            body={
                "name": name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created["id"]


def main():
    if not DEST_PARENT_ID:
        log.error("Environment variable WEEKLY_DEST_FOLDER is not set.")
        sys.exit(1)

    # Authenticate
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    drive = build("drive", "v3", credentials=creds)

    # Prepare dated folder
    today = datetime.utcnow().strftime("%d.%m.%Y")
    backup_folder_id = find_or_create_folder(drive, DEST_PARENT_ID, today)
    log.info(f"Using backup folder: {today} (id={backup_folder_id})")

    # Copy each spreadsheet
    for fid in FILE_SOURCES:
        try:
            meta = (
                drive.files()
                .get(fileId=fid, fields="name", supportsAllDrives=True)
                .execute()
            )
            name = meta["name"]
            drive.files().copy(
                fileId=fid,
                body={"name": name, "parents": [backup_folder_id]},
                supportsAllDrives=True,
            ).execute()
            log.info(f"Copied spreadsheet: {name}")
        except HttpError as e:
            log.warning(f"Skipped {fid}: {e}")

    log.info("✅ Spreadsheet backup complete.")


if __name__ == "__main__":
    main()
