"""
weekly_backup.py
────────────────
Back up a fixed list of Google Drive folders / spreadsheets into a dated
sub‑folder of the destination folder.  Older backups beyond KEEP newest
are deleted first so the service‑account’s 15 GB quota never fills up.

• Put service‑account JSON next to this file and name it credentials.json
• Run:  python weekly_backup.py
"""

import logging
import sys
from collections import deque
from datetime import datetime
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =======================================================================
# CONSTANTS / SETTINGS --------------------------------------------------
SCOPES           = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

DEST_FOLDER_ID = "1q9spRw8OX_V9OXghNbmZ2a2PHSb07cgF"   # <<< target folder
KEEP           = 5                # keep this many most‑recent backups
DATE_FMT       = "%d.%m.%Y"       # folder‑name pattern you want
# -----------------------------------------------------------------------
# IDs of the folders / spreadsheets to back up
SOURCES = [
    # ─── folders ───
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    "1-sVtj8AdMB7pQAadjB9_CUmQ67gOXswi",
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP",
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
    # ─── spreadsheets ───
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",
    "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
    "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
    "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1ZplJsdLtZaWnIcU4TdgI_zWZkmpuqh7kc1DDp25NtME",
    "10_x9pkkfmm2K3L6l35P1B1kBG0dSpZ4MQMADXMLvg9I",
    "1aEkju3lf6MfeXIcbiq3Gu6T1KCkFnVLyRrOwS8iLvTM",
    "1XFo-LxfkFXg9EUipvVAys0vIJ2xjdlTsx7MwVjeyQHY",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
]
# =======================================================================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

GOOGLE_SHEET = "application/vnd.google-apps.spreadsheet"
EXCEL_MIMES  = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/vnd.ms-excel.sheet.macroEnabled.12",
    "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
}
TARGET_MIMES = {GOOGLE_SHEET, *EXCEL_MIMES}
FOLDER_MIME  = "application/vnd.google-apps.folder"


# ---------- helpers -----------------------------------------------------
def safe_delete(drive, file_id: str) -> None:
    try:
        drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
    except HttpError as e:
        if e.resp.status == 403:
            log.warning("No permission to delete %s; skipping", file_id)
        else:
            raise


def delete_folder_recursive(drive, folder_id: str) -> None:
    """Best‑effort permanent delete of a folder tree owned by the SA."""
    children = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=1000,
    ).execute().get("files", [])
    for ch in children:
        if ch["mimeType"] == FOLDER_MIME:
            delete_folder_recursive(drive, ch["id"])
        else:
            safe_delete(drive, ch["id"])
    safe_delete(drive, folder_id)


def purge_old_backups(drive) -> None:
    """Delete dated backup folders beyond KEEP newest."""
    resp = drive.files().list(
        q=(
            f"'{DEST_FOLDER_ID}' in parents and mimeType='{FOLDER_MIME}' "
            "and trashed=false and 'me' in owners"
        ),
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        pageSize=1000,
    ).execute()

    backups = []
    for f in resp.get("files", []):
        try:
            datetime.strptime(f["name"], DATE_FMT)
            backups.append(f)
        except ValueError:
            pass

    backups.sort(key=lambda f: datetime.strptime(f["name"], DATE_FMT), reverse=True)
    for f in backups[KEEP:]:
        log.info("Purging old backup %s …", f["name"])
        delete_folder_recursive(drive, f["id"])


def gather_target_files(drive, root_folder: str) -> list[dict]:
    """Return every Google Sheet or Excel under root_folder (recursive)."""
    found, visited = {}, set()
    queue = deque([root_folder])
    flags = dict(
        supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1000
    )

    while queue:
        fid = queue.popleft()
        if fid in visited:
            continue
        visited.add(fid)

        page_token = None
        while True:
            resp = drive.files().list(
                q=f"'{fid}' in parents and trashed=false",
                fields=(
                    "nextPageToken,files(id,name,mimeType,shortcutDetails/targetId,"
                    "shortcutDetails/targetMimeType)"
                ),
                pageToken=page_token,
                **flags,
            ).execute()

            for f in resp.get("files", []):
                mt = f["mimeType"]
                if mt == FOLDER_MIME:
                    queue.append(f["id"])
                elif mt in TARGET_MIMES:
                    found[f["id"]] = f
                elif mt == "application/vnd.google-apps.shortcut":
                    tgt_mt = f["shortcutDetails"]["targetMimeType"]
                    if tgt_mt in TARGET_MIMES:
                        tgt_id = f["shortcutDetails"]["targetId"]
                        found[tgt_id] = {"id": tgt_id, "name": f["name"] + " (shortcut)"}

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    return list(found.values())
# ------------------------------------------------------------------------


def main() -> None:
    try:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
        drive = build("drive", "v3", credentials=creds)
    except Exception:
        log.exception("Authentication failed")
        sys.exit(1)

    today = datetime.utcnow().strftime(DATE_FMT)

    # -- 1. free quota -----------------------------------------------------
    purge_old_backups(drive)

    # -- 2. create today’s backup root ------------------------------------
    backup_root = drive.files().create(
        body={
            "name": today,
            "mimeType": FOLDER_MIME,
            "parents": [DEST_FOLDER_ID],
        },
        fields="id",
    ).execute()["id"]
    log.info("Created daily backup folder %s (id=%s)", today, backup_root)

    # -- 3. iterate over sources ------------------------------------------
    seen = set()  # handle accidental duplicates
    for src_id in SOURCES:
        if src_id in seen:
            continue
        seen.add(src_id)

        try:
            meta = drive.files().get(
                fileId=src_id,
                fields="id,name,mimeType",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute()
        except HttpError as e:
            log.warning("Cannot access %s — %s", src_id, e)
            continue

        name, mt = meta["name"], meta["mimeType"]

        if mt == FOLDER_MIME:
            log.info("Scanning folder %s …", name)
            dst_folder = drive.files().create(
                body={"name": name, "mimeType": FOLDER_MIME, "parents": [backup_root]},
                fields="id",
            ).execute()["id"]

            files = gather_target_files(drive, src_id)
            log.info("  %d spreadsheet/Excel files found", len(files))

            for f in files:
                try:
                    drive.files().copy(
                        fileId=f["id"],
                        body={"name": f["name"], "parents": [dst_folder]},
                        supportsAllDrives=True,
                    ).execute()
                    log.info("  Copied %s", f["name"])
                except HttpError as e:
                    log.warning("  Skip %s — %s", f["name"], e)

        elif mt in TARGET_MIMES:
            try:
                drive.files().copy(
                    fileId=src_id,
                    body={"name": name, "parents": [backup_root]},
                    supportsAllDrives=True,
                ).execute()
                log.info("Copied %s", name)
            except HttpError as e:
                log.warning("Skip %s — %s", name, e)

        else:
            log.info("Skipping %s (unsupported mimeType %s)", name, mt)

    log.info("Backup completed")


if __name__ == "__main__":
    main()
