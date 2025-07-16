# """
# Back up every Google Sheet or Excel file found under SOURCE_FOLDER_ID
# into a dated sub‑folder of the fixed destination folder
# `1FVa6HFoI3HSsgtGbbkT8BNPkZXFIz1rF` (shared drive).

# ✓ Fully shared‑drive‑compatible (`supportsAllDrives=True` everywhere).
# """
# import os
# import sys
# import logging
# from datetime import datetime
# from pathlib import Path
# from collections import deque

# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError

# # --------------------------------------------------
# SCOPES = ["https://www.googleapis.com/auth/drive"]
# CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

# READ_FLAGS = dict(
#     supportsAllDrives=True,
#     includeItemsFromAllDrives=True,
#     pageSize=1000,
# )
# WRITE_FLAGS = dict(supportsAllDrives=True)
# # --------------------------------------------------

# logging.basicConfig(
#     level=logging.INFO,
#     format="[%(asctime)s] %(levelname)s: %(message)s",
#     datefmt="%Y-%m-%dT%H:%M:%SZ",
# )
# log = logging.getLogger(__name__)

# # ---------- file types we want ----------
# GOOGLE_SHEET = "application/vnd.google-apps.spreadsheet"
# EXCEL_MIMES = {
#     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#     "application/vnd.ms-excel",
#     "application/vnd.ms-excel.sheet.macroEnabled.12",
#     "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
# }
# TARGET_MIMES = {GOOGLE_SHEET, *EXCEL_MIMES}
# # ----------------------------------------

# # -------- FIXED DESTINATION FOLDER --------
# DEST_PARENT_ID = "1FVa6HFoI3HSsgtGbbkT8BNPkZXFIz1rF"   # ← put dated backups here
# # -----------------------------------------


# def delete_folder_recursive(drive, folder_id: str) -> None:
#     children = (
#         drive.files()
#         .list(
#             q=f"'{folder_id}' in parents and trashed=false",
#             fields="files(id,mimeType)",
#             **READ_FLAGS,
#         )
#         .execute()
#         .get("files", [])
#     )
#     for ch in children:
#         if ch["mimeType"] == "application/vnd.google-apps.folder":
#             delete_folder_recursive(drive, ch["id"])
#         else:
#             drive.files().delete(fileId=ch["id"], **WRITE_FLAGS).execute()
#     drive.files().delete(fileId=folder_id, **WRITE_FLAGS).execute()


# def gather_spreadsheets_and_excels(drive, root_folder: str) -> list[dict]:
#     found, visited = {}, set()
#     queue = deque([root_folder])

#     while queue:
#         fid = queue.popleft()
#         if fid in visited:
#             continue
#         visited.add(fid)

#         page_token = None
#         while True:
#             resp = (
#                 drive.files()
#                 .list(
#                     q=f"'{fid}' in parents and trashed=false",
#                     fields=(
#                         "nextPageToken, "
#                         "files(id,name,mimeType,shortcutDetails/targetId,"
#                         "shortcutDetails/targetMimeType)"
#                     ),
#                     pageToken=page_token,
#                     **READ_FLAGS,
#                 )
#                 .execute()
#             )
#             for f in resp.get("files", []):
#                 mt = f["mimeType"]

#                 if mt == "application/vnd.google-apps.folder":
#                     queue.append(f["id"])
#                 elif mt in TARGET_MIMES:
#                     found[f["id"]] = f
#                 elif mt == "application/vnd.google-apps.shortcut":
#                     tgt_mt = f["shortcutDetails"]["targetMimeType"]
#                     if tgt_mt in TARGET_MIMES:
#                         tgt_id = f["shortcutDetails"]["targetId"]
#                         found[tgt_id] = {"id": tgt_id, "name": f["name"] + " (shortcut)"}
#             page_token = resp.get("nextPageToken")
#             if not page_token:
#                 break
#     return list(found.values())


# def main() -> None:
#     try:
#         creds = service_account.Credentials.from_service_account_file(
#             CREDENTIALS_FILE, scopes=SCOPES
#         )
#         drive = build("drive", "v3", credentials=creds)

#         src_root = os.environ["SOURCE_FOLDER_ID"]        # only this one stays env‑driven
#         today = datetime.utcnow().strftime("%d.%m.%Y")

#         # ----- wipe old backup -----
#         res = drive.files().list(
#             q=(
#                 f"'{DEST_PARENT_ID}' in parents and "
#                 "mimeType='application/vnd.google-apps.folder' "
#                 f"and name='{today}' and trashed=false"
#             ),
#             fields="files(id)",
#             **READ_FLAGS,
#         ).execute()
#         if res.get("files"):
#             log.info("Deleting existing backup folder %s …", today)
#             delete_folder_recursive(drive, res["files"][0]["id"])

#         # ----- create new dated folder -----
#         backup_id = (
#             drive.files()
#             .create(
#                 body={
#                     "name": today,
#                     "mimeType": "application/vnd.google-apps.folder",
#                     "parents": [DEST_PARENT_ID],
#                 },
#                 fields="id",
#                 **WRITE_FLAGS,
#             )
#             .execute()["id"]
#         )
#         log.info("Created backup folder %s (id=%s)", today, backup_id)

#         # ----- gather & copy -----
#         files = gather_spreadsheets_and_excels(drive, src_root)
#         log.info("Total spreadsheets/Excels found: %d", len(files))

#         skipped = []
#         for f in files:
#             try:
#                 drive.files().copy(
#                     fileId=f["id"],
#                     body={"name": f["name"], "parents": [backup_id]},
#                     **WRITE_FLAGS,
#                 ).execute()
#                 log.info("Copied %s", f["name"])
#             except HttpError as e:
#                 if e.resp.status in (403, 404):
#                     log.warning(
#                         "Skipped %s (id=%s) — %s",
#                         f["name"],
#                         f["id"],
#                         "not accessible" if e.resp.status == 403 else "not found",
#                     )
#                     skipped.append((f["name"], f["id"]))
#                     continue
#                 raise

#         log.info(
#             "Backup finished: %d copied, %d skipped",
#             len(files) - len(skipped),
#             len(skipped),
#         )
#         if skipped:
#             log.warning(
#                 "Skipped files:\n%s",
#                 "\n".join(f" • {n} ({i})" for n, i in skipped),
#             )
#     except HttpError as e:
#         log.error("Google API error: %s", e)
#         sys.exit(1)
#     except Exception:
#         log.exception("Unexpected error")
#         sys.exit(1)


# if __name__ == "__main__":
#     main()
"""
Back up every Google Sheet or Excel file found under SOURCE_FOLDER_ID
into a dated sub‑folder of the fixed destination folder
`DEST_FOLDER_ID` (shared drive).

✓ Fully shared‑drive‑compatible (`supportsAllDrives=True` everywhere).
✓ Robust against files/folders that vanished or are not deletable.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from collections import deque

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_FILE = Path(__file__).with_name("credentials.json")

READ_FLAGS = dict(
    supportsAllDrives=True,
    includeItemsFromAllDrives=True,
    pageSize=1000,
)
WRITE_FLAGS = dict(supportsAllDrives=True)
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------- file types we want ----------
GOOGLE_SHEET = "application/vnd.google-apps.spreadsheet"
EXCEL_MIMES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/vnd.ms-excel.sheet.macroEnabled.12",
    "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
}
TARGET_MIMES = {GOOGLE_SHEET, *EXCEL_MIMES}
# ----------------------------------------

# -------- FIXED DESTINATION FOLDER --------
# can be overridden by env DEST_FOLDER_ID
DEST_PARENT_ID_DEFAULT = "1FVa6HFoI3HSsgtGbbkT8BNPkZXFIz1rF"
# -----------------------------------------


# ---------- helpers -------------------------------------------------
def safe_delete(drive, fid: str) -> None:
    """Delete, but ignore 403/404 so the flow keeps going."""
    try:
        drive.files().delete(fileId=fid, **WRITE_FLAGS).execute()
    except HttpError as e:
        if e.resp.status in (403, 404):
            log.warning(
                "Skip delete %s — %s",
                fid,
                "not accessible" if e.resp.status == 403 else "gone",
            )
        else:
            raise


def delete_folder_recursive(drive, folder_id: str) -> None:
    """Hard‑delete a folder and its contents (robust)."""
    kids = (
        drive.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,mimeType)",
            **READ_FLAGS,
        )
        .execute()
        .get("files", [])
    )
    for k in kids:
        if k["mimeType"] == "application/vnd.google-apps.folder":
            delete_folder_recursive(drive, k["id"])
        else:
            safe_delete(drive, k["id"])
    safe_delete(drive, folder_id)


def gather_spreadsheets_and_excels(drive, root_folder: str) -> list[dict]:
    found, visited = {}, set()
    queue = deque([root_folder])

    while queue:
        fid = queue.popleft()
        if fid in visited:
            continue
        visited.add(fid)

        page_token = None
        while True:
            resp = (
                drive.files()
                .list(
                    q=f"'{fid}' in parents and trashed=false",
                    fields=(
                        "nextPageToken, "
                        "files(id,name,mimeType,shortcutDetails/targetId,"
                        "shortcutDetails/targetMimeType)"
                    ),
                    pageToken=page_token,
                    **READ_FLAGS,
                )
                .execute()
            )
            for f in resp.get("files", []):
                mt = f["mimeType"]

                if mt == "application/vnd.google-apps.folder":
                    queue.append(f["id"])
                elif mt in TARGET_MIMES:
                    found[f["id"]] = f
                elif mt == "application/vnd.google-apps.shortcut":
                    tgt_mt = f["shortcutDetails"]["targetMimeType"]
                    if tgt_mt in TARGET_MIMES:
                        tgt_id = f["shortcutDetails"]["targetId"]
                        found[tgt_id] = {
                            "id": tgt_id,
                            "name": f["name"] + " (shortcut)",
                        }
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    return list(found.values())


# ---------- main ----------------------------------------------------
def main() -> None:
    try:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
        drive = build("drive", "v3", credentials=creds)

        src_root = os.environ["SOURCE_FOLDER_ID"]  # required
        dest_parent = os.getenv("DEST_FOLDER_ID", DEST_PARENT_ID_DEFAULT)

        today = datetime.utcnow().strftime("%d.%m.%Y")

        # ----- wipe old backup (if any) -----
        res = (
            drive.files()
            .list(
                q=(
                    f"'{dest_parent}' in parents and "
                    "mimeType='application/vnd.google-apps.folder' "
                    f"and name='{today}' and trashed=false"
                ),
                fields="files(id)",
                **READ_FLAGS,
            )
            .execute()
        )
        if res.get("files"):
            log.info("Deleting existing backup folder %s …", today)
            delete_folder_recursive(drive, res["files"][0]["id"])

        # ----- create new dated folder -----
        backup_id = (
            drive.files()
            .create(
                body={
                    "name": today,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [dest_parent],
                },
                fields="id",
                **WRITE_FLAGS,
            )
            .execute()["id"]
        )
        log.info("Created backup folder %s (id=%s)", today, backup_id)

        # ----- gather & copy -----
        files = gather_spreadsheets_and_excels(drive, src_root)
        log.info("Total spreadsheets/Excels found: %d", len(files))

        skipped = []
        for f in files:
            try:
                drive.files().copy(
                    fileId=f["id"],
                    body={"name": f["name"], "parents": [backup_id]},
                    **WRITE_FLAGS,
                ).execute()
                log.info("Copied %s", f["name"])
            except HttpError as e:
                if e.resp.status in (403, 404):
                    log.warning(
                        "Skipped %s (id=%s) — %s",
                        f["name"],
                        f["id"],
                        "not accessible" if e.resp.status == 403 else "not found",
                    )
                    skipped.append((f["name"], f["id"]))
                    continue
                raise

        log.info(
            "Backup finished: %d copied, %d skipped",
            len(files) - len(skipped),
            len(skipped),
        )
        if skipped:
            log.warning(
                "Skipped files:\n%s",
                "\n".join(f" • {n} ({i})" for n, i in skipped),
            )
    except HttpError as e:
        log.error("Google API error: %s", e)
        sys.exit(1)
    except Exception:
        log.exception("Unexpected error")
        sys.exit(1)


if __name__ == "__main__":
    main()
