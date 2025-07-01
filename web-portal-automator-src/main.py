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
# # --------------------------------------------------

# logging.basicConfig(
#     level=logging.INFO,
#     format="[%(asctime)s] %(levelname)s: %(message)s",
#     datefmt="%Y-%m-%dT%H:%M:%SZ",
# )
# log = logging.getLogger(__name__)


# def delete_folder_recursive(drive, folder_id: str) -> None:
#     """Delete a folder and all its contents."""
#     children = (
#         drive.files()
#         .list(
#             q=f"'{folder_id}' in parents and trashed=false",
#             fields="files(id,mimeType)",
#             supportsAllDrives=True,
#             includeItemsFromAllDrives=True,
#             pageSize=1000,
#         )
#         .execute()
#         .get("files", [])
#     )
#     for ch in children:
#         if ch["mimeType"] == "application/vnd.google-apps.folder":
#             delete_folder_recursive(drive, ch["id"])
#         else:
#             drive.files().delete(fileId=ch["id"]).execute()
#     drive.files().delete(fileId=folder_id).execute()


# def list_every_sheet(drive, root_folder: str) -> list[dict]:
#     """Return every Google Sheet reachable under *root_folder* (recurses; resolves shortcuts)."""
#     sheets: dict[str, dict] = {}
#     visited_folders: set[str] = set()
#     queue = deque([root_folder])
#     q_flags = dict(
#         supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1000
#     )

#     while queue:
#         fid = queue.popleft()
#         if fid in visited_folders:
#             continue
#         visited_folders.add(fid)

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
#                     **q_flags,
#                 )
#                 .execute()
#             )
#             for f in resp.get("files", []):
#                 mt = f["mimeType"]
#                 if mt == "application/vnd.google-apps.folder":
#                     queue.append(f["id"])
#                 elif mt == "application/vnd.google-apps.spreadsheet":
#                     sheets[f["id"]] = f
#                 elif mt == "application/vnd.google-apps.shortcut":
#                     if (
#                         f["shortcutDetails"]["targetMimeType"]
#                         == "application/vnd.google-apps.spreadsheet"
#                     ):
#                         target_id = f["shortcutDetails"]["targetId"]
#                         sheets[target_id] = {
#                             "id": target_id,
#                             "name": f["name"] + " (shortcut)",
#                         }
#             page_token = resp.get("nextPageToken")
#             if not page_token:
#                 break

#     return list(sheets.values())


# def main() -> None:
#     try:
#         creds = service_account.Credentials.from_service_account_file(
#             CREDENTIALS_FILE, scopes=SCOPES
#         )
#         drive = build("drive", "v3", credentials=creds)

#         src_root = os.environ["SOURCE_FOLDER_ID"]
#         dst_parent = os.environ["DEST_FOLDER_ID"]
#         today = datetime.utcnow().strftime("%d.%m.%Y")

#         # ----- wipe old backup folder (if any) -----
#         res = drive.files().list(
#             q=(
#                 f"'{dst_parent}' in parents and "
#                 "mimeType='application/vnd.google-apps.folder' "
#                 f"and name='{today}' and trashed=false"
#             ),
#             fields="files(id)",
#             supportsAllDrives=True,
#             includeItemsFromAllDrives=True,
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
#                     "parents": [dst_parent],
#                 },
#                 fields="id",
#             )
#             .execute()["id"]
#         )
#         log.info("Created backup folder %s (id=%s)", today, backup_id)

#         # ----- gather every spreadsheet -----
#         sheets = list_every_sheet(drive, src_root)
#         log.info("Total spreadsheets found: %d", len(sheets))

#         skipped = []
#         for s in sheets:
#             try:
#                 drive.files().copy(
#                     fileId=s["id"], body={"name": s["name"], "parents": [backup_id]}
#                 ).execute()
#                 log.info("Copied %s", s["name"])
#             except HttpError as e:
#                 if e.resp.status in (403, 404):
#                     log.warning(
#                         "Skipped %s (id=%s) — %s",
#                         s["name"],
#                         s["id"],
#                         "not accessible" if e.resp.status == 403 else "not found",
#                     )
#                     skipped.append((s["name"], s["id"]))
#                     continue
#                 raise  # any other HTTP error still aborts job

#         log.info("Backup finished: %d copied, %d skipped", len(sheets) - len(skipped), len(skipped))
#         if skipped:
#             log.warning("Skipped files:\n%s", "\n".join(f" • {n} ({i})" for n, i in skipped))
#     except HttpError as e:
#         log.error("Google API error: %s", e)
#         sys.exit(1)
#     except Exception:
#         log.exception("Unexpected error")
#         sys.exit(1)


# if __name__ == "__main__":
#     main()
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
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
    "application/vnd.ms-excel",                                           # .xls
    "application/vnd.ms-excel.sheet.macroEnabled.12",                     # .xlsm
    "application/vnd.ms-excel.sheet.binary.macroEnabled.12",              # .xlsb
}
TARGET_MIMES = {GOOGLE_SHEET, *EXCEL_MIMES}
# ----------------------------------------


def delete_folder_recursive(drive, folder_id: str) -> None:
    """Delete a folder and all its contents."""
    children = (
        drive.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
        )
        .execute()
        .get("files", [])
    )
    for ch in children:
        if ch["mimeType"] == "application/vnd.google-apps.folder":
            delete_folder_recursive(drive, ch["id"])
        else:
            drive.files().delete(fileId=ch["id"]).execute()
    drive.files().delete(fileId=folder_id).execute()


def gather_spreadsheets_and_excels(drive, root_folder: str) -> list[dict]:
    """Return every Google Sheet *or* Excel file under root_folder (recursive, shortcut‑aware)."""
    found: dict[str, dict] = {}
    visited_folders: set[str] = set()
    queue = deque([root_folder])
    flags = dict(
        supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1000
    )

    while queue:
        fid = queue.popleft()
        if fid in visited_folders:
            continue
        visited_folders.add(fid)

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
                    **flags,
                )
                .execute()
            )
            for f in resp.get("files", []):
                mt = f["mimeType"]

                # ---- recurse into sub‑folders ----
                if mt == "application/vnd.google-apps.folder":
                    queue.append(f["id"])
                    continue

                # ---- native files we want ----
                if mt in TARGET_MIMES:
                    found[f["id"]] = f
                    continue

                # ---- shortcuts we care about ----
                if mt == "application/vnd.google-apps.shortcut":
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


def main() -> None:
    try:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )
        drive = build("drive", "v3", credentials=creds)

        src_root = os.environ["SOURCE_FOLDER_ID"]
        dst_parent = os.environ["DEST_FOLDER_ID"]
        today = datetime.utcnow().strftime("%d.%m.%Y")

        # -------- wipe old backup if present --------
        res = drive.files().list(
            q=(
                f"'{dst_parent}' in parents and "
                "mimeType='application/vnd.google-apps.folder' "
                f"and name='{today}' and trashed=false"
            ),
            fields="files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
        if res.get("files"):
            log.info("Deleting existing backup folder %s …", today)
            delete_folder_recursive(drive, res["files"][0]["id"])

        # -------- create new dated folder --------
        backup_id = (
            drive.files()
            .create(
                body={
                    "name": today,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [dst_parent],
                },
                fields="id",
            )
            .execute()["id"]
        )
        log.info("Created backup folder %s (id=%s)", today, backup_id)

        # -------- gather target files --------
        files = gather_spreadsheets_and_excels(drive, src_root)
        log.info("Total spreadsheets/Excels found: %d", len(files))

        skipped = []
        for f in files:
            try:
                drive.files().copy(
                    fileId=f["id"], body={"name": f["name"], "parents": [backup_id]}
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
                raise  # other errors still abort

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
