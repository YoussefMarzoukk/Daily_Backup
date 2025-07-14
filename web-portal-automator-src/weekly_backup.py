"""
weekly_backup.py – full‑tree clone with duplicate suppression
──────────────────────────────────────────────────────────────
✓ Auth order: credentials.json (script dir → repo root) → KEY env‑var
✓ Shared‑drive compatible (supportsAllDrives=True everywhere)
✓ Rotates dated backups, keeping the newest KEEP
✓ Recursively clones each folder source (all file types) into the dated backup
✓ Skips duplicate files that differ only by "Copy of", "Copia de" or "(1)" suffix
"""

import json, logging, re, sys
from collections import deque, Counter
from datetime import datetime
from pathlib import Path
from os import getenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ────────── CONFIG ──────────
DEST_FOLDER_ID = "1q9spRw8OX_V9OXghNbmZ2a2PHSb07cgF"   # where dated folder goes
KEEP           = 5                                     # keep N recent backups
DATE_FMT       = "%d.%m.%Y"
SCOPES         = ["https://www.googleapis.com/auth/drive"]

SOURCES = [
    # folders
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb", "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB", "1-sVtj8AdMB7pQAadjB9_CUmQ67gOXswi",
    "1g6FARH-wKNk9o0s74X60cifwcc6YDqoP", "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
    # individual spreadsheets
    "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M", "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
    "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4", "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
    "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta", "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
    "1ZplJsdLtZaWnIcU4TdgI_zWZkmpuqh7kc1DDp25NtME", "10_x9pkkfmm2K3L6l35P1B1kBG0dSpZ4MQMADXMLvg9I",
    "1aEkju3lf6MfeXIcbiq3Gu6T1KCkFnVLyRrOwS8iLvTM", "1XFo-LxfkFXg9EUipvVAys0vIJ2xjdlTsx7MwVjeyQHY",
    "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
]
# ────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

FOLDER_MIME = "application/vnd.google-apps.folder"
LIST_FLAGS  = dict(supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1000)
WRITE_FLAGS = dict(supportsAllDrives=True)
GET_FLAGS   = dict(supportsAllDrives=True)

# canonical‑name helper  (Copy of / (1) etc.)
canon_rx = re.compile(r"^(?:(?:Copy of |Copia de )+)?(.+?)(?: \(\d+\))?(\.[^.]+)?$",
                      re.IGNORECASE)
def canon(name: str) -> str:
    stem, ext = canon_rx.match(name).groups()
    return f"{stem.strip().lower()}{ext or ''}"

# ────────── auth helper ──────────
def load_creds():
    here = Path(__file__).resolve().parent
    for p in (here / "credentials.json", here.parent / "credentials.json"):
        if p.exists():
            return service_account.Credentials.from_service_account_file(p, scopes=SCOPES)
    if (key := getenv("KEY")):
        return service_account.Credentials.from_service_account_info(json.loads(key), scopes=SCOPES)
    raise RuntimeError("Service‑account creds not found (credentials.json or KEY env var)")

# ────────── housekeeping ──────────
def rotate_backups(drive):
    resp = drive.files().list(
        q=f"'{DEST_FOLDER_ID}' in parents and trashed=false and mimeType='{FOLDER_MIME}' and 'me' in owners",
        fields="files(id,name)", **LIST_FLAGS).execute()
    dated = []
    for f in resp["files"]:
        try:
            datetime.strptime(f["name"], DATE_FMT)
            dated.append(f)
        except ValueError:
            pass
    dated.sort(key=lambda f: datetime.strptime(f["name"], DATE_FMT), reverse=True)
    for f in dated[KEEP:]:
        drive.files().delete(fileId=f["id"], **WRITE_FLAGS).execute()
        log.info("Deleted old backup %s", f["name"])

# ────────── tree clone ──────────
def clone_folder(drive, src_id: str, dst_parent: str):
    """Recursively clone src folder into dst_parent.
       Returns (copied_files, duplicate_files) counts."""
    # create destination folder with same name
    src_meta = drive.files().get(fileId=src_id, fields="name", **GET_FLAGS).execute()
    dst_id = drive.files().create(
        body={"name": src_meta["name"], "mimeType": FOLDER_MIME, "parents":[dst_parent]},
        fields="id", **WRITE_FLAGS).execute()["id"]

    copied, dup = 0, 0
    dup_guard = set()                      # canonical names in this folder

    page = None
    while True:
        resp = drive.files().list(
            q=f"'{src_id}' in parents and trashed=false",
            fields=("nextPageToken,"
                    "files(id,name,mimeType)"),
            pageToken=page, **LIST_FLAGS).execute()
        for f in resp["files"]:
            if f["mimeType"] == FOLDER_MIME:
                c, d = clone_folder(drive, f["id"], dst_id)
                copied += c; dup += d
            else:
                cn = canon(f["name"])
                if cn in dup_guard:
                    dup += 1
                    continue
                dup_guard.add(cn)
                try:
                    drive.files().copy(
                        fileId=f["id"], body={"name": f["name"], "parents":[dst_id]},
                        **WRITE_FLAGS).execute()
                    copied += 1
                except HttpError as e:
                    log.warning("    skip %s – %s", f["name"], e)
        page = resp.get("nextPageToken")
        if not page:
            break
    return copied, dup

# ────────── main ──────────
def main():
    drive = build("drive", "v3", credentials=load_creds())

    today = datetime.utcnow().strftime(DATE_FMT)
    rotate_backups(drive)

    backup_root = drive.files().create(
        body={"name":today, "mimeType":FOLDER_MIME, "parents":[DEST_FOLDER_ID]},
        fields="id", **WRITE_FLAGS).execute()["id"]
    log.info("Created backup folder %s (id=%s)", today, backup_root)

    for sid in dict.fromkeys(SOURCES):      # removes accidental duplicates in SOURCES
        try:
            meta = drive.files().get(fileId=sid, fields="id,name,mimeType", **GET_FLAGS).execute()
        except HttpError as e:
            log.warning("Cannot access %s – %s", sid, e)
            continue

        name, mt = meta["name"], meta["mimeType"]

        if mt == FOLDER_MIME:
            log.info("Cloning folder %s …", name)
            copied, dup = clone_folder(drive, sid, backup_root)
            log.info("Folder %-25s → %5d copied, %4d duplicates skipped", name, copied, dup)

        else:  # single file (any type)
            # avoid duplicate single‑file entries
            if drive.files().list(
                q=f"'{backup_root}' in parents and name='{name}' and trashed=false",
                fields="files(id)", **LIST_FLAGS).execute()["files"]:
                log.info("File   %-25s → already exists, skipped", name)
                continue
            drive.files().copy(
                fileId=sid, body={"name":name, "parents":[backup_root]}, **WRITE_FLAGS).execute()
            log.info("File   %-25s → copied", name)

    log.info("Backup completed")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Unexpected error")
        sys.exit(1)
