# """
# weekly_backup.py
# ────────────────
# Full‑tree Google Drive backup with:

# • Recursively clones each folder source (all file types) into a dated
#   sub‑folder; preserves sub‑folder structure.
# • Duplicate guard – only the first file with any given canonical name
#   (ignores “Copy of …”, “Copia de …”, “(1)” etc.) is copied in each folder.
# • Heart‑beat log every LOG_EVERY processed items so long runs show
#   continuous progress.
# • Automatic exponential back‑off on every Drive API call plus retries for
#   transient network / SSL / token‑refresh errors.
# • Rotation – keeps the newest KEEP dated backups in DEST_FOLDER_ID.
# • Auth – credentials.json (script dir → repo root) → KEY env‑var
#   (GitHub Secret containing the raw service‑account JSON).

# **The former “Resume” folder has been removed from SOURCES.**

# Run:
#     python weekly_backup.py
# """

# from __future__ import annotations
# import json, logging, random, re, sys, time, ssl, socket
# from datetime import datetime
# from pathlib import Path
# from os import getenv

# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from google.auth.exceptions import RefreshError

# # ─────────── SETTINGS ───────────
# DEST_FOLDER_ID = "1q9spRw8OX_V9OXghNbmZ2a2PHSb07cgF"   # backup root
# KEEP           = 5                                     # retain newest N backups
# DATE_FMT       = "%d.%m.%Y"                            # dated folder name
# SCOPES         = ["https://www.googleapis.com/auth/drive"]

# LOG_EVERY      = 100                                   # heartbeat frequency

# SOURCES = [
#     # folder sources
#     "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
#     "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
#     "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
#     "1-sVtj8AdMB7pQAadjB9_CUmQ67gOXswi",
#     "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",
#     # individual spreadsheets / files
#     "1zvHfXlJ_U1ra6itGwjVy2O1_N-uDJn9xmEuen7Epk1M",
#     "1P6A405z9-zy_QAEihk0tdsdvFGssQ26f79IJO6cgjD4",
#     "1x-XkSVBSprrZWMNJKAxEI2S2QfqIhU50GMuHXTGyPx4",
#     "1inqfbzosNG6Xf8AxJEJH8yoSLJy3b6_7c8cqy1yXq6s",
#     "1cE-eC__yz6bz931D3DyFj-ZyzJGIx-Ta",
#     "1HhMiTjrFYqgl33IcFS2X1gAtAW42hVCIxLMd6UVUjN8",
#     "1ZplJsdLtZaWnIcU4TdgI_zWZkmpuqh7kc1DDp25NtME",
#     "10_x9pkkfmm2K3L6l35P1B1kBG0dSpZ4MQMADXMLvg9I",
#     "1aEkju3lf6MfeXIcbiq3Gu6T1KCkFnVLyRrOwS8iLvTM",
#     "1XFo-LxfkFXg9EUipvVAys0vIJ2xjdlTsx7MwVjeyQHY",
#     "1JESHGsBdVLEqCiLssy7ZZ12S6V-0mZMc",
# ]
# # ────────────────────────────────

# logging.basicConfig(
#     level=logging.INFO,
#     format="[%(asctime)s] %(levelname)s: %(message)s",
#     datefmt="%Y-%m-%dT%H:%M:%SZ",
# )
# log = logging.getLogger(__name__)

# # ---------- Drive request flags ----------
# FOLDER_MIME = "application/vnd.google-apps.folder"
# LIST_FLAGS  = dict(supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1000)
# WRITE_FLAGS = dict(supportsAllDrives=True)
# GET_FLAGS   = dict(supportsAllDrives=True)

# # ---------- back‑off wrapper ----------
# TRANSIENT_EXC = (
#     HttpError, RefreshError,
#     ssl.SSLError, socket.timeout, ConnectionResetError,
#     ConnectionAbortedError, BrokenPipeError, OSError,
# )

# def gapi_execute(req, *, max_tries: int = 7):
#     delay = 2
#     for attempt in range(1, max_tries + 1):
#         try:
#             return req.execute()
#         except TRANSIENT_EXC as e:
#             if isinstance(e, HttpError):
#                 st = e.resp.status
#                 body = e.content.decode() if isinstance(e.content, bytes) else str(e)
#                 retriable = (
#                     st in (429, 500, 502, 503, 504) or
#                     (st == 403 and ("userRateLimitExceeded" in body or "rateLimitExceeded" in body))
#                 )
#                 if not retriable:
#                     raise
#             if attempt == max_tries:
#                 raise
#             sleep_for = delay + random.uniform(0, 1)
#             log.warning("Transient error (%s, attempt %d/%d). Sleeping %.1fs …",
#                         type(e).__name__, attempt, max_tries, sleep_for)
#             time.sleep(sleep_for)
#             delay *= 2
# # ------------------------------------------

# # ---------- helpers ----------
# canon_rx = re.compile(r"^(?:(?:Copy of |Copia de )+)?(.+?)(?: \(\d+\))?(\.[^.]+)?$", re.I)
# def canonical(name: str) -> str:
#     """Return lower‑cased canonical name without 'Copy of', ' (1)' etc."""
#     stem, ext = canon_rx.match(name).groups()
#     return f"{stem.strip().lower()}{ext or ''}"

# def load_credentials():
#     here = Path(__file__).resolve().parent
#     for p in (here / "credentials.json", here.parent / "credentials.json"):
#         if p.exists():
#             return service_account.Credentials.from_service_account_file(p, scopes=SCOPES)
#     if (raw := getenv("KEY")):
#         return service_account.Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
#     raise RuntimeError("Service‑account credentials not found")
# # --------------------------------

# # ---------- housekeeping ----------
# def rotate_backups(drive):
#     resp = gapi_execute(drive.files().list(
#         q=f"'{DEST_FOLDER_ID}' in parents and trashed=false "
#           f"and mimeType='{FOLDER_MIME}' and 'me' in owners",
#         fields="files(id,name)", **LIST_FLAGS))
#     dated = []
#     for f in resp["files"]:
#         try:
#             datetime.strptime(f["name"], DATE_FMT)
#             dated.append(f)
#         except ValueError:
#             pass
#     dated.sort(key=lambda f: datetime.strptime(f["name"], DATE_FMT), reverse=True)
#     for f in dated[KEEP:]:
#         gapi_execute(drive.files().delete(fileId=f["id"], **WRITE_FLAGS))
#         log.info("Deleted old backup %s", f["name"])
# # ---------------------------------

# # ---------- recursive clone ----------
# def clone_folder(drive, src_id: str, dst_parent: str) -> tuple[int, int]:
#     """Clone an entire folder tree; return (copied, duplicates)."""
#     src_name = gapi_execute(drive.files().get(
#         fileId=src_id, fields="name", **GET_FLAGS))["name"]
#     dst_id = gapi_execute(drive.files().create(
#         body={"name": src_name, "mimeType": FOLDER_MIME, "parents": [dst_parent]},
#         fields="id", **WRITE_FLAGS))["id"]

#     copied = dup = processed = 0
#     dup_guard: set[str] = set()
#     page = None
#     while True:
#         resp = gapi_execute(drive.files().list(
#             q=f"'{src_id}' in parents and trashed=false",
#             fields="nextPageToken,files(id,name,mimeType)",
#             pageToken=page, **LIST_FLAGS))
#         for f in resp["files"]:
#             processed += 1
#             if f["mimeType"] == FOLDER_MIME:
#                 c, d = clone_folder(drive, f["id"], dst_id)
#                 copied += c; dup += d
#             else:
#                 cn = canonical(f["name"])
#                 if cn in dup_guard:
#                     dup += 1
#                 else:
#                     dup_guard.add(cn)
#                     try:
#                         gapi_execute(drive.files().copy(
#                             fileId=f["id"],
#                             body={"name": f["name"], "parents": [dst_id]},
#                             **WRITE_FLAGS))
#                         copied += 1
#                     except HttpError as e:
#                         log.warning("    Skip %s – %s", f["name"], e)
#             if processed % LOG_EVERY == 0:
#                 log.info("    …%s: %d items processed", src_name, processed)
#         page = resp.get("nextPageToken")
#         if not page:
#             break
#     return copied, dup
# # --------------------------------------

# # ---------- main ----------
# def main():
#     drive = build("drive", "v3", credentials=load_credentials())
#     today = datetime.utcnow().strftime(DATE_FMT)

#     rotate_backups(drive)

#     backup_root = gapi_execute(drive.files().create(
#         body={"name": today, "mimeType": FOLDER_MIME, "parents": [DEST_FOLDER_ID]},
#         fields="id", **WRITE_FLAGS))["id"]
#     log.info("Created backup folder %s (id=%s)", today, backup_root)

#     for sid in dict.fromkeys(SOURCES):  # de‑duplicate list
#         try:
#             meta = gapi_execute(drive.files().get(
#                 fileId=sid, fields="id,name,mimeType", **GET_FLAGS))
#         except HttpError as e:
#             log.warning("Cannot access %s – %s", sid, e)
#             continue

#         name, mt = meta["name"], meta["mimeType"]

#         if mt == FOLDER_MIME:
#             log.info("Cloning folder %s …", name)
#             c, d = clone_folder(drive, sid, backup_root)
#             log.info("Folder %-25s → %5d copied, %4d duplicates skipped", name, c, d)
#         else:
#             existing = gapi_execute(drive.files().list(
#                 q=f"'{backup_root}' in parents and name='{name}' and trashed=false",
#                 fields="files(id)", **LIST_FLAGS))["files"]
#             if existing:
#                 log.info("File   %-25s → already exists, skipped", name)
#                 continue
#             gapi_execute(drive.files().copy(
#                 fileId=sid, body={"name": name, "parents": [backup_root]},
#                 **WRITE_FLAGS))
#             log.info("File   %-25s → copied", name)

#     log.info("Backup completed")
# # --------------------------------

# if __name__ == "__main__":
#     try:
#         main()
#     except Exception:
#         log.exception("Unexpected error")
#         sys.exit(1)
"""
weekly_backup.py
────────────────
Full‑tree Google Drive backup with:

• Recursively clones each folder source (all file types) into a dated
  sub‑folder; preserves sub‑folder structure.
• Duplicate guard – only the first file with any given canonical name
  (ignores “Copy of …”, “Copia de …”, “(1)” etc.) is copied in each folder.
• Heart‑beat log every LOG_EVERY processed items so long runs show
  continuous progress.
• Automatic exponential back‑off on every Drive API call plus retries for
  transient network / SSL / token‑refresh errors.
• Rotation – keeps the newest KEEP dated backups in DEST_FOLDER_ID.
• Auth – credentials.json (script dir → repo root) → KEY env‑var
  (GitHub Secret containing the raw service‑account JSON).

Run:
    python weekly_backup.py
"""

from __future__ import annotations
import json, logging, random, re, sys, time, ssl, socket
from datetime import datetime
from pathlib import Path
from os import getenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

# ─────────── SETTINGS ───────────
# If WEEKLY_DEST_FOLDER is exported (e.g. in the GitHub‑Actions YAML)
# it overrides the hard‑coded default below.
DEST_FOLDER_ID = getenv("WEEKLY_DEST_FOLDER", "14EmdS7r31S4zjf9Z-FRY4gv6SzCVU1BG")

KEEP           = 5                    # retain newest N backups
DATE_FMT       = "%d.%m.%Y"           # dated folder name (e.g. 16.07.2025)
SCOPES         = ["https://www.googleapis.com/auth/drive"]

LOG_EVERY      = 100                  # heartbeat frequency (items)
# ────────────────────────────────

SOURCES = [
    # folder sources
    "16VQxSSw_Zybv7GtFMhQgzyzyE5EEX9gb",
    "10lXwcwYGsbdIYLhkL9862RP4Xi1L-p9v",
    "17O23nAlgh2fnlBcIBmk2K7JBeUAAQZfB",
    "1-sVtj8AdMB7pQAadjB9_CUmQ67gOXswi",
    "1GSWRpzm9OMNQF7Wbcgr7cLE5zX8gPEbO",

    # individual spreadsheets / files
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

    # --- new ---
    "1lAPxxKAFkcwXHavT2a5bQiPFtD0ddUJn",      # + Work calendar sheet
    "1vFzcPK72TDX_ZA0R6_kwJgEH3p7QNc7e5Zg4D4Cup-E",  # +
    "1MNF5OR44GeC9opM5VN00gRP9UP88eTyQnaEjy0I2C7o",  # +
    "1mTBJW-i5jjVDjypqD5tDM0jCuAg021oc",            # +
    "12OnW564sXL5yeEo2w7Bvs9ThDeJIgZOw",            # +
]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------- Drive request flags ----------
FOLDER_MIME = "application/vnd.google-apps.folder"
LIST_FLAGS  = dict(supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1000)
WRITE_FLAGS = dict(supportsAllDrives=True)
GET_FLAGS   = dict(supportsAllDrives=True)

# ---------- back‑off wrapper ----------
TRANSIENT_EXC = (
    HttpError, RefreshError,
    ssl.SSLError, socket.timeout, ConnectionResetError,
    ConnectionAbortedError, BrokenPipeError, OSError,
)

def gapi_execute(req, *, max_tries: int = 7):
    delay = 2
    for attempt in range(1, max_tries + 1):
        try:
            return req.execute()
        except TRANSIENT_EXC as e:
            if isinstance(e, HttpError):
                st = e.resp.status
                body = e.content.decode() if isinstance(e.content, bytes) else str(e)
                retriable = (
                    st in (429, 500, 502, 503, 504) or
                    (st == 403 and ("userRateLimitExceeded" in body or "rateLimitExceeded" in body))
                )
                if not retriable:
                    raise
            if attempt == max_tries:
                raise
            sleep_for = delay + random.uniform(0, 1)
            log.warning("Transient error (%s, attempt %d/%d). Sleeping %.1fs …",
                        type(e).__name__, attempt, max_tries, sleep_for)
            time.sleep(sleep_for)
            delay *= 2
# ------------------------------------------

# ---------- helpers ----------
canon_rx = re.compile(r"^(?:(?:Copy of |Copia de )+)?(.+?)(?: \(\d+\))?(\.[^.]+)?$", re.I)
def canonical(name: str) -> str:
    """Return lower‑cased canonical name without 'Copy of', ' (1)' etc."""
    stem, ext = canon_rx.match(name).groups()
    return f"{stem.strip().lower()}{ext or ''}"

def load_credentials():
    here = Path(__file__).resolve().parent
    for p in (here / "credentials.json", here.parent / "credentials.json"):
        if p.exists():
            return service_account.Credentials.from_service_account_file(p, scopes=SCOPES)
    if (raw := getenv("KEY")):
        return service_account.Credentials.from_service_account_info(json.loads(raw), scopes=SCOPES)
    raise RuntimeError("Service‑account credentials not found")
# --------------------------------

# ---------- housekeeping ----------
def rotate_backups(drive):
    resp = gapi_execute(drive.files().list(
        q=f"'{DEST_FOLDER_ID}' in parents and trashed=false "
          f"and mimeType='{FOLDER_MIME}' and 'me' in owners",
        fields="files(id,name)", **LIST_FLAGS))
    dated = []
    for f in resp["files"]:
        try:
            datetime.strptime(f["name"], DATE_FMT)
            dated.append(f)
        except ValueError:
            pass
    dated.sort(key=lambda f: datetime.strptime(f["name"], DATE_FMT), reverse=True)
    for f in dated[KEEP:]:
        gapi_execute(drive.files().delete(fileId=f["id"], **WRITE_FLAGS))
        log.info("Deleted old backup %s", f["name"])
# ---------------------------------

# ---------- recursive clone ----------
def clone_folder(drive, src_id: str, dst_parent: str) -> tuple[int, int]:
    """Clone an entire folder tree; return (copied, duplicates)."""
    src_name = gapi_execute(drive.files().get(
        fileId=src_id, fields="name", **GET_FLAGS))["name"]
    dst_id = gapi_execute(drive.files().create(
        body={"name": src_name, "mimeType": FOLDER_MIME, "parents": [dst_parent]},
        fields="id", **WRITE_FLAGS))["id"]

    copied = dup = processed = 0
    dup_guard: set[str] = set()
    page = None
    while True:
        resp = gapi_execute(drive.files().list(
            q=f"'{src_id}' in parents and trashed=false",
            fields="nextPageToken,files(id,name,mimeType)",
            pageToken=page, **LIST_FLAGS))
        for f in resp["files"]:
            processed += 1
            if f["mimeType"] == FOLDER_MIME:
                c, d = clone_folder(drive, f["id"], dst_id)
                copied += c; dup += d
            else:
                cn = canonical(f["name"])
                if cn in dup_guard:
                    dup += 1
                else:
                    dup_guard.add(cn)
                    try:
                        gapi_execute(drive.files().copy(
                            fileId=f["id"],
                            body={"name": f["name"], "parents": [dst_id]},
                            **WRITE_FLAGS))
                        copied += 1
                    except HttpError as e:
                        log.warning("    Skip %s – %s", f["name"], e)
            if processed % LOG_EVERY == 0:
                log.info("    …%s: %d items processed", src_name, processed)
        page = resp.get("nextPageToken")
        if not page:
            break
    return copied, dup
# --------------------------------------

# ---------- main ----------
def main():
    drive = build("drive", "v3", credentials=load_credentials())
    today = datetime.utcnow().strftime(DATE_FMT)

    rotate_backups(drive)

    backup_root = gapi_execute(drive.files().create(
        body={"name": today, "mimeType": FOLDER_MIME, "parents": [DEST_FOLDER_ID]},
        fields="id", **WRITE_FLAGS))["id"]
    log.info("Created backup folder %s (id=%s)", today, backup_root)

    for sid in dict.fromkeys(SOURCES):  # de‑duplicate list
        try:
            meta = gapi_execute(drive.files().get(
                fileId=sid, fields="id,name,mimeType", **GET_FLAGS))
        except HttpError as e:
            log.warning("Cannot access %s – %s", sid, e)
            continue

        name, mt = meta["name"], meta["mimeType"]

        if mt == FOLDER_MIME:
            log.info("Cloning folder %s …", name)
            c, d = clone_folder(drive, sid, backup_root)
            log.info("Folder %-25s → %5d copied, %4d duplicates skipped", name, c, d)
        else:
            existing = gapi_execute(drive.files().list(
                q=f"'{backup_root}' in parents and name='{name}' and trashed=false",
                fields="files(id)", **LIST_FLAGS))["files"]
            if existing:
                log.info("File   %-25s → already exists, skipped", name)
                continue
            gapi_execute(drive.files().copy(
                fileId=sid, body={"name": name, "parents": [backup_root]},
                **WRITE_FLAGS))
            log.info("File   %-25s → copied", name)

    log.info("Backup completed")
# --------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Unexpected error")
        sys.exit(1)

