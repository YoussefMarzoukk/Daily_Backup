import os
import sys
import logging
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%SZ'
)
logger = logging.getLogger()

SCOPES = ['https://www.googleapis.com/auth/drive']

def find_folder(drive, parent_id, name):
    """Return folder ID if a folder named `name` exists under `parent_id`, else None."""
    query = (
        f"'{parent_id}' in parents "
        "and mimeType='application/vnd.google-apps.folder' "
        f"and name='{name}' and trashed=false"
    )
    resp = drive.files().list(q=query, fields='files(id)').execute()
    files = resp.get('files', [])
    return files[0]['id'] if files else None

def delete_folder(drive, folder_id):
    """Recursively delete a folder and its contents."""
    # Delete all children first
    children = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields='files(id, mimeType)'
    ).execute().get('files', [])
    for c in children:
        if c['mimeType'] == 'application/vnd.google-apps.folder':
            delete_folder(drive, c['id'])
        else:
            drive.files().delete(fileId=c['id']).execute()
    # Then delete the folder itself
    drive.files().delete(fileId=folder_id).execute()
    logger.info(f"Deleted existing folder ID={folder_id}")

def main():
    try:
        # load creds from credentials.json in repo root
        creds = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=SCOPES
        )
        drive = build('drive', 'v3', credentials=creds)

        src = os.environ['SOURCE_FOLDER_ID']
        dst_parent = os.environ['DEST_FOLDER_ID']
        # format today as DD.MM.YYYY
        today = datetime.utcnow().strftime('%d.%m.%Y')

        # 1) If a folder named today already exists, delete it
        existing = find_folder(drive, dst_parent, today)
        if existing:
            logger.info(f"Found existing backup folder for {today} (ID={existing}), deleting it...")
            delete_folder(drive, existing)

        # 2) Create fresh dated folder
        body = {
            'name': today,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [dst_parent]
        }
        created = drive.files().create(body=body, fields='id').execute()
        backup_folder_id = created['id']
        logger.info(f'Created backup folder "{today}" (ID: {backup_folder_id})')

        # 3) List spreadsheets in source
        query = (
            f"'{src}' in parents "
            "and mimeType='application/vnd.google-apps.spreadsheet' "
            "and trashed=false"
        )
        sheets = drive.files().list(q=query, fields='files(id,name)').execute().get('files', [])
        logger.info(f'Found {len(sheets)} sheet(s) to copy.')

        # 4) Copy each
        for s in sheets:
            drive.files().copy(
                fileId=s['id'],
                body={'name': s['name'], 'parents': [backup_folder_id]}
            ).execute()
            logger.info(f'Copied: "{s["name"]}"')

        logger.info("Backup completed successfully.")
    except HttpError as e:
        logger.error(f"Google API error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        sys.exit(1)

if __name__ == '__main__':
    main()
