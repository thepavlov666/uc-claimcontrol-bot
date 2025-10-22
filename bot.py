import os
import io
import re
import datetime
import logging
from collections import defaultdict

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request


BOT_TOKEN = "8334927048:AAF4jbGXmQe1M_xtHe5bxohBCyD7nk_YdG8"
DRIVE_PARENT_FOLDER_ID = "1taxJWF3Z8vapMxPJVs9HKh39q8k1VE58"
SHEET_ID = "1x2rieo6SfxWK9yWW3Wpe_QYB0NJSV5iWd8J-FFS22TQ"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
media_buffer = defaultdict(list)  # store media in a group temporarily


def get_google_services():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w", encoding="utf-8") as token:
            token.write(creds.to_json())
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    return drive, sheets



def parse_caption(caption: str):
    if not caption:
        return None, None
    caption = caption.strip()
    match = re.match(r"(ID\d{3,})\s+(.+)", caption)
    if not match:
        return None, None
    return match.group(1).upper(), match.group(2).strip()


def find_or_create_client_folder(drive, parent_id: str, client_id: str, name: str) -> str:
    folder_name = f"{client_id} - {name}".replace("'", "").strip()
    query = (
        f"'{parent_id}' in parents and "
        f"name = '{folder_name}' and "
        f"mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )
    res = drive.files().list(
        q=query,
        spaces="drive",
        fields="files(id,name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    folder = drive.files().create(
        body=metadata,
        fields="id",
        supportsAllDrives=True
    ).execute()
    return folder["id"]


async def upload_and_log_file(drive, sheets, buf, file_name, client_id, name):
    folder_id = find_or_create_client_folder(drive, DRIVE_PARENT_FOLDER_ID, client_id, name)
    media = MediaIoBaseUpload(buf, mimetype="application/octet-stream", resumable=False)
    metadata = {"name": file_name, "parents": [folder_id]}
    uploaded = drive.files().create(
        body=metadata,
        media_body=media,
        fields="id, webViewLink",
        supportsAllDrives=True
    ).execute()
    link = uploaded.get("webViewLink", "")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_data = [[client_id, name, file_name, timestamp, link]]
    sheets.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range="A1",
        valueInputOption="USER_ENTERED",
        body={"values": row_data}
    ).execute()
    return link


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 UCClaimControl Bulk Upload Bot is running.\n"
        "Select multiple files or photos and send with a caption like:\n\n"
        "ID001 John Smith"
    )


async def handle_media_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles media groups (bulk upload)."""
    group_id = update.message.media_group_id
    if not group_id:
        # Single file — pass to single handler
        await handle_file(update, context)
        return
    media_buffer[group_id].append(update)


async def finalize_media_group(context: ContextTypes.DEFAULT_TYPE):
    """Process collected media after a short delay."""
    to_process = list(media_buffer.items())
    for group_id, updates in to_process:
        del media_buffer[group_id]
        if not updates:
            continue
        first_message = updates[0].message
        caption = first_message.caption or ""
        client_id, name = parse_caption(caption)
        if not client_id or not name:
            await first_message.reply_text("❗ Please provide caption like: ID001 John Smith")
            continue

        drive, sheets = get_google_services()
        links = []

        for u in updates:
            msg = u.message
            if msg.document:
                file_obj = msg.document
                tg_file = await file_obj.get_file()
                file_name = file_obj.file_name or f"{client_id}_{int(datetime.datetime.now().timestamp())}"
            elif msg.photo:
                file_obj = msg.photo[-1]
                tg_file = await file_obj.get_file()
                file_name = f"{client_id}_{int(datetime.datetime.now().timestamp())}.jpg"
            else:
                continue

            buf = io.BytesIO()
            await tg_file.download_to_memory(out=buf)
            buf.seek(0)

            link = await upload_and_log_file(drive, sheets, buf, file_name, client_id, name)
            links.append(link)

        await first_message.reply_text(
            f"✅ {len(links)} files uploaded for {name} ({client_id})\n" +
            "\n".join([f"🔗 {l}" for l in links])
        )


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles single file uploads (no media group)."""
    caption = update.message.caption or ""
    client_id, name = parse_caption(caption)
    if not client_id or not name:
        await update.message.reply_text("❗ Please add caption like: ID001 John Smith")
        return

    tg_file = None
    file_name = None
    if update.message.document:
        doc = update.message.document
        tg_file = await doc.get_file()
        file_name = doc.file_name or f"{client_id}_{int(datetime.datetime.now().timestamp())}"
    elif update.message.photo:
        photo = update.message.photo[-1]
        tg_file = await photo.get_file()
        file_name = f"{client_id}_{int(datetime.datetime.now().timestamp())}.jpg"
    else:
        await update.message.reply_text("⚠️ Please send a document or photo.")
        return

    buf = io.BytesIO()
    await tg_file.download_to_memory(out=buf)
    buf.seek(0)

    drive, sheets = get_google_services()
    link = await upload_and_log_file(drive, sheets, buf, file_name, client_id, name)
    await update.message.reply_text(
        f"✅ File uploaded for {name} ({client_id})\n🔗 {link}")



def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(MessageHandler(filters.ALL & filters.UpdateType.MESSAGE, handle_media_group))

    # run job queue to finalize media group after delay
    job_queue = app.job_queue
    job_queue.run_repeating(finalize_media_group, interval=2.0, first=3.0)

    print("🚀 UCClaimControl Bulk Upload Bot running... Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
