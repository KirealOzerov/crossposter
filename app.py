import os
import json
import time
import requests
import gc
from fastapi import FastAPI
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

app = FastAPI()

# --- КОНФИГУРАЦИЯ ---
SHEET_ID = os.environ.get('SPREADSHEET_ID')
SHEET_NAME = 'Plan'
SERVICE_ACCOUNT_JSON = os.environ.get('SERVICE_ACCOUNT_JSON')

def get_gspread_service():
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
    return build('sheets', 'v4', credentials=creds), build('drive', 'v3', credentials=creds)

def post_to_vk(text, files_data):
    token = os.environ.get('VK_TOKEN')
    group_id = os.environ.get('VK_GROUP_ID')
    if not token or not group_id: return
    
    try:
        attachments = []
        # Загрузка фото в ВК
        for filename, content in files_data.items():
            upload_url_res = requests.get(
                "https://api.vk.com/method/photos.getWallUploadServer",
                params={'access_token': token, 'group_id': group_id, 'v': '5.131'}
            ).json()
            
            upload_url = upload_url_res.get('response', {}).get('upload_url')
            if upload_url:
                files = {'photo': (filename, content)}
                save_data = requests.post(upload_url, files=files).json()
                photo_res = requests.get(
                    "https://api.vk.com/method/photos.saveWallPhoto",
                    params={
                        'access_token': token, 'group_id': group_id, 'v': '5.131',
                        'server': save_data['server'], 'photo': save_data['photo'], 'hash': save_data['hash']
                    }
                ).json()
                if 'response' in photo_res:
                    p = photo_res['response'][0]
                    attachments.append(f"photo{p['owner_id']}_{p['id']}")

        # Публикация на стену
        requests.get(
            "https://api.vk.com/method/wall.post",
            params={
                'access_token': token, 'owner_id': f"-{group_id}",
                'message': text, 'attachments': ",".join(attachments), 'v': '5.131'
            }
        )
        print("✅ ВК: Опубликовано")
    except Exception as e:
        print(f"❌ ВК Ошибка: {e}")

def post_to_telegram(text, files_data):
    token = os.environ.get('TG_TOKEN', '').strip()
    chat_id = os.environ.get('TG_CHAT_ID', '').strip()
    if not token or not chat_id:
        print("⚠️ TG: Проверьте токены в настройках")
        return

    base_url = f"https://api.telegram.org/bot{token}"
    try:
        if not files_data:
            requests.post(f"{base_url}/sendMessage", data={'chat_id': chat_id, 'text': text})
        else:
            # Отправляем файлы по одному, чтобы не перегружать память
            for filename, content in files_data.items():
                requests.post(
                    f"{base_url}/sendDocument",
                    data={'chat_id': chat_id, 'caption': text},
                    files={'document': (filename, content)}
                )
        print("✅ Telegram: Опубликовано")
    except Exception as e:
        print(f"❌ TG Ошибка: {e}")

def worker():
    print("🤖 Воркер запущен. Ожидание задач...")
    while True:
        try:
            sheets, drive = get_gspread_service()
            result = sheets.spreadsheets().values().get(
                spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A2:E100"
            ).execute()
            rows = result.get('values', [])

            for i, row in enumerate(rows):
                if len(row) >= 3 and row[2] == 'Pending':
                    row_idx = i + 2
                    print(f"📦 Обработка строки {row_idx}...")
                    
                    # Ставим статус Processing
                    sheets.spreadsheets().values().update(
                        spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!C{row_idx}",
                        valueInputOption="RAW", body={'values': [['Processing']]}
                    ).execute()

                    text = row[3] if len(row) > 3 else ""
                    file_ids = [fid.strip() for fid in row[4].split(',')] if len(row) > 4 and row[4] else []
                    
                    # Скачиваем файлы
                    media_files = {}
                    for fid in file_ids:
                        request = drive.files().get_media(fileId=fid)
                        file_stream = io.BytesIO()
                        downloader = MediaIoBaseDownload(file_stream, request)
                        done = False
                        while not done:
                            _, done = downloader.next_chunk()
                        file_stream.seek(0)
                        media_files[f"{fid}.jpg"] = file_stream.read()
                    
                    # Отправка
                    post_to_vk(text, media_files)
                    post_to_telegram(text, media_files)

                    # Очистка памяти
                    media_files.clear()
                    gc.collect()

                    # Статус Posted
                    sheets.spreadsheets().values().update(
                        spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!C{row_idx}",
                        valueInputOption="RAW", body={'values': [['Posted']]}
                    ).execute()
                    
            time.sleep(30) # Проверка раз в 30 секунд
        except Exception as e:
            print(f"❌ Ошибка воркера: {e}")
            time.sleep(60)

@app.on_event("startup")
async def startup_event():
    import threading
    threading.Thread(target=worker, daemon=True).start()

@app.get("/")
def read_root():
    return {"status": "active", "worker": "running"}
