import os
import time
import json
import requests
import vk_api
import threading
import io
from fastapi import FastAPI
import uvicorn
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "Crossposter is active"}

# Настройки из Google
SPREADSHEET_ID = '1WAxb89g4hMVZS2lWVZDh4afJrmKIKryJSXZm80TK1Fc'

def get_services():
    try:
        info = json.loads(os.environ.get('SERVICE_ACCOUNT_JSON'))
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'])
        drive = build('drive', 'v3', credentials=creds, cache_discovery=False)
        sheets = build('sheets', 'v4', credentials=creds, cache_discovery=False)
        return drive, sheets
    except Exception as e:
        print(f"❌ Ошибка авторизации Google: {e}")
        return None, None

def download_from_drive(drive_service, file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except Exception as e:
        print(f"❌ Ошибка скачивания с Drive ({file_id}): {e}")
        return None

def post_to_telegram(text, files):
    token = os.environ.get('TG_TOKEN', '').strip()
    chat_id = os.environ.get('TG_CHAT_ID', '').strip()
    
    if not token:
        print("⚠️ TG_TOKEN не найден в настройках Render!")
        return
    if not chat_id:
        print("⚠️ TG_CHAT_ID не найден в настройках Render!")
        return

    print(f"📡 Попытка отправки в ТГ (чат: {chat_id})...")
    base_url = f"https://api.telegram.org/bot{token}"
    
    try:
        if not files:
            r = requests.post(f"{base_url}/sendMessage", data={'chat_id': chat_id, 'text': text}, timeout=20)
            print(f"📡 Ответ ТГ (текст): {r.status_code} {r.text}")
        else:
            for f_id, f_data in files.items():
                r = requests.post(
                    f"{base_url}/sendDocument", 
                    data={'chat_id': chat_id, 'caption': text}, 
                    files={'document': (f_id, f_data)}, timeout=30)
                print(f"📡 Ответ ТГ (файл): {r.status_code} {r.text}")
        print("✅ Telegram: Запрос отправлен")
    except Exception as e:
        print(f"❌ Telegram Critical Error: {e}")

def post_to_vk(text, files):
    token = os.environ.get('VK_TOKEN', '').strip()
    group_id = os.environ.get('VK_GROUP_ID', '').strip()
    
    if not token or not group_id:
        print("❌ Ошибка: Токен ВК или GroupID отсутствуют")
        return False

    try:
        vk_session = vk_api.VkApi(token=token)
        vk = vk_session.get_api()
        upload = vk_api.VkUpload(vk_session)
        
        attachments = []
        if files:
            for f_id, f_data in files.items():
                # Загружаем файл как фото на стену
                img = io.BytesIO(f_data)
                photo = upload.photo_wall(img, group_id=int(group_id))[0]
                attachments.append(f"photo{photo['owner_id']}_{photo['id']}")
        
        vk.wall.post(owner_id=f"-{group_id}", message=text, attachments=",".join(attachments))
        print(f"📡 ВК Ответ: Успешно")
        return True
    except Exception as e:
        print(f"❌ Ошибка ВК: {e}")
        return False

def worker():
    print("🤖 Воркер запущен. Ожидание задач...")
    while True:
        try:
            drive, sheets = get_services()
            if not sheets: 
                time.sleep(30); continue

            result = sheets.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range='Plan!A2:J').execute()
            rows = result.get('values', [])

            for i, row in enumerate(rows):
                if len(row) < 3 or row[2] != 'Pending': continue
                
                row_num = i + 2
                print(f"📦 Обработка поста в строке {row_num}...")
                
                # Статус: Processing
                sheets.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=f'Plan!C{row_num}',
                    valueInputOption='RAW', body={'values': [['Processing']]}).execute()

                text = row[3] if len(row) > 3 else ""
                media_ids = row[4].split(',') if (len(row) > 4 and row[4]) else []
                
                # Скачиваем файлы
                downloaded = {}
                for mid in media_ids:
                    file_content = download_from_drive(drive, mid.strip())
                    if file_content:
                        downloaded[mid.strip()] = file_content.read()

                # Публикация
                post_to_telegram(text, downloaded)
                post_to_vk(text, downloaded)

                # Статус: Posted
                sheets.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=f'Plan!C{row_num}',
                    valueInputOption='RAW', body={'values': [['Posted']]}).execute()
                print(f"✅ Строка {row_num} опубликована!")

            time.sleep(30)
        except Exception as e:
            print(f"⚠️ Ошибка воркера: {e}")
            time.sleep(30)

# Запуск в фоне
threading.Thread(target=worker, daemon=True).start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)
