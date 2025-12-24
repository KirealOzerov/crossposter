import os, json, time, requests, gc, io
from fastapi import FastAPI
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

app = FastAPI()

SHEET_ID = os.environ.get('SPREADSHEET_ID')
SHEET_NAME = 'Plan'
SERVICE_ACCOUNT_JSON = os.environ.get('SERVICE_ACCOUNT_JSON')

def get_gspread_service():
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
    return build('sheets', 'v4', credentials=creds), build('drive', 'v3', credentials=creds)

def post_to_vk(text, file_paths):
    token = os.environ.get('VK_TOKEN')
    group_id = os.environ.get('VK_GROUP_ID')
    if not token or not group_id or not file_paths: return
    try:
        attachments = []
        for path in file_paths:
            # 1. Получаем сервер для загрузки
            res = requests.get("https://api.vk.com/method/photos.getWallUploadServer",
                params={'access_token': token, 'group_id': group_id, 'v': '5.131'}).json()
            upload_url = res.get('response', {}).get('upload_url')
            
            if upload_url:
                # 2. Загружаем файл
                with open(path, 'rb') as f:
                    up_res = requests.post(upload_url, files={'photo': f}).json()
                # 3. Сохраняем фото
                save_res = requests.get("https://api.vk.com/method/photos.saveWallPhoto",
                    params={'access_token': token, 'group_id': group_id, 'v': '5.131',
                            'server': up_res['server'], 'photo': up_res['photo'], 'hash': up_res['hash']}).json()
                if 'response' in save_res:
                    p = save_res['response'][0]
                    attachments.append(f"photo{p['owner_id']}_{p['id']}")

        # 4. Постим на стену
        requests.get("https://api.vk.com/method/wall.post",
            params={'access_token': token, 'owner_id': f"-{group_id}", 'message': text, 
                    'attachments': ",".join(attachments), 'v': '5.131'})
        print("✅ ВК: Опубликовано с фото", flush=True)
    except Exception as e: print(f"❌ ВК Ошибка: {e}", flush=True)

def post_to_telegram(text, file_paths):
    token, chat_id = os.environ.get('TG_TOKEN', '').strip(), os.environ.get('TG_CHAT_ID', '').strip()
    if not token or not chat_id: return
    base_url = f"https://api.telegram.org/bot{token}"
    try:
        if not file_paths:
            requests.post(f"{base_url}/sendMessage", data={'chat_id': chat_id, 'text': text})
        else:
            # Отправляем как PHOTO, а не как документ
            for path in file_paths:
                with open(path, 'rb') as f:
                    requests.post(f"{base_url}/sendPhoto", data={'chat_id': chat_id, 'caption': text}, files={'photo': f})
        print("✅ Telegram: Опубликовано как фото", flush=True)
    except Exception as e: print(f"❌ TG Ошибка: {e}", flush=True)

def worker():
    print("🤖 Воркер запущен и слушает таблицу...", flush=True)
    while True:
        try:
            sheets, drive = get_gspread_service()
            result = sheets.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A2:E100").execute()
            rows = result.get('values', [])
            for i, row in enumerate(rows):
                if len(row) >= 3 and row[2] == 'Pending':
                    row_idx = i + 2
                    print(f"📦 Найдена строка {row_idx}, начинаю работу...", flush=True)
                    
                    sheets.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!C{row_idx}",
                        valueInputOption="RAW", body={'values': [['Processing']]} ).execute()
                    
                    text = row[3] if len(row) > 3 else ""
                    file_ids = [fid.strip() for fid in row[4].split(',')] if len(row) > 4 and row[4] else []
                    
                    paths = []
                    for fid in file_ids:
                        temp_path = f"/tmp/{fid}.jpg"
                        request = drive.files().get_media(fileId=fid)
                        with io.FileIO(temp_path, 'wb') as fh:
                            downloader = MediaIoBaseDownload(fh, request)
                            done = False
                            while not done: _, done = downloader.next_chunk()
                        paths.append(temp_path)
                    
                    post_to_telegram(text, paths)
                    post_to_vk(text, paths)
                    
                    for p in paths: 
                        if os.path.exists(p): os.remove(p)
                    
                    sheets.spreadsheets().values().update(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!C{row_idx}",
                        valueInputOption="RAW", body={'values': [['Posted']]} ).execute()
                    print(f"🏁 Строка {row_idx} готова!", flush=True)
                    gc.collect()
            time.sleep(30)
        except Exception as e:
            print(f"❌ Ошибка: {e}", flush=True)
            time.sleep(60)

@app.on_event("startup")
async def startup_event():
    import threading
    threading.Thread(target=worker, daemon=True).start()

@app.get("/")
def read_root(): return {"status": "online"}
