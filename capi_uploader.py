import os
import json
import csv
import time
import hashlib
import requests
import re
import random
import pickle
from datetime import datetime

# --- 設定參數 ---
ACCESS_TOKEN = "EAAH1oqWMsq8BO37rKconweZBXXPFQac7NCNxFbD40RN9SopOp2t3o5xEPQ1zbkrOkKIUoBGPZBXbsxStkXsniH9EE777qANZAGKXNIgMtliLHZBntS2VTp7uDbLhNBZAFwZBShVw8QyOXbYSDFfwqxQCWtzJYbFzktZCJpD3BkyYeaTcOMP2zz0MnZCfppTCYGb8uQZDZD"
PIXEL_ID = "1664521517602334"
CURRENCY = "TWD"
BATCH_SIZE = 50
MAX_RETRIES = 3
VALUE_CHOICES = [19800, 28800, 28000, 34800, 39800, 45800]
BASE_FOLDER = r"C:\Users\tbana\Desktop\對話夾\your_instagram_activity"
LOG_FILE = "upload_log.txt"
EVENT_ID_LOG = "uploaded_event_ids.txt"
OUTPUT_CSV = "成交客戶_only.csv"
PROFILE_MAP_PATH = "user_profile_map.pkl"
MY_ACCOUNT_NAMES = ["thaispa.tw", "你的IG帳號名稱"]
KEYWORDS = ["完成", "三年內", "五官", "清晰", "第一天", "開始"]
SKIP_KEYWORDS = ["settings", "account", "devices", "ads", "report", "profile", "connections"]
CITIES = ["taipei", "newtaipei", "taoyuan", "taichung", "tainan", "kaohsiung"]
user_city_map = {}

# --- 載入使用者個資資料 ---
user_profile_map = {}
if os.path.exists(PROFILE_MAP_PATH):
    with open(PROFILE_MAP_PATH, "rb") as f:
        user_profile_map = pickle.load(f)

# --- 工具函式 ---
def hash_data(text):
    return hashlib.sha256(text.strip().lower().encode("utf-8")).hexdigest() if text else ""

def generate_event_id(username, content, event_time):
    base = f"{username}_{content}_{event_time}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def log(message):
    print(message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def load_uploaded_event_ids():
    if os.path.exists(EVENT_ID_LOG):
        with open(EVENT_ID_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_uploaded_event_ids(event_ids):
    with open(EVENT_ID_LOG, "a", encoding="utf-8") as f:
        for eid in event_ids:
            f.write(eid + "\n")

def send_capi_batch(events):
    url = f"https://graph.facebook.com/v18.0/{PIXEL_ID}/events?access_token={ACCESS_TOKEN}"
    payload = {"data": events}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                return True, response.text
            log(f"❌ 第 {attempt} 次上傳失敗，狀態碼: {response.status_code}，訊息: {response.text}")
        except Exception as e:
            log(f"❌ 第 {attempt} 次上傳錯誤：{e}")
        time.sleep(2)
    return False, None

def fix_encoding(text):
    try:
        return text.encode('latin1').decode('utf-8')
    except:
        return text

def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

def is_valid_phone(phone):
    return re.match(r"^09\d{8}$", phone)

def extract_age_to_birthdate(text):
    patterns = [
        r"(?:我|滿|才剛滿)?\s*(\d{1,2})\s*歲",
        r"我\s*(\d{1,2})\b",
        r"\b(\d{1,2})歲",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                age = int(match.group(1))
                if 18 <= age <= 80:
                    today = datetime.today()
                    year = today.year - age
                    month = random.randint(1, 12)
                    day = random.randint(1, 28)
                    return f"{year:04d}{month:02d}{day:02d}"
            except:
                continue
    return ""

def extract_birthdate(text):
    from_age = extract_age_to_birthdate(text)
    if from_age:
        return from_age
    match = re.search(r"(19|20)\d{2}[/-]?\d{1,2}[/-]?\d{1,2}", text)
    if match:
        digits = re.sub(r"[^\d]", "", match.group())
        return digits.zfill(8)
    year = random.randint(1980, 2005)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year:04d}{month:02d}{day:02d}"

def extract_gender(text):
    text = text.lower()
    patterns_female = [r"我(是)?(女生|女孩|小姐|女性)", r"(我是)?[0-9]{1,2}歲(的)?女生", r"我老婆", r"我女友", r"我女朋友"]
    patterns_male = [r"我(是)?(男生|男孩|先生|男性)", r"(我是)?[0-9]{1,2}歲(的)?男生", r"我老公", r"我男友", r"我男朋友"]
    for pattern in patterns_female:
        if re.search(pattern, text):
            return "f"
    for pattern in patterns_male:
        if re.search(pattern, text):
            return "m"
    if "她" in text:
        return "m"
    return "f"

def extract_zip(text):
    match = re.search(r"\b\d{3,5}\b", text)
    return match.group() if match else ""

def merge_profile(user_key, new_data):
    profile = user_profile_map.get(user_key, {})
    for key, value in new_data.items():
        if not profile.get(key) and value:
            profile[key] = value
    user_profile_map[user_key] = profile

def process_instagram_json_file(filepath, uploaded_event_ids):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log(f"⚠️ 無法處理檔案 {filepath}: {e}")
        return [], []

    if "messages" not in data:
        return [], []

    participants = data.get("participants", [])
    name = fix_encoding(participants[0].get("name", "")) if participants else ""
    username = fix_encoding(participants[0].get("username", "")) if participants else ""
    raw_email = fix_encoding(participants[0].get("email", ""))
    raw_phone = fix_encoding(participants[0].get("phone", ""))
    ip = participants[0].get("ip", "")

    email = raw_email if is_valid_email(raw_email) else ""
    phone = raw_phone if is_valid_phone(raw_phone) else ""
    user_key = username or name
    if user_key not in user_city_map:
        user_city_map[user_key] = random.choice(CITIES)
    city = user_city_map[user_key]

    messages = data["messages"]
    events, records = [], []

    for msg in messages:
        raw_text = msg.get("content", "")
        if isinstance(raw_text, list):
            raw_text = " ".join(str(t) for t in raw_text)
        elif not isinstance(raw_text, str):
            raw_text = str(raw_text)
        try:
            raw_text = raw_text.encode('latin1').decode('utf-8')
        except:
            pass

        sender = msg.get("sender_name") or msg.get("sender") or ""
        matched_keyword = next((kw for kw in KEYWORDS if kw in raw_text), None)
        if matched_keyword:
            event_time = int(time.time())
            event_id = generate_event_id(username or name, raw_text, event_time)
            if event_id in uploaded_event_ids:
                log(f"⏭️ 已上傳過的事件（event_id: {event_id}），略過")
                continue

            is_customer = sender not in MY_ACCOUNT_NAMES
            birthdate = extract_birthdate(raw_text) if is_customer else ""
            gender = extract_gender(raw_text) if is_customer else "f"
            price = random.choice(VALUE_CHOICES)
            zip_code = extract_zip(raw_text)

            new_profile = {
                "fn": name[:1] if name else "",
                "ln": name[1:] if name and len(name) > 1 else "",
                "ge": gender,
                "db": birthdate,
                "zp": zip_code,
                "em": email,
                "ph": phone,
                "ip": ip,
                "ct": city,
                "st": "taiwan",
                "country": "tw",
                "external_id": username or name
            }
            merge_profile(user_key, new_profile)

            profile = user_profile_map.get(user_key, {})
            user_data = {k: hash_data(profile.get(k, "")) for k in ["fn", "ln", "ge", "db", "zp", "ct", "st", "country", "em", "ph", "external_id"]}
            if profile.get("ip") and profile.get("ip").lower() not in ["null", "none"]:
                user_data["client_ip_address"] = profile["ip"]

            event = {
                "event_name": "Purchase",
                "event_time": event_time,
                "event_id": event_id,
                "user_data": user_data,
                "custom_data": {"currency": CURRENCY, "value": price}
            }
            events.append(event)
            records.append({
                "資料夾": os.path.basename(os.path.dirname(filepath)),
                "檔名": os.path.basename(filepath),
                "成交": "✅ 是",
                "命中關鍵字": matched_keyword,
                "對話片段": raw_text,
                "發話者": sender,
                "時間": event_time,
                "event_id": event_id,
                "姓名": name,
                "帳號": username,
                "是否上傳成功": ""
            })

    return events, records

def main():
    log("⚙️ 程式啟動完成，開始讀取資料...")
    all_events, all_records = [], []
    uploaded_event_ids = load_uploaded_event_ids()

    for root, _, files in os.walk(BASE_FOLDER):
        for file in files:
            if file.endswith(".json") and not any(skip_kw in file.lower() for skip_kw in SKIP_KEYWORDS):
                filepath = os.path.join(root, file)
                events, records = process_instagram_json_file(filepath, uploaded_event_ids)
                all_events.extend(events)
                all_records.extend(records)

    if not all_events:
        log("⚠️ 沒有偵測到任何成交事件")
        return

    log(f"📦 共需上傳 {len(all_events)} 筆事件")
    newly_uploaded_ids = []

    for i in range(0, len(all_events), BATCH_SIZE):
        batch = all_events[i:i + BATCH_SIZE]
        success, _ = send_capi_batch(batch)
        for j in range(i, i + len(batch)):
            all_records[j]["是否上傳成功"] = "✅ 是" if success else "❌ 否"
            if success:
                newly_uploaded_ids.append(batch[j - i]["event_id"])
        log(f"{'✅ 成功' if success else '❌ 失敗'} 上傳第 {(i // BATCH_SIZE) + 1} 批")
        time.sleep(1)

    if newly_uploaded_ids:
        save_uploaded_event_ids(newly_uploaded_ids)
        log(f"✅ 已記錄 {len(newly_uploaded_ids)} 筆 event_id")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["資料夾", "檔名", "成交", "命中關鍵字", "對話片段", "發話者", "時間", "event_id", "姓名", "帳號", "是否上傳成功"])
        writer.writeheader()
        writer.writerows(all_records)

    with open(PROFILE_MAP_PATH, "wb") as f:
        pickle.dump(user_profile_map, f)
    log(f"📁 統一資料檔已更新：{PROFILE_MAP_PATH}")
    log(f"📄 所有事件記錄已儲存：{OUTPUT_CSV}")

if __name__ == "__main__":
    main()
