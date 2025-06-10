from flask import Flask, request, render_template_string
import os
import hashlib
import requests
import random
import json
import re
from datetime import datetime
from openpyxl import Workbook
from pathlib import Path

app = Flask(__name__)

# --- 設定區 ---
PIXEL_ID = "1664521517602334"
ACCESS_TOKEN = "EAAH1oqWMsq8BO37rKconweZBXXPFQac7NCNxFbD40RN9SopOp2t3o5xEPQ1zbkrOkKIUoBGPZBXbsxStkXsniH9EE777qANZAGKXNIgMtliLHZBntS2VTp7uDbLhNBZAFwZBShVw8QyOXbYSDFfwqxQCWtzJYbFzktZCJpD3BkyYeaTcOMP2zz0MnZCfppTCYGb8uQZDZD"
CURRENCY = "TWD"
VALUE_CHOICES = [19800, 28000, 28800, 34800, 39800, 45800]
CITIES = ["taipei", "newtaipei", "taoyuan", "taichung", "tainan", "kaohsiung"]

# ✅ 請設定你自己的備份資料夾（例：D:/feedbacks）
BACKUP_FOLDER = Path(r"C:\Users\tbana\Desktop\客戶建議")
BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

# --- 表單 HTML ---
HTML_FORM = '''
<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="UTF-8"><title>服務滿意度調查</title></head>
<body>
    <h2>服務滿意度調查</h2>
    <form action="/submit" method="post">
        姓名：<input type="text" name="name" required><br><br>
        出生年月日：<input type="date" name="birthdate" required><br><br>
        性別：
        <select name="gender" required><option value="男">男</option><option value="女">女</option></select><br><br>
        Email：<input type="email" name="email"><br><br>
        電話：<input type="tel" name="phone"><br><br>
        您覺得小編的服務態度如何？解說是否清楚易懂？<br>
        <textarea name="attitude" rows="4" cols="50" required></textarea><br><br>
        您對我們的服務有什麼建議？<br>
        <textarea name="suggestion" rows="4" cols="50"></textarea><br><br>
        <input type="submit" value="送出">
    </form>
    <p style="color: gray; font-size: 14px;">
        感謝您的建議，我們將傾聽每位顧客的心聲，增加服務改善。<br>
        以上個人相關資訊僅做為售後服務紀錄，不做其他用途。
    </p>
</body>
</html>
'''

THANK_YOU_PAGE = '''
<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="UTF-8"><title>感謝您的填寫</title></head>
<body>
    <h3>感謝您的建議，我們將傾聽每位顧客的心聲，增加服務改善。</h3>
    <p>以上個人相關資訊僅做為售後服務紀錄，不做其他用途。</p>
</body>
</html>
'''

# --- 工具函式 ---
def hash_data(value):
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest() if value else ""

def clean_phone(phone):
    phone = re.sub(r"[^\d]", "", phone)
    if phone.startswith("09"):
        phone = "886" + phone[1:]
    return phone

def is_valid_email(email):
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(pattern, email)

def save_to_excel(data, name):
    safe_name = re.sub(r"[^\w\u4e00-\u9fff]", "_", name)
    filepath = BACKUP_FOLDER / f"{safe_name}.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.append(list(data.keys()))
    ws.append(list(data.values()))
    wb.save(filepath)
    print(f"✅ 資料已儲存為 Excel：{filepath}")

def send_to_meta(data, ip):
    event_time = int(datetime.now().timestamp())
    event_id = hashlib.md5((data.get("Email", "") + str(event_time)).encode("utf-8")).hexdigest()
    value = random.choice(VALUE_CHOICES)
    city = random.choice(CITIES)

    user_data = {
        "ge": hash_data("m" if data["性別"] == "男" else "f"),
        "db": hash_data(data["出生年月日"].replace("-", "")),
        "ct": hash_data(city),
        "country": hash_data("tw"),
        "client_ip_address": ip,
        "external_id": hash_data(data.get("Email", "") or event_id)
    }

    if data.get("Email") and is_valid_email(data["Email"]):
        user_data["em"] = hash_data(data["Email"])
    if data.get("電話"):
        cleaned = clean_phone(data["電話"])
        if len(cleaned) >= 9:
            user_data["ph"] = hash_data(cleaned)
    if data.get("姓名"):
        user_data["ln"] = hash_data(data["姓名"])

    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": event_time,
            "event_id": event_id,
            "action_source": "website",
            "user_data": user_data,
            "custom_data": {
                "currency": CURRENCY,
                "value": value
            }
        }]
    }

    print("📤 上傳至 Meta Payload：", json.dumps(payload, indent=2, ensure_ascii=False))

    try:
        res = requests.post(
            f"https://graph.facebook.com/v23.0/{PIXEL_ID}/events?access_token={ACCESS_TOKEN}",
            json=payload,
            timeout=10
        )
        print(f"✅ Meta 回傳：{res.status_code} - {res.text}")
    except Exception as e:
        print(f"❌ 上傳 Meta 失敗：{e}")

# --- 路由 ---
@app.route("/", methods=["GET"])
def form():
    return render_template_string(HTML_FORM)

@app.route("/submit", methods=["POST"])
def submit():
    data = {
        "姓名": request.form["name"],
        "出生年月日": request.form["birthdate"],
        "性別": request.form["gender"],
        "Email": request.form.get("email", ""),
        "電話": request.form.get("phone", ""),
        "服務態度是否清楚易懂": request.form["attitude"],
        "建議": request.form.get("suggestion", "")
    }

    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "127.0.0.1")
    if ip == "127.0.0.1":
        ip = "8.8.8.8"  # 模擬真實 IP 避免被拒

    # 儲存 Excel
    save_to_excel(data, data["姓名"])

    # 上傳至 Meta
    send_to_meta(data, ip)

    return render_template_string(THANK_YOU_PAGE)

# --- 執行 ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
