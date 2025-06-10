from flask import Flask, request, render_template_string
from openpyxl import Workbook
from pathlib import Path
import hashlib
import requests
import smtplib
from email.message import EmailMessage
from datetime import datetime
import re
import random

app = Flask(__name__)

# === 直接寫死的 Meta Conversion API 設定 ===
PIXEL_ID = "1664521517602334"
ACCESS_TOKEN = "EAAH1oqWMsq8BO37rKconweZBXXPFQac7NCNxFbD40RN9SopOp2t3o5xEPQ1zbkrOkKIUoBGPZBXbsxStkXsniH9EE777qANZAGKXNIgMtliLHZBntS2VTp7uDbLhNBZAFwZBShVw8QyOXbYSDFfwqxQCWtzJYbFzktZCJpD3BkyYeaTcOMP2zz0MnZCfppTCYGb8uQZDZD"
API_URL = f"https://graph.facebook.com/v23.0/{PIXEL_ID}/events"
CURRENCY = "TWD"
VALUE_CHOICES = [19800, 28000, 28800, 34800, 39800, 45800]

# === Email 設定（保持環境變數或改成寫死都可） ===
FROM_EMAIL = "你的寄件 email"
EMAIL_PASSWORD = "你的 email 密碼"
TO_EMAIL_1 = "第一位收件人"
TO_EMAIL_2 = "第二位收件人"

BACKUP_FOLDER = Path("form_backups")
BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

HTML_FORM = '''
<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="UTF-8"><title>服務滿意度調查</title></head>
<body>
    <h2>服務滿意度調查</h2>
    <form action="/submit" method="post">
        姓名：<input type="text" name="name" required><br><br>
        出生年月日：<input type="date" name="birthday"><br><br>
        性別：
        <select name="gender">
            <option value="female">女性</option>
            <option value="male">男性</option>
        </select><br><br>
        Email：<input type="email" name="email"><br><br>
        電話：<input type="text" name="phone"><br><br>
        您覺得小編的服務態度如何？解說是否清楚易懂？<br>
        <textarea name="satisfaction" rows="3" cols="40"></textarea><br><br>
        您對我們的服務有什麼建議？<br>
        <textarea name="suggestion" rows="3" cols="40"></textarea><br><br>
        <button type="submit">送出</button>
    </form>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_FORM)

def hash_sha256(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def normalize_phone(phone):
    cleaned = re.sub(r"[^\d]", "", phone)
    return "886" + cleaned.lstrip("0") if cleaned.startswith("09") else cleaned

def save_to_excel(data, file_path):
    wb = Workbook()
    ws = wb.active
    ws.append(list(data.keys()))
    ws.append(list(data.values()))
    wb.save(file_path)

def send_email_with_attachment(file_path):
    msg = EmailMessage()
    msg['Subject'] = '新客戶表單回報'
    msg['From'] = FROM_EMAIL
    msg['To'] = [TO_EMAIL_1, TO_EMAIL_2]
    msg.set_content("請查收附件中的客戶填寫資料。")

    with open(file_path, 'rb') as f:
        msg.add_attachment(f.read(), maintype='application',
                           subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                           filename=file_path.name)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(FROM_EMAIL, EMAIL_PASSWORD)
        smtp.send_message(msg)

@app.route('/submit', methods=['POST'])
def submit():
    name = request.form.get("name", "").strip()
    birthday = request.form.get("birthday", "").strip()
    gender = request.form.get("gender", "female")
    email = request.form.get("email", "").strip().lower()
    phone = normalize_phone(request.form.get("phone", "").strip())
    satisfaction = request.form.get("satisfaction", "").strip()
    suggestion = request.form.get("suggestion", "").strip()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{timestamp}.xlsx"
    file_path = BACKUP_FOLDER / filename

    raw_data = {
        "姓名": name,
        "生日": birthday,
        "性別": gender,
        "Email": email,
        "電話": phone,
        "您覺得小編的服務態度如何？解說是否清楚易懂？": satisfaction,
        "您對我們的服務有什麼建議？": suggestion,
        "提交時間": timestamp,
    }
    save_to_excel(raw_data, file_path)

    user_data = {
        "fn": hash_sha256(name),
        "ge": "m" if gender == "male" else "f",
        "country": hash_sha256("tw"),
        "client_ip_address": request.remote_addr or "1.1.1.1"
    }
    if email:
        user_data["em"] = hash_sha256(email)
    if phone:
        user_data["ph"] = hash_sha256(phone)
    if birthday:
        try:
            dt = datetime.strptime(birthday, "%Y-%m-%d")
            user_data["db"] = dt.strftime("%Y%m%d")
        except:
            pass

    payload = {
        "data": [{
            "event_name": "FormSubmit",
            "event_time": int(datetime.now().timestamp()),
            "event_source_url": "https://meta-form-2.onrender.com/",
            "user_data": user_data,
            "custom_data": {
                "currency": CURRENCY,
                "value": random.choice(VALUE_CHOICES),
                "external_id": hash_sha256(name + phone + email)
            },
            "action_source": "website"
        }]
    }

    headers = {"Content-Type": "application/json"}
    requests.post(API_URL, headers=headers, json=payload, params={"access_token": ACCESS_TOKEN})

    send_email_with_attachment(file_path)
    return "感謝您提供寶貴建議"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
