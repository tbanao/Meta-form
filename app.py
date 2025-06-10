from flask import Flask, request, render_template_string
from openpyxl import Workbook
from pathlib import Path
import hashlib
import requests
import smtplib
from email.message import EmailMessage
from datetime import datetime
import re
import os

app = Flask(__name__)

# === Meta Conversion API 設定 ===
PIXEL_ID = os.getenv("PIXEL_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
API_URL = f"https://graph.facebook.com/v18.0/{PIXEL_ID}/events"
CURRENCY = "TWD"
DEFAULT_VALUE = 20000

# === Email & 備份設定 ===
FROM_EMAIL = os.getenv("FROM_EMAIL")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TO_EMAIL_1 = os.getenv("TO_EMAIL_1")
TO_EMAIL_2 = os.getenv("TO_EMAIL_2")

BACKUP_FOLDER = Path("form_backups")
BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

# === HTML 表單 ===
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
        <textarea name="您覺得小編的服務態度如何？解說是否清楚易懂？" rows="3" cols="40"></textarea><br><br>
        您對我們的服務有什麼建議？<br>
        <textarea name="您對我們的服務有什麼建議？" rows="3" cols="40"></textarea><br><br>
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

def send_email_with_attachment(file_path, form_data):
    msg = EmailMessage()
    msg['Subject'] = '新客戶表單回報'
    msg['From'] = FROM_EMAIL
    msg['To'] = [TO_EMAIL_1, TO_EMAIL_2]

    content = f"""您有一份新的客戶填寫資料：

姓名：{form_data['姓名']}
生日：{form_data['生日']}
性別：{form_data['性別']}
Email：{form_data['Email']}
電話：{form_data['電話']}

✅ 滿意度調查：
{form_data['您覺得小編的服務態度如何？解說是否清楚易懂？']}

💡 建議回饋：
{form_data['您對我們的服務有什麼建議？']}

提交時間：{form_data['提交時間']}

附件為完整填寫內容 Excel 檔案。
"""
    msg.set_content(content)

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
    satisfaction = request.form.get("您覺得小編的服務態度如何？解說是否清楚易懂？", "").strip()
    suggestion = request.form.get("您對我們的服務有什麼建議？", "").strip()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{timestamp}.xlsx"
    file_path = BACKUP_FOLDER / filename

    form_data = {
        "姓名": name,
        "生日": birthday,
        "性別": gender,
        "Email": email,
        "電話": phone,
        "您覺得小編的服務態度如何？解說是否清楚易懂？": satisfaction,
        "您對我們的服務有什麼建議？": suggestion,
        "提交時間": timestamp,
    }

    save_to_excel(form_data, file_path)
    send_email_with_attachment(file_path, form_data)

    # === Meta CAPI 上傳 ===
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
            "event_source_url": "https://yourdomain.onrender.com/",
            "user_data": user_data,
            "custom_data": {
                "currency": CURRENCY,
                "value": DEFAULT_VALUE,
                "external_id": hash_sha256(name + phone + email)
            },
            "action_source": "website"
        }]
    }

    headers = {"Content-Type": "application/json"}
    requests.post(API_URL, headers=headers, json=payload, params={"access_token": ACCESS_TOKEN})

    return "提交成功！感謝您的填寫。"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
