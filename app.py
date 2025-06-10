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

# ====== Meta Conversion API 設定（直接填） ======
PIXEL_ID = "1664521517602334"
ACCESS_TOKEN = "EAAH1oqWMsq8BO5t3qLkIYbmIx4bt8o2pYMTNV14YJojE3EN7rG3ZC2GkmHpGeLgn1RepPRaIRUbbgo1QRocv98WqhhcrAGFOt3T6c1ah3fpObM6aLWecyQOpyhdCw3ZCS4xUp8ZAZA6vFpwetOFj6K9WZBiZByLxtuRsNAzCDNifMtxZBAVOENYCeMDrvbTJ5AVqwZDZD"
API_URL = f"https://graph.facebook.com/v23.0/{PIXEL_ID}/events"
CURRENCY = "TWD"
VALUE_CHOICES = [19800, 28000, 28800, 34800, 39800, 45800]

# ====== Email 設定（直接填） ======
FROM_EMAIL = "thairayshin@gmail.com"
EMAIL_PASSWORD = "omtsdcpqngodcfaq"
TO_EMAIL_1 = "z316le725@icloud.com"
TO_EMAIL_2 = "tbanao@icloud.com"
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

def build_email_content(data):
    # 將表單資料轉成可閱讀的純文字
    lines = []
    for key, value in data.items():
        lines.append(f"{key}: {value}")
    return "\n".join(lines)

def send_email_with_attachment(file_path, raw_data):
    msg = EmailMessage()
    msg['Subject'] = '新客戶表單回報'
    msg['From'] = FROM_EMAIL
    msg['To'] = [TO_EMAIL_1, TO_EMAIL_2]
    msg.set_content("客戶填寫內容如下：\n\n" + build_email_content(raw_data))

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

    # 儲存至 Excel
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

    # Meta user_data
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
    response = requests.post(API_URL, headers=headers, json=payload, params={"access_token": ACCESS_TOKEN})

    # 將 Meta 回傳內容列印到 log
    print("Meta 上傳結果：", response.status_code)
    print("Meta 回應內容：", response.text)

    send_email_with_attachment(file_path, raw_data)
    return "感謝您提供寶貴建議"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
