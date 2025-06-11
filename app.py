import os
import re
import random
import hashlib
import requests
import smtplib
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from flask import Flask, request, render_template_string
from openpyxl import Workbook

app = Flask(__name__)

# ====== 從環境變數讀取設定 ======
PIXEL_ID      = os.environ["PIXEL_ID"]
ACCESS_TOKEN  = os.environ["ACCESS_TOKEN"]
API_URL       = f"https://graph.facebook.com/v14.0/{PIXEL_ID}/events"
CURRENCY      = "TWD"
VALUE_CHOICES = [19800, 28000, 28800, 34800, 39800, 45800]

FROM_EMAIL     = os.environ["FROM_EMAIL"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]
TO_EMAIL_1     = os.environ["TO_EMAIL_1"]
TO_EMAIL_2     = os.environ["TO_EMAIL_2"]

# ====== 表單備份資料夾 ======
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

def hash_sha256(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def normalize_phone(phone: str) -> str:
    cleaned = re.sub(r"[^\d]", "", phone)
    return "886" + cleaned.lstrip("0") if cleaned.startswith("09") else cleaned

def save_to_excel(data: dict, file_path: Path):
    wb = Workbook()
    ws = wb.active
    ws.append(list(data.keys()))
    ws.append(list(data.values()))
    wb.save(file_path)

def build_email_content(data: dict) -> str:
    return "\n".join(f"{k}: {v}" for k, v in data.items())

def send_email_with_attachment(file_path: Path, raw_data: dict):
    msg = EmailMessage()
    msg["Subject"] = "新客戶表單回報"
    msg["From"]    = FROM_EMAIL
    msg["To"]      = [TO_EMAIL_1, TO_EMAIL_2]
    msg.set_content("客戶填寫內容如下：\n\n" + build_email_content(raw_data))

    with open(file_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=file_path.name
        )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(FROM_EMAIL, EMAIL_PASSWORD)
        smtp.send_message(msg)

@app.route('/submit', methods=['POST'])
def submit():
    # 1. 讀表單
    name       = request.form.get("name", "").strip()
    birthday   = request.form.get("birthday", "").strip()
    gender     = request.form.get("gender", "female")
    email      = request.form.get("email", "").strip().lower()
    phone      = normalize_phone(request.form.get("phone", "").strip())
    satisfaction = request.form.get("satisfaction", "").strip()
    suggestion = request.form.get("suggestion", "").strip()

    # 2. 備份 Excel
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    fn        = f"{name}_{ts}.xlsx"
    file_path = BACKUP_FOLDER / fn
    raw_data  = {
        "姓名": name, "生日": birthday, "性別": gender,
        "Email": email, "電話": phone,
        "服務態度評價": satisfaction, "建議": suggestion,
        "提交時間": ts
    }
    save_to_excel(raw_data, file_path)

    # 3. 組 user_data
    user_data = {
        "fn": hash_sha256(name),
        "ge": "m" if gender=="male" else "f",
        "country": hash_sha256("tw"),
        "client_ip_address": request.remote_addr or "0.0.0.0"
    }
    if email:
        user_data["em"] = hash_sha256(email)
    if phone:
        user_data["ph"] = hash_sha256(phone)
    if birthday:
        try:
            dt = datetime.strptime(birthday, "%Y-%m-%d")
            user_data["db"] = dt.strftime("%Y%m%d")
        except ValueError:
            pass

    # 4. 構建 payload（加上必填 action_source）
    payload = {
        "data": [{
            "event_name":      "FormSubmit",
            "event_time":      int(datetime.now().timestamp()),
            "action_source":   "website",
            "event_source_url":"https://your-domain.com/",  # 改成你自己的
            "user_data":       user_data,
            "custom_data": {
                "currency":    CURRENCY,
                "value":       random.choice(VALUE_CHOICES),
                "external_id": hash_sha256(name + phone + email)
            }
        }]
    }

    # 5. 呼叫 Meta CAPI
    resp = requests.post(
        API_URL,
        json=payload,
        params={"access_token": ACCESS_TOKEN},
        headers={"Content-Type": "application/json"}
    )
    print("Meta 上傳結果：", resp.status_code, resp.text)

    # 6. 發信通知
    send_email_with_attachment(file_path, raw_data)

    return "感謝您提供寶貴建議"

if __name__ == "__main__":
    # 若環境變數沒設，程式啟動就會錯誤，提醒你去設定
    for var in ["PIXEL_ID","ACCESS_TOKEN","FROM_EMAIL","EMAIL_PASSWORD","TO_EMAIL_1","TO_EMAIL_2"]:
        if var not in os.environ:
            raise RuntimeError(f"❌ 未設定環境變數：{var}")
    app.run(host="0.0.0.0", port=10000)
