from flask import Flask, request, render_template_string
import csv
import os
import hashlib
import requests
import random
import json
from datetime import datetime

app = Flask(__name__)

# --- Meta Conversion API 設定 ---
PIXEL_ID = "1664521517602334"
ACCESS_TOKEN = "EAAH1oqWMsq8BO37rKconweZBXXPFQac7NCNxFbD40RN9SopOp2t3o5xEPQ1zbkrOkKIUoBGPZBXbsxStkXsniH9EE777qANZAGKXNIgMtliLHZBntS2VTp7uDbLhNBZAFwZBShVw8QyOXbYSDFfwqxQCWtzJYbFzktZCJpD3BkyYeaTcOMP2zz0MnZCfppTCYGb8uQZDZD"  # ← 請填入你自己的有效權杖
CURRENCY = "TWD"
VALUE_CHOICES = [19800, 28000, 28800, 34800, 39800, 45800]
CITIES = ["taipei", "newtaipei", "taoyuan", "taichung", "tainan", "kaohsiung"]
CSV_FILE = "feedback.csv"

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
        Email：<input type="email" name="email" required><br><br>
        電話：<input type="tel" name="phone" required><br><br>
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

def hash_data(value):
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest() if value else ""

def send_to_meta(email, phone, gender, birthdate, ip):
    event_time = int(datetime.now().timestamp())
    event_id = hashlib.md5((email + str(event_time)).encode("utf-8")).hexdigest()
    value = random.choice(VALUE_CHOICES)
    city = random.choice(CITIES)

    user_data = {
        "em": hash_data(email),
        "ph": hash_data(phone),
        "ge": "m" if gender == "男" else "f",
        "db": birthdate.replace("-", ""),  # 格式 YYYYMMDD
        "country": "TW",                   # ← 正確格式，大寫 TW
        "client_ip_address": ip
        # 可選加上 ct: city.lower()，但請確認有效支援的城市再加
        # "ct": city.lower()
    }

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

    url = f"https://graph.facebook.com/v18.0/{PIXEL_ID}/events?access_token={ACCESS_TOKEN}"

    print("📤 即將上傳的 Meta payload：")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    try:
        res = requests.post(url, json=payload, timeout=10)
        print(f"✅ Meta 回傳：{res.status_code} - {res.text}")
    except Exception as e:
        print(f"❌ 上傳至 Meta 失敗：{e}")

@app.route("/", methods=["GET"])
def form():
    return render_template_string(HTML_FORM)

@app.route("/submit", methods=["POST"])
def submit():
    data = {
        "姓名": request.form["name"],
        "出生年月日": request.form["birthdate"],
        "性別": request.form["gender"],
        "Email": request.form["email"],
        "電話": request.form["phone"],
        "服務態度是否清楚易懂": request.form["attitude"],
        "建議": request.form.get("suggestion", "")
    }

    ip = request.remote_addr or "127.0.0.1"

    # 儲存到 CSV
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

    # 回傳至 Meta
    send_to_meta(data["Email"], data["電話"], data["性別"], data["出生年月日"], ip)

    return render_template_string(THANK_YOU_PAGE)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)
