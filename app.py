import os
import re
import random
import hashlib
import requests
import smtplib
import pickle
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from flask import Flask, request, render_template_string
from openpyxl import Workbook

print("==== Render 檢查目錄檔案 ====")
for filename in os.listdir('.'):
    print("檔案：", filename)
for filename in ["uploaded_event_ids.txt", "user_profile_map.pkl"]:
    print(f"檢查檔案 {filename}: {'存在' if os.path.exists(filename) else '不存在'}")
print("==============================")

app = Flask(__name__)

# 六都清單
CITIES = ["taipei", "newtaipei", "taoyuan", "taichung", "tainan", "kaohsiung"]
# 城市-郵遞區號對照表
CITY_ZIP_MAP = {
    "taipei": "100",
    "newtaipei": "220",
    "taoyuan": "330",
    "taichung": "400",
    "tainan": "700",
    "kaohsiung": "800",
}

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

PROFILE_MAP_PATH = "user_profile_map.pkl"
BACKUP_FOLDER = Path("form_backups")
BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

HTML_FORM = '''
<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <title>服務滿意度調查</title>
    <style>
        body {
            background: #f2f6fb;
            font-family: "微軟正黑體", Arial, sans-serif;
        }
        .form-container {
            background: rgba(255,255,255,0.93);
            max-width: 400px;
            margin: 60px auto 0 auto;
            padding: 36px 32px 28px 32px;
            border-radius: 16px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.10);
            text-align: center;
        }
        input, select, textarea, button {
            width: 90%;
            padding: 6px 10px;
            margin: 6px 0 12px 0;
            border: 1px solid #ccc;
            border-radius: 4px;
            font-size: 16px;
            background: #fafbfc;
        }
        button {
            background: #568cf5;
            color: #fff;
            border: none;
            font-weight: bold;
            padding: 10px 0;
            transition: background 0.3s;
        }
        button:hover {
            background: #376ad8;
        }
        h2 {
            margin-top: 0;
            color: #34495e;
        }
    </style>
    <!-- Meta Pixel Code -->
    <script>
    !function(f,b,e,v,n,t,s)
    {if(f.fbq)return;n=f.fbq=function(){n.callMethod?
    n.callMethod.apply(n,arguments):n.queue.push(arguments)};
    if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
    n.queue=[];t=b.createElement(e);t.async=!0;
    t.src=v;s=b.getElementsByTagName(e)[0];
    s.parentNode.insertBefore(t,s)}(window, document,'script',
    'https://connect.facebook.net/en_US/fbevents.js');
    fbq('init', '1664521517602334');
    fbq('track', 'PageView');
    </script>
    <noscript>
        <img height="1" width="1" style="display:none"
             src="https://www.facebook.com/tr?id=1664521517602334&ev=PageView&noscript=1"/>
    </noscript>
    <!-- End Meta Pixel Code -->
</head>
<body>
    <div class="form-container">
        <h2>服務滿意度調查</h2>
        <form id="feedbackForm" action="/submit" method="post" onsubmit="return beforeSubmit();">
            姓名：<input type="text" name="name" required><br>
            出生年月日：<input type="date" name="birthday"><br>
            性別：
            <select name="gender">
                <option value="female">女性</option>
                <option value="male">男性</option>
            </select><br>
            Email：<input type="email" name="email"><br>
            電話：<input type="text" name="phone"><br>
            您覺得我們小編的服務態度如何？解說是否清楚易懂？<br>
            <textarea name="satisfaction" rows="3" cols="40"></textarea><br>
            您對我們的服務有什麼建議？<br>
            <textarea name="suggestion" rows="3" cols="40"></textarea><br>
            <input type="hidden" name="event_id" id="event_id" value="">
            <button type="submit">送出</button>
        </form>
    </div>
    <script>
    function generateEventID() {
        return 'evt_' + Date.now() + '_' + Math.floor(Math.random()*100000);
    }
    function beforeSubmit() {
        var eid = generateEventID();
        document.getElementById('event_id').value = eid;
        fbq('track', 'Purchase', {}, {eventID: eid});
        return true;
    }
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_FORM)

def hash_sha256(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest() if text else ""

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
    name         = request.form.get("name", "").strip()
    birthday     = request.form.get("birthday", "").strip()
    gender       = request.form.get("gender", "female")
    email        = request.form.get("email", "").strip().lower()
    phone        = normalize_phone(request.form.get("phone", "").strip())
    satisfaction = request.form.get("satisfaction", "").strip()
    suggestion   = request.form.get("suggestion", "").strip()
    event_id     = request.form.get("event_id", "")

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

    # ====== 補強 user_profile_map（地區隨機分配六都+對應郵遞區號，並保證同 user_key 永遠一樣）======
    user_key = email or phone or name
    if os.path.exists(PROFILE_MAP_PATH):
        with open(PROFILE_MAP_PATH, "rb") as f:
            user_profile_map = pickle.load(f)
    else:
        user_profile_map = {}

    profile = user_profile_map.get(user_key, {})
    # 地區分配
    if "ct" in profile and profile["ct"]:
        city = profile["ct"]
    else:
        city = random.choice(CITIES)
    zip_code = CITY_ZIP_MAP.get(city, "")

    new_profile = {
        "fn": name[:1] if name else "",
        "ln": name[1:] if name and len(name) > 1 else "",
        "em": email,
        "ph": phone,
        "db": birthday,
        "ge": "f" if gender == "female" else "m",
        "ct": city,
        "st": "taiwan",
        "country": "tw",
        "zp": zip_code,
        "external_id": email or phone or name
    }
    for k, v in new_profile.items():
        if v:
            profile[k] = v
    user_profile_map[user_key] = profile
    with open(PROFILE_MAP_PATH, "wb") as f:
        pickle.dump(user_profile_map, f)
    # ====== end 補強 ======

    # ====== 上傳事件到 Meta CAPI，地區與郵遞區號也一併送出 ======
    user_data = {
        "external_id": hash_sha256(name + phone + email)
    }
    if name:
        user_data["fn"] = hash_sha256(name)
    if email:
        user_data["em"] = hash_sha256(email)
    if phone:
        user_data["ph"] = hash_sha256(phone)
    if profile.get("ct"):
        user_data["ct"] = hash_sha256(profile["ct"])
    if profile.get("st"):
        user_data["st"] = hash_sha256(profile["st"])
    if profile.get("country"):
        user_data["country"] = hash_sha256(profile["country"])
    if profile.get("zp"):
        user_data["zp"] = hash_sha256(profile["zp"])

    custom_data = {
        "currency":    CURRENCY,
        "value":       random.choice(VALUE_CHOICES),
        "gender":      gender,
        "birthday":    birthday,
        "satisfaction": satisfaction,
        "suggestion":  suggestion,
        "submit_time": ts
    }

    payload = {
        "data": [{
            "event_name":    "Purchase",
            "event_time":    int(datetime.now().timestamp()),
            "event_id":      event_id,
            "action_source": "system_generated",
            "user_data":     user_data,
            "custom_data":   custom_data
        }],
        "upload_tag": f"form_{ts}"
    }

    print("送出 Meta payload：", payload)

    resp = requests.post(
        API_URL,
        json=payload,
        params={"access_token": ACCESS_TOKEN},
        headers={"Content-Type": "application/json"}
    )
    print("Meta Dataset 回應：", resp.status_code, resp.text)

    send_email_with_attachment(file_path, raw_data)

    return "感謝您提供寶貴建議"

if __name__ == "__main__":
    for var in ["PIXEL_ID","ACCESS_TOKEN","FROM_EMAIL","EMAIL_PASSWORD","TO_EMAIL_1","TO_EMAIL_2"]:
        if var not in os.environ:
            raise RuntimeError(f"❌ 未設定環境變數：{var}")
    app.run(host="0.0.0.0", port=10000)
