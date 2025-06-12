import os
import re
import json
import random
import time            # ← 必要：用於產生 event_id
import hashlib
import requests
import smtplib
import pickle
import fcntl
from contextlib import contextmanager
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from flask import Flask, request, render_template_string
from openpyxl import Workbook

# ====== 目錄檢查（佈署時可保留觀察用） ======
print("==== Render 檢查目錄檔案 ====")
for fn in os.listdir('.'):
    print("檔案：", fn)
for fn in ["uploaded_event_ids.txt", "user_profile_map.pkl"]:
    print(f"檢查 {fn}: {'存在' if os.path.exists(fn) else '不存在'}")
print("============================\n")

app = Flask(__name__)

# ---------- 地區設定 ----------
CITIES = ["taipei", "newtaipei", "taoyuan", "taichung", "tainan", "kaohsiung"]
CITY_ZIP_MAP = {
    "taipei": "100", "newtaipei": "220", "taoyuan": "330",
    "taichung": "400", "tainan": "700", "kaohsiung": "800"
}

# ---------- 環境變數 ----------
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

# ---------- File-lock 工具 ----------
@contextmanager
def locked_file(path: str, mode: str):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield f
        fcntl.flock(f, fcntl.LOCK_UN)

# ---------- 前端 HTML ----------
HTML_FORM = '''
<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>服務滿意度調查</title>
<style>
body{background:#f2f6fb;font-family:"微軟正黑體",Arial,sans-serif}
.form-container{background:rgba(255,255,255,0.93);max-width:400px;margin:60px auto;padding:36px 32px;border-radius:16px;
box-shadow:0 4px 16px rgba(0,0,0,0.10);text-align:center}
input,select,textarea,button{width:90%;padding:6px 10px;margin:6px 0 12px;border:1px solid #ccc;border-radius:4px;font-size:16px;background:#fafbfc}
button{background:#568cf5;color:#fff;border:none;font-weight:bold;padding:10px 0;transition:background .3s}
button:hover{background:#376ad8}
h2{margin-top:0;color:#34495e}
</style>
<script>
!function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;
n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}
(window,document,'script','https://connect.facebook.net/en_US/fbevents.js');
fbq('init','1664521517602334');fbq('track','PageView');
function genID(){return 'evt_'+Date.now()+'_'+Math.floor(Math.random()*1e5)}
function beforeSubmit(){var id=genID();document.getElementById('eid').value=id;
fbq('track','Purchase',{}, {eventID:id});return true;}
</script></head><body>
<div class="form-container">
<h2>服務滿意度調查</h2>
<form action="/submit" method="post" onsubmit="return beforeSubmit();">
姓名：<input type="text" name="name" required><br>
出生年月日：<input type="date" name="birthday"><br>
性別：<select name="gender"><option value="female">女性</option><option value="male">男性</option></select><br>
Email：<input type="email" name="email"><br>
電話：<input type="text" name="phone"><br>
城市：
<select name="city">
  <option value="">--請選擇--</option>
  <option value="taipei">台北</option><option value="newtaipei">新北</option>
  <option value="taoyuan">桃園</option><option value="taichung">台中</option>
  <option value="tainan">台南</option><option value="kaohsiung">高雄</option>
</select><br>
您覺得我們小編的服務態度如何？<br>
<textarea name="satisfaction" rows="3" cols="40"></textarea><br>
您對我們的服務有什麼建議？<br>
<textarea name="suggestion" rows="3" cols="40"></textarea><br>
<input type="hidden" id="eid" name="front_event_id" value="">
<button type="submit">送出</button>
</form></div></body></html>
'''

# ---------- 工具函式 ----------
def hash_sha256(txt: str) -> str:
    return hashlib.sha256(txt.encode('utf-8')).hexdigest() if txt else ""

def normalize_phone(phone: str) -> str:
    cleaned = re.sub(r"[^\d]", "", phone)
    return "886" + cleaned.lstrip("0") if cleaned.startswith("09") else cleaned

def save_to_excel(data: dict, fpath: Path):
    wb = Workbook(); ws = wb.active
    ws.append(list(data.keys())); ws.append(list(data.values())); wb.save(fpath)

def build_email_content(d: dict) -> str:
    return "\n".join(f"{k}: {v}" for k, v in d.items())

def send_email_with_attachment(fpath: Path, row: dict):
    msg = EmailMessage()
    msg["Subject"] = "新客戶表單回報"
    msg["From"]    = FROM_EMAIL
    msg["To"]      = [TO_EMAIL_1, TO_EMAIL_2]
    msg.set_content("客戶填寫內容如下：\n\n"+build_email_content(row))
    with open(fpath, "rb") as f:
        msg.add_attachment(f.read(), maintype="application",
                           subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           filename=fpath.name)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(FROM_EMAIL, EMAIL_PASSWORD); s.send_message(msg)

# ---------- 路由 ----------
@app.route('/')
def index(): return render_template_string(HTML_FORM)

@app.route('/submit', methods=['POST'])
def submit():
    # ------ 取得表單資料 ------
    name   = request.form.get("name","").strip()
    birthday = request.form.get("birthday","").strip()
    gender = request.form.get("gender","female")
    email  = request.form.get("email","").strip().lower()
    phone  = normalize_phone(request.form.get("phone","").strip())
    city   = request.form.get("city","").strip()
    satisfaction = request.form.get("satisfaction","").strip()
    suggestion   = request.form.get("suggestion","").strip()

    # ------ 建立 Excel 備份 ------
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fpath = BACKUP_FOLDER / f"{name}_{ts}.xlsx"
    raw = {"姓名":name,"生日":birthday,"性別":gender,"Email":email,"電話":phone,
           "城市":city,"服務態度評價":satisfaction,"建議":suggestion,"提交時間":ts}
    save_to_excel(raw, fpath)

    # ------ 讀寫 profile_map （with lock） ------
    if not os.path.exists(PROFILE_MAP_PATH):
        with open(PROFILE_MAP_PATH,"wb") as f: pickle.dump({}, f)

    user_key = email or phone or name
    with locked_file(PROFILE_MAP_PATH,"rb") as f:
        profile_map = pickle.load(f)

    profile = profile_map.get(user_key, {})
    if not city and profile.get("ct"): city = profile["ct"]
    zip_code = CITY_ZIP_MAP.get(city,"")

    # 拆姓名
    if re.match(r"^[\u4e00-\u9fa5]{2,4}$", name):
        fn, ln = name[:1], name[1:]
    else:
        parts = name.split(); fn = parts[0]; ln = " ".join(parts[1:]) if len(parts)>1 else ""

    profile.update({
        "fn":fn, "ln":ln, "em":email, "ph":phone, "db":birthday,
        "ge":"f" if gender=="female" else "m",
        "ct":city, "st":"taiwan" if city else "",
        "country":"tw" if city else "", "zp":zip_code,
        "external_id": email or phone or name
    })
    profile_map[user_key]=profile
    with locked_file(PROFILE_MAP_PATH,"wb") as f:
        pickle.dump(profile_map, f)

    # ------ user_data 雜湊 ------
    user_data = {"external_id": hash_sha256(email or phone or name)}
    if fn:   user_data["fn"] = hash_sha256(fn)
    if ln:   user_data["ln"] = hash_sha256(ln)
    if email: user_data["em"] = hash_sha256(email)
    if phone: user_data["ph"] = hash_sha256(phone)
    for fld in ("ct","st","country","zp"):
        if profile.get(fld): user_data[fld] = hash_sha256(profile[fld])

    # ------ custom_data ------
    custom_data = {"currency":CURRENCY,"value":random.choice(VALUE_CHOICES),
                   "gender":gender,"birthday":birthday,
                   "satisfaction":satisfaction,"suggestion":suggestion,
                   "submit_time":ts}

    # ------ 後端 event_id ------
    event_id = f"evt_{int(time.time()*1000)}_{random.randint(1000,9999)}"

    payload = {
        "data":[{
            "event_name":"Purchase",
            "event_time": int(datetime.now().timestamp()),
            "event_id": event_id,
            "action_source":"system_generated",
            "user_data": user_data,
            "custom_data": custom_data
        }],
        "upload_tag": f"form_{ts}"
    }

    print("送出 Meta payload:", json.dumps(payload, ensure_ascii=False))
    resp = requests.post(API_URL, json=payload,
                         params={"access_token":ACCESS_TOKEN},
                         headers={"Content-Type":"application/json"})
    print("Meta Dataset 回應:", resp.status_code, resp.text)

    # ------ Email 通知 ------
    send_email_with_attachment(fpath, raw)

    return "感謝您提供寶貴建議！"

# ---------- 主程序 ----------
if __name__ == "__main__":
    for var in ["PIXEL_ID","ACCESS_TOKEN","FROM_EMAIL","EMAIL_PASSWORD","TO_EMAIL_1","TO_EMAIL_2"]:
        if var not in os.environ:
            raise RuntimeError(f"❌ 缺少環境變數：{var}")
    app.run(host="0.0.0.0", port=10000)
