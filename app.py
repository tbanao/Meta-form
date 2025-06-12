import os, re, json, random, time, hashlib, requests, smtplib, pickle, fcntl
from contextlib import contextmanager
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from flask import (
    Flask, request, render_template_string,
    redirect, url_for, session, make_response
)
from openpyxl import Workbook

# ===== 1) 必填/格式提示 2) 手機國碼處理 3) 回寫狀態 7) Retry queue
#    10) CSRF 11) HTTPS/HSTS 12) Log 脫敏 均已整合 =====

# ───────── 基本設定 ─────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me")          # ★ CSRF 需要
HSTS_HEADER = "max-age=63072000; includeSubDomains; preload"        # ★ HTTPS

PIXEL_ID     = os.environ["PIXEL_ID"]
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
API_URL      = f"https://graph.facebook.com/v14.0/{PIXEL_ID}/events"
CURRENCY     = "TWD"
VALUE_CHOICES = [19800, 28000, 28800, 34800, 39800, 45800]

FROM_EMAIL     = os.environ["FROM_EMAIL"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]
TO_EMAILS      = [os.environ["TO_EMAIL_1"], os.environ["TO_EMAIL_2"]]

CITIES = ["taipei","newtaipei","taoyuan","taichung","tainan","kaohsiung"]
CITY_ZIP_MAP = {"taipei":"100","newtaipei":"220","taoyuan":"330",
                "taichung":"400","tainan":"700","kaohsiung":"800"}

PROFILE_MAP_PATH = "user_profile_map.pkl"
BACKUP_FOLDER = Path("form_backups"); BACKUP_FOLDER.mkdir(exist_ok=True)
RETRY_FILE = "retry_queue.jsonl"                                     # ★ Retry
Path(RETRY_FILE).touch(exist_ok=True)

# ───────── 檢查檔案 ─────────
print("==== 檢查目錄檔案 ====")
for fn in os.listdir('.'): print("檔案:", fn)
for fn in [PROFILE_MAP_PATH, RETRY_FILE]: 
    print(f"檢查 {fn}: {'存在' if os.path.exists(fn) else '不存在'}")
print("======================\n")

# ───────── File-lock 工具 ─────────
@contextmanager
def locked_file(path: str, mode: str):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX); yield f; fcntl.flock(f, fcntl.LOCK_UN)

# ───────── 脫敏顯示工具 (12) ─────────
def mask(s, keep=2):
    if not s: return ""
    return s[:keep] + "*"*(max(0,len(s)-keep-2)) + s[-2:]

# ───────── 雜湊 / 格式 ─────────
hash_sha256 = lambda t: hashlib.sha256(t.encode()).hexdigest() if t else ""
def normalize_phone(p):
    p = re.sub(r"[^\d]", "", p)
    if p.startswith("8869"): return p
    if p.startswith("09"): return "886"+p.lstrip("0")
    return p

# ───────── CSRF token (10) ─────────
def generate_csrf():
    if "csrf_token" not in session:
        session["csrf_token"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf_token"]

def verify_csrf(tok): return tok and tok==session.get("csrf_token")

# ───────── HTML 表單 ─────────
HTML_FORM = '''
<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>服務滿意度調查</title>
<style>body{background:#f2f6fb;font-family:"微軟正黑體",Arial,sans-serif}
.form-container{background:rgba(255,255,255,.93);max-width:400px;margin:60px auto;padding:36px;border-radius:16px;
box-shadow:0 4px 16px rgba(0,0,0,.1);text-align:center}
input,select,textarea,button{width:90%;padding:6px 10px;margin:6px 0 12px;border:1px solid #ccc;border-radius:4px;
font-size:16px;background:#fafbfc}
button{background:#568cf5;color:#fff;border:none;font-weight:bold;padding:10px 0}
button:hover{background:#376ad8}h2{margin-top:0;color:#34495e}</style>
<script>
!function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;
n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}
(window,document,'script','https://connect.facebook.net/en_US/fbevents.js');
fbq('init','1664521517602334');fbq('track','PageView');
function genID(){return 'evt_'+Date.now()+'_'+Math.floor(Math.random()*1e5)}
function beforeSubmit(){
  let phone = document.querySelector('input[name=phone]').value;
  if(phone && !/^09\\d{8}$/.test(phone)){alert('手機格式需 09xxxxxxxx'); return false;}
  let id=genID();document.getElementById('eid').value=id;
  fbq('track','Purchase',{}, {eventID:id});return true;}
</script></head><body>
<div class="form-container"><h2>服務滿意度調查</h2>
<form action="/submit" method="post" onsubmit="return beforeSubmit();">
<input type="hidden" name="csrf_token" value="{{csrf}}">
姓名：<input type="text" name="name" required><br>
出生年月日：<input type="date" name="birthday"><br>
性別：<select name="gender"><option value="female">女性</option><option value="male">男性</option></select><br>
Email：<input type="email" name="email" pattern="[^@]+@[^@]+\\.[^@]+" required><br>
手機：<input type="text" name="phone" pattern="09\\d{8}" required><br>
城市：<select name="city">
  <option value="">--請選擇--</option>{% for c in cities %}
  <option value="{{c}}">{{c}}</option>{% endfor %}</select><br>
服務態度滿意度：<textarea name="satisfaction" rows="2"></textarea><br>
建議：<textarea name="suggestion" rows="2"></textarea><br>
<input type="hidden" id="eid" name="front_event_id">
<button type="submit">送出</button></form></div></body></html>
'''

# ───────── Flask Hooks (11) ─────────
@app.before_request
def force_https():
    if request.headers.get('X-Forwarded-Proto','http')!='https':
        return redirect(request.url.replace("http://","https://"), code=301)

@app.after_request
def add_hsts(resp):
    resp.headers['Strict-Transport-Security'] = HSTS_HEADER
    return resp

# ───────── Routes ─────────
@app.route('/')
def index():
    return render_template_string(HTML_FORM, csrf=generate_csrf(), cities=CITIES)

@app.route('/submit', methods=['POST'])
def submit():
    if not verify_csrf(request.form.get("csrf_token")):
        return "CSRF token 錯誤", 400   # ★ CSRF 檢查

    # 取得表單資料
    name=request.form["name"].strip()
    birthday=request.form.get("birthday","").strip()
    gender=request.form.get("gender","female")
    email=request.form["email"].strip().lower()
    phone=normalize_phone(request.form["phone"])
    city=request.form.get("city","").strip()
    satisfaction=request.form.get("satisfaction","")
    suggestion=request.form.get("suggestion","")

    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    fpath = BACKUP_FOLDER / f"{name}_{ts}.xlsx"
    save_to_excel({"姓名":name,"Email":email,"電話":phone,"城市":city,"提交時間":ts}, fpath)

    # 讀/寫 profile_map
    if not os.path.exists(PROFILE_MAP_PATH):
        with open(PROFILE_MAP_PATH,"wb") as f: pickle.dump({},f)
    with locked_file(PROFILE_MAP_PATH,"rb") as f: profiles=pickle.load(f)

    key=email or phone or name
    prof=profiles.get(key,{})

    # 拆姓名
    fn,ln=(name[:1],name[1:]) if re.match(r"^[\u4e00-\u9fa5]{2,4}$",name)\
         else (name.split()[0], " ".join(name.split()[1:]) if len(name.split())>1 else "")
    if not city and prof.get("ct"): city=prof["ct"]
    prof.update({"fn":fn,"ln":ln,"em":email,"ph":phone,"ct":city,
                 "zp":CITY_ZIP_MAP.get(city,""),"external_id":key})
    profiles[key]=prof
    with locked_file(PROFILE_MAP_PATH,"wb") as f: pickle.dump(profiles,f)

    # user_data
    ud={"external_id":hash_sha256(key),"fn":hash_sha256(fn),"ln":hash_sha256(ln),
        "em":hash_sha256(email),"ph":hash_sha256(phone)}
    if city: ud["ct"]=hash_sha256(city); ud["zp"]=hash_sha256(prof["zp"])

    custom={"currency":CURRENCY,"value":random.choice(VALUE_CHOICES),
            "submit_time":ts,"gender":gender,"birthday":birthday}

    event_id=f"evt_{int(time.time()*1000)}_{random.randint(1000,9999)}"
    payload={"data":[{
        "event_name":"Purchase","event_time":int(time.time()),
        "event_id":event_id,"action_source":"system_generated",
        "user_data":ud,"custom_data":custom}],
        "upload_tag":f"form_{ts}"}

    print("▶ 送出:", mask(email), mask(phone))
    resp=requests.post(API_URL,json=payload,params={"access_token":ACCESS_TOKEN})

    # 回寫狀態 (3)
    prof["last_capi_status"]={"code":resp.status_code,
                              "fbtrace_id":resp.json().get("fbtrace_id") if resp.ok else "",
                              "at":ts}
    with locked_file(PROFILE_MAP_PATH,"wb") as f: pickle.dump(profiles,f)

    if not resp.ok:                                        # ★ 7) Retry queue
        with open(RETRY_FILE,"a",encoding="utf-8") as f:
            f.write(json.dumps(payload,ensure_ascii=False)+"\n")

    send_email_with_attachment(fpath, {"姓名":name,"Email":email,"電話":phone})

    return "感謝您提供寶貴建議！"

# ───────── Main ─────────
if __name__=="__main__":
    for var in ["PIXEL_ID","ACCESS_TOKEN","FROM_EMAIL","EMAIL_PASSWORD",
                "TO_EMAIL_1","TO_EMAIL_2"]: 
        assert var in os.environ, f"缺少環境變數 {var}"
    app.run(host="0.0.0.0", port=10000)
