import os, re, json, random, time, hashlib, requests, smtplib, pickle, fcntl, traceback
from contextlib import contextmanager
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from flask import Flask, request, render_template_string, redirect, session
from openpyxl import Workbook

# ──────────── 基本設定 ────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24).hex())
HSTS_HEADER = "max-age=63072000; includeSubDomains; preload"

PIXEL_ID, ACCESS_TOKEN = os.environ["PIXEL_ID"], os.environ["ACCESS_TOKEN"]
API_URL = f"https://graph.facebook.com/v14.0/{PIXEL_ID}/events"
CURRENCY, VALUE_CHOICES = "TWD", [19800,28000,28800,34800,39800,45800]

FROM_EMAIL, EMAIL_PASSWORD = os.environ["FROM_EMAIL"], os.environ["EMAIL_PASSWORD"]
TO_EMAILS = [os.environ["TO_EMAIL_1"], os.environ["TO_EMAIL_2"]]

CITIES = ["taipei","newtaipei","taoyuan","taichung","tainan","kaohsiung"]
CITY_ZIP_MAP = {"taipei":"100","newtaipei":"220","taoyuan":"330",
                "taichung":"400","tainan":"700","kaohsiung":"800"}

PROFILE_MAP_PATH = "user_profile_map.pkl"
BACKUP_FOLDER = Path("form_backups"); BACKUP_FOLDER.mkdir(exist_ok=True)
RETRY_FILE = "retry_queue.jsonl"; Path(RETRY_FILE).touch(exist_ok=True)

# ──────────── 工具 ────────────
@contextmanager
def locked(path, mode):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX); yield f; fcntl.flock(f, fcntl.LOCK_UN)

mask = lambda s,k=2: s[:k]+"*"*(max(0,len(s)-k-2))+s[-2:] if s else ""
hash_sha256 = lambda t: hashlib.sha256(t.encode()).hexdigest() if t else ""
normalize_phone = lambda p: ("886"+re.sub(r"[^\d]","",p).lstrip("0")) if p.startswith("09") else re.sub(r"[^\d]","",p)

def csrf_token():
    if "csrf" not in session: session["csrf"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]

def save_excel(data:dict, path:Path):
    wb = Workbook(); ws = wb.active; ws.append(data.keys()); ws.append(data.values()); wb.save(path)

def send_mail(path:Path, data:dict):
    msg = EmailMessage(); msg["Subject"]="新客戶表單回報"; msg["From"]=FROM_EMAIL; msg["To"]=TO_EMAILS
    msg.set_content("\n".join(f"{k}: {v}" for k,v in data.items()))
    with open(path,"rb") as f: msg.add_attachment(f.read(), maintype="application",
                                                  subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                  filename=path.name)
    with smtplib.SMTP_SSL("smtp.gmail.com",465) as s: s.login(FROM_EMAIL,EMAIL_PASSWORD); s.send_message(msg)

# ──────────── HTML 表單（無城市欄位） ────────────
HTML = '''
<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>服務滿意度調查</title>
<style>body{background:#f2f6fb;font-family:"微軟正黑體",Arial,sans-serif}
.form-container{background:rgba(255,255,255,.93);max-width:400px;margin:60px auto;padding:36px;border-radius:16px;
box-shadow:0 4px 16px rgba(0,0,0,.1);text-align:center}
input,select,textarea,button{width:90%;padding:6px 10px;margin:6px 0 12px;border:1px solid #ccc;border-radius:4px;font-size:16px;background:#fafbfc}
button{background:#568cf5;color:#fff;border:none;font-weight:bold;padding:10px 0}
button:hover{background:#376ad8}h2{margin-top:0;color:#34495e}</style>
<script>
!function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;
n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}
(window,document,'script','https://connect.facebook.net/en_US/fbevents.js');
fbq('init','{{PIXEL}}');fbq('track','PageView');
function gen(){return 'evt_'+Date.now()+'_'+Math.random().toString(36).slice(2)}
function beforeSubmit(){
  let p=document.querySelector('input[name=phone]').value;
  if(p&&!/^09\\d{8}$/.test(p)){alert('手機格式需09xxxxxxxx');return false;}
  let id=gen();document.getElementById('eid').value=id;
  fbq('track','Purchase',{}, {eventID:id});return true;}
</script></head><body>
<div class="form-container"><h2>服務滿意度調查</h2>
<form method="post" action="/submit" onsubmit="return beforeSubmit();">
<input type="hidden" name="csrf_token" value="{{csrf}}">
姓名：<input type="text" name="name" required><br>
出生年月日：<input type="date" name="birthday"><br>
性別：<select name="gender"><option value="female">女性</option><option value="male">男性</option></select><br>
Email：<input type="email" name="email" pattern="[^@]+@[^@]+\\.[^@]+" required><br>
手機：<input type="text" name="phone" pattern="09\\d{8}" required><br>
服務態度滿意度：<textarea name="satisfaction" rows="2"></textarea><br>
建議：<textarea name="suggestion" rows="2"></textarea><br>
<input type="hidden" id="eid" name="front_event_id">
<button type="submit">送出</button></form></div></body></html>
'''

# ──────────── 安全 Hook ────────────
@app.before_request
def force_https():
    if request.headers.get("X-Forwarded-Proto","http")!="https":
        return redirect(request.url.replace("http://","https://"), code=301)
@app.after_request
def add_hsts(r): r.headers["Strict-Transport-Security"]=HSTS_HEADER; return r

# ──────────── Routes ────────────
@app.route('/healthz')
def healthz(): return "OK",200

@app.route('/')
def index():
    return render_template_string(HTML, csrf=csrf_token(), PIXEL=PIXEL_ID)

@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token")!=session.get("csrf"): return "CSRF token 錯誤",400

    name=request.form["name"].strip()
    birthday=request.form.get("birthday","").strip()
    gender=request.form.get("gender","female")
    email=request.form["email"].strip().lower()
    phone=normalize_phone(request.form["phone"])
    sat=request.form.get("satisfaction","")
    sug=request.form.get("suggestion","")

    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    raw={"姓名":name,"生日":birthday,"性別":gender,"Email":email,"電話":phone,
         "服務態度評價":sat,"建議":sug,"提交時間":ts}

    backup=BACKUP_FOLDER/f"{name}_{ts}.xlsx"; save_excel(raw, backup)

    if not Path(PROFILE_MAP_PATH).exists():
        with open(PROFILE_MAP_PATH,"wb") as f: pickle.dump({},f)
    with locked(PROFILE_MAP_PATH,"rb") as f: profiles=pickle.load(f)

    key=email or phone or name
    prof=profiles.get(key,{})
    fn,ln=(name[:1],name[1:]) if re.match(r"^[\u4e00-\u9fa5]{2,4}$",name)\
        else (name.split()[0], " ".join(name.split()[1:]) if len(name.split())>1 else "")
    city=prof.get("ct") or random.choice(CITIES)
    prof.update({"fn":fn,"ln":ln,"em":email,"ph":phone,"ct":city,
                 "zp":CITY_ZIP_MAP[city],"external_id":key})
    profiles[key]=prof
    with locked(PROFILE_MAP_PATH,"wb") as f: pickle.dump(profiles,f)

    event_id=request.form.get("front_event_id","").strip() or f"evt_{int(time.time()*1000)}_{random.randint(1000,9999)}"

    ud={"external_id":hash_sha256(key),"fn":hash_sha256(fn),"ln":hash_sha256(ln),
        "em":hash_sha256(email),"ph":hash_sha256(phone),
        "ct":hash_sha256(city),"zp":hash_sha256(CITY_ZIP_MAP[city])}

    custom={"currency":CURRENCY,"value":random.choice(VALUE_CHOICES),
            "submit_time":ts,"gender":gender,"birthday":birthday,
            "satisfaction":sat,"suggestion":sug}

    payload={"data":[{"event_name":"Purchase","event_time":int(time.time()),
                      "event_id":event_id,"action_source":"system_generated",
                      "user_data":ud,"custom_data":custom}],
             "upload_tag":f"form_{ts}"}

    print("▶ 送出:", mask(email), mask(phone))
    try:
        resp=requests.post(API_URL,json=payload,params={"access_token":ACCESS_TOKEN},timeout=10)
    except Exception as e:
        print("❌ CAPI error:", e); traceback.print_exc()
        resp=type("obj",(),{"ok":False,"status_code":0,"json":lambda self:{}})()

    prof["last_capi_status"]={"code":resp.status_code,
                              "fbtrace":resp.json().get("fbtrace_id") if resp.ok else "",
                              "at":ts}
    with locked(PROFILE_MAP_PATH,"wb") as f: pickle.dump(profiles,f)
    if not resp.ok:
        with open(RETRY_FILE,"a",encoding="utf-8") as f: f.write(json.dumps(payload)+"\n")

    send_mail(backup, raw)
    return "感謝您提供寶貴建議！"

# ──────────── Main ────────────
if __name__=="__main__":
    for v in ["PIXEL_ID","ACCESS_TOKEN","FROM_EMAIL","EMAIL_PASSWORD","TO_EMAIL_1","TO_EMAIL_2"]:
        assert v in os.environ, f"缺少環境變數 {v}"
    app.run(host="0.0.0.0", port=10000)
