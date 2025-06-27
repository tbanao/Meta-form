#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — 2025-06-28 完整可部署版
--------------------------------------------------
• 補件候選：只要 em 或 ph 有值
• 每月同一筆只補件一次（下月重置）
• PageView → Purchase 兩段串流
• user_data 哈希所有 PII 欄位：fn/ln, em, ph, ge, country, dob
• custom_data 上傳價值、滿意度、建議
• 表單寫入 user_profile_map，Excel 備份、Email 通知
"""

import os, re, time, json, hashlib, logging, smtplib, sys, fcntl, pickle, threading, random
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

import requests
from flask import Flask, request, render_template_string, redirect, session, make_response
from openpyxl import Workbook
from werkzeug.middleware.proxy_fix import ProxyFix

# ─────────────── 基本設定 ─────────────── #
REQUIRED = [
    "PIXEL_ID","ACCESS_TOKEN",
    "FROM_EMAIL","EMAIL_PASSWORD",
    "TO_EMAIL_1","TO_EMAIL_2",
    "SECRET_KEY"
]
missing = [v for v in REQUIRED if not os.getenv(v)]
if missing:
    logging.critical("缺少環境變數：%s", ", ".join(missing))
    sys.exit(1)

PIXEL_ID = os.getenv("PIXEL_ID")
TOKEN    = os.getenv("ACCESS_TOKEN")
API_URL  = f"https://graph.facebook.com/v19.0/{PIXEL_ID}/events"
CURRENCY = "TWD"
PRICES   = [19800,28800,34800,39800,45800]

USER_PROFILE_MAP_PATH = "user_profile_map.pkl"
EVENT_LOG            = Path("event_submit_log.txt")
BACKUP               = Path("form_backups"); BACKUP.mkdir(exist_ok=True)
AUTO_USED_PATH       = "auto_used.pkl"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")
app.wsgi_app   = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

sha = lambda s: hashlib.sha256(s.encode()).hexdigest() if s else ""
norm_phone = lambda p: ("886"+re.sub(r"[^\d]","",p).lstrip("0")) if p.startswith("09") else re.sub(r"[^\d]","",p)

@contextmanager
def locked(path, mode):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield f
        fcntl.flock(f, fcntl.LOCK_UN)

# ───────────── 拆名、csrf、日誌 ───────────── #
DOUBLE_SURNAMES = {
    '歐陽','司馬','上官','夏侯','諸葛','聞人','東方','赫連','皇甫','尉遲','羊舌',
    '淳于','公孫','仲孫','單于','令狐','鐘離','宇文','長孫','慕容','鮮于','拓跋',
    '軒轅','百里','東郭','南宮','西門','北宮','呼延','梁丘','左丘','第五','太史'
}
def split_name(name: str):
    if not name: return "",""
    s = name.strip()
    if " " in s or "," in s:
        s = s.replace(",", " ")
        parts = [p for p in s.split() if p]
        return (parts[0], " ".join(parts[1:])) if len(parts)>1 else ("",parts[0])
    if len(s)>=3 and s[:2] in DOUBLE_SURNAMES:
        return s[:2], s[2:]
    if len(s)>=2:
        return s[0], s[1:]
    return s,""

def csrf():
    if "csrf" not in session:
        session["csrf"] = sha(str(time.time()))
    return session["csrf"]

def log_event(ts, eid, fake=False):
    with EVENT_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{ts},{eid},{'auto' if fake else 'manual'}\n")

def recent_real_event_within(hours=36):
    cutoff = time.time() - hours*3600
    if not EVENT_LOG.exists(): return False
    with EVENT_LOG.open() as f:
        for line in reversed(list(f)):
            ts = int(line.split(",")[0])
            if ts < cutoff: break
            if "manual" in line: return True
    return False

# ───────────── 月度去重 & 抽取 ───────────── #
def get_auto_used():
    if os.path.exists(AUTO_USED_PATH):
        with open(AUTO_USED_PATH,"rb") as f:
            used = pickle.load(f)
    else:
        used = {}
    cur = datetime.now().strftime("%Y%m")
    if used.get("yyyymm") != cur:
        used = {"yyyymm":cur, "used":set()}
    return used

def set_auto_used(k):
    used = get_auto_used()
    used["used"].add(k)
    with open(AUTO_USED_PATH,"wb") as f:
        pickle.dump(used, f)

def pick_user():
    used = get_auto_used()
    with locked(USER_PROFILE_MAP_PATH,"a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
    pool = [(k,u) for k,u in mp.items()
            if k not in used["used"] and (u.get("em") or u.get("ph"))]
    if not pool: return None,None
    k,u = random.choice(pool)
    set_auto_used(k)
    return k,u

# ───────────── CAPI 組資料與送出 ───────────── #
def build_user_data(u, ext_id):
    # 拆生日
    dob = u.get("birthday","")
    y=m=d=""
    if dob:
        parts=dob.split("-")
        if len(parts)==3:
            y,m,d = parts
    ud = {
        "em":sha(u.get("em","")),
        "ph":sha(u.get("ph","")),
        "fn":sha(u.get("fn","")),
        "ln":sha(u.get("ln","")),
        "ge":sha(u.get("ge","")),
        "country":sha(u.get("country","")),
        "db": sha(y+m+d),
        "doby": sha(y),
        "dobm": sha(m),
        "dobd": sha(d),
        "external_id": sha(ext_id),
        "client_ip_address": u.get("client_ip_address",""),
        "client_user_agent": u.get("client_user_agent",""),
        "fbc": u.get("fbc",""),
        "fbp": u.get("fbp","")
    }
    return ud

def send_capi(events, tag):
    payload = {"data": events, "upload_tag": tag}
    r = requests.post(API_URL, json=payload, params={"access_token":TOKEN}, timeout=10)
    logging.info("[CAPI] %s → %s", r.status_code, r.text)
    r.raise_for_status()

def send_auto_event():
    k,u = pick_user()
    if not u:
        logging.info("[Auto] 無可補件對象")
        return
    ts = int(time.time())
    price = random.choice(PRICES)
    pv_id       = f"evt_{ts}_{random.randrange(10**8):08d}"
    purchase_id = f"evt_{ts}_{random.randrange(10**8):08d}"

    # 更新 map
    with locked(USER_PROFILE_MAP_PATH,"a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
        mp[k]["event_id"] = purchase_id
        mp[k]["value"]    = price
        f.seek(0); pickle.dump(mp,f); f.truncate()

    ext = u.get("em") or u.get("ph") or k
    ud = build_user_data(u, ext)

    pv = {
        "event_name":"PageView",
        "event_time":ts-random.randint(60,300),
        "event_id":pv_id,
        "action_source":"website",
        "user_data":ud
    }
    purchase = {
        "event_name":"Purchase",
        "event_time":ts,
        "event_id":purchase_id,
        "action_source":"website",
        "user_data":ud,
        "custom_data":{"currency":CURRENCY,"value":price}
    }
    try:
        send_capi([pv,purchase], tag=f"auto_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}")
    except Exception as e:
        logging.error("[Auto CAPI] 失敗：%s", e)

    log_event(ts, purchase_id, fake=True)

threading.Thread(target=lambda: (
    time.sleep(5),
    send_auto_event()
), daemon=True).start()

# ───────────── HTML 模板 ───────────── #
HTML = '''<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8">
<title>服務滿意度調查</title>
<style>
body{background:#f2f6fb;font-family:"微軟正黑體",Arial,sans-serif}
.form-container{background:#fff;max-width:420px;margin:60px auto;padding:36px;
  border-radius:16px;box-shadow:0 4px 16px rgba(0,0,0,.1);text-align:center}
input,select,textarea,button{width:90%;padding:6px 10px;margin:6px 0 12px;
  border:1px solid #ccc;border-radius:4px;font-size:16px;background:#fafbfc}
button{background:#568cf5;color:#fff;border:none;font-weight:bold;padding:10px 0}
button:hover{background:#376ad8}
.inline-group{display:flex;gap:6px;justify-content:center;align-items:center}
.inline-group select{width:auto}
</style>
<script>
!function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;
n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}
(window,document,'script','https://connect.facebook.net/en_US/fbevents.js');
fbq('init','{{PIXEL_ID}}');fbq('track','PageView');
function gC(n){return(document.cookie.match('(^|;) ?'+n+'=([^;]*)(;|$)')||[])[2]||''}
function sC(n,v){document.cookie=n+'='+v+';path=/;SameSite=Lax'}
(function(){
  if(!gC('_fbp')) sC('_fbp','fb.1.'+Date.now()/1000+'.'+Math.random().toString().slice(2));
  const id=new URLSearchParams(location.search).get('fbclid');
  if(id&&!gC('_fbc')) sC('_fbc','fb.1.'+Date.now()/1000+'.'+id);
})();
const PRICES={{PRICES}};
function gid(){return'evt_'+Date.now()+'_'+Math.random().toString(36).slice(2)}
window.addEventListener('DOMContentLoaded',()=>{
  const now=new Date(),y=now.getFullYear();
  for(let i=y-90;i<=y;i++){const o=new Option(i,i);if(i===y-25)o.selected=true;byear.appendChild(o)}
  for(let i=1;i<=12;i++) bmonth.appendChild(new Option(i,i.toString().padStart(2,'0')));
  for(let i=1;i<=31;i++) bday.appendChild(new Option(i,i.toString().padStart(2,'0')));
  const update=()=>birthday.value=byear.value+'-'+bmonth.value+'-'+bday.value;
  [byear,bmonth,bday].forEach(s=>s.addEventListener('change',update));update();
});
function send(e){
  e.preventDefault();
  if(!/^09\\d{8}$/.test(document.querySelector('[name=phone]').value))
    return alert('手機格式需 09xxxxxxxx');
  const price=PRICES[Math.floor(Math.random()*PRICES.length)];
  const id=gid();
  eid.value=id;priceInput.value=price;fbc.value=gC('_fbc');fbp.value=gC('_fbp');
  fbq('track','Purchase',{value:price,currency:"{{CURRENCY}}"},
      {eventID:id,eventCallback:()=>e.target.submit()});
  setTimeout(()=>e.target.submit(),800);
}
</script></head>
<body>
<div class="form-container"><h2>服務滿意度調查</h2>
<form onsubmit="send(event)" method="post" action="/submit">
  <input type="hidden" name="csrf_token" value="{{ csrf() }}">
  姓名：<input name="name" required><br>
  出生年月日：
  <div class="inline-group">
    <select id="byear"></select> 年
    <select id="bmonth"></select> 月
    <select id="bday"></select> 日
  </div>
  <input type="hidden" name="birthday" id="birthday"><br>
  性別：
  <select name="gender">
    <option value="female">女性</option>
    <option value="male">男性</option>
  </select><br>
  Email：<input name="email" type="email" required><br>
  手機：<input name="phone" pattern="09\\d{8}" required><br>
  您覺得我們小編服務態度如何：<textarea name="satisfaction"></textarea><br>
  建議：<textarea name="suggestion"></textarea><br>
  <input type="hidden" name="event_id" id="eid">
  <input type="hidden" name="price"    id="priceInput">
  <input type="hidden" name="fbc"      id="fbc">
  <input type="hidden" name="fbp"      id="fbp">
  <button>送出</button>
</form></div></body></html>'''

# ───────────── Flask 路由 & 表單處理 ───────────── #
@app.before_request
def https_redirect():
    if request.headers.get("X-Forwarded-Proto","http")!="https":
        return redirect(request.url.replace("http://","https://"),301)

@app.route('/')
def index():
    return render_template_string(
        HTML,
        PIXEL_ID=PIXEL_ID,
        PRICES=json.dumps(PRICES),
        CURRENCY=CURRENCY,
        csrf=csrf
    )

@app.route('/submit', methods=['POST'])
def submit():
    # CSRF 驗證
    if request.form.get("csrf_token") != session.get("csrf"):
        return "CSRF!", 400

    # 讀表單
    d = {k: request.form.get(k,"").strip() for k in
         ("name","birthday","gender","email","phone","satisfaction","suggestion")}
    d["phone"] = norm_phone(d["phone"])
    price     = int(request.form["price"])
    eid       = request.form["event_id"]
    fbc,fbp   = request.form.get("fbc",""), request.form.get("fbp","")
    ts        = int(time.time())

    # 拆名
    fn, ln = split_name(d["name"])
    # gender
    ge = "f" if d["gender"].lower() in ("female","f","女") else "m"
    country = "tw"
    real_ip = request.remote_addr or ""
    ua      = request.headers.get("User-Agent","")

    # 更新 user_profile_map.pkl
    with locked(USER_PROFILE_MAP_PATH,"a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
        f.seek(0)
        keys = []
        if d["email"]: keys.append(d["email"].lower())
        if d["phone"]: keys.append(d["phone"])
        if d["name"] and d["birthday"]: keys.append(f"{d['name']}|{d['birthday']}")
        for k in keys:
            u = mp.get(k,{})
            u.update({
                "name":d["name"], "fn":fn, "ln":ln,
                "birthday":d["birthday"], "db":d["birthday"].replace("-",""),
                "ge":ge, "country":country,
                "em":d["email"].lower(), "ph":d["phone"],
                "event_id":eid, "value":price,
                "client_ip_address":real_ip,
                "client_user_agent":ua,
                "fbc":fbc, "fbp":fbp,
                "satisfaction":d["satisfaction"],
                "suggestion":d["suggestion"]
            })
            mp[k]=u
        pickle.dump(mp,f)
        f.truncate()
        logging.info("user_profile_map updated: %s", keys)

    # Excel 備份
    xls = BACKUP / f"{d['name']}_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}.xlsx"
    wb = Workbook(); ws = wb.active
    ws.append(list(d.keys())+["price","time"])
    ws.append(list(d.values())+[price, datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")])
    wb.save(xls)

    # 組 CAPI user_data
    proto = {
        "fn":fn,"ln":ln,"em":d["email"].lower(),"ph":d["phone"],
        "ge":ge,"country":country,"birthday":d["birthday"],"db":d["birthday"].replace("-",""),
        "client_ip_address":real_ip,"client_user_agent":ua,
        "fbc":fbc,"fbp":fbp
    }
    ud = build_user_data(proto, d["email"] or d["phone"] or d["name"])

    # 送 PageView + Purchase
    pv = {
        "event_name":"PageView",
        "event_time":ts- random.randint(60,300),
        "event_id": f"{eid}_pv",
        "action_source":"website",
        "user_data":ud
    }
    purchase = {
        "event_name":"Purchase",
        "event_time":ts,
        "event_id":eid,
        "action_source":"website",
        "user_data":ud,
        "custom_data":{
            "currency":CURRENCY,
            "value":price,
            # 將滿意度、建議也放 custom_data
            "satisfaction": d["satisfaction"],
            "suggestion": d["suggestion"]
        }
    }
    try:
        send_capi([pv,purchase], tag=f"form_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}")
    except Exception as e:
        logging.error("[CAPI] 失敗：%s", e)

    log_event(ts,eid, fake=False)

    # Email 通知
    try:
        tos = [os.getenv("TO_EMAIL_1"), os.getenv("TO_EMAIL_2")]
        body = "\n".join([
            f"【填單時間】{datetime.utcfromtimestamp(ts):%Y-%m-%d %H:%M:%S}",
            f"【姓名】{d['name']}", f"【Email】{d['email']}",
            f"【手機】{d['phone']}",
            f"【生日】{d['birthday']}",
            f"【性別】{'女性' if ge=='f' else '男性'}",
            f"【交易金額】NT${price:,}",
            f"【Event ID】{eid}",
            f"【滿意度】{d['satisfaction']}",
            f"【建議】{d['suggestion']}"
        ])
        msg = EmailMessage()
        msg["Subject"] = "新客戶表單回報"
        msg["From"]    = os.getenv("FROM_EMAIL")
        msg["To"]      = ",".join(tos)
        msg.set_content(body, charset="utf-8")
        with open(xls,"rb") as fp:
            msg.add_attachment(
                fp.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=xls.name
            )
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(os.getenv("FROM_EMAIL"), os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
    except Exception:
        logging.exception("Email 通知失敗")

    return make_response("感謝您的填寫！",200)

if __name__=="__main__":
    port = int(os.getenv("PORT",8000))
    logging.info("Listening on %s", port)
    app.run("0.0.0.0", port)