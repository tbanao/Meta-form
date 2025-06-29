#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — 2025-06-28 thread每20分鐘檢查+真人送件時log補件剩餘時間
"""

import os, re, time, json, hashlib, logging, smtplib, sys, fcntl, pickle, threading, random, shutil
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

import requests
from flask import Flask, request, render_template_string, redirect, session, make_response
from openpyxl import Workbook
from werkzeug.middleware.proxy_fix import ProxyFix

# ====== 設定區 ======
REQUIRED = [
    "PIXEL_ID", "ACCESS_TOKEN",
    "FROM_EMAIL", "EMAIL_PASSWORD",
    "TO_EMAIL_1", "TO_EMAIL_2",
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
PRICES   = [19800, 28800, 34800, 39800, 45800]

USER_PROFILE_MAP_PATH = "user_profile_map.pkl"
EVENT_LOG             = Path("event_submit_log.txt")
BACKUP                = Path("form_backups"); BACKUP.mkdir(exist_ok=True)
AUTO_USED_PATH        = "auto_used.pkl"
FAILED_LOG            = Path("capi_failed.log")
IPINFO_TOKEN          = "12f0afcbb25f7c"
LAST_EVENT_FILE       = "last_event_time.txt"

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

def load_user_map():
    if not os.path.exists(USER_PROFILE_MAP_PATH) or os.path.getsize(USER_PROFILE_MAP_PATH)==0:
        return {}
    with locked(USER_PROFILE_MAP_PATH, "a+b") as f:
        f.seek(0)
        try:
            return pickle.load(f)
        except Exception:
            return {}

def save_user_map(mp):
    with locked(USER_PROFILE_MAP_PATH, "a+b") as f:
        f.seek(0)
        pickle.dump(mp, f)
        f.truncate()

def backup_map():
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    if os.path.exists(USER_PROFILE_MAP_PATH):
        shutil.copy2(USER_PROFILE_MAP_PATH, BACKUP / f"user_profile_map_{now}.pkl")

DOUBLE_SURNAMES = {'歐陽','司馬','上官','夏侯','諸葛','聞人','東方','赫連','皇甫','尉遲','羊舌','淳于','公孫','仲孫','單于','令狐','鐘離','宇文','長孫','慕容','鮮于','拓跋','軒轅','百里','東郭','南宮','西門','北宮','呼延','梁丘','左丘','第五','太史'}
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
            if ",manual" in line: return True
    return False

def ip_lookup(ip):
    try:
        if not ip or ip.startswith("127.") or ip.startswith("192.168.") or ip.startswith("10."):
            return {}, ""
        url = f"https://ipinfo.io/{ip}?token={IPINFO_TOKEN}"
        resp = requests.get(url, timeout=3)
        if resp.status_code != 200:
            return {}, ""
        data = resp.json()
        ct = data.get("city", "") or data.get("region", "")
        zipc = data.get("postal", "")
        return {"ct": ct, "zip": zipc}, f"[查城市]{ip}→{ct}/{zipc}"
    except Exception as e:
        return {}, f"[IPinfo失敗]{ip}:{e}"

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
    mp = load_user_map()
    pool = [(k,u) for k,u in mp.items() if k not in used["used"] and (u.get("em") or u.get("ph"))]
    if not pool: return None,None
    k,u = random.choice(pool)
    set_auto_used(k)
    return k,u

def build_user_data(u, ext_id, ct_zip):
    dob = u.get("birthday","")
    y=m=d=""
    if dob:
        parts=dob.split("-")
        if len(parts)==3: y,m,d=parts
    ud = {
        "em":        sha(u.get("em","")),
        "ph":        sha(u.get("ph","")),
        "fn":        sha(u.get("fn","")),
        "ln":        sha(u.get("ln","")),
        "ge":        sha(u.get("ge","")),
        "country":   sha(u.get("country","")),
        "db":        sha(y+m+d),
        "doby":      sha(y),
        "dobm":      sha(m),
        "dobd":      sha(d),
        "external_id":sha(ext_id),
        "client_ip_address":u.get("client_ip_address",""),
        "client_user_agent":u.get("client_user_agent",""),
        "fbc":       u.get("fbc",""),
        "fbp":       u.get("fbp","")
    }
    if ct_zip.get("ct"):  ud["ct"] = sha(ct_zip["ct"])
    if ct_zip.get("zip"): ud["zp"] = sha(ct_zip["zip"])
    return ud

def send_capi(events, tag, retry=0):
    payload = {"data": events, "upload_tag": tag}
    try:
        r = requests.post(API_URL, json=payload, params={"access_token":TOKEN}, timeout=10)
        logging.info("[CAPI] %s → %s", r.status_code, r.text)
        if r.status_code != 200:
            with FAILED_LOG.open("a", encoding="utf-8") as f:
                f.write(f"[{datetime.now()}] {r.status_code} {r.text} ({tag})\n")
            if retry < 3:
                time.sleep(3)
                return send_capi(events, tag, retry+1)
            else:
                notify_email("[Meta CAPI自動補件失敗]", f"補件傳送3次都失敗\n\npayload:\n{json.dumps(events,ensure_ascii=False)}\n\nMeta回應:\n{r.text}")
        return r
    except Exception as e:
        with FAILED_LOG.open("a", encoding="utf-8") as f:
            f.write(f"[{datetime.now()}] [ERROR] {str(e)} ({tag})\n")
        if retry < 3:
            time.sleep(3)
            return send_capi(events, tag, retry+1)
        else:
            notify_email("[Meta CAPI自動補件失敗]", f"補件API連線失敗3次\n\n{str(e)}\n\npayload:\n{json.dumps(events,ensure_ascii=False)}")
        raise

def notify_email(subject, body, xls=None):
    try:
        tos=[os.getenv("TO_EMAIL_1"),os.getenv("TO_EMAIL_2")]
        msg=EmailMessage()
        msg["Subject"]=subject
        msg["From"]=os.getenv("FROM_EMAIL")
        msg["To"]=",".join(tos)
        msg.set_content(body,charset="utf-8")
        if xls:
            with open(xls,"rb") as fp:
                msg.add_attachment(fp.read(),
                    maintype="application",
                    subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    filename=Path(xls).name)
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(os.getenv("FROM_EMAIL"),os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
    except Exception as e:
        logging.exception("Email 通知失敗")

def update_last_event_time():
    with open(LAST_EVENT_FILE, "w") as f:
        f.write(str(int(time.time())))

def get_last_event_time():
    try:
        with open(LAST_EVENT_FILE, "r") as f:
            return int(f.read().strip())
    except:
        return 0

def send_auto_event():
    key,u = pick_user()
    if not u:
        logging.info("[Auto] 無可補件對象")
        return
    ts    = int(time.time())
    price = random.choice(PRICES)
    pv_id       = f"evt_{ts}_{random.randrange(10**8):08d}"
    purchase_id = f"evt_{ts}_{random.randrange(10**8):08d}"
    real_ip = u.get("client_ip_address","")
    real_ua = u.get("client_user_agent","")
    fbc = f"fb.1.{ts}.{random.randint(10**9,10**10-1)}"
    fbp = f"fb.1.{ts}.{random.randint(10**9,10**10-1)}"
    ct_zip, iplog = ip_lookup(real_ip)

    # 更新 map 並自動備份
    mp = load_user_map()
    mp[key]["event_id"] = purchase_id
    mp[key]["value"]    = price
    save_user_map(mp)
    backup_map()

    ud = build_user_data(u, u.get("em") or u.get("ph") or key, ct_zip)
    ud.update({"client_ip_address":real_ip,"client_user_agent":real_ua,"fbc":fbc,"fbp":fbp})
    pv = {
        "event_name":"PageView",
        "event_time":ts-random.randint(60,300),
        "event_id":pv_id,"action_source":"website","user_data":ud
    }
    purchase = {
        "event_name":"Purchase",
        "event_time":ts,
        "event_id":purchase_id,"action_source":"website","user_data":ud,
        "custom_data":{"currency":CURRENCY,"value":price}
    }
    try:
        send_capi([pv,purchase], tag=f"auto_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}")
        log_event(ts, purchase_id, fake=True)
        msg = f"自動補件客戶\n補件時間：{datetime.now():%Y-%m-%d %H:%M:%S}\n補件對象key: {key}\nEmail: {u.get('em','')}\n手機: {u.get('ph','')}\n姓名: {u.get('name','')}\n{iplog}"
        notify_email("[Meta自動補件通知]", msg)
        update_last_event_time()
        logging.info("[Auto] 成功補件 %s", purchase_id)
    except Exception as e:
        logging.error("[Auto] 補件失敗：%s", e)

# ===== 改良自動補件 Thread，每20分鐘檢查一次 =====
target_wait = None
def auto_wake():
    global target_wait
    random.seed()
    interval_sec = 20*60  # 20分鐘
    wait_min = 32*3600
    wait_max = 38*3600
    target_wait = random.randint(wait_min, wait_max)
    logging.info(f"[Auto] 本輪目標補件間隔：{target_wait//3600}小時")
    while True:
        last_event = get_last_event_time()
        now = int(time.time())
        since_last = now - last_event
        remain = target_wait - since_last
        if remain <= 0:
            send_auto_event()
            target_wait = random.randint(wait_min, wait_max)
            logging.info(f"[Auto] 新一輪目標補件間隔：{target_wait//3600}小時")
        else:
            logging.info(f"[Auto] 距離下次自動補件預計剩 {remain//3600} 小時 {remain%3600//60} 分")
        time.sleep(interval_sec)

threading.Thread(target=auto_wake, daemon=True).start()

HTML = r'''<!DOCTYPE html>
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
window.addEventListener('DOMContentLoaded', () => {
  const byear  = document.getElementById('byear');
  const bmonth = document.getElementById('bmonth');
  const bday   = document.getElementById('bday');
  const birthday = document.getElementById('birthday');
  const now = new Date(), cy = now.getFullYear();
  for(let y=cy-90; y<=cy; y++){
    const o = new Option(y,y);
    if(y===cy-25) o.selected = true;
    byear.appendChild(o);
  }
  for(let m=1; m<=12; m++){
    const mm = String(m).padStart(2,'0');
    bmonth.appendChild(new Option(mm,mm));
  }
  for(let d=1; d<=31; d++){
    const dd = String(d).padStart(2,'0');
    bday.appendChild(new Option(dd,dd));
  }
  const upd = ()=> birthday.value = `${byear.value}-${bmonth.value}-${bday.value}`;
  byear.addEventListener('change',upd);
  bmonth.addEventListener('change',upd);
  bday.addEventListener('change',upd);
  upd();
});
const PRICES = {{PRICES}};
function gid(){return'evt_'+Date.now()+'_'+Math.random().toString(36).slice(2);}
function send(e){
  e.preventDefault();
  const phone = document.querySelector('[name=phone]').value;
  if(!/^09\d{8}$/.test(phone)){
    alert('手機格式需 09xxxxxxxx'); return;
  }
  const price = PRICES[Math.floor(Math.random()*PRICES.length)];
  const id    = gid();
  document.getElementById('eid').value    = id;
  document.getElementById('priceInput').value = price;
  document.getElementById('fbc').value    = gC('_fbc');
  document.getElementById('fbp').value    = gC('_fbp');
  fbq('track','Purchase',{value:price,currency:"{{CURRENCY}}"},
      {eventID:id,eventCallback:()=>e.target.submit()});
  setTimeout(()=>e.target.submit(),800);
}
</script></head>
<body>
<div class="form-container">
  <h2>服務滿意度調查</h2>
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
    手機：<input name="phone" pattern="09\d{8}" required><br>
    您覺得我們小編服務態度如何：<textarea name="satisfaction"></textarea><br>
    建議：<textarea name="suggestion"></textarea><br>
    <input type="hidden" name="event_id" id="eid">
    <input type="hidden" name="price"    id="priceInput">
    <input type="hidden" name="fbc"      id="fbc">
    <input type="hidden" name="fbp"      id="fbp">
    <button>送出</button>
  </form>
</div>
</body>
</html>'''

@app.route('/healthz')
@app.route('/health')
def health():
    return "OK", 200

@app.before_request
def https_redirect():
    if request.headers.get("X-Forwarded-Proto","http") != "https":
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
    global target_wait
    if request.form.get("csrf_token") != session.get("csrf"):
        return "CSRF!", 400

    d = {k: request.form.get(k,"").strip() for k in
         ("name","birthday","gender","email","phone","satisfaction","suggestion")}
    d["phone"] = norm_phone(d["phone"])
    price     = int(request.form["price"])
    eid       = request.form["event_id"]
    fbc, fbp  = request.form.get("fbc",""), request.form.get("fbp","")
    ts        = int(time.time())
    fn, ln    = split_name(d["name"])
    ge        = "f" if d["gender"].lower() in ("female","f","女") else "m"
    country   = "tw"
    real_ip   = request.remote_addr or ""
    ua        = request.headers.get("User-Agent","")
    ct_zip, iplog = ip_lookup(real_ip)

    mp = load_user_map()
    for k in filter(None, [
        d["email"].lower() if d["email"] else None,
        d["phone"],
        f"{d['name']}|{d['birthday']}" if d["birthday"] else None
    ]):
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
        mp[k] = u
    save_user_map(mp)
    backup_map()
    update_last_event_time()

    # ===== 新增真人送件時log下次自動補件剩餘時間 =====
    try:
        last_event = get_last_event_time()
        now = int(time.time())
        wait = target_wait if target_wait is not None else 34*3600
        since_last = now - last_event
        remain = wait - since_last
        remain = max(remain, 0)
        logging.info(f"[Manual] 本次真人送件，下次自動補件預計剩 {remain//3600} 小時 {remain%3600//60} 分")
    except Exception as e:
        logging.error(f"[Manual] 計算補件倒數出錯：{e}")

    xls = BACKUP / f"{d['name']}_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}.xlsx"
    wb  = Workbook(); ws = wb.active
    ws.append(list(d.keys()) + ["price","time"])
    ws.append(list(d.values()) + [price, datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")])
    wb.save(xls)

    proto = {
        "fn":fn, "ln":ln,
        "em":d["email"].lower(), "ph":d["phone"],
        "ge":ge, "country":country,
        "birthday":d["birthday"], "db":d["birthday"].replace("-",""),
        "client_ip_address":real_ip, "client_user_agent":ua,
        "fbc":fbc, "fbp":fbp
    }
    ud = build_user_data(proto, d["email"] or d["phone"] or d["name"], ct_zip)

    pv = {
        "event_name":"PageView",
        "event_time":ts - random.randint(60,300),
        "event_id":f"{eid}_pv",
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
            "satisfaction":d["satisfaction"],
            "suggestion":d["suggestion"]
        }
    }
    try:
        send_capi([pv,purchase], tag=f"form_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}")
    except Exception as e:
        logging.error("[CAPI] 失敗：%s", e)

    log_event(ts, eid, fake=False)

    try:
        tos = [os.getenv("TO_EMAIL_1"), os.getenv("TO_EMAIL_2")]
        body = "\n".join([
            f"【填單時間】{datetime.utcfromtimestamp(ts):%Y-%m-%d %H:%M:%S}",
            f"【姓名】{d['name']}",
            f"【Email】{d['email']}",
            f"【手機】{d['phone']}",
            f"【生日】{d['birthday']}",
            f"【性別】{'女性' if ge=='f' else '男性'}",
            f"【城市】{ct_zip.get('ct','')}",
            f"【郵遞區號】{ct_zip.get('zip','')}",
            f"【交易金額】NT${price:,}",
            f"【Event ID】{eid}",
            f"【滿意度】{d['satisfaction']}",
            f"【建議】{d['suggestion']}",
            f"{iplog}"
        ])
        notify_email("新客戶表單回報", body, xls)
    except Exception:
        logging.exception("Email 通知失敗")

    return make_response("感謝您的填寫！", 200)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    logging.info("Listening on %s", port)
    app.run("0.0.0.0", port)
