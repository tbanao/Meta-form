#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — 2025-06-28
--------------------------------------------------
• 補件候選：只要 em 或 ph 有值
• 每月同一筆只補件一次（下月重置）
• 新成交自動寫進 user_profile_map
• 補件時全部欄位都上傳（沒填就空字串）
• 滿意度調查表 Flask 表單一併寫入 user_profile_map
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
BACKUP  = Path("form_backups"); BACKUP.mkdir(exist_ok=True)
EVENT_LOG = Path("event_submit_log.txt")
AUTO_USED_PATH = "auto_used.pkl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")
app.wsgi_app   = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)
HSTS = "max-age=63072000; includeSubDomains; preload"

sha  = lambda s: hashlib.sha256(s.encode()).hexdigest() if s else ""
norm_phone = lambda p: ("886"+re.sub(r"[^\d]","",p).lstrip("0")) if p.startswith("09") else re.sub(r"[^\d]","",p)

# ─────────────── 工具函式 ─────────────── #
@contextmanager
def locked(path, mode):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield f
        fcntl.flock(f, fcntl.LOCK_UN)

def csrf():
    if "csrf" not in session:
        session["csrf"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]

DOUBLE_SURNAMES = {
    '歐陽','司馬','上官','夏侯','諸葛','聞人','東方','赫連','皇甫','尉遲','羊舌',
    '淳于','公孫','仲孫','單于','令狐','鐘離','宇文','長孫','慕容','鮮于','拓跋',
    '軒轅','百里','東郭','南宮','西門','北宮','呼延','梁丘','左丘','第五','太史'
}
def split_name(name: str):
    if not name:
        return "", ""
    s = name.strip()
    if " " in s or "," in s:
        s = s.replace(",", " ")
        parts = [p for p in s.split() if p]
        return (parts[0], " ".join(parts[1:])) if len(parts) > 1 else ("", parts[0])
    if len(s) >= 3 and s[:2] in DOUBLE_SURNAMES:
        return s[:2], s[2:]
    if len(s) >= 2:
        return s[0], s[1:]
    return s, ""

def user_display_name(u: dict) -> str:
    n = (u.get("name") or "").strip()
    if not n:
        n = ((u.get("fn", "") + u.get("ln", "")).strip())
    return n or u.get("em") or u.get("ph") or "(未知)"

def log_event(ts: int, eid: str, fake: bool):
    with EVENT_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{ts},{eid},real,{'auto' if fake else 'manual'}\n")

def recent_real_event_within(hours=36) -> bool:
    cutoff = time.time() - hours * 3600
    if not EVENT_LOG.exists():
        return False
    with EVENT_LOG.open("r", encoding="utf-8") as f:
        for line in reversed(list(f)):
            try:
                ts, _, flag, _ = line.strip().split(",", 3)
                if int(ts) < cutoff:
                    break
                if flag == "real":
                    return True
            except:
                continue
    return False

# ──────────── 補件候選 & 抽取 ──────────── #
def get_auto_used():
    if os.path.exists(AUTO_USED_PATH):
        with open(AUTO_USED_PATH, "rb") as f:
            used = pickle.load(f)
    else:
        used = {}
    now = datetime.now()
    cur_yyyymm = now.strftime("%Y%m")
    if used.get("yyyymm") != cur_yyyymm:
        used = {"yyyymm": cur_yyyymm, "used": set()}
    return used

def set_auto_used(key):
    used = get_auto_used()
    used["used"].add(key)
    with open(AUTO_USED_PATH, "wb") as f:
        pickle.dump(used, f)

def get_random_user_profile():
    used = get_auto_used()
    with locked(USER_PROFILE_MAP_PATH, "a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
    pool = [(k, u) for k, u in mp.items()
            if k not in used["used"] and (u.get("em") or u.get("ph"))]
    return random.choice(pool) if pool else (None, None)

def random_event_id():
    return f"evt_{int(time.time())}_{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=16))}"

def send_auto_event(reason="隨機補件"):
    k, u = get_random_user_profile()
    if not u:
        logging.warning("[補件] 沒有可抽的補件對象（本月都抽過了）")
        return

    eid   = random_event_id()
    price = random.choice(PRICES)
    ts    = int(time.time())
    fn, ln = split_name(u.get("name", "") or (u.get("fn", "") + u.get("ln", "")))
    name_disp = user_display_name(u)
    set_auto_used(k)

    with locked(USER_PROFILE_MAP_PATH, "a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
        mp[k]["event_id"] = eid
        mp[k]["value"]    = price
        f.seek(0)
        pickle.dump(mp, f)
        f.truncate()

    ud = {}
    for field in [
        "em","ph","fn","ln","db","birthday","ge","country","name",
        "client_ip_address","client_user_agent","fbc","fbp"
    ]:
        v = u.get(field, "")
        ud[field] = sha(v) if field in ("em","ph","fn","ln","db","ge","country") else v
    ud["external_id"] = sha(u.get("em") or u.get("ph") or name_disp)

    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": ts,
            "event_id": eid,
            "action_source": "website",
            "user_data": ud,
            "custom_data": {"currency": CURRENCY, "value": price}
        }],
        "upload_tag": f"auto_{datetime.utcfromtimestamp(ts).strftime('%Y%m%d_%H%M%S')}"
    }
    try:
        r = requests.post(API_URL, json=payload,
                          params={"access_token": TOKEN}, timeout=10)
        logging.info("[自動補件] Meta CAPI %s → %s", r.status_code, r.text)
        r.raise_for_status()
    except Exception as e:
        logging.error("[自動補件] CAPI failed: %s", e)

    log_event(ts, eid, fake=True)

    try:
        tos = [os.getenv("TO_EMAIL_1"), os.getenv("TO_EMAIL_2")]
        tos = [t for t in tos if t]
        body = (
            f"【Meta 自動補件通報】\n\n"
            f"通報時間：{datetime.utcfromtimestamp(ts):%Y-%m-%d %H:%M:%S}\n"
            f"自動補件原因：{reason}\n\n"
            f"【補件客戶資訊】\n"
            f"姓名　：{name_disp}\nEmail ：{u.get('em','')}\n電話　：{u.get('ph','')}\n\n"
            f"Event ID：{eid}\n補件金額：NT${price:,}\n\n"
            f"【備註】此信為自動通知，請勿回覆。"
        )
        msg = EmailMessage()
        msg["Subject"] = "【自動補件通報】Meta CAPI"
        msg["From"] = os.getenv("FROM_EMAIL")
        msg["To"] = ",".join(tos)
        msg.set_content(body, charset="utf-8")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(os.getenv("FROM_EMAIL"), os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
    except Exception:
        logging.exception("❌ Auto-email 發送失敗")

def next_auto_interval():
    return random.randint(32 * 3600, 38 * 3600)

def auto_loop():
    while True:
        try:
            if not recent_real_event_within(36):
                logging.info("[自動補件] 36h 內無事件 → 補件")
                send_auto_event()
                time.sleep(next_auto_interval())
            else:
                logging.info("[自動補件] 36h 內已有事件 → 1h 後再檢查")
                time.sleep(3600)
        except Exception as e:
            logging.exception("自動補件主迴圈錯誤：%s", e)
            time.sleep(3600)

threading.Thread(target=auto_loop, daemon=True).start()

# ─────────────── HTML（原封不動） ─────────────── #
HTML = r'''<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"><title>服務滿意度調查</title>
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
</script></head><body>
<div class="form-container">
<h2>服務滿意度調查</h2>
<form onsubmit="send(event)" method="post" action="/submit">
<input type="hidden" name="csrf_token" value="{{ csrf }}">
姓名：<input name="name" required><br>
出生年月日：<div class="inline-group">
<select id="byear"></select> 年 <select id="bmonth"></select> 月 <select id="bday"></select> 日
</div><input type="hidden" name="birthday" id="birthday"><br>
性別：<select name="gender"><option value="female">女性</option><option value="male">男性</option></select><br>
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

# ─────────────── Flask 路由 ─────────────── #
@app.before_request
def https_redirect():
    if request.headers.get("X-Forwarded-Proto", "http") != "https":
        return redirect(request.url.replace("http://", "https://"), 301)

@app.after_request
def add_hsts(r):
    r.headers["Strict-Transport-Security"] = HSTS
    return r

@app.route('/healthz')
@app.route('/health')
def health():
    return "OK", 200

@app.route('/')
def index():
    return render_template_string(
        HTML,
        csrf=csrf(),
        PIXEL_ID=PIXEL_ID,
        PRICES=json.dumps(PRICES),
        CURRENCY=CURRENCY
    )

# ─────────────── 表單提交 ─────────────── #
@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token") != session.get("csrf"):
        return "CSRF!", 400

    d = {k: request.form.get(k, "").strip() for k in
         ("name", "birthday", "gender", "email", "phone", "satisfaction", "suggestion")}
    d["phone"] = norm_phone(d["phone"])
    price   = int(request.form["price"])
    new_eid = request.form["event_id"]
    fbc, fbp = request.form.get("fbc", ""), request.form.get("fbp", "")
    ts = int(time.time())

    fn, ln  = split_name(d["name"])
    birthday = d["birthday"].replace("/", "-") if d["birthday"] else ""
    gender   = "f" if d["gender"].lower() in ("female", "f", "女") else \
               "m" if d["gender"].lower() in ("male", "m", "男") else ""
    country  = "tw"
    real_ip  = request.remote_addr or ""
    ua       = request.headers.get("User-Agent", "")

    # 更新 map
    with locked(USER_PROFILE_MAP_PATH, "a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
        keys = []
        if d["email"]: keys.append(d["email"].lower())
        if d["phone"]: keys.append(d["phone"])
        if d["name"] and birthday: keys.append(f"{d['name']}|{birthday}")
        eid = next((mp.get(k, {}).get("event_id") for k in keys if k in mp), None) or new_eid
        for k in keys:
            u = mp.get(k, {})
            u.update({
                "fn": fn, "ln": ln, "db": birthday,
                "ge": gender, "country": country,
                "em": d["email"].lower(), "ph": d["phone"],
                "name": d["name"], "birthday": birthday,
                "event_id": eid, "value": price,
                "client_ip_address": real_ip, "client_user_agent": ua,
                "fbc": fbc, "fbp": fbp,
                "satisfaction": d.get("satisfaction", ""),
                "suggestion": d.get("suggestion", "")
            })
            mp[k] = u
        f.seek(0)
        pickle.dump(mp, f)
        f.truncate()
        logging.info("user_profile_map.pkl updated (%s)", ", ".join(keys))

    # Excel 備份
    xls = BACKUP / f"{d['name']}_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}.xlsx"
    wb  = Workbook()
    ws  = wb.active
    ws.append(list(d.keys()) + ["price", "time"])
    ws.append(list(d.values()) + [price, datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')])
    wb.save(xls)

    # 上傳 Meta CAPI
    ud = {}
    for field in [
        "em","ph","fn","ln","db","birthday","ge","country","name",
        "client_ip_address","client_user_agent","fbc","fbp"
    ]:
        v = locals().get(field) or d.get(field) or ""
        ud[field] = sha(v) if field in ("em","ph","fn","ln","db","ge","country") else v
    ud["external_id"] = sha(d["email"] or d["phone"] or d["name"])

    payload = {
        "data":[{
            "event_name":"Purchase","event_time":ts,"event_id":eid,
            "action_source":"website","user_data":ud,
            "custom_data":{"currency":CURRENCY,"value":price}
        }],
        "upload_tag":f"form_{datetime.utcnow():%Y%m%d_%H%M%S}"
    }
    try:
        r = requests.post(API_URL, json=payload, params={"access_token": TOKEN}, timeout=10)
        logging.info("Meta CAPI %s → %s", r.status_code, r.text)
        r.raise_for_status()
    except Exception as e:
        logging.error("CAPI upload failed → queued retry: %s", e)

    log_event(ts, eid, fake=False)

    # Email 通知
    try:
        tos = [os.getenv("TO_EMAIL_1"), os.getenv("TO_EMAIL_2")]
        if not all(tos):
            raise ValueError("TO_EMAIL_1 與 TO_EMAIL_2 必須都設定！")
        body = "\n".join([
            f"【填單時間】{datetime.utcfromtimestamp(ts):%Y-%m-%d %H:%M:%S}",
            f"【姓名】{d['name']}", f"【Email】{d['email']}", f"【電話】{d['phone']}",
            f"【生日】{d['birthday'] or '-'}",
            f"【性別】{'男性' if gender=='m' else '女性' if gender=='f' else d['gender'] or '-'}",
            f"【交易金額】NT${price:,}", f"【Event ID】{eid}",
            f"【服務態度滿意度】{d['satisfaction'] or '-'}",
            f"【建議內容】\n{d['suggestion'] or '-'}"
        ])
        msg = EmailMessage()
        msg["Subject"] = "新客戶表單回報"
        msg["From"]    = os.getenv("FROM_EMAIL")
        msg["To"]      = ",".join(tos)
        msg.set_content(body, charset="utf-8")
        with open(xls, "rb") as fp:
            msg.add_attachment(fp.read(), maintype="application",
                               subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               filename=xls.name)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(os.getenv("FROM_EMAIL"), os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
        logging.info("✉️ 表單 Email sent → %s", msg["To"])
    except Exception:
        logging.exception("❌ 表單 Email 發送失敗")

    return make_response("感謝您的填寫！", 200)

# ─────────────── 主程式入口 ─────────────── #
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    logging.info("Listening on %s", port)
    app.run("0.0.0.0", port)
