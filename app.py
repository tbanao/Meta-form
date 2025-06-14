#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — 強化覆蓋率＋詳細Email內容 2025-06-15
1. 客戶填單時自動回傳姓名/生日/性別/國別(台灣)到Meta
2. Email通知詳細（內文＋excel附檔）
3. Meta回傳狀態紀錄於 Render Log
"""

import os, re, json, time, random, hashlib, logging, smtplib, sys, fcntl
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

import requests
from flask import Flask, request, render_template_string, redirect, session, make_response
from openpyxl import Workbook
from werkzeug.middleware.proxy_fix import ProxyFix

# ── 必填 ENV ─────────────────────────────────────
REQUIRED = [
    "PIXEL_ID", "ACCESS_TOKEN",
    "FROM_EMAIL", "EMAIL_PASSWORD", "TO_EMAIL_1", "SECRET_KEY"
]
missing = [v for v in REQUIRED if not os.getenv(v)]
if missing:
    logging.critical("缺少環境變數：%s", ", ".join(missing))
    sys.exit(1)

PIXEL_ID, TOKEN = os.getenv("PIXEL_ID"), os.getenv("ACCESS_TOKEN")
API_URL  = f"https://graph.facebook.com/v19.0/{PIXEL_ID}/events"
CURRENCY = "TWD"
PRICES   = [19800, 28800, 34800, 39800, 45800]

# ── 基本設定 ─────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)
HSTS = "max-age=63072000; includeSubDomains; preload"

BACKUP = Path("form_backups"); BACKUP.mkdir(exist_ok=True)
RETRY  = Path("retry_queue.jsonl"); RETRY.touch(exist_ok=True)

sha = lambda s: hashlib.sha256(s.encode()).hexdigest() if s else ""
def norm_phone(p: str):
    p = re.sub(r"[^\d]", "", p)
    return ("886" + p.lstrip("0")) if p.startswith("09") else p

@contextmanager
def locked(p, m):
    with open(p, m) as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield f
        fcntl.flock(f, fcntl.LOCK_UN)

def csrf():
    if "csrf" not in session:
        session["csrf"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]

def split_name(name):
    name = name.strip()
    if len(name) >= 2:
        return name[0], name[1:]
    elif name:
        return name, ""
    else:
        return "", ""

# ── HTML (Pixel + cookie + raw block) ───────────
HTML = f'''<!DOCTYPE html>
<html lang="zh-TW">
<head>
  <meta charset="UTF-8">
  <title>服務滿意度調查</title>
  <style>
    body{{ background:#f2f6fb; font-family:"微軟正黑體",Arial,sans-serif }}
    .form-container{{ background:#fff; max-width:420px; margin:60px auto; padding:36px;
      border-radius:16px; box-shadow:0 4px 16px rgba(0,0,0,.1); text-align:center }}
    input, select, textarea, button{{ width:90%; padding:6px 10px; margin:6px 0 12px;
      border:1px solid #ccc; border-radius:4px; font-size:16px; background:#fafbfc }}
    button{{ background:#568cf5; color:#fff; border:none; font-weight:bold; padding:10px 0 }}
    button:hover{{ background:#376ad8 }}
  </style>

  {{% raw %}}
  <script>
  !function(f,b,e,v,n,t,s){{if(f.fbq)return;n=f.fbq=function(){{n.callMethod?
    n.callMethod.apply(n,arguments):n.queue.push(arguments)}};if(!f._fbq)f._fbq=n;
    n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
    t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}}
  (window,document,'script','https://connect.facebook.net/en_US/fbevents.js');
  fbq('init','{PIXEL_ID}'); fbq('track','PageView');

  function gC(n){{ return (document.cookie.match('(^|;) ?'+n+'=([^;]*)(;|$)')||[])[2]||'' }}
  function sC(n,v){{ document.cookie = n + '=' + v + ';path=/;SameSite=Lax' }}
  (function(){{ if(!gC('_fbp')) sC('_fbp','fb.1.'+Date.now()/1000+'.'+Math.random().toString().slice(2));
    const id = new URLSearchParams(location.search).get('fbclid');
    if(id && !gC('_fbc')) sC('_fbc','fb.1.'+Date.now()/1000+'.'+id);
  }})();

  const PRICES = {PRICES};
  function gid(){{ return 'evt_' + Date.now() + '_' + Math.random().toString(36).slice(2) }}
  function send(e){{
    e.preventDefault();
    if(!/^09\\d{{8}}$/.test(document.querySelector('[name=phone]').value))
      return alert('手機格式需 09xxxxxxxx'), false;
    const price = PRICES[Math.floor(Math.random()*PRICES.length)];
    const id = gid();
    ['eid','price','fbc','fbp'].forEach(k => document.getElementById(k).value =
      k==='eid'? id : k==='price'? price : k==='fbc'? gC('_fbc') : gC('_fbp')
    );
    fbq('track','Purchase',
      {{ value:price, currency:"{CURRENCY}" }},
      {{ eventID:id, eventCallback:()=>e.target.submit() }}
    );
    setTimeout(()=>e.target.submit(),800);
  }}
  </script>
  {{% endraw %}}
</head>
<body>
  <div class="form-container">
    <h2>服務滿意度調查</h2>
    <form onsubmit="send(event)" method="post" action="/submit">
      <input type="hidden" name="csrf_token" value="{{{{ csrf }}}}">
      姓名：<input name="name" required><br>
      出生年月日：<input type="date" name="birthday"><br>
      性別：
      <select name="gender">
        <option value="female">女性</option>
        <option value="male">男性</option>
      </select><br>
      Email：<input name="email" type="email" required><br>
      手機：<input name="phone" pattern="09\\d{{8}}" required><br>
      服務態度滿意度：<textarea name="satisfaction"></textarea><br>
      建議：<textarea name="suggestion"></textarea><br>
      <input type="hidden" id="eid"   name="event_id">
      <input type="hidden" id="price" name="price">
      <input type="hidden" id="fbc"   name="fbc">
      <input type="hidden" id="fbp"   name="fbp">
      <button>送出</button>
    </form>
  </div>
</body>
</html>'''

# ── HTTPS / HSTS ───────────────────────────────
@app.before_request
def https_redirect():
    if request.headers.get("X-Forwarded-Proto","http") != "https":
        return redirect(request.url.replace("http://","https://"), 301)

@app.after_request
def add_hsts(r):
    r.headers["Strict-Transport-Security"] = HSTS
    return r

# ── Health & Index ────────────────────────────
@app.route('/healthz')
@app.route('/health')
def health():
    return "OK", 200

@app.route('/')
def index():
    return render_template_string(HTML, csrf=csrf())

# ── Submit ────────────────────────────────────
@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token") != session.get("csrf"):
        return "CSRF!", 400

    # 1️⃣ 讀表單並正規化
    d = {k: request.form.get(k, "").strip() for k in
         ("name","birthday","gender","email","phone","satisfaction","suggestion")}
    d["phone"] = norm_phone(d["phone"])
    price = int(request.form["price"])
    eid   = request.form["event_id"]
    fbc   = request.form.get("fbc","")
    fbp   = request.form.get("fbp","")
    ts    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # 2️⃣ 姓名/生日/性別/國別補強
    fn, ln = split_name(d["name"])
    birthday = d["birthday"].replace("/", "-") if d["birthday"] else ""
    gender = "f" if d["gender"].lower() in ["female", "f", "女"] else "m" if d["gender"].lower() in ["male", "m", "男"] else ""
    country = "tw"

    # 3️⃣ Excel 備份
    xls = BACKUP / f"{d['name']}_{ts}.xlsx"
    wb  = Workbook(); ws = wb.active
    ws.append(list(d.keys()) + ["price","time"])
    ws.append(list(d.values()) + [price, ts])
    wb.save(xls)

    # 4️⃣ CAPI 上傳 (含真實 IP)
    real_ip = request.remote_addr or ""
    ud = {
        "external_id": sha(d["email"] or d["phone"] or d["name"]),
        "em": sha(d["email"].lower()),
        "ph": sha(d["phone"]),
        "fn": sha(fn),
        "ln": sha(ln),
        "db": sha(birthday),
        "ge": sha(gender),    # Meta 官方建議 gender 也要 hash
        "country": sha(country),
        "client_ip_address": real_ip,
        "client_user_agent": request.headers.get("User-Agent",""),
        "fbc": fbc,
        "fbp": fbp
    }
    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_id": eid,
            "action_source": "website",
            "user_data": ud,
            "custom_data": {"currency": CURRENCY, "value": price}
        }],
        "upload_tag": f"form_{ts}"
    }
    try:
        r = requests.post(API_URL, json=payload, params={"access_token": TOKEN}, timeout=10)
        logging.info("Meta CAPI %s → %s", r.status_code, r.text)
        r.raise_for_status()
    except Exception as e:
        logging.error("CAPI failed → queued retry: %s", e)
        with RETRY.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload) + "\n")

    # 5️⃣ Email 通知（詳細內容＋excel檔）
    try:
        tos = [t for t in [os.getenv("TO_EMAIL_1"), os.getenv("TO_EMAIL_2")] if t]
        if not tos:
            raise ValueError("TO_EMAIL_1 未設定")
        body = "\n".join([
            f"【填單時間】{ts}",
            f"【姓名】{d['name']}",
            f"【Email】{d['email']}",
            f"【電話】{d['phone']}",
            f"【生日】{d['birthday'] or '-'}",
            f"【性別】{'男性' if gender=='m' else '女性' if gender=='f' else d['gender'] or '-'}",
            f"【交易金額】NT${price:,}",
            f"【Event ID】{eid}",
            f"【服務態度滿意度】{d['satisfaction'] or '-'}",
            f"【建議內容】\n{d['suggestion'] or '-'}"
        ])
        msg = EmailMessage()
        msg["Subject"] = "新客戶表單回報"
        msg["From"]    = os.getenv("FROM_EMAIL")
        msg["To"]      = ",".join(tos)
        msg.set_content(body, charset="utf-8")
        with open(xls, "rb") as fp:
            msg.add_attachment(
                fp.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=xls.name
            )
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(os.getenv("FROM_EMAIL"), os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
        logging.info("✉️ Email sent → %s", msg["To"])
    except Exception:
        logging.exception("❌ Email 發送失敗")

    return make_response("感謝您的填寫！", 200)

# ── main ───────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    logging.info("Listening on %s", port)
    app.run("0.0.0.0", port)
