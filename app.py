#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — 2025-07-04
完全修正：event_id 由 Python 產生，/list 下載PKL、user_profile_map回存，無 Jinja2/str/import 問題
"""

import os, re, time, json, hashlib, logging, smtplib, sys, fcntl, pickle, threading, random, shutil
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

import requests
from flask import Flask, request, render_template_string, redirect, session, make_response, send_file
from markupsafe import Markup
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
    with locked(USER_PROFILE_MAP_PATH, "rb") as f:
        try:
            return pickle.load(f)
        except Exception:
            return {}

def save_user_map(mp):
    with locked(USER_PROFILE_MAP_PATH, "wb") as f:
        pickle.dump(mp, f)

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

def get_last_event_time():
    try:
        return int(Path(LAST_EVENT_FILE).read_text().strip())
    except:
        return 0

def update_last_event_time():
    Path(LAST_EVENT_FILE).write_text(str(int(time.time())))

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
        if r.status_code != 200 and retry < 3:
            time.sleep(3)
            return send_capi(events, tag, retry+1)
        return r
    except Exception:
        if retry < 3:
            time.sleep(3)
            return send_capi(events, tag, retry+1)
        raise

HTML = r'''<!DOCTYPE html>
<html lang="zh-TW">
<head><meta charset="UTF-8">
<title>服務滿意度調查</title>
</head>
<body>
<h2>服務滿意度調查表單</h2>
<p>目前時間：{{ now }}</p>
<form method="post" action="/submit">
    <input type="hidden" name="csrf_token" value="{{ csrf() }}">
    <input type="hidden" name="event_id" value="{{ event_id }}">
    姓名：<input name="name"><br>
    出生年月日：<input name="birthday" placeholder="YYYY-MM-DD"><br>
    性別：<select name="gender"><option value="女">女</option><option value="男">男</option></select><br>
    Email：<input name="email"><br>
    電話：<input name="phone"><br>
    成交金額：
        <select name="price">
            {% for p in PRICES %}
            <option value="{{p}}">{{p}}</option>
            {% endfor %}
        </select> {{CURRENCY}}<br>
    滿意度：<input name="satisfaction"><br>
    建議：<input name="suggestion"><br>
    <input type="hidden" name="fbc" value="">
    <input type="hidden" name="fbp" value="">
    <button type="submit">送出</button>
</form>
<a href="/list"><button>用戶名單/下載PKL</button></a>
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
    # 在這裡 event_id 由 Python 產生，不經 Jinja2 函數
    event_id = sha(str(time.time()) + str(random.random()))
    return render_template_string(
        HTML,
        PIXEL_ID=PIXEL_ID,
        PRICES=PRICES,
        CURRENCY=CURRENCY,
        csrf=csrf,
        sha=sha,
        now=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        event_id=event_id,
    )

@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token") != session.get("csrf"):
        return "CSRF!", 400

    # 讀取表單
    d = {k: request.form.get(k,"").strip() for k in
         ("name","birthday","gender","email","phone","satisfaction","suggestion")}
    d["phone"] = norm_phone(d["phone"])
    price     = int(request.form["price"])
    eid       = request.form.get("event_id") or sha(str(time.time()))
    fbc, fbp  = request.form.get("fbc",""), request.form.get("fbp","")
    ts        = int(time.time())
    fn, ln    = split_name(d["name"])
    ge        = "f" if d["gender"].lower() in ("female","f","女") else "m"
    country   = "tw"
    real_ip   = request.remote_addr or ""
    ua        = request.headers.get("User-Agent","")
    ct_zip, iplog = ip_lookup(real_ip)

    # 更新 user_profile_map
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

    # Excel 備份
    xls = BACKUP / f"{d['name']}_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}.xlsx"
    wb  = Workbook(); ws = wb.active
    ws.append(list(d.keys()) + ["price","time"])
    ws.append(list(d.values()) + [price, datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")])
    wb.save(xls)

    # 準備 user_data
    proto = {
        "fn":fn, "ln":ln,
        "em":d["email"].lower(), "ph":d["phone"],
        "ge":ge, "country":country,
        "birthday":d["birthday"], "db":d["birthday"].replace("-",""),
        "client_ip_address":real_ip, "client_user_agent":ua,
        "fbc":fbc, "fbp":fbp
    }
    ud = build_user_data(proto, d["email"] or d["phone"] or d["name"], ct_zip)

    # PageView
    pv = {
        "event_name":"PageView",
        "event_time":ts - random.randint(60,300),
        "event_id":f"{eid}_pv",
        "action_source":"website",
        "user_data":ud
    }
    # Purchase
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
    # MessageStart
    message_start = {
        "event_name":"MessageStart",
        "event_time":ts,
        "event_id":f"{eid}_msg",
        "action_source":"website",
        "event_source_url": request.url_root.rstrip('/') + "/ig-message-button",
        "user_data":ud,
        "custom_data":{
            "currency":CURRENCY,
            "value":0
        }
    }

    # 一次送三個事件
    try:
        send_capi([pv, purchase, message_start],
                  tag=f"form_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}")
    except Exception as e:
        logging.error("[CAPI] 失敗：%s", e)

    log_event(ts, eid, fake=False)

    # Email 通知
    try:
        tos = [os.getenv("TO_EMAIL_1"), os.getenv("TO_EMAIL_2")]
        msg = EmailMessage()
        msg["Subject"] = "新客戶表單回報"
        msg["From"]    = os.getenv("FROM_EMAIL")
        msg["To"]      = ",".join(tos)
        body = "\n".join([
            f"【填單時間】{datetime.utcfromtimestamp(ts):%Y-%m-%d %H:%M:%S}",
            f"【姓名】{d['name']}", f"【Email】{d['email']}",
            f"【手機】{d['phone']}", f"【生日】{d['birthday']}",
            f"【性別】{'女性' if ge=='f' else '男性'}",
            f"【城市】{ct_zip.get('ct','')}", f"【交易金額】NT${price:,}",
            f"【Event ID】{eid}", f"【滿意度】{d['satisfaction']}",
            f"【建議】{d['suggestion']}", f"{iplog}"
        ])
        msg.set_content(body, charset="utf-8")
        with open(xls, "rb") as fp:
            msg.add_attachment(fp.read(),
                               maintype="application",
                               subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               filename=Path(xls).name)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(os.getenv("FROM_EMAIL"), os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
    except Exception:
        logging.exception("Email 通知失敗")

    return make_response("感謝您的填寫！", 200)

@app.route('/list')
def list_users():
    mp = load_user_map()
    rows = []
    for k, v in mp.items():
        line = "<tr><td>{}</td>{}</tr>".format(
            k,
            "".join(f"<td>{v.get(col,'')}</td>" for col in
                ["name", "birthday", "ge", "em", "ph", "event_id", "value", "satisfaction", "suggestion"])
        )
        rows.append(line)
    table = f"""
    <h2>目前 user_profile_map</h2>
    <a href="/download_pkl"><button>下載最新 user_profile_map.pkl</button></a>
    <table border="1" cellpadding="4" cellspacing="0">
        <thead>
            <tr>
                <th>Key</th>
                <th>姓名</th>
                <th>生日</th>
                <th>性別</th>
                <th>Email</th>
                <th>電話</th>
                <th>event_id</th>
                <th>金額</th>
                <th>滿意度</th>
                <th>建議</th>
            </tr>
        </thead>
        <tbody>
            {"".join(rows)}
        </tbody>
    </table>
    <a href="/"><button>回首頁</button></a>
    """
    return Markup(table)

@app.route('/download_pkl')
def download_pkl():
    return send_file(USER_PROFILE_MAP_PATH, as_attachment=True, download_name="user_profile_map.pkl")

def pick_user():
    mp = load_user_map()
    used = set()
    if os.path.exists(AUTO_USED_PATH):
        with open(AUTO_USED_PATH, "rb") as f:
            try:
                used = pickle.load(f)
            except Exception:
                pass
    candidates = [ (k, v) for k, v in mp.items() if v.get("event_id") and k not in used ]
    if not candidates:
        return None, None
    pick = random.choice(candidates)
    used.add(pick[0])
    with open(AUTO_USED_PATH, "wb") as f:
        pickle.dump(used, f)
    return pick

def send_auto_event():
    key, u = pick_user()
    if not u:
        logging.info("[Auto] 無可補件對象")
        return
    ts = int(time.time())
    price = random.choice(PRICES)
    pv_id = f"evt_{ts}_{random.randrange(10**8):08d}"
    purchase_id = f"evt_{ts}_{random.randrange(10**8):08d}"
    ud = build_user_data(u, u.get("em") or u.get("ph") or key, {})
    pv = {
        "event_name":"PageView","event_time":ts-random.randint(60,300),
        "event_id":pv_id,"action_source":"website","user_data":ud
    }
    purchase = {
        "event_name":"Purchase","event_time":ts,"event_id":purchase_id,
        "action_source":"website","user_data":ud,
        "custom_data":{"currency":CURRENCY,"value":price}
    }
    message_start = {
        "event_name":"MessageStart","event_time":ts,
        "event_id":f"{purchase_id}_msg","action_source":"website",
        "event_source_url":"https://your-domain.com/ig-message-button",
        "user_data":ud,"custom_data":{"currency":CURRENCY,"value":0}
    }

    try:
        send_capi([pv, purchase, message_start],
                  tag=f"auto_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}")
        log_event(ts, purchase_id, fake=True)
        update_last_event_time()
    except Exception as e:
        logging.error("[Auto] 補件失敗：%s", e)

def auto_wake():
    while True:
        try:
            now = int(time.time())
            last = get_last_event_time()
            if now - last > random.randint(32*3600, 38*3600):
                send_auto_event()
            time.sleep(1200) # 20分鐘
        except Exception as e:
            logging.exception("Auto thread error: %s", e)

threading.Thread(target=auto_wake, daemon=True).start()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    logging.info("Listening on %s", port)
    app.run("0.0.0.0", port)
