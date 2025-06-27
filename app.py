#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — 2025-06-28
--------------------------------------------------
• 自動補件：本月同一 key（email / phone / name|birthday）只補一次
• 抽到後依序送 PageView → Purchase（PageView 時間戳比 Purchase 早 30-120 秒）
• user_data 只傳有值欄位；em/ph/fn/ln/db/ge/country 雜湊 (SHA-256)
• 表單送出時亦立即寫入 map、回傳 Purchase
"""

import os, re, time, json, hashlib, logging, smtplib, sys, fcntl, pickle, threading, random
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from email.message import EmailMessage

import requests
from flask import Flask, request, render_template_string, redirect, session, make_response
from openpyxl import Workbook
from werkzeug.middleware.proxy_fix import ProxyFix

# ══════════════ 基本參數 ══════════════ #
REQUIRED = [
    "PIXEL_ID", "ACCESS_TOKEN",
    "FROM_EMAIL", "EMAIL_PASSWORD",
    "TO_EMAIL_1", "TO_EMAIL_2",
    "SECRET_KEY"
]
_missing = [v for v in REQUIRED if not os.getenv(v)]
if _missing:
    logging.critical("缺少環境變數：%s", ", ".join(_missing))
    sys.exit(1)

PIXEL_ID = os.getenv("PIXEL_ID")
TOKEN    = os.getenv("ACCESS_TOKEN")
API_URL  = f"https://graph.facebook.com/v19.0/{PIXEL_ID}/events"
CURRENCY = "TWD"
PRICES   = [19800, 28800, 34800, 39800, 45800]

USER_PROFILE_MAP_PATH = "user_profile_map.pkl"
BACKUP = Path("form_backups"); BACKUP.mkdir(exist_ok=True)
EVENT_LOG = Path("event_submit_log.txt")
AUTO_USED_PATH = "auto_used.pkl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)
HSTS = "max-age=63072000; includeSubDomains; preload"

sha  = lambda s: hashlib.sha256(s.encode()).hexdigest() if s else ""
norm_phone = lambda p: ("886" + re.sub(r"[^\d]", "", p).lstrip("0")) if p.startswith("09") else re.sub(r"[^\d]", "", p)

# ══════════════ 工具函式 ══════════════ #
@contextmanager
def locked(path: str, mode: str):
    """跨程序鎖定 pickle 檔案"""
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield f
        fcntl.flock(f, fcntl.LOCK_UN)

def csrf():
    if "csrf" not in session:
        session["csrf"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]

DOUBLE_SURNAMES = {"歐陽","司馬","上官","夏侯","諸葛","聞人","東方","赫連","皇甫","尉遲","羊舌",
                   "淳于","公孫","仲孫","單于","令狐","鐘離","宇文","長孫","慕容","鮮于","拓跋",
                   "軒轅","百里","東郭","南宮","西門","北宮","呼延","梁丘","左丘","第五","太史"}
def split_name(name: str):
    if not name:
        return "", ""
    s = name.strip()
    if " " in s or "," in s:                       # 英文
        s = s.replace(",", " ")
        parts = [p for p in s.split() if p]
        return (parts[0], " ".join(parts[1:])) if len(parts) > 1 else ("", parts[0])
    if len(s) >= 3 and s[:2] in DOUBLE_SURNAMES:   # 複姓
        return s[:2], s[2:]
    if len(s) >= 2:                                # 單姓
        return s[0], s[1:]
    return s, ""

def log_event(ts:int, eid:str, fake:bool):
    with EVENT_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{ts},{eid},{'auto' if fake else 'real'}\n")

def recent_real_event_within(hours=36):
    cutoff = time.time() - hours*3600
    if not EVENT_LOG.exists():
        return False
    with EVENT_LOG.open("r", encoding="utf-8") as f:
        for line in reversed(list(f)):
            ts, flag = line.strip().split(",", 2)[:2]
            if int(ts) < cutoff:
                break
            if flag == "real":
                return True
    return False

# ══════════════ 自動補件邏輯 ══════════════ #
def _auto_used():
    if os.path.exists(AUTO_USED_PATH):
        with open(AUTO_USED_PATH, "rb") as f:
            used = pickle.load(f)
    else:
        used = {}
    cur_tag = datetime.now().strftime("%Y%m")
    if used.get("yyyymm") != cur_tag:
        used = {"yyyymm": cur_tag, "used": set()}
    return used

def get_random_user():
    used = _auto_used()
    with locked(USER_PROFILE_MAP_PATH, "a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
    pool = [(k, u) for k, u in mp.items()
            if k not in used["used"] and (u.get("em") or u.get("ph"))]
    return random.choice(pool) if pool else (None, None)

def random_event_id():
    return f"evt_{int(time.time())}_{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=16))}"

def _build_user_data(u:dict, display_name:str):
    ud = {}
    for field in ["em","ph","fn","ln","db","ge","country"]:
        if v := u.get(field):
            ud[field] = sha(v)
    # 非雜湊欄位
    for field in ["birthday","name","client_ip_address","client_user_agent","fbc","fbp"]:
        if v := u.get(field):
            ud[field] = v
    ud["external_id"] = sha(u.get("em") or u.get("ph") or display_name)
    return ud

def send_auto_event(reason="auto_fill"):
    key, user = get_random_user()
    if not user:
        logging.info("[自動補件] 本月已無可用候選")
        return

    # 產生兩組事件
    eid_pv  = random_event_id()
    eid_buy = random_event_id()
    ts_buy  = int(time.time())
    ts_pv   = ts_buy - random.randint(30, 120)     # PageView 提前 30-120 秒
    price   = random.choice(PRICES)

    # 更新 map
    with locked(USER_PROFILE_MAP_PATH, "a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
        mp[key]["event_id"] = eid_buy
        mp[key]["value"]    = price
        f.seek(0); pickle.dump(mp, f); f.truncate()

    # 標註本月已用
    used = _auto_used(); used["used"].add(key)
    with open(AUTO_USED_PATH, "wb") as f: pickle.dump(used, f)

    fn, ln = split_name(user.get("name","") or (user.get("fn","")+user.get("ln","")))
    user.setdefault("fn", fn); user.setdefault("ln", ln)
    disp_name = user_display_name(user)
    ud = _build_user_data(user, disp_name)

    def _post(payload):
        r = requests.post(API_URL, json=payload, params={"access_token":TOKEN}, timeout=10)
        r.raise_for_status()
        logging.info("[自動補件] %s %s", payload["data"][0]["event_name"], r.status_code)

    # PageView
    pv_payload = {
        "data":[{
            "event_name":"PageView","event_time":ts_pv,"event_id":eid_pv,
            "action_source":"website","user_data":ud,"custom_data":{}
        }],
        "upload_tag":f"auto_pv_{datetime.utcfromtimestamp(ts_pv):%Y%m%d_%H%M%S}"
    }
    # Purchase
    buy_payload = {
        "data":[{
            "event_name":"Purchase","event_time":ts_buy,"event_id":eid_buy,
            "action_source":"website","user_data":ud,
            "custom_data":{"currency":CURRENCY,"value":price}
        }],
        "upload_tag":f"auto_buy_{datetime.utcfromtimestamp(ts_buy):%Y%m%d_%H%M%S}"
    }
    try:
        _post(pv_payload); _post(buy_payload)
    except Exception as e:
        logging.error("[自動補件] CAPI 失敗：%s", e)

    log_event(ts_buy, eid_buy, fake=True)

    # 簡易通知信
    try:
        tos = [os.getenv("TO_EMAIL_1"), os.getenv("TO_EMAIL_2")]
        tos = [t for t in tos if t]
        body = (
            f"【Meta 自動補件通報】\n通報時間：{datetime.utcfromtimestamp(ts_buy):%Y-%m-%d %H:%M:%S}\n"
            f"PageView → Purchase 已送出\n\n"
            f"姓名　：{disp_name}\nEmail ：{user.get('em','')}\n電話　：{user.get('ph','')}\n"
            f"EventID (Buy)：{eid_buy}\n金額：NT${price:,}\n"
        )
        msg = EmailMessage()
        msg["Subject"]="【自動補件通報】Meta CAPI"
        msg["From"]=os.getenv("FROM_EMAIL"); msg["To"]=",".join(tos)
        msg.set_content(body, charset="utf-8")
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(os.getenv("FROM_EMAIL"), os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
    except Exception: logging.exception("通知信失敗")

# 自動補件背景執行緒
def auto_loop():
    while True:
        try:
            if not recent_real_event_within(36):
                logging.info("[自動補件] 36h 內無真實事件，觸發補件")
                send_auto_event()
                time.sleep(random.randint(32*3600, 38*3600))
            else:
                logging.info("[自動補件] 最近 36h 已有真實事件，1h 後再檢查")
                time.sleep(3600)
        except Exception as e:
            logging.exception("自動補件主迴圈錯誤：%s", e)
            time.sleep(3600)

threading.Thread(target=auto_loop, daemon=True).start()

# ══════════════ HTML（與前版相同，已省略） ══════════════ #
HTML = open(__file__).read().split("HTML = r'''",1)[1].rsplit("'''",1)[0]  # ← 無變動，直接讀自身檔案

# ══════════════ Flask 路由 / 表單提交 ══════════════ #
@app.before_request
def https_redirect():
    if request.headers.get("X-Forwarded-Proto","http") != "https":
        return redirect(request.url.replace("http://","https://"), 301)

@app.after_request
def add_hsts(r):
    r.headers["Strict-Transport-Security"] = HSTS
    return r

@app.route('/healthz')
@app.route('/health')
def health(): return "OK", 200

@app.route('/')
def index():
    return render_template_string(HTML,
        csrf=csrf(), PIXEL_ID=PIXEL_ID,
        PRICES=json.dumps(PRICES), CURRENCY=CURRENCY)

@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token") != session.get("csrf"):
        return "CSRF!", 400

    d = {k: request.form.get(k,"").strip() for k in
         ("name","birthday","gender","email","phone","satisfaction","suggestion")}
    d["phone"] = norm_phone(d["phone"])
    price, eid_new = int(request.form["price"]), request.form["event_id"]
    fbc, fbp = request.form.get("fbc",""), request.form.get("fbp","")
    ts = int(time.time())

    fn, ln  = split_name(d["name"])
    b_day   = d["birthday"].replace("/","-") if d["birthday"] else ""
    gender  = "f" if d["gender"].lower() in ("female","f","女") else \
              "m" if d["gender"].lower() in ("male","m","男") else ""
    real_ip = request.remote_addr or ""; ua = request.headers.get("User-Agent","")
    country = "tw"

    with locked(USER_PROFILE_MAP_PATH,"a+b") as f:
        f.seek(0)
        mp = pickle.load(f) if os.path.getsize(USER_PROFILE_MAP_PATH) else {}
        keys=[]; 
        if d["email"]:keys.append(d["email"].lower())
        if d["phone"]:keys.append(d["phone"])
        if d["name"] and b_day:keys.append(f"{d['name']}|{b_day}")
        eid = next((mp.get(k,{}).get("event_id") for k in keys if k in mp), None) or eid_new
        for k in keys:
            u = mp.get(k,{})
            u.update({
                "fn":fn,"ln":ln,"db":b_day,"ge":gender,"country":country,
                "em":d["email"].lower(),"ph":d["phone"],
                "name":d["name"],"birthday":b_day,
                "event_id":eid,"value":price,
                "client_ip_address":real_ip,"client_user_agent":ua,
                "fbc":fbc,"fbp":fbp,
                "satisfaction":d["satisfaction"],"suggestion":d["suggestion"]
            }); mp[k]=u
        f.seek(0); pickle.dump(mp,f); f.truncate()

    # Excel 備份
    xls = BACKUP / f"{d['name']}_{datetime.utcfromtimestamp(ts):%Y%m%d_%H%M%S}.xlsx"
    wb=Workbook(); ws=wb.active
    ws.append(list(d.keys())+["price","time"])
    ws.append(list(d.values())+[price,datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")])
    wb.save(xls)

    # 組 user_data
    ud = _build_user_data(
        dict(em=d["email"], ph=d["phone"], fn=fn, ln=ln, db=b_day, ge=gender,
             country=country, name=d["name"], birthday=b_day,
             client_ip_address=real_ip, client_user_agent=ua,
             fbc=fbc, fbp=fbp),
        d["name"]
    )

    payload = {
        "data":[{
            "event_name":"Purchase","event_time":ts,"event_id":eid,
            "action_source":"website","user_data":ud,
            "custom_data":{"currency":CURRENCY,"value":price}
        }],
        "upload_tag":f"form_{datetime.utcnow():%Y%m%d_%H%M%S}"
    }
    try:
        r=requests.post(API_URL,json=payload,params={"access_token":TOKEN},timeout=10)
        r.raise_for_status()
        logging.info("表單 Purchase %s", r.status_code)
    except Exception as e:
        logging.error("CAPI upload failed: %s",e)

    log_event(ts, eid, fake=False)
    return make_response("感謝您的填寫！",200)

# ─────────────── 主程式入口 ══════════════ #
if __name__ == "__main__":
    port=int(os.getenv("PORT",8000))
    logging.info("Listening on %s",port)
    app.run("0.0.0.0",port)
