import os, re, json, random, time, hashlib, requests, smtplib, pickle, fcntl, traceback
from contextlib import contextmanager
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from flask import Flask, request, render_template_string, redirect, session
from openpyxl import Workbook

# ─── 基本設定 ──────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-me")
HSTS_HEADER = "max-age=63072000; includeSubDomains; preload"

PIXEL_ID      = os.environ["PIXEL_ID"]
ACCESS_TOKEN  = os.environ["ACCESS_TOKEN"]
API_URL       = f"https://graph.facebook.com/v14.0/{PIXEL_ID}/events"
CURRENCY      = "TWD"
VALUE_CHOICES = [19800, 28000, 28800, 34800, 39800, 45800]

FROM_EMAIL     = os.environ["FROM_EMAIL"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]
TO_EMAILS      = [os.environ["TO_EMAIL_1"], os.environ["TO_EMAIL_2"]]

CITIES = ["taipei","newtaipei","taoyuan","taichung","tainan","kaohsiung"]
CITY_ZIP_MAP = {"taipei":"100","newtaipei":"220","taoyuan":"330",
                "taichung":"400","tainan":"700","kaohsiung":"800"}

PROFILE_MAP_PATH = "user_profile_map.pkl"
BACKUP_FOLDER = Path("form_backups"); BACKUP_FOLDER.mkdir(exist_ok=True)
RETRY_FILE = "retry_queue.jsonl"; Path(RETRY_FILE).touch(exist_ok=True)

# ─── 工具 ───────────────────────────────────────────
@contextmanager
def locked_file(path, mode):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX); yield f; fcntl.flock(f, fcntl.LOCK_UN)

def mask(s, keep=2): return s[:keep] + "*"*(max(0, len(s)-keep-2)) + s[-2:] if s else ""
hash_sha256 = lambda t: hashlib.sha256(t.encode()).hexdigest() if t else ""
def normalize_phone(p): 
    p = re.sub(r"[^\d]","",p)
    return p if p.startswith("8869") else ("886"+p.lstrip("0") if p.startswith("09") else p)

# ─── CSRF ──────────────────────────────────────────
def generate_csrf():
    if "csrf" not in session: session["csrf"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]
def verify_csrf(t): return t and t == session.get("csrf")

# ─── HTML 表單 (省略… 與你現有相同) ─────────────────
HTML_FORM = '''<html>…略…</html>'''

# ─── HTTPS / HSTS 防護 ─────────────────────────────
@app.before_request
def force_https():
    if request.headers.get("X-Forwarded-Proto","http") != "https":
        return redirect(request.url.replace("http://","https://"), code=301)
@app.after_request
def add_hsts(r): r.headers["Strict-Transport-Security"]=HSTS_HEADER; return r

# ─── Healthz ───────────────────────────────────────
@app.route('/healthz')
def healthz(): return "OK", 200

# ─── 表單頁 ────────────────────────────────────────
@app.route('/')
def index(): return render_template_string(HTML_FORM, csrf=generate_csrf(), cities=CITIES)

# ─── 表單提交 ──────────────────────────────────────
@app.route('/submit', methods=['POST'])
def submit():
    if not verify_csrf(request.form.get("csrf_token")):
        return "CSRF token 錯誤", 400

    # 取表單資料
    name = request.form["name"].strip()
    birthday = request.form.get("birthday","").strip()
    gender = request.form.get("gender","female")
    email = request.form["email"].strip().lower()
    phone = normalize_phone(request.form["phone"])
    city  = request.form.get("city","").strip()
    sat   = request.form.get("satisfaction","")
    sug   = request.form.get("suggestion","")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = BACKUP_FOLDER / f"{name}_{ts}.xlsx"
    Workbook().active.append(["姓名",name,"Email",email,"電話",phone,"城市",city,"提交",ts])
    Workbook().save(backup)

    # profile_map 讀寫
    if not Path(PROFILE_MAP_PATH).exists():
        with open(PROFILE_MAP_PATH,"wb") as f: pickle.dump({},f)
    with locked_file(PROFILE_MAP_PATH,"rb") as f: profs = pickle.load(f)

    key = email or phone or name
    prof = profs.get(key, {})
    fn, ln = (name[:1], name[1:]) if re.match(r"^[\u4e00-\u9fa5]{2,4}$", name) else (name.split()[0], " ".join(name.split()[1:]) if len(name.split())>1 else "")
    if not city and prof.get("ct"): city = prof["ct"]
    prof.update({"fn":fn,"ln":ln,"em":email,"ph":phone,"ct":city,"zp":CITY_ZIP_MAP.get(city,""),"external_id":key})
    profs[key]=prof
    with locked_file(PROFILE_MAP_PATH,"wb") as f: pickle.dump(profs,f)

    # ── ★ event_id 去重：沿用前端帶來的值 ----------
    event_id = request.form.get("front_event_id","").strip()
    if not event_id:
        event_id = f"evt_{int(time.time()*1000)}_{random.randint(1000,9999)}"

    # user_data
    ud = {"external_id": hash_sha256(key),
          "fn": hash_sha256(fn), "ln": hash_sha256(ln),
          "em": hash_sha256(email), "ph": hash_sha256(phone)}
    if city: ud["ct"] = hash_sha256(city); ud["zp"] = hash_sha256(prof["zp"])

    custom = {"currency":CURRENCY,"value":random.choice(VALUE_CHOICES),
              "submit_time":ts,"gender":gender,"birthday":birthday}

    payload = {"data":[{
        "event_name":"Purchase","event_time":int(time.time()),
        "event_id":event_id,"action_source":"system_generated",
        "user_data":ud,"custom_data":custom}],
        "upload_tag":f"form_{ts}"}

    print("▶ 送出:", mask(email), mask(phone))
    try:
        resp = requests.post(API_URL, json=payload,
                             params={"access_token":ACCESS_TOKEN},
                             timeout=10)
    except Exception as e:
        print("❌ CAPI 錯誤:", e); traceback.print_exc()
        resp = type("obj",(),{"ok":False,"status_code":0,"json":lambda self:{}})()

    # 狀態寫回＋失敗入 Retry
    prof["last_capi_status"] = {"code":resp.status_code,
                                "fbtrace":resp.json().get("fbtrace_id") if resp.ok else "",
                                "at":ts}
    with locked_file(PROFILE_MAP_PATH,"wb") as f: pickle.dump(profs,f)
    if not resp.ok:
        with open(RETRY_FILE,"a",encoding="utf-8") as f: f.write(json.dumps(payload)+"\n")

    send_email_with_attachment(backup, {"姓名":name,"Email":email,"電話":phone})
    return "感謝您提供寶貴建議！"

# ─── 主程序 ───────────────────────────────────────
if __name__=="__main__":
    for v in ["PIXEL_ID","ACCESS_TOKEN","FROM_EMAIL","EMAIL_PASSWORD","TO_EMAIL_1","TO_EMAIL_2"]:
        assert v in os.environ, f"缺少環境變數 {v}"
    app.run(host="0.0.0.0", port=10000)
