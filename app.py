# app.py  (2025-06-12 －完整版：_fbc/_fbp 自動補齊 & routes 修正)
import os, re, json, random, time, hashlib, requests, smtplib, pickle, fcntl, logging
from contextlib import contextmanager
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from flask import Flask, request, render_template_string, redirect, session, make_response

# ─── 必填 ENV ──────────────────────────────────────
for v in ["PIXEL_ID", "ACCESS_TOKEN",
          "FROM_EMAIL", "EMAIL_PASSWORD", "TO_EMAIL_1", "TO_EMAIL_2",
          "SECRET_KEY"]:
    assert v in os.environ, f"缺少環境變數 {v}"

PIXEL_ID     = os.environ["PIXEL_ID"]
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
CURRENCY     = "TWD"
VALUE_CHOICES= [19800, 28000, 28800, 34800, 39800, 45800]

# ─── Logging ──────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s",
                    datefmt="%H:%M:%S")

# ─── Flask ────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ["SECRET_KEY"]
HSTS_HEADER    = "max-age=63072000; includeSubDomains; preload"
API_URL        = f"https://graph.facebook.com/v19.0/{PIXEL_ID}/events"

# ─── 檔案路徑 ──────────────────────────────────────
PROFILE_MAP = "user_profile_map.pkl"
BACKUP_DIR   = Path("form_backups"); BACKUP_DIR.mkdir(exist_ok=True)
RETRY_FILE   = "retry_queue.jsonl"; Path(RETRY_FILE).touch(exist_ok=True)

# ─── 共用工具 ──────────────────────────────────────
@contextmanager
def locked(path, mode):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX); yield f; fcntl.flock(f, fcntl.LOCK_UN)

mask = lambda s,k=2: s[:k] + "*"*(max(0,len(s)-k-2)) + s[-2:] if s else ""
sha  = lambda t: hashlib.sha256(t.encode()).hexdigest() if t else ""
norm_phone = lambda p: ("886"+re.sub(r"[^\d]","",p).lstrip("0")) if p.startswith("09") else re.sub(r"[^\d]","",p)

def csrf():
    if "csrf" not in session:
        session["csrf"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]

# ─── 前端 HTML（含 _fbc/_fbp 自動補齊）────────────
HTML = f'''
<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>服務滿意度調查</title>
<style>
body{{background:#f2f6fb;font-family:"微軟正黑體",Arial,sans-serif}}
.form-container{{background:#fff;max-width:420px;margin:60px auto;padding:36px;
border-radius:16px;box-shadow:0 4px 16px rgba(0,0,0,.1);text-align:center}}
input,select,textarea,button{{width:90%;padding:6px 10px;margin:6px 0 12px;
border:1px solid #ccc;border-radius:4px;font-size:16px;background:#fafbfc}}
button{{background:#568cf5;color:#fff;border:none;font-weight:bold;padding:10px 0}}
button:hover{{background:#376ad8}}
</style>
<script>
!function(f,b,e,v,n,t,s){{if(f.fbq)return;n=f.fbq=function(){{n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)}};if(!f._fbq)f._fbq=n;
n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}}(window,document,'script',
'https://connect.facebook.net/en_US/fbevents.js');
fbq('init','{PIXEL_ID}');fbq('track','PageView');

/* ===== 自動補 _fbc / _fbp ===== */
function getC(n){{return (document.cookie.match('(^|;) ?'+n+'=([^;]*)(;|$)')||[])[2]||'';}}
function setC(n,v){{document.cookie=n+'='+v+';path=/;SameSite=Lax';}}
(function ensureFbCookie(){{
  if(!getC('_fbp')) setC('_fbp','fb.1.'+Math.floor(Date.now()/1000)+'.'+Math.floor(Math.random()*1e16));
  const qs=new URLSearchParams(location.search), fbclid=qs.get('fbclid');
  if(fbclid && !getC('_fbc')) setC('_fbc','fb.1.'+Math.floor(Date.now()/1000)+'.'+fbclid);
}})();

const PRICES={VALUE_CHOICES};
function genID(){{return 'evt_'+Date.now()+'_'+Math.random().toString(36).slice(2);}}
function beforeSubmit(e){{
  e.preventDefault();
  const phone=document.querySelector('[name=phone]').value;
  if(phone && !/^09\\d{{8}}$/.test(phone)){{alert('手機格式需09xxxxxxxx');return false;}}

  const price=PRICES[Math.floor(Math.random()*PRICES.length)];
  const eid  =genID();

  document.getElementById('eid').value  =eid;
  document.getElementById('price').value=price;
  document.getElementById('fbc').value  =getC('_fbc');
  document.getElementById('fbp').value  =getC('_fbp');

  fbq('track','Purchase',
      {{value:price,currency:'{CURRENCY}'}},
      {{eventID:eid,eventCallback:()=>e.target.submit()}});
  setTimeout(()=>e.target.submit(),800);  // 保險
}}
</script></head><body>
<div class="form-container"><h2>服務滿意度調查</h2>
<form method="post" action="/submit" onsubmit="beforeSubmit(event)">
  <input type="hidden" name="csrf_token" value="{{{{csrf}}}}">
  姓名：<input type="text" name="name" required><br>
  出生年月日：<input type="date" name="birthday"><br>
  性別：
  <select name="gender"><option value="female">女性</option><option value="male">男性</option></select><br>
  Email：<input type="email" name="email" required><br>
  手機：<input type="text" name="phone" pattern="09\\d{{8}}" required><br>
  服務態度滿意度：<textarea name="satisfaction" rows="2"></textarea><br>
  建議：<textarea name="suggestion" rows="2"></textarea><br>

  <input type="hidden" id="eid"   name="event_id">
  <input type="hidden" id="price" name="price">
  <input type="hidden" id="fbc"   name="fbc">
  <input type="hidden" id="fbp"   name="fbp">

  <button type="submit">送出</button>
</form></div></body></html>
'''

# ─── HTTPS / HSTS ─────────────────────────────────
@app.before_request
def force_https():
    if request.headers.get("X-Forwarded-Proto", "http") != "https":
        return redirect(request.url.replace("http://", "https://"), 301)

@app.after_request
def add_hsts(r):
    r.headers["Strict-Transport-Security"] = HSTS_HEADER
    return r

# ─── Routes ──────────────────────────────────────
@app.route('/healthz')
def healthz():
    return "OK", 200

@app.route('/')
def index():
    return render_template_string(HTML, csrf=csrf())

# ─── Submit ──────────────────────────────────────
@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token") != session.get("csrf"):
        return "CSRF token 錯誤", 400

    # 取表單資料
    name   = request.form["name"].strip()
    bday   = request.form.get("birthday", "").strip()
    gender = request.form.get("gender", "female")
    email  = request.form["email"].lower().strip()
    phone  = norm_phone(request.form["phone"])
    sat    = request.form.get("satisfaction", "")
    sug    = request.form.get("suggestion", "")

    price  = int(request.form.get("price") or random.choice(VALUE_CHOICES))
    eid    = request.form.get("event_id") or f"evt_{int(time.time()*1000)}_{random.randint(1000,9999)}"
    fbc    = request.form.get("fbc") or request.cookies.get("_fbc","")
    fbp    = request.form.get("fbp") or request.cookies.get("_fbp","")

    if not fbc and (ref := request.referrer):
        if m := re.search(r"fbclid=([^&]+)", ref):
            fbc = f"fb.1.{int(time.time())}.{m.group(1)}"

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Excel 備份
    xls = BACKUP_DIR / f"{name}_{ts}.xlsx"
    from openpyxl import Workbook; wb = Workbook(); ws = wb.active
    ws.append(["姓名","Email","電話","價值","提交時間"])
    ws.append([name,email,phone,price,ts]); wb.save(xls)

    # user_profile_map 更新
    if not Path(PROFILE_MAP).exists():
        with open(PROFILE_MAP,"wb") as f: pickle.dump({},f)
    with locked(PROFILE_MAP,"rb") as f: profiles = pickle.load(f)

    key = email or phone or name
    fn,ln = (name[:1],name[1:]) if re.match(r"^[\u4e00-\u9fa5]{2,4}$",name) else \
            (name.split()[0], " ".join(name.split()[1:]) if len(name.split())>1 else "")
    city_choices = ["taipei","newtaipei","taoyuan","taichung","tainan","kaohsiung"]
    zp_map = {"taipei":"100","newtaipei":"220","taoyuan":"330",
              "taichung":"400","tainan":"700","kaohsiung":"800"}
    prof   = profiles.get(key, {})
    city   = prof.get("ct") or random.choice(city_choices)
    prof.update({"fn":fn,"ln":ln,"em":email,"ph":phone,"ct":city,"zp":zp_map[city]})
    profiles[key] = prof
    with locked(PROFILE_MAP,"wb") as f: pickle.dump(profiles,f)

    # CAPI user_data
    ud = {"external_id":sha(key), "fn":sha(fn), "ln":sha(ln),
          "em":sha(email), "ph":sha(phone),
          "ct":sha(city), "zp":sha(zp_map[city]),
          "client_ip_address": request.remote_addr or "",
          "client_user_agent": request.headers.get("User-Agent",""),
          "fbc":fbc, "fbp":fbp}

    custom = {"currency":CURRENCY, "value":price,
              "submit_time":ts, "gender":gender,
              "birthday":bday, "satisfaction":sat, "suggestion":sug}

    payload = {"data":[{"event_name":"Purchase","event_time":int(time.time()),
                        "event_id":eid,"action_source":"website",
                        "event_source_url":request.url_root.rstrip('/')+'/',
                        "user_data":ud,"custom_data":custom}],
               "upload_tag":f"form_{ts}"}

    # 發送 CAPI
    logging.info("▶ CAPI 送出 %s / %s", mask(email), mask(phone))
    try:
        resp = requests.post(API_URL, json=payload,
                             params={"access_token":ACCESS_TOKEN}, timeout=10)
    except Exception:
        logging.exception("❌ CAPI request error")
        resp = type("obj",(),{"ok":False,"status_code":0,"text":""})()

    logging.info("CAPI ➜ %s  %s", resp.status_code, resp.text)
    if not resp.ok:
        with open(RETRY_FILE,"a",encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False)+"\n")

    # 寄信通知
    msg = EmailMessage()
    msg["Subject"] = "新客戶表單"
    msg["From"]    = os.environ["FROM_EMAIL"]
    msg["To"]      = ",".join([os.environ["TO_EMAIL_1"], os.environ["TO_EMAIL_2"]])
    msg.set_content(f"{name} 送出表單 (價值 {price})")
    with open(xls,"rb") as f: msg.add_attachment(f.read(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=xls.name)
    with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
        s.login(os.environ["FROM_EMAIL"], os.environ["EMAIL_PASSWORD"])
        s.send_message(msg)

    return make_response("感謝您提供寶貴建議！", 200)

# ─── Main ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
