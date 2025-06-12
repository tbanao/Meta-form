# app.py – Render-Ready (Pixel + CAPI + Email + Excel)  2025-06-12 修正版
import os, re, json, time, random, hashlib, logging, smtplib, requests, sys, pickle, fcntl
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage
from flask import Flask, request, render_template_string, redirect, session, make_response
from openpyxl import Workbook

# ── 必填環境變數 ──────────────────────────────────
REQ = ["PIXEL_ID","ACCESS_TOKEN",
       "FROM_EMAIL","EMAIL_PASSWORD","TO_EMAIL_1","SECRET_KEY"]
miss = [v for v in REQ if v not in os.environ]
if miss:
    sys.stderr.write(f"[FATAL] 缺少環境變數：{', '.join(miss)}\n")
    sys.exit(1)

PIXEL_ID     = os.environ["PIXEL_ID"]
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
CURRENCY     = "TWD"
VALUE_CHOICES= [19800,28800,34800,39800,45800]

# ── 基本設定 ─────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s",
                    datefmt="%H:%M:%S")

app = Flask(__name__)
app.secret_key = os.environ["SECRET_KEY"]
API_URL = f"https://graph.facebook.com/v19.0/{PIXEL_ID}/events"
HSTS    = "max-age=63072000; includeSubDomains; preload"

# ── 路徑 ─────────────────────────────────────────
BACKUP_DIR = Path("form_backups"); BACKUP_DIR.mkdir(exist_ok=True)
RETRY_FILE = "retry_queue.jsonl"; Path(RETRY_FILE).touch(exist_ok=True)

# ── 共用函式 ────────────────────────────────────
mask = lambda s,k=2: s[:k]+"*"*(max(0,len(s)-k-2))+s[-2:] if s else ""
sha  = lambda s: hashlib.sha256(s.encode()).hexdigest() if s else ""
def norm_phone(p): p=re.sub(r"[^\d]","",p); return ("886"+p.lstrip("0")) if p.startswith("09") else p
@contextmanager
def locked(path, mode):
    with open(path, mode) as f:
        fcntl.flock(f, fcntl.LOCK_EX); yield f; fcntl.flock(f, fcntl.LOCK_UN)
def csrf():
    if "csrf" not in session:
        session["csrf"] = hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]

# ── 前端 HTML（Pixel + _fbc/_fbp 自動補）──────────
HTML = f'''<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>服務滿意度調查</title>
<style>
body{{background:#f2f6fb;font-family:"微軟正黑體",Arial,sans-serif}}
.form-container{{background:#fff;max-width:420px;margin:60px auto;padding:36px;
border-radius:16px;box-shadow:0 4px 16px rgba(0,0,0,.1);text-align:center}}
input,select,textarea,button{{width:90%;padding:6px 10px;margin:6px 0 12px;border:1px solid #ccc;
border-radius:4px;font-size:16px;background:#fafbfc}}
button{{background:#568cf5;color:#fff;border:none;font-weight:bold;padding:10px 0}}
button:hover{{background:#376ad8}}
</style>
<script>
!function(f,b,e,v,n,t,s){{if(f.fbq)return;n=f.fbq=function(){{n.callMethod?
n.callMethod.apply(n,arguments):n.queue.push(arguments)}};if(!f._fbq)f._fbq=n;
n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}}(window,document,'script',
'https://connect.facebook.net/en_US/fbevents.js');
fbq('init','{PIXEL_ID}'); fbq('track','PageView');

/* cookie 補齊 */
function getC(n){{return (document.cookie.match('(^|;) ?'+n+'=([^;]*)(;|$)')||[])[2]||'';}}
function setC(n,v){{document.cookie=n+'='+v+';path=/;SameSite=Lax';}}
(function(){{if(!getC('_fbp'))setC('_fbp','fb.1.'+Date.now()/1000+'.'+Math.random().toString().slice(2));
const id=new URLSearchParams(location.search).get('fbclid');
if(id&&!getC('_fbc'))setC('_fbc','fb.1.'+Date.now()/1000+'.'+id);}})();

const PRICES={VALUE_CHOICES};
function genID(){{return 'evt_'+Date.now()+'_'+Math.random().toString(36).slice(2);}}
function beforeSubmit(e){{
  e.preventDefault();
  const ph=document.querySelector('[name=phone]').value;
  if(ph&&!/^09\\d{{8}}$/.test(ph)){{alert('手機格式需09xxxxxxxx');return false;}}
  const price=PRICES[Math.floor(Math.random()*PRICES.length)];
  const eid=genID();
  document.getElementById('eid').value=eid;
  document.getElementById('price').value=price;
  document.getElementById('fbc').value=getC('_fbc');
  document.getElementById('fbp').value=getC('_fbp');
  fbq('track','Purchase',{{value:price,currency:'{CURRENCY}'}},
      {{eventID:eid,eventCallback:()=>e.target.submit()}});
  setTimeout(()=>e.target.submit(),800);
}}
</script></head><body>
<div class="form-container"><h2>服務滿意度調查</h2>
<form onsubmit="beforeSubmit(event)" method="post" action="/submit">
  <input type="hidden" name="csrf_token" value="{{{{csrf}}}}">
  姓名：<input name="name" required><br>
  出生年月日：<input type="date" name="birthday"><br>
  性別：<select name="gender"><option value="female">女性</option><option value="male">男性</option></select><br>
  Email：<input name="email" type="email" required><br>
  手機：<input name="phone" pattern="09\\d{{8}}" required><br>
  服務態度滿意度：<textarea name="satisfaction"></textarea><br>
  建議：<textarea name="suggestion"></textarea><br>
  <input type="hidden" id="eid"   name="event_id">
  <input type="hidden" id="price" name="price">
  <input type="hidden" id="fbc"   name="fbc">
  <input type="hidden" id="fbp"   name="fbp">
  <button>送出</button>
</form></div></body></html>'''

# ── HTTPS / HSTS ────────────────────────────────
@app.before_request
def _https():
    if request.headers.get("X-Forwarded-Proto","http")!="https":
        return redirect(request.url.replace("http://","https://"),301)
@app.after_request
def _hsts(r):
    r.headers["Strict-Transport-Security"]=HSTS
    return r

# ── Health & Index ─────────────────────────────
@app.route('/healthz')
def healthz(): return "OK",200
@app.route('/')
def index():  return render_template_string(HTML, csrf=csrf())

# ── Submit ─────────────────────────────────────
@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token")!=session.get("csrf"): return "CSRF!",400

    # 收資料
    f = {k:request.form.get(k,"").strip() for k in
         ("name","birthday","gender","email","phone","satisfaction","suggestion")}
    f["phone"] = norm_phone(f["phone"])
    price  = int(request.form["price"])
    eid    = request.form["event_id"]
    fbc    = request.form.get("fbc","")
    fbp    = request.form.get("fbp","")
    ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Excel 備份
    xls = BACKUP_DIR/f"{f['name']}_{ts}.xlsx"
    wb  = Workbook(); ws = wb.active
    ws.append(list(f.keys())+["price","time"]); ws.append(list(f.values())+[price,ts])
    wb.save(xls)

    # CAPI
    user_data = {"external_id":sha(f["email"] or f["phone"] or f["name"]),
                 "em":sha(f["email"]), "ph":sha(f["phone"]),
                 "client_ip_address":request.remote_addr or "",
                 "client_user_agent":request.headers.get("User-Agent",""),
                 "fbc":fbc,"fbp":fbp}
    capi_payload = {"data":[{"event_name":"Purchase",
                             "event_time":int(time.time()),
                             "event_id":eid,
                             "action_source":"website",
                             "user_data":user_data,
                             "custom_data":{"currency":CURRENCY,"value":price}}],
                    "upload_tag":f"form_{ts}"}
    try:
        r = requests.post(API_URL, json=capi_payload,
                          params={"access_token":ACCESS_TOKEN}, timeout=10)
        logging.info("CAPI %s %s", r.status_code, r.text)
        if not r.ok: raise RuntimeError
    except Exception as e:
        logging.error("CAPI failed → queued retry: %s", e)
        with open(RETRY_FILE,"a",encoding="utf-8") as fp:
            fp.write(json.dumps(capi_payload)+"\n")

    # Email
    body = "\n".join([
        f"姓名: {f['name']}",
        f"Email: {f['email']}",
        f"電話: {f['phone']}",
        f"生日: {f['birthday'] or '-'}",
        f"性別: {'男性' if f['gender']=='male' else '女性'}",
        f"服務態度滿意度: {f['satisfaction'] or '-'}",
        "建議內容:",
        f['suggestion'] or "-"
    ])
    msg = EmailMessage()
    msg["Subject"] = "新客戶表單回報"
    msg["From"]    = os.environ["FROM_EMAIL"]
    msg["To"]      = os.environ["TO_EMAIL_1"]
    msg.set_content(body, charset="utf-8")
    with open(xls,"rb") as fp:
        msg.add_attachment(fp.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=xls.name)
    with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
        s.login(os.environ["FROM_EMAIL"], os.environ["EMAIL_PASSWORD"])
        s.send_message(msg)

    return make_response("感謝您的填寫！",200)

# ── Local run / Render dyn PORT ──────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    logging.info("Listening on %s", port)
    app.run("0.0.0.0", port)