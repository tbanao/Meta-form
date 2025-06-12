# app.py – Stable 2025-06-12
import os, re, json, time, random, hashlib, logging, smtplib, requests, sys, fcntl
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage
from flask import Flask, request, render_template_string, redirect, session, make_response
from openpyxl import Workbook

# ── 必填 ENV ──────────────────────────────────────
NEEDED = ["PIXEL_ID","ACCESS_TOKEN",
          "FROM_EMAIL","EMAIL_PASSWORD","TO_EMAIL_1","SECRET_KEY"]
lack = [v for v in NEEDED if not os.getenv(v)]
if lack:
    logging.critical("缺少環境變數：%s", ", ".join(lack)); sys.exit(1)

PIXEL_ID, TOKEN = os.getenv("PIXEL_ID"), os.getenv("ACCESS_TOKEN")
API_URL = f"https://graph.facebook.com/v19.0/{PIXEL_ID}/events"
CURRENCY = "TWD"
VALUES   = [19800,28800,34800,39800,45800]

# ── Flask ────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")
HSTS = "max-age=63072000; includeSubDomains; preload"

# ── 資料夾 ────────────────────────────────────────
BACKUP = Path("form_backups"); BACKUP.mkdir(exist_ok=True)
RETRY  = Path("retry_queue.jsonl"); RETRY.touch(exist_ok=True)

# ── 工具 ─────────────────────────────────────────
sha  = lambda s: hashlib.sha256(s.encode()).hexdigest() if s else ""
mask = lambda s,k=2: s[:k]+"*"*(max(0,len(s)-k-2))+s[-2:] if s else ""
def norm_phone(p:str):
    p=re.sub(r"[^\d]","",p)
    return ("886"+p.lstrip("0")) if p.startswith("09") else p
@contextmanager
def locked(p,mode):
    with open(p,mode) as f:
        fcntl.flock(f,fcntl.LOCK_EX); yield f; fcntl.flock(f,fcntl.LOCK_UN)
def csrf():
    if "csrf" not in session:
        session["csrf"]=hashlib.md5(os.urandom(16)).hexdigest()
    return session["csrf"]

# ── HTML（Pixel + fbc/fbp 自補）───────────────────
HTML=f'''<!DOCTYPE html><html lang="zh-TW"><head><meta charset="UTF-8">
<title>服務滿意度調查</title><style>
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

function getC(n){{return(document.cookie.match('(^|;) ?'+n+'=([^;]*)(;|$)')||[])[2]||''}}
function setC(n,v){{document.cookie=n+'='+v+';path=/;SameSite=Lax'}}
(function(){{ if(!getC('_fbp'))setC('_fbp','fb.1.'+Date.now()/1000+'.'+Math.random().toString().slice(2));
const id=new URLSearchParams(location.search).get('fbclid');
if(id&&!getC('_fbc'))setC('_fbc','fb.1.'+Date.now()/1000+'.'+id); }})();

const PRICES={VALUES};
function gid(){{return 'evt_'+Date.now()+'_'+Math.random().toString(36).slice(2)}}
function send(e){{
  e.preventDefault();
  if(!/^09\\d{{8}}$/.test(document.querySelector('[name=phone]').value))
    return alert('手機格式需09xxxxxxxx'),!1;
  const price=PRICES[Math.floor(Math.random()*PRICES.length)], eid=gid();
  ['eid','price','fbc','fbp'].forEach(x=>document.getElementById(x).value=
    x==='eid'?eid:x==='price'?price:x==='fbc'?getC('_fbc'):getC('_fbp'));
  fbq('track','Purchase',{{value:price,currency:'{CURRENCY}'}},
      {{eventID:eid,eventCallback:()=>e.target.submit()}});
  setTimeout(()=>e.target.submit(),800);
}}
</script></head><body>
<div class="form-container"><h2>服務滿意度調查</h2>
<form onsubmit="send(event)" method="post" action="/submit">
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
def https_redirect():
    if request.headers.get("X-Forwarded-Proto","http")!="https":
        return redirect(request.url.replace("http://","https://"),301)
@app.after_request
def add_hsts(r): r.headers["Strict-Transport-Security"]=HSTS; return r

# ── 路由 ────────────────────────────────────────
@app.route('/healthz'); app.route('/health')(lambda:("OK",200))
@app.route('/')
def index(): return render_template_string(HTML, csrf=csrf())

# ── Submit ─────────────────────────────────────
@app.route('/submit', methods=['POST'])
def submit():
    if request.form.get("csrf_token")!=session.get("csrf"): return "CSRF!",400

    data = {k:request.form.get(k,"").strip() for k in
            ("name","birthday","gender","email","phone","satisfaction","suggestion")}
    data["phone"]=norm_phone(data["phone"])
    price = int(request.form["price"]); eid=request.form["event_id"]
    fbc=request.form.get("fbc",""); fbp=request.form.get("fbp","")
    ts=datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Excel
    xls=BACKUP/f"{data['name']}_{ts}.xlsx"
    wb=Workbook(); ws=wb.active
    ws.append(list(data.keys())+["price","time"]); ws.append(list(data.values())+[price,ts])
    wb.save(xls)

    # CAPI
    ud={"external_id":sha(data["email"] or data["phone"] or data["name"]),
        "em":sha(data["email"]), "ph":sha(data["phone"]),
        "client_ip_address":request.remote_addr or "",
        "client_user_agent":request.headers.get("User-Agent",""),
        "fbc":fbc,"fbp":fbp}
    payload={"data":[{"event_name":"Purchase","event_time":int(time.time()),
                      "event_id":eid,"action_source":"website",
                      "user_data":ud,"custom_data":{"currency":CURRENCY,"value":price}}],
             "upload_tag":f"form_{ts}"}
    try:
        resp=requests.post(API_URL,json=payload,params={"access_token":TOKEN},timeout=10)
        logging.info("CAPI %s %s",resp.status_code,resp.text)
        resp.raise_for_status()
    except Exception as e:
        logging.error("CAPI failed, queued retry: %s",e)
        with open(RETRY,"a",encoding="utf-8") as fp: fp.write(json.dumps(payload)+"\n")

    # Email
    try:
        tos=[t for t in [os.getenv("TO_EMAIL_1"),os.getenv("TO_EMAIL_2")] if t]
        assert tos,"未設定 TO_EMAIL_1 / 2"
        body="\n".join([
            f"姓名: {data['name']}",
            f"Email: {data['email']}",
            f"電話: {data['phone']}",
            f"生日: {data['birthday'] or '-'}",
            f"性別: {'男性' if data['gender']=='male' else '女性'}",
            f"服務態度滿意度: {data['satisfaction'] or '-'}",
            "建議內容:\n"+(data['suggestion'] or "-")
        ])
        msg=EmailMessage(); msg["Subject"]="新客戶表單回報"
        msg["From"]=os.getenv("FROM_EMAIL"); msg["To"]=",".join(tos)
        msg.set_content(body,charset="utf-8")
        with open(xls,"rb") as fp:
            msg.add_attachment(fp.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=xls.name)
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(os.getenv("FROM_EMAIL"), os.getenv("EMAIL_PASSWORD"))
            s.send_message(msg)
        logging.info("✉️ Email sent to %s",msg["To"])
    except Exception:
        logging.exception("❌ Email 發送失敗")

    return make_response("感謝您的填寫！",200)

# ── main ───────────────────────────────────────
if __name__=="__main__":
    port=int(os.getenv("PORT",8000))
    logging.info("Listening on %s",port)
    app.run("0.0.0.0",port)
