import asyncio, aiohttp, json, os, time, csv
from aiohttp import web
from pathlib import Path
from collections import defaultdict, deque

API=os.environ["HELIUS_API_KEY"]
RPC=f"https://mainnet.helius-rpc.com/?api-key={API}"
WSS=f"wss://mainnet.helius-rpc.com/?api-key={API}"
PORT=int(os.getenv("PORT","8080"))

STATE=Path(os.getenv("WBS_STATE_DIR","state")); STATE.mkdir(parents=True,exist_ok=True)
SEED_FILE=os.getenv("WBS_SEED_FILE","wallets_seed.txt")
EVENTS=STATE/"events.csv"; TRADES=STATE/"paper_trades.csv"; SENSORS=STATE/"sensor_pool.json"

# ===== FROZEN WBS-A v0.1 TRADE RULE =====
WINDOW=300
MAX_ADDS=2
MIN_RATIO=.20
GUARD=10

START=float(os.getenv("WBS_STARTING_KRW","500000"))
POS=float(os.getenv("WBS_POSITION_FRACTION",".10"))
MAX_OPEN=int(os.getenv("WBS_MAX_OPEN_POSITIONS","3"))
COST=float(os.getenv("WBS_ROUND_TRIP_COST_PCT",".03"))

# ===== DISCOVERY ENGINE v0.2 =====
MIN_WQS=float(os.getenv("WBS_MIN_WQS","45"))
MAX_DYNAMIC=int(os.getenv("WBS_MAX_DYNAMIC_SENSORS","100"))
PROGRAM_SAMPLE=float(os.getenv("WBS_DISCOVERY_SAMPLE_SEC","12"))
HEARTBEAT=int(os.getenv("WBS_HEARTBEAT_SEC","60"))
HELIUS_MONTHLY_LIMIT=int(os.getenv("HELIUS_MONTHLY_CREDIT_LIMIT","1000000"))
HELIUS_RPC_RPS=float(os.getenv("HELIUS_RPC_RPS","8"))
MAX_INFLIGHT_HANDLES=int(os.getenv("WBS_MAX_INFLIGHT_HANDLES","24"))

PROGRAMS={
    "pumpfun":"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "pumpswap":"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
}
IGNORE={
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
}

DASHBOARD_HTML = r"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WBS Live Dashboard</title>
<style>
:root{--bg:#0b1020;--card:#151c33;--card2:#10172a;--text:#eaf0ff;--muted:#94a3b8;--green:#35d07f;--yellow:#ffd166;--red:#ff6b6b;--blue:#67b7ff}
*{box-sizing:border-box} body{margin:0;background:linear-gradient(180deg,#080d1a,#0d1324);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
.wrap{max-width:1180px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:16px}
h1{font-size:22px;margin:0}.live{display:flex;align-items:center;gap:8px;color:var(--green);font-weight:700}.dot{width:10px;height:10px;border-radius:99px;background:var(--green);box-shadow:0 0 16px var(--green)}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.card{background:rgba(21,28,51,.92);border:1px solid #25304e;border-radius:16px;padding:15px;box-shadow:0 8px 24px rgba(0,0,0,.18)}
.label{font-size:12px;color:var(--muted);margin-bottom:7px}.big{font-size:25px;font-weight:800}.sub{font-size:12px;color:var(--muted);margin-top:5px}
.two{display:grid;grid-template-columns:1.1fr .9fr;gap:12px;margin-top:12px}.section-title{font-size:14px;font-weight:800;margin-bottom:10px}.section-head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin:18px 2px 10px}.section-head h2{font-size:17px;margin:0}.section-head a{font-size:12px;color:var(--blue);text-decoration:none}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px 7px;border-bottom:1px solid #26314d}th{color:var(--muted);font-weight:600}
.badge{display:inline-block;padding:3px 7px;border-radius:999px;font-size:11px;font-weight:700}.ok{background:rgba(53,208,127,.13);color:var(--green)}.wait{background:rgba(255,209,102,.13);color:var(--yellow)}
.timeline{display:flex;flex-direction:column;gap:8px;max-height:420px;overflow:auto}.event{background:var(--card2);border-radius:12px;padding:10px}.event .t{font-size:11px;color:var(--muted);margin-bottom:4px}.event b{font-size:13px}
.meter{height:12px;background:#0a0f1e;border-radius:99px;overflow:hidden;margin:12px 0 8px}.meter>div{height:100%;width:0;background:linear-gradient(90deg,var(--green),var(--blue));transition:width .4s}.metric-list{display:grid;grid-template-columns:repeat(2,1fr);gap:9px}.metric{background:var(--card2);border-radius:11px;padding:10px}.metric b{display:block;font-size:18px;margin-top:3px}.full{margin-top:12px}.danger{color:var(--red)!important}.warn{color:var(--yellow)!important}.muted{color:var(--muted)}
.rule{margin-top:12px;background:#10172a;border:1px solid #25304e;border-radius:14px;padding:12px;font-size:12px;color:#b8c4da}
@media(max-width:850px){.grid{grid-template-columns:repeat(2,1fr)}.two{grid-template-columns:1fr}} @media(max-width:480px){.grid{grid-template-columns:1fr 1fr}.big{font-size:21px}.wrap{padding:12px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div><h1>WBS LIVE PAPER</h1><div class="sub">Smart Behavior 실시간 탐색 · 모의매매 · 운영 모니터링</div></div>
    <div class="live"><span class="dot"></span><span id="liveText">LIVE</span></div>
  </div>

  <div class="grid">
    <div class="card"><div class="label">모의 자산</div><div class="big" id="equity">-</div><div class="sub">시작 500,000원</div></div>
    <div class="card"><div class="label">Sensor Pool</div><div class="big" id="sensors">-</div><div class="sub" id="sensorSub">-</div></div>
    <div class="card"><div class="label">발견 후보</div><div class="big" id="candidates">-</div><div class="sub">능동 온체인 탐색 누적</div></div>
    <div class="card"><div class="label">최근 1시간 활동</div><div class="big" id="active">-</div><div class="sub">현재 센서 기준</div></div>
    <div class="card"><div class="label">오픈 포지션</div><div class="big" id="open">-</div><div class="sub">최대 3개</div></div>
    <div class="card"><div class="label">완료 모의매매</div><div class="big" id="trades">-</div><div class="sub">PAPER SELL 기준</div></div>
    <div class="card"><div class="label">마지막 신규 Sensor</div><div class="big" style="font-size:18px" id="lastSensor">-</div><div class="sub" id="lastSensorScore">-</div></div>
    <div class="card"><div class="label">엔진 상태</div><div class="big" style="font-size:18px" id="engine">-</div><div class="sub" id="updated">-</div></div>
  </div>

  <div class="section-head"><h2>Helius 사용량</h2><a href="https://dashboard.helius.dev" target="_blank" rel="noopener">공식 사용량 확인 ↗</a></div>
  <div class="grid">
    <div class="card"><div class="label">이번 달 추정 Credits</div><div class="big" id="monthCredits">-</div><div class="sub" id="monthLimit">-</div></div>
    <div class="card"><div class="label">오늘 추정 Credits</div><div class="big" id="todayCredits">-</div><div class="sub">앱이 직접 센 RPC 요청 기준</div></div>
    <div class="card"><div class="label">RPC 처리 속도</div><div class="big" id="rpcRate">-</div><div class="sub" id="rpcLatency">-</div></div>
    <div class="card"><div class="label">API 오류 / 429</div><div class="big" id="apiErrors">-</div><div class="sub" id="lastError">-</div></div>
  </div>

  <div class="two">
    <div class="card">
      <div class="section-title">월간 사용률 <span class="muted" id="usagePct">-</span></div>
      <div class="meter"><div id="usageBar"></div></div>
      <div class="sub" id="usageNotice">-</div>
      <div class="sub" id="methodBreakdown">-</div>
    </div>
    <div class="card">
      <div class="section-title">실시간 파이프라인</div>
      <div class="metric-list">
        <div class="metric"><span class="label">WS 이벤트</span><b id="wsEvents">-</b></div>
        <div class="metric"><span class="label">가져온 트랜잭션</span><b id="txFetched">-</b></div>
        <div class="metric"><span class="label">평가 지갑</span><b id="walletsScored">-</b></div>
        <div class="metric"><span class="label">통과 / 탈락</span><b id="scoreResult">-</b></div>
      </div>
      <div class="sub" id="pipelineAge">-</div>
    </div>
  </div>

  <div class="two">
    <div class="card">
      <div class="section-title">Sensor Pool</div>
      <table>
        <thead><tr><th>지갑</th><th>구분</th><th>WQS</th><th>최근 활동</th></tr></thead>
        <tbody id="sensorRows"></tbody>
      </table>
    </div>
    <div class="card">
      <div class="section-title">최근 이벤트</div>
      <div class="timeline" id="events"></div>
    </div>
  </div>

  <div class="card full">
    <div class="section-title">최근 후보 평가</div>
    <table>
      <thead><tr><th>지갑</th><th>판정</th><th>WQS</th><th>표본</th><th>출처</th><th>평가 시각</th></tr></thead>
      <tbody id="candidateRows"></tbody>
    </table>
  </div>

  <div class="rule">
    <b>동결 WBS-A v0.1:</b> 2-wallet / 5분 / A HOLD / A adds≤2 / B≥20% / B 진입 후 10초 no-sell → PAPER BUY.
    탐색 엔진은 Pump.fun/PumpSwap 실시간 흐름에서 신규 지갑 후보를 능동 발굴한다.
  </div>
</div>

<script>
const fmt=n=>Number(n||0).toLocaleString('ko-KR');
const ago=s=>{
  if(s===null||s===undefined)return '-';
  if(s<60)return Math.max(0,Math.floor(s))+'초 전';
  if(s<3600)return Math.floor(s/60)+'분 전';
  if(s<86400)return Math.floor(s/3600)+'시간 전';
  return Math.floor(s/86400)+'일 전';
};
const duration=s=>{
  s=Math.max(0,Math.floor(Number(s||0)));
  if(s<60)return s+'초';
  if(s<3600)return Math.floor(s/60)+'분';
  if(s<86400)return Math.floor(s/3600)+'시간 '+Math.floor((s%3600)/60)+'분';
  return Math.floor(s/86400)+'일 '+Math.floor((s%86400)/3600)+'시간';
};
async function refresh(){
  try{
    const r=await fetch('/api/status',{cache:'no-store'});
    const d=await r.json();
    document.getElementById('equity').textContent=fmt(Math.round(d.equity))+'원';
    document.getElementById('sensors').textContent=d.sensors;
    document.getElementById('sensorSub').textContent=`Seed ${d.seed_count} · 신규 ${d.dynamic_count}`;
    document.getElementById('candidates').textContent=d.candidates;
    document.getElementById('active').textContent=d.active_1h;
    document.getElementById('open').textContent=d.open_positions;
    document.getElementById('trades').textContent=d.closed_trades;
    document.getElementById('engine').textContent=d.engine_alive?'정상 감시중':'확인 필요';
    document.getElementById('updated').textContent=`가동 ${duration(d.uptime_sec)} · 갱신 ${new Date(d.server_time*1000).toLocaleTimeString('ko-KR')}`;
    const h=d.helius||{};
    document.getElementById('monthCredits').textContent=fmt(h.month_credits_est);
    document.getElementById('todayCredits').textContent=fmt(h.today_credits_est);
    document.getElementById('monthLimit').textContent=`한도 ${fmt(h.monthly_limit)} · ${Number(h.month_pct||0).toFixed(2)}%`;
    document.getElementById('rpcRate').textContent=Number(h.rpc_per_min||0).toFixed(1)+'/분';
    document.getElementById('rpcLatency').textContent=`평균 ${fmt(h.avg_rpc_ms)}ms · 총 ${fmt(h.session_rpc_attempts)}회`;
    document.getElementById('apiErrors').textContent=`${fmt(h.session_rpc_errors)} / ${fmt(h.rate_limits)}`;
    document.getElementById('lastError').textContent=h.last_error_age_sec==null?'오류 없음':'마지막 오류 '+ago(h.last_error_age_sec);
    const pct=Math.min(Number(h.month_pct||0),100);
    document.getElementById('usagePct').textContent=Number(h.month_pct||0).toFixed(3)+'%';
    document.getElementById('usageBar').style.width=pct+'%';
    document.getElementById('usageBar').style.background=pct>=80?'var(--red)':(pct>=50?'var(--yellow)':'linear-gradient(90deg,var(--green),var(--blue))');
    document.getElementById('usageNotice').textContent=h.estimate_notice||'-';
    document.getElementById('methodBreakdown').textContent='RPC '+Object.entries(h.methods||{}).map(([k,v])=>`${k} ${fmt(v)}`).join(' · ');
    document.getElementById('wsEvents').textContent=fmt(h.ws_notifications);
    document.getElementById('txFetched').textContent=fmt(h.transactions_fetched);
    document.getElementById('walletsScored').textContent=fmt(h.wallets_scored);
    document.getElementById('scoreResult').textContent=`${fmt(h.wallets_accepted)} / ${fmt(h.wallets_rejected)}`;
    document.getElementById('pipelineAge').textContent=`마지막 이벤트 ${ago(h.last_ws_age_sec)} · 처리중 ${fmt(d.inflight_handles)}/${fmt(d.max_inflight_handles)} · 샘플절약 ${fmt(h.program_throttled)} · 과부하드롭 ${fmt(h.overload_dropped)}`;
    if(d.last_sensor){
      document.getElementById('lastSensor').textContent=d.last_sensor.wallet;
      document.getElementById('lastSensorScore').textContent='WQS '+d.last_sensor.wqs+' · '+d.last_sensor.source;
    } else {
      document.getElementById('lastSensor').textContent='아직 없음';
      document.getElementById('lastSensorScore').textContent='후보 평가 중';
    }
    document.getElementById('sensorRows').innerHTML=d.sensor_table.map(x=>
      `<tr><td>${x.wallet}</td><td><span class="badge ${x.kind==='Seed'?'wait':'ok'}">${x.kind}</span></td><td>${x.wqs??'-'}</td><td>${ago(x.activity_age_sec)}</td></tr>`
    ).join('');
    document.getElementById('candidateRows').innerHTML=(d.candidate_table||[]).map(x=>
      `<tr><td>${x.wallet}</td><td><span class="badge ${x.accepted?'ok':'wait'}">${x.accepted?'통과':'탈락'}</span></td><td>${x.wqs??'-'}</td><td>${x.details?.sample??'-'}건</td><td>${x.source}</td><td>${ago(x.age_sec)}</td></tr>`
    ).join('') || '<tr><td colspan="6" class="muted">아직 평가가 완료된 후보가 없습니다.</td></tr>';
    document.getElementById('events').innerHTML=d.recent_events.map(x=>
      `<div class="event"><div class="t">${new Date(x.ts*1000).toLocaleTimeString('ko-KR')}</div><b>${x.type}</b> ${x.text}</div>`
    ).join('') || '<div class="sub">아직 표시할 이벤트가 없습니다.</div>';
    document.getElementById('liveText').textContent='LIVE';
  }catch(e){
    document.getElementById('liveText').textContent='RECONNECTING';
  }
}
refresh(); setInterval(refresh,3000);
</script>
</body></html>"""

def now(): return int(time.time())

def seed_wallets():
    return [x.strip() for x in Path(SEED_FILE).read_text().splitlines()
            if x.strip() and not x.strip().startswith("#")]

def putcsv(path,row,fields):
    new=not path.exists()
    with path.open("a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        if new:w.writeheader()
        w.writerow(row)

def csv_rows(path):
    if not path.exists(): return []
    try:
        with path.open("r",encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

class UsageMeter:
    """App-side Helius usage estimate. The Helius billing dashboard is authoritative."""
    def __init__(self,path):
        self.path=path
        self.started_at=now()
        self.session=defaultdict(int)
        self.latencies=deque(maxlen=200)
        self.methods=defaultdict(int)
        self.month=""
        self.day=""
        self.month_credits=0
        self.day_credits=0
        self.last_rpc_at=0
        self.last_ws_at=0
        self.last_error_at=0
        self._load()
        self._roll()

    def _keys(self,t=None):
        tm=time.gmtime(t or time.time())
        return time.strftime("%Y-%m",tm),time.strftime("%Y-%m-%d",tm)

    def _load(self):
        if not self.path.exists():return
        try:
            d=json.loads(self.path.read_text())
            self.month=str(d.get("month",""))
            self.day=str(d.get("day",""))
            self.month_credits=int(d.get("month_credits",0))
            self.day_credits=int(d.get("day_credits",0))
            self.methods.update({str(k):int(v) for k,v in d.get("methods",{}).items()})
        except Exception:
            pass

    def _roll(self):
        month,day=self._keys()
        if month!=self.month:
            self.month=month
            self.month_credits=0
            self.methods.clear()
        if day!=self.day:
            self.day=day
            self.day_credits=0

    def _credit_cost(self,method):
        if method=="getProgramAccounts" or "Archive" in method:return 10
        return 1

    def rpc_attempt(self,method):
        self._roll()
        cost=self._credit_cost(method)
        self.session["rpc_attempts"]+=1
        self.month_credits+=cost
        self.day_credits+=cost
        self.methods[method]+=1
        self.last_rpc_at=now()

    def rpc_success(self,latency_ms):
        self.session["rpc_success"]+=1
        self.latencies.append(latency_ms)

    def rpc_error(self,rate_limited=False):
        self.session["rpc_errors"]+=1
        if rate_limited:self.session["rate_limits"]+=1
        self.last_error_at=now()

    def inc(self,key,n=1):
        self.session[key]+=n

    def ws(self,kind):
        self.session["ws_notifications"]+=1
        self.session[f"ws_{kind}"]+=1
        self.last_ws_at=now()

    def save(self):
        self._roll()
        try:
            self.path.write_text(json.dumps({
                "month":self.month,"day":self.day,
                "month_credits":self.month_credits,"day_credits":self.day_credits,
                "methods":dict(self.methods),"updated_at":now()
            },ensure_ascii=False,indent=2))
        except Exception as e:
            print("[USAGE_SAVE_ERR]",repr(e),flush=True)

    def snapshot(self):
        self._roll()
        attempts=self.session["rpc_attempts"]
        avg=sum(self.latencies)/len(self.latencies) if self.latencies else 0
        pct=100*self.month_credits/max(HELIUS_MONTHLY_LIMIT,1)
        elapsed=max(now()-self.started_at,1)
        return {
            "estimate_notice":"앱 자체 추정치 · 실제 청구량은 Helius 대시보드 기준",
            "monthly_limit":HELIUS_MONTHLY_LIMIT,
            "month_credits_est":self.month_credits,
            "month_pct":round(pct,3),
            "today_credits_est":self.day_credits,
            "session_rpc_attempts":attempts,
            "session_rpc_success":self.session["rpc_success"],
            "session_rpc_errors":self.session["rpc_errors"],
            "rate_limits":self.session["rate_limits"],
            "rpc_per_min":round(attempts*60/elapsed,2),
            "avg_rpc_ms":round(avg,1),
            "last_rpc_age_sec":now()-self.last_rpc_at if self.last_rpc_at else None,
            "last_error_age_sec":now()-self.last_error_at if self.last_error_at else None,
            "methods":dict(sorted(self.methods.items(),key=lambda x:x[1],reverse=True)),
            "ws_notifications":self.session["ws_notifications"],
            "ws_wallet":self.session["ws_wallet"],
            "ws_mint":self.session["ws_mint"],
            "ws_program":self.session["ws_program"],
            "last_ws_age_sec":now()-self.last_ws_at if self.last_ws_at else None,
            "program_throttled":self.session["program_throttled"],
            "overload_dropped":self.session["overload_dropped"],
            "transactions_fetched":self.session["transactions_fetched"],
            "swap_events":self.session["swap_events"],
            "wallets_scored":self.session["wallets_scored"],
            "wallets_accepted":self.session["wallets_accepted"],
            "wallets_rejected":self.session["wallets_rejected"],
            "reconnects":self.session["reconnects"]
        }

USAGE=UsageMeter(STATE/"helius_usage.json")

class RpcPacer:
    def __init__(self,rps):
        self.interval=1/max(rps,0.1)
        self.lock=asyncio.Lock()
        self.next_at=0.0

    async def wait(self):
        async with self.lock:
            t=time.monotonic()
            delay=self.next_at-t
            if delay>0:await asyncio.sleep(delay)
            self.next_at=max(self.next_at,time.monotonic())+self.interval

RPC_PACER=RpcPacer(HELIUS_RPC_RPS)

async def rpc(s,m,p,retries=5):
    body={"jsonrpc":"2.0","id":1,"method":m,"params":p}
    for i in range(retries):
        await RPC_PACER.wait()
        USAGE.rpc_attempt(m)
        started=time.perf_counter()
        try:
            async with s.post(RPC,json=body,timeout=30) as r:
                if r.status==429:
                    USAGE.rpc_error(rate_limited=True)
                    await asyncio.sleep(min(2**i,15)); continue
                d=await r.json()
                if "error" in d: raise RuntimeError(d["error"])
                USAGE.rpc_success((time.perf_counter()-started)*1000)
                return d.get("result")
        except Exception:
            USAGE.rpc_error()
            if i==retries-1: raise
            await asyncio.sleep(min(2**i,15))

def keys(tx):
    out=[]
    for k in tx["transaction"]["message"].get("accountKeys",[]):
        out.append((k,False) if isinstance(k,str) else (k.get("pubkey"),bool(k.get("signer"))))
    return out

def swaps(tx,w):
    if not tx or not tx.get("meta") or tx["meta"].get("err") is not None:return []
    ks=keys(tx); idx=next((i for i,(p,s) in enumerate(ks) if p==w and s),None)
    if idx is None:return []
    preB=tx["meta"].get("preBalances") or []; postB=tx["meta"].get("postBalances") or []
    if idx>=len(preB) or idx>=len(postB):return []
    sol=(postB[idx]-preB[idx])/1e9

    def om(items):
        d=defaultdict(float)
        for x in items or []:
            if x.get("owner")!=w: continue
            m=x.get("mint"); ui=x.get("uiTokenAmount") or {}; a=ui.get("uiAmount")
            if not m: continue
            if a is None:
                try:a=float(ui.get("uiAmountString","0"))
                except:a=0.0
            d[m]+=float(a or 0)
        return d

    a,b=om(tx["meta"].get("preTokenBalances")),om(tx["meta"].get("postTokenBalances"))
    out=[]
    for m in set(a)|set(b):
        if m in IGNORE:continue
        td=b.get(m,0)-a.get(m,0)
        side="BUY" if td>0 and sol<0 else ("SELL" if td<0 and sol>0 else None)
        if not side:continue
        size=abs(sol); px=size/abs(td) if td else None
        out.append({"wallet":w,"mint":m,"side":side,"token_delta":td,"sol_size":size,
                    "px":px,"time":tx.get("blockTime") or now(),
                    "sig":tx["transaction"]["signatures"][0]})
    return out

async def gettx(s,sig):
    return await rpc(s,"getTransaction",[sig,{"encoding":"jsonParsed",
        "maxSupportedTransactionVersion":0,"commitment":"confirmed"}])

class Engine:
    def __init__(self,s):
        self.s=s
        self.seed=set(seed_wallets())
        self.sensor=set(self.seed)
        self.dynamic={}
        self.ws=None
        self.ws_connected=False
        self.rid=1000
        self.pending_sub={}
        self.sub={}
        self.subbed={"wallet":set(),"mint":set(),"program":set()}
        self.hold=defaultdict(lambda:defaultdict(float))
        self.bs=defaultdict(dict)
        self.pending={}
        self.open={}
        self.eq=START
        self.lastpx={}
        self.seen=set()
        self.last_activity=defaultdict(int)
        self.last_prog=defaultdict(float)
        self.cand=defaultdict(lambda:{"buys":0,"mints":set()})
        self.scoring=set()
        self.inflight_handles=0
        self.recent=deque(maxlen=40)
        self.started_at=now()
        self.last_loop_activity=now()
        self.closed_trades=len(csv_rows(TRADES))

        if SENSORS.exists():
            try:
                d=json.loads(SENSORS.read_text())
                self.dynamic=d.get("dynamic",{})
                for w,v in self.dynamic.items():
                    if v.get("accepted"):self.sensor.add(w)
            except Exception:
                pass

    def push(self,typ,text):
        self.recent.appendleft({"ts":now(),"type":typ,"text":text})

    def save(self):
        SENSORS.write_text(json.dumps({
            "updated_at":now(),
            "seed":sorted(self.seed),
            "dynamic":self.dynamic
        },ensure_ascii=False,indent=2))

    async def subscribe(self,kind,target,label=None):
        if target in self.subbed[kind]: return
        self.rid+=1
        rid=self.rid
        await self.ws.send_json({
            "jsonrpc":"2.0","id":rid,"method":"logsSubscribe",
            "params":[{"mentions":[target]},{"commitment":"confirmed"}]
        })
        self.pending_sub[rid]=(kind,label or target)
        self.subbed[kind].add(target)

    async def ack(self,d):
        rid=d.get("id")
        if rid not in self.pending_sub or "result" not in d:return False
        self.sub[d["result"]]=self.pending_sub.pop(rid)
        return True

    async def wqs(self,w):
        sigs=await rpc(self.s,"getSignaturesForAddress",[w,{"limit":30,"commitment":"confirmed"}]) or []
        ok=[x for x in sigs if x.get("err") is None]
        sample=ok[:10]
        n=0
        ms=set()
        for x in sample:
            tx=await gettx(self.s,x["signature"])
            ev=swaps(tx,w)
            n+=len(ev)
            ms|={e["mint"] for e in ev}
            await asyncio.sleep(.12)
        fail=1-len(ok)/max(len(sigs),1)
        score=100*(.45*min(n/5,1)+.30*min(len(ms)/3,1)+.25*max(0,1-fail))
        return round(score,2),{
            "sample":len(sample),"swaps":n,"mints":len(ms),"fail_rate":round(fail,3)
        }

    async def score(self,w,source):
        if w in self.scoring or w in self.sensor:return
        if len(self.sensor)>=len(self.seed)+MAX_DYNAMIC:return

        # Avoid repeatedly rescoring rejected wallets for 30 minutes.
        old=self.dynamic.get(w)
        if old and not old.get("accepted") and now()-int(old.get("ts",0))<1800:
            return

        self.scoring.add(w)
        try:
            score,det=await self.wqs(w)
            acc=score>=MIN_WQS
            USAGE.inc("wallets_scored")
            USAGE.inc("wallets_accepted" if acc else "wallets_rejected")
            self.dynamic[w]={
                "wqs_proxy":score,"details":det,"source":source,
                "ts":now(),"accepted":acc
            }
            self.save()
            msg=f"{'ACCEPT' if acc else 'reject'} {w[:8]} WQS={score} source={source}"
            print("[DISCOVERY]",msg,flush=True)
            self.push("DISCOVERY",msg)
            if acc:
                self.sensor.add(w)
                await self.subscribe("wallet",w)
        except Exception as e:
            print("[DISCOVERY_ERR]",w[:8],repr(e),flush=True)
            self.push("ERROR",f"Discovery {w[:8]} {type(e).__name__}")
        finally:
            self.scoring.discard(w)

    async def candidate(self,e,source):
        if e["side"]!="BUY" or e["wallet"] in self.sensor:return
        USAGE.inc("candidate_buys")
        c=self.cand[e["wallet"]]
        c["buys"]+=1
        c["mints"].add(e["mint"])
        if c["buys"]>=2 or len(c["mints"])>=2:
            asyncio.create_task(self.score(e["wallet"],source))

    async def event(self,e,source,eligible=True):
        USAGE.inc("swap_events")
        putcsv(EVENTS,{
            "ts":e["time"],"source":source,"wallet":e["wallet"],
            "mint":e["mint"],"side":e["side"],"sol_size":e["sol_size"],
            "px":e["px"],"sig":e["sig"]
        },["ts","source","wallet","mint","side","sol_size","px","sig"])

        w,m=e["wallet"],e["mint"]
        self.last_activity[w]=e["time"]
        self.last_loop_activity=now()
        if e["px"]:self.lastpx[m]=e["px"]

        if not eligible:return

        if e["side"]=="BUY":
            self.hold[w][m]+=max(e["token_delta"],0)
            st=self.bs[m].get(w)
            if not st or not st.get("active"):
                st={"first":e["time"],"adds":0,"cum":e["sol_size"],"active":True}
            else:
                st["adds"]+=1
                st["cum"]+=e["sol_size"]
            self.bs[m][w]=st

            if self.ws is not None:
                await self.subscribe("mint",m)

            for A,a in list(self.bs[m].items()):
                if A==w or A not in self.sensor or w not in self.sensor or not a.get("active"):
                    continue
                dt=e["time"]-a["first"]
                if not(0<=dt<=WINDOW) or a["adds"]>MAX_ADDS:continue
                ratio=e["sol_size"]/max(a["cum"],1e-12)
                if ratio<MIN_RATIO:continue

                k=f"{m}:{A}:{w}:{e['sig']}"
                if k in self.pending:continue

                self.pending[k]={
                    "mint":m,"A":A,"B":w,"B_px":e["px"],"bad":False
                }
                text=f"{m[:8]} A={A[:6]} B={w[:6]} guard=10s"
                print("[PENDING]",text,flush=True)
                self.push("PENDING",text)
                asyncio.create_task(self.guard(k))
        else:
            self.hold[w][m]=max(0,self.hold[w][m]+min(e["token_delta"],0))
            if w in self.bs[m]:
                self.bs[m][w]["active"]=self.hold[w][m]>1e-12

            for p in self.pending.values():
                if p["mint"]==m and w in (p["A"],p["B"]):
                    p["bad"]=True

            for tid,p in list(self.open.items()):
                if p["mint"]==m and w in (p["A"],p["B"]):
                    await self.close(tid,e)
                    break

    async def guard(self,k):
        await asyncio.sleep(GUARD)
        p=self.pending.pop(k,None)
        if not p or p["bad"]:
            print("[REJECT] distribution guard",flush=True)
            self.push("REJECT","10초 Distribution Guard")
            return
        if len(self.open)>=MAX_OPEN:
            print("[REJECT] max open",flush=True)
            self.push("REJECT","최대 동시 포지션")
            return

        px=self.lastpx.get(p["mint"]) or p["B_px"]
        if not px:return

        stake=self.eq*POS
        tid=f"T{int(time.time()*1000)}"
        self.open[tid]={**p,"entry":px,"stake":stake,"entry_ts":now()}
        text=f"{p['mint'][:8]} stake={stake:,.0f}KRW"
        print("[PAPER BUY]",tid,text,flush=True)
        self.push("PAPER BUY",text)

    async def close(self,tid,e):
        p=self.open.pop(tid)
        px=e["px"] or self.lastpx.get(p["mint"])
        if not px:return

        gross=px/p["entry"]-1
        net=gross-COST
        pnl=p["stake"]*net
        self.eq+=pnl
        self.closed_trades+=1

        row={
            "trade_id":tid,"mint":p["mint"],"A":p["A"],"B":p["B"],
            "entry_ts":p["entry_ts"],"exit_ts":e["time"],
            "entry_px":p["entry"],"exit_px":px,"gross_roi":gross,
            "net_roi":net,"stake_krw":p["stake"],"pnl_krw":pnl,
            "equity_after_krw":self.eq,"exit_wallet":e["wallet"],
            "exit_signature":e["sig"]
        }
        putcsv(TRADES,row,list(row))

        text=f"{p['mint'][:8]} ROI={net*100:.2f}% equity={self.eq:,.0f}KRW"
        print("[PAPER SELL]",tid,text,flush=True)
        self.push("PAPER SELL",text)

    async def handle(self,sig,kind,target):
        if sig in self.seen:return

        # Throttle BEFORE getTransaction to protect free Helius quota.
        if kind=="program":
            t=time.time()
            if t-self.last_prog[target]<PROGRAM_SAMPLE:
                USAGE.inc("program_throttled")
                return
            self.last_prog[target]=t

        self.seen.add(sig)
        tx=await gettx(self.s,sig)
        if not tx:return
        USAGE.inc("transactions_fetched")

        self.last_loop_activity=now()

        if kind=="wallet":
            for e in swaps(tx,target):
                await self.event(e,"sensor",True)

        elif kind=="mint":
            for w,sg in keys(tx):
                if not sg:continue
                for e in swaps(tx,w):
                    if e["mint"]!=target:continue
                    await self.candidate(e,"mint-live")
                    if w in self.sensor:
                        await self.event(e,"mint-live",True)

        else:
            for w,sg in keys(tx):
                if not sg:continue
                for e in swaps(tx,w):
                    await self.candidate(e,f"program:{target}")

    def schedule_handle(self,sig,kind,target):
        if self.inflight_handles>=MAX_INFLIGHT_HANDLES:
            USAGE.inc("overload_dropped")
            return
        self.inflight_handles+=1

        async def wrapped():
            try:
                await self.handle(sig,kind,target)
            except Exception as e:
                USAGE.inc("handler_errors")
                print("[HANDLE_ERR]",type(e).__name__,flush=True)
            finally:
                self.inflight_handles-=1

        asyncio.create_task(wrapped())

    async def bootstrap(self):
        print("[BOOTSTRAP] checking seed activity",flush=True)
        for w in sorted(self.seed):
            try:
                sigs=await rpc(self.s,"getSignaturesForAddress",[
                    w,{"limit":20,"commitment":"confirmed"}
                ]) or []

                if sigs:
                    self.last_activity[w]=sigs[0].get("blockTime") or 0

                age=now()-self.last_activity[w] if self.last_activity[w] else None
                print(f"[SEED] {w[:8]} last_tx_age_sec={age}",flush=True)

                # During bootstrap we do NOT subscribe mints before WS exists.
                for x in [s for s in sigs if s.get("err") is None][:5]:
                    tx=await gettx(self.s,x["signature"])
                    for e in swaps(tx,w):
                        await self.event(e,"bootstrap-seed",False)
                    await asyncio.sleep(.12)

            except Exception as e:
                print("[BOOTSTRAP_ERR]",w[:8],repr(e),flush=True)
        print("[BOOTSTRAP] complete",flush=True)

    def status(self):
        t=now()
        active=sum(
            1 for w in self.sensor
            if self.last_activity.get(w,0) and t-self.last_activity[w]<3600
        )

        accepted=[]
        for w,v in self.dynamic.items():
            if v.get("accepted"):
                accepted.append((w,v))
        accepted.sort(key=lambda x:int(x[1].get("ts",0)),reverse=True)

        last_sensor=None
        if accepted:
            w,v=accepted[0]
            last_sensor={
                "wallet":w[:10]+"…",
                "wqs":v.get("wqs_proxy"),
                "source":v.get("source","")
            }

        candidate_table=[]
        for w,v in sorted(
            self.dynamic.items(),
            key=lambda x:int(x[1].get("ts",0)),reverse=True
        )[:12]:
            candidate_table.append({
                "wallet":w[:10]+"…",
                "wqs":v.get("wqs_proxy"),
                "accepted":bool(v.get("accepted")),
                "source":v.get("source",""),
                "age_sec":t-int(v.get("ts",0)) if v.get("ts") else None,
                "details":v.get("details",{})
            })

        table=[]
        for w in sorted(self.sensor):
            v=self.dynamic.get(w)
            age=t-self.last_activity[w] if self.last_activity.get(w) else None
            table.append({
                "wallet":w[:8]+"…",
                "kind":"Dynamic" if v and v.get("accepted") else "Seed",
                "wqs":v.get("wqs_proxy") if v else None,
                "activity_age_sec":age
            })
        table.sort(key=lambda x:(0 if x["kind"]=="Dynamic" else 1, x["activity_age_sec"] if x["activity_age_sec"] is not None else 10**12))

        return {
            "server_time":t,
            "engine_alive":self.ws_connected,
            "uptime_sec":t-self.started_at,
            "equity":self.eq,
            "sensors":len(self.sensor),
            "seed_count":len(self.seed),
            "dynamic_count":len(self.sensor)-len(self.seed),
            "candidates":len(self.cand),
            "active_1h":active,
            "open_positions":len(self.open),
            "closed_trades":self.closed_trades,
            "last_sensor":last_sensor,
            "candidate_table":candidate_table,
            "sensor_table":table,
            "helius":USAGE.snapshot(),
            "inflight_handles":self.inflight_handles,
            "max_inflight_handles":MAX_INFLIGHT_HANDLES,
            "recent_events":list(self.recent)[:20]
        }

    async def heartbeat(self):
        while True:
            await asyncio.sleep(HEARTBEAT)
            USAGE.save()
            d=self.status()
            print(
                f"[HEARTBEAT] sensors={d['sensors']} dynamic={d['dynamic_count']} "
                f"active_1h={d['active_1h']} candidates={d['candidates']} "
                f"open={d['open_positions']} equity={d['equity']:,.0f}",
                flush=True
            )

    async def dashboard(self):
        async def home(request):
            return web.Response(text=DASHBOARD_HTML,content_type="text/html")
        async def api_status(request):
            return web.json_response(self.status())
        async def health(request):
            return web.json_response({"ok":True,"engine_alive":self.status()["engine_alive"]})

        app=web.Application()
        app.router.add_get("/",home)
        app.router.add_get("/api/status",api_status)
        app.router.add_get("/health",health)

        runner=web.AppRunner(app)
        await runner.setup()
        site=web.TCPSite(runner,"0.0.0.0",PORT)
        await site.start()
        print(f"[DASHBOARD] listening on 0.0.0.0:{PORT}",flush=True)
        return runner

    async def run(self):
        print("WBS LIVE PAPER v0.3.0 + OPS DASHBOARD",flush=True)
        print(
            "Frozen trade rule unchanged: "
            "2-wallet/5m/A HOLD/A adds<=2/B>=20%/10s no-sell",
            flush=True
        )
        print(
            "Active discovery: seed sensors + Pump.fun/PumpSwap sampled live flow",
            flush=True
        )
        print(f"Starting paper equity: {self.eq:,.0f} KRW",flush=True)

        runner=await self.dashboard()
        try:
            # Critical startup order:
            # connect live WebSocket FIRST, subscribe immediately,
            # then do slow bootstrap in background.
            async with self.s.ws_connect(WSS,heartbeat=20,autoping=True) as ws:
                self.ws=ws
                self.ws_connected=True
                self.last_loop_activity=now()
                print("[LIVE] websocket connected - real-time detection started",flush=True)
                self.push("LIVE","실시간 탐지 시작")

                for w in sorted(self.sensor):
                    await self.subscribe("wallet",w)
                    await asyncio.sleep(.05)

                for name,p in PROGRAMS.items():
                    await self.subscribe("program",p,name)

                print("[LIVE] seed + Pump.fun + PumpSwap subscriptions requested",flush=True)
                asyncio.create_task(self.heartbeat())
                asyncio.create_task(self.bootstrap())

                async for msg in ws:
                    if msg.type!=aiohttp.WSMsgType.TEXT:
                        continue

                    d=json.loads(msg.data)
                    if await self.ack(d):
                        continue

                    pa=d.get("params") or {}
                    sid=pa.get("subscription")
                    val=(pa.get("result") or {}).get("value") or {}
                    sig=val.get("signature")
                    target=self.sub.get(sid)

                    if sig and target:
                        USAGE.ws(target[0])
                        self.last_loop_activity=now()
                        self.schedule_handle(sig,target[0],target[1])
        finally:
            self.ws_connected=False
            self.ws=None
            await runner.cleanup()

async def main():
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=45)
    ) as s:
        reconnect_delay=5
        while True:
            try:
                await Engine(s).run()
                reconnect_delay=5
            except Exception as e:
                USAGE.inc("reconnects")
                USAGE.save()
                status=getattr(e,"status",None)
                print("[RECONNECT]",type(e).__name__,f"status={status}" if status else "",flush=True)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay=min(reconnect_delay*2,60)

if __name__=="__main__":
    asyncio.run(main())
