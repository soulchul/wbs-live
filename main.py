import asyncio, aiohttp, json, os, time, csv
from pathlib import Path
from collections import defaultdict

API=os.environ["HELIUS_API_KEY"]
RPC=f"https://mainnet.helius-rpc.com/?api-key={API}"
WSS=f"wss://mainnet.helius-rpc.com/?api-key={API}"
STATE=Path(os.getenv("WBS_STATE_DIR","state")); STATE.mkdir(parents=True,exist_ok=True)
SEED_FILE=os.getenv("WBS_SEED_FILE","wallets_seed.txt")
EVENTS=STATE/"events.csv"; TRADES=STATE/"paper_trades.csv"; SENSORS=STATE/"sensor_pool.json"

# FROZEN WBS-A v0.1
WINDOW=300; MAX_ADDS=2; MIN_RATIO=.20; GUARD=10
START=float(os.getenv("WBS_STARTING_KRW","500000"))
POS=float(os.getenv("WBS_POSITION_FRACTION",".10"))
MAX_OPEN=int(os.getenv("WBS_MAX_OPEN_POSITIONS","3"))
COST=float(os.getenv("WBS_ROUND_TRIP_COST_PCT",".03"))

# v0.2 discovery only
MIN_WQS=float(os.getenv("WBS_MIN_WQS","45"))
MAX_DYNAMIC=int(os.getenv("WBS_MAX_DYNAMIC_SENSORS","100"))
PROGRAM_SAMPLE=float(os.getenv("WBS_DISCOVERY_SAMPLE_SEC","12"))
HEARTBEAT=int(os.getenv("WBS_HEARTBEAT_SEC","60"))

PROGRAMS={
 "pumpfun":"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
 "pumpswap":"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
}
IGNORE={
 "So11111111111111111111111111111111111111112",
 "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
}

def now(): return int(time.time())
def seeds():
    return [x.strip() for x in Path(SEED_FILE).read_text().splitlines()
            if x.strip() and not x.strip().startswith("#")]
def putcsv(path,row,fields):
    new=not path.exists()
    with path.open("a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        if new:w.writeheader()
        w.writerow(row)

async def rpc(s,m,p,retries=5):
    body={"jsonrpc":"2.0","id":1,"method":m,"params":p}
    for i in range(retries):
        try:
            async with s.post(RPC,json=body,timeout=30) as r:
                if r.status==429:
                    await asyncio.sleep(min(2**i,15)); continue
                d=await r.json()
                if "error" in d: raise RuntimeError(d["error"])
                return d.get("result")
        except Exception:
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
        self.s=s; self.seed=set(seeds()); self.sensor=set(self.seed); self.dynamic={}
        self.ws=None; self.rid=1000; self.pending_sub={}; self.sub={}
        self.subbed={"wallet":set(),"mint":set(),"program":set()}
        self.hold=defaultdict(lambda:defaultdict(float)); self.bs=defaultdict(dict)
        self.pending={}; self.open={}; self.eq=START; self.lastpx={}; self.seen=set()
        self.last_activity=defaultdict(int); self.last_prog=defaultdict(float)
        self.cand=defaultdict(lambda:{"buys":0,"mints":set()}); self.scoring=set()
        if SENSORS.exists():
            try:
                d=json.loads(SENSORS.read_text()); self.dynamic=d.get("dynamic",{})
                for w,v in self.dynamic.items():
                    if v.get("accepted"):self.sensor.add(w)
            except:pass

    def save(self):
        SENSORS.write_text(json.dumps({"updated_at":now(),"seed":sorted(self.seed),
            "dynamic":self.dynamic},indent=2))

    async def subscribe(self,kind,target,label=None):
        if target in self.subbed[kind]: return
        self.rid+=1; rid=self.rid
        await self.ws.send_json({"jsonrpc":"2.0","id":rid,"method":"logsSubscribe",
            "params":[{"mentions":[target]},{"commitment":"confirmed"}]})
        self.pending_sub[rid]=(kind,label or target); self.subbed[kind].add(target)

    async def ack(self,d):
        rid=d.get("id")
        if rid not in self.pending_sub or "result" not in d:return False
        self.sub[d["result"]]=self.pending_sub.pop(rid); return True

    async def wqs(self,w):
        sigs=await rpc(self.s,"getSignaturesForAddress",[w,{"limit":30,"commitment":"confirmed"}]) or []
        ok=[x for x in sigs if x.get("err") is None]; sample=ok[:10]
        n=0; ms=set()
        for x in sample:
            tx=await gettx(self.s,x["signature"]); ev=swaps(tx,w)
            n+=len(ev); ms|={e["mint"] for e in ev}; await asyncio.sleep(.12)
        fail=1-len(ok)/max(len(sigs),1)
        score=100*(.45*min(n/5,1)+.30*min(len(ms)/3,1)+.25*max(0,1-fail))
        return round(score,2),{"sample":len(sample),"swaps":n,"mints":len(ms),"fail_rate":round(fail,3)}

    async def score(self,w,source):
        if w in self.scoring or w in self.sensor or len(self.sensor)>=len(self.seed)+MAX_DYNAMIC:return
        self.scoring.add(w)
        try:
            score,det=await self.wqs(w); acc=score>=MIN_WQS
            self.dynamic[w]={"wqs_proxy":score,"details":det,"source":source,"ts":now(),"accepted":acc}
            self.save()
            print(f"[DISCOVERY] {'ACCEPT' if acc else 'reject'} {w[:8]} WQS={score} source={source}",flush=True)
            if acc:self.sensor.add(w); await self.subscribe("wallet",w)
        except Exception as e: print("[DISCOVERY_ERR]",w[:8],repr(e),flush=True)
        finally:self.scoring.discard(w)

    async def candidate(self,e,source):
        if e["side"]!="BUY" or e["wallet"] in self.sensor:return
        c=self.cand[e["wallet"]]; c["buys"]+=1; c["mints"].add(e["mint"])
        if c["buys"]>=2 or len(c["mints"])>=2: asyncio.create_task(self.score(e["wallet"],source))

    async def event(self,e,source,eligible=True):
        putcsv(EVENTS,{"ts":e["time"],"source":source,"wallet":e["wallet"],"mint":e["mint"],
            "side":e["side"],"sol_size":e["sol_size"],"px":e["px"],"sig":e["sig"]},
            ["ts","source","wallet","mint","side","sol_size","px","sig"])
        w,m=e["wallet"],e["mint"]; self.last_activity[w]=e["time"]
        if e["px"]:self.lastpx[m]=e["px"]
        if not eligible:return

        if e["side"]=="BUY":
            self.hold[w][m]+=max(e["token_delta"],0); st=self.bs[m].get(w)
            if not st or not st.get("active"):
                st={"first":e["time"],"adds":0,"cum":e["sol_size"],"active":True}
            else:
                st["adds"]+=1; st["cum"]+=e["sol_size"]
            self.bs[m][w]=st; await self.subscribe("mint",m)
            for A,a in list(self.bs[m].items()):
                if A==w or A not in self.sensor or w not in self.sensor or not a.get("active"):continue
                dt=e["time"]-a["first"]
                if not(0<=dt<=WINDOW) or a["adds"]>MAX_ADDS:continue
                ratio=e["sol_size"]/max(a["cum"],1e-12)
                if ratio<MIN_RATIO:continue
                k=f"{m}:{A}:{w}:{e['sig']}"
                self.pending[k]={"mint":m,"A":A,"B":w,"B_px":e["px"],"bad":False}
                print(f"[PENDING] {m[:8]} A={A[:6]} B={w[:6]} guard=10s",flush=True)
                asyncio.create_task(self.guard(k))
        else:
            self.hold[w][m]=max(0,self.hold[w][m]+min(e["token_delta"],0))
            if w in self.bs[m]:self.bs[m][w]["active"]=self.hold[w][m]>1e-12
            for p in self.pending.values():
                if p["mint"]==m and w in (p["A"],p["B"]):p["bad"]=True
            for tid,p in list(self.open.items()):
                if p["mint"]==m and w in (p["A"],p["B"]):await self.close(tid,e); break

    async def guard(self,k):
        await asyncio.sleep(GUARD); p=self.pending.pop(k,None)
        if not p or p["bad"]: print("[REJECT] distribution guard",flush=True); return
        if len(self.open)>=MAX_OPEN: print("[REJECT] max open",flush=True); return
        px=self.lastpx.get(p["mint"]) or p["B_px"]
        if not px:return
        stake=self.eq*POS; tid=f"T{int(time.time()*1000)}"
        self.open[tid]={**p,"entry":px,"stake":stake,"entry_ts":now()}
        print(f"[PAPER BUY] {tid} {p['mint'][:8]} stake={stake:,.0f}KRW",flush=True)

    async def close(self,tid,e):
        p=self.open.pop(tid); px=e["px"] or self.lastpx.get(p["mint"])
        if not px:return
        gross=px/p["entry"]-1; net=gross-COST; pnl=p["stake"]*net; self.eq+=pnl
        row={"trade_id":tid,"mint":p["mint"],"A":p["A"],"B":p["B"],"entry_ts":p["entry_ts"],
             "exit_ts":e["time"],"entry_px":p["entry"],"exit_px":px,"gross_roi":gross,
             "net_roi":net,"stake_krw":p["stake"],"pnl_krw":pnl,"equity_after_krw":self.eq,
             "exit_wallet":e["wallet"],"exit_signature":e["sig"]}
        putcsv(TRADES,row,list(row))
        print(f"[PAPER SELL] {tid} ROI={net*100:.2f}% equity={self.eq:,.0f}KRW",flush=True)

    async def handle(self,sig,kind,target):
        if sig in self.seen:return
        if kind=="program":
            t=time.time()
            if t-self.last_prog[target]<PROGRAM_SAMPLE:return
            self.last_prog[target]=t
        self.seen.add(sig); tx=await gettx(self.s,sig)
        if not tx:return
        if kind=="wallet":
            for e in swaps(tx,target):await self.event(e,"sensor",True)
        elif kind=="mint":
            for w,sg in keys(tx):
                if not sg:continue
                for e in swaps(tx,w):
                    if e["mint"]!=target:continue
                    await self.candidate(e,"mint-live")
                    if w in self.sensor:await self.event(e,"mint-live",True)
        else:
            for w,sg in keys(tx):
                if not sg:continue
                for e in swaps(tx,w):await self.candidate(e,f"program:{target}")

    async def bootstrap(self):
        print("[BOOTSTRAP] checking seed activity",flush=True)
        for w in sorted(self.seed):
            try:
                sigs=await rpc(self.s,"getSignaturesForAddress",[w,{"limit":20,"commitment":"confirmed"}]) or []
                if sigs:self.last_activity[w]=sigs[0].get("blockTime") or 0
                age=now()-self.last_activity[w] if self.last_activity[w] else None
                print(f"[SEED] {w[:8]} last_tx_age_sec={age}",flush=True)
                for x in [s for s in sigs if s.get("err") is None][:5]:
                    tx=await gettx(self.s,x["signature"])
                    for e in swaps(tx,w):await self.event(e,"bootstrap-seed",True)
                    await asyncio.sleep(.12)
            except Exception as e:print("[BOOTSTRAP_ERR]",w[:8],repr(e),flush=True)

    async def heartbeat(self):
        while True:
            await asyncio.sleep(HEARTBEAT)
            active=sum(1 for w in self.sensor if now()-self.last_activity.get(w,0)<3600)
            print(f"[HEARTBEAT] sensors={len(self.sensor)} dynamic={len(self.sensor)-len(self.seed)} active_1h={active} candidates={len(self.cand)} open={len(self.open)} equity={self.eq:,.0f}",flush=True)

    async def run(self):
        print("WBS LIVE PAPER v0.2",flush=True)
        print("Frozen trade rule unchanged: 2-wallet/5m/A HOLD/A adds<=2/B>=20%/10s no-sell",flush=True)
        print("Active discovery: seed sensors + Pump.fun/PumpSwap sampled live flow",flush=True)
        print(f"Starting paper equity: {self.eq:,.0f} KRW",flush=True)
        await self.bootstrap()
        async with self.s.ws_connect(WSS,heartbeat=20,autoping=True) as ws:
            self.ws=ws
            for w in sorted(self.sensor):await self.subscribe("wallet",w); await asyncio.sleep(.05)
            for name,p in PROGRAMS.items():await self.subscribe("program",p,name)
            asyncio.create_task(self.heartbeat())
            async for msg in ws:
                if msg.type!=aiohttp.WSMsgType.TEXT:continue
                d=json.loads(msg.data)
                if await self.ack(d):continue
                pa=d.get("params") or {}; sid=pa.get("subscription")
                val=(pa.get("result") or {}).get("value") or {}; sig=val.get("signature")
                target=self.sub.get(sid)
                if sig and target:asyncio.create_task(self.handle(sig,target[0],target[1]))

async def main():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as s:
        while True:
            try:await Engine(s).run()
            except Exception as e:print("[RECONNECT]",repr(e),flush=True); await asyncio.sleep(5)

if __name__=="__main__":asyncio.run(main())
