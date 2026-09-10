import asyncio, aiohttp, json, os, time, csv, math
from pathlib import Path
from collections import defaultdict, deque

RPC_HTTP = f"https://mainnet.helius-rpc.com/?api-key={os.environ['HELIUS_API_KEY']}"
RPC_WSS  = f"wss://mainnet.helius-rpc.com/?api-key={os.environ['HELIUS_API_KEY']}"

SEED_FILE = os.getenv("WBS_SEED_FILE", "wallets_seed.txt")
STATE_DIR = Path(os.getenv("WBS_STATE_DIR", "state"))
STATE_DIR.mkdir(exist_ok=True)
EVENTS_CSV = STATE_DIR / "events.csv"
TRADES_CSV = STATE_DIR / "paper_trades.csv"
SENSORS_JSON = STATE_DIR / "sensor_pool.json"

# ---- Frozen WBS-A v0.1 ----
CONVERGENCE_WINDOW_SEC = 300
MAX_A_PRE_B_ADDS = 2
MIN_B_TO_A_SIZE_RATIO = 0.20
NO_SELL_GUARD_SEC = 10

STARTING_KRW = float(os.getenv("WBS_STARTING_KRW", "500000"))
POSITION_FRACTION = float(os.getenv("WBS_POSITION_FRACTION", "0.10"))
MAX_OPEN_POSITIONS = int(os.getenv("WBS_MAX_OPEN_POSITIONS", "3"))
ROUND_TRIP_COST_PCT = float(os.getenv("WBS_ROUND_TRIP_COST_PCT", "0.03"))

# Discovery/WQS MVP parameters. These do NOT change the frozen trade rule.
DISCOVERY_HISTORY_TX = int(os.getenv("WBS_DISCOVERY_HISTORY_TX", "80"))
MIN_WQS = float(os.getenv("WBS_MIN_WQS", "45"))
MAX_DYNAMIC_SENSORS = int(os.getenv("WBS_MAX_DYNAMIC_SENSORS", "100"))

IGNORE_MINTS = {
    "So11111111111111111111111111111111111111112",   # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", # USDC
    "Es9vMFrzaCERmJfrF4H2FYDkR9VgrdLG6uM7m3K8p5fM",   # common USDT mint fallback
}

def now():
    return int(time.time())

def load_seeds():
    return [x.strip() for x in Path(SEED_FILE).read_text().splitlines()
            if x.strip() and not x.strip().startswith("#")]

def append_csv(path, row, headers):
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        if not exists:
            w.writeheader()
        w.writerow(row)

async def rpc(session, method, params, retries=6):
    payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params}
    for i in range(retries):
        try:
            async with session.post(RPC_HTTP, json=payload, timeout=30) as r:
                if r.status == 429:
                    await asyncio.sleep(min(2 ** i, 20))
                    continue
                data = await r.json()
                if "error" in data:
                    raise RuntimeError(data["error"])
                return data.get("result")
        except Exception:
            if i == retries - 1:
                raise
            await asyncio.sleep(min(2 ** i, 20))
    return None

def account_keys(tx):
    msg = tx["transaction"]["message"]
    keys = msg.get("accountKeys", [])
    out = []
    for k in keys:
        if isinstance(k, str):
            out.append((k, False))
        else:
            out.append((k.get("pubkey"), bool(k.get("signer"))))
    return out

def parse_wallet_swap(tx, wallet):
    """Conservative signer + SOL delta + token delta classifier."""
    if not tx or tx.get("meta") is None or tx["meta"].get("err") is not None:
        return []
    keys = account_keys(tx)
    idx = next((i for i,(pk,signer) in enumerate(keys) if pk == wallet and signer), None)
    if idx is None:
        return []

    meta = tx["meta"]
    pre_bal = meta.get("preBalances") or []
    post_bal = meta.get("postBalances") or []
    if idx >= len(pre_bal) or idx >= len(post_bal):
        return []
    sol_delta = (post_bal[idx] - pre_bal[idx]) / 1e9

    def owner_map(items):
        d = defaultdict(float)
        for x in items or []:
            if x.get("owner") != wallet:
                continue
            mint = x.get("mint")
            if not mint:
                continue
            ui = x.get("uiTokenAmount") or {}
            amt = ui.get("uiAmount")
            if amt is None:
                try:
                    amt = float(ui.get("uiAmountString","0"))
                except Exception:
                    amt = 0.0
            d[mint] += float(amt or 0)
        return d

    pre = owner_map(meta.get("preTokenBalances"))
    post = owner_map(meta.get("postTokenBalances"))
    events = []
    for mint in set(pre) | set(post):
        if mint in IGNORE_MINTS:
            continue
        delta = post.get(mint,0) - pre.get(mint,0)
        if abs(delta) <= 1e-15:
            continue
        side = None
        if delta > 0 and sol_delta < 0:
            side = "BUY"
        elif delta < 0 and sol_delta > 0:
            side = "SELL"
        if not side:
            continue
        sol_size = abs(sol_delta)
        px = sol_size / abs(delta) if delta else None
        events.append({
            "wallet": wallet,
            "mint": mint,
            "side": side,
            "token_delta": delta,
            "sol_delta": sol_delta,
            "sol_size": sol_size,
            "px_sol_per_token": px,
            "block_time": tx.get("blockTime") or now(),
            "signature": tx["transaction"]["signatures"][0],
        })
    return events

async def get_tx(session, sig):
    return await rpc(session, "getTransaction", [
        sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":0,
              "commitment":"confirmed"}
    ])

async def quick_wqs(session, wallet):
    """
    MVP quality screen for newly discovered wallets.
    Intentionally simple and frozen separately from WBS trade logic.
    Score rewards successful signer swap activity + diversity and penalizes failures.
    It is NOT yet a full realized-PnL WQS.
    """
    sigs = await rpc(session, "getSignaturesForAddress", [
        wallet, {"limit": DISCOVERY_HISTORY_TX, "commitment":"confirmed"}
    ]) or []
    if not sigs:
        return 0.0, {}
    ok = [s for s in sigs if s.get("err") is None]
    fail_rate = 1 - len(ok)/max(len(sigs),1)
    sample = ok[:min(25,len(ok))]
    swaps, mints = 0, set()
    for s in sample:
        tx = await get_tx(session, s["signature"])
        evs = parse_wallet_swap(tx, wallet)
        swaps += len(evs)
        mints.update(e["mint"] for e in evs)
        await asyncio.sleep(0.11)
    activity = min(swaps/10, 1.0)
    diversity = min(len(mints)/6, 1.0)
    reliability = max(0, 1-fail_rate)
    score = 100*(0.45*activity + 0.30*diversity + 0.25*reliability)
    return round(score,2), {
        "sample_sigs":len(sigs), "sample_swaps":swaps,
        "unique_mints":len(mints), "fail_rate":round(fail_rate,4)
    }

class Engine:
    def __init__(self, session):
        self.session = session
        self.seed_wallets = set(load_seeds())
        self.sensors = set(self.seed_wallets)
        self.dynamic = {}
        self.wallet_sub_ids = {}
        self.mint_sub_ids = {}
        self.sub_to_target = {}
        self.holdings = defaultdict(lambda: defaultdict(float))
        self.buy_state = defaultdict(dict)   # mint -> wallet -> state
        self.pending = {}
        self.open_positions = {}
        self.paper_equity = STARTING_KRW
        self.last_market_px = {}
        self.seen_sigs = set()
        self.ws = None
        self.rpc_id = 1000

    def save_sensors(self):
        SENSORS_JSON.write_text(json.dumps({
            "updated_at": now(),
            "seed": sorted(self.seed_wallets),
            "dynamic": self.dynamic,
        }, ensure_ascii=False, indent=2))

    async def subscribe_wallet(self, wallet):
        if wallet in self.wallet_sub_ids:
            return
        self.rpc_id += 1
        rid = self.rpc_id
        await self.ws.send_json({
            "jsonrpc":"2.0","id":rid,"method":"logsSubscribe",
            "params":[{"mentions":[wallet]}, {"commitment":"confirmed"}]
        })
        self.sub_to_target[("pending",rid)] = ("wallet", wallet)

    async def subscribe_mint(self, mint):
        if mint in self.mint_sub_ids:
            return
        self.rpc_id += 1
        rid = self.rpc_id
        await self.ws.send_json({
            "jsonrpc":"2.0","id":rid,"method":"logsSubscribe",
            "params":[{"mentions":[mint]}, {"commitment":"confirmed"}]
        })
        self.sub_to_target[("pending",rid)] = ("mint", mint)

    async def handle_sub_ack(self, msg):
        rid = msg.get("id")
        if rid is None or "result" not in msg:
            return False
        key = ("pending", rid)
        if key not in self.sub_to_target:
            return False
        typ, target = self.sub_to_target.pop(key)
        subid = msg["result"]
        self.sub_to_target[("sub",subid)] = (typ,target)
        if typ == "wallet":
            self.wallet_sub_ids[target] = subid
        else:
            self.mint_sub_ids[target] = subid
        return True

    async def log_event(self, e, source):
        append_csv(EVENTS_CSV, {
            "ts":e["block_time"],"source":source,"wallet":e["wallet"],
            "mint":e["mint"],"side":e["side"],"sol_size":e["sol_size"],
            "px_sol_per_token":e["px_sol_per_token"],"signature":e["signature"]
        }, ["ts","source","wallet","mint","side","sol_size","px_sol_per_token","signature"])

    async def maybe_discover(self, tx, mint):
        # From a token transaction, inspect every signer as a candidate buyer/seller.
        for wallet, signer in account_keys(tx):
            if not signer or not wallet or wallet in self.sensors:
                continue
            evs = parse_wallet_swap(tx, wallet)
            if not any(e["mint"] == mint and e["side"] == "BUY" for e in evs):
                continue
            if len(self.sensors) >= len(self.seed_wallets)+MAX_DYNAMIC_SENSORS:
                return
            score, details = await quick_wqs(self.session, wallet)
            self.dynamic[wallet] = {
                "wqs_proxy":score, "details":details,
                "discovered_on_mint":mint, "ts":now(),
                "accepted": score >= MIN_WQS
            }
            self.save_sensors()
            if score >= MIN_WQS:
                self.sensors.add(wallet)
                await self.subscribe_wallet(wallet)
                print(f"[DISCOVERY] accepted {wallet[:8]}.. WQS={score}")

    async def process_event(self, e, source):
        await self.log_event(e, source)
        mint, wallet, side = e["mint"], e["wallet"], e["side"]
        px = e.get("px_sol_per_token")
        if px:
            self.last_market_px[mint] = px

        if side == "BUY":
            self.holdings[wallet][mint] += max(e["token_delta"],0)
            st = self.buy_state[mint].get(wallet)
            if not st or st["active"] is False:
                st = {
                    "first_buy_ts":e["block_time"], "last_buy_ts":e["block_time"],
                    "buy_count":1, "adds":0, "cum_sol":e["sol_size"],
                    "active":True, "first_px":px
                }
            else:
                st["buy_count"] += 1
                st["adds"] += 1
                st["last_buy_ts"] = e["block_time"]
                st["cum_sol"] += e["sol_size"]
            self.buy_state[mint][wallet] = st
            await self.subscribe_mint(mint)

            # Find prior active A among accepted sensors.
            for A, a in list(self.buy_state[mint].items()):
                if A == wallet or A not in self.sensors or wallet not in self.sensors:
                    continue
                if not a.get("active"):
                    continue
                dt = e["block_time"] - a["first_buy_ts"]
                if dt < 0 or dt > CONVERGENCE_WINDOW_SEC:
                    continue
                # Frozen filters known at B arrival.
                if a["adds"] > MAX_A_PRE_B_ADDS:
                    continue
                ratio = e["sol_size"] / max(a["cum_sol"],1e-12)
                if ratio < MIN_B_TO_A_SIZE_RATIO:
                    continue
                key = f"{mint}:{A}:{wallet}:{e['signature']}"
                if key in self.pending:
                    continue
                self.pending[key] = {
                    "mint":mint,"A":A,"B":wallet,"B_ts":e["block_time"],
                    "B_px":px,"A_cum_sol":a["cum_sol"],"B_sol":e["sol_size"],
                    "ratio":ratio,"invalid":False
                }
                print(f"[PENDING] {mint[:8]}.. A={A[:6]} B={wallet[:6]} guard=10s")
                asyncio.create_task(self.guard_and_enter(key))

        elif side == "SELL":
            self.holdings[wallet][mint] = max(0, self.holdings[wallet][mint] + min(e["token_delta"],0))
            if wallet in self.buy_state[mint]:
                self.buy_state[mint][wallet]["active"] = self.holdings[wallet][mint] > 1e-12

            # invalidate pending guards if A/B sells
            for k,p in list(self.pending.items()):
                if p["mint"] == mint and wallet in (p["A"],p["B"]):
                    p["invalid"] = True

            # Frozen exit: first A/B sell after paper entry.
            for k,pos in list(self.open_positions.items()):
                if pos["mint"] == mint and wallet in (pos["A"],pos["B"]):
                    await self.close_position(k, e)
                    break

    async def guard_and_enter(self, key):
        await asyncio.sleep(NO_SELL_GUARD_SEC)
        p = self.pending.pop(key, None)
        if not p or p["invalid"]:
            print(f"[REJECT] {p['mint'][:8] if p else ''} distribution guard")
            return
        if len(self.open_positions) >= MAX_OPEN_POSITIONS:
            print("[REJECT] max open positions")
            return

        # We need a post-guard on-chain proxy price. Prefer latest observed token trade.
        entry_px = self.last_market_px.get(p["mint"]) or p["B_px"]
        if not entry_px:
            print("[REJECT] no price proxy")
            return
        stake = self.paper_equity * POSITION_FRACTION
        units = stake / entry_px  # synthetic KRW-per-SOL scaling cancels in ROI
        tid = f"T{int(time.time()*1000)}"
        self.open_positions[tid] = {
            **p, "trade_id":tid, "entry_ts":now(), "entry_px":entry_px,
            "stake_krw":stake, "units_proxy":units,
        }
        print(f"[PAPER BUY] {tid} {p['mint'][:8]}.. stake={stake:,.0f} KRW")

    async def close_position(self, tid, sell_event):
        pos = self.open_positions.pop(tid)
        exit_px = sell_event.get("px_sol_per_token") or self.last_market_px.get(pos["mint"])
        if not exit_px:
            return
        gross = exit_px / pos["entry_px"] - 1
        net = gross - ROUND_TRIP_COST_PCT
        pnl = pos["stake_krw"] * net
        self.paper_equity += pnl
        row = {
            "trade_id":tid,"mint":pos["mint"],"A":pos["A"],"B":pos["B"],
            "entry_ts":pos["entry_ts"],"exit_ts":sell_event["block_time"],
            "entry_px":pos["entry_px"],"exit_px":exit_px,
            "gross_roi":gross,"net_roi":net,"stake_krw":pos["stake_krw"],
            "pnl_krw":pnl,"equity_after_krw":self.paper_equity,
            "exit_wallet":sell_event["wallet"],"exit_signature":sell_event["signature"]
        }
        append_csv(TRADES_CSV,row,list(row.keys()))
        print(f"[PAPER SELL] {tid} ROI={net*100:.2f}% equity={self.paper_equity:,.0f} KRW")

    async def handle_signature(self, sig, typ, target):
        if sig in self.seen_sigs:
            return
        self.seen_sigs.add(sig)
        tx = await get_tx(self.session, sig)
        if not tx:
            return
        if typ == "wallet":
            evs = parse_wallet_swap(tx, target)
            for e in evs:
                await self.process_event(e, "sensor")
        else:
            # Mint subscription provides a discovery/market-observation lane.
            # Parse all signers for this mint.
            for wallet, signer in account_keys(tx):
                if not signer or not wallet:
                    continue
                evs = parse_wallet_swap(tx, wallet)
                for e in evs:
                    if e["mint"] == target:
                        if e.get("px_sol_per_token"):
                            self.last_market_px[target] = e["px_sol_per_token"]
                        if wallet in self.sensors:
                            await self.process_event(e, "mint")
            await self.maybe_discover(tx, target)

    async def run(self):
        print("WBS LIVE PAPER v0.1")
        print("Frozen rule: 2-wallet/5m/A HOLD/A adds<=2/B size>=20%/10s no-sell")
        print(f"Starting paper equity: {self.paper_equity:,.0f} KRW")
        async with self.session.ws_connect(RPC_WSS, heartbeat=20, autoping=True) as ws:
            self.ws = ws
            for w in sorted(self.sensors):
                await self.subscribe_wallet(w)
                await asyncio.sleep(0.08)

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if await self.handle_sub_ack(data):
                        continue
                    params = data.get("params") or {}
                    subid = params.get("subscription")
                    result = params.get("result") or {}
                    val = result.get("value") or {}
                    sig = val.get("signature")
                    target = self.sub_to_target.get(("sub",subid))
                    if sig and target:
                        asyncio.create_task(self.handle_signature(sig, target[0], target[1]))
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break

async def main():
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                eng = Engine(session)
                await eng.run()
            except Exception as e:
                print("[RECONNECT]", repr(e))
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
