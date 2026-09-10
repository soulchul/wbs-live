# WBS LIVE PAPER v0.1

Real-time Solana paper-trading prototype.

## Frozen trading rule

The strategy rule is **locked** for the out-of-sample test:

1. Wallet A buys a token.
2. A is still holding.
3. Independent sensor wallet B buys within 5 minutes.
4. A has made at most 2 adds before B.
5. B's buy size is at least 20% of A's cumulative buy size.
6. For 10 seconds after B enters, neither A nor B may sell.
7. If all conditions survive, create a PAPER BUY.
8. PAPER EXIT = first observed sell from A or B after entry.

Paper settings:
- Start equity: KRW 500,000
- Position: 10% of current equity
- Max concurrent positions: 3
- Conservative extra round-trip cost proxy: 3 percentage points
- No real orders, no private key, no wallet signing.

## Dynamic wallet discovery

The original 8 wallets are only seed sensors.

When an active token emits transactions from an unknown signer, the program:
- detects the candidate,
- samples recent history,
- computes an MVP WQS proxy,
- accepts candidates above `MIN_WQS`,
- subscribes to accepted wallets,
- grows the sensor pool up to 100 dynamic wallets.

Important: this is NOT "scan every Solana wallet on Earth." It is graph expansion from live token activity.
That avoids requiring an expensive full-chain firehose while still testing Smart Behavior outside the original sample.

The MVP WQS is deliberately conservative/simple. It is NOT yet the final realized-PnL WQS.
The frozen WBS-A trade rule is separate from WQS discovery logic.

## Files written

`state/events.csv`
All parsed BUY/SELL observations, including data that does not become a paper trade.

`state/paper_trades.csv`
Completed paper trades, ROI, PnL, and running paper equity.

`state/sensor_pool.json`
Seed + discovered candidate wallets and their WQS proxy.

## Run

Set environment variable:

`HELIUS_API_KEY=...`

Then:

`pip install -r requirements.txt`
`python main.py`

Never place the API key inside this repository.

## Cloud deployment

This project is Docker-ready. Use a continuously running worker/container platform,
set the `HELIUS_API_KEY` environment variable there, and mount/persist the `state/`
directory if the provider supports persistent storage.

Do not use GitHub Actions as the long-running live process; Actions is better for finite batch jobs.

## Important limitations of v0.1

- Uses standard Solana WebSocket `logsSubscribe` + standard RPC to remain compatible with a free Helius prototype.
- Dynamic discovery is local to tokens touched by the sensor graph, not a full-chain scan.
- The post-10-second entry price is an on-chain proxy from the latest observed transaction, not a guaranteed executable quote.
- The conservative swap classifier requires the wallet to be a signer and pairs token delta with native SOL delta.
- Complex routes that settle primarily through token-token or wrapped-token mechanics may be missed.
- Wallet clustering/controller detection is logged conceptually but not yet production-grade.
- Real-time state is not reconstructed after a restart; run continuity should be treated as a new observation session.
- Before live-money use, the parser, pricing, state recovery, clustering, RPC completeness, and execution model all require a stronger production implementation.

This version is for out-of-sample PAPER validation only.
