import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import math
import json

# --- Polymarket endpoints (official) ---
URL_MARKETS = "https://gamma-api.polymarket.com/markets"
URL_HISTORY = "https://clob.polymarket.com/prices-history"

# --- Config ---
LIMIT = 500
MAX_MARKETS = 100
FEE = 0.02  # 2% fee on net winnings
LOOKBACK_DAYS = 30  # backtest window for markets that resolved within the last month

# Horizons (labels -> timedeltas)
HORIZONS = {
    "30d": timedelta(days=10),
    "5d": timedelta(days=5),
}

# Historical leader accuracy prior by horizon label (from your doc; tune as needed)
HISTORICAL_ACCURACY = {
    "10d": 0.91,
    "5d": 0.95,
}

def logit(p: float) -> float:
    return math.log(p / (1 - p))

def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def calculate_blended_probability(q_market: float, horizon_label: str, lam: float = 0.5) -> float:
    """
    Blend market-implied probability with historical leader accuracy (in log-odds space).
    lam in [0,1]: weight toward the historical prior.
    """
    q_market = min(max(q_market, 1e-6), 1 - 1e-6)
    a = HISTORICAL_ACCURACY.get(horizon_label, 0.91)
    a = min(max(a, 1e-6), 1 - 1e-6)
    return sigmoid((1 - lam) * logit(q_market) + lam * logit(a))

def get_betting_markets(keyword: str | None = None):
    """
    Fetch closed markets from Polymarket and filter to those that ended in the last LOOKBACK_DAYS.
    All time handling in UTC.
    """
    params = {
        "closed": "true",
        "limit": LIMIT,
        "order": "endDate",
        "ascending": "false",
        "search": keyword or "",
    }
    r = requests.get(URL_MARKETS, params=params, timeout=30)
    r.raise_for_status()
    markets = r.json()

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    def parse_end_dt(m):
        # gamma markets often include "endDate" as ISO8601
        end_raw = m.get("endDate") or m.get("endTime") or m.get("end_time")
        if not end_raw:
            return None
        try:
            # Force parse as UTC if no offset present
            dt = pd.to_datetime(end_raw, utc=True).to_pydatetime()
            return dt
        except Exception:
            return None

    filtered = []
    for m in markets:
        dt = parse_end_dt(m)
        if dt is None:
            continue
        if dt >= cutoff:
            filtered.append(m)

    return filtered

def parse_token_ids(market):
    token_ids = market.get("clobTokenIds")
    if token_ids is None:
        return None
    if isinstance(token_ids, list):
        return token_ids
    if isinstance(token_ids, str):
        try:
            return json.loads(token_ids)
        except Exception:
            return None
    return None

def fetch_history_any(token_id: str):
    """
    Fetch full price history for an outcome token.
    """
    r = requests.get(URL_HISTORY, params={"market": token_id, "interval": "all"}, timeout=30)
    if r.ok:
        return (r.json() or {}).get("history") or []
    return []

def normalize_history_rows(rows):
    """
    Normalize to {'t': <unix_seconds>, 'p': <price>} and convert to UTC-aware datetime.
    """
    norm = []
    for e in rows or []:
        if not isinstance(e, dict):
            continue
        t = e.get("t") or e.get("timestamp") or e.get("time")
        p = e.get("p")
        if t is None or p is None:
            continue
        # Many Polymarket histories use Unix seconds. Treat as UTC.
        try:
            ts = pd.to_datetime(int(t), unit="s", utc=True)  # UTC-aware
        except Exception:
            # Fallback: let pandas infer, force utc
            ts = pd.to_datetime(t, utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        norm.append({"ts": ts, "p": float(p)})
    print(norm)
    return norm

best_ev_descriptive = []

def backtest():
    markets = get_betting_markets()
    all_results = []

    for idx, m in enumerate(markets):
        if idx >= MAX_MARKETS:
            break

        title = (m.get("question") or "").strip() or "Untitled market"
        token_ids = parse_token_ids(m)
        if not token_ids or len(token_ids) < 2:
            continue

        # Outcome price histories
        h1 = normalize_history_rows(fetch_history_any(token_ids[0]))
        h2 = normalize_history_rows(fetch_history_any(token_ids[1]))
        if not h1 or not h2:
            continue

        df1 = pd.DataFrame(h1)
        df2 = pd.DataFrame(h2)

        # Merge on UTC timestamps
        merged = pd.merge(df1, df2, on="ts", how="inner", suffixes=("_o1", "_o2"))
        if merged.empty:
            continue

        # Sort by time just in case
        merged = merged.sort_values("ts").reset_index(drop=True)

        # For each horizon, snapshot at (final_ts - delta) in UTC
        final_ts = merged["ts"].iloc[-1]  # tz-aware UTC
        for h_label, delta in HORIZONS.items():
            target_ts = final_ts - delta

            # Choose last observation at or BEFORE target_ts (all UTC-aware)
            mask = merged["ts"] <= target_ts
            if not mask.any():
                continue
            snap = merged.loc[mask].iloc[-1]

            p1 = float(snap["p_o1"])
            p2 = float(snap["p_o2"])

            # Leader side = higher price at snapshot
            leader_q = max(p1, p2)
            underdog_q = min(p1, p2)

            # Blend leader probability toward historical leader accuracy
            p_blend = calculate_blended_probability(leader_q, h_label, lam=0.5)

            # EV after fees for leader and underdog (treated analogously)
            ev_leader = p_blend * (1 - FEE) - leader_q
            ev_underdog = (1 - p_blend) * (1 - FEE) - (1 - leader_q)

            best_side = "leader" if ev_leader >= ev_underdog else "underdog"
            best_ev = max(ev_leader, ev_underdog)

            print(best_ev)

            all_results.append({
                "market": title,
                "horizon": h_label,
                "snapshot_ts_utc": snap["ts"],  # tz-aware UTC
                "leader_price": leader_q,
                "underdog_price": underdog_q,
                "p_blend": p_blend,
                "ev_leader": ev_leader,
                "ev_underdog": ev_underdog,
                "best_side": best_side,
                "best_ev": best_ev,
            })

    if not all_results:
        print("No qualifying snapshots found in the last 30 days.")
        return

    results_df = pd.DataFrame(all_results)
    # Quick rollup
    summary = (
        results_df
        .groupby("horizon")["best_ev"]
        .agg(["count", "mean", "median"])
        .rename(columns={"count": "trades", "mean": "avg_EV", "median": "med_EV"})
        .sort_index()
    )

    print("Backtest summary (UTC throughout):")
    print(summary)
    print(results_df["best_ev"].describe())
    
    # Optionally return the detailed frame for inspection
    return results_df, summary

if __name__ == "__main__":
    detailed, summary = backtest()
