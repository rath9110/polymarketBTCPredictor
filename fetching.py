import requests, pandas as pd, time
from datetime import datetime, timedelta
import math
import json

def get_betting_markets(keyword=None):
    keyword = str(keyword or "").lower()
    url = "https://gamma-api.polymarket.com/markets"
    LIMIT = 500
    if keyword == "":
        params = {
            "closed": "true",
            "limit":  LIMIT,
            "order": "endDate",
            "ascending": "false"
        }
    else:
        params = {
            "closed": "true",
            "limit":  LIMIT,
            "order": "endDate",
            "ascending": "false",
            "search": keyword
        }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    markets = response.json()

    def match_keyword(m):
        question = (m.get("question") or "").lower()
        descrip_and_tion = (m.get("descrip_and_tion") or "").lower()
        # category = m.get("category")
        return (keyword in question) or (keyword in descrip_and_tion)

    betting_markets = []
    for m in markets:
        if match_keyword(m):
            betting_markets.append(m)
        else:
            continue
    return betting_markets

betting_markets = get_betting_markets()

KEYWORD = "bitcoin"

def parse_token_ids(m):
    """
    clobTokenIds may already be a list of strings, or a JSON-encoded string.
    Do NOT join and json.loads the concatenation—just load if string, pass-through if list.
    """
    token_ids = m.get("clobTokenIds")
    if token_ids is None:
        return None
    if isinstance(token_ids, list):
        return token_ids
    if isinstance(token_ids, str):
        try:
            return json.loads(token_ids)
        except Excep_and_tion:
            return None
    return None

def parse_outcomes(m):
    outs = m.get("outcomes")
    if outs is None:
        return None
    if isinstance(outs, list):
        return outs
    if isinstance(outs, str):
        try:
            return json.loads(outs)
        except Excep_and_tion:
            return None
    return None

def fetch_history_any(token_id):
    """
    Try multiple intervals/endpoints since 'interval=max' often returns emp_and_ty:
    1) interval=all
    2) no interval
    3) interval=max (last)
    Return a list of dicts (possibly emp_and_ty).
    """
    url = "https://clob.polymarket.com/prices-history"

    r = requests.get(url, params={"market": token_id, "interval": "all"}, timeout=30)
    if r.ok:
        data = (r.json() or {}).get("history") or []
        if data:
            return data

    r = requests.get(url, params={"market": token_id}, timeout=30)
    if r.ok:
        data = (r.json() or {}).get("history") or []
        if data:
            return data

    r = requests.get(url, params={"market": token_id, "interval": "max"}, timeout=30)
    if r.ok:
        data = (r.json() or {}).get("history") or []
        if data:
            return data
    return []

def normalize_history_rows(rows):
    """
    Ensure each row has 't' and 'p' keys.
    Some responses may use 'timestamp' or 'time'; map them to 't'.
    """
    norm = []
    for p_and_t in rows or []:
        if isinstance(p_and_t, dict):
            t = p_and_t.get("t")
            if t is None:
                t = p_and_t.get("timestamp", p_and_t.get("time"))
            p = p_and_t.get("p")
            if t is not None and p is not None:
                norm.append({"t": t, "p": p})
    return norm

all_merged = []
all_results = []
processed = 0
MAX_MARKETS = 50

for m in betting_markets:
    if processed >= MAX_MARKETS:
        break

    market_title = m.get("question") or "Untitled market"

    outcome_values = parse_outcomes(m)
    if not outcome_values or len(outcome_values) < 2:
        # print(f"Skipping {market_title} — outcomes not binary or missing")
        continue
    outcome_values_1 = outcome_values[0]
    outcome_values_2 = outcome_values[1]

    token_ids = parse_token_ids(m)
    if not token_ids or len(token_ids) < 2:
        # print(f"Skipping {market_title} — no/invalid clobTokenIds")
        continue

    outcome_1_token = token_ids[0]
    outcome_2_token = token_ids[1]

    outcome_1_raw = fetch_history_any(outcome_1_token)
    outcome_2_raw = fetch_history_any(outcome_2_token)

    if not outcome_1_raw and not outcome_2_raw:
        # print(f"Skipping {market_title} — no CLOB price history available")
        continue

    outcome_1_data = normalize_history_rows(outcome_1_raw)
    outcome_2_data = normalize_history_rows(outcome_2_raw)

    outcome_1 = pd.DataFrame(outcome_1_data)
    outcome_2 = pd.DataFrame(outcome_2_data)

    if "t" not in outcome_1.columns or "t" not in outcome_2.columns:
        # print(f"Skipping {market_title} missing 't' column in price history")
        continue

    merged_outcomes_df = outcome_1.merge(outcome_2, on="t", suffixes=("_outcome_1", "_outcome_2"))

    if merged_outcomes_df.empty:
        continue

    merged_outcomes_df["title"] = market_title
    merged_outcomes_df["datetime"] = merged_outcomes_df["t"].apply(lambda x: datetime.fromtimestamp(x).isoformat())
    merged_outcomes_df["outcome_value_1"] = outcome_values_1
    merged_outcomes_df["outcome_value_2"] = outcome_values_2
    merged_outcomes_df = merged_outcomes_df.drop(["t"], axis=1)

    all_merged.append((merged_outcomes_df))
    processed += 1

    # Determine final winner based on last price
    last_row = merged_outcomes_df.iloc[-1]
    final_winner = 0 if last_row["p_outcome_1"] > last_row["p_outcome_2"] else 1

    # Convert datetime to pandas Timestamp for easier arithmetic
    merged_outcomes_df["datetime"] = pd.to_datetime(merged_outcomes_df["datetime"])

    # Define the horizons we are testing
    horizons = {
        "4h": timedelta(hours=4),
        "12h": timedelta(hours=12),
        "24h": timedelta(hours=24),
        "1w": timedelta(weeks=1),
        "1m": timedelta(days=30)
    }

    accuracy_records = []

    final_time = merged_outcomes_df["datetime"].iloc[-1]

    for label, delta in horizons.items():
        target_time = final_time - delta

        # Find closest timestamp *before* or equal to the target time
        df_before = merged_outcomes_df[merged_outcomes_df["datetime"] <= target_time]

        if df_before.empty:
            continue  # Not enough history to evaluate this horizon

        snapshot = df_before.iloc[-1]  # most recent value before target_time

        prediction = 0 if snapshot["p_outcome_1"] > snapshot["p_outcome_2"] else 1

        accuracy_records.append({
            "market": market_title,
            "horizon": label,
            "prediction": prediction,
            "final_winner": final_winner,
            "correct": int(prediction == final_winner)
        })

    # Append these to a global list so we can compute aggregate accuracy later
    all_results.extend(accuracy_records)

results_df = pd.DataFrame(all_results)

summary = (
    results_df.groupby("horizon")["correct"]
    .agg(["sum", "count"])
    .rename(columns={"sum": "correct", "count": "total"})
)
summary["accuracy"] = summary["correct"] / summary["total"]

print(summary)
