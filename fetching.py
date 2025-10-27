import requests, pandas as pd, time
from datetime import datetime
import json

def get_btc_markets(pagination_offset):
    url = "https://gamma-api.polymarket.com/markets"
    LIMIT = 500
    params = {
        "closed": "true",
        "limit":  LIMIT,
        "order": "endDate",
        "ascending": "false",
        "category": "Crypto",
        "offset": pagination_offset
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    markets = response.json()

    KEYWORD = "bitcoin"

    def match_keyword(m):
        question = m.get("question").lower()
        description = m.get("description").lower()
        category = m.get("category")
        return (KEYWORD.lower() in question) or (KEYWORD.lower() in description) and ("Crypto" in category) 

    btc_markets= []
    for m in markets:
        if match_keyword(m):
            btc_markets.append(m)
        else:
            continue
    return btc_markets

btc_markets = get_btc_markets(0)

KEYWORD = "bitcoin"
print(f"Found {len(btc_markets)} closed & resolved markets matching '{KEYWORD}'")
 
def parse_token_ids(m):
    outcomes = m["outcomes"]
    if outcomes != '["Yes", "No"]':
        return None
    token_ids = m["clobTokenIds"]
    raw = "".join(token_ids)
    token_ids_unnested = json.loads(raw)
    return token_ids_unnested

all_btc_markets = []
for m in btc_markets:
    market_title = m["question"]
    type(parse_token_ids(m))
    token_ids = parse_token_ids(m)
    if token_ids is None:
        continue
    yes_token = token_ids[0]
    no_token  = token_ids[1]
    historic_price_url = "https://clob.polymarket.com/prices-history"

    hparams_yes  = {"market": yes_token, "interval": "max"}
    hparams_no   = {"market": no_token, "interval": "max"}
    yes_price_history = requests.get(historic_price_url, params=hparams_yes, timeout=30)
    no_price_history = requests.get(historic_price_url, params=hparams_no, timeout=30)
    if not yes_price_history.ok:
        continue
    if not no_price_history.ok:
        continue

    yes_data = yes_price_history.json()["history"]
    no_data  = no_price_history.json()["history"]
    yes = pd.DataFrame(yes_data)
    no = pd.DataFrame(no_data)

    if "t" not in yes.columns or "t" not in no.columns:
        #print(f"Skipping {market_title} missing 't' column in price history")
        continue
    merged_yes_no_df = yes.merge(no, on="t", suffixes=("_yes", "_no"))
    merged_yes_no_df.head()
    merged_yes_no_df['title'] = market_title
    print(merged_yes_no_df.head())
    
'''    
    for pt in data:
        rows.append({
            "market_id": m["id"],
            "slug": m.get("slug"),
            "question": m.get("question"),
            "yes_token": yes_token,
            "timestamp": pt["t"],
            "datetime_utc": datetime.utcfromtimestamp(pt["t"]).isoformat(),
            "yes_price": pt["p"]
        })

df = pd.DataFrame(rows)
print(df.head(10))
'''