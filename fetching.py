import requests, pandas as pd, time
from datetime import datetime
import json

url = "https://gamma-api.polymarket.com/markets"
LIMIT   = 500
params = {
    "closed": "true",
    "limit":  LIMIT,
    "order": "endDate",
    "ascending": "false"

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
        print(btc_markets)
        btc_markets.append(m)
    else:
        continue

print(f"Found {len(btc_markets)} closed & resolved markets matching '{KEYWORD}'")
 
def parse_token_ids(m):
    outcomes = m["outcomes"]
    if outcomes != '["Yes", "No"]':
        return None
    token_ids = m["clobTokenIds"]
    raw = "".join(token_ids)
    token_ids_unnested = json.loads(raw)
    return token_ids_unnested

rows = []
for m in btc_markets:
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
    yes_data = yes_price_history.json()
    no_data  = no_price_history.json()
    print(yes_data, no_data)

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