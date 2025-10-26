# Polymarket Bitcoin Odds Tracker

Fetches and analyzes old Polymarket prediction markets betting on bitcoin outcome to understand if odds x time before closing can be used to predict btc prices on the closing date.

Source for hypothesis: https://polymarket.com/accuracy


## Overview
This project uses the [Polymarket Gamma API](https://gamma-api.polymarket.com) and the [CLOB API](https://clob.polymarket.com) to:

- Search for markets containing a specific keyword (default: `"bitcoin"`)
- Retrieve each market’s CLOB token IDs (for YES/NO outcomes)
- Fetch the price history (implied probability over time)
- Output and optionally visualize how sentiment changes across time