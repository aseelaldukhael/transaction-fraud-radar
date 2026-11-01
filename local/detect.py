"""Run the rules over transactions.csv in time order -> alerts.csv  (batch replay of the stream)."""
import csv, json, sys
from datetime import datetime
from collections import defaultdict
from rules import CardState, check

src = sys.argv[1] if len(sys.argv) > 1 else "data/transactions.csv"
out = sys.argv[2] if len(sys.argv) > 2 else "data/alerts.csv"
states = defaultdict(CardState)
n = a = 0
with open(src, encoding="utf-8") as f, open(out, "w", newline="", encoding="utf-8") as g:
    w = csv.writer(g); w.writerow(["txn_id", "event_time", "card_id", "rule", "details"])
    for r in csv.DictReader(f):
        n += 1
        ts = datetime.fromisoformat(r["event_time"])
        for rule, det in check(states[r["card_id"]], ts, float(r["lat"]), float(r["lon"]), float(r["amount_sar"])):
            w.writerow([r["txn_id"], r["event_time"], r["card_id"], rule, json.dumps(det)]); a += 1
print(f"transactions read: {n:,} | alerts: {a:,}")
