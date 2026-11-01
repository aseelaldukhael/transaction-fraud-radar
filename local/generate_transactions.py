"""
Synthetic Saudi card-transaction generator (for the fraud-detection pipeline).

IMPORTANT: all data here is SIMULATED. No real customers or bank data.
Fraud is injected on purpose, so the true labels are known and we can
measure how good the detection rules are.

Outputs (in --out folder):
  transactions.csv   what a bank stream would contain (NO fraud label)
  labels.csv         ground truth: txn_id, is_fraud, fraud_pattern (for evaluation only)

Run:
  python generate_transactions.py --customers 2000 --days 7 --seed 42
"""
import argparse, csv, math, random
from datetime import datetime, timedelta
from pathlib import Path

CITIES = {  # name: (lat, lon, weight)
    "Riyadh": (24.7136, 46.6753, 30), "Jeddah": (21.4858, 39.1925, 20),
    "Dammam": (26.4207, 50.0888, 8), "Khobar": (26.2172, 50.1971, 6),
    "Makkah": (21.3891, 39.8579, 8), "Madinah": (24.5247, 39.5692, 7),
    "Abha": (18.2164, 42.5053, 4), "Tabuk": (28.3835, 36.5662, 4),
    "Buraydah": (26.3260, 43.9750, 5), "Hail": (27.5114, 41.7208, 3),
}
# category: (weight, median amount SAR, channel weights POS/online/ATM)
CATS = {
    "groceries": (22, 120, (0.9, 0.1, 0)), "restaurants": (20, 70, (0.85, 0.15, 0)),
    "fuel": (14, 90, (1, 0, 0)), "retail": (12, 250, (0.7, 0.3, 0)),
    "pharmacy": (7, 80, (0.9, 0.1, 0)), "transport": (6, 35, (0.6, 0.4, 0)),
    "online_services": (7, 60, (0, 1, 0)), "electronics": (3, 900, (0.5, 0.5, 0)),
    "jewelry": (1.5, 1500, (0.9, 0.1, 0)), "cash_withdrawal": (7.5, 500, (0, 0, 1)),
}
RISKY = ["electronics", "jewelry", "online_services"]

def km(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2-la1)/2)**2 + math.cos(la1)*math.cos(la2)*math.sin((lo2-lo1)/2)**2
    return 6371*2*math.asin(math.sqrt(h))

def wchoice(rng, d):
    keys = list(d); return rng.choices(keys, weights=[d[k][-1] if isinstance(d[k], tuple) and len(d[k])==3 and k in CITIES else d[k][0] for k in keys])[0]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--fraud-rate", type=float, default=0.003, help="chance per customer-day of a fraud event")
    ap.add_argument("--start", default="2026-10-01")
    ap.add_argument("--out", default="data")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    start = datetime.fromisoformat(a.start)

    city_names = list(CITIES); city_w = [CITIES[c][2] for c in city_names]
    cat_names = list(CATS); cat_w = [CATS[c][0] for c in cat_names]
    merchants, n = {}, 0
    for c in city_names:
        for k in cat_names:
            merchants[(c, k)] = [f"M{n+i:06d}" for i in range(15)]; n += 15

    customers = []
    for i in range(a.customers):
        home = rng.choices(city_names, city_w)[0]
        fav = []
        while len(fav) < 4:   # favourite categories, picked by popularity
            x = rng.choices(cat_names, cat_w)[0]
            if x not in fav: fav.append(x)
        customers.append(dict(
            cid=f"C{i:05d}", card=f"CARD{i:05d}", home=home,
            scale=rng.lognormvariate(0, 0.35),            # personal spending level
            rate=rng.choice([0.6, 1, 1.5, 2.5, 4]),       # txns per day
            fav=fav, peak=rng.choice([13, 18, 20, 21])))  # usual busy hour

    rows, labels = [], {}
    tid = 0
    events = {}
    cur = {"id": None}
    def add(ts, c, cat, city, amt, chan, fraud=None):
        nonlocal tid
        tid += 1
        lat, lon, _ = CITIES[city]
        t = f"T{tid:08d}"
        m = rng.choice(merchants[(city, cat)])
        rows.append((ts, t, c["card"], c["cid"], m, cat, city,
                     round(lat + rng.uniform(-.05, .05), 5), round(lon + rng.uniform(-.05, .05), 5),
                     round(amt, 2), chan))
        labels[t] = fraud
        events[t] = cur["id"] if fraud else ""

    def legit_amount(c, cat):
        return max(1.0, rng.lognormvariate(math.log(CATS[cat][1]*c["scale"]), 0.55))
    def channel(cat):
        w = CATS[cat][2]; return rng.choices(["POS", "online", "ATM"], w)[0]

    for day in range(a.days):
        d0 = start + timedelta(days=day)
        for c in customers:
            today = c["home"] if rng.random() > 0.04 else rng.choices(city_names, city_w)[0]   # 4%: travelling that day
            k = sum(1 for _ in range(int(c["rate"]*3)) if rng.random() < 1/3)   # ~Poisson(rate)
            for _ in range(k):
                hr = int(min(23, max(6, rng.gauss(c["peak"], 4)))) 
                ts = d0 + timedelta(hours=hr, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
                cat = rng.choice(c["fav"]) if rng.random() < 0.7 else rng.choices(cat_names, cat_w)[0]
                city = today
                add(ts, c, cat, city, legit_amount(c, cat), channel(cat))
            # ---- injected fraud events ----
            if rng.random() < a.fraud_rate:
                pat = rng.choice(["high_amount", "velocity", "impossible_travel", "card_testing"])
                cur["id"] = f"E{len(set(v for v in events.values() if v))+1:04d}"
                base = d0 + timedelta(hours=rng.choice([1, 2, 3, 4, 14, 22, 23]), minutes=rng.randint(0, 59))
                if pat == "high_amount":
                    cat = rng.choice(RISKY)
                    add(base, c, cat, c["home"], c["scale"]*rng.uniform(3000, 9000), channel(cat), pat)
                elif pat == "velocity":
                    for j in range(rng.randint(6, 10)):
                        add(base + timedelta(seconds=j*rng.randint(8, 18)), c, rng.choice(["online_services", "retail", "electronics"]),
                            c["home"], rng.uniform(120, 700), "online", pat)
                elif pat == "impossible_travel":
                    far = [x for x in city_names if km(CITIES[x], CITIES[c["home"]]) > 600]
                    far = far or [x for x in city_names if x != c["home"]]
                    add(base, c, "groceries", c["home"], legit_amount(c, "groceries"), "POS")      # genuine one
                    fc = rng.choice(["electronics", "jewelry", "retail"])
                    add(base + timedelta(minutes=rng.randint(4, 15)), c, fc, rng.choice(far),
                        rng.uniform(600, 4000), channel(fc), pat)
                else:  # card_testing: tiny amounts, then a big one
                    for j in range(rng.randint(4, 7)):
                        add(base + timedelta(seconds=j*rng.randint(10, 25)), c, "online_services", c["home"], rng.uniform(1, 5), "online", pat)
                    add(base + timedelta(minutes=2), c, "electronics", c["home"], rng.uniform(1500, 5000), "online", pat)

    rows.sort(key=lambda r: (r[0], r[1]))
    with open(out/"transactions.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_time", "txn_id", "card_id", "customer_id", "merchant_id", "category", "city", "lat", "lon", "amount_sar", "channel"])
        for r in rows: w.writerow([r[0].isoformat(timespec="seconds")] + list(r[1:]))
    with open(out/"labels.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["txn_id", "is_fraud", "fraud_pattern", "fraud_event"])
        for r in rows:
            p = labels[r[1]]; w.writerow([r[1], int(p is not None), p or "", events[r[1]]])
    nf = sum(1 for v in labels.values() if v)
    print(f"transactions: {len(rows):,} | fraud: {nf} ({nf/len(rows):.2%}) | saved in {out}/")

if __name__ == "__main__":
    main()
