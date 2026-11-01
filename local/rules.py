"""
Fraud rules. Written as a small stateful function: it sees ONE transaction at a time
(in time order) plus a little memory per card. This is exactly how a streaming job works,
so the same logic can run inside Spark Structured Streaming later.
"""
import math
from collections import deque
from datetime import datetime

# ---- thresholds (easy to tune, and to explain in an interview) ----
HIGH_MULT = 8          # amount > 8x the card's usual amount ...
HIGH_MIN_SAR = 3000    # ... and at least this much (tuned: 1500 gave 335 false alarms)
NEW_CARD_LIMIT = 3000  # card with little history: flag above this amount
VELOCITY_N, VELOCITY_SEC = 5, 120      # 5 transactions within 2 minutes
TRAVEL_KMH = 900       # faster than a plane = impossible
TRAVEL_MIN_KM = 300
TEST_SMALL_SAR, TEST_N, TEST_SEC = 10, 3, 300   # 3 tiny payments within 5 minutes
TEST_BIG_SAR, TEST_FOLLOW_SEC = 1000, 600       # then a big one within 10 minutes

def km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    h = math.sin((p2-p1)/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(math.radians(lon2-lon1)/2)**2
    return 6371*2*math.asin(math.sqrt(h))

class CardState:
    __slots__ = ("amounts", "recent", "last", "small", "last_small_alert")
    def __init__(self):
        self.amounts = deque(maxlen=30)   # last 30 legit-looking amounts
        self.recent = deque()             # timestamps of the last few minutes
        self.last = None                  # (ts, lat, lon)
        self.small = deque()              # timestamps of tiny payments
        self.last_small_alert = None

def check(state, ts: datetime, lat, lon, amount):
    """Return a list of (rule, details-dict) for this transaction, and update the card memory."""
    hits = []
    t = ts.timestamp()

    # 1) unusually large amount
    if len(state.amounts) >= 3:
        usual = sorted(state.amounts)[len(state.amounts)//2]
        if amount >= HIGH_MIN_SAR and amount > HIGH_MULT*usual:
            hits.append(("high_amount", {"amount": amount, "usual": round(usual, 1)}))
    elif amount >= NEW_CARD_LIMIT:
        hits.append(("high_amount", {"amount": amount, "usual": None}))

    # 2) many transactions in a short time
    state.recent.append(t)
    while state.recent and t - state.recent[0] > VELOCITY_SEC:
        state.recent.popleft()
    if len(state.recent) >= VELOCITY_N:
        hits.append(("velocity", {"count": len(state.recent), "seconds": VELOCITY_SEC}))

    # 3) impossible travel
    if state.last:
        t0, la0, lo0 = state.last
        d = km(la0, lo0, lat, lon); hrs = max((t-t0)/3600, 1/3600)
        if d >= TRAVEL_MIN_KM and d/hrs > TRAVEL_KMH:
            hits.append(("impossible_travel", {"km": round(d), "minutes": round((t-t0)/60, 1)}))
    state.last = (t, lat, lon)

    # 4) card testing: tiny payments, then a big one
    if amount <= TEST_SMALL_SAR:
        state.small.append(t)
        while state.small and t - state.small[0] > TEST_SEC:
            state.small.popleft()
        if len(state.small) >= TEST_N:
            hits.append(("card_testing", {"small_count": len(state.small)}))
            state.last_small_alert = t
    elif amount >= TEST_BIG_SAR and state.last_small_alert and t - state.last_small_alert <= TEST_FOLLOW_SEC:
        hits.append(("card_testing", {"followed_by_big": amount}))

    # remember normal-looking amounts only (so fraud does not teach the card to be "normal")
    if not hits:
        state.amounts.append(amount)
    return hits
