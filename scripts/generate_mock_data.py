#!/usr/bin/env python3
"""
generate_mock_data.py
=====================
Mock user-event / clickstream data generator for a data pipeline project.

- Pure Python standard library (no external dependencies).
- Reproducible with a fixed seed.
- Writes one JSONL file per day:  data/raw/events_YYYY-MM-DD.jsonl
- Intentionally injects a controlled amount of "dirty" data so you can build
  real validation / cleaning steps.
- Includes a few "anomaly" days (error spike, traffic/revenue drop, flash sale)
  so you have something meaningful for Telegram / email alerting to fire on.
- Also writes data/manifest.json with per-day stats (ground truth) so you can
  verify your pipeline's output against it.

Usage:
    python3 generate_mock_data.py                      # defaults
    python3 generate_mock_data.py --out ./data/raw --start 2026-10-11 --days 30 --seed 42
"""

import argparse
import json
import os
import random
import string
import uuid
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------- #
# Reference data
# --------------------------------------------------------------------------- #

EVENT_TYPES = [
    "page_view", "click", "search", "add_to_cart", "remove_from_cart",
    "checkout_start", "purchase", "login", "logout", "signup", "error",
]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
OS_BY_DEVICE = {
    "mobile": ["iOS", "Android"],
    "tablet": ["iPadOS", "Android"],
    "desktop": ["Windows", "macOS", "Linux"],
}
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]

GEO = [
    ("TH", "Bangkok"), ("TH", "Chiang Mai"), ("TH", "Phuket"), ("TH", "Khon Kaen"),
    ("SG", "Singapore"), ("US", "New York"), ("US", "San Francisco"),
    ("JP", "Tokyo"), ("GB", "London"), ("AU", "Sydney"), ("DE", "Berlin"),
]
# Weighted toward Thailand so there is a clear "home" market
GEO_WEIGHTS = [34, 10, 8, 5, 10, 7, 5, 6, 4, 3, 3]

UTM_SOURCES = ["google", "facebook", "instagram", "tiktok", "email", "line", "direct"]
UTM_MEDIUMS = ["cpc", "organic", "social", "email", "referral", "none"]
CAMPAIGNS = ["11_11_sale", "new_arrivals", "retargeting", "welcome_series",
             "flash_sale", "loyalty", "none"]

PAYMENT_METHODS = ["credit_card", "promptpay", "cod", "bank_transfer", "wallet"]

ERROR_CODES = [
    ("E500", "internal_server_error", "critical"),
    ("E502", "bad_gateway", "critical"),
    ("E400", "bad_request", "warning"),
    ("E404", "not_found", "warning"),
    ("E_TIMEOUT", "request_timeout", "warning"),
    ("E_PAYMENT", "payment_gateway_declined", "critical"),
]

# Small product catalog
CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "grocery"]
PRODUCTS = []
for i in range(1, 61):
    cat = CATEGORIES[(i - 1) % len(CATEGORIES)]
    PRODUCTS.append({
        "product_id": f"p_{i:04d}",
        "product_name": f"{cat.capitalize()} Item {i:04d}",
        "category": cat,
        "price": round(random.Random(i).uniform(50, 5000), 2),
    })

APP_VERSIONS = ["2.3.1", "2.3.2", "2.4.0", "2.4.1", "3.0.0"]

STATIC_PAGES = ["/", "/home", "/search", "/cart", "/checkout", "/account",
                "/deals", "/categories", "/help"]

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def rand_id(prefix, n=12):
    return prefix + uuid.uuid4().hex[:n]


def iso(dt):
    """ISO-8601 with milliseconds and trailing Z (UTC)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{dt.microsecond // 1000:03d}Z"


# Hour-of-day weights (local-ish daily rhythm: quiet overnight, peaks midday & evening)
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2, 4, 7, 9, 10, 11, 12,
    12, 11, 10, 10, 11, 12, 14, 15, 13, 9, 5, 2,
]


def pick_hour(rnd):
    return rnd.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]


# --------------------------------------------------------------------------- #
# User population (stable across days)
# --------------------------------------------------------------------------- #

def build_users(rnd, n_users=300):
    users = []
    for i in range(1, n_users + 1):
        device = rnd.choice(DEVICE_TYPES)
        geo = rnd.choices(GEO, weights=GEO_WEIGHTS, k=1)[0]
        users.append({
            "user_id": f"u_{i:05d}",
            "anonymous_id": rand_id("anon_", 16),
            "device_type": device,
            "os": rnd.choice(OS_BY_DEVICE[device]),
            "browser": rnd.choice(BROWSERS),
            "country": geo[0],
            "city": geo[1],
            # engagement weight -> some users are far more active (power users)
            "weight": rnd.choices([1, 2, 5, 12], weights=[55, 25, 15, 5], k=1)[0],
        })
    return users


# --------------------------------------------------------------------------- #
# Session / event generation
# --------------------------------------------------------------------------- #

def new_session_context(rnd, users, anon_prob=0.08):
    """Return the shared attributes for one session."""
    if rnd.random() < anon_prob:
        device = rnd.choice(DEVICE_TYPES)
        geo = rnd.choices(GEO, weights=GEO_WEIGHTS, k=1)[0]
        ctx = {
            "user_id": None,
            "anonymous_id": rand_id("anon_", 16),
            "device_type": device,
            "os": rnd.choice(OS_BY_DEVICE[device]),
            "browser": rnd.choice(BROWSERS),
            "country": geo[0],
            "city": geo[1],
        }
    else:
        u = rnd.choices(users, weights=[x["weight"] for x in users], k=1)[0]
        ctx = {
            "user_id": u["user_id"],
            "anonymous_id": u["anonymous_id"],
            "device_type": u["device_type"],
            "os": u["os"],
            "browser": u["browser"],
            "country": u["country"],
            "city": u["city"],
        }

    ctx["session_id"] = rand_id("s_", 16)
    ctx["app_version"] = rnd.choices(APP_VERSIONS, weights=[5, 10, 25, 35, 25], k=1)[0]

    # Acquisition / UTM is set at session entry (~45% of sessions carry campaign info)
    if rnd.random() < 0.45:
        ctx["utm_source"] = rnd.choice(UTM_SOURCES)
        ctx["utm_medium"] = rnd.choice(UTM_MEDIUMS)
        ctx["utm_campaign"] = rnd.choice(CAMPAIGNS)
        ctx["referrer"] = rnd.choice([
            "https://www.google.com/", "https://www.facebook.com/",
            "https://l.instagram.com/", "https://t.co/", None,
        ])
    else:
        ctx["utm_source"] = None
        ctx["utm_medium"] = None
        ctx["utm_campaign"] = None
        ctx["referrer"] = None
    return ctx


def make_event(ctx, event_type, ts, properties, page_url, page_title):
    """Build a single clean event dict in canonical field order."""
    # ingestion lag: 0.2s .. ~90s after the event occurred
    lag_ms = random.randint(200, 90000)
    received = ts + timedelta(milliseconds=lag_ms)
    return {
        "event_id": rand_id("evt_", 12),
        "event_timestamp": iso(ts),
        "received_at": iso(received),
        "user_id": ctx["user_id"],
        "anonymous_id": ctx["anonymous_id"],
        "session_id": ctx["session_id"],
        "event_type": event_type,
        "page_url": page_url,
        "page_title": page_title,
        "referrer": ctx["referrer"],
        "utm_source": ctx["utm_source"],
        "utm_medium": ctx["utm_medium"],
        "utm_campaign": ctx["utm_campaign"],
        "device_type": ctx["device_type"],
        "os": ctx["os"],
        "browser": ctx["browser"],
        "country": ctx["country"],
        "city": ctx["city"],
        "app_version": ctx["app_version"],
        "properties": properties,
    }


def generate_session(rnd, ctx, start_ts, purchase_boost=1.0, error_boost=1.0):
    """Generate a realistic ordered list of events for a single session."""
    events = []
    ts = start_ts

    def advance():
        nonlocal ts
        ts = ts + timedelta(seconds=rnd.randint(2, 180),
                            milliseconds=rnd.randint(0, 999))

    # 1) Entry event
    entry_roll = rnd.random()
    if entry_roll < 0.08:
        method = rnd.choice(["email", "google", "facebook", "line"])
        events.append(make_event(ctx, "login", ts, {"method": method}, "/login", "Login"))
    elif entry_roll < 0.11 and ctx["user_id"]:
        method = rnd.choice(["email", "google"])
        events.append(make_event(ctx, "signup", ts, {"method": method}, "/signup", "Create account"))
    else:
        page = rnd.choice(["/", "/home", "/deals"])
        events.append(make_event(ctx, "page_view", ts,
                                 {"load_time_ms": rnd.randint(120, 2600)},
                                 page, "Home"))
    advance()

    # 2) Optional search
    if rnd.random() < 0.55:
        q = rnd.choice(["shoes", "iphone case", "t-shirt", "coffee maker",
                        "lipstick", "yoga mat", "snacks", "headphones",
                        "monitor", "backpack"])
        events.append(make_event(ctx, "search", ts,
                                 {"query": q, "results_count": rnd.randint(0, 240)},
                                 "/search", f"Search: {q}"))
        advance()

    # 3) Browse products (1..5 product page views, with clicks)
    viewed = []
    for _ in range(rnd.randint(1, 5)):
        prod = rnd.choice(PRODUCTS)
        viewed.append(prod)
        events.append(make_event(ctx, "page_view", ts,
                                 {"product_id": prod["product_id"],
                                  "category": prod["category"],
                                  "load_time_ms": rnd.randint(120, 3000)},
                                 f"/products/{prod['product_id']}",
                                 prod["product_name"]))
        advance()
        if rnd.random() < 0.4:
            events.append(make_event(ctx, "click", ts,
                                     {"element": rnd.choice(
                                         ["image-zoom", "size-selector",
                                          "reviews-tab", "related-product"]),
                                      "product_id": prod["product_id"]},
                                     f"/products/{prod['product_id']}",
                                     prod["product_name"]))
            advance()

    # 4) Funnel: add_to_cart -> checkout_start -> purchase (with drop-off)
    cart = []
    if viewed and rnd.random() < 0.45:
        for prod in rnd.sample(viewed, k=rnd.randint(1, len(viewed))):
            qty = rnd.randint(1, 3)
            cart.append((prod, qty))
            events.append(make_event(ctx, "add_to_cart", ts,
                                     {"product_id": prod["product_id"],
                                      "product_name": prod["product_name"],
                                      "category": prod["category"],
                                      "price": prod["price"],
                                      "quantity": qty,
                                      "currency": "THB"},
                                     "/cart", "Shopping cart"))
            advance()
            # occasional remove_from_cart
            if rnd.random() < 0.12:
                events.append(make_event(ctx, "remove_from_cart", ts,
                                         {"product_id": prod["product_id"],
                                          "quantity": qty, "currency": "THB"},
                                         "/cart", "Shopping cart"))
                advance()
                cart.pop()

        if cart and rnd.random() < 0.6:
            cart_value = round(sum(p["price"] * q for p, q in cart), 2)
            items_count = sum(q for _, q in cart)
            events.append(make_event(ctx, "checkout_start", ts,
                                     {"cart_value": cart_value,
                                      "items_count": items_count,
                                      "currency": "THB"},
                                     "/checkout", "Checkout"))
            advance()

            conv_prob = min(0.98, 0.55 * purchase_boost)
            if rnd.random() < conv_prob:
                discount = round(cart_value * rnd.choice([0, 0, 0, 0.05, 0.1, 0.15]), 2)
                amount = round(cart_value - discount, 2)
                events.append(make_event(ctx, "purchase", ts,
                                         {"order_id": rand_id("ord_", 10),
                                          "amount": amount,
                                          "discount": discount,
                                          "items_count": items_count,
                                          "payment_method": rnd.choice(PAYMENT_METHODS),
                                          "currency": "THB"},
                                         "/checkout/success", "Order confirmed"))
                advance()

    # 5) Random error events (rare, boosted on anomaly days)
    if rnd.random() < min(0.95, 0.015 * error_boost):
        code, msg, sev = rnd.choice(ERROR_CODES)
        events.append(make_event(ctx, "error", ts,
                                 {"error_code": code, "message": msg, "severity": sev},
                                 rnd.choice(STATIC_PAGES), "Error"))
        advance()

    # 6) Occasional explicit logout
    if ctx["user_id"] and rnd.random() < 0.15:
        events.append(make_event(ctx, "logout", ts, {}, "/account", "Account"))

    return events


# --------------------------------------------------------------------------- #
# Dirty-data injection (documented, counted)
# --------------------------------------------------------------------------- #

def inject_quality_issues(rnd, events):
    """Mutate a copy of the day's events to add realistic data-quality problems.
    Returns (new_events, issue_counts)."""
    counts = {
        "duplicate_rows": 0,
        "null_event_type": 0,
        "malformed_timestamp": 0,
        "negative_amount": 0,
        "missing_session_id": 0,
    }
    out = list(events)

    n = len(out)

    # a) duplicate rows (exact dupes incl. event_id) ~0.6%
    for _ in range(max(1, int(n * 0.006))):
        out.append(json.loads(json.dumps(rnd.choice(events))))
        counts["duplicate_rows"] += 1

    # b) null event_type ~0.3%
    for ev in rnd.sample(out, k=max(1, int(len(out) * 0.003))):
        ev["event_type"] = None
        counts["null_event_type"] += 1

    # c) malformed timestamps ~0.4%
    for ev in rnd.sample(out, k=max(1, int(len(out) * 0.004))):
        ev["event_timestamp"] = rnd.choice([
            ev["event_timestamp"].replace("T", " ").replace("Z", ""),  # space, no Z
            str(int(datetime.now(timezone.utc).timestamp())),          # epoch seconds
            "",                                                          # empty
            "not-a-date",
        ])
        counts["malformed_timestamp"] += 1

    # d) negative / zero amounts on some purchases
    purchases = [ev for ev in out if ev.get("event_type") == "purchase"]
    for ev in rnd.sample(purchases, k=max(1, int(len(purchases) * 0.03))) if purchases else []:
        ev["properties"]["amount"] = rnd.choice([-ev["properties"]["amount"], 0])
        counts["negative_amount"] += 1

    # e) missing session_id ~0.2%
    for ev in rnd.sample(out, k=max(1, int(len(out) * 0.002))):
        ev["session_id"] = None
        counts["missing_session_id"] += 1

    return out, counts


# --------------------------------------------------------------------------- #
# Anomaly schedule (relative to start date) -> keyed by YYYY-MM-DD at runtime
# --------------------------------------------------------------------------- #

def build_anomaly_schedule(start_date, days):
    """Place a few anomalies at fixed offsets within the window."""
    sched = {}
    def put(offset, **kw):
        if 0 <= offset < days:
            d = (start_date + timedelta(days=offset)).strftime("%Y-%m-%d")
            sched[d] = kw
    put(7,  kind="error_spike",   error_boost=18.0, note="Deploy gone wrong: error rate spikes")
    put(14, kind="flash_sale",    volume_mult=1.9, purchase_boost=1.7, note="11.11 flash sale: traffic + conversion up")
    put(23, kind="traffic_drop",  volume_mult=0.3, purchase_boost=0.4, note="Outage / tracking broken: volume + revenue drop")
    return sched


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Generate mock clickstream JSONL data.")
    parser.add_argument("--out", default="data/raw", help="output directory for daily JSONL files")
    parser.add_argument("--start", default="2026-10-11", help="start date YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=30, help="number of days")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument("--min-events", type=int, default=600, help="base min events/day")
    parser.add_argument("--max-events", type=int, default=1000, help="base max events/day")
    args = parser.parse_args()

    rnd = random.Random(args.seed)
    random.seed(args.seed)  # for module-level random used in make_event lag

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    os.makedirs(args.out, exist_ok=True)

    users = build_users(rnd)
    anomalies = build_anomaly_schedule(start_date, args.days)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "seed": args.seed,
        "start_date": args.start,
        "days": args.days,
        "n_users": len(users),
        "n_products": len(PRODUCTS),
        "schema_version": "1.0",
        "days_detail": [],
        "totals": {},
    }

    grand_total = 0
    grand_by_type = {}

    for offset in range(args.days):
        day = start_date + timedelta(days=offset)
        day_str = day.strftime("%Y-%m-%d")
        anom = anomalies.get(day_str)

        # weekly seasonality: weekends ~15% lighter; slight upward trend over window
        weekday = day.weekday()  # 0=Mon
        seasonal = 0.85 if weekday >= 5 else 1.0
        trend = 1.0 + 0.004 * offset
        volume_mult = (anom.get("volume_mult", 1.0) if anom else 1.0)
        purchase_boost = (anom.get("purchase_boost", 1.0) if anom else 1.0)
        error_boost = (anom.get("error_boost", 1.0) if anom else 1.0)

        target = int(rnd.randint(args.min_events, args.max_events)
                     * seasonal * trend * volume_mult)

        day_events = []
        while len(day_events) < target:
            ctx = new_session_context(rnd, users)
            hour = pick_hour(rnd)
            start_ts = datetime(day.year, day.month, day.day, hour,
                                rnd.randint(0, 59), rnd.randint(0, 59),
                                rnd.randint(0, 999) * 1000, tzinfo=timezone.utc)
            day_events.extend(
                generate_session(rnd, ctx, start_ts,
                                 purchase_boost=purchase_boost,
                                 error_boost=error_boost)
            )

        # inject quality issues
        day_events, issue_counts = inject_quality_issues(rnd, day_events)

        # events arrive out of order in reality -> shuffle lightly
        rnd.shuffle(day_events)

        # write JSONL
        path = os.path.join(args.out, f"events_{day_str}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for ev in day_events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")

        # stats (ground truth)
        by_type = {}
        revenue = 0.0
        for ev in day_events:
            et = ev.get("event_type")
            by_type[et] = by_type.get(et, 0) + 1
            if et == "purchase":
                amt = ev.get("properties", {}).get("amount", 0) or 0
                if isinstance(amt, (int, float)) and amt > 0:
                    revenue += amt
            grand_by_type[et] = grand_by_type.get(et, 0) + 1
        grand_total += len(day_events)

        manifest["days_detail"].append({
            "date": day_str,
            "weekday": day.strftime("%a"),
            "file": f"events_{day_str}.jsonl",
            "total_events": len(day_events),
            "events_by_type": by_type,
            "revenue_thb": round(revenue, 2),
            "quality_issues": issue_counts,
            "anomaly": (anom.get("kind") if anom else None),
            "anomaly_note": (anom.get("note") if anom else None),
        })

        flag = f"  [ANOMALY: {anom['kind']}]" if anom else ""
        print(f"{day_str} ({day.strftime('%a')}): {len(day_events):>5} events, "
              f"revenue ฿{revenue:,.0f}{flag}")

    manifest["totals"] = {
        "total_events": grand_total,
        "events_by_type": grand_by_type,
        "total_files": args.days,
    }

    manifest_path = os.path.join(os.path.dirname(args.out.rstrip("/")) or ".",
                                 "manifest.json")
    # place manifest at data/manifest.json (parent of raw) when out ends with /raw
    parent = os.path.dirname(os.path.normpath(args.out))
    manifest_path = os.path.join(parent if parent else ".", "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print("-" * 60)
    print(f"TOTAL: {grand_total:,} events across {args.days} files")
    print(f"By type: {json.dumps(grand_by_type, ensure_ascii=False)}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
