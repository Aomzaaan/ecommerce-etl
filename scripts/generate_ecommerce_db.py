#!/usr/bin/env python3
"""
generate_ecommerce_db.py
========================
สร้าง mock database (SQLite) ชื่อ ecommerce.db สำหรับโค้ด ETL (Extractor/Transformer/Loader)

ตารางและคอลัมน์ ออกแบบให้ "ตรงกับที่โค้ดใช้":
  customers : customer_id, name, email, country, signup_date
  products  : product_id, name, category, price
  orders    : order_id, customer_id, product_id, quantity, unit_price, total,
              status, order_date

คอลัมน์ที่โค้ด ETL ใช้จริง:
  - orders    : order_id, customer_id, product_id, order_date, status, total
  - customers : customer_id, name
  - products  : product_id, category

ใช้ Python standard library ล้วน (sqlite3 + random) — ไม่ต้องลงอะไรเพิ่ม
reproducible ด้วย seed

วิธีรัน:
    python3 scripts/generate_ecommerce_db.py              # -> ecommerce.db
    python3 scripts/generate_ecommerce_db.py --db ecommerce.db --orders 6000 --seed 42
"""

import argparse
import random
import sqlite3
from datetime import datetime, timedelta

# ------------------------------------------------------------------ #
# ข้อมูลอ้างอิง
# ------------------------------------------------------------------ #
FIRST_NAMES = ["Somchai", "Suda", "Anan", "Nicha", "Peter", "Mary", "Hiro",
               "Yuki", "John", "Lisa", "Krit", "Ploy", "Arthit", "Mint",
               "David", "Emma", "Chen", "Wei", "Noi", "Fah"]
LAST_NAMES = ["Jaidee", "Srisuk", "Kittichai", "Wong", "Smith", "Tanaka",
              "Lee", "Brown", "Phan", "Rangsan", "Meesook",
              "Suwan", "Garcia", "Kim", "Nguyen"]
COUNTRIES = ["TH", "SG", "US", "JP", "GB", "AU", "DE", "VN"]
COUNTRY_WEIGHTS = [45, 10, 12, 8, 6, 5, 6, 8]

CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "grocery", "toys"]

# สถานะคำสั่งซื้อ และน้ำหนัก (completed เยอะสุด, cancelled/refunded มีพอให้คำนวณ cancel_rate)
STATUSES = ["completed", "shipped", "pending", "cancelled", "refunded"]
STATUS_WEIGHTS = [58, 14, 12, 11, 5]


def build_customers(rnd, n):
    rows = []
    for cid in range(1, n + 1):
        name = f"{rnd.choice(FIRST_NAMES)} {rnd.choice(LAST_NAMES)}"
        email = f"user{cid:04d}@example.com"
        country = rnd.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        signup = (datetime(2024, 1, 1) + timedelta(days=rnd.randint(0, 650))).strftime("%Y-%m-%d")
        rows.append((cid, name, email, country, signup))
    return rows


def build_products(rnd, n):
    rows = []
    for pid in range(1, n + 1):
        cat = CATEGORIES[(pid - 1) % len(CATEGORIES)]
        name = f"{cat.capitalize()} Item {pid:03d}"
        price = round(rnd.uniform(50, 6000), 2)
        rows.append((pid, name, cat, price))
    return rows


def build_orders(rnd, n_orders, n_customers, products, days, end_date):
    """สร้างคำสั่งซื้อ ย้อนหลัง `days` วัน จนถึง end_date."""
    rows = []
    # ลูกค้าบางคนซื้อบ่อยกว่า (power buyers) -> ให้ top_customers มีความหมาย
    cust_weights = [rnd.choices([1, 2, 5, 12], weights=[55, 25, 15, 5], k=1)[0]
                    for _ in range(n_customers)]
    cust_ids = list(range(1, n_customers + 1))

    for oid in range(1, n_orders + 1):
        customer_id = rnd.choices(cust_ids, weights=cust_weights, k=1)[0]
        product = rnd.choice(products)          # (product_id, name, category, price)
        product_id = product[0]
        unit_price = product[3]
        quantity = rnd.choices([1, 2, 3, 4, 5], weights=[50, 25, 13, 7, 5], k=1)[0]
        total = round(unit_price * quantity, 2)
        status = rnd.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

        # วันเวลา: สุ่มย้อนหลังในช่วง days วัน (มีทั้งในและนอกหน้าต่าง 30 วัน)
        day_offset = rnd.randint(0, days - 1)
        hour = rnd.randint(8, 23)
        minute = rnd.randint(0, 59)
        second = rnd.randint(0, 59)
        dt = end_date - timedelta(days=day_offset)
        order_date = dt.replace(hour=hour, minute=minute, second=second)
        rows.append((oid, customer_id, product_id, quantity, unit_price, total,
                     status, order_date.strftime("%Y-%m-%d %H:%M:%S")))
    return rows


def main():
    ap = argparse.ArgumentParser(description="Generate mock ecommerce SQLite database.")
    ap.add_argument("--db", default="ecommerce.db", help="path ไฟล์ database ที่จะสร้าง")
    ap.add_argument("--customers", type=int, default=300)
    ap.add_argument("--products", type=int, default=60)
    ap.add_argument("--orders", type=int, default=6000)
    ap.add_argument("--days", type=int, default=90, help="ช่วงวันย้อนหลังของ order_date")
    ap.add_argument("--end", default="2026-11-09", help="วันล่าสุดของคำสั่งซื้อ YYYY-MM-DD")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rnd = random.Random(args.seed)
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    customers = build_customers(rnd, args.customers)
    products = build_products(rnd, args.products)
    orders = build_orders(rnd, args.orders, args.customers, products, args.days, end_date)

    # --- เขียนลง SQLite ---
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    # ลบตารางเก่า (ถ้ามี) แล้วสร้างใหม่
    cur.executescript("""
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS customers;
        DROP TABLE IF EXISTS products;

        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT,
            country     TEXT,
            signup_date TEXT
        );
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            category   TEXT,
            price      REAL
        );
        CREATE TABLE orders (
            order_id    INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id  INTEGER,
            quantity    INTEGER,
            unit_price  REAL,
            total       REAL,
            status      TEXT,
            order_date  TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id)  REFERENCES products(product_id)
        );
    """)

    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", customers)
    cur.executemany("INSERT INTO products  VALUES (?,?,?,?)", products)
    cur.executemany("INSERT INTO orders    VALUES (?,?,?,?,?,?,?,?)", orders)
    conn.commit()

    # --- สรุปให้ดู ---
    print(f"สร้าง {args.db} สำเร็จ")
    print(f"  customers : {len(customers):>6,} แถว")
    print(f"  products  : {len(products):>6,} แถว")
    print(f"  orders    : {len(orders):>6,} แถว")
    print("  สถานะคำสั่งซื้อ:")
    for status, cnt in cur.execute(
            "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY COUNT(*) DESC"):
        print(f"    - {status:<10}: {cnt:,}")
    row = cur.execute("SELECT MIN(order_date), MAX(order_date) FROM orders").fetchone()
    print(f"  ช่วงวันที่ : {row[0]}  ->  {row[1]}")
    conn.close()


if __name__ == "__main__":
    main()
