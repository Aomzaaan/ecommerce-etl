import pandas as pd
import sqlite3
import os
import json
from datetime import datetime


class Extractor:
    def __init__(self, db_path):
        self.db_path = db_path

    def extract_orders(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql("SELECT * FROM orders", conn)
        conn.close()
        return df

    def extract_customers(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql("SELECT * FROM customers", conn)
        conn.close()
        return df

    def extract_products(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql("SELECT * FROM products", conn)
        conn.close()
        return df


class Transformer:
    def __init__(self, orders, customers, products):
        self.orders = orders
        self.customers = customers
        self.products = products
        self.orders["order_date"] = pd.to_datetime(self.orders["order_date"])

    def daily_summary(self):
        latest_date = self.orders["order_date"].max()
        start_date = latest_date - pd.Timedelta(days=30)
        last_30_days = self.orders[
            (self.orders["order_date"] > start_date)
            & (self.orders["order_date"] <= latest_date)
        ]
        last_30_days_nc = last_30_days[last_30_days["status"] != "completed"].copy()
        return (
            last_30_days_nc.groupby(self.orders["order_date"].dt.date)["total"]
            .sum()
            .reset_index()
        )

    def top_customers(self, n=10):
        or_cus = self.orders.merge(self.customers, on="customer_id")
        or_cus_com = or_cus[or_cus["status"] == "completed"].copy()
        return (
            or_cus_com.groupby(["customer_id", "name"])["total"]
            .sum()
            .sort_values(ascending=False)
            .head(n)
        )

    def category_performance(self):
        merged = self.orders.merge(self.products, on="product_id")
        completed = merged[merged["status"] == "completed"]

        sales = completed.groupby("category")["total"].sum().rename("sales")
        order_count = (
            merged.groupby("category")["order_id"].nunique().rename("order_count")
        )
        cancel_rate = (
            merged.groupby("category")
            .apply(lambda g: (g["status"] == "cancelled").mean() * 100)
            .rename("cancel_rate")
        )

        return pd.concat([sales, order_count, cancel_rate], axis=1).reset_index()


class Loader:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def save_csv(self, df, filename):
        path = os.path.join(self.output_dir, filename)
        df.to_csv(path, index=False, encoding="utf-8-sig")

    def save_json(self, df, filename):
        path = os.path.join(self.output_dir, filename)
        df.to_json(path, orient="records", force_ascii=False, indent=2)


class Pipeline:
    def __init__(self, db_path, output_dir):
        self.extractor = Extractor(db_path)
        self.loader = Loader(output_dir)
        self.log_file = os.path.join(output_dir, "pipeline_log.txt")

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} | {message}\n"
        print(line, end="")
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(line)

    def run(self):
        start = datetime.now()
        self.log("START pipeline")

        try:
            # Extract
            orders = self.extractor.extract_orders()
            self.log(f"Extracted {len(orders)} orders")

            customers = self.extractor.extract_customers()
            self.log(f"Extracted {len(customers)} customers")

            products = self.extractor.extract_products()
            self.log(f"Extracted {len(products)} products")

            # Transform
            transformer = Transformer(orders, customers, products)

            daily = transformer.daily_summary()
            self.log(f"Transformed daily_summary: {len(daily)} rows")

            top = transformer.top_customers()
            self.log(f"Transformed top_customers: {len(top)} rows")

            cat = transformer.category_performance()
            self.log(f"Transformed category_performance: {len(cat)} rows")

            # Load
            today = datetime.now().strftime("%Y-%m-%d")
            self.loader.save_csv(daily, f"daily_summary_{today}.csv")
            self.log(f"Saved daily_summary_{today}.csv")

            self.loader.save_csv(top.reset_index(), "top_customers.csv")
            self.log("Saved top_customers.csv")

            self.loader.save_csv(cat, "category_performance.csv")
            self.log("Saved category_performance.csv")

            duration = (datetime.now() - start).total_seconds()
            self.log(f"END pipeline (took {duration:.1f}s)\n")

        except Exception as e:
            self.log(f"ERROR: {e}")
            raise


if __name__ == "__main__":
    pipeline = Pipeline("ecommerce.db", "reports")
    pipeline.run()
