"""
flow.py — เวอร์ชัน Prefect ของ ETL pipeline
===========================================
ไฟล์นี้ "ไม่เขียน logic ใหม่" — ยืมคลาส Extractor / Transformer / Loader
จาก etl_pipeline.py มาใช้ แล้วแค่เอา Prefect มา "ครอบ" เพื่อสั่งงาน (orchestrate)

แนวคิดสั้น ๆ 2 คำ:
  • @task  = 1 ขั้นงานย่อย (เช่น ดึง orders, แปลง daily_summary)  -> Prefect จับตา/retry/log ให้
  • @flow  = ตัวงานใหญ่ ที่เรียง task ต่อกัน (E -> T -> L)         -> คือ pipeline ทั้งอัน

ต้องลงก่อน:  pip install prefect pandas
รันครั้งเดียว:  python3 flow.py
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from prefect import flow, task, get_run_logger

# ยืมคลาสเดิมของเรามาใช้ซ้ำ (logic ทั้งหมดอยู่ที่ไฟล์นั้นเหมือนเดิม)
from etl_pipeline import Extractor, Transformer, Loader

# โฟลเดอร์ที่ไฟล์ flow.py นี้อยู่ (ใช้อ้างอิง path แบบ "ไม่ขึ้นกับตำแหน่งที่รัน")
# ป้องกันปัญหา: ถ้าใช้ path สัมพัทธ์แล้วรันผิดโฟลเดอร์ sqlite จะสร้าง db เปล่า -> "no such table"
BASE_DIR = Path(__file__).resolve().parent


# =================================================================== #
# EXTRACT — 3 task (ดึง 3 ตาราง)
# retries=2 หมายถึง ถ้า task นี้พัง จะลองใหม่อีก 2 ครั้ง ห่างกัน 5 วินาที
# (เผื่อ database ติด lock ชั่วคราว) — นี่คือข้อดีของ Prefect ที่เราได้ "ฟรี"
# =================================================================== #
@task(retries=2, retry_delay_seconds=5)
def extract_orders(db_path: str):
    logger = get_run_logger()                 # logger ของ Prefect (จะโผล่ใน UI ด้วย)
    df = Extractor(db_path).extract_orders()
    logger.info(f"ดึง orders ได้ {len(df)} แถว")
    return df


@task(retries=2, retry_delay_seconds=5)
def extract_customers(db_path: str):
    logger = get_run_logger()
    df = Extractor(db_path).extract_customers()
    logger.info(f"ดึง customers ได้ {len(df)} แถว")
    return df


@task(retries=2, retry_delay_seconds=5)
def extract_products(db_path: str):
    logger = get_run_logger()
    df = Extractor(db_path).extract_products()
    logger.info(f"ดึง products ได้ {len(df)} แถว")
    return df


# =================================================================== #
# TRANSFORM — 3 task (เรียกเมธอดใน Transformer เดิม)
# =================================================================== #
@task
def transform_daily_summary(orders, customers, products):
    logger = get_run_logger()
    result = Transformer(orders, customers, products).daily_summary()
    logger.info(f"daily_summary: {len(result)} แถว")
    return result


@task
def transform_top_customers(orders, customers, products):
    logger = get_run_logger()
    result = Transformer(orders, customers, products).top_customers()
    logger.info(f"top_customers: {len(result)} แถว")
    return result


@task
def transform_category_performance(orders, customers, products):
    logger = get_run_logger()
    result = Transformer(orders, customers, products).category_performance()
    logger.info(f"category_performance: {len(result)} แถว")
    return result


# =================================================================== #
# LOAD — task บันทึกไฟล์ (ใช้ Loader เดิม)
# =================================================================== #
@task
def load_csv(df, output_dir: str, filename: str):
    logger = get_run_logger()
    Loader(output_dir).save_csv(df, filename)
    logger.info(f"บันทึก {filename}")


# =================================================================== #
# FLOW — ตัว pipeline ใหญ่ ที่เรียง task ทั้งหมดต่อกัน
# =================================================================== #
@flow(name="ecommerce-etl")
def etl_flow(db_path: Optional[str] = None, output_dir: Optional[str] = None):
    logger = get_run_logger()

    # ถ้าไม่ได้ส่ง path มา ให้ใช้ไฟล์ที่อยู่ "ข้าง ๆ flow.py" เสมอ
    if db_path is None:
        db_path = str(BASE_DIR / "ecommerce.db")
    if output_dir is None:
        output_dir = str(BASE_DIR / "reports")

    # เช็กก่อนเลยว่าไฟล์ db มีจริงและไม่ว่าง ถ้าไม่ใช่ให้แจ้งชัด ๆ (จะได้ไม่งงกับ "no such table")
    db_file = Path(db_path)
    if not db_file.exists() or db_file.stat().st_size == 0:
        raise FileNotFoundError(
            f"ไม่พบฐานข้อมูลที่ใช้งานได้ที่ {db_path}\n"
            f"สร้างก่อนด้วย:  python scripts/generate_ecommerce_db.py --db \"{db_path}\""
        )

    logger.info(f"เริ่ม flow | ใช้ db: {db_path}")

    # ---- Extract ----
    # เรียกตรง ๆ แบบนี้ = ทำทีละอันตามลำดับ (อ่านง่าย เหมาะเริ่มต้น)
    orders = extract_orders(db_path)
    customers = extract_customers(db_path)
    products = extract_products(db_path)

    # ---- Transform ----
    daily = transform_daily_summary(orders, customers, products)
    top = transform_top_customers(orders, customers, products)
    cat = transform_category_performance(orders, customers, products)

    # ---- Load ----
    today = datetime.now().strftime("%Y-%m-%d")
    load_csv(daily, output_dir, f"daily_summary_{today}.csv")
    load_csv(top.reset_index(), output_dir, "top_customers.csv")
    load_csv(cat, output_dir, "category_performance.csv")

    logger.info("จบ flow เรียบร้อย")


# =================================================================== #
# จุดเริ่มทำงานเมื่อรัน:  python3 flow.py
# =================================================================== #
if __name__ == "__main__":
    # รันครั้งเดียวทันที
    etl_flow()

    # ---- ถ้าอยากตั้งให้รันเองทุกวัน 07:00 น. ให้คอมเมนต์บรรทัดบน แล้วเปิดบรรทัดล่าง ----
    # etl_flow.serve(name="daily-ecommerce-etl", cron="0 7 * * *")
    #   cron "0 7 * * *" = ทุกวัน เวลา 07:00
    #   คำสั่งนี้จะค้างไว้ (ไม่จบ) คอยรันตามเวลา — กด Ctrl+C เพื่อหยุด
