# E-commerce ETL Pipeline (Prefect)

Mini data pipeline แบบ ETL (Extract → Transform → Load) อ่านข้อมูลจาก SQLite
(`ecommerce.db`) สรุปเป็นรายงาน CSV แล้วสั่งงานด้วย **Prefect**

---

## สิ่งที่ต้องติดตั้งบนเครื่อง (Prerequisites)

1. **Python 3.12 (แนะนำ)** — https://www.python.org/downloads/
   - ตอนติดตั้งบน Windows ให้ **ติ๊ก "Add Python to PATH"** ด้วย
   - ⚠️ **อย่าใช้ Python เวอร์ชันล่าสุดสุด (เช่น 3.15)** เพราะ `pandas`/`prefect` ยังทำ wheel ไม่ทัน
     จะเจอ error `Failed building wheel for pyyaml` — ใช้ 3.12 จะชัวร์สุด (ดู Troubleshooting ท้ายไฟล์)
2. **Git** — https://git-scm.com/downloads
3. **VS Code** — https://code.visualstudio.com/
   - แนะนำลง extension **"Python"** (ของ Microsoft) ใน VS Code

เช็กว่าลงแล้ว (เปิด terminal / PowerShell):
```bash
python --version      # ควรได้ 3.10+
git --version
```

---

## ติดตั้งโปรเจกต์ (ครั้งแรกบนเครื่องใหม่)

```bash
# 1) ดึงโค้ดจาก GitHub
git clone https://github.com/<ชื่อคุณ>/<ชื่อ repo>.git
cd <ชื่อ repo>

# 2) สร้าง virtual environment (ห้องแยก lib ของโปรเจกต์นี้)
python -m venv .venv

# 3) เปิดใช้งาน venv
#    Windows (PowerShell):
.venv\Scripts\Activate.ps1
#    Windows (cmd):
#    .venv\Scripts\activate.bat
#    Mac / Linux:
#    source .venv/bin/activate

# 4) ติดตั้ง library ที่โปรเจกต์ใช้
pip install -r requirements.txt
```

> ถ้า PowerShell ฟ้องว่ารัน Activate.ps1 ไม่ได้ ให้รันครั้งเดียว:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` แล้วตอบ `Y`

---

## วิธีรัน

```bash
# ถ้ายังไม่มี ecommerce.db (เช่น เลือกไม่ commit db ขึ้น git) ให้สร้างก่อน:
python scripts/generate_ecommerce_db.py --db ecommerce.db

# รัน pipeline ด้วย Prefect
python flow.py
```

ผลลัพธ์จะอยู่ในโฟลเดอร์ `reports/`:
- `daily_summary_YYYY-MM-DD.csv`
- `top_customers.csv`
- `category_performance.csv`

รันแบบไม่ผ่าน Prefect (โค้ด ETL ล้วน ๆ) ก็ได้: `python etl_pipeline.py`

---

## โครงสร้างโปรเจกต์

```
mini_project/
├─ etl_pipeline.py    # คลาส ETL: Extractor / Transformer / Loader / Pipeline
├─ flow.py            # เวอร์ชัน Prefect (ยืมคลาสจาก etl_pipeline มาใช้)
├─ ecommerce.db       # ฐานข้อมูลตัวอย่าง (SQLite)
├─ scripts/
│  └─ generate_ecommerce_db.py   # สร้าง/รีเฟรช ecommerce.db
├─ reports/           # ผลลัพธ์ (สร้างใหม่ได้เสมอ -> อยู่ใน .gitignore)
├─ requirements.txt   # รายการ library
└─ .gitignore
```

---

## ตั้งเวลาให้รันเอง (ออปชัน)

ใน `flow.py` ท้ายไฟล์ สลับมาใช้:
```python
etl_flow.serve(name="daily-ecommerce-etl", cron="0 7 * * *")   # ทุกวัน 07:00
```

เปิดหน้า Dashboard ของ Prefect (อีก terminal):
```bash
prefect server start        # แล้วเปิด http://127.0.0.1:4200
```

---

## Troubleshooting

### `Failed building wheel for pyyaml` (ตอน pip install)
เกิดจากใช้ **Python ใหม่เกินไป** (เช่น 3.15) ที่ library ยังไม่มี wheel สำเร็จรูป วิธีแก้:

```powershell
# 1) ลบ venv เก่า แล้วสร้างใหม่ด้วย Python 3.12
Remove-Item -Recurse -Force .venv
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
python --version                     # ต้องขึ้น 3.12.x

# 2) อัปเดต pip แล้วลงใหม่
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### `no such table: orders` (ตอนรัน flow)
แปลว่าหา `ecommerce.db` ไม่เจอ หรือไฟล์ว่าง — สร้างใหม่ด้วย:
```bash
python scripts/generate_ecommerce_db.py --db ecommerce.db
```

### PowerShell รัน `Activate.ps1` ไม่ได้ (ฟ้องสีแดง)
รันครั้งเดียวแล้วตอบ `Y`:
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```
