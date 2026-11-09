# Mock Data — User Events / Clickstream

ข้อมูลจำลองสำหรับฝึกทำ data pipeline (Prefect orchestration + GitHub + Telegram/Email alerting)
สร้างด้วย `scripts/generate_mock_data.py` (Python stdlib ล้วน, reproducible ด้วย `seed=42`)

---

## 1. โครงสร้างโฟลเดอร์

```
mini_project/
├─ data/
│  ├─ raw/
│  │   ├─ events_2026-10-11.jsonl   ← 1 ไฟล์ = 1 วัน (JSON Lines)
│  │   ├─ events_2026-10-12.jsonl
│  │   └─ ... (รวม 30 ไฟล์)
│  └─ manifest.json                 ← ground truth: สถิติต่อวัน ใช้ cross-check ผล pipeline
└─ scripts/
   └─ generate_mock_data.py         ← รันใหม่/แก้ค่าได้ (--start --days --seed ฯลฯ)
```

- **รูปแบบไฟล์:** JSONL — 1 บรรทัด = 1 event (1 JSON object) เหมาะกับ incremental / streaming load
- **ช่วงเวลา:** 2026-10-11 ถึง 2026-11-09 (30 วัน)
- **ปริมาณ:** รวม ~24,100 events (~600–1,000 ต่อวัน, วันหยุด/เทรนด์ทำให้แกว่ง)
- **timezone:** ทุก timestamp เป็น **UTC** (ลงท้าย `Z`)

---

## 2. Schema (field ต่อ 1 event)

| field | type | คำอธิบาย |
|---|---|---|
| `event_id` | string | รหัส event เฉพาะ (`evt_...`) — **ใช้ dedup** (มี duplicate ตั้งใจใส่ไว้) |
| `event_timestamp` | string (ISO-8601, UTC) | เวลาที่เกิด event ฝั่ง client เช่น `2026-10-11T20:53:26.494Z` |
| `received_at` | string (ISO-8601, UTC) | เวลาที่ server รับ (มี ingestion lag 0.2–90s → ใช้ทดสอบ late-arriving) |
| `user_id` | string \| null | รหัสผู้ใช้ (`u_00001`) — **null = anonymous** (~8%) |
| `anonymous_id` | string | device/cookie id ติดตัวเสมอ (`anon_...`) ใช้ join กับ user ภายหลังได้ |
| `session_id` | string \| null | รหัส session (`s_...`) — รวม event ใน session เดียวกัน |
| `event_type` | string \| null | ประเภท event (ดูหัวข้อ 3) — **null = dirty row** |
| `page_url` | string | path ของหน้า เช่น `/products/p_0012` |
| `page_title` | string | ชื่อหน้า |
| `referrer` | string \| null | ที่มาของ traffic |
| `utm_source` / `utm_medium` / `utm_campaign` | string \| null | ข้อมูล marketing attribution (set ตอนเข้า session, ~45%) |
| `device_type` | string | `mobile` \| `desktop` \| `tablet` |
| `os` | string | iOS / Android / Windows / macOS / Linux / iPadOS |
| `browser` | string | Chrome / Safari / Firefox / Edge / Samsung Internet |
| `country` / `city` | string | geo (เอนไปทาง TH) |
| `app_version` | string | เวอร์ชันแอป เช่น `2.4.0` |
| `properties` | object | field เพิ่มเติมตาม `event_type` (ดูหัวข้อ 3) |

---

## 3. Event types และ `properties`

| event_type | ~สัดส่วน | `properties` ที่สำคัญ |
|---|---|---|
| `page_view` | สูงสุด | `product_id?`, `category?`, `load_time_ms` |
| `click` | สูง | `element`, `product_id?` |
| `search` | กลาง | `query`, `results_count` |
| `add_to_cart` | กลาง | `product_id`, `product_name`, `category`, `price`, `quantity`, `currency` |
| `remove_from_cart` | ต่ำ | `product_id`, `quantity`, `currency` |
| `checkout_start` | ต่ำ | `cart_value`, `items_count`, `currency` |
| `purchase` | ต่ำ | `order_id`, `amount`, `discount`, `items_count`, `payment_method`, `currency` |
| `login` / `logout` / `signup` | ต่ำ | `method` (email/google/facebook/line) |
| `error` | หายากมาก | `error_code`, `message`, `severity` (warning/critical) |

**Funnel ใน session** เรียงตามจริง: `page_view → search → page_view(product) → add_to_cart → checkout_start → purchase` (มี drop-off แต่ละขั้น) — เหมาะกับการทำ funnel / conversion analytics

---

## 4. Data quality issues (ตั้งใจใส่ไว้ให้ validate)

ข้อมูล "ไม่สะอาด 100%" โดยตั้งใจ เพื่อให้มีงานให้ pipeline ทำจริง (clean / validate / quarantine):

| ปัญหา | จำนวนโดยประมาณ | ควรจัดการยังไง |
|---|---|---|
| **Duplicate rows** (ซ้ำทั้ง `event_id`) | ~130 | dedup ด้วย `event_id` |
| **Null `event_type`** | ~61 | drop / ส่งเข้า quarantine |
| **Malformed `event_timestamp`** (ไม่มี `Z`, เป็น epoch, ว่าง, `not-a-date`) | ~80 | parse แบบ safe + log error |
| **Negative / zero `amount`** ใน purchase | ~29 | validate ค่า > 0 |
| **Null `session_id`** | ~34 | handle / flag |

> จำนวนจริงต่อวันดูได้ใน `data/manifest.json → days_detail[].quality_issues`

---

## 5. Anomaly days (สำหรับ demo แจ้งเตือน Telegram / Email)

วางไว้ 3 วัน เพื่อให้ระบบ alerting มีอะไรให้ "ยิง" จริง:

| วันที่ | ชนิด | สิ่งที่เกิดขึ้น | Alert ที่ควรติด |
|---|---|---|---|
| **2026-10-18** | `error_spike` | error เด้งขึ้น ~18 เท่า (22 errors วันเดียว) | error rate / count เกิน threshold |
| **2026-10-25** | `flash_sale` | traffic ~1.9x, conversion สูง, รายได้ ~฿290k | spike detection (traffic/revenue) |
| **2026-11-03** | `traffic_drop` | event เหลือ ~30% (319 rows), รายได้ **฿0** | volume drop / zero-revenue alert |

ไอเดีย threshold เริ่มต้น (ปรับได้): แจ้งเตือนเมื่อ error/วัน > 15, หรือ event/วัน หลุด ±40% จากค่าเฉลี่ย 7 วัน, หรือ revenue/วัน = 0

---

## 6. manifest.json (ground truth)

`data/manifest.json` เก็บสถิติที่ถูกต้องของข้อมูลที่ generate ไว้ ใช้ **เทียบผลลัพธ์ pipeline** ได้ตรง ๆ:

- `totals` — รวมทั้งหมด + `events_by_type`
- `days_detail[]` — ต่อวัน: `total_events`, `events_by_type`, `revenue_thb`, `quality_issues`, `anomaly`

---

## 7. ไอเดียต่อยอด (คุณทำเองได้เลย)

- **Ingest แบบ incremental:** 1 Prefect flow run ต่อ 1 ไฟล์/วัน (`events_{date}.jsonl`) — parametrize ด้วย date
- **Transform:** parse → dedup (`event_id`) → validate (timestamp/amount/session) → แยก clean vs quarantine
- **Load:** ลงฐานข้อมูล/parquet (เช่น `data/processed/`) หรือ table แยกตามวัน
- **Quality gate:** ถ้า invalid rate เกิน X% → fail task → ยิง alert
- **Alerting (Telegram/Email):** hook เข้ากับ Prefect state handler หรือ task ตรวจ anomaly เทียบ manifest/rolling average
- **GitHub:** commit โค้ด pipeline + `scripts/` (ข้อมูล raw ขนาดเล็กพอ push ได้ หรือจะใส่ `.gitignore` ให้ `data/` ก็ได้)

---

## 8. สร้างข้อมูลใหม่ / ปรับแต่ง

```bash
# ค่า default (เหมือนที่ให้มา)
python3 scripts/generate_mock_data.py --out data/raw --start 2026-10-11 --days 30 --seed 42

# เช่น อยากได้ 90 วัน หรือข้อมูลเยอะขึ้น
python3 scripts/generate_mock_data.py --days 90 --min-events 2000 --max-events 4000
```

เปลี่ยน `--seed` = ได้ชุดข้อมูลใหม่ (seed เดิม = ได้เหมือนเดิมเป๊ะ)
