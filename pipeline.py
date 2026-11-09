"""
pipeline.py — Daily clickstream ETL pipeline (เวอร์ชันเต็ม ใช้งานได้จริง)
========================================================================
ไฟล์นี้ใช้ library มาตรฐานของ Python ล้วน ๆ (ไม่ต้องลงอะไรเพิ่ม)

แนวคิด: 1 ครั้งที่รัน = ประมวลผลข้อมูล 1 วัน (ไฟล์ events_YYYY-MM-DD.jsonl 1 ไฟล์)
ลำดับการทำงาน:  read_data()  ->  verify_data()  ->  load_data()

วิธีลองรัน (ใน terminal):
    python3 pipeline.py                      # รันวันตัวอย่างที่ตั้งไว้ด้านล่าง
หรือเรียกจากโค้ดอื่น / Prefect:
    from datetime import date
    EventPipeline(run_date=date(2026, 10, 18)).run()
"""

import json
import os
import smtplib
import urllib.parse
import urllib.request
import logging
from datetime import date, datetime
from email.mime.text import MIMEText
from pathlib import Path

# ตั้งค่า "logging" = ระบบพิมพ์ข้อความบอกสถานะ (เหมือน print แต่มีเวลา/ระดับความสำคัญให้)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("pipeline")

# รายชื่อ event_type ที่เรา "รู้จัก" ไว้ใช้ตรวจว่าแถวไหนมี type แปลกปลอม
KNOWN_EVENT_TYPES = {
    "page_view", "click", "search", "add_to_cart", "remove_from_cart",
    "checkout_start", "purchase", "login", "logout", "signup", "error",
}


class EventPipeline:
    """
    คลาสสำหรับประมวลผลข้อมูล clickstream รายวัน
    (อ่าน -> ตรวจ -> บันทึก)
    """

    def __init__(
        self,
        run_date: date,                       # วันที่ของข้อมูลที่จะประมวลผล
        raw_dir: str = "data/raw",            # โฟลเดอร์ไฟล์ดิบ (input)
        processed_dir: str = "data/processed",# โฟลเดอร์เก็บแถว "ดี" (output)
        quarantine_dir: str = "data/quarantine", # โฟลเดอร์เก็บแถว "เสีย"
        max_invalid_ratio: float = 0.05,      # ถ้าแถวเสียเกิน 5% -> แจ้งเตือน
        max_error_events: int = 15,           # ถ้ามี event "error" เกิน 15 -> แจ้งเตือน
        min_events_per_day: int = 200,        # ถ้าข้อมูลน้อยกว่านี้ -> แจ้งเตือน (traffic drop)
    ) -> None:
        # __init__ คือเมธอดที่ทำงานตอน "สร้าง" object ขึ้นมา
        # หน้าที่ของมันคือ "จำ" ค่าต่าง ๆ ไว้ใน self เพื่อให้เมธอดอื่นเรียกใช้ได้
        self.run_date = run_date
        self.date_str = run_date.strftime("%Y-%m-%d")   # แปลงวันที่เป็นข้อความ เช่น "2026-10-18"
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.quarantine_dir = Path(quarantine_dir)
        self.max_invalid_ratio = max_invalid_ratio
        self.max_error_events = max_error_events
        self.min_events_per_day = min_events_per_day

        # path ของไฟล์ input สำหรับวันนี้ เช่น data/raw/events_2026-10-18.jsonl
        self.input_file = self.raw_dir / f"events_{self.date_str}.jsonl"

    # =================================================================== #
    # STEP 1 — READ : อ่านไฟล์ JSONL ของวันนั้น
    # =================================================================== #
    def read_data(self) -> list[dict]:
        """
        อ่านไฟล์ของวันนี้ แล้วคืนค่าเป็น list ของ event (แต่ละตัวเป็น dict)

        JSONL = ไฟล์ที่ 1 บรรทัดคือ 1 JSON object
        เราจึงอ่านทีละบรรทัด แล้วแปลงข้อความ -> dict ด้วย json.loads()
        """
        # ถ้าไม่มีไฟล์ของวันนี้ ให้หยุดและแจ้ง error ทันที
        if not self.input_file.exists():
            raise FileNotFoundError(f"ไม่พบไฟล์ข้อมูล: {self.input_file}")

        rows: list[dict] = []   # กล่องว่างไว้เก็บ event ทุกตัว

        # เปิดไฟล์ (with ... as f จะปิดไฟล์ให้อัตโนมัติเมื่อทำงานเสร็จ)
        with open(self.input_file, encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):   # วนอ่านทีละบรรทัด (นับเลขบรรทัดด้วย)
                line = line.strip()                        # ตัดช่องว่าง/ขึ้นบรรทัดใหม่ท้ายบรรทัด
                if not line:                               # ข้ามบรรทัดว่าง
                    continue
                try:
                    rows.append(json.loads(line))          # แปลงข้อความ JSON -> dict แล้วเก็บ
                except json.JSONDecodeError:
                    # ถ้าบรรทัดนั้นไม่ใช่ JSON ที่ถูกต้อง เราข้ามไปและเตือนไว้
                    log.warning("บรรทัด %d อ่านไม่ได้ (ไม่ใช่ JSON) — ข้าม", line_no)

        log.info("READ : อ่านได้ %d แถว จาก %s", len(rows), self.input_file.name)
        return rows

    # =================================================================== #
    # STEP 2 — VERIFY : ตรวจและคัดแยกแถวดี/เสีย
    # =================================================================== #
    def verify_data(self, rows: list[dict]) -> dict:
        """
        เดินดูทุกแถว ตัดสินว่าแต่ละแถว "ผ่าน" หรือ "ไม่ผ่าน"
        คืนค่าเป็น dict: {"valid": [...], "invalid": [...], "stats": {...}}
          - valid   = แถวที่ดี พร้อมใช้งานต่อ
          - invalid = แถวที่มีปัญหา (แนบเหตุผลไว้ด้วย)
          - stats   = สรุปตัวเลขไว้ดูภาพรวม
        """
        valid: list[dict] = []
        invalid: list[dict] = []
        seen_ids: set = set()   # เก็บ event_id ที่เจอแล้ว ไว้ตรวจว่า "ซ้ำ" หรือไม่

        for row in rows:
            # _find_problem คืน None ถ้าแถวดี, คืน "ข้อความเหตุผล" ถ้าแถวเสีย
            problem = self._find_problem(row, seen_ids)
            if problem is None:
                valid.append(row)
                seen_ids.add(row["event_id"])             # จดว่า id นี้ใช้แล้ว
            else:
                # เก็บแถวเสีย พร้อมเหตุผล เพื่อย้อนดูทีหลัง
                invalid.append({"reason": problem, "row": row})

        # --- รวมสถิติไว้ดูภาพรวม ---
        total = len(valid) + len(invalid)
        # นับว่าแต่ละเหตุผลเกิดกี่ครั้ง
        reasons: dict[str, int] = {}
        for item in invalid:
            reasons[item["reason"]] = reasons.get(item["reason"], 0) + 1
        # นับจำนวน event แต่ละชนิด (เฉพาะแถวดี)
        by_type: dict[str, int] = {}
        for row in valid:
            t = row.get("event_type")
            by_type[t] = by_type.get(t, 0) + 1
        # รวมยอดขายของวันนี้ (เฉพาะ purchase ที่ amount ถูกต้อง)
        revenue = 0.0
        for row in valid:
            if row.get("event_type") == "purchase":
                revenue += row["properties"]["amount"]

        stats = {
            "total": total,
            "valid": len(valid),
            "invalid": len(invalid),
            "invalid_ratio": (len(invalid) / total) if total else 0.0,
            "invalid_reasons": reasons,
            "events_by_type": by_type,
            "revenue": round(revenue, 2),
        }

        log.info("VERIFY: ดี %d / เสีย %d (%.2f%%) | รายได้ ฿%s",
                 len(valid), len(invalid), stats["invalid_ratio"] * 100,
                 f"{revenue:,.0f}")
        return {"valid": valid, "invalid": invalid, "stats": stats}

    def _find_problem(self, row: dict, seen_ids: set) -> str | None:
        """
        ตรวจ 1 แถว ว่ามีปัญหาอะไรไหม
        - คืน None  = แถวนี้ดี
        - คืน str   = แถวนี้เสีย (ข้อความคือเหตุผล)
        (เครื่องหมาย _ หน้าชื่อ = ธรรมเนียมบอกว่าเป็นเมธอด "ภายใน" ใช้ช่วยงานเมธอดอื่น)
        """
        # 1) ซ้ำ: ถ้า event_id นี้เคยเจอแล้ว = แถวซ้ำ
        event_id = row.get("event_id")
        if event_id in seen_ids:
            return "duplicate_event_id"

        # 2) ข้อมูลจำเป็นต้องมีและห้ามว่าง/None
        for field in ("event_id", "event_timestamp", "event_type", "session_id"):
            if row.get(field) in (None, ""):
                return f"missing_{field}"

        # 3) เวลาต้องอยู่ในรูปแบบที่ถูกต้อง
        if not self._is_valid_timestamp(row["event_timestamp"]):
            return "bad_timestamp"

        # 4) ชนิด event ต้องเป็นชนิดที่เรารู้จัก
        if row["event_type"] not in KNOWN_EVENT_TYPES:
            return "unknown_event_type"

        # 5) ถ้าเป็นการซื้อ (purchase) ยอดเงินต้องเป็นตัวเลขและมากกว่า 0
        if row["event_type"] == "purchase":
            amount = row.get("properties", {}).get("amount")
            if not isinstance(amount, (int, float)) or amount <= 0:
                return "invalid_amount"

        return None   # ผ่านทุกข้อ = แถวดี

    @staticmethod
    def _is_valid_timestamp(ts) -> bool:
        """
        เช็กว่าข้อความเวลา ถูกต้องไหม (ต้องเป็นแบบ 2026-10-18T20:53:26.494Z)
        @staticmethod = เมธอดที่ไม่ต้องใช้ self (เป็นฟังก์ชันช่วยเฉย ๆ)
        """
        if not isinstance(ts, str) or not ts.endswith("Z"):
            return False
        try:
            # แปลง "...Z" ให้ Python เข้าใจ แล้วลอง parse ดู ถ้าแปลงได้ = ถูกต้อง
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False

    # =================================================================== #
    # STEP 3 — LOAD : บันทึกผลลง processed / quarantine
    # =================================================================== #
    def load_data(self, verified: dict) -> dict:
        """
        เอาผลจากขั้น verify มาบันทึกลงไฟล์
          - แถวดี  -> data/processed/events_YYYY-MM-DD.jsonl
          - แถวเสีย -> data/quarantine/invalid_YYYY-MM-DD.jsonl
        (ในงานจริงจะเปลี่ยนเป็น insert ลงฐานข้อมูล / เขียน parquet ก็ได้ แค่แก้ตรงนี้)
        """
        valid = verified["valid"]
        invalid = verified["invalid"]

        # สร้างโฟลเดอร์ปลายทางถ้ายังไม่มี (parents=True สร้างโฟลเดอร์แม่ให้ด้วย)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

        processed_file = self.processed_dir / f"events_{self.date_str}.jsonl"
        quarantine_file = self.quarantine_dir / f"invalid_{self.date_str}.jsonl"

        # เขียนแถวดี ทีละบรรทัด (กลับเป็น JSONL เหมือนเดิม)
        with open(processed_file, "w", encoding="utf-8") as f:
            for row in valid:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        # เขียนแถวเสีย (เก็บทั้งเหตุผลและตัวข้อมูล)
        with open(quarantine_file, "w", encoding="utf-8") as f:
            for item in invalid:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        result = {
            "rows_loaded": len(valid),
            "rows_quarantined": len(invalid),
            "processed_file": str(processed_file),
            "quarantine_file": str(quarantine_file),
        }
        log.info("LOAD  : บันทึก %d แถว -> %s | กักไว้ %d แถว",
                 len(valid), processed_file.name, len(invalid))
        return result

    # =================================================================== #
    # RUN : สั่งทำงานครบทั้ง 3 ขั้น + ด่านตรวจคุณภาพ
    # =================================================================== #
    def run(self) -> dict:
        """
        รันทั้ง pipeline สำหรับ 1 วัน
        (ตอนต่อ Prefect: เอาแต่ละ step ไปห่อเป็น @task แล้วเรียกเรียงกันใน @flow)
        """
        log.info("=== เริ่ม pipeline วันที่ %s ===", self.date_str)

        rows = self.read_data()               # ขั้น 1
        verified = self.verify_data(rows)     # ขั้น 2

        # ด่านตรวจคุณภาพ: ถ้าผิดปกติ จะยิงแจ้งเตือน (แต่ยังทำงานต่อ)
        self._check_quality_gate(verified)

        result = self.load_data(verified)     # ขั้น 3
        log.info("=== จบ pipeline วันที่ %s ===", self.date_str)
        return result

    def _check_quality_gate(self, verified: dict) -> None:
        """
        ด่านตรวจคุณภาพ: เทียบตัวเลขกับเกณฑ์ที่ตั้งไว้ ถ้าผิดปกติให้แจ้งเตือน
        (นี่คือจุดที่เชื่อมกับ anomaly 3 วันในข้อมูลตัวอย่าง)
        """
        stats = verified["stats"]

        # 1) แถวเสียเยอะเกินเกณฑ์
        if stats["invalid_ratio"] > self.max_invalid_ratio:
            self._send_alert(
                "ข้อมูลเสียเกินเกณฑ์",
                f"วัน {self.date_str}: แถวเสีย {stats['invalid_ratio']*100:.1f}% "
                f"(เกณฑ์ {self.max_invalid_ratio*100:.0f}%)",
            )

        # 2) error เยอะผิดปกติ (เช่นวัน 2026-10-18 ที่ error พุ่ง)
        n_error = stats["events_by_type"].get("error", 0)
        if n_error > self.max_error_events:
            self._send_alert(
                "พบ error เยอะผิดปกติ",
                f"วัน {self.date_str}: มี error {n_error} ครั้ง (เกณฑ์ {self.max_error_events})",
            )

        # 3) ข้อมูลน้อยผิดปกติ = อาจ traffic drop (เช่นวัน 2026-11-03)
        if stats["total"] < self.min_events_per_day:
            self._send_alert(
                "ปริมาณข้อมูลตก",
                f"วัน {self.date_str}: มีแค่ {stats['total']} แถว "
                f"(ปกติควร >= {self.min_events_per_day})",
            )

        # 4) ไม่มีรายได้เลยทั้งที่มีข้อมูล = น่าสงสัย
        if stats["revenue"] == 0 and stats["total"] > 0:
            self._send_alert(
                "รายได้เป็น 0",
                f"วัน {self.date_str}: ไม่มียอดขายเลย ทั้งที่มีข้อมูล {stats['total']} แถว",
            )

    # =================================================================== #
    # ALERT : แจ้งเตือนผ่าน Telegram และ/หรือ Email
    # =================================================================== #
    def _send_alert(self, subject: str, message: str) -> None:
        """
        ยิงแจ้งเตือน
        - จะพิมพ์ log เสมอ
        - ถ้าตั้งค่า "ตัวแปรสภาพแวดล้อม" (environment variable) ไว้ จะส่งจริงด้วย

        Telegram ต้องตั้ง:  TELEGRAM_BOT_TOKEN , TELEGRAM_CHAT_ID
        Email ต้องตั้ง:     SMTP_HOST , SMTP_USER , SMTP_PASS , ALERT_EMAIL_TO
        (วิธีตั้ง env var ไม่ต้องแก้โค้ด เช่นบน Windows PowerShell:  $env:TELEGRAM_BOT_TOKEN="xxxx")
        """
        full_text = f"🚨 [{self.date_str}] {subject}\n{message}"
        log.warning("ALERT : %s | %s", subject, message)

        self._send_telegram(full_text)
        self._send_email(subject=f"[Pipeline Alert] {subject}", body=full_text)

    @staticmethod
    def _send_telegram(text: str) -> None:
        """ส่งข้อความเข้า Telegram (ใช้ urllib ของ Python ไม่ต้องลง library)"""
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            return  # ยังไม่ได้ตั้งค่า -> ข้ามไป (ไม่ error)

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10) as resp:
                resp.read()
            log.info("ALERT : ส่ง Telegram สำเร็จ")
        except Exception as e:   # noqa: BLE001  (จับทุก error กันไม่ให้ pipeline ล้มเพราะส่งเตือนไม่ได้)
            log.error("ALERT : ส่ง Telegram ไม่สำเร็จ: %s", e)

    @staticmethod
    def _send_email(subject: str, body: str) -> None:
        """ส่งอีเมลแจ้งเตือนผ่าน SMTP (ใช้ smtplib ของ Python ไม่ต้องลง library)"""
        host = os.getenv("SMTP_HOST")
        user = os.getenv("SMTP_USER")
        password = os.getenv("SMTP_PASS")
        to_addr = os.getenv("ALERT_EMAIL_TO")
        if not all([host, user, password, to_addr]):
            return  # ยังไม่ได้ตั้งค่า -> ข้ามไป

        msg = MIMEText(body, _charset="utf-8")
        msg["Subject"] = subject
        msg["From"] = user
        msg["To"] = to_addr
        try:
            port = int(os.getenv("SMTP_PORT", "587"))
            with smtplib.SMTP(host, port, timeout=15) as server:
                server.starttls()
                server.login(user, password)
                server.send_message(msg)
            log.info("ALERT : ส่ง Email สำเร็จ")
        except Exception as e:   # noqa: BLE001
            log.error("ALERT : ส่ง Email ไม่สำเร็จ: %s", e)


# =================================================================== #
# ส่วนนี้จะทำงานเมื่อรันไฟล์ตรง ๆ ด้วย  python3 pipeline.py
# =================================================================== #
if __name__ == "__main__":
    # ลองรัน 1 วัน (เปลี่ยนวันที่ได้ตามใจ)
    pipe = EventPipeline(run_date=date(2026, 10, 18))   # วันนี้เป็นวัน error spike
    summary = pipe.run()
    print("\nสรุปผล:", json.dumps(summary, ensure_ascii=False, indent=2))
