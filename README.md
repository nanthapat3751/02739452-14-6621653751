# ⚽ Football Multi-Source Data Pipeline

ข้อสอบปลายภาค 02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ (ภาคต้น 2569)

**หมวด:** กีฬา
**ขอบเขต:** ตารางคะแนน สถิติการแข่งขัน ผลประตู และอันดับดาวซัลโวของฟุตบอลลีกยุโรป (5 ลีกใหญ่) และไทยลีก (T1, T2)

ระบบรวบรวมข้อมูลจาก 3 แหล่งด้วย **Apache Airflow DAG** → จัดเก็บใน **PostgreSQL** → แสดงผลบน **เว็บ dashboard (Flask + Chart.js)** ที่อ่านจาก PostgreSQL โดยตรง

## แหล่งข้อมูล

| # | แหล่ง | วิธีดึง | ข้อมูลที่ได้ |
|---|------|--------|-------------|
| 1 | [Football-Data API](https://www.football-data.org/) (v4) | REST API (ต้องใช้ API key ฟรี) | ตารางคะแนน, ดาวซัลโว (Top 20), ผลการแข่งขันที่จบแล้ว ของ Premier League, La Liga, Serie A, Bundesliga, Ligue 1 |
| 2 | [Siamsport](https://www.siamsport.co.th/standings/football-international/premierleague/) | **Web scraping** HTML `<table>` ด้วย `requests` + `BeautifulSoup` | ตารางคะแนน 5 ลีกใหญ่ยุโรป + ไทยลีก 1 (ชื่อทีมภาษาไทย) |
| 3 | [Ballthai](https://www.ballthai.com/ตารางคะแนนไทยลีก/) | **Web scraping** ข้อมูล JSON ที่หน้าเว็บโหลดผ่าน JavaScript (`svc.ballthai.com`) ด้วย `requests` | ตารางคะแนน + ดาวซัลโว ไทยลีก 1 และไทยลีก 2 |

- ไม่ใช้ Selenium
- ตรวจ `robots.txt` แล้ว: siamsport.co.th และ ballthai.com เป็น `User-agent: * / Disallow:` (อนุญาตทั้งหมด). ดึงข้อมูลแบบหน่วงเวลา (sleep) ระหว่าง request และระบุ User-Agent
- **PDPA:** ไม่เก็บข้อมูลส่วนบุคคล เก็บเฉพาะชื่อทีม/ชื่อนักกีฬาอาชีพที่เผยแพร่สาธารณะและสถิติการแข่งขัน ไม่เก็บวันเกิด รูปภาพ หรือข้อมูลติดต่อ

## สถาปัตยกรรม

```
                 ┌─ TaskGroup football_data : extract[5 ลีก] (Dynamic Task Mapping) → transform → load ─┐
create_tables ──>├─ TaskGroup siamsport     : extract[5 ลีก] (Dynamic Task Mapping) → transform → load ─┤──> merge_dedup ──> quality_check ─┬─> dq_passed ──────────┬─> publish ─> record_run
                 └─ TaskGroup ballthai      : extract → transform → load ─────────────────────────────┘   (all_done)        (Branch)      └─> quarantine_bad_rows ┘
```

| ขั้น | รายละเอียด |
|-----|------------|
| **TaskGroup ×3** | 3 แหล่งรันขนานกัน แต่ละแหล่ง extract → transform → load ลงตาราง staging (`stg_standings`, `stg_scorers`, `stg_matches`) ด้วย **UPSERT** (`INSERT ... ON CONFLICT DO UPDATE`) |
| **Schema เดียวกัน** | ทุกแหล่งถูกแปลงเป็น schema เดียว: `league_code, season, team_name, position, played, won, draw, lost, goals_for, goals_against, goal_diff, points` |
| **แหล่งล่มแล้วไปต่อได้** | `merge_dedup` ใช้ `trigger_rule=all_done` และอ่านเฉพาะแถวของ `run_id` รอบนี้ → แหล่งที่ล่มไม่มีแถว แต่แหล่งที่เหลือยังถูกรวมและ publish ได้ (ทดสอบได้โดย Trigger DAG w/ config `{"fail_sources": ["siamsport"]}`) |
| **ตัดรายการซ้ำข้ามแหล่ง** | ทีมเดียวกันมาจากหลายแหล่ง: ไทยลีก 1 = Ballthai × Siamsport (`พีที ประจวบ เอฟซี` vs `ประจวบ เอฟซี`), ลีกยุโรป = Football-Data × Siamsport (`Man City` vs `แมนซิตี้`) → จับคู่ด้วย (1) ตาราง `team_alias` (2) ชื่อตรงกัน/ชื่อหนึ่งอยู่ในอีกชื่อ (3) สถิติตรงกันทุกค่า (4) อันดับ+แต้มตรงกัน แล้วรวมเป็นแถวเดียว (`sources = 'ballthai, siamsport'`) |
| **ตรวจคุณภาพ + แตกเส้นทาง** | `quality_check` (BranchPythonOperator) ตรวจ เช่น แข่ง = ชนะ+เสมอ+แพ้, ผลต่างประตู = ได้−เสีย, ค่าไม่ติดลบ, แต้ม ≤ 3×แข่ง, สกอร์ไม่ว่าง/ไม่ผิดปกติ, จำนวนแถวรวม ≥ 100 → ผ่านไป `dq_passed`, ไม่ผ่านไป `quarantine_bad_rows` (ย้ายแถวเสียไป `dq_quarantine`) แล้ว `publish` เฉพาะแถวที่ผ่าน |
| **UPSERT ปลายทาง** | `publish` UPSERT ลง `standings`, `top_scorers`, `matches` → รันซ้ำกี่ครั้งจำนวนแถวไม่เพิ่ม |
| **สถานะ pipeline** | `record_run` บันทึกสถานะแต่ละแหล่ง/ทุก task ลง `pipeline_runs` → แสดงที่หน้า "สถานะ Pipeline" บนเว็บ |

## โครงสร้างโปรเจกต์

```
├── dags/
│   ├── football_pipeline.py   # DAG
│   └── football_sources.py    # ฟังก์ชัน extract/transform ของแต่ละแหล่ง
├── web/
│   ├── app.py                 # Flask dashboard (query PostgreSQL โดยตรงด้วย psycopg2)
│   ├── templates/             # หน้าเว็บ (Jinja2) : หน้าแรก, ตารางคะแนน, ดาวซัลโว, ผลบอล, สถานะระบบ
│   ├── static/style.css
│   ├── Dockerfile
│   └── requirements.txt
├── Dockerfile                 # Airflow image + requirements.txt
├── docker-compose.yml
├── requirements.txt           # ไลบรารีของ DAG
└── .env.example
```

## วิธีรัน

ต้องมี Docker Desktop

```bash
git clone https://github.com/nanthapat3751/02739452-14-662165751.git
cd 02739452-14-662165751
cp .env.example .env        # แล้วใส่ FOOTBALL_DATA_API_KEY (สมัครฟรีที่ football-data.org) และตั้งรหัสผ่าน
docker compose up -d --build
```

| บริการ | URL | บัญชี |
|-------|-----|------|
| Airflow UI | http://localhost:8080 | ตาม `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` ใน `.env` |
| Dashboard | http://localhost:8501 | - |
| PostgreSQL (ข้อมูลฟุตบอล) | `localhost:5433` | ตาม `FOOTBALL_DB_*` ใน `.env` |

DAG `football_multi_source_pipeline` ถูก unpause อัตโนมัติและรันทุก 6 ชั่วโมง หรือกด ▶ Trigger DAG ใน Airflow UI ได้ทันที (รอบแรก ~2 นาที เพราะ Football-Data free tier จำกัด 10 requests/นาที)

> ถ้าไม่ใส่ `FOOTBALL_DATA_API_KEY` task ของ football_data จะ fail แต่ DAG ยังรวมข้อมูลจาก Siamsport + Ballthai ได้ (ตัวอย่างของกรณีแหล่งข้อมูลล่ม)

ตรวจ UPSERT (รันซ้ำแล้วจำนวนแถวไม่เพิ่ม):

```bash
docker compose exec football-db psql -U football -d football -c "select (select count(*) from standings) s, (select count(*) from top_scorers) t, (select count(*) from matches) m"
```

## Dashboard (http://localhost:8501)

ออกแบบโทนแดง-ดำ แบบเว็บข่าวกีฬา มีเมนูด้านบนแบ่งหน้า + แท็บเลือกลีก

| หน้า | เนื้อหา |
|-----|--------|
| **หน้าแรก** `/` | KPI (ทีม, ประตูรวม, ผลบอล, ดาวซัลโว, ทีมที่ยืนยันจาก 2 แหล่ง), ตารางคะแนนย่อทุกลีก, ดาวซัลโว Top 10, กราฟประตูเฉลี่ยต่อนัดแยกลีก, ผลบอลล่าสุด |
| **ตารางคะแนน** `/standings` | แท็บ 7 ลีก, ตารางคะแนนเต็ม (โซนอันดับ, แหล่งข้อมูลของแต่ละแถว), KPI จ่าฝูง/เกมรุก/เกมรับ, กราฟแต้ม, กราฟประตูได้-เสีย |
| **ดาวซัลโว** `/scorers` | ทุกลีก/แยกลีก, Top N, กราฟประตู+แอสซิสต์, ตาราง |
| **ผลบอล** `/matches` | ผลการแข่งขันแยกตามวัน, กรองผล (เจ้าบ้านชนะ/เสมอ/ทีมเยือนชนะ), กราฟประตูเฉลี่ยต่อ matchday, สัดส่วนผล |
| **สถานะระบบ** `/pipeline` | สถานะแต่ละแหล่งในรอบล่าสุด, ประวัติการรัน, จำนวนแถว, สถานะทุก task, แถวที่ไม่ผ่าน DQ (โบนัส) |

**ตัวกรอง** (ทุกหน้า): ฤดูกาล, แหล่งข้อมูล (football-data / siamsport / ballthai), ค้นหาชื่อทีม/นักเตะ
