"""
ฟังก์ชันดึงข้อมูล (extract) และแปลงข้อมูล (transform) ของแต่ละแหล่ง
แยกจากไฟล์ DAG เพื่อให้ทดสอบนอก Airflow ได้ (python football_sources.py)

แหล่งข้อมูล
1) Football-Data API (api.football-data.org/v4)     : ตารางคะแนน / ดาวซัลโว / ผลการแข่งขัน 5 ลีกใหญ่ยุโรป
2) Siamsport (www.siamsport.co.th/standings/...)     : web scraping ตาราง HTML ตารางคะแนน 5 ลีกใหญ่ยุโรป
3) Ballthai (www.ballthai.com)                       : ตารางคะแนน + ดาวซัลโว ไทยลีก 1/2
   หน้าเว็บ ballthai โหลดข้อมูลด้วย JavaScript จาก svc.ballthai.com จึงอ่าน JSON endpoint
   ที่หน้าเว็บเรียกใช้โดยตรง (ไม่ใช้ Selenium)

ไม่เก็บข้อมูลส่วนบุคคล (PDPA): ไม่เก็บวันเกิด/รูปภาพของนักเตะ เก็บเฉพาะชื่อที่เผยแพร่สาธารณะ + สถิติการแข่งขัน
"""
from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (compatible; KU-DataPlatform-Student/1.0; "
    "+https://github.com/) python-requests"
)
TIMEOUT = 30

# ลีกยุโรปที่ใช้ร่วมกันระหว่าง Football-Data และ Siamsport
# league_code: (football-data code, siamsport slug, ชื่อแสดงผล)
EURO_LEAGUES = {
    "EPL": ("PL", "premierleague", "Premier League"),
    "LALIGA": ("PD", "laliga", "La Liga"),
    "SERIEA": ("SA", "serie-a", "Serie A"),
    "BUNDESLIGA": ("BL1", "bundesliga", "Bundesliga"),
    "LIGUE1": ("FL1", "ligue1", "Ligue 1"),
}
THAI_LEAGUES = {
    "T1": ("t1", "Thai League 1"),
    "T2": ("t2", "Thai League 2"),
}
LEAGUE_NAMES = {k: v[2] for k, v in EURO_LEAGUES.items()} | {k: v[1] for k, v in THAI_LEAGUES.items()}
# หน้า standings ของ Siamsport (มีทั้ง 5 ลีกยุโรป และไทยลีก 1)
SS_LEAGUES = {k: f"football-international/{v[1]}" for k, v in EURO_LEAGUES.items()} | {
    "T1": "football-thailand/thaileague-1"}


def season_label(start_year: int) -> str:
    return f"{start_year}/{(start_year + 1) % 100:02d}"


def current_season() -> str:
    now = datetime.now(timezone.utc)
    return season_label(now.year if now.month >= 7 else now.year - 1)


def _get(url: str, headers: dict | None = None, params: dict | None = None, retries: int = 3):
    h = {"User-Agent": USER_AGENT}
    h.update(headers or {})
    last = None
    for attempt in range(retries):
        r = requests.get(url, headers=h, params=params, timeout=TIMEOUT)
        if r.status_code == 429:  # rate limit -> รอแล้วลองใหม่
            wait = int(r.headers.get("X-RequestCounter-Reset", 20)) + 1
            time.sleep(min(wait, 65))
            last = r
            continue
        r.raise_for_status()
        return r
    last.raise_for_status()


def _int(v, default=0):
    try:
        return int(str(v).strip().replace("+", ""))
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# 1) Football-Data API
# ---------------------------------------------------------------------------
FD_BASE = "https://api.football-data.org/v4"


def fd_extract(league_code: str) -> dict:
    """ดึง standings + scorers + matches (FINISHED) ของ 1 ลีก (3 requests, free tier 10 req/min)"""
    token = os.environ.get("FOOTBALL_DATA_API_KEY", "").strip()
    if not token:
        raise RuntimeError("ไม่ได้ตั้งค่า FOOTBALL_DATA_API_KEY ใน .env")
    code = EURO_LEAGUES[league_code][0]
    hdr = {"X-Auth-Token": token}
    out = {"league_code": league_code}
    out["standings"] = _get(f"{FD_BASE}/competitions/{code}/standings", hdr).json()
    time.sleep(6.5)
    out["scorers"] = _get(f"{FD_BASE}/competitions/{code}/scorers", hdr, {"limit": 20}).json()
    time.sleep(6.5)
    out["matches"] = _get(f"{FD_BASE}/competitions/{code}/matches", hdr, {"status": "FINISHED"}).json()
    time.sleep(6.5)
    return out


def fd_transform(raw_list: list[dict]) -> dict:
    standings, scorers, matches = [], [], []
    for raw in raw_list:
        if not raw:
            continue
        lg = raw["league_code"]
        st = raw["standings"]
        season = season_label(int(st["season"]["startDate"][:4]))
        total = next((s for s in st["standings"] if s.get("type") == "TOTAL"), st["standings"][0])
        for row in total["table"]:
            t = row["team"]
            standings.append({
                "source": "football-data", "league_code": lg, "season": season,
                "source_team_id": str(t["id"]),
                "team_name": t.get("shortName") or t.get("name"),
                "position": row["position"], "played": row["playedGames"],
                "won": row["won"], "draw": row["draw"], "lost": row["lost"],
                "goals_for": row["goalsFor"], "goals_against": row["goalsAgainst"],
                "goal_diff": row["goalDifference"], "points": row["points"],
            })
        for s in raw["scorers"].get("scorers", []):
            p, t = s["player"], s["team"]
            scorers.append({
                "source": "football-data", "league_code": lg, "season": season,
                "source_player_id": str(p["id"]), "player_name": p["name"],
                "team_name": t.get("shortName") or t.get("name"),
                "nationality": p.get("nationality"), "position": p.get("section") or p.get("position"),
                "goals": s.get("goals") or 0, "assists": s.get("assists"),
                "penalties": s.get("penalties"), "played_matches": s.get("playedMatches"),
            })
        for m in raw["matches"].get("matches", []):
            ft = m["score"]["fullTime"]
            matches.append({
                "source": "football-data", "match_id": f"fd{m['id']}", "league_code": lg,
                "season": season_label(int(m["season"]["startDate"][:4])),
                "matchday": m.get("matchday"), "utc_date": m["utcDate"],
                "home_team": m["homeTeam"].get("shortName") or m["homeTeam"].get("name"),
                "away_team": m["awayTeam"].get("shortName") or m["awayTeam"].get("name"),
                "home_score": ft.get("home"), "away_score": ft.get("away"),
                "status": m["status"],
            })
    return {"standings": standings, "scorers": scorers, "matches": matches}


# ---------------------------------------------------------------------------
# 2) Siamsport (web scraping HTML)
# ---------------------------------------------------------------------------
SS_BASE = "https://www.siamsport.co.th/standings/{path}/"


def ss_extract(league_code: str) -> dict:
    """ขูดตาราง <table> จากหน้า standings ของ Siamsport คืนค่าเป็น cell ดิบ (string)"""
    slug = SS_LEAGUES[league_code]
    html = _get(SS_BASE.format(path=slug)).content.decode("utf-8", errors="replace")  # เว็บไม่ส่ง charset
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise ValueError(f"ไม่พบตารางคะแนนในหน้า {slug}")
    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 10:
            continue  # แถวหัวตาราง
        img = tds[1].find("img")
        name = tds[1].find("span", class_="team-name-text")
        # รหัสทีม = ชื่อไฟล์โลโก้ (เช่น /static/team-logo/9259.png หรือ /static/<uuid>.png)
        m = re.search(r"/([\w-]+)\.\w+$", img["src"]) if img and img.get("src") else None
        logo_id = m.group(1) if m else None
        rows.append({
            "cells": [td.get_text(" ", strip=True) for td in tds],
            "team_name": name.get_text(strip=True) if name else tds[1].get_text(" ", strip=True),
            "logo_id": logo_id,
        })
    time.sleep(1)  # สุภาพกับเว็บต้นทาง
    return {"league_code": league_code, "season": current_season(), "rows": rows}


def ss_transform(raw_list: list[dict]) -> dict:
    standings = []
    for raw in raw_list:
        if not raw:
            continue
        for r in raw["rows"]:
            c = r["cells"]
            # คอลัมน์: # | ทีม | แข่ง | ชนะ | เสมอ | แพ้ | ได้ | เสีย | ต่าง | คะแนน
            standings.append({
                "source": "siamsport", "league_code": raw["league_code"], "season": raw["season"],
                "source_team_id": r["logo_id"] or r["team_name"],
                "team_name": r["team_name"].strip(),
                "position": _int(c[0]), "played": _int(c[2]), "won": _int(c[3]),
                "draw": _int(c[4]), "lost": _int(c[5]), "goals_for": _int(c[6]),
                "goals_against": _int(c[7]), "goal_diff": _int(c[8]), "points": _int(c[9]),
            })
    return {"standings": standings, "scorers": [], "matches": []}


# ---------------------------------------------------------------------------
# 3) Ballthai (JSON ที่หน้าเว็บ ballthai.com ใช้แสดงตาราง)
# ---------------------------------------------------------------------------
BT_API = "https://svc.ballthai.com/api"


def bt_extract() -> dict:
    out = {}
    for lg, (bt_id, _) in THAI_LEAGUES.items():
        st = _get(f"{BT_API}/standings", params={"league_id": bt_id}).json()
        time.sleep(1)
        sc = _get(f"{BT_API}/players/top-scorers", params={"league_id": bt_id, "limit": 50}).json()
        time.sleep(1)
        out[lg] = {"standings": st.get("data", []), "scorers": sc.get("data", [])}
    return out


def _bt_current_stage(rows: list[dict]) -> str | None:
    """ballthai คืนตารางหลาย stage (ฤดูกาลก่อน + ปัจจุบัน) -> เลือก stage ที่มีปีล่าสุด"""
    best, best_year = None, -1
    for r in rows:
        m = re.search(r"(20\d\d)/(\d\d)", r.get("stage_name") or "")
        if m and int(m.group(1)) > best_year:
            best, best_year = r["stage_name"], int(m.group(1))
    return best


def bt_transform(raw: dict) -> dict:
    standings, scorers = [], []
    for lg, payload in raw.items():
        rows = payload["standings"]
        stage = _bt_current_stage(rows)
        m = re.search(r"(20\d\d)/(\d\d)", stage or "")
        season = season_label(int(m.group(1))) if m else current_season()
        for r in rows:
            if stage and r.get("stage_name") != stage:
                continue
            standings.append({
                "source": "ballthai", "league_code": lg, "season": season,
                "source_team_id": str(r["team_id"]), "team_name": r["team_name"].strip(),
                "position": r["current_rank"], "played": r["matches_played"],
                "won": r["wins"], "draw": r["draws"], "lost": r["losses"],
                "goals_for": r["goals_for"], "goals_against": r["goals_against"],
                "goal_diff": r["goal_difference"], "points": r["points"],
            })
        for p in payload["scorers"]:
            scorers.append({
                "source": "ballthai", "league_code": lg, "season": season,
                "source_player_id": str(p["id"]), "player_name": p["name"].strip(),
                "team_name": (p.get("team_name") or "").strip(), "nationality": p.get("nationality"),
                "position": p.get("position"), "goals": p.get("goals") or 0,
                "assists": None, "penalties": None, "played_matches": None,
            })
    return {"standings": standings, "scorers": scorers, "matches": []}


if __name__ == "__main__":  # ทดสอบเร็ว ๆ นอก Airflow
    import json
    ss = ss_transform([ss_extract("EPL")])
    print("siamsport", len(ss["standings"]), json.dumps(ss["standings"][0], ensure_ascii=False))
    bt = bt_transform(bt_extract())
    print("ballthai", len(bt["standings"]), len(bt["scorers"]), json.dumps(bt["scorers"][0], ensure_ascii=False))
    if os.environ.get("FOOTBALL_DATA_API_KEY"):
        fd = fd_transform([fd_extract("EPL")])
        print("football-data", {k: len(v) for k, v in fd.items()})
