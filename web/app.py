"""
Football Hub — เว็บ dashboard (Flask) อ่านข้อมูลจาก PostgreSQL โดยตรง
หน้า: หน้าแรก / ตารางคะแนน / ดาวซัลโว / ผลบอล / สถานะระบบ (pipeline)
"""
import json
import math
import os
from datetime import timezone, timedelta

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, url_for

app = Flask(__name__)

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5433")),
    dbname=os.getenv("DB_NAME", "football"),
    user=os.getenv("DB_USER", "football"),
    password=os.getenv("DB_PASSWORD"),  # มาจาก .env ผ่าน docker-compose
)
BKK = timezone(timedelta(hours=7))

LEAGUES = [  # ลำดับเมนูลีก
    ("EPL", "พรีเมียร์ลีก", "Premier League"),
    ("LALIGA", "ลาลีกา", "La Liga"),
    ("SERIEA", "กัลโช่ เซเรีย อา", "Serie A"),
    ("BUNDESLIGA", "บุนเดสลีกา", "Bundesliga"),
    ("LIGUE1", "ลีกเอิง", "Ligue 1"),
    ("T1", "ไทยลีก 1", "Thai League 1"),
    ("T2", "ไทยลีก 2", "Thai League 2"),
]
LEAGUE_TH = {c: th for c, th, _ in LEAGUES}
SOURCES = ["football-data", "siamsport", "ballthai"]


def q(sql, params=None):
    try:
        with psycopg2.connect(**DB) as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]
    except psycopg2.Error as e:  # ยังไม่ได้รัน DAG -> ยังไม่มีตาราง
        app.logger.warning("DB error: %s", e)
        return []


def filters():
    """ตัวกรองที่ใช้ร่วมกันทุกหน้า (query string)"""
    seasons = [r["season"] for r in q("SELECT DISTINCT season FROM standings ORDER BY season DESC")]
    season = request.args.get("season") or (seasons[0] if seasons else None)
    srcs = request.args.getlist("source") or SOURCES
    return {
        "season": season, "seasons": seasons, "sources": srcs,
        "team": (request.args.get("team") or "").strip(),
        "league": request.args.get("league") or "",
    }


@app.context_processor
def inject_globals():
    last = q("SELECT status, finished_at FROM pipeline_runs ORDER BY finished_at DESC NULLS LAST LIMIT 1")
    return {
        "LEAGUES": LEAGUES, "LEAGUE_TH": LEAGUE_TH, "ALL_SOURCES": SOURCES,
        "last_run": last[0] if last else None, "endpoint": request.endpoint, "tojson": json.dumps,
        "fmt_time": lambda d: d.astimezone(BKK).strftime("%d/%m/%Y %H:%M") if d else "-",
        "page_url": page_url,
    }


def page_url(arg, n):
    """ลิงก์ไปหน้า n โดยคงตัวกรองเดิมไว้"""
    args = request.args.to_dict(flat=False)
    args[arg] = [n]
    return url_for(request.endpoint, **args)


def paginate(rows, per, arg="page"):
    """แบ่งหน้าฝั่ง server: ?page=N  คืน (แถวของหน้านี้, ข้อมูลหน้า)"""
    try:
        page = int(request.args.get(arg, 1))
    except ValueError:
        page = 1
    pages = max(1, math.ceil(len(rows) / per))
    page = min(max(page, 1), pages)
    start = (page - 1) * per
    return rows[start:start + per], {"page": page, "pages": pages, "total": len(rows), "arg": arg,
                                      "start": start, "end": min(start + per, len(rows))}


def source_clause(col, srcs, multi=False):
    """multi=True: คอลัมน์ sources เก็บหลายแหล่งคั่นด้วย comma"""
    if multi:
        return "(" + " OR ".join(f"{col} LIKE %s" for _ in srcs) + ")", [f"%{s}%" for s in srcs]
    return f"{col} = ANY(%s)", [list(srcs)]


# ------------------------------------------------------------------ หน้าแรก
@app.route("/")
def home():
    f = filters()
    sc, sp = source_clause("sources", f["sources"], multi=True)
    standings = q(f"SELECT * FROM standings WHERE season = %s AND {sc} ORDER BY league_code, position",
                  [f["season"], *sp])
    by_league = {}
    for r in standings:
        by_league.setdefault(r["league_code"], []).append(r)
    scorers = q("SELECT * FROM top_scorers WHERE season = %s ORDER BY goals DESC, player_name LIMIT 7",
                [f["season"]])
    results = q("SELECT * FROM matches WHERE season = %s ORDER BY utc_date DESC LIMIT 5", [f["season"]])
    goals = q("""SELECT league_code, sum(goals_for) goals, sum(played) played, count(*) teams
                 FROM standings WHERE season = %s GROUP BY league_code""", [f["season"]])
    m = q("SELECT count(*) n, coalesce(sum(total_goals),0) g FROM matches WHERE season = %s", [f["season"]])
    kpi = {
        "teams": len(standings),
        "leagues": len(by_league),
        "matches": m[0]["n"] if m else 0,
        "avg_goals": (m[0]["g"] / m[0]["n"]) if m and m[0]["n"] else None,
        "goals": sum(int(g["goals"] or 0) for g in goals),
        "verified": sum(1 for r in standings if (r["n_sources"] or 0) >= 2),
        "scorers": q("SELECT count(*) n FROM top_scorers WHERE season = %s", [f["season"]])[0]["n"]
        if standings else 0,
    }
    order = {c: i for i, (c, _, _) in enumerate(LEAGUES)}
    goals.sort(key=lambda g: order.get(g["league_code"], 99))
    chart = {
        "labels": [LEAGUE_TH.get(g["league_code"], g["league_code"]) for g in goals],
        "avg": [round(2 * int(g["goals"] or 0) / max(int(g["played"] or 0), 1), 2) for g in goals],
    }
    hl = request.args.get("hl") or next((c for c, _, _ in LEAGUES if c in by_league), None)
    return render_template("home.html", f=f, by_league=by_league, scorers=scorers, results=results,
                           kpi=kpi, chart=chart, hl=hl, hl_rows=by_league.get(hl, [])[:10])


# ------------------------------------------------------------------ ตารางคะแนน
@app.route("/standings")
def standings():
    f = filters()
    league = f["league"] or "EPL"
    sc, sp = source_clause("sources", f["sources"], multi=True)
    sql = f"SELECT * FROM standings WHERE league_code = %s AND season = %s AND {sc}"
    params = [league, f["season"], *sp]
    if f["team"]:
        sql += " AND (team_name ILIKE %s OR team_name_th ILIKE %s)"
        params += [f"%{f['team']}%"] * 2
    rows = q(sql + " ORDER BY position", params)
    n = len(rows)
    for r in rows:  # โซนสี: ลุ้นแชมป์/ยุโรป, ตกชั้น
        r["zone"] = "top" if r["position"] <= 4 else ("bottom" if n >= 10 and r["position"] > n - 3 else "")
    kpi = {
        "teams": n,
        "goals": sum(r["goals_for"] for r in rows),
        "leader": rows[0] if rows else None,
        "best_attack": max(rows, key=lambda r: r["goals_for"]) if rows else None,
        "best_defense": min(rows, key=lambda r: r["goals_against"]) if rows else None,
        "verified": sum(1 for r in rows if (r["n_sources"] or 0) >= 2),
    }
    chart = {
        "teams": [r["team_name_th"] or r["team_name"] for r in rows],
        "points": [r["points"] for r in rows],
        "gf": [r["goals_for"] for r in rows],
        "ga": [r["goals_against"] for r in rows],
    }
    page_rows, pg = paginate(rows, 10)
    return render_template("standings.html", f=f, league=league, rows=page_rows, n_rows=n, pg=pg,
                           kpi=kpi, chart=chart)


# ------------------------------------------------------------------ ดาวซัลโว
@app.route("/scorers")
def scorers():
    f = filters()
    league = f["league"]
    sc, sp = source_clause("source", f["sources"])
    sql = f"SELECT * FROM top_scorers WHERE season = %s AND {sc}"
    params = [f["season"], *sp]
    if league:
        sql += " AND league_code = %s"
        params.append(league)
    if f["team"]:
        sql += " AND (team_name ILIKE %s OR player_name ILIKE %s)"
        params += [f"%{f['team']}%"] * 2
    rows = q(sql + " ORDER BY goals DESC, assists DESC NULLS LAST, player_name", params)
    kpi = {
        "players": len(rows),
        "goals": sum(r["goals"] for r in rows),
        "top": rows[0] if rows else None,
        "assist_king": max((r for r in rows if r["assists"] is not None), key=lambda r: r["assists"], default=None),
    }
    chart_rows = rows[:10]
    chart = {"names": [r["player_name"].title() for r in chart_rows], "goals": [r["goals"] for r in chart_rows],
             "assists": [r["assists"] or 0 for r in chart_rows]}
    page_rows, pg = paginate(rows, 10)
    return render_template("scorers.html", f=f, league=league, rows=page_rows, pg=pg, kpi=kpi, chart=chart)


# ------------------------------------------------------------------ ผลบอล
@app.route("/matches")
def matches():
    f = filters()
    league = f["league"]
    sc, sp = source_clause("source", f["sources"])
    sql = f"SELECT * FROM matches WHERE season = %s AND {sc}"
    params = [f["season"], *sp]
    if league:
        sql += " AND league_code = %s"
        params.append(league)
    if f["team"]:
        sql += " AND (home_team ILIKE %s OR away_team ILIKE %s)"
        params += [f"%{f['team']}%"] * 2
    result = request.args.get("result") or ""
    if result in ("HOME", "DRAW", "AWAY"):
        sql += " AND result = %s"
        params.append(result)
    rows = q(sql + " ORDER BY utc_date DESC", params)
    page_rows, pg = paginate(rows, 10)
    days = {}
    for r in page_rows:
        days.setdefault(r["utc_date"].astimezone(BKK).strftime("%d/%m/%Y"), []).append(r)
    n = len(rows)
    kpi = {
        "matches": n,
        "goals": sum(r["total_goals"] for r in rows),
        "avg": (sum(r["total_goals"] for r in rows) / n) if n else None,
        "home": sum(r["result"] == "HOME" for r in rows), "draw": sum(r["result"] == "DRAW" for r in rows),
        "away": sum(r["result"] == "AWAY" for r in rows),
        "biggest": max(rows, key=lambda r: r["total_goals"]) if rows else None,
    }
    md = {}
    for r in rows:
        md.setdefault(r["matchday"], []).append(r["total_goals"])
    mds = sorted(k for k in md if k is not None)
    chart = {"md": mds, "avg": [round(sum(md[k]) / len(md[k]), 2) for k in mds]}
    return render_template("matches.html", f=f, league=league, days=days, kpi=kpi, chart=chart, result=result,
                           pg=pg)


# ------------------------------------------------------------------ สถานะระบบ
@app.route("/pipeline")
def pipeline():
    runs = q("SELECT * FROM pipeline_runs ORDER BY finished_at DESC NULLS LAST LIMIT 100")
    counts = q("""SELECT 'standings' AS t, count(*) n FROM standings UNION ALL
                  SELECT 'top_scorers', count(*) FROM top_scorers UNION ALL
                  SELECT 'matches', count(*) FROM matches""")
    by_source = q("""SELECT s AS source, count(*) n FROM (
                       SELECT unnest(string_to_array(sources, ', ')) s FROM standings
                       UNION ALL SELECT source FROM top_scorers UNION ALL SELECT source FROM matches) x
                     GROUP BY s ORDER BY s""")
    quarantine = q("SELECT * FROM dq_quarantine ORDER BY id DESC LIMIT 500")
    total = sum(c["n"] for c in counts)
    run_rows, pg = paginate(runs, 6)
    q_rows, qpg = paginate(quarantine, 5, "qpage")
    return render_template("pipeline.html", f=filters(), runs=run_rows, pg=pg, last=runs[0] if runs else None,
                           counts=counts, total=total, by_source=by_source, quarantine=q_rows, qpg=qpg)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8501, debug=True)
