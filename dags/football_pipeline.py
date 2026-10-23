"""
DAG: football_multi_source_pipeline
รวมตารางคะแนน / ดาวซัลโว / ผลการแข่งขัน ฟุตบอลลีกยุโรป + ไทยลีก จาก 3 แหล่ง -> PostgreSQL

create_tables
  -> [TaskGroup football_data | TaskGroup siamsport | TaskGroup ballthai]   (รันขนานกัน)
       แต่ละกลุ่ม: extract -> transform -> load (UPSERT ลง stg_*)
       football_data / siamsport ใช้ Dynamic Task Mapping (1 task ต่อ 1 ลีก)
  -> merge_dedup        (trigger_rule=all_done: แหล่งใดล่มก็ยังรวมแหล่งที่เหลือ + ตัดทีมซ้ำข้ามแหล่ง)
  -> quality_check      (BranchPythonOperator)
       ผ่าน   -> publish
       ไม่ผ่าน -> quarantine_bad_rows -> publish (เฉพาะแถวที่ผ่าน)
  -> record_run         (บันทึกสถานะ pipeline ให้หน้าเว็บ /status)

ทดสอบกรณีแหล่งล่ม: Trigger DAG w/ config {"fail_sources": ["siamsport"]}
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from psycopg2.extras import execute_values

import football_sources as fs

CONN_ID = "football_db"
EURO = list(fs.EURO_LEAGUES)
MIN_ROWS = 100

DDL = """
CREATE TABLE IF NOT EXISTS stg_standings (
    source TEXT, league_code TEXT, season TEXT, source_team_id TEXT, team_name TEXT,
    position INT, played INT, won INT, draw INT, lost INT,
    goals_for INT, goals_against INT, goal_diff INT, points INT,
    run_id TEXT, fetched_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (source, league_code, season, source_team_id)
);
CREATE TABLE IF NOT EXISTS stg_scorers (
    source TEXT, league_code TEXT, season TEXT, source_player_id TEXT, player_name TEXT,
    team_name TEXT, nationality TEXT, position TEXT, goals INT, assists INT, penalties INT,
    played_matches INT, run_id TEXT, fetched_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (source, league_code, season, source_player_id)
);
CREATE TABLE IF NOT EXISTS stg_matches (
    source TEXT, match_id TEXT, league_code TEXT, season TEXT, matchday INT, utc_date TIMESTAMPTZ,
    home_team TEXT, away_team TEXT, home_score INT, away_score INT, status TEXT,
    run_id TEXT, fetched_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (source, match_id)
);
-- จับคู่ทีมข้ามแหล่ง (เช่น siamsport 'แมนซิตี้' <-> football-data 'Man City') ให้ team_key คงที่ทุกรอบ
CREATE TABLE IF NOT EXISTS team_alias (
    league_code TEXT, source TEXT, source_team_id TEXT, team_key TEXT, team_name_en TEXT,
    PRIMARY KEY (league_code, source, source_team_id)
);
-- ตารางพักข้อมูลหลังรวม (work tables ต่อรอบ)
CREATE TABLE IF NOT EXISTS mrg_standings (
    league_code TEXT, season TEXT, team_key TEXT, team_name TEXT, team_name_th TEXT,
    position INT, played INT, won INT, draw INT, lost INT, goals_for INT, goals_against INT,
    goal_diff INT, points INT, sources TEXT, n_sources INT, dq_reason TEXT,
    PRIMARY KEY (league_code, season, team_key)
);
CREATE TABLE IF NOT EXISTS mrg_scorers (LIKE stg_scorers);
ALTER TABLE mrg_scorers ADD COLUMN IF NOT EXISTS dq_reason TEXT;
CREATE TABLE IF NOT EXISTS mrg_matches (LIKE stg_matches);
ALTER TABLE mrg_matches ADD COLUMN IF NOT EXISTS dq_reason TEXT;
-- ตารางปลายทางที่ dashboard อ่าน
CREATE TABLE IF NOT EXISTS standings (
    league_code TEXT, league_name TEXT, season TEXT, team_key TEXT, team_name TEXT, team_name_th TEXT,
    position INT, played INT, won INT, draw INT, lost INT, goals_for INT, goals_against INT,
    goal_diff INT, points INT, sources TEXT, n_sources INT, updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (league_code, season, team_key)
);
CREATE TABLE IF NOT EXISTS top_scorers (
    league_code TEXT, league_name TEXT, season TEXT, player_key TEXT, player_name TEXT, team_name TEXT,
    nationality TEXT, position TEXT, goals INT, assists INT, penalties INT, played_matches INT,
    source TEXT, updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (league_code, season, player_key)
);
CREATE TABLE IF NOT EXISTS matches (
    match_key TEXT PRIMARY KEY, league_code TEXT, league_name TEXT, season TEXT, matchday INT,
    utc_date TIMESTAMPTZ, home_team TEXT, away_team TEXT, home_score INT, away_score INT,
    total_goals INT, result TEXT, status TEXT, source TEXT, updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dq_quarantine (
    id SERIAL PRIMARY KEY, run_id TEXT, table_name TEXT, record JSONB, reason TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, status TEXT,
    source_status JSONB, task_states JSONB, rows_standings INT, rows_scorers INT, rows_matches INT,
    dedup_removed INT, dq_passed BOOLEAN, dq_issues INT
);
"""

STG_COLS = {
    "standings": ["source", "league_code", "season", "source_team_id", "team_name", "position", "played",
                  "won", "draw", "lost", "goals_for", "goals_against", "goal_diff", "points"],
    "scorers": ["source", "league_code", "season", "source_player_id", "player_name", "team_name",
                "nationality", "position", "goals", "assists", "penalties", "played_matches"],
    "matches": ["source", "match_id", "league_code", "season", "matchday", "utc_date", "home_team",
                "away_team", "home_score", "away_score", "status"],
}
STG_PK = {
    "standings": ["source", "league_code", "season", "source_team_id"],
    "scorers": ["source", "league_code", "season", "source_player_id"],
    "matches": ["source", "match_id"],
}


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)


def _upsert(cur, table: str, cols: list[str], pk: list[str], rows: list[tuple]):
    if not rows:
        return 0
    upd = [c for c in cols if c not in pk]
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(pk)}) DO UPDATE SET "
        + ", ".join(f"{c} = EXCLUDED.{c}" for c in upd)
    )
    execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def _check_simulated_failure(source: str):
    conf = (get_current_context()["dag_run"].conf or {})
    if source in conf.get("fail_sources", []):
        raise AirflowException(f"จำลองว่าแหล่ง {source} ล่ม (dag_run.conf.fail_sources)")


def _load_staging(data: dict) -> dict:
    """UPSERT ผลจาก transform ลง stg_* พร้อม run_id ของรอบนี้"""
    run_id = get_current_context()["run_id"]
    counts = {}
    with _hook().get_conn() as conn, conn.cursor() as cur:
        for kind, rows in data.items():
            cols = STG_COLS[kind]
            tuples = [tuple(r.get(c) for c in cols) + (run_id,) for r in rows]
            counts[kind] = _upsert(cur, f"stg_{kind}", cols + ["run_id"], STG_PK[kind], tuples)
        conn.commit()
    return counts


default_args = {"owner": "student", "retries": 1, "retry_delay": timedelta(seconds=20)}


@dag(
    dag_id="football_multi_source_pipeline",
    description="ตารางคะแนน/ดาวซัลโว/ผลบอล ลีกยุโรป+ไทยลีก จาก Football-Data API, Siamsport, Ballthai",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["football", "final-exam"],
)
def football_pipeline():

    @task
    def create_tables():
        _hook().run(DDL)

    # ------------------------------------------------------------------ แหล่งที่ 1
    @task_group(group_id="football_data")
    def football_data_group():
        @task(retries=0, max_active_tis_per_dagrun=1)  # free tier 10 req/min -> ทีละลีก
        def extract(league_code: str):
            _check_simulated_failure("football_data")
            return fs.fd_extract(league_code)

        @task(trigger_rule=TriggerRule.ALL_DONE)  # บางลีกล่ม ก็ใช้ลีกที่เหลือ
        def transform(raw_list):
            raws = [r for r in raw_list if r]
            if not raws:
                raise AirflowException("football-data: ไม่มีข้อมูลจากทุกลีก")
            return fs.fd_transform(raws)

        @task
        def load(data):
            return _load_staging(data)

        load(transform(extract.expand(league_code=EURO)))

    # ------------------------------------------------------------------ แหล่งที่ 2
    @task_group(group_id="siamsport")
    def siamsport_group():
        @task(retries=1)
        def extract(league_code: str):
            _check_simulated_failure("siamsport")
            return fs.ss_extract(league_code)

        @task(trigger_rule=TriggerRule.ALL_DONE)
        def transform(raw_list):
            raws = [r for r in raw_list if r]
            if not raws:
                raise AirflowException("siamsport: scrape ไม่ได้ทุกลีก")
            return fs.ss_transform(raws)

        @task
        def load(data):
            return _load_staging(data)

        load(transform(extract.expand(league_code=EURO)))

    # ------------------------------------------------------------------ แหล่งที่ 3
    @task_group(group_id="ballthai")
    def ballthai_group():
        @task(retries=1)
        def extract():
            _check_simulated_failure("ballthai")
            return fs.bt_extract()

        @task
        def transform(raw):
            return fs.bt_transform(raw)

        @task
        def load(data):
            return _load_staging(data)

        load(transform(extract()))

    # ------------------------------------------------------------------ รวม + ตัดซ้ำ
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def merge_dedup():
        """
        ใช้เฉพาะแถวที่โหลดในรอบนี้ (run_id ปัจจุบัน) -> แหล่งที่ล่มจะไม่มีแถว แต่แหล่งอื่นยังรวมได้
        ตัดทีมซ้ำข้ามแหล่ง (football-data vs siamsport ลีกเดียวกัน):
          1) team_alias ที่เคยจับคู่ไว้แล้ว
          2) สถิติตรงกันทุกค่า (แข่ง/ชนะ/เสมอ/แพ้/ได้/เสีย/แต้ม) และไม่ซ้ำกับทีมอื่น
          3) อันดับ + แต้มตรงกัน
        เมื่อซ้ำ ใช้ตัวเลขจาก football-data (API ทางการ) และเก็บชื่อไทยจาก siamsport
        """
        run_id = get_current_context()["run_id"]
        hook = _hook()
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM stg_standings WHERE run_id = %s UNION ALL "
                    "SELECT 1 FROM stg_scorers WHERE run_id = %s LIMIT 1", (run_id, run_id))
        if cur.fetchone() is None:
            raise AirflowException("ทุกแหล่งล่ม ไม่มีข้อมูลให้รวม")

        cur.execute(f"SELECT {', '.join(STG_COLS['standings'])} FROM stg_standings WHERE run_id = %s", (run_id,))
        cols = STG_COLS["standings"]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.execute("SELECT league_code, source, source_team_id, team_key, team_name_en FROM team_alias")
        alias = {(a[0], a[1], a[2]): (a[3], a[4]) for a in cur.fetchall()}
        new_alias = []

        stat_keys = ("played", "won", "draw", "lost", "goals_for", "goals_against", "points")
        merged, dedup_removed = [], 0

        def out_row(r, key, name_en, name_th, sources):
            return {**{k: r[k] for k in ("league_code", "season", "position", "played", "won", "draw",
                                         "lost", "goals_for", "goals_against", "goal_diff", "points")},
                    "team_key": key, "team_name": name_en or name_th, "team_name_th": name_th,
                    "sources": ", ".join(sources), "n_sources": len(sources)}

        leagues = sorted({r["league_code"] for r in rows})
        for lg in leagues:
            fd = [r for r in rows if r["league_code"] == lg and r["source"] == "football-data"]
            ss = [r for r in rows if r["league_code"] == lg and r["source"] == "siamsport"]
            bt = [r for r in rows if r["league_code"] == lg and r["source"] == "ballthai"]
            fd_by_key = {f"fd{r['source_team_id']}": r for r in fd}
            used = set()
            for s in ss:
                match_key = None
                a = alias.get((lg, "siamsport", s["source_team_id"]))
                if a and a[0] in fd_by_key and a[0] not in used:
                    match_key = a[0]
                if match_key is None:
                    sig = tuple(s[k] for k in stat_keys)
                    cands = [k for k, f in fd_by_key.items()
                             if k not in used and tuple(f[x] for x in stat_keys) == sig]
                    if len(cands) == 1:
                        match_key = cands[0]
                if match_key is None:
                    cands = [k for k, f in fd_by_key.items() if k not in used
                             and f["position"] == s["position"] and f["points"] == s["points"]]
                    if len(cands) == 1:
                        match_key = cands[0]
                if match_key:
                    f = fd_by_key[match_key]
                    used.add(match_key)
                    dedup_removed += 1
                    new_alias.append((lg, "siamsport", s["source_team_id"], match_key, f["team_name"]))
                    s["_merged"] = True
                    merged.append(out_row(f, match_key, f["team_name"], s["team_name"],
                                          ["football-data", "siamsport"]))
                elif a:  # football-data ล่มรอบนี้ แต่เคยจับคู่ไว้ -> ใช้ key เดิม ไม่เกิดแถวซ้ำ
                    merged.append(out_row(s, a[0], a[1], s["team_name"], ["siamsport"]))
                else:
                    merged.append(out_row(s, f"ss{s['source_team_id']}", None, s["team_name"], ["siamsport"]))
            for key, f in fd_by_key.items():
                if key not in used:
                    merged.append(out_row(f, key, f["team_name"], None, ["football-data"]))
            for b in bt:
                merged.append(out_row(b, f"bt{b['source_team_id']}", None, b["team_name"], ["ballthai"]))

        # ทีมเดียวกันอาจถูกใส่ซ้ำ (เช่น alias ชี้ key เดียวกัน) -> เก็บแถวแรก
        uniq = {}
        for m in merged:
            uniq.setdefault((m["league_code"], m["season"], m["team_key"]), m)
        dedup_removed += len(merged) - len(uniq)

        mcols = ["league_code", "season", "team_key", "team_name", "team_name_th", "position", "played",
                 "won", "draw", "lost", "goals_for", "goals_against", "goal_diff", "points",
                 "sources", "n_sources"]
        cur.execute("TRUNCATE mrg_standings, mrg_scorers, mrg_matches")
        execute_values(cur, f"INSERT INTO mrg_standings ({', '.join(mcols)}) VALUES %s",
                       [tuple(m[c] for c in mcols) for m in uniq.values()])
        _upsert(cur, "team_alias", ["league_code", "source", "source_team_id", "team_key", "team_name_en"],
                ["league_code", "source", "source_team_id"], new_alias)
        # ดาวซัลโว/ผลบอล ไม่มีแหล่งซ้อนกัน แต่ตัดซ้ำด้วย DISTINCT ON กันข้อมูลซ้ำในแหล่งเดียว
        cur.execute("""
            INSERT INTO mrg_scorers
            SELECT DISTINCT ON (league_code, season, lower(player_name), lower(team_name)) *, NULL
            FROM stg_scorers WHERE run_id = %s
            ORDER BY league_code, season, lower(player_name), lower(team_name), goals DESC""", (run_id,))
        cur.execute("""
            INSERT INTO mrg_matches
            SELECT DISTINCT ON (league_code, utc_date, lower(home_team), lower(away_team)) *, NULL
            FROM stg_matches WHERE run_id = %s
            ORDER BY league_code, utc_date, lower(home_team), lower(away_team)""", (run_id,))
        # ทดสอบเส้นทาง DQ ไม่ผ่าน: Trigger DAG w/ config {"inject_bad_rows": true}
        if (get_current_context()["dag_run"].conf or {}).get("inject_bad_rows"):
            cur.execute("""
                INSERT INTO mrg_standings (league_code, season, team_key, team_name, team_name_th, position,
                    played, won, draw, lost, goals_for, goals_against, goal_diff, points, sources, n_sources)
                VALUES ('EPL', %s, 'test_bad_row', 'TEST BAD ROW', NULL, 99, 5, 1, 1, 1, -2, 3, 9, 40, 'test', 1)
            """, (fs.current_season(),))
        conn.commit()
        cur.execute("SELECT (SELECT count(*) FROM mrg_standings), (SELECT count(*) FROM mrg_scorers), "
                    "(SELECT count(*) FROM mrg_matches)")
        n = cur.fetchone()
        conn.close()
        return {"standings": n[0], "scorers": n[1], "matches": n[2], "dedup_removed": dedup_removed}

    # ------------------------------------------------------------------ ตรวจคุณภาพ + แตกเส้นทาง
    def quality_check(**context):
        rules = {
            "mrg_standings": [
                ("team_name IS NULL OR team_name = ''", "ไม่มีชื่อทีม"),
                ("played <> won + draw + lost", "แข่ง != ชนะ+เสมอ+แพ้"),
                ("goal_diff <> goals_for - goals_against", "ผลต่างประตูไม่ตรง"),
                ("LEAST(played, won, draw, lost, goals_for, goals_against) < 0", "ค่าติดลบ"),
                ("points > 3 * played", "แต้มเกินที่เป็นไปได้"),
                ("position < 1 OR position > 40", "อันดับผิดช่วง"),
            ],
            "mrg_scorers": [
                ("player_name IS NULL OR player_name = ''", "ไม่มีชื่อนักเตะ"),
                ("goals IS NULL OR goals < 0 OR goals > 100", "จำนวนประตูผิดปกติ"),
            ],
            "mrg_matches": [
                ("home_score IS NULL OR away_score IS NULL", "ไม่มีสกอร์"),
                ("home_score < 0 OR away_score < 0 OR home_score + away_score > 20", "สกอร์ผิดปกติ"),
                ("home_team = away_team", "ทีมเหย้า=ทีมเยือน"),
            ],
        }
        issues = 0
        with _hook().get_conn() as conn, conn.cursor() as cur:
            for table, checks in rules.items():
                for cond, reason in checks:
                    cur.execute(f"UPDATE {table} SET dq_reason = COALESCE(dq_reason || '; ', '') || %s "
                                f"WHERE {cond}", (reason,))
                    issues += cur.rowcount
            cur.execute("SELECT (SELECT count(*) FROM mrg_standings) + (SELECT count(*) FROM mrg_scorers)"
                        " + (SELECT count(*) FROM mrg_matches)")
            total = cur.fetchone()[0]
            conn.commit()
        too_few = total < MIN_ROWS
        context["ti"].xcom_push(key="dq", value={"issues": issues, "total": total, "too_few": too_few})
        return "quarantine_bad_rows" if issues or too_few else "dq_passed"

    quality = BranchPythonOperator(task_id="quality_check", python_callable=quality_check)
    dq_passed = EmptyOperator(task_id="dq_passed")

    @task
    def quarantine_bad_rows():
        run_id = get_current_context()["run_id"]
        moved = 0
        with _hook().get_conn() as conn, conn.cursor() as cur:
            for t in ("mrg_standings", "mrg_scorers", "mrg_matches"):
                cur.execute(f"""
                    INSERT INTO dq_quarantine (run_id, table_name, record, reason)
                    SELECT %s, %s, to_jsonb(x) - 'dq_reason', x.dq_reason FROM {t} x
                    WHERE x.dq_reason IS NOT NULL""", (run_id, t.replace("mrg_", "")))
                moved += cur.rowcount
                cur.execute(f"DELETE FROM {t} WHERE dq_reason IS NOT NULL")
            conn.commit()
        return moved

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish():
        """UPSERT จาก mrg_* -> ตารางปลายทาง (รันซ้ำกี่ครั้งจำนวนแถวไม่เพิ่ม)"""
        names = json.dumps(fs.LEAGUE_NAMES)
        with _hook().get_conn() as conn, conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO standings (league_code, league_name, season, team_key, team_name, team_name_th,
                    position, played, won, draw, lost, goals_for, goals_against, goal_diff, points,
                    sources, n_sources, updated_at)
                SELECT league_code, (%s::jsonb)->>league_code, season, team_key, team_name, team_name_th,
                    position, played, won, draw, lost, goals_for, goals_against, goal_diff, points,
                    sources, n_sources, now()
                FROM mrg_standings
                ON CONFLICT (league_code, season, team_key) DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    team_name_th = COALESCE(EXCLUDED.team_name_th, standings.team_name_th),
                    position = EXCLUDED.position, played = EXCLUDED.played, won = EXCLUDED.won,
                    draw = EXCLUDED.draw, lost = EXCLUDED.lost, goals_for = EXCLUDED.goals_for,
                    goals_against = EXCLUDED.goals_against, goal_diff = EXCLUDED.goal_diff,
                    points = EXCLUDED.points, sources = EXCLUDED.sources, n_sources = EXCLUDED.n_sources,
                    updated_at = now()""", (names,))
            # ลบแถวค้างจากรอบก่อน (เช่น ทีมที่เคยไม่ได้จับคู่ แล้วรอบนี้จับคู่ได้) เฉพาะลีกที่มีข้อมูลรอบนี้
            cur.execute("""
                DELETE FROM standings s WHERE EXISTS (
                    SELECT 1 FROM mrg_standings m WHERE m.league_code = s.league_code AND m.season = s.season)
                AND NOT EXISTS (SELECT 1 FROM mrg_standings m WHERE m.league_code = s.league_code
                    AND m.season = s.season AND m.team_key = s.team_key)""")
            cur.execute("""
                INSERT INTO top_scorers (league_code, league_name, season, player_key, player_name, team_name,
                    nationality, position, goals, assists, penalties, played_matches, source, updated_at)
                SELECT league_code, (%s::jsonb)->>league_code, season,
                    CASE source WHEN 'football-data' THEN 'fd' ELSE 'bt' END || source_player_id,
                    player_name, team_name, nationality, position, goals, assists, penalties,
                    played_matches, source, now()
                FROM mrg_scorers
                ON CONFLICT (league_code, season, player_key) DO UPDATE SET
                    player_name = EXCLUDED.player_name, team_name = EXCLUDED.team_name,
                    goals = EXCLUDED.goals, assists = EXCLUDED.assists, penalties = EXCLUDED.penalties,
                    played_matches = EXCLUDED.played_matches, updated_at = now()""", (names,))
            cur.execute("""
                INSERT INTO matches (match_key, league_code, league_name, season, matchday, utc_date,
                    home_team, away_team, home_score, away_score, total_goals, result, status, source, updated_at)
                SELECT match_id, league_code, (%s::jsonb)->>league_code, season, matchday, utc_date,
                    home_team, away_team, home_score, away_score, home_score + away_score,
                    CASE WHEN home_score > away_score THEN 'HOME' WHEN home_score < away_score THEN 'AWAY'
                         ELSE 'DRAW' END, status, source, now()
                FROM mrg_matches
                ON CONFLICT (match_key) DO UPDATE SET
                    home_score = EXCLUDED.home_score, away_score = EXCLUDED.away_score,
                    total_goals = EXCLUDED.total_goals, result = EXCLUDED.result,
                    status = EXCLUDED.status, updated_at = now()""", (names,))
            conn.commit()
            cur.execute("SELECT (SELECT count(*) FROM standings), (SELECT count(*) FROM top_scorers), "
                        "(SELECT count(*) FROM matches)")
            n = cur.fetchone()
        return {"standings": n[0], "top_scorers": n[1], "matches": n[2]}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_run():
        ctx = get_current_context()
        merge_stats = ctx["ti"].xcom_pull(task_ids="merge_dedup")
        dag_run = ctx["dag_run"]
        tis = dag_run.get_task_instances()
        states = {f"{ti.task_id}" + (f"[{ti.map_index}]" if ti.map_index >= 0 else ""): ti.state
                  for ti in tis if ti.task_id != "record_run"}
        source_status = {}
        for grp in ("football_data", "siamsport", "ballthai"):
            st = states.get(f"{grp}.load")
            source_status[grp] = "success" if st == "success" else "failed"
        dq = ctx["ti"].xcom_pull(task_ids="quality_check", key="dq") or {}
        pub_state = states.get("publish")
        status = "success" if pub_state == "success" and all(
            v == "success" for v in source_status.values()) else (
            "partial" if pub_state == "success" else "failed")
        ms = merge_stats or {}
        with _hook().get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (run_id, started_at, finished_at, status, source_status, task_states,
                    rows_standings, rows_scorers, rows_matches, dedup_removed, dq_passed, dq_issues)
                VALUES (%s, %s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET finished_at = now(), status = EXCLUDED.status,
                    source_status = EXCLUDED.source_status, task_states = EXCLUDED.task_states,
                    rows_standings = EXCLUDED.rows_standings, rows_scorers = EXCLUDED.rows_scorers,
                    rows_matches = EXCLUDED.rows_matches, dedup_removed = EXCLUDED.dedup_removed,
                    dq_passed = EXCLUDED.dq_passed, dq_issues = EXCLUDED.dq_issues""",
                (dag_run.run_id, dag_run.start_date, status, json.dumps(source_status), json.dumps(states),
                 ms.get("standings"), ms.get("scorers"), ms.get("matches"), ms.get("dedup_removed"),
                 bool(dq) and not dq.get("issues") and not dq.get("too_few"), dq.get("issues")))
            conn.commit()
        return status

    t_create = create_tables()
    sources = [football_data_group(), siamsport_group(), ballthai_group()]
    merged = merge_dedup()
    quarantine = quarantine_bad_rows()
    published = publish()

    t_create >> sources >> merged >> quality
    quality >> [dq_passed, quarantine]
    [dq_passed, quarantine] >> published
    published >> record_run()


football_pipeline()
