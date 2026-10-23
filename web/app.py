"""
Football Data Dashboard (Streamlit) — อ่านข้อมูลจาก PostgreSQL โดยตรง
หน้า: ภาพรวมตารางคะแนน / ดาวซัลโว / ผลการแข่งขัน / สถานะ pipeline
"""
import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Football Multi-Source Dashboard", page_icon="⚽", layout="wide")

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5433")),
    dbname=os.getenv("DB_NAME", "football"),
    user=os.getenv("DB_USER", "football"),
    password=os.getenv("DB_PASSWORD", "football"),
)


@st.cache_data(ttl=60)
def q(sql: str, params=None) -> pd.DataFrame:
    try:
        with psycopg2.connect(**DB) as conn:
            return pd.read_sql_query(sql, conn, params=params)
    except Exception as e:  # ตารางยังไม่ถูกสร้าง (ยังไม่เคยรัน DAG)
        st.session_state["db_error"] = str(e)
        return pd.DataFrame()


st.sidebar.title("⚽ Football Dashboard")
page = st.sidebar.radio("หน้า", ["ตารางคะแนน", "ดาวซัลโว", "ผลการแข่งขัน", "สถานะ Pipeline"])
if st.sidebar.button("🔄 รีเฟรชข้อมูล"):
    st.cache_data.clear()

standings = q("SELECT * FROM standings ORDER BY league_code, position")
scorers = q("SELECT * FROM top_scorers ORDER BY goals DESC, player_name")
matches = q("SELECT * FROM matches ORDER BY utc_date DESC")

if standings.empty and page != "สถานะ Pipeline":
    st.warning("ยังไม่มีข้อมูลใน PostgreSQL — เปิด Airflow (http://localhost:8080) แล้ว Trigger DAG "
               "`football_multi_source_pipeline` ก่อน")
    if "db_error" in st.session_state:
        st.caption(st.session_state["db_error"])
    st.stop()

# ---------------- ตัวกรอง ----------------
leagues = sorted(standings["league_name"].dropna().unique()) if not standings.empty else []
st.sidebar.subheader("ตัวกรอง")
sel_leagues = st.sidebar.multiselect("ลีก", leagues, default=leagues)
seasons = sorted(standings["season"].dropna().unique(), reverse=True) if not standings.empty else []
sel_season = st.sidebar.selectbox("ฤดูกาล", seasons) if seasons else None
all_sources = ["football-data", "siamsport", "ballthai"]
sel_sources = st.sidebar.multiselect("แหล่งข้อมูล", all_sources, default=all_sources)
team_kw = st.sidebar.text_input("ค้นหาทีม (ไทย/อังกฤษ)")


def filt_league(df):
    if df.empty:
        return df
    df = df[df["league_name"].isin(sel_leagues)]
    if sel_season:
        df = df[df["season"] == sel_season]
    return df


def has_source(src_col: pd.Series) -> pd.Series:
    return src_col.fillna("").apply(lambda s: any(x in s for x in sel_sources))


# ======================================================= ตารางคะแนน
if page == "ตารางคะแนน":
    st.title("ตารางคะแนนฟุตบอลลีกยุโรป + ไทยลีก")
    df = filt_league(standings)
    df = df[has_source(df["sources"])]
    if team_kw:
        df = df[df["team_name"].fillna("").str.contains(team_kw, case=False)
                | df["team_name_th"].fillna("").str.contains(team_kw, case=False)]

    sc = filt_league(scorers)
    mt = filt_league(matches)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("จำนวนทีม", f"{len(df):,}")
    k2.metric("จำนวนลีก", df["league_code"].nunique())
    k3.metric("ประตูรวม (ตารางคะแนน)", f"{int(df['goals_for'].sum()):,}")
    avg = (mt["total_goals"].mean() if not mt.empty else float("nan"))
    k4.metric("ประตูเฉลี่ย/นัด", f"{avg:.2f}" if pd.notna(avg) else "-")
    k5.metric("ทีมที่ยืนยันจาก 2 แหล่ง", int((df["n_sources"] >= 2).sum()))

    if not sc.empty:
        top = sc.iloc[0]
        st.info(f"🥇 ดาวซัลโวสูงสุดในตัวกรอง: **{top['player_name']}** ({top['team_name']}, "
                f"{top['league_name']}) — {int(top['goals'])} ประตู")

    for lg in [l for l in leagues if l in sel_leagues]:
        d = df[df["league_name"] == lg].sort_values("position")
        if d.empty:
            continue
        st.subheader(lg)
        c1, c2 = st.columns([3, 2])
        c1.dataframe(
            d[["position", "team_name", "team_name_th", "played", "won", "draw", "lost",
               "goals_for", "goals_against", "goal_diff", "points", "sources"]].rename(columns={
                "position": "#", "team_name": "ทีม", "team_name_th": "ชื่อไทย", "played": "แข่ง",
                "won": "ชนะ", "draw": "เสมอ", "lost": "แพ้", "goals_for": "ได้", "goals_against": "เสีย",
                "goal_diff": "+/-", "points": "แต้ม", "sources": "แหล่ง"}),
            hide_index=True, use_container_width=True, height=min(38 * len(d) + 40, 760))
        fig = px.bar(d.sort_values("points"), x="points", y="team_name", orientation="h",
                     color="goal_diff", color_continuous_scale="RdYlGn",
                     labels={"points": "แต้ม", "team_name": "", "goal_diff": "+/-"})
        fig.update_layout(height=min(30 * len(d) + 80, 760), margin=dict(l=0, r=0, t=10, b=0))
        c2.plotly_chart(fig, use_container_width=True)

    st.subheader("เปรียบเทียบประตูได้-เสีย ทุกทีม")
    fig = px.scatter(df, x="goals_against", y="goals_for", color="league_name", size="points",
                     hover_name="team_name", labels={"goals_against": "เสีย", "goals_for": "ได้"})
    st.plotly_chart(fig, use_container_width=True)

# ======================================================= ดาวซัลโว
elif page == "ดาวซัลโว":
    st.title("อันดับดาวซัลโว")
    df = filt_league(scorers)
    df = df[df["source"].isin(sel_sources)]
    if team_kw:
        df = df[df["team_name"].fillna("").str.contains(team_kw, case=False)]
    n = st.slider("แสดงกี่อันดับ", 5, 50, 15)
    k1, k2, k3 = st.columns(3)
    k1.metric("นักเตะ", len(df))
    k2.metric("ประตูรวม", int(df["goals"].sum()) if not df.empty else 0)
    k3.metric("ประตูสูงสุด", int(df["goals"].max()) if not df.empty else 0)
    top = df.sort_values("goals", ascending=False).head(n)
    fig = px.bar(top.sort_values("goals"), x="goals", y="player_name", color="league_name",
                 orientation="h", hover_data=["team_name", "assists", "penalties"],
                 labels={"goals": "ประตู", "player_name": "", "league_name": "ลีก"})
    fig.update_layout(height=max(400, 28 * len(top)))
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df[["league_name", "player_name", "team_name", "nationality", "goals", "assists",
                     "penalties", "played_matches", "source"]], hide_index=True, use_container_width=True)

# ======================================================= ผลการแข่งขัน
elif page == "ผลการแข่งขัน":
    st.title("ผลการแข่งขัน / ผลประตู")
    df = filt_league(matches)
    df = df[df["source"].isin(sel_sources)]
    if team_kw:
        df = df[df["home_team"].str.contains(team_kw, case=False) | df["away_team"].str.contains(team_kw, case=False)]
    if df.empty:
        st.info("ไม่มีผลการแข่งขันตามตัวกรอง (ผลการแข่งขันมาจาก Football-Data API — ต้องตั้ง API key)")
        st.stop()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("จำนวนนัด", len(df))
    k2.metric("ประตูรวม", int(df["total_goals"].sum()))
    k3.metric("เฉลี่ย/นัด", f"{df['total_goals'].mean():.2f}")
    k4.metric("เจ้าบ้านชนะ", f"{(df['result'] == 'HOME').mean() * 100:.0f}%")
    c1, c2 = st.columns(2)
    g = df.groupby(["league_name", "matchday"], as_index=False)["total_goals"].mean()
    c1.plotly_chart(px.line(g, x="matchday", y="total_goals", color="league_name", markers=True,
                            labels={"total_goals": "ประตูเฉลี่ย/นัด", "matchday": "นัดที่"}),
                    use_container_width=True)
    r = df.groupby(["league_name", "result"], as_index=False).size()
    c2.plotly_chart(px.bar(r, x="league_name", y="size", color="result", barmode="stack",
                           labels={"size": "จำนวนนัด", "league_name": ""}), use_container_width=True)
    show = df.assign(score=df["home_score"].astype(str) + " - " + df["away_score"].astype(str))
    st.dataframe(show[["utc_date", "league_name", "matchday", "home_team", "score", "away_team", "source"]],
                 hide_index=True, use_container_width=True)

# ======================================================= สถานะ pipeline
else:
    st.title("สถานะ Pipeline (Airflow DAG: football_multi_source_pipeline)")
    runs = q("SELECT * FROM pipeline_runs ORDER BY finished_at DESC NULLS LAST LIMIT 30")
    if runs.empty:
        st.warning("ยังไม่มีประวัติการรัน")
        st.stop()
    last = runs.iloc[0]
    icon = {"success": "🟢", "partial": "🟡", "failed": "🔴"}
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("สถานะรอบล่าสุด", f"{icon.get(last['status'], '⚪')} {last['status']}")
    k2.metric("เวลาเสร็จ", str(pd.to_datetime(last["finished_at"]).tz_convert("Asia/Bangkok"))[:19])
    k3.metric("ทีมซ้ำข้ามแหล่งที่ตัดออก", int(last["dedup_removed"] or 0))
    k4.metric("แถวไม่ผ่าน DQ", int(last["dq_issues"] or 0))
    st.subheader("สถานะแต่ละแหล่ง (รอบล่าสุด)")
    cols = st.columns(3)
    for c, (src, stt) in zip(cols, (last["source_status"] or {}).items()):
        c.metric(src, f"{'🟢' if stt == 'success' else '🔴'} {stt}")
    counts = q("""SELECT 'standings' t, count(*) n FROM standings UNION ALL
                  SELECT 'top_scorers', count(*) FROM top_scorers UNION ALL
                  SELECT 'matches', count(*) FROM matches""")
    st.subheader(f"จำนวนแถวในฐานข้อมูล: {int(counts['n'].sum()):,} แถว")
    st.dataframe(counts, hide_index=True)
    st.subheader("ประวัติการรัน")
    st.dataframe(runs[["run_id", "status", "finished_at", "source_status", "rows_standings", "rows_scorers",
                       "rows_matches", "dedup_removed", "dq_passed", "dq_issues"]],
                 hide_index=True, use_container_width=True)
    with st.expander("สถานะทุก task รอบล่าสุด"):
        st.json(last["task_states"])
    qa = q("SELECT run_id, table_name, reason, record, created_at FROM dq_quarantine ORDER BY id DESC LIMIT 100")
    st.subheader("แถวที่ถูกกักกัน (ไม่ผ่านการตรวจคุณภาพ)")
    st.dataframe(qa, hide_index=True, use_container_width=True) if not qa.empty else st.success("ไม่มี")
