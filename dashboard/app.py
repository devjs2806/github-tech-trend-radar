"""
GitHub Tech Trend Radar - dashboard.

Reads the result CSVs that the Spark jobs wrote into output/ and shows them.
Run:  streamlit run dashboard/app.py
Then open http://localhost:8501
"""
import os
import time

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")

st.set_page_config(page_title="GitHub Tech Trend Radar", page_icon="📡", layout="wide")


def load(name):
    path = os.path.join(OUT, f"{name}.csv")
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def missing(what, how):
    st.info(f"No {what} yet. Run: `{how}`")


def link(repo):
    return f"https://github.com/{repo}"


st.title("📡 GitHub Tech Trend Radar")
st.caption("Big-data analysis of public GitHub activity · Flume → HDFS → Hive / PySpark → MLlib")

summary = load("dataset_summary")
if summary is not None and len(summary):
    s = summary.iloc[0]
    c = st.columns(5)
    c[0].metric("Events analysed", f"{int(s['total_events']):,}")
    c[1].metric("Repositories", f"{int(s['distinct_repos']):,}")
    c[2].metric("Developers", f"{int(s['distinct_users']):,}")
    c[3].metric("Hours covered", f"{s['hours_covered']}")
    c[4].metric("Period (UTC)", f"{str(s['first_event_utc'])[:10]} → {str(s['last_event_utc'])[:10]}")
else:
    st.warning("No results found in output/. Run job 1 first: `spark-submit spark_jobs/job1_trend_analysis.py`")

tabs = st.tabs(["🔥 Trending", "🧠 Languages & topics", "⏱ Activity", "🤖 ML forecast",
                "🧩 Repo clusters", "📶 Live stream"])

# ------------------------------------------------------------------ trending
with tabs[0]:
    tr = load("trending_repos")
    if tr is None or tr.empty:
        missing("trending repos", "spark-submit spark_jobs/job1_trend_analysis.py")
    else:
        wh = int(tr["window_hours"].iloc[0])
        st.subheader(f"Accelerating repos (stars in last {wh}h vs the {wh}h before)")
        top = tr.head(15)
        st.bar_chart(top.set_index("repo_name")["recent"], horizontal=True, height=450)
        show = tr.copy()
        show["link"] = show["repo_name"].map(link)
        st.dataframe(show, hide_index=True, use_container_width=True,
                     column_config={"link": st.column_config.LinkColumn("GitHub")})

    col1, col2 = st.columns(2)
    with col1:
        em = load("emerging_repos")
        st.subheader("🌱 Emerging (brand-new) repos")
        if em is None or em.empty:
            st.write("None found in this period.")
        else:
            st.dataframe(em, hide_index=True, use_container_width=True)
    with col2:
        ts = load("top_starred_repos")
        st.subheader("⭐ Most starred overall")
        if ts is not None:
            st.dataframe(ts, hide_index=True, use_container_width=True)

    rh = load("repo_health")
    if rh is not None and not rh.empty:
        st.subheader("🩺 Repo health: real development vs hype")
        st.bar_chart(rh["status"].value_counts())
        st.dataframe(rh.head(50), hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ languages
with tabs[1]:
    ls = load("language_summary")
    if ls is None or ls.empty:
        missing("language data", "python3 enrich_metadata.py  then  spark-submit spark_jobs/job2_language_trends.py")
    else:
        c1, c2 = st.columns([3, 2])
        with c1:
            st.subheader("Star share by language")
            st.bar_chart(ls.head(12).set_index("language")["share_percent"], height=380)
        with c2:
            st.subheader("Momentum (recent ÷ previous)")
            st.dataframe(ls[["language", "stars", "repos", "momentum", "trend"]].head(15),
                         hide_index=True, use_container_width=True)
        tl = load("language_timeline")
        if tl is not None and not tl.empty:
            st.subheader("Stars per language over time (6-hour blocks)")
            piv = tl.pivot_table(index="block", columns="language", values="stars", fill_value=0)
            st.line_chart(piv, height=380)
        tp = load("topic_trends")
        if tp is not None and not tp.empty:
            st.subheader("Hot topics")
            c1, c2 = st.columns(2)
            c1.bar_chart(tp.head(15).set_index("topic")["stars"], horizontal=True, height=420)
            c2.dataframe(tp, hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ activity
with tabs[2]:
    hr = load("hourly_activity")
    if hr is None or hr.empty:
        missing("activity data", "spark-submit spark_jobs/job1_trend_analysis.py")
    else:
        st.subheader("Events per hour (UTC)")
        hr["hour_ts"] = pd.to_datetime(hr["hour_ts"])
        st.line_chart(hr.set_index("hour_ts")[["events", "pushes", "stars", "pull_requests"]], height=350)

        hm = load("activity_heatmap")
        c1, c2 = st.columns([3, 2])
        if hm is not None and not hm.empty:
            with c1:
                st.subheader("When does the world code? (IST)")
                order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                piv = hm.pivot_table(index="dow", columns="hour", values="count", fill_value=0)
                piv = piv.reindex([d for d in order if d in piv.index]).reindex(columns=range(24), fill_value=0)
                fig, ax = plt.subplots(figsize=(10, 3.2))
                im = ax.imshow(piv.values, aspect="auto", cmap="YlOrRd")
                ax.set_yticks(range(len(piv.index)), piv.index)
                ax.set_xticks(range(0, 24, 2), [f"{h:02d}" for h in range(0, 24, 2)])
                ax.set_xlabel("Hour of day (IST)")
                fig.colorbar(im, ax=ax, label="events")
                fig.tight_layout()
                st.pyplot(fig)
        et = load("event_type_counts")
        if et is not None:
            with c2:
                st.subheader("Event types")
                st.bar_chart(et.set_index("type")["count"], horizontal=True, height=330)

        c1, c2, c3 = st.columns(3)
        for col, name, title in [(c1, "top_orgs", "🏢 Most active organisations"),
                                 (c2, "top_contributors", "👩‍💻 Most active developers (no bots)"),
                                 (c3, "bot_activity", "🤖 Bot share per event type")]:
            df = load(name)
            if df is not None:
                col.subheader(title)
                col.dataframe(df.head(20), hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ ML
with tabs[3]:
    mm = load("ml_forecast_metrics")
    if mm is None or mm.empty:
        missing("model results", "spark-submit spark_jobs/job3_ml_models.py")
    else:
        st.subheader("Next-hour star forecast — model comparison (test set)")
        st.dataframe(mm, hide_index=True, use_container_width=True)
        st.caption("RMSE/MAE: lower is better · R²: closer to 1 is better · "
                   "a useful model must beat the naive baseline.")
        pr = load("ml_forecast_predictions")
        if pr is not None and not pr.empty:
            repo = st.selectbox("Repo", sorted(pr["repo_name"].unique()))
            one = pr[pr["repo_name"] == repo].copy()
            one["hour_ts"] = pd.to_datetime(one["hour_ts"])
            st.line_chart(one.set_index("hour_ts")[["actual_stars", "predicted_stars"]], height=330)
        c1, c2 = st.columns(2)
        fc = load("ml_next_hour_forecast")
        if fc is not None:
            c1.subheader("🔮 Predicted stars in the next hour")
            c1.dataframe(fc.head(20), hide_index=True, use_container_width=True)
        fi = load("ml_feature_importance")
        if fi is not None:
            c2.subheader("Feature importance (GBT)")
            c2.bar_chart(fi.set_index("feature")["importance"], horizontal=True)

# ------------------------------------------------------------------ clusters
with tabs[4]:
    cs = load("cluster_summary")
    if cs is None or cs.empty:
        missing("clusters", "spark-submit spark_jobs/job3_ml_models.py")
    else:
        st.subheader(f"K-Means repo types (silhouette = {cs['silhouette'].iloc[0]:.3f})")
        st.dataframe(cs.drop(columns=["silhouette"]), hide_index=True, use_container_width=True)
        shares = cs.set_index("cluster_name")[["star_share", "fork_share", "push_share", "pr_share", "issue_share"]]
        st.bar_chart(shares, height=380)
        rc = load("repo_clusters")
        if rc is not None:
            pick = st.selectbox("Show repos in cluster", cs["cluster_name"].tolist())
            st.dataframe(rc[rc["cluster_name"] == pick].head(100), hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ live
with tabs[5]:
    st.subheader("Real-time leaderboard (Spark Structured Streaming)")
    auto = st.toggle("Auto-refresh every 15 s", value=False)
    lt = load("live_trending")
    if lt is None or lt.empty:
        missing("live results", "python3 fetch_live_events.py  +  Flume  +  spark-submit spark_jobs/job4_streaming_trends.py")
    else:
        st.caption(f"Window ending {lt['window_end_utc'].iloc[0]} UTC · updated {lt['updated_at_utc'].iloc[0]} UTC")
        c1, c2 = st.columns([3, 2])
        c1.bar_chart(lt.head(15).set_index("repo_name")["stars_in_window"], horizontal=True, height=420)
        le = load("live_event_types")
        if le is not None:
            c2.dataframe(le, hide_index=True, use_container_width=True)
        th = load("live_throughput")
        if th is not None and not th.empty:
            st.subheader("Events per minute arriving through the pipeline")
            th["minute_utc"] = pd.to_datetime(th["minute_utc"])
            st.area_chart(th.set_index("minute_utc")["events"], height=250)
    if auto:
        time.sleep(15)
        st.rerun()
