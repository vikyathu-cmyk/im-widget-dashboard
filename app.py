import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import snowflake.connector

st.set_page_config(
    page_title="IM_DEDICATED Widget Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Snowflake connection (key-pair auth) ──────────────────────────────────────

@st.cache_resource(ttl=300)
def get_connection():
    cfg = st.secrets["snowflake"]
    import base64
    from cryptography.hazmat.primitives.serialization import (
        load_der_private_key, Encoding, PrivateFormat, NoEncryption
    )
    # private_key in secrets is the raw base64 DER (no PEM headers)
    key_der = base64.b64decode(cfg["private_key"])
    private_key = load_der_private_key(key_der, password=None, backend=default_backend())
    private_key_der = private_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        private_key=private_key_der,
        warehouse=cfg["warehouse"],
        role=cfg.get("role", ""),
        database=cfg.get("database", "STREAMS"),
        schema=cfg.get("schema", "PUBLIC"),
    )


def run_query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


# ── SQL ───────────────────────────────────────────────────────────────────────

WIDGET_SQL = """
WITH im_dedicated AS (
    SELECT DISTINCT de_id::STRING AS de_id
    FROM analytics.public.dde_daily_logs
    WHERE de_tag = 'IM_DEDICATED'
      AND dt >= DATEADD(day, -3, %(start_dt)s::DATE)
),
impressions_flat AS (
    SELECT
        e.DT::DATE       AS dt,
        c.city_name,
        f.value:link::STRING   AS link,
        f.value:title::STRING  AS widget_title,
        f.value:index::INT     AS widget_position,
        e.de
    FROM STREAMS.PUBLIC.DP_DE_SUPER_APP_EVENT e
    INNER JOIN im_dedicated im  ON e.de::STRING = im.de_id
    INNER JOIN de.swiggy.de_info d ON e.de = d.de_id
    INNER JOIN analytics.public.city_attributes c ON d.city_id = c.city_id,
    LATERAL FLATTEN(INPUT => PARSE_JSON(e.ov)) f
    WHERE e.dt BETWEEN %(start_dt)s::DATE AND %(end_dt)s::DATE
      AND e.SN  = 'roadrunner_de-home'
      AND e."on" = 'promotion-widget-items-impression'
      AND e.ET  = 'impression'
),
impressions AS (
    SELECT
        dt, city_name, link, widget_title, widget_position,
        COUNT(*)           AS impressions,
        COUNT(DISTINCT de) AS impressions_unique
    FROM impressions_flat
    GROUP BY 1,2,3,4,5
),
clicks AS (
    SELECT
        e.DT::DATE         AS dt,
        c.city_name,
        e.f4               AS link,
        e.f2               AS widget_title,
        COUNT(*)           AS clicks,
        COUNT(DISTINCT e.de) AS clicks_unique
    FROM STREAMS.PUBLIC.DP_DE_SUPER_APP_EVENT e
    INNER JOIN im_dedicated im  ON e.de::STRING = im.de_id
    INNER JOIN de.swiggy.de_info d ON e.de = d.de_id
    INNER JOIN analytics.public.city_attributes c ON d.city_id = c.city_id
    WHERE e.dt BETWEEN %(start_dt)s::DATE AND %(end_dt)s::DATE
      AND e.SN  = 'roadrunner_de-home'
      AND e."on" = 'widget'
      AND e.ov  = 'promotions-item'
      AND e.ET  = 'click'
    GROUP BY 1,2,3,4
)
SELECT
    i.dt,
    i.city_name,
    i.widget_title,
    i.link,
    i.widget_position,
    SUM(i.impressions)                                                          AS impressions,
    SUM(COALESCE(c.clicks, 0))                                                  AS clicks,
    ROUND(SUM(COALESCE(c.clicks,0))        / NULLIF(SUM(i.impressions),0)*100, 2)  AS ctr_pct,
    SUM(i.impressions_unique)                                                   AS impressions_unique,
    SUM(COALESCE(c.clicks_unique, 0))                                           AS clicks_unique,
    ROUND(SUM(COALESCE(c.clicks_unique,0)) / NULLIF(SUM(i.impressions_unique),0)*100, 2) AS ctr_unique_pct
FROM impressions i
LEFT JOIN clicks c
    ON  i.link      = c.link
    AND i.dt        = c.dt
    AND i.city_name = c.city_name
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC, 6 DESC
"""

# ── Sidebar controls ──────────────────────────────────────────────────────────

with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/1/13/Swiggy_logo.svg/320px-Swiggy_logo.svg.png",
        width=140,
    )
    st.title("IM_DEDICATED\nWidget Dashboard")
    st.divider()

    default_end   = date.today() - timedelta(days=1)
    default_start = default_end - timedelta(days=13)

    start_dt = st.date_input("Start date", value=default_start, max_value=default_end)
    end_dt   = st.date_input("End date",   value=default_end,   min_value=start_dt, max_value=date.today())

    refresh_clicked = st.button("🔄  Refresh data", use_container_width=True)

    auto_refresh = st.checkbox("Auto-refresh every 30 min", value=False)
    if auto_refresh:
        st.info("Page will reload automatically every 30 minutes.")
        st.markdown(
            """<meta http-equiv="refresh" content="1800">""",
            unsafe_allow_html=True,
        )

    st.divider()
    st.caption("Data source: `STREAMS.PUBLIC.DP_DE_SUPER_APP_EVENT`")
    st.caption("Filter: `de_tag = 'IM_DEDICATED'`")

# ── Load data ─────────────────────────────────────────────────────────────────

cache_key = f"{start_dt}_{end_dt}"

@st.cache_data(ttl=1800, show_spinner="Querying Snowflake…")
def load_data(start: str, end: str) -> pd.DataFrame:
    df = run_query(WIDGET_SQL, {"start_dt": start, "end_dt": end})
    df.columns = [c.lower() for c in df.columns]
    df["dt"] = pd.to_datetime(df["dt"])
    for col in ["impressions", "clicks", "impressions_unique", "clicks_unique"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in ["ctr_pct", "ctr_unique_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)
    return df


if refresh_clicked:
    st.cache_data.clear()

try:
    df = load_data(str(start_dt), str(end_dt))
    load_ok = True
except Exception as e:
    st.error(f"Snowflake error: {e}")
    st.stop()

if df.empty:
    st.warning("No data returned for the selected date range.")
    st.stop()

# ── Filters ───────────────────────────────────────────────────────────────────

all_cities  = sorted(df["city_name"].dropna().unique())
all_widgets = sorted(df["widget_title"].dropna().unique())

with st.sidebar:
    st.divider()
    selected_cities = st.multiselect(
        "Filter cities",
        options=all_cities,
        default=[],
        placeholder="All cities",
    )
    selected_widgets = st.multiselect(
        "Filter widgets",
        options=all_widgets,
        default=[],
        placeholder="All widgets",
    )

filtered = df.copy()
if selected_cities:
    filtered = filtered[filtered["city_name"].isin(selected_cities)]
if selected_widgets:
    filtered = filtered[filtered["widget_title"].isin(selected_widgets)]

# ── KPI cards ─────────────────────────────────────────────────────────────────

st.markdown("## 📊 IM_DEDICATED Widget Performance")
st.caption(f"Date range: **{start_dt}** → **{end_dt}** · Last refreshed: **{pd.Timestamp.now().strftime('%d %b %Y, %H:%M')}**")

total_imp      = filtered["impressions"].sum()
total_clicks   = filtered["clicks"].sum()
overall_ctr    = round(total_clicks / total_imp * 100, 2) if total_imp else 0
total_imp_uniq = filtered["impressions_unique"].sum()
total_clk_uniq = filtered["clicks_unique"].sum()
overall_ctr_u  = round(total_clk_uniq / total_imp_uniq * 100, 2) if total_imp_uniq else 0
n_cities       = filtered["city_name"].nunique()
n_widgets      = filtered["widget_title"].nunique()

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Total Impressions",        f"{total_imp:,}")
k2.metric("Total Clicks",             f"{total_clicks:,}")
k3.metric("Overall CTR",              f"{overall_ctr}%")
k4.metric("Unique Impressions",       f"{total_imp_uniq:,}")
k5.metric("Unique CTR",               f"{overall_ctr_u}%")
k6.metric("Cities / Widgets",         f"{n_cities} / {n_widgets}")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_overview, tab_widget, tab_position, tab_city, tab_raw = st.tabs([
    "📈 Daily Trend",
    "🧩 Widget Level",
    "📍 Position Level",
    "🏙️ City Level",
    "🗃️ Raw Data",
])

# ─ Tab 1: Daily trend ─────────────────────────────────────────────────────────

with tab_overview:
    st.subheader("Daily Impressions & Clicks")

    daily = (
        filtered.groupby("dt", as_index=False)
        .agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            impressions_unique=("impressions_unique", "sum"),
            clicks_unique=("clicks_unique", "sum"),
        )
    )
    daily["ctr_pct"]        = (daily["clicks"]        / daily["impressions"].replace(0, None) * 100).round(2)
    daily["ctr_unique_pct"] = (daily["clicks_unique"] / daily["impressions_unique"].replace(0, None) * 100).round(2)

    fig = go.Figure()
    fig.add_bar(x=daily["dt"], y=daily["impressions"], name="Impressions",
                marker_color="#FC8019", opacity=0.85)
    fig.add_bar(x=daily["dt"], y=daily["clicks"],      name="Clicks",
                marker_color="#3D9BFF", opacity=0.85)
    fig.add_scatter(x=daily["dt"], y=daily["ctr_pct"], name="CTR (%)",
                    yaxis="y2", line=dict(color="#FFD700", width=2), mode="lines+markers")
    fig.update_layout(
        barmode="group",
        yaxis=dict(title="Count"),
        yaxis2=dict(title="CTR (%)", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.1),
        margin=dict(t=20, b=20),
        height=380,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Daily Unique Impressions & Unique CTR")
    fig2 = go.Figure()
    fig2.add_bar(x=daily["dt"], y=daily["impressions_unique"], name="Unique Impressions",
                 marker_color="#FC8019", opacity=0.75)
    fig2.add_bar(x=daily["dt"], y=daily["clicks_unique"],      name="Unique Clicks",
                 marker_color="#3D9BFF", opacity=0.75)
    fig2.add_scatter(x=daily["dt"], y=daily["ctr_unique_pct"], name="Unique CTR (%)",
                     yaxis="y2", line=dict(color="#00E5A0", width=2), mode="lines+markers")
    fig2.update_layout(
        barmode="group",
        yaxis=dict(title="Unique Count"),
        yaxis2=dict(title="Unique CTR (%)", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.1),
        margin=dict(t=20, b=20),
        height=380,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig2, use_container_width=True)

# ─ Tab 2: Widget level ────────────────────────────────────────────────────────

with tab_widget:
    st.subheader("Widget-Level Performance")

    wdf = (
        filtered.groupby("widget_title", as_index=False)
        .agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            impressions_unique=("impressions_unique", "sum"),
            clicks_unique=("clicks_unique", "sum"),
        )
    )
    wdf["ctr_pct"]        = (wdf["clicks"]        / wdf["impressions"].replace(0, None) * 100).round(2)
    wdf["ctr_unique_pct"] = (wdf["clicks_unique"] / wdf["impressions_unique"].replace(0, None) * 100).round(2)
    wdf = wdf.sort_values("impressions", ascending=False)

    col_l, col_r = st.columns(2)

    with col_l:
        fig_w = px.bar(
            wdf.head(20),
            x="impressions", y="widget_title",
            orientation="h",
            color="ctr_pct",
            color_continuous_scale=["#1A1D27", "#FC8019"],
            labels={"impressions": "Impressions", "widget_title": "Widget", "ctr_pct": "CTR (%)"},
            title="Top 20 Widgets by Impressions (color = CTR %)",
        )
        fig_w.update_layout(height=520, margin=dict(t=40, b=10),
                            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_w, use_container_width=True)

    with col_r:
        fig_ctr = px.bar(
            wdf.sort_values("ctr_pct", ascending=False).head(20),
            x="ctr_pct", y="widget_title",
            orientation="h",
            color="clicks",
            color_continuous_scale=["#1A1D27", "#3D9BFF"],
            labels={"ctr_pct": "CTR (%)", "widget_title": "Widget", "clicks": "Clicks"},
            title="Top 20 Widgets by CTR % (color = Clicks)",
        )
        fig_ctr.update_layout(height=520, margin=dict(t=40, b=10),
                              plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_ctr, use_container_width=True)

    st.subheader("Widget daily trend")
    top10_widgets = wdf.head(10)["widget_title"].tolist()
    wtrend = (
        filtered[filtered["widget_title"].isin(top10_widgets)]
        .groupby(["dt", "widget_title"], as_index=False)
        .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"))
    )
    metric_w = st.radio("Metric", ["impressions", "clicks"], horizontal=True, key="wmetric")
    fig_wt = px.line(
        wtrend, x="dt", y=metric_w, color="widget_title",
        labels={"dt": "Date", metric_w: metric_w.title(), "widget_title": "Widget"},
        title=f"Top 10 Widgets — daily {metric_w}",
    )
    fig_wt.update_layout(height=380, margin=dict(t=40, b=10),
                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_wt, use_container_width=True)

    st.dataframe(
        wdf.rename(columns={
            "widget_title": "Widget", "impressions": "Impressions", "clicks": "Clicks",
            "ctr_pct": "CTR (%)", "impressions_unique": "Uniq Impr",
            "clicks_unique": "Uniq Clicks", "ctr_unique_pct": "Uniq CTR (%)",
        }),
        use_container_width=True, hide_index=True,
    )

# ─ Tab 3: Position level ──────────────────────────────────────────────────────

with tab_position:
    st.subheader("Widget Position Performance")
    st.caption("Position 0 = top of the carousel, higher index = further right/down.")

    pos_df = (
        filtered.groupby("widget_position", as_index=False)
        .agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            impressions_unique=("impressions_unique", "sum"),
            clicks_unique=("clicks_unique", "sum"),
        )
    )
    pos_df["ctr_pct"]        = (pos_df["clicks"]        / pos_df["impressions"].replace(0, None) * 100).round(2)
    pos_df["ctr_unique_pct"] = (pos_df["clicks_unique"] / pos_df["impressions_unique"].replace(0, None) * 100).round(2)
    pos_df = pos_df.sort_values("widget_position")

    c1, c2 = st.columns(2)
    with c1:
        fig_pos_imp = px.bar(
            pos_df, x="widget_position", y="impressions",
            color="ctr_pct", color_continuous_scale=["#1A1D27", "#FC8019"],
            labels={"widget_position": "Position (index)", "impressions": "Impressions", "ctr_pct": "CTR (%)"},
            title="Impressions by Position (color = CTR %)",
        )
        fig_pos_imp.update_layout(height=380, margin=dict(t=40, b=10),
                                  plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_pos_imp, use_container_width=True)

    with c2:
        fig_pos_ctr = px.line(
            pos_df, x="widget_position", y=["ctr_pct", "ctr_unique_pct"],
            markers=True,
            labels={"widget_position": "Position (index)", "value": "CTR (%)", "variable": "Metric"},
            title="CTR (%) by Position",
            color_discrete_map={"ctr_pct": "#FC8019", "ctr_unique_pct": "#00E5A0"},
        )
        fig_pos_ctr.update_layout(height=380, margin=dict(t=40, b=10),
                                  plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_pos_ctr, use_container_width=True)

    # Heatmap: widget_title × position
    st.subheader("Widget × Position Heatmap (Impressions)")
    pivot = (
        filtered.groupby(["widget_title", "widget_position"])["impressions"]
        .sum()
        .unstack(fill_value=0)
    )
    # Keep top 20 widgets by total impressions
    top20 = filtered.groupby("widget_title")["impressions"].sum().nlargest(20).index
    pivot = pivot.loc[pivot.index.isin(top20)].sort_values(
        by=pivot.columns.tolist(), ascending=False
    )
    fig_heat = px.imshow(
        pivot,
        aspect="auto",
        color_continuous_scale="Oranges",
        labels={"x": "Position (index)", "y": "Widget", "color": "Impressions"},
        title="Impressions Heatmap — Top 20 Widgets × Position",
    )
    fig_heat.update_layout(height=500, margin=dict(t=40, b=10),
                           paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_heat, use_container_width=True)

    st.dataframe(
        pos_df.rename(columns={
            "widget_position": "Position", "impressions": "Impressions", "clicks": "Clicks",
            "ctr_pct": "CTR (%)", "impressions_unique": "Uniq Impr",
            "clicks_unique": "Uniq Clicks", "ctr_unique_pct": "Uniq CTR (%)",
        }),
        use_container_width=True, hide_index=True,
    )

# ─ Tab 4: City level ──────────────────────────────────────────────────────────

with tab_city:
    st.subheader("City-Level Performance")

    city_df = (
        filtered.groupby("city_name", as_index=False)
        .agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            impressions_unique=("impressions_unique", "sum"),
            clicks_unique=("clicks_unique", "sum"),
        )
    )
    city_df["ctr_pct"]        = (city_df["clicks"]        / city_df["impressions"].replace(0, None) * 100).round(2)
    city_df["ctr_unique_pct"] = (city_df["clicks_unique"] / city_df["impressions_unique"].replace(0, None) * 100).round(2)
    city_df = city_df.sort_values("impressions", ascending=False)

    col_cl, col_cr = st.columns(2)
    with col_cl:
        fig_city_imp = px.bar(
            city_df.head(25),
            x="impressions", y="city_name",
            orientation="h",
            color="ctr_pct",
            color_continuous_scale=["#1A1D27", "#FC8019"],
            labels={"impressions": "Impressions", "city_name": "City", "ctr_pct": "CTR (%)"},
            title="Top 25 Cities by Impressions",
        )
        fig_city_imp.update_layout(height=600, margin=dict(t=40, b=10),
                                   plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_city_imp, use_container_width=True)

    with col_cr:
        fig_city_ctr = px.bar(
            city_df.sort_values("ctr_pct", ascending=False).head(25),
            x="ctr_pct", y="city_name",
            orientation="h",
            color="clicks",
            color_continuous_scale=["#1A1D27", "#3D9BFF"],
            labels={"ctr_pct": "CTR (%)", "city_name": "City", "clicks": "Clicks"},
            title="Top 25 Cities by CTR %",
        )
        fig_city_ctr.update_layout(height=600, margin=dict(t=40, b=10),
                                   plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_city_ctr, use_container_width=True)

    st.subheader("City daily trend")
    top5_cities = city_df.head(5)["city_name"].tolist()
    city_trend = (
        filtered[filtered["city_name"].isin(top5_cities)]
        .groupby(["dt", "city_name"], as_index=False)
        .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"), ctr_pct=("ctr_pct", "mean"))
    )
    metric_c = st.radio("Metric", ["impressions", "clicks", "ctr_pct"], horizontal=True, key="cmetric")
    fig_ct = px.line(
        city_trend, x="dt", y=metric_c, color="city_name",
        markers=True,
        labels={"dt": "Date", metric_c: metric_c.replace("_", " ").title(), "city_name": "City"},
        title=f"Top 5 Cities — daily {metric_c.replace('_', ' ')}",
    )
    fig_ct.update_layout(height=380, margin=dict(t=40, b=10),
                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_ct, use_container_width=True)

    st.dataframe(
        city_df.rename(columns={
            "city_name": "City", "impressions": "Impressions", "clicks": "Clicks",
            "ctr_pct": "CTR (%)", "impressions_unique": "Uniq Impr",
            "clicks_unique": "Uniq Clicks", "ctr_unique_pct": "Uniq CTR (%)",
        }),
        use_container_width=True, hide_index=True,
    )

# ─ Tab 5: Raw data ────────────────────────────────────────────────────────────

with tab_raw:
    st.subheader("Raw Query Results")
    st.caption(f"{len(filtered):,} rows · filtered from {len(df):,} total rows")

    search = st.text_input("Search (widget title or city)", "")
    display_df = filtered.copy()
    if search:
        mask = (
            display_df["widget_title"].str.contains(search, case=False, na=False)
            | display_df["city_name"].str.contains(search, case=False, na=False)
        )
        display_df = display_df[mask]

    st.dataframe(
        display_df.rename(columns={
            "dt": "Date", "city_name": "City", "widget_title": "Widget",
            "link": "Link", "widget_position": "Position",
            "impressions": "Impressions", "clicks": "Clicks", "ctr_pct": "CTR (%)",
            "impressions_unique": "Uniq Impr", "clicks_unique": "Uniq Clicks",
            "ctr_unique_pct": "Uniq CTR (%)",
        }),
        use_container_width=True, hide_index=True,
    )

    csv = display_df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇ Download CSV", data=csv,
                       file_name=f"im_dedicated_widgets_{start_dt}_{end_dt}.csv",
                       mime="text/csv")
