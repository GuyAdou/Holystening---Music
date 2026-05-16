import json
import os
import requests
import streamlit as st
import plotly.graph_objects as go
from google.cloud import storage
from google.oauth2 import service_account

st.set_page_config(
    page_title="Holystening",
    page_icon="✦",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    * { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }

    .stApp {
        background-color: #f0f4ff;
        background-image:
            radial-gradient(ellipse at 20% 20%, rgba(180, 210, 255, 0.7) 0%, transparent 55%),
            radial-gradient(ellipse at 80% 10%, rgba(210, 190, 255, 0.6) 0%, transparent 50%),
            radial-gradient(ellipse at 60% 80%, rgba(160, 230, 220, 0.5) 0%, transparent 50%),
            radial-gradient(ellipse at 10% 70%, rgba(255, 200, 220, 0.5) 0%, transparent 50%);
        background-attachment: fixed;
        color: #1d1d1f;
    }
    .main .block-container { padding: 0 4rem 4rem 4rem; max-width: 1400px; }

    section[data-testid="stSidebar"] { display: none; }
    header[data-testid="stHeader"] { background: transparent; }
    .stDeployButton { display: none; }
    #MainMenu { display: none; }
    footer { display: none; }

    /* Hero */
    .hero {
        padding: 80px 0 60px 0;
        text-align: center;
    }
    .hero-eyebrow {
        font-size: 14px;
        font-weight: 500;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        color: #6e6e73;
        margin-bottom: 16px;
    }
    .hero-title {
        font-size: 72px;
        font-weight: 700;
        letter-spacing: -0.03em;
        line-height: 1.05;
        background: linear-gradient(135deg, #1d1d1f 0%, #6e6e73 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 24px;
    }
    .hero-subtitle {
        font-size: 20px;
        font-weight: 300;
        color: #6e6e73;
        max-width: 580px;
        margin: 0 auto;
        line-height: 1.6;
    }

    /* Liquid glass base */
    .glass-card {
        background: rgba(255, 255, 255, 0.45);
        border: 1px solid rgba(255, 255, 255, 0.7);
        border-radius: 20px;
        padding: 32px;
        backdrop-filter: blur(40px) saturate(180%);
        -webkit-backdrop-filter: blur(40px) saturate(180%);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.08), inset 0 1px 0 rgba(255,255,255,0.8);
    }

    /* Summary card */
    .summary-card {
        background: rgba(255, 255, 255, 0.5);
        border: 1px solid rgba(255, 255, 255, 0.75);
        border-radius: 28px;
        padding: 44px 52px;
        margin: 0 0 48px 0;
        position: relative;
        overflow: hidden;
        backdrop-filter: blur(60px) saturate(200%);
        -webkit-backdrop-filter: blur(60px) saturate(200%);
        box-shadow: 0 16px 48px rgba(0, 0, 0, 0.08), inset 0 1px 0 rgba(255,255,255,0.9);
    }
    .summary-card::before {
        content: '✦';
        position: absolute;
        top: 36px;
        right: 44px;
        font-size: 28px;
        color: rgba(0,0,0,0.08);
    }
    .summary-card::after {
        content: '';
        position: absolute;
        top: -60px;
        right: -60px;
        width: 200px;
        height: 200px;
        background: radial-gradient(circle, rgba(180,210,255,0.4) 0%, transparent 70%);
        pointer-events: none;
    }
    .summary-label {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #6e6e73;
        margin-bottom: 12px;
    }
    .summary-text {
        font-size: 22px;
        font-weight: 400;
        color: #1d1d1f;
        line-height: 1.55;
    }

    /* Stat pills */
    .stat-pill {
        background: rgba(255, 255, 255, 0.55);
        border: 1px solid rgba(255, 255, 255, 0.8);
        border-radius: 100px;
        padding: 22px 32px;
        text-align: center;
        display: inline-block;
        width: 100%;
        backdrop-filter: blur(40px);
        -webkit-backdrop-filter: blur(40px);
        box-shadow: 0 4px 20px rgba(0,0,0,0.06), inset 0 1px 0 rgba(255,255,255,0.9);
    }
    .stat-number {
        font-size: 42px;
        font-weight: 700;
        letter-spacing: -0.03em;
        color: #1d1d1f;
        line-height: 1;
    }
    .stat-label {
        font-size: 13px;
        color: #6e6e73;
        margin-top: 6px;
        font-weight: 400;
    }

    /* Section titles */
    .section-title {
        font-size: 32px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: #1d1d1f;
        margin: 56px 0 24px 0;
    }

    /* Song cards */
    .song-title {
        font-size: 13px;
        font-weight: 600;
        color: #1d1d1f;
        margin-top: 12px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .song-artist {
        font-size: 12px;
        color: #6e6e73;
        margin-top: 3px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .song-lyric {
        font-size: 11px;
        color: #aeaeb2;
        margin-top: 8px;
        font-style: italic;
        line-height: 1.4;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        overflow: hidden;
    }

    /* Insight cards */
    .insight-card {
        background: rgba(255, 255, 255, 0.45);
        border: 1px solid rgba(255, 255, 255, 0.75);
        border-radius: 20px;
        padding: 28px;
        backdrop-filter: blur(40px) saturate(180%);
        -webkit-backdrop-filter: blur(40px) saturate(180%);
        box-shadow: 0 8px 24px rgba(0,0,0,0.06), inset 0 1px 0 rgba(255,255,255,0.85);
    }
    .insight-label {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #6e6e73;
        margin-bottom: 14px;
    }
    .insight-text {
        font-size: 15px;
        color: #3a3a3c;
        line-height: 1.7;
        font-weight: 400;
    }
    .confidence-badge {
        display: inline-block;
        background: rgba(0,0,0,0.05);
        border: 1px solid rgba(0,0,0,0.08);
        border-radius: 100px;
        padding: 4px 12px;
        font-size: 11px;
        font-weight: 600;
        color: #6e6e73;
        margin-top: 14px;
        letter-spacing: 0.05em;
    }

    /* Divider */
    .apple-divider {
        border: none;
        border-top: 1px solid rgba(0,0,0,0.06);
        margin: 48px 0;
    }

    /* Streamlit image liquid glass wrapper */
    [data-testid="stImage"] img {
        border-radius: 14px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.12);
    }

    .js-plotly-plot { border-radius: 16px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_latest_data():
    key_json = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    key_dict = json.loads(key_json)
    credentials = service_account.Credentials.from_service_account_info(key_dict)
    client = storage.Client(credentials=credentials, project="holystening-pipeline")
    bucket = client.bucket("holystening-data")
    blobs = sorted(bucket.list_blobs(prefix="christian_songs_"), key=lambda b: b.name, reverse=True)
    if not blobs:
        return None
    for blob in blobs:
        data = json.loads(blob.download_as_text())
        if data.get("songs") and len(data["songs"]) > 0:
            return data
    return None


@st.cache_data(ttl=86400)
def get_cover(spotify_id):
    try:
        url = f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{spotify_id}"
        res = requests.get(url, timeout=5)
        return res.json().get("thumbnail_url")
    except Exception:
        return None


data = load_latest_data()
if not data:
    st.error("No data found.")
    st.stop()

songs = data["songs"]
analysis = data["analysis"]
import re as _re
analyzed_count = data.get("analyzed_count")
if analyzed_count is None:
    _m = _re.search(r'analysis of (\d+)', analysis.get("executive_summary", analysis.get("headline_finding", "")))
    analyzed_count = int(_m.group(1)) if _m else len(songs)
categories = analysis["emotional_analysis"]["categories"]
summary_text = analysis.get("headline_finding") or analysis.get("executive_summary", "")
notable_patterns = analysis.get("notable_patterns", [])
lyrics_address = analysis.get("what_the_lyrics_address") or analysis.get("listener_profile", {}).get("evidence_based_inference", "")
artist_note = analysis.get("artist_concentration_note")
data_limitations = analysis.get("data_limitations", [])

# ── Hero ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="hero-eyebrow">Christian Music Intelligence</div>
    <div class="hero-title">Holystening</div>
    <div class="hero-subtitle">What believers are listening to right now on Spotify</div>
</div>
""", unsafe_allow_html=True)

# ── Summary card ─────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="summary-card">
    <div class="summary-label">Key Finding</div>
    <div class="summary-text">{summary_text}</div>
</div>
""", unsafe_allow_html=True)

# ── Stat pills ────────────────────────────────────────────────────────────────
top_cat = max(categories, key=lambda c: c["percentage"])

stats = [
    (analyzed_count, "Songs Analyzed"),
    (len(categories), "Emotional Themes"),
    (top_cat["name"], f"Top Theme · {top_cat['percentage']}%"),
]
_, c1, c2, c3, _ = st.columns([1, 2, 2, 2, 1])
for col, (val, label) in zip([c1, c2, c3], stats):
    col.markdown(f"""
    <div class="stat-pill">
        <div class="stat-number" style="font-size: {'28px' if len(str(val)) > 10 else '42px'}">{val}</div>
        <div class="stat-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown('<hr class="apple-divider">', unsafe_allow_html=True)

# ── Chart + Insights ──────────────────────────────────────────────────────────
left, right = st.columns([3, 2], gap="large")

with left:
    st.markdown('<div class="section-title">Emotional Themes</div>', unsafe_allow_html=True)

    names = [c["name"] for c in categories]
    percentages = [c["percentage"] for c in categories]
    blue_shades = ["#1e3a8a", "#1d4ed8", "#3b82f6", "#60a5fa", "#93c5fd", "#bfdbfe"]
    colors = blue_shades[:len(names)]

    fig = go.Figure(go.Bar(
        x=percentages,
        y=names,
        orientation="h",
        marker=dict(
            color=colors,
            line=dict(width=0),
            opacity=0.9
        ),
        text=[f"{p}%" for p in percentages],
        textposition="outside",
        textfont=dict(color="#1d1d1f", size=14, family="Inter")
    ))
    fig.update_layout(
        paper_bgcolor="rgba(255,255,255,0.6)",
        plot_bgcolor="rgba(245,247,255,0.8)",
        font=dict(color="#1d1d1f", family="Inter"),
        xaxis=dict(range=[0, 115], showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, tickfont=dict(size=14, color="#1d1d1f")),
        margin=dict(l=10, r=70, t=20, b=20),
        height=340,
        bargap=0.35
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown('<div class="section-title">Patterns</div>', unsafe_allow_html=True)

    if lyrics_address:
        st.markdown(f"""
        <div class="insight-card" style="margin-bottom:16px">
            <div class="insight-label">What These Lyrics Address</div>
            <div class="insight-text">{lyrics_address}</div>
        </div>
        """, unsafe_allow_html=True)

    for p in notable_patterns[:2]:
        caveat_html = f'<div style="color:#aeaeb2;font-size:12px;margin-top:10px">⚠ {p["caveat"]}</div>' if p.get("caveat") else ""
        songs_html = "".join([f'<div style="color:#6e6e73;font-size:12px;margin-top:4px">· {s}</div>' for s in p.get("supporting_songs", [])[:3]])
        quote_html = f'<div style="font-style:italic;color:#aeaeb2;font-size:12px;margin-top:10px">"{p["supporting_quote"]}"</div>' if p.get("supporting_quote") else ""
        st.markdown(f"""
        <div class="insight-card" style="margin-bottom:16px">
            <div class="insight-label">Pattern</div>
            <div class="insight-text">{p["pattern"]}</div>
            {songs_html}
            {quote_html}
            {caveat_html}
        </div>
        """, unsafe_allow_html=True)

    if artist_note:
        st.markdown(f"""
        <div class="insight-card" style="margin-bottom:16px;border-color:rgba(255,200,100,0.4)">
            <div class="insight-label">Artist Concentration</div>
            <div class="insight-text" style="font-size:13px">{artist_note}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown('<hr class="apple-divider">', unsafe_allow_html=True)

# ── Song Grid ─────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Top Songs</div>', unsafe_allow_html=True)

cols = st.columns(5, gap="small")
for i, song in enumerate(songs[:20]):
    cover = get_cover(song["spotify_id"])
    with cols[i % 5]:
        if cover:
            st.image(cover, use_container_width=True)
        st.markdown(f"""
        <div class="song-title">{song['title']}</div>
        <div class="song-artist">{song['artist']}</div>
        <div class="song-lyric">{song.get('lyrics_first_line', '')}</div>
        <br/>
        """, unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown('<hr class="apple-divider">', unsafe_allow_html=True)
st.markdown("""
<div style="text-align:center;color:#3a3a3c;font-size:13px;padding-bottom:32px">
    Powered by Spotify · Llama 3.3 · Apache Airflow · Google Cloud
</div>
""", unsafe_allow_html=True)
