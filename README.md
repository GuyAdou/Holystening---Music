# Holystening

A weekly data pipeline that tracks the state of Christian on Spotify. It pulls songs, grabs lyrics, runs LLM sentiment analysis, and surfaces the results in a dashboard.

---

## How it works

The pipeline runs every Monday morning via Apache Airflow and goes through four steps:

**1. Fetch top songs**
Pulls the top 50 tracks from "Top Christian & Gospel" Spotify playlist using the RapidAPI Spotify endpoint. Songs are ranked by popularity score.

**2. Fetch lyrics**
For each song, queries the Musixmatch API to retrieve lyrics. Targets 30 songs with valid lyrics, skipping instrumentals or tracks where lyrics aren't available.

**3. Sentiment analysis**
Sends all the lyrics together in a single prompt to Llama 3.1 (via Groq). The model returns structured JSON with emotional theme categories, percentages, an executive summary, the strongest evidence-backed claim, and a listener profile inference.

**4. Store to GCS**
Saves the full payload as a timestamped JSON file in Google Cloud Storage. The dashboard always reads the most recent file.

```
Spotify playlist → lyrics → Llama 3.1 → GCS → Streamlit
```

---

## Dashboard

Built with Streamlit and Plotly. Reads directly from GCS on load (cached for 1 hour).

- **Summary card** — one-sentence key finding from the LLM
- **Stat pills** — songs analyzed, number of emotional themes, top theme
- **Bar chart** — emotional theme breakdown by percentage
- **Insights panel** — listener profile and strongest evidence-backed claim with confidence level
- **Song grid** — top 20 tracks with Spotify cover art and first lyric line

---

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (CeleryExecutor, Docker) |
| Playlist data | RapidAPI — spotify81 |
| Lyrics | RapidAPI — spotify-web-api3 (Musixmatch) |
| LLM inference | Groq — Llama 3.1 8B |
| Storage | Google Cloud Storage |
| Dashboard | Streamlit + Plotly |

---

## Project structure

```
dags/
  christian_top_song.py   # Airflow DAG — all four pipeline tasks
streamlit_app.py          # Dashboard
docker-compose.yaml       # Local Airflow setup
requirements.txt          # Dashboard dependencies
```
