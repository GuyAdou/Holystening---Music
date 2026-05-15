import http.client
import json
import re
import time
import urllib.parse
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(schedule="0 6 * * 1", start_date=datetime(2024, 1, 1), catchup=False)
def christian_top_songs():

    @task()
    def fetch_top_songs():
        rapidapi_key = Variable.get("RAPIDAPI_KEY")
        playlist_id = "37i9dQZF1DXcb6CQIjdqKy"

        conn = http.client.HTTPSConnection("spotify81.p.rapidapi.com")
        headers = {
            'x-rapidapi-key': rapidapi_key,
            'x-rapidapi-host': "spotify81.p.rapidapi.com",
            'Content-Type': "application/json"
        }
        conn.request("GET", f"/playlist?id={playlist_id}", headers=headers)
        res = conn.getresponse()
        data = json.loads(res.read().decode("utf-8"))

        songs = []
        for item in data["tracks"]["items"]:
            track = item.get("track")
            if track:
                songs.append({
                    "title": track["name"],
                    "artist": track["artists"][0]["name"],
                    "spotify_id": track["id"],
                    "popularity": track.get("popularity", 0)
                })

        songs = sorted(songs, key=lambda x: x["popularity"], reverse=True)
        print(f"Fetched {len(songs)} songs")
        return songs

    @task()
    def fetch_lyrics(songs):
        import requests
        from bs4 import BeautifulSoup

        genius_token = Variable.get("GENIUS_ACCESS_TOKEN")
        results = []
        target = 100
        attempted = 0

        print(f"Starting lyrics fetch. Input: {len(songs)} songs, target: {target}")

        for song in songs:
            if len(results) >= target:
                break
            attempted += 1
            try:
                # Search Genius for the song
                query = urllib.parse.quote(f"{song['title']} {song['artist']}")
                headers = {"Authorization": f"Bearer {genius_token}"}
                search = requests.get(f"https://api.genius.com/search?q={query}", headers=headers, timeout=5)
                hits = search.json().get("response", {}).get("hits", [])

                if not hits:
                    print(f"  ✗ [{attempted}] {song['title']} — not found on Genius")
                    continue

                song_url = hits[0]["result"]["url"]
                page = requests.get(song_url, timeout=10)
                soup = BeautifulSoup(page.text, "html.parser")

                containers = soup.find_all("div", attrs={"data-lyrics-container": "true"})
                if not containers:
                    print(f"  ✗ [{attempted}] {song['title']} — lyrics container not found")
                    continue

                lines = []
                for container in containers:
                    for br in container.find_all("br"):
                        br.replace_with("\n")
                    text = container.get_text(separator="\n")
                    lines.extend([l.strip() for l in text.split("\n") if l.strip()])

                if lines:
                    song["lyrics"] = '\n'.join(lines)
                    song["lyrics_first_line"] = lines[0]
                    results.append(song)
                    print(f"  ✓ [{len(results)}/{target}] {song['title']}")
                else:
                    print(f"  ✗ [{attempted}] {song['title']} — empty lyrics")

                time.sleep(0.5)
            except Exception as e:
                print(f"  ✗ Lyrics error for {song['title']}: {e}")

        print(f"Lyrics fetch complete: {len(results)}/{attempted} songs had lyrics")
        return results

    @task()
    def analyze_sentiment(songs):
        from groq import Groq

        client = Groq(api_key=Variable.get("GROQ_API_KEY"))

        analyze_count = min(len(songs), 100)
        print(f"analyze_sentiment received {len(songs)} songs, analyzing {analyze_count}")

        songs_text = "\n\n".join([
            f"Song {i+1}: '{s['title']}' by {s['artist']}\nLyrics:\n{s['lyrics']}"
            for i, s in enumerate(songs[:analyze_count])
        ])

        prompt = f"""You are a qualitative data analyst examining {analyze_count} Christian songs. Analyze the lyrics and return ONLY valid JSON in this format:
{{
  "executive_summary": "one sentence capturing the most important finding",
  "emotional_analysis": {{
    "categories": [
      {{"name": "...", "count": 0, "percentage": 0, "songs": []}}
    ]
  }},
  "strongest_evidence_backed_claim": {{
    "claim": "...",
    "evidence": "...",
    "confidence": "HIGH/MEDIUM/LOW"
  }},
  "listener_profile": {{
    "evidence_based_inference": "..."
  }}
}}

SONGS:
{songs_text}"""

        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=4000
        )

        response_text = response.choices[0].message.content
        if "```" in response_text:
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]

        analysis = json.loads(response_text.strip())
        print(f"Analysis complete: {analysis['executive_summary']}")

        return {"songs": songs, "analyzed_count": analyze_count, "analysis": analysis}

    @task()
    def store_to_gcs(payload):
        from google.cloud import storage
        from google.oauth2 import service_account

        key_json = Variable.get("GCP_SERVICE_ACCOUNT_KEY")
        key_dict = json.loads(key_json)
        credentials = service_account.Credentials.from_service_account_info(key_dict)
        client = storage.Client(credentials=credentials, project="holystening-pipeline")

        bucket = client.bucket("holystening-data")
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        filename = f"christian_songs_{timestamp}.json"

        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(payload, indent=2), content_type="application/json")

        print(f"Stored to gs://holystening-data/{filename}")
        return filename

    songs = fetch_top_songs()
    songs_with_lyrics = fetch_lyrics(songs)
    results = analyze_sentiment(songs_with_lyrics)
    store_to_gcs(results)


christian_top_songs()
