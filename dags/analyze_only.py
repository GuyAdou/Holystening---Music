"""
Run only the analysis + GCS store steps using lyrics already in Airflow XCom.

    docker exec holystening-airflow-worker-1 python /opt/airflow/dags/analyze_only.py
"""

import json
import random
import sys
from collections import Counter, defaultdict
from datetime import datetime

import psycopg2
from google.cloud import storage
from google.oauth2.service_account import Credentials
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

from airflow.models import Variable


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_json(text, prompt=None, model=None, gen_config=None):
    def _clean(t):
        # Try to find JSON object or array anywhere in the text
        t = t.strip()
        if "```" in t:
            parts = t.split("```")
            for part in parts:
                if part.startswith("json"):
                    part = part[4:]
                part = part.strip()
                if part.startswith("{") or part.startswith("["):
                    t = part
                    break
        # Find first { or [ and last } or ]
        start = min(
            (t.find("{") if t.find("{") != -1 else len(t)),
            (t.find("[") if t.find("[") != -1 else len(t))
        )
        if start < len(t):
            t = t[start:]
        return t.strip()
    try:
        return json.loads(_clean(text))
    except json.JSONDecodeError as e:
        print(f"  JSON parse failed: {e}. Raw response (first 500 chars): {text[:500]}")
        if model and prompt:
            print("  Retrying with explicit JSON instruction...")
            retry_response = model.generate_content(
                f"Return ONLY valid JSON with no explanation, no markdown, no code fences. Start your response with {{ or [.\n\n{prompt}",
                generation_config=gen_config
            )
            return json.loads(_clean(retry_response.text))
        raise


def get_songs_from_xcom():
    conn = psycopg2.connect("postgresql://airflow:airflow@postgres/airflow")
    cur = conn.cursor()
    cur.execute("""
        SELECT value FROM xcom
        WHERE dag_id = 'christian_top_songs'
          AND task_id = 'fetch_lyrics'
        ORDER BY timestamp DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        print("No XCom found for fetch_lyrics.")
        sys.exit(1)
    val = row[0]
    return val if isinstance(val, list) else json.loads(val)


# ── Step 1: Per-batch extraction ──────────────────────────────────────────────

def extract_all(songs, analyze_count, model):
    cfg = GenerationConfig(temperature=0.1, max_output_tokens=8192)
    batch_size = 10
    total_batches = (analyze_count + batch_size - 1) // batch_size
    all_extracted = []

    for batch_start in range(0, analyze_count, batch_size):
        batch = songs[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        print(f"  Extracting batch {batch_num}/{total_batches} ({len(batch)} songs)...")

        songs_text = "\n\n".join([
            f"Song {batch_start + i + 1}: '{s['title']}' by {s['artist']}\nLyrics:\n{s['lyrics']}"
            for i, s in enumerate(batch)
        ])

        prompt = f"""You are extracting structured data from Christian songs for later analysis. For each song below, return precise observations grounded in the actual lyrics. Do NOT inflate ordinary devotional content into hidden complexity — if a song is straightforwardly about praise, gratitude, or God's faithfulness, label it that way. Only assign a more specific or unusual theme if a specific lyric supports it.

If lyrics are not in English, work in the original language but write output in English. If lyrics are too short or fragmentary to assess, set primary_theme to "insufficient_lyrics".

Return ONLY valid JSON:
{{
  "songs": [
    {{
      "title": "...",
      "artist": "...",
      "primary_theme": "4-10 word plain-language description of what the song is actually about",
      "secondary_theme": "4-10 words OR null if single-themed",
      "addressee": "one of: God, self, listener, named_person, unclear",
      "emotional_register": "one of: praise, lament, confession, declaration, longing, gratitude, exhortation, narrative, other",
      "evidence_quote": "exact lyric, 5-20 words, that best supports primary_theme",
      "language": "ISO code: en, es, pt, etc."
    }}
  ]
}}

SONGS:
{songs_text}"""

        result = parse_json(model.generate_content(prompt, generation_config=cfg).text, prompt, model, cfg)
        all_extracted.extend(result["songs"])

    return all_extracted


# ── Step 2: Derive category names from extracted data in Python ───────────────

def derive_category_names(all_extracted):
    """
    Group primary_theme by emotional_register to produce data-driven
    category names without an extra API call (which triggers recitation filters).
    """
    register_themes = defaultdict(list)
    for s in all_extracted:
        reg = s.get("emotional_register", "other")
        theme = s.get("primary_theme", "")
        if theme and theme != "insufficient_lyrics":
            register_themes[reg].append(theme)

    # Map registers to readable category names, merging small ones into "other"
    register_map = {
        "praise": "Praise & Adoration",
        "gratitude": "Gratitude & Testimony",
        "lament": "Lament & Honest Struggle",
        "confession": "Personal Confession & Transformation",
        "declaration": "Declaration of God's Character",
        "longing": "Longing & Surrender",
        "exhortation": "Call to Action & Evangelism",
        "narrative": "Narrative & Testimony",
        "other": "Other",
    }

    # Only keep categories with at least 2 songs; fold the rest into "Other"
    category_names = []
    other_count = 0
    for reg, themes in sorted(register_themes.items(), key=lambda x: -len(x[1])):
        if len(themes) >= 2:
            name = register_map.get(reg, reg.title())
            if name not in category_names:
                category_names.append(name)
        else:
            other_count += 1

    if other_count > 0 and "Other" not in category_names:
        category_names.append("Other")

    return category_names


# ── Step 3: Tag songs in Python using emotional_register ─────────────────────

REGISTER_TO_CATEGORY = {
    "praise":      "Praise & Adoration",
    "gratitude":   "Gratitude & Testimony",
    "lament":      "Lament & Honest Struggle",
    "confession":  "Personal Confession & Transformation",
    "declaration": "Declaration of God's Character",
    "longing":     "Longing & Surrender",
    "exhortation": "Call to Action & Evangelism",
    "narrative":   "Narrative & Testimony",
    "other":       "Other",
}

def tag_songs_python(all_extracted):
    tagged = []
    for s in all_extracted:
        reg = s.get("emotional_register", "other")
        category = REGISTER_TO_CATEGORY.get(reg, "Other")
        tagged.append({"title": s["title"], "artist": s["artist"], "category": category})
    return tagged


# ── Step 4: Python counting ───────────────────────────────────────────────────

def build_counts(tagged, all_extracted, category_names, songs, analyze_count):
    category_songs_map = defaultdict(list)
    for s in tagged:
        category_songs_map[s["category"]].append(f"{s['title']} – {s['artist']}")

    category_list = []
    for name in category_names:
        songs_in_cat = category_songs_map.get(name, [])
        count = len(songs_in_cat)
        percentage = round(count / analyze_count * 100, 1)
        category_list.append({"name": name, "count": count, "percentage": percentage, "songs": songs_in_cat})
    category_list.sort(key=lambda x: x["count"], reverse=True)

    register_counter = Counter(s.get("emotional_register", "other") for s in all_extracted)
    language_counter = Counter(s.get("language", "en") for s in all_extracted)
    artist_counter = Counter(s["artist"] for s in songs[:analyze_count])

    return category_list, register_counter, language_counter, artist_counter


# ── Step 5: Synthesis with real counts ───────────────────────────────────────

def synthesize(all_extracted, category_list, register_counter, language_counter, artist_counter, analyze_count, model):
    cfg = GenerationConfig(temperature=0.3, max_output_tokens=4096)

    category_counts = "\n".join([f"- {c['name']}: {c['count']} songs ({c['percentage']}%)" for c in category_list])
    register_counts = "\n".join([f"- {k}: {v}" for k, v in register_counter.most_common()])
    language_counts = "\n".join([f"- {k}: {v}" for k, v in language_counter.most_common()])
    artist_counts_text = "\n".join([f"- {a}: {c}" for a, c in artist_counter.most_common(10)])

    sample_songs = random.sample(all_extracted, min(15, len(all_extracted)))
    sampled_lyrics = "\n".join([
        f'"{s.get("evidence_quote", "")}" — {s["title"]} by {s["artist"]}' for s in sample_songs
    ])

    per_song_themes = "\n".join([
        f"- {s['title']} ({s['artist']}): {s['primary_theme']}" for s in all_extracted
    ])

    prompt = f"""You are writing an analytical note about {analyze_count} Christian songs currently trending on Spotify.

ALL COUNTS ARE PRE-COMPUTED — use them exactly, do not invent percentages:

Category breakdown:
{category_counts}

Emotional register breakdown:
{register_counts}

Language breakdown:
{language_counts}

Top artists by song count:
{artist_counts_text}

Sample of 15 lyric excerpts:
{sampled_lyrics}

All song themes:
{per_song_themes}

INSTRUCTIONS: Report honestly. If the picture is conventional, say so plainly. Only call something distinctive if at least 3 specific songs support it. Do not infer listener demographics — describe the spiritual/emotional situations the lyrics address.

Return ONLY valid JSON:
{{
  "headline_finding": "one honest sentence with a real number from the pre-computed data above",
  "notable_patterns": [
    {{
      "pattern": "specific observation grounded in actual song titles",
      "supporting_songs": ["Song – Artist", "Song – Artist", "Song – Artist"],
      "supporting_quote": "one exact lyric from the samples above",
      "caveat": "what would weaken this claim, or null"
    }}
  ],
  "what_the_lyrics_address": "2-3 sentences on the emotional/spiritual situations these lyrics speak to, referencing actual song patterns",
  "artist_concentration_note": "flag any artist with >10% of songs by name, or null",
  "data_limitations": ["short honest list"]
}}"""

    result = parse_json(model.generate_content(prompt, generation_config=cfg).text, prompt, model, cfg)
    return result


# ── Store ──────────────────────────────────────────────────────────────────────

def store(songs, analyzed_count, category_list, register_counter, language_counter, synthesis):
    key_dict = json.loads(Variable.get("GCP_SERVICE_ACCOUNT_KEY"))
    credentials = Credentials.from_service_account_info(key_dict)
    client = storage.Client(credentials=credentials, project="holystening-pipeline")

    analysis = {
        "headline_finding": synthesis["headline_finding"],
        "emotional_analysis": {"categories": category_list},
        "notable_patterns": synthesis["notable_patterns"],
        "what_the_lyrics_address": synthesis["what_the_lyrics_address"],
        "artist_concentration_note": synthesis.get("artist_concentration_note"),
        "data_limitations": synthesis.get("data_limitations", []),
        "register_breakdown": dict(register_counter),
        "language_breakdown": dict(language_counter),
    }

    payload = {"songs": songs, "analyzed_count": analyzed_count, "analysis": analysis}
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    filename = f"christian_songs_{timestamp}.json"

    bucket = client.bucket("holystening-data")
    bucket.blob(filename).upload_from_string(json.dumps(payload, indent=2), content_type="application/json")
    print(f"Stored to gs://holystening-data/{filename}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    songs = get_songs_from_xcom()
    print(f"Loaded {len(songs)} songs from XCom")

    key_dict = json.loads(Variable.get("GCP_SERVICE_ACCOUNT_KEY"))
    credentials = Credentials.from_service_account_info(
        key_dict, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    vertexai.init(project="holystening-pipeline", location="us-central1", credentials=credentials)
    model = GenerativeModel("gemini-2.5-flash")

    analyze_count = min(len(songs), 100)

    print(f"\nStep 1: Extracting themes from {analyze_count} songs...")
    all_extracted = extract_all(songs, analyze_count, model)

    print(f"\nStep 2: Deriving category names from extracted data...")
    category_names = derive_category_names(all_extracted)
    print(f"  Categories: {category_names}")

    print(f"\nStep 3: Tagging {len(all_extracted)} songs (Python, no API)...")
    tagged = tag_songs_python(all_extracted)

    print(f"\nStep 4: Counting...")
    category_list, register_counter, language_counter, artist_counter = build_counts(
        tagged, all_extracted, category_names, songs, analyze_count
    )
    for c in category_list:
        print(f"  {c['name']}: {c['count']} ({c['percentage']}%)")

    print(f"\nStep 5: Synthesizing...")
    synthesis = synthesize(all_extracted, category_list, register_counter, language_counter, artist_counter, analyze_count, model)
    print(f"  Headline: {synthesis['headline_finding']}")

    store(songs, analyze_count, category_list, register_counter, language_counter, synthesis)
    print("\nDone.")
