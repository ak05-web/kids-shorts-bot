"""
=============================================================
 KIDS YOUTUBE SHORTS PIPELINE v3 — ZERO INVESTMENT
 3 Videos/Day · English US · Full SEO · US EST Schedule
=============================================================

 TOTAL COST: $0.00 — no credit card required anywhere.

 FREE STACK:
   Google Gemini API   → script + SEO (free, no card)
   Microsoft Edge TTS  → English voiceover (free, no card)
   Pollinations.ai     → cartoon images (free, no card)
   Mixkit CDN          → royalty-free music (free, no card)
   FFmpeg              → video assembly (open source)
   YouTube Data API v3 → auto-upload (free quota)
   GitHub Actions      → daily scheduler (free on public repo)

 HOW TO GET GEMINI API KEY (zero investment):
   1. Go to: aistudio.google.com
   2. Sign in with any Google account (free)
   3. Click "Get API key" → "Create API key" → copy it
   4. No credit card. No billing. No limit prompt.
   Free tier: 15 requests/min · 1,000,000 tokens/day

 DAILY SCHEDULE (US Eastern Time):
   Slot 1 → 8:00 AM EST  → Animal Facts
   Slot 2 → 3:00 PM EST  → Dinosaur Facts
   Slot 3 → 7:00 PM EST  → Ocean Animals

 INSTALL:
   pip install google-generativeai edge-tts pytrends \
               requests pytz google-api-python-client \
               google-auth-oauthlib Pillow

   brew install ffmpeg        (Mac)
   sudo apt install ffmpeg    (Linux/GitHub Actions)

 USAGE:
   python pipeline_v3.py --slot 1
   python pipeline_v3.py --slot 2
   python pipeline_v3.py --slot 3
=============================================================
"""

import os
import sys
import json
import time
import asyncio
import argparse
import requests
import subprocess
import pickle
from pathlib import Path
from datetime import datetime, timedelta

import pytz
from groq import Groq

# ─────────────────────────────────────────────────────────────
# CONFIG — fill GEMINI_API_KEY only, everything else is free
# ─────────────────────────────────────────────────────────────

# Get free key at: aistudio.google.com  (no credit card)
# NEW — replace with these:
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = "llama-3.3-70b-versatile"

YOUTUBE_CLIENT_FILE = "client_secrets.json"
YOUTUBE_TOKEN_FILE  = "youtube_token.pickle"

# Language — locked to English US everywhere
LANGUAGE_CODE       = "en"
LANGUAGE_LOCALE     = "en-US"


# Video dimensions (YouTube Shorts standard)
VIDEO_WIDTH         = 1080
VIDEO_HEIGHT        = 1920
THUMBNAIL_WIDTH     = 1280
THUMBNAIL_HEIGHT    = 720
FPS                 = 30
NUM_SCENES          = 8
MUSIC_VOLUME        = 0.18
CHANNEL_NAME        = "WOW Animals!"

# US Eastern timezone (handles EST/EDT daylight saving automatically)
US_EASTERN = pytz.timezone("America/New_York")


# ─────────────────────────────────────────────────────────────
# SLOT CONFIGURATION — 3 niches, 3 upload times
# ─────────────────────────────────────────────────────────────

SLOTS = {
    1: {
        "name"        : "Animal Facts",
        "niche"       : "fun animal facts for kids",
        "publish_hour": 8,
        "publish_min" : 0,
        "color_theme" : "4CAF50",   # green
        "topic_pool"  : [
            "octopus", "axolotl", "platypus", "mantis shrimp", "tardigrade",
            "chameleon", "mimic octopus", "archerfish", "pistol shrimp", "narwhal",
            "aye-aye", "pangolin", "capybara", "blue-footed booby", "shoebill stork",
            "dumbo octopus", "glass frog", "star-nosed mole", "naked mole rat",
            "quokka", "proboscis monkey", "dugong", "yeti crab", "fossa",
            "secretary bird", "saiga antelope", "blobfish", "wombat", "kinkajou",
            "tapir", "okapi", "binturong", "cassowary", "sun bear",
        ],
    },
    2: {
        "name"        : "Dinosaur Facts",
        "niche"       : "dinosaur facts for kids",
        "publish_hour": 15,
        "publish_min" : 0,
        "color_theme" : "FF6B35",   # orange
        "topic_pool"  : [
            "T-Rex", "velociraptor", "triceratops", "stegosaurus", "brachiosaurus",
            "ankylosaurus", "pterodactyl", "spinosaurus", "diplodocus", "allosaurus",
            "pachycephalosaurus", "parasaurolophus", "iguanodon", "carnotaurus",
            "therizinosaurus", "deinonychus", "mosasaurus", "plesiosaur",
            "microraptor", "oviraptor", "gallimimus", "ceratosaurus",
            "argentinosaurus", "giganotosaurus", "troodon", "baryonyx",
        ],
    },
    3: {
        "name"        : "Ocean Animals",
        "niche"       : "ocean animals for kids",
        "publish_hour": 19,
        "publish_min" : 0,
        "color_theme" : "2196F3",   # blue
        "topic_pool"  : [
            "blue whale", "great white shark", "manta ray", "giant squid",
            "seahorse", "clownfish", "anglerfish", "jellyfish",
            "dolphin", "sea turtle", "starfish", "electric eel", "puffer fish",
            "hammerhead shark", "beluga whale", "manatee", "cuttlefish",
            "moray eel", "lobster", "hermit crab", "sea otter",
            "humpback whale", "orca", "nautilus", "flying fish",
            "bioluminescent squid", "vampire squid", "sunfish",
        ],
    },
}

# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────

def setup_dirs(slot: int) -> Path:
    base = Path(f"output/slot_{slot}")
    for sub in ["scenes", "audio", "images"]:
        (base / sub).mkdir(parents=True, exist_ok=True)
    Path("assets").mkdir(exist_ok=True)
    return base

def log(stage: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{stage.upper():10}] {msg}")

def run_cmd(cmd: list, label: str = "cmd"):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{result.stderr[-800:]}")
    return result

def get_audio_duration(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True
    )
    try:
        return float(r.stdout.strip())
    except Exception:
        return 7.0

def clean_json(text: str) -> str:
    """Strip markdown code fences that LLMs sometimes wrap around JSON."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```json or ```) and last line (```)
        lines = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(lines)
    return text.strip()


# ─────────────────────────────────────────────────────────────
# STAGE 1 — TOPIC PICKER (PyTrends — free)
# ─────────────────────────────────────────────────────────────

def pick_topic(slot_cfg: dict) -> str:
    log("topic", f"Picking topic for: {slot_cfg['name']}")
    try:
        from pytrends.request import TrendReq
        pt = TrendReq(hl=LANGUAGE_LOCALE, tz=300)
        pt.build_payload([slot_cfg["niche"]], timeframe="now 7-d", geo="US")
        related = pt.related_queries()
        queries = related.get(slot_cfg["niche"], {}).get("top")
        if queries is not None and not queries.empty:
            topic = queries["query"].iloc[0]
            log("topic", f"PyTrends: {topic}")
            return topic
    except Exception as e:
        log("topic", f"PyTrends unavailable ({e})")

    # Day-of-year rotation so topics don't repeat within a month
    pool = slot_cfg["topic_pool"]
    idx  = (datetime.now().timetuple().tm_yday + slot_cfg["publish_hour"]) % len(pool)
    topic = pool[idx]
    log("topic", f"Pool rotation: {topic}")
    return topic


# ─────────────────────────────────────────────────────────────
# STAGE 2 — SCRIPT + FULL SEO (Google Gemini — free, no card)
# ─────────────────────────────────────────────────────────────

def generate_script_and_seo(topic: str, slot_cfg: dict) -> dict:
    """
    Uses Google Gemini 1.5 Flash (free tier).
    Generates complete script + all YouTube SEO data in one call.

    Free tier limits:
      - 15 requests per minute
      - 1,000,000 tokens per day
      - No credit card required
    """
    log("groq", f"Generating script + SEO for: {topic}")

    client = Groq(api_key=GROQ_API_KEY)

    prompt = f"""You are a YouTube Shorts scriptwriter AND YouTube SEO specialist.

Channel: "{CHANNEL_NAME}"
Language: English (US) — all text must be in English
Audience: Kids aged 4-10
Niche: {slot_cfg['niche']}
Topic: {topic}

Generate a complete production package. Your entire response must be valid JSON only.
Do not include any explanation, markdown, or code fences — just the raw JSON object.

{{
  "topic": "{topic}",

  "seo": {{
    "title_main": "...(max 60 chars, starts with power word, includes topic + for Kids)",
    "title_ab":   "...(alternative title, max 60 chars, different angle)",
    "description": "...(hook sentence first. Then 3 sentences about the topic. End with subscribe CTA. 350-450 chars.)",
    "chapters": [
      {{"time": "0:00", "label": "Intro"}},
      {{"time": "0:07", "label": "..."}},
      {{"time": "0:14", "label": "..."}},
      {{"time": "0:21", "label": "..."}},
      {{"time": "0:28", "label": "..."}},
      {{"time": "0:35", "label": "..."}},
      {{"time": "0:42", "label": "..."}},
      {{"time": "0:49", "label": "..."}},
      {{"time": "0:56", "label": "Subscribe"}}
    ],
    "tags": [
      "{topic}", "{topic} for kids", "{topic} facts", "{topic} shorts",
      "{topic} educational", "{slot_cfg['niche']}", "animal facts for kids",
      "kids learning", "educational shorts", "kids youtube", "science for kids",
      "nature for kids", "amazing animals", "wow facts", "kids education",
      "learning for kids", "children educational", "kids channel", "fun facts",
      "fun facts for kids"
    ],
    "hashtags": ["#Shorts", "#KidsLearning", "#AnimalFacts", "#WOWAnimals", "#EducationForKids"],
    "pinned_comment": "...(1-2 sentences with main keyword, ends with a question kids can answer, uses emoji)",
    "thumbnail_prompt": "...(image prompt: bright cartoon Pixar style, {topic} animal close-up, happy surprised face, vibrant background, no text, child-friendly)"
  }},

  "hook": "...(exciting opening question max 10 words shown as text overlay)",
  "cta":  "...(end card call-to-action max 10 words)",

  "scenes": [
    {{
      "scene_number": 1,
      "narration":    "...(English US, max 20 words, simple vocabulary, excited tone)",
      "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...(describe {topic} scene)",
      "text_overlay": "...(3-6 words ALL CAPS plus emoji)"
    }},
    {{"scene_number": 2, "narration": "...", "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...", "text_overlay": "..."}},
    {{"scene_number": 3, "narration": "...", "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...", "text_overlay": "..."}},
    {{"scene_number": 4, "narration": "...", "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...", "text_overlay": "..."}},
    {{"scene_number": 5, "narration": "...", "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...", "text_overlay": "..."}},
    {{"scene_number": 6, "narration": "...", "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...", "text_overlay": "..."}},
    {{"scene_number": 7, "narration": "...", "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...", "text_overlay": "..."}},
    {{"scene_number": 8, "narration": "...", "image_prompt": "Cute cartoon Pixar style, bright vibrant colors, child-friendly, ...", "text_overlay": "..."}}
  ]
}}

Rules:
- Exactly 8 scenes, no more, no less
- All narration in simple English, max 20 words each
- Each text_overlay uses ALL CAPS + emoji (e.g. "3 HEARTS! 💙💙💙")
- Each scene has one wow/surprising fact
- tags array must have exactly 20 items
- hashtags array must have exactly 5 items
- description must be kid-safe and educational
- RESPOND WITH RAW JSON ONLY — no markdown, no explanation
"""

    # Retry up to 3 times (Groq free tier can occasionally time out)

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a YouTube Shorts scriptwriter and SEO specialist. Always respond with valid JSON only. No markdown, no explanation, no code fences."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.7,
                max_tokens=3000,
            )
            raw  = clean_json(response.choices[0].message.content)
            data = json.loads(raw)
            break
        except json.JSONDecodeError as e:
            log("groq", f"JSON parse error (attempt {attempt+1}): {e}")
            if attempt == 2:
                raise
            time.sleep(4)
        except Exception as e:
            log("groq", f"API error (attempt {attempt+1}): {e}")
            if attempt == 2:
                raise
            time.sleep(10)

    # Append chapters + hashtags to description
    chapters_text = "\n".join(
        f"{ch['time']} {ch['label']}"
        for ch in data["seo"]["chapters"]
    )
    data["seo"]["description"] = (
        data["seo"]["description"].rstrip()
        + f"\n\n⏱ CHAPTERS\n{chapters_text}"
        + f"\n\n{' '.join(data['seo']['hashtags'])}"
    )

    log("groq", f"Script ready: '{data['seo']['title_main']}'")
    log("groq", f"Scenes: {len(data['scenes'])} | Tags: {len(data['seo']['tags'])}")
    return data


# ─────────────────────────────────────────────────────────────
# STAGE 3 — ENGLISH VOICEOVER (Edge TTS — 100% free, no card)
# ─────────────────────────────────────────────────────────────

def generate_voiceover(data: dict, out_dir: Path) -> list[Path]:
    from gtts import gTTS
    log("voice", "Generating English voiceover (gTTS)...")
    audio_dir = out_dir / "audio"
    paths     = []
    for scene in data["scenes"]:
        n    = scene["scene_number"]
        out  = audio_dir / f"scene_{n:02d}.mp3"
        tts  = gTTS(text=scene["narration"], lang="en", tld="us", slow=False)
        tts.save(str(out))
        log("voice", f"  Scene {n}: done")
        paths.append(out)
        time.sleep(0.3)
    return paths


# ─────────────────────────────────────────────────────────────
# STAGE 4 — CARTOON IMAGES + SEO THUMBNAIL (Pollinations.ai)
# ─────────────────────────────────────────────────────────────

def _fetch_image(prompt: str, w: int, h: int, seed: int, out: Path) -> bool:
    from urllib.parse import quote
    url = (
        f"https://image.pollinations.ai/prompt/{quote(prompt)}"
        f"?width={w}&height={h}&seed={seed}&nologo=true&model=flux"
    )
    try:
        r = requests.get(url, timeout=90)
        if r.status_code == 200 and len(r.content) > 5000:
            out.write_bytes(r.content)
            return True
    except Exception:
        pass
    return False

def _fallback_image(out: Path, w: int, h: int, color: str = "purple"):
    run_cmd(["ffmpeg", "-y", "-f", "lavfi",
             "-i", f"color=c={color}:size={w}x{h}:duration=1",
             "-frames:v", "1", str(out)], "img-fallback")

def generate_images(data: dict, out_dir: Path) -> tuple[list[Path], Path]:
    log("images", "Generating cartoon images + thumbnail...")
    img_dir = out_dir / "images"
    paths   = []

    for scene in data["scenes"]:
        n   = scene["scene_number"]
        out = img_dir / f"scene_{n:02d}.jpg"
        prompt = scene["image_prompt"] + ", no text, no watermark, vertical 9:16"
        ok = False
        for attempt in range(3):
            ok = _fetch_image(prompt, VIDEO_WIDTH, VIDEO_HEIGHT, n * 37 + attempt, out)
            if ok:
                break
            log("images", f"  Scene {n}: retry {attempt + 1}...")
            time.sleep(5)
        if not ok:
            _fallback_image(out, VIDEO_WIDTH, VIDEO_HEIGHT)
        paths.append(out)
        log("images", f"  Scene {n}: {'ok' if ok else 'fallback'}")
        time.sleep(3.0)

    # SEO Thumbnail (horizontal 16:9)
    thumb_raw   = img_dir / "thumb_raw.jpg"
    thumb_final = img_dir / "thumb_final.jpg"
    ok = _fetch_image(
        data["seo"]["thumbnail_prompt"] + ", horizontal landscape, no text",
        THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT, 9999, thumb_raw
    )
    if not ok:
        _fallback_image(thumb_raw, THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT, "blue")

    # Burn title onto thumbnail
    safe_title = data["seo"]["title_main"][:40].replace("'", "\\'").replace(":", "\\:")
    run_cmd([
        "ffmpeg", "-y", "-i", str(thumb_raw),
        "-vf",
        (
            f"drawtext=text='{safe_title}':"
            f"fontsize=72:fontcolor=white:borderw=6:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.76:"
            f"box=1:boxcolor=black@0.45:boxborderw=10"
        ),
        str(thumb_final)
    ], "Thumbnail")

    log("images", f"Done: {len(paths)} scenes + thumbnail")
    return paths, thumb_final


# ─────────────────────────────────────────────────────────────
# STAGE 5 — BACKGROUND MUSIC (Mixkit CDN — free, no card)
# ─────────────────────────────────────────────────────────────

MUSIC_TRACKS = [
    "https://assets.mixkit.co/music/preview/mixkit-fun-and-quirky-122.mp3",
    "https://assets.mixkit.co/music/preview/mixkit-cheerful-fun-and-quirky-268.mp3",
    "https://assets.mixkit.co/music/preview/mixkit-kids-fun-game-show-248.mp3",
    "https://assets.mixkit.co/music/preview/mixkit-life-is-a-dream-837.mp3",
]

def get_music(slot: int) -> Path:
    path = Path(f"assets/bgmusic_slot{slot}.mp3")
    if path.exists():
        return path
    url = MUSIC_TRACKS[(slot - 1) % len(MUSIC_TRACKS)]
    log("music", f"Downloading: {url}")
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200 and len(r.content) > 10_000:
            path.write_bytes(r.content)
            log("music", "Music cached.")
            return path
    except Exception:
        pass
    # Silent fallback
    run_cmd(["ffmpeg", "-y", "-f", "lavfi", "-i",
             "anullsrc=r=44100:cl=stereo", "-t", "70", str(path)])
    return path


# ─────────────────────────────────────────────────────────────
# STAGE 6 — VIDEO ASSEMBLY (FFmpeg — open source, free)
# ─────────────────────────────────────────────────────────────

def build_scene_clip(image: Path, audio: Path, overlay: str, n: int, out: Path):
    dur     = get_audio_duration(audio) + 0.4
    safe_ov = overlay.replace("'", "\\'").replace(":", "\\:")

    zoom = (
        f"scale={VIDEO_WIDTH*2}:{VIDEO_HEIGHT*2},"
        f"zoompan=z='min(zoom+0.0007,1.06)':d={int(dur*FPS)}:"
        f"x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={FPS},setsar=1"
    )
    fade_in  = 0.25
    fade_out = round(dur - 0.25, 3)
    txt = (
        f"drawtext=text='{safe_ov}':"
        f"fontsize=66:fontcolor=white:borderw=5:bordercolor=black:"
        f"x=(w-text_w)/2:y=h*0.72:"
        f"alpha='if(lt(t,{fade_in}),t/{fade_in},if(gt(t,{fade_out}),({dur}-t)/{fade_in},1))'"
    )
    run_cmd([
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(image),
        "-i", str(audio),
        "-filter_complex", f"[0:v]{zoom},{txt}[v]",
        "-map", "[v]", "-map", "1:a",
        "-t", str(dur),
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-ar", "44100", "-shortest",
        str(out),
    ], f"Scene-{n}")

def build_card(text1: str, text2: str, text3: str, color: str, out: Path, dur: float):
    t1 = text1.replace("'", "\\'").replace(":", "\\:")
    t2 = text2.replace("'", "\\'").replace(":", "\\:")
    t3 = text3.replace("'", "\\'").replace(":", "\\:")
    run_cmd([
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"color=c={color}:size={VIDEO_WIDTH}x{VIDEO_HEIGHT}:duration={dur}:rate={FPS}",
        "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo:d={dur}",
        "-vf",
        (
            f"drawtext=text='{t1}':fontsize=72:fontcolor=white:"
            f"borderw=5:bordercolor=black:x=(w-text_w)/2:y=h*0.36,"
            f"drawtext=text='{t2}':fontsize=54:fontcolor=yellow:"
            f"borderw=4:bordercolor=black:x=(w-text_w)/2:y=h*0.50,"
            f"drawtext=text='{t3}':fontsize=46:fontcolor=white:"
            f"borderw=3:bordercolor=black:x=(w-text_w)/2:y=h*0.63"
        ),
        "-map", "0:v", "-map", "1:a",
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-ar", "44100", "-shortest",
        str(out)
    ], "Card")
   
def assemble_video(
    data: dict, slot_cfg: dict,
    images: list[Path], audios: list[Path],
    music: Path, out_dir: Path
) -> Path:
    log("video", "Assembling Short...")
    sd    = out_dir / "scenes"
    color = slot_cfg["color_theme"]
    clips = []

    # Title card
    tc = sd / "title.mp4"
    build_card(data["hook"], data["seo"]["title_main"][:45], CHANNEL_NAME, color, tc, 2.5)
    clips.append(tc)

    # Scene clips
    for img, aud, scene in zip(images, audios, data["scenes"]):
        sc = sd / f"clip_{scene['scene_number']:02d}.mp4"
        build_scene_clip(img, aud, scene["text_overlay"], scene["scene_number"], sc)
        clips.append(sc)

    # End card
    ec = sd / "end.mp4"
    build_card("SUBSCRIBE for more!", data["cta"], CHANNEL_NAME, color, ec, 3.0)
    clips.append(ec)

    # Concatenate
    concat_txt = out_dir / "concat.txt"
    concat_txt.write_text("\n".join(f"file '{p.resolve()}'" for p in clips))
    raw = out_dir / "raw.mp4"
    run_cmd(["ffmpeg", "-y", "-f", "concat", "-safe", "0",
             "-i", str(concat_txt), "-c", "copy", str(raw)], "Concat")

    # Mix background music
    total_dur = get_audio_duration(raw)
    date_str  = datetime.now().strftime("%Y%m%d")
    slot_name = slot_cfg["name"].replace(" ", "_").lower()
    final     = out_dir / f"short_{slot_name}_{date_str}.mp4"

    run_cmd([
        "ffmpeg", "-y",
        "-i", str(raw),
        "-stream_loop", "-1", "-i", str(music),
        "-filter_complex",
        (
            f"[1:a]volume={MUSIC_VOLUME},atrim=0:{total_dur}[bg];"
            "[0:a][bg]amix=inputs=2:duration=first:dropout_transition=2[aout]"
        ),
        "-map", "0:v", "-map", "[aout]",
        "-c:v", "copy", "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart", str(final)
    ], "MixAudio")

    sz = final.stat().st_size // (1024 * 1024)
    log("video", f"Final: {final.name} ({sz} MB)")
    return final


# ─────────────────────────────────────────────────────────────
# STAGE 7 — SEO METADATA BUILDER
# ─────────────────────────────────────────────────────────────

def build_seo_meta(data: dict) -> dict:
    seo      = data["seo"]
    raw_tags = [str(t)[:30] for t in seo.get("tags", [])]
    while raw_tags and len(",".join(raw_tags)) > 490:
        raw_tags.pop()

    return {
        "title"                  : seo["title_main"][:100],
        "title_ab"               : seo.get("title_ab", seo["title_main"])[:100],
        "description"            : seo["description"][:5000],
        "tags"                   : raw_tags,
        "pinned_comment"         : seo.get("pinned_comment", ""),
        "categoryId"             : "27",       # Education
        "defaultLanguage"        : LANGUAGE_CODE,
        "defaultAudioLanguage"   : LANGUAGE_CODE,
        "madeForKids"            : True,
        "selfDeclaredMadeForKids": True,
    }


# ─────────────────────────────────────────────────────────────
# STAGE 8 — YOUTUBE UPLOAD (Data API v3 — free quota)
# ─────────────────────────────────────────────────────────────

def get_publish_time_utc(slot_cfg: dict) -> str:
    """Compute next publish time in US Eastern, return as UTC string."""
    now_est     = datetime.now(US_EASTERN)
    publish_est = now_est.replace(
        hour=slot_cfg["publish_hour"],
        minute=slot_cfg["publish_min"],
        second=0, microsecond=0
    )
    if now_est >= publish_est:
        publish_est += timedelta(days=1)
    publish_utc = publish_est.astimezone(pytz.utc)
    return publish_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def youtube_upload(
    video_path: Path,
    thumbnail_path: Path,
    seo_meta: dict,
    slot_cfg: dict,
) -> str:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    # ── Credentials from env vars — no pickle, never expires ──
    REFRESH_TOKEN = os.getenv("YT_REFRESH_TOKEN", "")
    CLIENT_ID     = os.getenv("YT_CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "")
    TOKEN_URI     = "https://oauth2.googleapis.com/token"

    log("upload", f"Refresh token length: {len(REFRESH_TOKEN)}")
    log("upload", f"Client ID length:     {len(CLIENT_ID)}")
    log("upload", f"Client Secret length: {len(CLIENT_SECRET)}")

    creds = Credentials(
        token         = None,
        refresh_token = REFRESH_TOKEN,
        token_uri     = TOKEN_URI,
        client_id     = CLIENT_ID,
        client_secret = CLIENT_SECRET,
        scopes        = ["https://www.googleapis.com/auth/youtube.force-ssl"]
    )

    # Auto-refresh — gets new access token every run
    creds.refresh(Request())
    log("upload", "YouTube credentials refreshed OK.")

    yt         = build("youtube", "v3", credentials=creds)
    publish_at = get_publish_time_utc(slot_cfg)

    log("upload", f"Title:  {seo_meta['title']}")
    log("upload", f"Publish at: {publish_at}")

    # ── 1. Upload video ──────────────────────────────────────
    body = {
        "snippet": {
            "title"              : seo_meta["title"],
            "description"        : seo_meta["description"],
            "tags"               : seo_meta["tags"],
            "categoryId"         : seo_meta["categoryId"],
            "defaultLanguage"    : seo_meta["defaultLanguage"],
            "defaultAudioLanguage": seo_meta["defaultAudioLanguage"],
        },
        "status": {
            "privacyStatus"           : "private",
            "publishAt"               : publish_at,
            "selfDeclaredMadeForKids" : seo_meta["selfDeclaredMadeForKids"],
            "madeForKids"             : seo_meta["madeForKids"],
            "embeddable"              : True,
            "publicStatsViewable"     : True,
        },
    }

    media   = MediaFileUpload(str(video_path), mimetype="video/mp4",
                              resumable=True, chunksize=5*1024*1024)
    req     = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp    = None
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            log("upload", f"  {int(status.progress()*100)}%")

    video_id = resp["id"]
    log("upload", f"Uploaded! youtube.com/shorts/{video_id}")

    # ── 2. Upload SEO thumbnail ──────────────────────────────
    try:
        yt.thumbnails().set(
            videoId    = video_id,
            media_body = MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg")
        ).execute()
        log("upload", "Thumbnail set.")
    except Exception as e:
        log("upload", f"Thumbnail (non-fatal): {e}")

    # ── 3. Set English localization ──────────────────────────
    try:
        yt.videos().update(
            part = "localizations",
            body = {
                "id": video_id,
                "localizations": {
                    LANGUAGE_CODE: {
                        "title"      : seo_meta["title"],
                        "description": seo_meta["description"],
                    }
                }
            }
        ).execute()
        log("upload", "Localization set (en).")
    except Exception as e:
        log("upload", f"Localization (non-fatal): {e}")

    # ── 4. Pin first SEO comment ─────────────────────────────
    try:
        time.sleep(3)
        yt.commentThreads().insert(
            part = "snippet",
            body = {
                "snippet": {
                    "videoId"        : video_id,
                    "topLevelComment": {
                        "snippet": {"textOriginal": seo_meta["pinned_comment"]}
                    }
                }
            }
        ).execute()
        log("upload", f"Pinned comment: {seo_meta['pinned_comment'][:50]}...")
    except Exception as e:
        log("upload", f"Pinned comment (non-fatal): {e}")

    return video_id


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run_slot(slot: int):
    slot_cfg = SLOTS[slot]
    start    = time.time()
    out_dir  = setup_dirs(slot)

    log("pipeline", "=" * 58)
    log("pipeline", f"   SLOT {slot} — {slot_cfg['name'].upper()}")
    log("pipeline", f"   Publish: {slot_cfg['publish_hour']:02d}:00 US Eastern")
    log("pipeline", f"   Budget: $0.00 — no credit card used")
    log("pipeline", "=" * 58)

    try:
        topic       = pick_topic(slot_cfg)
        data        = generate_script_and_seo(topic, slot_cfg)
        audios      = generate_voiceover(data, out_dir)
        images, thumb = generate_images(data, out_dir)
        music       = get_music(slot)
        final_video = assemble_video(data, slot_cfg, images, audios, music, out_dir)
        seo_meta    = build_seo_meta(data)
        video_id    = youtube_upload(final_video, thumb, seo_meta, slot_cfg)

        elapsed = time.time() - start
        log("pipeline", "=" * 58)
        log("pipeline", f"   DONE in {elapsed/60:.1f} min")
        log("pipeline", f"   URL: youtube.com/shorts/{video_id}")
        log("pipeline", f"   Cost this run: $0.00")
        log("pipeline", "=" * 58)

        # Append to log
        with Path("output/upload_log.jsonl").open("a") as f:
            f.write(json.dumps({
                "date"      : datetime.now(US_EASTERN).isoformat(),
                "slot"      : slot,
                "niche"     : slot_cfg["name"],
                "topic"     : topic,
                "video_id"  : video_id,
                "title"     : seo_meta["title"],
                "publish_at": get_publish_time_utc(slot_cfg),
                "cost_usd"  : 0.00,
            }) + "\n")

        return {"success": True, "video_id": video_id}

    except Exception as e:
        log("pipeline", f"ERROR: {e}")
        import traceback; traceback.print_exc()
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kids Shorts Pipeline v3 — Zero Investment")
    parser.add_argument("--slot", type=int, choices=[1, 2, 3], required=True,
                        help="1=8AM Animal Facts, 2=3PM Dino Facts, 3=7PM Ocean Animals (all EST)")
    run_slot(parser.parse_args().slot)
