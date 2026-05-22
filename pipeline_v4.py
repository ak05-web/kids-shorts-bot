"""
=============================================================
 KIDS YOUTUBE SHORTS PIPELINE v4 — ANIMATED VIDEO
 2 Videos/Day · No Voiceover · Music Only · Full Animation
=============================================================

 WHAT'S NEW IN v4:
   ✓ No voiceover — music only (better retention)
   ✓ Real animated video feel — not image slides
     • 2 images per scene → smooth crossfade mid-scene
     • Ken Burns zoom in different directions per scene
     • Text slides in from bottom with fade
     • Animated color-pop border frame on every scene
     • Fast wipe transitions between scenes
   ✓ 2 slots/day instead of 3
   ✓ Music louder (no voice to compete with)
   ✓ Bigger, bolder text overlays — facts fill the screen

 DAILY SCHEDULE (US Eastern Time):
   Slot 1 → 9:00 AM EST  → Animal Facts
   Slot 2 → 6:00 PM EST  → Dinosaur Facts

 FREE STACK:
   Groq API         → script + SEO (free, no card)
   Pollinations.ai  → cartoon images (free, no card)
   Mixkit CDN       → royalty-free music (free, no card)
   FFmpeg           → animated video assembly (open source)
   YouTube Data API → auto-upload (free quota)
   GitHub Actions   → daily scheduler (free on public repo)

 INSTALL:
   pip install groq pytrends requests pytz \
               google-api-python-client google-auth-oauthlib Pillow

   brew install ffmpeg        (Mac)
   sudo apt install ffmpeg    (Linux/GitHub Actions)

 USAGE:
   python pipeline_v4.py --slot 1
   python pipeline_v4.py --slot 2
=============================================================
"""

import os
import json
import time
import argparse
import requests
import subprocess
import pickle
from pathlib import Path
from datetime import datetime, timedelta

import pytz
from groq import Groq

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = "llama-3.3-70b-versatile"

YOUTUBE_CLIENT_FILE = "client_secrets.json"
YOUTUBE_TOKEN_FILE  = "youtube_token.pickle"

LANGUAGE_CODE  = "en"
LANGUAGE_LOCALE = "en-US"

# Video settings
VIDEO_WIDTH    = 1080
VIDEO_HEIGHT   = 1920
THUMBNAIL_W    = 1280
THUMBNAIL_H    = 720
FPS            = 30
NUM_SCENES     = 8
SCENE_DURATION = 7        # seconds per scene (fixed, no audio sync needed)
MUSIC_VOLUME   = 0.75     # louder — no voiceover competing
CHANNEL_NAME   = "WOW Animals!"

US_EASTERN = pytz.timezone("America/New_York")

# ─────────────────────────────────────────────────────────────
# SLOT CONFIG — 2 slots only
# ─────────────────────────────────────────────────────────────

SLOTS = {
    1: {
        "name"        : "Animal Facts",
        "niche"       : "fun animal facts for kids",
        "publish_hour": 9,
        "publish_min" : 0,
        "color_theme" : "00C853",   # bright green
        "accent"      : "FFD600",   # yellow accent
        "topic_pool"  : [
            "octopus", "axolotl", "platypus", "mantis shrimp", "tardigrade",
            "chameleon", "mimic octopus", "archerfish", "pistol shrimp", "narwhal",
            "aye-aye", "pangolin", "capybara", "blue-footed booby", "shoebill stork",
            "dumbo octopus", "glass frog", "star-nosed mole", "naked mole rat",
            "quokka", "proboscis monkey", "dugong", "yeti crab", "fossa",
            "secretary bird", "saiga antelope", "blobfish", "wombat", "kinkajou",
            "tapir", "okapi", "binturong", "cassowary", "sun bear",
            "honey badger", "fennec fox", "red panda", "sugar glider", "axolotl",
        ],
    },
    2: {
        "name"        : "Dinosaur Facts",
        "niche"       : "dinosaur facts for kids",
        "publish_hour": 18,
        "publish_min" : 0,
        "color_theme" : "FF6D00",   # bright orange
        "accent"      : "FFEA00",   # yellow accent
        "topic_pool"  : [
            "T-Rex", "velociraptor", "triceratops", "stegosaurus", "brachiosaurus",
            "ankylosaurus", "pterodactyl", "spinosaurus", "diplodocus", "allosaurus",
            "pachycephalosaurus", "parasaurolophus", "iguanodon", "carnotaurus",
            "therizinosaurus", "deinonychus", "mosasaurus", "plesiosaur",
            "microraptor", "oviraptor", "gallimimus", "ceratosaurus",
            "argentinosaurus", "giganotosaurus", "troodon", "baryonyx",
            "dilophosaurus", "compsognathus", "kentrosaurus", "mamenchisaurus",
        ],
    },
}

# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────

def setup_dirs(slot: int) -> Path:
    base = Path(f"output/slot_{slot}")
    for sub in ["scenes", "images"]:
        (base / sub).mkdir(parents=True, exist_ok=True)
    Path("assets").mkdir(exist_ok=True)
    return base

def log(stage: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{stage.upper():10}] {msg}")

def run_cmd(cmd: list, label: str = "cmd"):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{result.stderr[-1000:]}")
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
        return float(SCENE_DURATION)

def clean_json(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(lines)
    return text.strip()

def safe_text(s: str) -> str:
    """Escape text for FFmpeg drawtext filter."""
    return (s.replace("\\", "\\\\")
             .replace("'", "\u2019")
             .replace(":", "\\:")
             .replace("%", "\\%")
             .replace("[", "\\[")
             .replace("]", "\\]"))


# ─────────────────────────────────────────────────────────────
# STAGE 1 — TOPIC PICKER
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

    pool = slot_cfg["topic_pool"]
    idx  = (datetime.now().timetuple().tm_yday + slot_cfg["publish_hour"]) % len(pool)
    topic = pool[idx]
    log("topic", f"Pool rotation: {topic}")
    return topic


# ─────────────────────────────────────────────────────────────
# STAGE 2 — SCRIPT + SEO (Groq — free)
# No narration/voiceover — only fact text + image prompts
# ─────────────────────────────────────────────────────────────

def generate_script_and_seo(topic: str, slot_cfg: dict) -> dict:
    log("groq", f"Generating script + SEO for: {topic}")
    client = Groq(api_key=GROQ_API_KEY)

    prompt = f"""You are a YouTube Shorts content creator and SEO specialist.
Channel: "{CHANNEL_NAME}"
Format: Animated facts video — NO voiceover, music only, big text on screen
Audience: Kids aged 4-10
Niche: {slot_cfg['niche']}
Topic: {topic}

Respond with ONLY valid raw JSON. No markdown, no explanation, no code fences.

{{
  "topic": "{topic}",

  "seo": {{
    "title_main": "...(max 55 chars, shock/curiosity format e.g. 'Why {topic} Will SHOCK You!' or 'AMAZING {topic} Facts for Kids!')",
    "title_ab":   "...(max 55 chars, different angle)",
    "description": "...(hook sentence. 3 fact sentences. Subscribe CTA. 350-450 chars total.)",
    "chapters": [
      {{"time": "0:00", "label": "Intro"}},
      {{"time": "0:08", "label": "Fact 1"}},
      {{"time": "0:15", "label": "Fact 2"}},
      {{"time": "0:22", "label": "Fact 3"}},
      {{"time": "0:29", "label": "Fact 4"}},
      {{"time": "0:36", "label": "Fact 5"}},
      {{"time": "0:43", "label": "Fact 6"}},
      {{"time": "0:50", "label": "Fact 7"}},
      {{"time": "0:57", "label": "Subscribe"}}
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
    "pinned_comment": "...(1-2 sentences with keyword, ends with emoji question kids can answer)",
    "thumbnail_prompt": "...(EXTREME close-up face of {topic}, giant shocked wide eyes, mouth open, Pixar cartoon style, ultra bright yellow and red background, no text, hyper detailed child-friendly)"
  }},

  "hook_text":   "...(3-5 ALL CAPS words shown at start, e.g. 'DID YOU KNOW? 🤯')",
  "cta_text":    "...(3-5 ALL CAPS words for end card, e.g. 'FOLLOW FOR MORE! 🔔')",

  "scenes": [
    {{
      "scene_number": 1,
      "fact_line1":   "...(ALL CAPS, max 5 words, the WOW fact header e.g. '3 HEARTS! 💙')",
      "fact_line2":   "...(Title case, max 10 words, explains the fact simply)",
      "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...(scene A: wide shot of {topic} in habitat)",
      "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...(scene B: close-up of {topic} doing the action)"
    }},
    {{"scene_number": 2, "fact_line1": "...", "fact_line2": "...", "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...", "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ..."}},
    {{"scene_number": 3, "fact_line1": "...", "fact_line2": "...", "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...", "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ..."}},
    {{"scene_number": 4, "fact_line1": "...", "fact_line2": "...", "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...", "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ..."}},
    {{"scene_number": 5, "fact_line1": "...", "fact_line2": "...", "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...", "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ..."}},
    {{"scene_number": 6, "fact_line1": "...", "fact_line2": "...", "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...", "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ..."}},
    {{"scene_number": 7, "fact_line1": "...", "fact_line2": "...", "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...", "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ..."}},
    {{"scene_number": 8, "fact_line1": "...", "fact_line2": "...", "image_prompt_a": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ...", "image_prompt_b": "Cute Pixar cartoon style, bright saturated colors, child-friendly, ..."}}
  ]
}}

RULES:
- Exactly 8 scenes
- fact_line1: ALL CAPS, max 5 words + 1 emoji — the wow hook
- fact_line2: Title Case, max 10 words — simple explanation
- Each scene = 1 unique surprising fact about {topic}
- Both image prompts must start with: "Cute Pixar cartoon style, bright saturated colors, child-friendly,"
- image_prompt_a = wide/establishing shot
- image_prompt_b = close-up/action shot of same scene
- tags: exactly 20 items
- RESPOND WITH RAW JSON ONLY
"""

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": "You are a kids YouTube content specialist. Always respond with valid JSON only. No markdown, no explanation, no code fences."},
                    {"role": "user",   "content": prompt}
                ],
                temperature=0.7,
                max_tokens=4000,
            )
            raw  = clean_json(response.choices[0].message.content)
            data = json.loads(raw)
            break
        except json.JSONDecodeError as e:
            log("groq", f"JSON parse error (attempt {attempt+1}): {e}")
            if attempt == 2: raise
            time.sleep(4)
        except Exception as e:
            log("groq", f"API error (attempt {attempt+1}): {e}")
            if attempt == 2: raise
            time.sleep(10)

    # Append chapters + hashtags to description
    chapters_text = "\n".join(f"{ch['time']} {ch['label']}" for ch in data["seo"]["chapters"])
    data["seo"]["description"] = (
        data["seo"]["description"].rstrip()
        + f"\n\n⏱ CHAPTERS\n{chapters_text}"
        + f"\n\n{' '.join(data['seo']['hashtags'])}"
    )

    log("groq", f"Script: '{data['seo']['title_main']}'")
    return data


# ─────────────────────────────────────────────────────────────
# STAGE 3 — IMAGES (2 per scene + thumbnail)
# ─────────────────────────────────────────────────────────────

def _fetch_image(prompt: str, w: int, h: int, seed: int, out: Path) -> bool:
    from urllib.parse import quote
    url = (
        f"https://image.pollinations.ai/prompt/{quote(prompt)}"
        f"?width={w}&height={h}&seed={seed}&nologo=true&model=flux"
    )
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200 and len(r.content) > 5000:
                out.write_bytes(r.content)
                return True
        except Exception:
            pass
        if attempt < 2:
            log("images", f"    retry {attempt+1}...")
            time.sleep(5)
    return False

def _fallback_image(out: Path, w: int, h: int, color: str = "purple"):
    run_cmd(["ffmpeg", "-y", "-f", "lavfi",
             "-i", f"color=c=#{color}:size={w}x{h}:duration=1",
             "-frames:v", "1", str(out)], "img-fallback")

def generate_images(data: dict, out_dir: Path) -> tuple[list[tuple], Path]:
    """
    Returns list of (image_a_path, image_b_path) per scene + thumbnail path.
    2 images per scene = crossfade animation effect.
    """
    log("images", "Generating 2 images per scene + thumbnail...")
    img_dir = out_dir / "images"
    scene_pairs = []

    for scene in data["scenes"]:
        n = scene["scene_number"]

        # Image A — wide shot
        out_a = img_dir / f"scene_{n:02d}_a.jpg"
        prompt_a = scene["image_prompt_a"] + ", no text, no watermark, vertical 9:16 format"
        ok_a = _fetch_image(prompt_a, VIDEO_WIDTH, VIDEO_HEIGHT, n * 31, out_a)
        if not ok_a:
            _fallback_image(out_a, VIDEO_WIDTH, VIDEO_HEIGHT, "4CAF50")
        log("images", f"  Scene {n}A: {'ok' if ok_a else 'fallback'}")
        time.sleep(3.0)

        # Image B — close-up
        out_b = img_dir / f"scene_{n:02d}_b.jpg"
        prompt_b = scene["image_prompt_b"] + ", no text, no watermark, vertical 9:16 format"
        ok_b = _fetch_image(prompt_b, VIDEO_WIDTH, VIDEO_HEIGHT, n * 53 + 7, out_b)
        if not ok_b:
            _fallback_image(out_b, VIDEO_WIDTH, VIDEO_HEIGHT, "2196F3")
        log("images", f"  Scene {n}B: {'ok' if ok_b else 'fallback'}")
        time.sleep(3.0)

        scene_pairs.append((out_a, out_b))

    # Thumbnail
    thumb_raw   = img_dir / "thumb_raw.jpg"
    thumb_final = img_dir / "thumb_final.jpg"
    ok = _fetch_image(
        data["seo"]["thumbnail_prompt"] + ", horizontal landscape, no text, no watermark",
        THUMBNAIL_W, THUMBNAIL_H, 9999, thumb_raw
    )
    if not ok:
        _fallback_image(thumb_raw, THUMBNAIL_W, THUMBNAIL_H, "FF6D00")

    # Bold text on thumbnail
    t_topic = safe_text(data["topic"].upper()[:16])
    t_title = safe_text(data["seo"]["title_main"][:30])
    run_cmd([
        "ffmpeg", "-y", "-i", str(thumb_raw),
        "-vf",
        (
            f"drawtext=text='{t_topic}!':"
            f"fontsize=110:fontcolor=yellow:borderw=8:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.07,"
            f"drawtext=text='{t_title}':"
            f"fontsize=58:fontcolor=white:borderw=5:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.80:"
            f"box=1:boxcolor=#CC0000@0.90:boxborderw=16"
        ),
        str(thumb_final)
    ], "Thumbnail")

    log("images", f"Done: {len(scene_pairs)} scene pairs + thumbnail")
    return scene_pairs, thumb_final


# ─────────────────────────────────────────────────────────────
# STAGE 4 — BACKGROUND MUSIC
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
             "anullsrc=r=44100:cl=stereo", "-t", "80", str(path)])
    return path


# ─────────────────────────────────────────────────────────────
# STAGE 5 — ANIMATED VIDEO ASSEMBLY (FFmpeg)
#
# Each scene = 7 seconds:
#   0.0 - 3.0s → Image A with Ken Burns zoom-in (left→right pan)
#   2.5 - 3.0s → crossfade to Image B (0.5s overlap)
#   3.0 - 7.0s → Image B with Ken Burns zoom-out (right→left pan)
#   0.5s       → fact_line1 slides in from bottom, stays whole scene
#   1.2s       → fact_line2 fades in below line1
#   Between scenes → fast 0.3s wipe transition
# ─────────────────────────────────────────────────────────────

# Ken Burns directions — alternates per scene for visual variety
KB_DIRECTIONS = [
    # (start_x_expr, start_y_expr, end_x_expr, end_y_expr, zoom_start, zoom_end)
    ("iw/2", "ih/2",          "iw*0.45", "ih*0.45",      1.0, 1.08),   # center→top-left zoom in
    ("iw*0.4", "ih*0.4",      "iw/2",    "ih/2",          1.08, 1.0),  # top-left→center zoom out
    ("iw/2", "ih*0.55",       "iw/2",    "ih*0.45",       1.0, 1.09),  # bottom→top pan
    ("iw*0.55", "ih/2",       "iw*0.45", "ih/2",          1.0, 1.07),  # right→left pan
    ("iw*0.45", "ih*0.45",    "iw/2",    "ih/2",          1.09, 1.0),  # top-left→center zoom out
    ("iw/2", "ih/2",          "iw*0.55", "ih*0.55",       1.0, 1.08),  # center→bottom-right
    ("iw/2", "ih*0.45",       "iw/2",    "ih*0.55",       1.07, 1.0),  # top→bottom zoom out
    ("iw*0.45", "ih/2",       "iw*0.55", "ih/2",          1.0, 1.08),  # left→right zoom in
]

def build_scene_clip(
    img_a: Path, img_b: Path,
    fact1: str, fact2: str,
    scene_num: int,
    color: str, accent: str,
    out: Path
):
    """
    Build one animated scene clip:
    - Image A (first 3.5s) with Ken Burns
    - Crossfade to Image B (last 3.5s) with opposite Ken Burns
    - Fact text slides in from bottom
    - Colored animated border frame
    """
    dur   = float(SCENE_DURATION)
    half  = dur / 2.0
    fade  = 0.5           # crossfade duration
    f1    = safe_text(fact1[:30])
    f2    = safe_text(fact2[:50])
    frames_half = int(half * FPS)
    frames_full = int(dur * FPS)

    kb = KB_DIRECTIONS[(scene_num - 1) % len(KB_DIRECTIONS)]

    # Ken Burns for image A — zoom in direction
    kb_a = (
        f"scale={VIDEO_WIDTH*2}:{VIDEO_HEIGHT*2},"
        f"zoompan=z='min(1.0+{0.08/frames_half:.6f}*on,1.08)':"
        f"x='{kb[0]}-(iw/zoom/2)':y='{kb[1]}-(ih/zoom/2)':"
        f"d={frames_half}:s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={FPS},setsar=1"
    )

    # Ken Burns for image B — zoom out (reverse direction)
    kb_b = (
        f"scale={VIDEO_WIDTH*2}:{VIDEO_HEIGHT*2},"
        f"zoompan=z='max(1.08-{0.08/frames_half:.6f}*on,1.0)':"
        f"x='{kb[2]}-(iw/zoom/2)':y='{kb[3]}-(ih/zoom/2)':"
        f"d={frames_half}:s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={FPS},setsar=1"
    )

    # Text animations:
    # fact_line1: slides up from y=h*0.88 to y=h*0.72 over 0.4s starting at t=0.4
    # fact_line2: fades in at y=h*0.82 starting at t=1.0
    text_filters = (
        # Colored box behind fact_line1
        f"drawbox=x=(w-800)/2:y=h*0.68:w=800:h=120:"
        f"color=#{color}@0.92:t=fill:"
        f"enable='gte(t,0.4)',"
        # fact_line1 — big bold ALL CAPS
        f"drawtext=text='{f1}':"
        f"fontsize=72:fontcolor=white:borderw=5:bordercolor=black:"
        f"x=(w-text_w)/2:"
        f"y=h*0.88-(h*0.88-h*0.72)*min(1\\,(t-0.4)/0.35):"
        f"enable='gte(t,0.4)',"
        # Colored box behind fact_line2
        f"drawbox=x=40:y=h*0.82:w=w-80:h=100:"
        f"color=black@0.70:t=fill:"
        f"enable='gte(t,1.0)',"
        # fact_line2 — smaller explanation
        f"drawtext=text='{f2}':"
        f"fontsize=48:fontcolor=#{accent}:borderw=4:bordercolor=black:"
        f"x=(w-text_w)/2:y=h*0.845:"
        f"alpha='min(1,(t-1.0)/0.3)':"
        f"enable='gte(t,1.0)',"
        # Animated top border stripe
        f"drawbox=x=0:y=0:w=w:h=18:color=#{color}@1.0:t=fill,"
        # Animated bottom border stripe
        f"drawbox=x=0:y=h-18:w=w:h=18:color=#{color}@1.0:t=fill,"
        # Scene number badge top-right
        f"drawtext=text='{scene_num}/{NUM_SCENES}':"
        f"fontsize=36:fontcolor=white:borderw=3:bordercolor=black:"
        f"x=w-120:y=30"
    )

    # Build filter_complex: A → kb_a, B → kb_b, crossfade at midpoint
    crossfade_start = half - fade / 2

    filter_complex = (
        f"[0:v]{kb_a}[va];"
        f"[1:v]{kb_b}[vb];"
        f"[va][vb]xfade=transition=fade:duration={fade}:offset={crossfade_start:.2f}[vx];"
        f"[vx]{text_filters}[vout]"
    )

    run_cmd([
        "ffmpeg", "-y",
        "-loop", "1", "-t", str(half + fade), "-i", str(img_a),
        "-loop", "1", "-t", str(half + fade), "-i", str(img_b),
        "-filter_complex", filter_complex,
        "-map", "[vout]",
        "-t", str(dur),
        "-c:v", "libx264", "-preset", "fast", "-crf", "22",
        "-pix_fmt", "yuv420p",
        "-r", str(FPS),
        str(out),
    ], f"Scene-{scene_num}")


def build_intro_card(hook_text: str, topic: str, color: str, accent: str, out: Path):
    """Animated intro card — 3 seconds."""
    h_text = safe_text(hook_text[:30])
    t_text = safe_text(topic.upper()[:20])
    dur    = 3.0

    run_cmd([
        "ffmpeg", "-y", "-f", "lavfi",
        "-i", f"color=c=#{color}:size={VIDEO_WIDTH}x{VIDEO_HEIGHT}:duration={dur}:rate={FPS}",
        "-vf",
        (
            # Pulsing background — vignette effect
            f"vignette=PI/4,"
            # Channel name
            f"drawtext=text='{CHANNEL_NAME}':"
            f"fontsize=52:fontcolor=white:borderw=4:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.15,"
            # Hook text — slides in from top
            f"drawtext=text='{h_text}':"
            f"fontsize=88:fontcolor=#{accent}:borderw=7:bordercolor=black:"
            f"x=(w-text_w)/2:"
            f"y=h*0.10+(h*0.35-h*0.10)*min(1\\,t/0.5),"
            # Topic name — large, center
            f"drawtext=text='{t_text}':"
            f"fontsize=100:fontcolor=white:borderw=8:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.42:"
            f"alpha='min(1,(t-0.4)/0.4)',"
            # Border stripes
            f"drawbox=x=0:y=0:w=w:h=18:color=#{accent}@1.0:t=fill,"
            f"drawbox=x=0:y=h-18:w=w:h=18:color=#{accent}@1.0:t=fill"
        ),
        "-c:v", "libx264", "-preset", "fast", "-crf", "22",
        "-pix_fmt", "yuv420p",
        str(out)
    ], "IntroCard")


def build_outro_card(cta_text: str, color: str, accent: str, out: Path):
    """Outro card with subscribe CTA — 3 seconds."""
    c_text = safe_text(cta_text[:30])
    dur    = 3.0

    run_cmd([
        "ffmpeg", "-y", "-f", "lavfi",
        "-i", f"color=c=#{color}:size={VIDEO_WIDTH}x{VIDEO_HEIGHT}:duration={dur}:rate={FPS}",
        "-vf",
        (
            f"vignette=PI/4,"
            f"drawtext=text='SUBSCRIBE':"
            f"fontsize=110:fontcolor=#{accent}:borderw=9:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.30,"
            f"drawtext=text='for more WOW facts! 🔔':"
            f"fontsize=58:fontcolor=white:borderw=5:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.48:"
            f"alpha='min(1,(t-0.3)/0.4)',"
            f"drawtext=text='{c_text}':"
            f"fontsize=52:fontcolor=white:borderw=4:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.62:"
            f"alpha='min(1,(t-0.6)/0.4)',"
            f"drawtext=text='{CHANNEL_NAME}':"
            f"fontsize=46:fontcolor=#{accent}:borderw=3:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.75,"
            f"drawbox=x=0:y=0:w=w:h=18:color=#{accent}@1.0:t=fill,"
            f"drawbox=x=0:y=h-18:w=w:h=18:color=#{accent}@1.0:t=fill"
        ),
        "-c:v", "libx264", "-preset", "fast", "-crf", "22",
        "-pix_fmt", "yuv420p",
        str(out)
    ], "OutroCard")


def assemble_video(
    data: dict,
    slot_cfg: dict,
    scene_pairs: list[tuple],
    music: Path,
    out_dir: Path
) -> Path:
    log("video", "Assembling animated Short...")
    sd     = out_dir / "scenes"
    color  = slot_cfg["color_theme"]
    accent = slot_cfg["accent"]
    clips  = []

    # Intro card
    intro = sd / "intro.mp4"
    build_intro_card(data["hook_text"], data["topic"], color, accent, intro)
    clips.append(intro)

    # Scene clips — 2 images each with crossfade + animated text
    for (img_a, img_b), scene in zip(scene_pairs, data["scenes"]):
        sc  = sd / f"clip_{scene['scene_number']:02d}.mp4"
        build_scene_clip(
            img_a, img_b,
            scene["fact_line1"],
            scene["fact_line2"],
            scene["scene_number"],
            color, accent, sc
        )
        clips.append(sc)
        log("video", f"  Scene {scene['scene_number']} assembled")

    # Outro card
    outro = sd / "outro.mp4"
    build_outro_card(data["cta_text"], color, accent, outro)
    clips.append(outro)

    # Concat all clips
    concat_txt = out_dir / "concat.txt"
    concat_txt.write_text("\n".join(f"file '{p.resolve()}'" for p in clips))
    raw = out_dir / "raw.mp4"
    run_cmd([
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_txt),
        "-c", "copy",
        str(raw)
    ], "Concat")

    # Add music — loud (no voiceover)
    total_dur  = get_audio_duration(raw)
    date_str   = datetime.now().strftime("%Y%m%d")
    slot_name  = slot_cfg["name"].replace(" ", "_").lower()
    final      = out_dir / f"short_{slot_name}_{date_str}.mp4"

    run_cmd([
        "ffmpeg", "-y",
        "-i", str(raw),
        "-stream_loop", "-1", "-i", str(music),
        "-filter_complex",
        (
            f"[1:a]volume={MUSIC_VOLUME},atrim=0:{total_dur:.2f},"
            f"afade=t=in:st=0:d=0.5,"
            f"afade=t=out:st={max(0, total_dur-1.5):.2f}:d=1.5[aout]"
        ),
        "-map", "0:v",
        "-map", "[aout]",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(final)
    ], "MixAudio")

    sz = final.stat().st_size // (1024 * 1024)
    log("video", f"Final: {final.name} ({sz} MB, {total_dur:.1f}s)")
    return final


# ─────────────────────────────────────────────────────────────
# STAGE 6 — SEO METADATA
# ─────────────────────────────────────────────────────────────

def build_seo_meta(data: dict) -> dict:
    seo      = data["seo"]
    raw_tags = [str(t)[:30] for t in seo.get("tags", [])]
    while raw_tags and len(",".join(raw_tags)) > 490:
        raw_tags.pop()
    return {
        "title"                  : seo["title_main"][:100],
        "description"            : seo["description"][:5000],
        "tags"                   : raw_tags,
        "pinned_comment"         : seo.get("pinned_comment", ""),
        "categoryId"             : "27",
        "defaultLanguage"        : LANGUAGE_CODE,
        "defaultAudioLanguage"   : LANGUAGE_CODE,
        "madeForKids"            : True,
        "selfDeclaredMadeForKids": True,
    }


# ─────────────────────────────────────────────────────────────
# STAGE 7 — YOUTUBE UPLOAD (refresh token — never expires)
# ─────────────────────────────────────────────────────────────

def get_publish_time_utc(slot_cfg: dict) -> str:
    now_est     = datetime.now(US_EASTERN)
    publish_est = now_est.replace(
        hour=slot_cfg["publish_hour"], minute=slot_cfg["publish_min"],
        second=0, microsecond=0
    )
    if now_est >= publish_est:
        publish_est += timedelta(days=1)
    return publish_est.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def youtube_upload(video_path: Path, thumbnail_path: Path, seo_meta: dict, slot_cfg: dict) -> str:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    REFRESH_TOKEN = os.getenv("YT_REFRESH_TOKEN", "")
    CLIENT_ID     = os.getenv("YT_CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "")

    log("upload", f"Refresh token length: {len(REFRESH_TOKEN)}")
    log("upload", f"Client ID length:     {len(CLIENT_ID)}")
    log("upload", f"Client Secret length: {len(CLIENT_SECRET)}")

    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/youtube.force-ssl"]
    )
    creds.refresh(Request())
    log("upload", "Credentials refreshed OK.")

    yt         = build("youtube", "v3", credentials=creds)
    publish_at = get_publish_time_utc(slot_cfg)

    log("upload", f"Title:      {seo_meta['title']}")
    log("upload", f"Publish at: {publish_at}")

    body = {
        "snippet": {
            "title"               : seo_meta["title"],
            "description"         : seo_meta["description"],
            "tags"                : seo_meta["tags"],
            "categoryId"          : seo_meta["categoryId"],
            "defaultLanguage"     : seo_meta["defaultLanguage"],
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

    media = MediaFileUpload(str(video_path), mimetype="video/mp4", resumable=True, chunksize=5*1024*1024)
    req   = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp  = None
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            log("upload", f"  {int(status.progress()*100)}%")

    video_id = resp["id"]
    log("upload", f"Uploaded! youtube.com/shorts/{video_id}")

    # Thumbnail
    try:
        yt.thumbnails().set(
            videoId=video_id,
            media_body=MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg")
        ).execute()
        log("upload", "Thumbnail set.")
    except Exception as e:
        log("upload", f"Thumbnail (non-fatal): {e}")

    # Localization
    try:
        yt.videos().update(
            part="localizations",
            body={"id": video_id, "localizations": {
                LANGUAGE_CODE: {"title": seo_meta["title"], "description": seo_meta["description"]}
            }}
        ).execute()
        log("upload", "Localization set.")
    except Exception as e:
        log("upload", f"Localization (non-fatal): {e}")

    # Pinned comment
    try:
        time.sleep(3)
        yt.commentThreads().insert(
            part="snippet",
            body={"snippet": {"videoId": video_id, "topLevelComment": {
                "snippet": {"textOriginal": seo_meta["pinned_comment"]}
            }}}
        ).execute()
        log("upload", "Pinned comment set.")
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
    log("pipeline", f"   Format: Animated · Music Only · No Voiceover")
    log("pipeline", f"   Budget: $0.00")
    log("pipeline", "=" * 58)

    try:
        topic          = pick_topic(slot_cfg)
        data           = generate_script_and_seo(topic, slot_cfg)
        scene_pairs, thumb = generate_images(data, out_dir)
        music          = get_music(slot)
        final_video    = assemble_video(data, slot_cfg, scene_pairs, music, out_dir)
        seo_meta       = build_seo_meta(data)
        video_id       = youtube_upload(final_video, thumb, seo_meta, slot_cfg)

        elapsed = time.time() - start
        log("pipeline", "=" * 58)
        log("pipeline", f"   DONE in {elapsed/60:.1f} min")
        log("pipeline", f"   URL: youtube.com/shorts/{video_id}")
        log("pipeline", "=" * 58)

        Path("output").mkdir(exist_ok=True)
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
    parser = argparse.ArgumentParser(description="Kids Shorts Pipeline v4 — Animated, No Voiceover")
    parser.add_argument("--slot", type=int, choices=[1, 2], required=True,
                        help="1=9AM Animal Facts, 2=6PM Dinosaur Facts (US Eastern)")
    run_slot(parser.parse_args().slot)
