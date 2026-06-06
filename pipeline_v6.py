"""
=============================================================
KIDS YOUTUBE SHORTS PIPELINE v6 — HF SPACE IMAGE-TO-VIDEO
2 Videos/Day · NO Text Overlay · Kids Psychology · Music Only
=============================================================

WHAT'S IN EACH VIDEO:
• Pollinations.ai → cute cartoon animal image (per scene)
• gradio_client → HF Space Image-to-Video → real .mp4 clip
• Clips stitched together with FFmpeg
• Kids-matched background music (topic-specific feel)
• NO text overlay, NO voiceover — pure visual + music
• Ken Burns fallback if HF Space times out

INSTALL:
pip install groq pytrends requests pytz gradio_client \
  google-api-python-client google-auth-oauthlib Pillow

Ubuntu/GitHub Actions:
sudo apt install -y ffmpeg

USAGE:
python pipeline_v6.py --slot 1
python pipeline_v6.py --slot 2
=============================================================
"""

import os
import json
import time
import random
import argparse
import requests
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote

import pytz
from groq import Groq

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL     = "llama-3.3-70b-versatile"
LANGUAGE_CODE  = "en"
VIDEO_WIDTH    = 1080
VIDEO_HEIGHT   = 1920
FPS            = 24
NUM_SCENES     = 5          # 5 clips × 4s = ~20s Short (analytics: 21-28s best, 71s = dead)
CLIP_DURATION  = 4          # seconds per HF video clip
MUSIC_VOLUME   = 0.88
CHANNEL_NAME   = "WOW Animals!"
US_EASTERN     = pytz.timezone("America/New_York")

# HF Spaces to try in order (fallback chain)
# These are public Image-to-Video spaces — no API key needed
HF_SPACES = [
    "wangfuyun/AnimateDiff-Lightning",   # Fast, reliable
    "multimodalart/stable-video-diffusion",
    "fffiloni/animatediff-lightning",
]

# ─────────────────────────────────────────────────────────────
# SLOT CONFIG — 2 videos/day
# ─────────────────────────────────────────────────────────────
SLOTS = {
    1: {
        "name":          "Dinosaur Facts Morning",
        "niche":         "dinosaur facts for kids",
        "publish_hour":  9,
        "publish_min":   0,
        "music_mood":    "epic adventure",
        # Analytics: popular dinos first, Mosasaurus CTR 3.77% = formula works
        "topic_pool": [
            "T-Rex", "velociraptor", "triceratops", "stegosaurus", "brachiosaurus",
            "ankylosaurus", "spinosaurus", "mosasaurus", "carnotaurus", "giganotosaurus",
            "diplodocus", "therizinosaurus", "pachycephalosaurus", "parasaurolophus",
            "allosaurus", "ceratosaurus", "baryonyx", "dilophosaurus", "microraptor",
            "argentinosaurus", "troodon", "gallimimus", "iguanodon", "kentrosaurus",
        ],
    },
    2: {
        "name":          "Dinosaur Facts Evening",
        "niche":         "dinosaur facts for kids",
        "publish_hour":  18,
        "publish_min":   0,
        "music_mood":    "epic adventure",
        # Different order so slots don't repeat same topic on same day
        "topic_pool": [
            "pterodactyl", "plesiosaur", "mosasaurus", "ankylosaurus", "stegosaurus",
            "T-Rex", "spinosaurus", "carnotaurus", "triceratops", "brachiosaurus",
            "velociraptor", "giganotosaurus", "diplodocus", "therizinosaurus",
            "allosaurus", "dilophosaurus", "baryonyx", "ceratosaurus", "microraptor",
            "argentinosaurus", "gallimimus", "parasaurolophus", "pachycephalosaurus",
            "iguanodon",
        ],
    },
}

# ─────────────────────────────────────────────────────────────
# MUSIC TRACKS — matched to mood
# ─────────────────────────────────────────────────────────────
MUSIC_BY_MOOD = {
    "epic adventure": [
        "https://assets.mixkit.co/music/preview/mixkit-life-is-a-dream-837.mp3",
        "https://assets.mixkit.co/music/preview/mixkit-adventure-orchestral-829.mp3",
        "https://assets.mixkit.co/music/preview/mixkit-epic-music-for-action-video-831.mp3",
    ],
    "playful upbeat": [
        "https://assets.mixkit.co/music/preview/mixkit-fun-and-quirky-122.mp3",
        "https://assets.mixkit.co/music/preview/mixkit-cheerful-fun-and-quirky-268.mp3",
        "https://assets.mixkit.co/music/preview/mixkit-kids-fun-game-show-248.mp3",
    ],
}

# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────
def setup_dirs(slot: int) -> Path:
    base = Path(f"output/slot_{slot}")
    for sub in ["clips", "images", "audio"]:
        (base / sub).mkdir(parents=True, exist_ok=True)
    Path("assets").mkdir(exist_ok=True)
    return base

def log(stage: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{stage.upper():10}] {msg}")

def run_cmd(cmd: list, label: str = "cmd") -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{result.stderr[-1500:]}")
    return result

def get_duration(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True
    )
    try:
        return float(r.stdout.strip())
    except Exception:
        return float(CLIP_DURATION)

def clean_json(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(lines)
    return text.strip()

# ─────────────────────────────────────────────────────────────
# STAGE 1 — TOPIC PICKER
# ─────────────────────────────────────────────────────────────
def pick_topic(slot_cfg: dict) -> str:
    log("topic", f"Picking for: {slot_cfg['name']}")
    try:
        from pytrends.request import TrendReq
        pt = TrendReq(hl="en-US", tz=300)
        pt.build_payload([slot_cfg["niche"]], timeframe="now 7-d", geo="US")
        related = pt.related_queries()
        queries = related.get(slot_cfg["niche"], {}).get("top")
        if queries is not None and not queries.empty:
            topic = queries["query"].iloc[0]
            log("topic", f"PyTrends: {topic}")
            return topic
    except Exception as e:
        log("topic", f"PyTrends skip ({e})")

    pool = slot_cfg["topic_pool"]
    idx  = (datetime.now().timetuple().tm_yday + slot_cfg["publish_hour"]) % len(pool)
    topic = pool[idx]
    log("topic", f"Pool: {topic}")
    return topic

# ─────────────────────────────────────────────────────────────
# STAGE 2 — SCENE PROMPTS + SEO (Groq)
# No text overlay needed → scenes are pure visual moments
# Kids psychology: surprise, cuteness, movement, bright colors
# ─────────────────────────────────────────────────────────────
def generate_scenes_and_seo(topic: str, slot_cfg: dict) -> dict:
    log("groq", f"Generating scene prompts for: {topic}")
    client = Groq(api_key=GROQ_API_KEY)

    prompt = f"""You are a YouTube Shorts content creator and SEO specialist.
Channel: "{CHANNEL_NAME}" | Topic: {topic} | Niche: {slot_cfg['niche']}
Audience: Kids aged 4-10 | Format: Pure visual video — NO text, NO voiceover
Video structure: {NUM_SCENES} short clips x {CLIP_DURATION}s = ~{NUM_SCENES*CLIP_DURATION}s total. SHORT = more replays = more Shorts feed push.

PROVEN FORMULA from channel analytics:
- Title: "AWESOME [Dino] for Kids!" = best performing (340 views, 3.77% CTR)
- Dinosaur content = 85% of all channel views
- 15-25 second videos perform best — 71s videos got almost zero views

Kids psychology hooks for dinosaurs:
- SURPRISE: dino doing something shocking or unexpected
- SCALE: show how MASSIVE the dinosaur is vs humans/cars/buildings  
- ACTION: running, hunting, roaring, attacking — kids LOVE this
- DANGER: dramatic teeth, claws, predator behavior
- COLOR: ultra-vivid Pixar palette, dramatic prehistoric sky/jungle

Respond ONLY with valid raw JSON. No markdown, no explanation.

{{
  "topic": "{topic}",
  "seo": {{
    "title_main": "AWESOME {topic} for Kids! 🦕",
    "title_ab": "Did You Know About {topic}? 😱 #shorts",
    "description": "...(1 shocking fact about {topic} + subscribe CTA, max 200 chars, NO hashtags here)",
    "tags": [
      "{topic}", "{topic} for kids", "{topic} facts", "{topic} dinosaur",
      "dinosaur facts for kids", "dinosaur kids video", "kids learning dinosaurs",
      "dinosaur shorts", "wow animals kids", "dino facts kids",
      "dinosaur educational", "kids youtube dinosaur", "fun dinosaur facts",
      "{topic} kids", "wow dinosaur", "dino kids video", "dinosaur video kids",
      "educational shorts", "kids channel", "dinosaur facts"
    ],
    "hashtags": ["#Shorts", "#DinosaurFacts", "#KidsLearning", "#WOWAnimals", "#DinoKids"],
    "pinned_comment": "...(fun dino question for kids with emojis, max 100 chars)"
  }},
  "scenes": [
    {{
      "scene_number": 1,
      "psychology_hook": "SURPRISE",
      "image_prompt": "...(40+ words: cute Pixar-style cartoon {topic} dinosaur, specific shocking feature or behavior, ultra vibrant colors, dramatic prehistoric background, 9:16 portrait, no text, no watermark, high detail)",
      "motion_style": "zoom_in"
    }},
    {{
      "scene_number": 2,
      "psychology_hook": "SCALE",
      "image_prompt": "...(40+ words: show massive size of {topic} compared to tiny human silhouette or car, dramatic scale, Pixar cartoon style, bright colors)",
      "motion_style": "slow_pan_right"
    }},
    {{
      "scene_number": 3,
      "psychology_hook": "ACTION",
      "image_prompt": "...(40+ words: {topic} in dramatic action — roaring, running, hunting, dynamic pose, motion blur, epic lighting, Pixar cartoon style)",
      "motion_style": "zoom_out"
    }},
    {{
      "scene_number": 4,
      "psychology_hook": "DANGER",
      "image_prompt": "...(40+ words: close-up of {topic} dramatic features — teeth, claws, eyes, menacing but cartoon-cute, kids-safe, Pixar style, vivid colors)",
      "motion_style": "slow_pan_left"
    }},
    {{
      "scene_number": 5,
      "psychology_hook": "COLOR",
      "image_prompt": "...(40+ words: {topic} in most colorful environment — lush jungle, volcanic sunset, crystal cave, ultra saturated palette, Pixar cartoon, gorgeous lighting)",
      "motion_style": "zoom_in"
    }}
  ]
}}

RULES:
- Exactly {NUM_SCENES} scenes. Tags exactly 20 items.
- title_main MUST be: "AWESOME {topic} for Kids! 🦕" — proven formula, do not change format.
- Each image_prompt must be 40+ words with very specific visual details.
- RESPOND WITH RAW JSON ONLY."""

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": "Kids YouTube specialist. Valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=3000,
            )
            raw  = clean_json(response.choices[0].message.content)
            data = json.loads(raw)
            break
        except json.JSONDecodeError as e:
            log("groq", f"JSON error attempt {attempt+1}: {e}")
            if attempt == 2: raise
            time.sleep(4)
        except Exception as e:
            log("groq", f"API error attempt {attempt+1}: {e}")
            if attempt == 2: raise
            time.sleep(10)

    # Assemble full description with hashtags
    data["seo"]["description"] = (
        data["seo"]["description"].rstrip()
        + f"\n\n{' '.join(data['seo']['hashtags'])}"
    )
    log("groq", f"Done: '{data['seo']['title_main']}'")
    return data

# ─────────────────────────────────────────────────────────────
# STAGE 3 — DOWNLOAD IMAGES (Pollinations.ai — free)
# ─────────────────────────────────────────────────────────────
def download_images(data: dict, out_dir: Path) -> list:
    log("images", f"Downloading {NUM_SCENES} cartoon images from Pollinations...")
    img_dir = out_dir / "images"
    paths   = []

    for scene in data["scenes"]:
        n        = scene["scene_number"]
        out_path = img_dir / f"scene_{n:02d}.jpg"

        # Use the AI-generated detailed prompt
        base_prompt = scene["image_prompt"]
        full_prompt = (
            f"{base_prompt}, Pixar Disney cartoon style, ultra vibrant colors, "
            f"child friendly, expressive happy face, no text, no watermark, "
            f"vertical portrait 9:16 aspect ratio, high quality"
        )

        url = (
            f"https://image.pollinations.ai/prompt/{quote(full_prompt)}"
            f"?width={VIDEO_WIDTH}&height={VIDEO_HEIGHT}"
            f"&seed={n * 137 + random.randint(0, 50)}&nologo=true&model=flux"
        )

        ok = False
        for attempt in range(4):
            try:
                r = requests.get(url, timeout=120)
                if r.status_code == 200 and len(r.content) > 8000:
                    out_path.write_bytes(r.content)
                    ok = True
                    break
            except Exception as ex:
                log("images", f"  Scene {n} attempt {attempt+1} error: {ex}")
            time.sleep(6)

        if not ok:
            # Fallback: colored rectangle
            run_cmd([
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", f"color=c=0x1a237e:size={VIDEO_WIDTH}x{VIDEO_HEIGHT}:duration=1",
                "-frames:v", "1", str(out_path)
            ], f"ImgFallback-{n}")
            log("images", f"  Scene {n}: FALLBACK color used")
        else:
            log("images", f"  Scene {n}: ok ({len(r.content)//1024}KB)")

        paths.append(out_path)
        time.sleep(4.0)   # Be polite to Pollinations

    log("images", "All images ready.")
    return paths

# ─────────────────────────────────────────────────────────────
# STAGE 4 — IMAGE-TO-VIDEO via HF Space (gradio_client)
# Robust: retries, timeout handling, fallback to Ken Burns
# ─────────────────────────────────────────────────────────────
def image_to_video_hf(image_path: Path, scene: dict, out_path: Path) -> bool:
    """
    Try HF Spaces in order. Return True if successful.
    Each space has different API signature — we handle top ones.
    """
    try:
        from gradio_client import Client, handle_file
    except ImportError:
        log("hf", "gradio_client not installed — falling back to Ken Burns")
        return False

    scene_num = scene["scene_number"]

    for space_id in HF_SPACES:
        log("hf", f"  Scene {scene_num}: Trying {space_id}...")
        for attempt in range(2):
            try:
                client = Client(space_id, verbose=False)

                # AnimateDiff-Lightning API
                if "AnimateDiff" in space_id or "animatediff" in space_id:
                    result = client.predict(
                        image=handle_file(str(image_path)),
                        motion_module="mm_sd_v15_v2.ckpt",
                        prompt="cute cartoon animal, smooth motion, vibrant colors, kids video",
                        n_prompt="text, watermark, blurry, ugly, deformed",
                        guidance_scale=7.5,
                        num_inference_steps=4,     # Lightning = fast
                        video_length=16,           # ~2-3 sec at 8fps
                        fn_index=0,
                        api_name="/predict",
                    )

                # Stable Video Diffusion API
                elif "stable-video" in space_id:
                    result = client.predict(
                        handle_file(str(image_path)),
                        25,    # num_frames
                        127,   # seed
                        6.0,   # fps_id
                        127.0, # motion_bucket_id
                        0.0,   # cond_aug
                        True,  # decode_chunk_size
                        api_name="/video",
                    )

                else:
                    # Generic — try common signature
                    result = client.predict(
                        handle_file(str(image_path)),
                        api_name="/predict",
                    )

                # Extract video path from result
                video_path = None
                if isinstance(result, str):
                    video_path = result
                elif isinstance(result, (list, tuple)):
                    for r in result:
                        if isinstance(r, str) and r.endswith(".mp4"):
                            video_path = r
                            break
                        elif isinstance(r, dict) and "video" in r:
                            video_path = r["video"]
                            break

                if video_path and Path(video_path).exists():
                    # Re-encode to exact dimensions + duration
                    run_cmd([
                        "ffmpeg", "-y", "-i", str(video_path),
                        "-vf", f"scale={VIDEO_WIDTH}:{VIDEO_HEIGHT}:force_original_aspect_ratio=decrease,"
                               f"pad={VIDEO_WIDTH}:{VIDEO_HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=black,"
                               f"setsar=1",
                        "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                        "-pix_fmt", "yuv420p", "-r", str(FPS),
                        "-t", str(CLIP_DURATION),
                        "-an",
                        str(out_path)
                    ], f"Reencode-{scene_num}")
                    log("hf", f"  Scene {scene_num}: HF video ok ({space_id})")
                    return True

            except Exception as e:
                log("hf", f"  Scene {scene_num} attempt {attempt+1} failed on {space_id}: {str(e)[:120]}")
                time.sleep(15)

        log("hf", f"  Scene {scene_num}: {space_id} exhausted, trying next...")
        time.sleep(10)

    return False  # All spaces failed

def ken_burns_clip(image_path: Path, scene: dict, out_path: Path):
    """
    FFmpeg Ken Burns effect — smooth zoom/pan matching motion_style.
    Kids love movement, this ensures every clip has motion even as fallback.
    """
    n            = scene["scene_number"]
    motion_style = scene.get("motion_style", "zoom_in")
    total_frames = FPS * CLIP_DURATION

    # Different motion expressions for each style
    # zooming from 1.0 to 1.12 feels natural and engaging for kids
    motion_filters = {
        "zoom_in":       f"zoompan=z='min(zoom+0.0018,1.12)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={total_frames}:s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={FPS}",
        "zoom_out":      f"zoompan=z='if(lte(zoom,1.0),1.12,max(1.0,zoom-0.0018))':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={total_frames}:s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={FPS}",
        "slow_pan_right":f"zoompan=z='1.08':x='min(iw*0.1+iw*0.0015*on,iw*0.2)':y='ih/2-(ih/zoom/2)':d={total_frames}:s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={FPS}",
        "slow_pan_left": f"zoompan=z='1.08':x='max(iw*0.2-iw*0.0015*on,0)':y='ih/2-(ih/zoom/2)':d={total_frames}:s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={FPS}",
    }

    vf = motion_filters.get(motion_style, motion_filters["zoom_in"])

    run_cmd([
        "ffmpeg", "-y",
        "-loop", "1",
        "-i", str(image_path),
        "-vf", vf,
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p", "-r", str(FPS),
        "-t", str(CLIP_DURATION),
        "-an",
        str(out_path)
    ], f"KenBurns-{n}")
    log("hf", f"  Scene {n}: Ken Burns fallback used ({motion_style})")

def generate_all_clips(data: dict, image_paths: list, out_dir: Path) -> list:
    log("clips", f"Generating {NUM_SCENES} video clips...")
    clips_dir = out_dir / "clips"
    clip_paths = []

    for i, scene in enumerate(data["scenes"]):
        n        = scene["scene_number"]
        img      = image_paths[i]
        out_clip = clips_dir / f"clip_{n:02d}.mp4"

        # Try HF Space first; fall back to Ken Burns
        hf_success = image_to_video_hf(img, scene, out_clip)
        if not hf_success:
            ken_burns_clip(img, scene, out_clip)

        clip_paths.append(out_clip)
        log("clips", f"  Clip {n}/{NUM_SCENES} ready.")

    log("clips", f"All {len(clip_paths)} clips generated!")
    return clip_paths

# ─────────────────────────────────────────────────────────────
# STAGE 5 — BACKGROUND MUSIC (topic/mood matched)
# ─────────────────────────────────────────────────────────────
def get_music(slot: int, slot_cfg: dict) -> Path:
    mood  = slot_cfg.get("music_mood", "playful upbeat")
    tracks = MUSIC_BY_MOOD.get(mood, MUSIC_BY_MOOD["playful upbeat"])
    # Rotate daily so same slot doesn't always use same track
    day_idx = datetime.now().timetuple().tm_yday % len(tracks)
    url = tracks[day_idx]

    path = Path(f"assets/music_slot{slot}_day{day_idx}.mp3")
    if path.exists() and path.stat().st_size > 10_000:
        log("music", f"Cached: {path.name}")
        return path

    log("music", f"Downloading {mood} music: {url}")
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 10_000:
                path.write_bytes(r.content)
                log("music", "Music downloaded.")
                return path
        except Exception as e:
            log("music", f"Attempt {attempt+1} failed: {e}")
        time.sleep(5)

    # Silent fallback
    log("music", "WARNING: Using silent fallback audio")
    run_cmd(["ffmpeg", "-y", "-f", "lavfi", "-i",
             "anullsrc=r=44100:cl=stereo", "-t", "90", str(path)])
    return path

# ─────────────────────────────────────────────────────────────
# STAGE 6 — ASSEMBLE FINAL VIDEO
# ─────────────────────────────────────────────────────────────
def assemble_video(clip_mp4s: list, music: Path, slot_cfg: dict, out_dir: Path) -> Path:
    log("video", "Assembling final Short...")

    # Step 1 — Add silent audio to all clips (needed for concat)
    clips_with_audio = []
    for i, clip in enumerate(clip_mp4s):
        dur   = get_duration(clip)
        out_a = clip.parent / f"{clip.stem}_wa.mp4"
        run_cmd([
            "ffmpeg", "-y",
            "-i", str(clip),
            "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy", "-c:a", "aac", "-ar", "44100",
            "-t", str(dur),
            str(out_a)
        ], f"AddAudio-{i}")
        clips_with_audio.append(out_a)

    # Step 2 — Concat all clips into raw video
    concat_txt = out_dir / "concat.txt"
    concat_txt.write_text(
        "\n".join(f"file '{p.resolve()}'" for p in clips_with_audio)
    )
    raw = out_dir / "raw.mp4"
    run_cmd([
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_txt),
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-c:a", "aac", "-ar", "44100",
        "-pix_fmt", "yuv420p",
        str(raw)
    ], "Concat")

    total_dur = get_duration(raw)
    log("video", f"Raw duration: {total_dur:.1f}s")

    # Step 3 — Mix in topic-matched music with fade in/out
    date_str   = datetime.now().strftime("%Y%m%d")
    slot_name  = slot_cfg["name"].replace(" ", "_").lower()
    final      = out_dir / f"short_{slot_name}_{date_str}.mp4"
    fade_start = max(0.0, total_dur - 2.0)

    run_cmd([
        "ffmpeg", "-y",
        "-i", str(raw),
        "-stream_loop", "-1",
        "-i", str(music),
        "-filter_complex",
        (
            f"[1:a]volume={MUSIC_VOLUME},"
            f"atrim=0:{total_dur:.3f},"
            f"afade=t=in:st=0:d=1.5,"
            f"afade=t=out:st={fade_start:.3f}:d=2.0"
            f"[aout]"
        ),
        "-map", "0:v",
        "-map", "[aout]",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(final)
    ], "MixMusic")

    sz = final.stat().st_size // (1024 * 1024)
    log("video", f"Final: {final.name} ({sz} MB, {total_dur:.1f}s)")
    return final

# ─────────────────────────────────────────────────────────────
# STAGE 7 — THUMBNAIL (best frame extract)
# ─────────────────────────────────────────────────────────────
def make_thumbnail(final_video: Path, out_dir: Path) -> Path:
    """Extract most visually interesting frame (mid-video)."""
    img_dir   = out_dir / "images"
    thumb_raw = img_dir / "thumb_raw.jpg"

    dur = get_duration(final_video)
    seek_time = dur * 0.35   # 35% into video = usually best animal shot

    run_cmd([
        "ffmpeg", "-y", "-ss", str(seek_time),
        "-i", str(final_video),
        "-vframes", "1", "-s", "1280x720",
        str(thumb_raw)
    ], "ThumbExtract")

    log("video", f"Thumbnail extracted at {seek_time:.1f}s")
    return thumb_raw

# ─────────────────────────────────────────────────────────────
# STAGE 8 — SEO METADATA
# ─────────────────────────────────────────────────────────────
def build_seo_meta(data: dict) -> dict:
    seo      = data["seo"]
    raw_tags = [str(t)[:30] for t in seo.get("tags", [])]
    while raw_tags and len(",".join(raw_tags)) > 490:
        raw_tags.pop()

    return {
        "title":                     seo["title_main"][:100],
        "description":               seo["description"][:5000],
        "tags":                      raw_tags,
        "pinned_comment":            seo.get("pinned_comment", ""),
        "categoryId":                "27",
        "defaultLanguage":           LANGUAGE_CODE,
        "defaultAudioLanguage":      LANGUAGE_CODE,
        "madeForKids":               True,
        "selfDeclaredMadeForKids":   True,
    }

# ─────────────────────────────────────────────────────────────
# STAGE 9 — YOUTUBE UPLOAD (refresh token — never expires)
# ─────────────────────────────────────────────────────────────
def get_publish_time_utc(slot_cfg: dict) -> str:
    now_est = datetime.now(US_EASTERN)
    pub_est = now_est.replace(
        hour=slot_cfg["publish_hour"], minute=slot_cfg["publish_min"],
        second=0, microsecond=0
    )
    if now_est >= pub_est:
        pub_est += timedelta(days=1)
    return pub_est.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

def youtube_upload(video_path: Path, thumbnail_path: Path,
                   seo_meta: dict, slot_cfg: dict) -> str:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    REFRESH_TOKEN  = os.getenv("YT_REFRESH_TOKEN", "")
    CLIENT_ID      = os.getenv("YT_CLIENT_ID", "")
    CLIENT_SECRET  = os.getenv("YT_CLIENT_SECRET", "")

    log("upload", f"Authenticating...")
    creds = Credentials(
        token=None, refresh_token=REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/youtube.force-ssl"]
    )
    creds.refresh(Request())
    log("upload", "Credentials OK.")

    yt         = build("youtube", "v3", credentials=creds)
    publish_at = get_publish_time_utc(slot_cfg)

    body = {
        "snippet": {
            "title":               seo_meta["title"],
            "description":         seo_meta["description"],
            "tags":                seo_meta["tags"],
            "categoryId":          seo_meta["categoryId"],
            "defaultLanguage":     seo_meta["defaultLanguage"],
            "defaultAudioLanguage":seo_meta["defaultAudioLanguage"],
        },
        "status": {
            "privacyStatus":              "private",
            "publishAt":                  publish_at,
            "selfDeclaredMadeForKids":    seo_meta["selfDeclaredMadeForKids"],
            "madeForKids":                seo_meta["madeForKids"],
            "embeddable":                 True,
            "publicStatsViewable":        True,
        },
    }

    media = MediaFileUpload(str(video_path), mimetype="video/mp4",
                            resumable=True, chunksize=5*1024*1024)
    req  = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp = None
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            log("upload", f"  {int(status.progress()*100)}%")

    video_id = resp["id"]
    log("upload", f"Uploaded! youtube.com/shorts/{video_id}")

    try:
        yt.thumbnails().set(
            videoId=video_id,
            media_body=MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg")
        ).execute()
        log("upload", "Thumbnail set.")
    except Exception as e:
        log("upload", f"Thumbnail (non-fatal): {e}")

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
# MAIN
# ─────────────────────────────────────────────────────────────
def run_slot(slot: int):
    slot_cfg = SLOTS[slot]
    start    = time.time()
    out_dir  = setup_dirs(slot)

    log("pipeline", "=" * 58)
    log("pipeline", f" SLOT {slot} — {slot_cfg['name'].upper()}")
    log("pipeline", f" Publish: {slot_cfg['publish_hour']:02d}:00 US Eastern")
    log("pipeline", f" Format: Image-to-Video (HF Space) + Music, NO text")
    log("pipeline", f" Budget: $0.00")
    log("pipeline", "=" * 58)

    try:
        # 1. Pick topic
        topic = pick_topic(slot_cfg)

        # 2. Generate scene prompts + SEO
        data = generate_scenes_and_seo(topic, slot_cfg)

        # 3. Download cartoon images
        image_paths = download_images(data, out_dir)

        # 4. Generate video clips (HF Space or Ken Burns fallback)
        clip_paths = generate_all_clips(data, image_paths, out_dir)

        # 5. Get topic-mood-matched background music
        music = get_music(slot, slot_cfg)

        # 6. Assemble final video with music
        final_video = assemble_video(clip_paths, music, slot_cfg, out_dir)

        # 7. Extract thumbnail
        thumbnail = make_thumbnail(final_video, out_dir)

        # 8. Build SEO metadata
        seo_meta = build_seo_meta(data)

        # 9. Upload to YouTube
        video_id = youtube_upload(final_video, thumbnail, seo_meta, slot_cfg)

        elapsed = time.time() - start
        log("pipeline", "=" * 58)
        log("pipeline", f" DONE in {elapsed/60:.1f} min")
        log("pipeline", f" URL: youtube.com/shorts/{video_id}")
        log("pipeline", "=" * 58)

        Path("output").mkdir(exist_ok=True)
        with Path("output/upload_log.jsonl").open("a") as f:
            f.write(json.dumps({
                "date":       datetime.now(US_EASTERN).isoformat(),
                "slot":       slot,
                "niche":      slot_cfg["name"],
                "topic":      topic,
                "video_id":   video_id,
                "title":      seo_meta["title"],
                "publish_at": get_publish_time_utc(slot_cfg),
                "cost_usd":   0.00,
            }) + "\n")

    except Exception as e:
        log("pipeline", f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kids Shorts Bot v6")
    parser.add_argument("--slot", type=int, required=True, choices=[1, 2],
                        help="Slot 1 = Animal Facts (9AM), Slot 2 = Dino Facts (6PM)")
    args = parser.parse_args()
    run_slot(args.slot)
