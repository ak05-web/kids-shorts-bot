"""
=============================================================
 KIDS YOUTUBE SHORTS PIPELINE v5 — REAL 2D ANIMATION
 2 Videos/Day · Manim Animation · Animal Images · Music Only
=============================================================

 WHAT'S IN EACH VIDEO:
   • Real cartoon animal image as background (Pollinations.ai)
   • Dark overlay so text is readable
   • Fact header box slides in with animation
   • Body text fades in line by line
   • Progress bar grows at bottom
   • Animated intro + subscribe outro
   • Kids music (no voiceover)

 INSTALL:
   pip install groq pytrends requests pytz manim \
               google-api-python-client google-auth-oauthlib Pillow

   Ubuntu/GitHub Actions:
     sudo apt install -y ffmpeg libcairo2-dev libpango1.0-dev pkg-config

 USAGE:
   python pipeline_v5.py --slot 1
   python pipeline_v5.py --slot 2
=============================================================
"""

import os
import json
import time
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

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = "llama-3.3-70b-versatile"

LANGUAGE_CODE   = "en"
LANGUAGE_LOCALE = "en-US"

VIDEO_WIDTH   = 1080
VIDEO_HEIGHT  = 1920
FPS           = 30
NUM_SCENES    = 8
SCENE_SECS    = 7
MUSIC_VOLUME  = 0.80
CHANNEL_NAME  = "WOW Animals!"

US_EASTERN = pytz.timezone("America/New_York")

# ─────────────────────────────────────────────────────────────
# SLOT CONFIG
# ─────────────────────────────────────────────────────────────

SLOTS = {
    1: {
        "name"        : "Animal Facts",
        "niche"       : "fun animal facts for kids",
        "publish_hour": 9,
        "publish_min" : 0,
        "bg_color"    : "#0D1B2A",
        "accent1"     : "#FFD60A",
        "accent2"     : "#06D6A0",
        "topic_pool"  : [
            "octopus", "axolotl", "platypus", "mantis shrimp", "tardigrade",
            "chameleon", "mimic octopus", "archerfish", "pistol shrimp", "narwhal",
            "aye-aye", "pangolin", "capybara", "blue-footed booby", "shoebill stork",
            "dumbo octopus", "glass frog", "star-nosed mole", "naked mole rat",
            "quokka", "proboscis monkey", "dugong", "yeti crab", "fossa",
            "secretary bird", "blobfish", "wombat", "kinkajou", "tapir",
            "okapi", "binturong", "cassowary", "sun bear", "honey badger",
            "fennec fox", "red panda", "sugar glider",
        ],
    },
    2: {
        "name"        : "Dinosaur Facts",
        "niche"       : "dinosaur facts for kids",
        "publish_hour": 18,
        "publish_min" : 0,
        "bg_color"    : "#1A0A2E",
        "accent1"     : "#FF9F1C",
        "accent2"     : "#CBFF8C",
        "topic_pool"  : [
            "T-Rex", "velociraptor", "triceratops", "stegosaurus", "brachiosaurus",
            "ankylosaurus", "pterodactyl", "spinosaurus", "diplodocus", "allosaurus",
            "pachycephalosaurus", "parasaurolophus", "iguanodon", "carnotaurus",
            "therizinosaurus", "deinonychus", "mosasaurus", "plesiosaur",
            "microraptor", "oviraptor", "gallimimus", "ceratosaurus",
            "argentinosaurus", "giganotosaurus", "troodon", "baryonyx",
            "dilophosaurus", "compsognathus", "kentrosaurus",
        ],
    },
}

# ─────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────

def setup_dirs(slot: int) -> Path:
    base = Path(f"output/slot_{slot}")
    for sub in ["scenes", "manim_scenes", "images"]:
        (base / sub).mkdir(parents=True, exist_ok=True)
    Path("assets").mkdir(exist_ok=True)
    return base

def log(stage: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{stage.upper():10}] {msg}")

def run_cmd(cmd: list, label: str = "cmd"):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{result.stderr[-1200:]}")
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
        return float(SCENE_SECS)

def clean_json(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(lines)
    return text.strip()

def wrap_fact(text: str, max_chars: int = 20) -> list:
    words   = text.split()
    lines   = []
    current = ""
    for word in words:
        if len(current) + len(word) + 1 <= max_chars:
            current = (current + " " + word).strip()
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines[:3]


# ─────────────────────────────────────────────────────────────
# STAGE 1 — TOPIC PICKER
# ─────────────────────────────────────────────────────────────

def pick_topic(slot_cfg: dict) -> str:
    log("topic", f"Picking: {slot_cfg['name']}")
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
        log("topic", f"PyTrends skip ({e})")

    pool  = slot_cfg["topic_pool"]
    idx   = (datetime.now().timetuple().tm_yday + slot_cfg["publish_hour"]) % len(pool)
    topic = pool[idx]
    log("topic", f"Pool: {topic}")
    return topic


# ─────────────────────────────────────────────────────────────
# STAGE 2 — SCRIPT + SEO (Groq)
# ─────────────────────────────────────────────────────────────

def generate_script_and_seo(topic: str, slot_cfg: dict) -> dict:
    log("groq", f"Generating for: {topic}")
    client = Groq(api_key=GROQ_API_KEY)

    prompt = f"""You are a YouTube Shorts content creator and SEO specialist.
Channel: "{CHANNEL_NAME}" | Format: animated facts video, NO voiceover, music only
Audience: Kids aged 4-10 | Niche: {slot_cfg['niche']} | Topic: {topic}

Respond ONLY with valid raw JSON. No markdown, no explanation.

{{
  "topic": "{topic}",
  "seo": {{
    "title_main": "...(max 55 chars, e.g. 'AMAZING {topic} Facts for Kids!')",
    "title_ab":   "...(max 55 chars, different angle)",
    "description": "...(hook + 3 fact sentences + subscribe CTA, 350-450 chars)",
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
    "pinned_comment": "...(keyword + emoji question for kids)"
  }},
  "hook": "...(max 4 words ALL CAPS e.g. 'DID YOU KNOW?')",
  "cta":  "...(max 4 words ALL CAPS e.g. 'FOLLOW FOR MORE!')",
  "scenes": [
    {{"scene_number": 1, "fact_header": "...(ALL CAPS max 4 words + emoji)", "fact_body": "...(max 12 words Title Case)", "emoji": "...(1-2 emojis)"}},
    {{"scene_number": 2, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 3, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 4, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 5, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 6, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 7, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 8, "fact_header": "...", "fact_body": "...", "emoji": "..."}}
  ]
}}
RULES: Exactly 8 scenes. tags exactly 20 items. RESPOND WITH RAW JSON ONLY."""

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": "Kids YouTube specialist. Valid JSON only."},
                    {"role": "user",   "content": prompt}
                ],
                temperature=0.7,
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

    chapters_text = "\n".join(f"{ch['time']} {ch['label']}" for ch in data["seo"]["chapters"])
    data["seo"]["description"] = (
        data["seo"]["description"].rstrip()
        + f"\n\n⏱ CHAPTERS\n{chapters_text}"
        + f"\n\n{' '.join(data['seo']['hashtags'])}"
    )
    log("groq", f"Done: '{data['seo']['title_main']}'")
    return data


# ─────────────────────────────────────────────────────────────
# STAGE 3 — DOWNLOAD ANIMAL IMAGES (Pollinations.ai — free)
# One cartoon image per scene used as Manim background
# ─────────────────────────────────────────────────────────────

def download_scene_images(data: dict, out_dir: Path) -> list:
    log("images", f"Downloading {NUM_SCENES} cartoon images...")
    img_dir = out_dir / "images"
    paths   = []

    for scene in data["scenes"]:
        n   = scene["scene_number"]
        out = img_dir / f"scene_{n:02d}.jpg"

        prompt = (
            f"Cute Pixar cartoon style, bright vibrant colors, child-friendly, "
            f"{data['topic']} animal, expressive happy face, colorful background, "
            f"no text, no watermark, vertical portrait 9:16 format"
        )
        url = (
            f"https://image.pollinations.ai/prompt/{quote(prompt)}"
            f"?width={VIDEO_WIDTH}&height={VIDEO_HEIGHT}"
            f"&seed={n * 42}&nologo=true&model=flux"
        )

        ok = False
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=120)
                if r.status_code == 200 and len(r.content) > 5000:
                    out.write_bytes(r.content)
                    ok = True
                    break
            except Exception:
                pass
            log("images", f"  Scene {n} retry {attempt+1}...")
            time.sleep(5)

        if not ok:
            # Fallback: solid color background
            run_cmd([
                "ffmpeg", "-y", "-f", "lavfi",
                "-i", f"color=c=0x1a237e:size={VIDEO_WIDTH}x{VIDEO_HEIGHT}:duration=1",
                "-frames:v", "1", str(out)
            ], f"ImgFallback-{n}")
            log("images", f"  Scene {n}: fallback color")
        else:
            log("images", f"  Scene {n}: ok")

        paths.append(out)
        time.sleep(3.0)

    log("images", "All images ready")
    return paths


# ─────────────────────────────────────────────────────────────
# STAGE 4 — MANIM SCENE WRITER + RENDERER
# ─────────────────────────────────────────────────────────────

SCENE_PALETTES = [
    {"circle": "#FFD60A", "header_bg": "#FFD60A", "header_txt": "#0D1B2A", "body": "#FFFFFF"},
    {"circle": "#FF6B6B", "header_bg": "#FF6B6B", "header_txt": "#FFFFFF", "body": "#FFFFFF"},
    {"circle": "#2DD4BF", "header_bg": "#2DD4BF", "header_txt": "#002B36", "body": "#FFFFFF"},
    {"circle": "#FB923C", "header_bg": "#FB923C", "header_txt": "#1E0A00", "body": "#FFFFFF"},
    {"circle": "#818CF8", "header_bg": "#818CF8", "header_txt": "#0A1628", "body": "#FFFFFF"},
    {"circle": "#86EFAC", "header_bg": "#86EFAC", "header_txt": "#022C22", "body": "#FFFFFF"},
    {"circle": "#E879F9", "header_bg": "#E879F9", "header_txt": "#1C0027", "body": "#FFFFFF"},
    {"circle": "#FDE047", "header_bg": "#FDE047", "header_txt": "#0C0A00", "body": "#FFFFFF"},
]


def write_intro_script(topic: str, hook: str, emoji: str, slot_cfg: dict,
                       out_path: Path, scene_name: str):
    script = f'''from manim import *
config.pixel_width  = {VIDEO_WIDTH}
config.pixel_height = {VIDEO_HEIGHT}
config.frame_rate   = {FPS}

class {scene_name}(Scene):
    def construct(self):
        self.camera.background_color = ManimColor("{slot_cfg['bg_color']}")

        circles = VGroup(*[
            Circle(radius=r, color=ManimColor("{slot_cfg['accent2']}"),
                   fill_opacity=0.06, stroke_opacity=0.25)
            for r in [1.5, 2.5, 3.5, 4.5, 5.5]
        ])
        self.add(circles)

        channel = Text("{CHANNEL_NAME}", font_size=44,
                       color=ManimColor("{slot_cfg['accent1']}"), weight=BOLD)
        channel.to_edge(UP, buff=0.5)

        hook_txt = Text("{hook}", font_size=100, color=WHITE, weight=BOLD)
        hook_txt.set_stroke(color=ManimColor("{slot_cfg['accent1']}"), width=3)
        hook_txt.move_to(UP * 1.0)

        topic_txt = Text("{topic.upper()}", font_size=76,
                         color=ManimColor("{slot_cfg['accent2']}"), weight=BOLD)
        topic_txt.next_to(hook_txt, DOWN, buff=0.5)

        emoji_txt = Text("{emoji}", font_size=100)
        emoji_txt.next_to(topic_txt, DOWN, buff=0.4)

        self.play(FadeIn(channel, shift=DOWN*0.3), run_time=0.4)
        self.play(FadeIn(hook_txt, scale=0.4), run_time=0.6)
        self.play(Write(topic_txt), run_time=0.7)
        self.play(FadeIn(emoji_txt, scale=0.5, shift=UP*0.2), run_time=0.5)
        self.play(hook_txt.animate.scale(1.08), rate_func=there_and_back, run_time=0.5)
        self.wait({SCENE_SECS} - 2.7)
'''
    out_path.write_text(script)


def write_fact_script(scene_number: int, topic: str, fact_header: str,
                      fact_body: str, emoji: str, image_path: Path,
                      total_scenes: int, out_path: Path, scene_name: str):
    pal         = SCENE_PALETTES[(scene_number - 1) % len(SCENE_PALETTES)]
    prog_frac   = scene_number / total_scenes
    body_lines  = wrap_fact(fact_body, max_chars=20)
    img_str     = str(image_path.resolve()).replace("\\", "/")

    # Build body Text lines
    body_code = ""
    for line in body_lines:
        safe_line = line.replace('"', '\\"')
        body_code += f'\n        Text("{safe_line}", font_size=54, color=WHITE, weight=BOLD),'

    # Escape special chars in header
    safe_header = fact_header.replace('"', '\\"')
    safe_topic  = topic.upper().replace('"', '\\"')
    safe_emoji  = emoji.replace('"', '\\"') if emoji else "⭐"

    script = f'''from manim import *
config.pixel_width  = {VIDEO_WIDTH}
config.pixel_height = {VIDEO_HEIGHT}
config.frame_rate   = {FPS}

class {scene_name}(Scene):
    def construct(self):
        self.camera.background_color = BLACK

        # ── Animal image background ─────────────────────────────
        bg = ImageMobject("{img_str}")
        bg.stretch_to_fit_width(config.frame_width)
        bg.stretch_to_fit_height(config.frame_height)
        bg.move_to(ORIGIN)

        # Semi-transparent dark overlay so text pops
        overlay = Rectangle(
            width=config.frame_width,
            height=config.frame_height,
            fill_color=BLACK,
            fill_opacity=0.50,
            stroke_opacity=0
        )
        self.add(bg, overlay)

        # ── Scene counter ───────────────────────────────────────
        counter = Text("{scene_number}/{total_scenes}",
                       font_size=38, color=ManimColor("{pal['circle']}"), weight=BOLD)
        counter.to_corner(UL, buff=0.45)

        # ── Topic label ─────────────────────────────────────────
        topic_lbl = Text("{safe_topic}", font_size=42, color=WHITE)
        topic_lbl.set_opacity(0.80)
        topic_lbl.to_edge(UP, buff=0.42)

        # ── Emoji ───────────────────────────────────────────────
        emoji_obj = Text("{safe_emoji}", font_size=110)
        emoji_obj.move_to(UP * 2.5)

        # ── Fact header pill ────────────────────────────────────
        header_bg = RoundedRectangle(
            width=8.8, height=1.55, corner_radius=0.35,
            fill_color=ManimColor("{pal['header_bg']}"),
            fill_opacity=0.95, stroke_opacity=0
        )
        header_bg.move_to(UP * 0.55)

        header_txt = Text("{safe_header}", font_size=66,
                          color=ManimColor("{pal['header_txt']}"), weight=BOLD)
        header_txt.move_to(header_bg.get_center())

        # ── Body text ───────────────────────────────────────────
        body_group = VGroup({body_code}
        )
        body_group.arrange(DOWN, buff=0.18)
        body_group.next_to(header_bg, DOWN, buff=0.30)

        body_bg = SurroundingRectangle(
            body_group, color=BLACK,
            fill_opacity=0.65, stroke_opacity=0,
            buff=0.22, corner_radius=0.22
        )

        # ── Progress bar ─────────────────────────────────────────
        bar_bg = Rectangle(
            width=10, height=0.20,
            fill_color=WHITE, fill_opacity=0.22, stroke_opacity=0
        )
        bar_bg.to_edge(DOWN, buff=0.38)

        bar_fill = Rectangle(
            width=10 * {prog_frac:.4f}, height=0.20,
            fill_color=ManimColor("{pal['circle']}"),
            fill_opacity=1.0, stroke_opacity=0
        )
        bar_fill.move_to(bar_bg.get_left(), aligned_edge=LEFT)

        # ── Animate ─────────────────────────────────────────────
        self.play(
            FadeIn(counter),
            FadeIn(topic_lbl, shift=DOWN*0.15),
            run_time=0.30
        )
        self.play(
            FadeIn(emoji_obj, scale=0.35, shift=DOWN*0.25),
            run_time=0.45
        )
        self.play(
            FadeIn(header_bg, scale=0.82),
            run_time=0.30
        )
        self.play(
            Write(header_txt),
            run_time=0.50
        )
        self.play(
            FadeIn(body_bg),
            LaggedStart(
                *[FadeIn(line, shift=RIGHT*0.22) for line in body_group],
                lag_ratio=0.20
            ),
            run_time=0.55
        )
        self.play(
            FadeIn(bar_bg),
            GrowFromEdge(bar_fill, LEFT),
            run_time=0.38
        )
        self.play(
            header_bg.animate.scale(1.04),
            header_txt.animate.scale(1.04),
            rate_func=there_and_back,
            run_time=0.38
        )
        self.wait({SCENE_SECS} - 2.86)
'''
    out_path.write_text(script)


def write_outro_script(cta: str, slot_cfg: dict, out_path: Path, scene_name: str):
    safe_cta = cta.replace('"', '\\"')
    script = f'''from manim import *
config.pixel_width  = {VIDEO_WIDTH}
config.pixel_height = {VIDEO_HEIGHT}
config.frame_rate   = {FPS}

class {scene_name}(Scene):
    def construct(self):
        self.camera.background_color = ManimColor("{slot_cfg['bg_color']}")

        stars = VGroup(*[
            Star(n=5, outer_radius=0.3 + i*0.08,
                 color=ManimColor("{slot_cfg['accent1']}"), fill_opacity=0.25)
            .move_to([3.2*(-1 if i%2==0 else 1)*0.7, (i-4)*0.85, 0])
            for i in range(8)
        ])

        sub_txt = Text("SUBSCRIBE", font_size=104,
                       color=ManimColor("{slot_cfg['accent1']}"), weight=BOLD)
        sub_txt.move_to(UP * 1.6)

        bell = Text("🔔", font_size=95)
        bell.next_to(sub_txt, DOWN, buff=0.22)

        cta_txt = Text("{safe_cta}", font_size=58, color=WHITE, weight=BOLD)
        cta_txt.next_to(bell, DOWN, buff=0.28)

        ch_txt = Text("{CHANNEL_NAME}", font_size=44,
                      color=ManimColor("{slot_cfg['accent2']}"))
        ch_txt.to_edge(DOWN, buff=1.1)

        self.play(
            LaggedStart(*[FadeIn(s, scale=0.5) for s in stars],
                        lag_ratio=0.10),
            run_time=0.55
        )
        self.play(Write(sub_txt), run_time=0.65)
        self.play(FadeIn(bell, scale=0.5, shift=DOWN*0.25), run_time=0.38)
        self.play(
            FadeIn(cta_txt, shift=UP*0.18),
            FadeIn(ch_txt),
            run_time=0.48
        )
        self.play(sub_txt.animate.scale(1.10), rate_func=there_and_back, run_time=0.55)
        self.wait({SCENE_SECS} - 2.61)
'''
    out_path.write_text(script)


def render_manim_scene(script_path: Path, scene_name: str, out_dir: Path) -> Path:
    """Render one Manim scene. Returns path to output MP4."""
    render_dir = out_dir / "manim_scenes"

    run_cmd([
        "manim",
        str(script_path.resolve()),
        scene_name,
        "--format", "mp4",
        "--media_dir", str(render_dir.resolve()),
        "--output_file", scene_name,
        "--resolution", f"{VIDEO_WIDTH},{VIDEO_HEIGHT}",
        "--frame_rate", str(FPS),
        "-q", "h",
        "--disable_caching",
    ], f"Manim-{scene_name}")

    # Search for output file
    found = list(render_dir.rglob(f"{scene_name}.mp4"))
    if found:
        return found[0]
    raise FileNotFoundError(f"Manim output not found for {scene_name}")


def generate_all_scenes(data: dict, slot_cfg: dict,
                        image_paths: list, out_dir: Path) -> list:
    log("manim", f"Rendering {NUM_SCENES + 2} scenes...")
    sd = out_dir / "manim_scenes"
    rendered = []

    # Intro
    sname = "IntroScene"
    sp    = sd / f"{sname}.py"
    write_intro_script(
        topic=data["topic"],
        hook=data["hook"],
        emoji=data["scenes"][0].get("emoji", "🤩"),
        slot_cfg=slot_cfg,
        out_path=sp,
        scene_name=sname,
    )
    rendered.append(render_manim_scene(sp, sname, out_dir))
    log("manim", "  Intro done")

    # Fact scenes
    for scene in data["scenes"]:
        n     = scene["scene_number"]
        sname = f"Scene{n:02d}"
        sp    = sd / f"{sname}.py"
        write_fact_script(
            scene_number=n,
            topic=data["topic"],
            fact_header=scene["fact_header"],
            fact_body=scene["fact_body"],
            emoji=scene.get("emoji", "⭐"),
            image_path=image_paths[n - 1],
            total_scenes=NUM_SCENES,
            out_path=sp,
            scene_name=sname,
        )
        rendered.append(render_manim_scene(sp, sname, out_dir))
        log("manim", f"  Scene {n}/{NUM_SCENES} done")

    # Outro
    sname = "OutroScene"
    sp    = sd / f"{sname}.py"
    write_outro_script(
        cta=data["cta"],
        slot_cfg=slot_cfg,
        out_path=sp,
        scene_name=sname,
    )
    rendered.append(render_manim_scene(sp, sname, out_dir))
    log("manim", "  Outro done")

    log("manim", f"All {len(rendered)} scenes rendered!")
    return rendered


# ─────────────────────────────────────────────────────────────
# STAGE 5 — BACKGROUND MUSIC
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
            log("music", "Cached.")
            return path
    except Exception:
        pass
    run_cmd(["ffmpeg", "-y", "-f", "lavfi", "-i",
             "anullsrc=r=44100:cl=stereo", "-t", "90", str(path)])
    return path


# ─────────────────────────────────────────────────────────────
# STAGE 6 — ASSEMBLE FINAL VIDEO
# FIX: Manim outputs video-only. We add silent audio first,
#      then concat, then replace with real music.
# ─────────────────────────────────────────────────────────────

def assemble_video(scene_mp4s: list, music: Path,
                   slot_cfg: dict, out_dir: Path) -> Path:
    log("video", "Assembling final Short...")

    # Step 1 — Add silent audio track to each Manim clip
    # (Manim renders video-only; concat fails without matching streams)
    clips_with_audio = []
    for i, clip in enumerate(scene_mp4s):
        dur     = get_duration(clip)
        out_a   = clip.parent / f"{clip.stem}_wa.mp4"
        run_cmd([
            "ffmpeg", "-y",
            "-i", str(clip),
            "-f", "lavfi",
            "-i", f"anullsrc=r=44100:cl=stereo",
            "-map", "0:v",
            "-map", "1:a",
            "-c:v", "copy",
            "-c:a", "aac", "-ar", "44100",
            "-t", str(dur),
            str(out_a)
        ], f"AddSilentAudio-{i}")
        clips_with_audio.append(out_a)

    # Step 2 — Concat all clips
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
    log("video", f"Total duration: {total_dur:.1f}s")

    # Step 3 — Replace silent audio with real music
    date_str  = datetime.now().strftime("%Y%m%d")
    slot_name = slot_cfg["name"].replace(" ", "_").lower()
    final     = out_dir / f"short_{slot_name}_{date_str}.mp4"

    fade_out_start = max(0.0, total_dur - 2.0)

    run_cmd([
        "ffmpeg", "-y",
        "-i", str(raw),
        "-stream_loop", "-1",
        "-i", str(music),
        "-filter_complex",
        (
            f"[1:a]volume={MUSIC_VOLUME},"
            f"atrim=0:{total_dur:.3f},"
            f"afade=t=in:st=0:d=1.0,"
            f"afade=t=out:st={fade_out_start:.3f}:d=2.0"
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
# STAGE 7 — THUMBNAIL
# ─────────────────────────────────────────────────────────────

def make_thumbnail(final_video: Path, data: dict, out_dir: Path) -> Path:
    img_dir   = out_dir / "images"
    thumb_raw = img_dir / "thumb_raw.jpg"
    thumb_fin = img_dir / "thumb_final.jpg"

    run_cmd([
        "ffmpeg", "-y", "-ss", "4",
        "-i", str(final_video),
        "-vframes", "1", "-s", "1280x720",
        str(thumb_raw)
    ], "ThumbExtract")

    t_topic = data["topic"].upper()[:16].replace("'", "\u2019").replace(":", "\\:")
    t_title = data["seo"]["title_main"][:30].replace("'", "\u2019").replace(":", "\\:")

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
        str(thumb_fin)
    ], "ThumbText")
    return thumb_fin


# ─────────────────────────────────────────────────────────────
# STAGE 8 — SEO METADATA
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

    REFRESH_TOKEN = os.getenv("YT_REFRESH_TOKEN", "")
    CLIENT_ID     = os.getenv("YT_CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "")

    log("upload", f"Token lengths: {len(REFRESH_TOKEN)} / {len(CLIENT_ID)} / {len(CLIENT_SECRET)}")

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
        yt.videos().update(
            part="localizations",
            body={"id": video_id, "localizations": {
                LANGUAGE_CODE: {
                    "title"      : seo_meta["title"],
                    "description": seo_meta["description"]
                }
            }}
        ).execute()
        log("upload", "Localization set.")
    except Exception as e:
        log("upload", f"Localization (non-fatal): {e}")

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
    log("pipeline", f"   SLOT {slot} — {slot_cfg['name'].upper()}")
    log("pipeline", f"   Publish: {slot_cfg['publish_hour']:02d}:00 US Eastern")
    log("pipeline", f"   Format: Manim Animation + Animal Images + Music")
    log("pipeline", f"   Budget: $0.00")
    log("pipeline", "=" * 58)

    try:
        # 1. Pick topic
        topic = pick_topic(slot_cfg)

        # 2. Generate script + SEO
        data = generate_script_and_seo(topic, slot_cfg)

        # 3. Download animal images (background for each scene)
        image_paths = download_scene_images(data, out_dir)

        # 4. Render Manim animated scenes
        scene_mp4s = generate_all_scenes(data, slot_cfg, image_paths, out_dir)

        # 5. Get background music
        music = get_music(slot)

        # 6. Assemble final video with music
        final_video = assemble_video(scene_mp4s, music, slot_cfg, out_dir)

        # 7. Make thumbnail
        thumbnail = make_thumbnail(final_video, data, out_dir)

        # 8. Build SEO metadata
        seo_meta = build_seo_meta(data)

        # 9. Upload to YouTube
        video_id = youtube_upload(final_video, thumbnail, seo_meta, slot_cfg)

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
    parser = argparse.ArgumentParser(
        description="Kids Shorts v5 — Manim Animation + Animal Images + Music"
    )
    parser.add_argument(
        "--slot", type=int, choices=[1, 2], required=True,
        help="1=9AM Animal Facts, 2=6PM Dinosaur Facts (US Eastern)"
    )
    run_slot(parser.parse_args().slot)
