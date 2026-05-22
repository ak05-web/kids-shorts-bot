"""
=============================================================
 KIDS YOUTUBE SHORTS PIPELINE v5 — REAL 2D ANIMATION
 2 Videos/Day · Manim Animation · Music Only · No Voiceover
=============================================================

 HOW IT WORKS:
   Groq generates facts → Python dynamically writes Manim
   scene code → Manim renders real animated MP4 per scene
   → FFmpeg stitches + adds music → YouTube upload

 EACH SCENE HAS:
   • Colorful animated background with moving shapes
   • Fact text writes itself onto screen (typewriter effect)
   • Animal name bounces in from top
   • Stars/circles pop around the fact
   • Smooth fade transitions between scenes
   • Progress bar animates at bottom

 INSTALL:
   pip install groq pytrends requests pytz \
               google-api-python-client google-auth-oauthlib \
               manim

   System deps (Ubuntu/GitHub Actions):
     sudo apt install -y libcairo2-dev libpango1.0-dev \
       ffmpeg texlive-full

   Mac:
     brew install cairo pango ffmpeg
     pip install manim

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
import textwrap
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

LANGUAGE_CODE   = "en"
LANGUAGE_LOCALE = "en-US"

VIDEO_WIDTH   = 1080
VIDEO_HEIGHT  = 1920
FPS           = 30
NUM_SCENES    = 8
SCENE_SECS    = 7        # seconds per animated scene
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
        "bg_color"    : "#0D1B2A",   # deep navy
        "accent1"     : "#FFD60A",   # bright yellow
        "accent2"     : "#06D6A0",   # teal green
        "accent3"     : "#FF6B6B",   # coral red
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
        "bg_color"    : "#1A0A2E",   # deep purple
        "accent1"     : "#FF9F1C",   # orange
        "accent2"     : "#CBFF8C",   # lime green
        "accent3"     : "#E0FBFC",   # light cyan
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
    for sub in ["scenes", "manim_scenes"]:
        (base / sub).mkdir(parents=True, exist_ok=True)
    Path("assets").mkdir(exist_ok=True)
    return base

def log(stage: str, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{stage.upper():10}] {msg}")

def run_cmd(cmd: list, label: str = "cmd", cwd: Path = None):
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
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

def wrap_fact(text: str, max_chars: int = 22) -> list[str]:
    """Split fact into lines of max_chars for Manim rendering."""
    words = text.split()
    lines = []
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
    return lines[:3]  # max 3 lines


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

    pool = slot_cfg["topic_pool"]
    idx  = (datetime.now().timetuple().tm_yday + slot_cfg["publish_hour"]) % len(pool)
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
Channel: "{CHANNEL_NAME}" | Format: 2D animated facts video, NO voiceover, music only
Audience: Kids aged 4-10 | Niche: {slot_cfg['niche']} | Topic: {topic}

Respond ONLY with valid raw JSON. No markdown, no explanation.

{{
  "topic": "{topic}",
  "seo": {{
    "title_main": "...(max 55 chars, shock format e.g. 'AMAZING {topic} Facts for Kids!')",
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
  "hook":  "...(max 4 words ALL CAPS e.g. 'DID YOU KNOW?')",
  "cta":   "...(max 4 words ALL CAPS e.g. 'FOLLOW FOR MORE!')",
  "scenes": [
    {{
      "scene_number": 1,
      "fact_header":  "...(max 4 words ALL CAPS, the WOW hook e.g. '3 HEARTS!')",
      "fact_body":    "...(max 12 words, simple explanation, Title Case)",
      "emoji":        "...(1-2 relevant emojis as string)"
    }},
    {{"scene_number": 2, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 3, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 4, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 5, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 6, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 7, "fact_header": "...", "fact_body": "...", "emoji": "..."}},
    {{"scene_number": 8, "fact_header": "...", "fact_body": "...", "emoji": "..."}}
  ]
}}

RULES:
- Exactly 8 scenes
- fact_header: ALL CAPS max 4 words + emoji
- fact_body: max 12 words, Title Case, simple for kids
- tags: exactly 20 items
- RESPOND WITH RAW JSON ONLY"""

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": "Kids YouTube content specialist. Respond with valid JSON only."},
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
# STAGE 3 — MANIM ANIMATION RENDERER
#
# We dynamically write a Manim Python script for each scene,
# then run manim to render it as an MP4.
# ─────────────────────────────────────────────────────────────

# Color palettes per scene (alternates for visual variety)
SCENE_PALETTES = [
    {"bg": "#0D1B2A", "circle": "#FFD60A", "rect": "#06D6A0", "header": "#FFFFFF", "body": "#FFD60A"},
    {"bg": "#1A0833", "circle": "#FF6B6B", "rect": "#4ECDC4", "header": "#FFFFFF", "body": "#FF6B6B"},
    {"bg": "#002B36", "circle": "#2DD4BF", "rect": "#F59E0B", "header": "#FFFFFF", "body": "#2DD4BF"},
    {"bg": "#1E0A00", "circle": "#FB923C", "rect": "#34D399", "header": "#FFFFFF", "body": "#FB923C"},
    {"bg": "#0A1628", "circle": "#818CF8", "rect": "#F472B6", "header": "#FFFFFF", "body": "#818CF8"},
    {"bg": "#022C22", "circle": "#86EFAC", "rect": "#FCD34D", "header": "#FFFFFF", "body": "#86EFAC"},
    {"bg": "#1C0027", "circle": "#E879F9", "rect": "#67E8F9", "header": "#FFFFFF", "body": "#E879F9"},
    {"bg": "#0C0A00", "circle": "#FDE047", "rect": "#F87171", "header": "#FFFFFF", "body": "#FDE047"},
]


def write_manim_scene(
    scene_number: int,
    topic: str,
    fact_header: str,
    fact_body: str,
    emoji: str,
    is_intro: bool,
    is_outro: bool,
    hook: str,
    cta: str,
    slot_cfg: dict,
    total_scenes: int,
    out_script: Path,
    scene_name: str,
):
    """
    Write a complete Manim Python script for one scene.
    Uses only standard Manim objects — no external images needed.
    """
    pal = SCENE_PALETTES[(scene_number - 1) % len(SCENE_PALETTES)]

    # Wrap fact body for multi-line display
    body_lines = wrap_fact(fact_body, max_chars=20)
    body_str   = " / ".join(body_lines)  # will be split in Manim code

    # Build body VGroup lines code
    body_lines_code = ""
    for i, line in enumerate(body_lines):
        body_lines_code += f"""
        Text("{line}", font_size=52, color=ManimColor("{pal['body']}"), weight=BOLD),"""

    # Progress fraction
    prog_frac = scene_number / total_scenes

    if is_intro:
        scene_code = f"""
    def construct(self):
        self.camera.background_color = ManimColor("{slot_cfg['bg_color']}")

        # Animated circles background
        circles = VGroup(*[
            Circle(radius=r, color=ManimColor("{slot_cfg['accent2']}"), fill_opacity=0.08, stroke_opacity=0.3)
            for r in [1.5, 2.5, 3.5, 4.5]
        ])
        self.add(circles)
        self.play(
            *[Rotate(c, angle=TAU, run_time=8, rate_func=linear) for c in circles],
            run_time=0
        )

        # Channel name top
        channel = Text("{CHANNEL_NAME}", font_size=44, color=ManimColor("{slot_cfg['accent1']}"), weight=BOLD)
        channel.to_edge(UP, buff=0.5)

        # Hook text — bounces in
        hook_txt = Text("{hook}", font_size=110, color=WHITE, weight=BOLD)
        hook_txt.set_stroke(color=ManimColor("{slot_cfg['accent1']}"), width=3)

        # Topic name
        topic_txt = Text("{topic.upper()}", font_size=80, color=ManimColor("{slot_cfg['accent2']}"), weight=BOLD)
        topic_txt.next_to(hook_txt, DOWN, buff=0.4)

        # Emoji
        emoji_txt = Text("{emoji}", font_size=90)
        emoji_txt.next_to(topic_txt, DOWN, buff=0.5)

        # Animate in sequence
        self.play(FadeIn(channel, shift=DOWN*0.3), run_time=0.4)
        self.play(
            hook_txt.animate.scale(1).set_opacity(1),
            FadeIn(hook_txt, scale=0.3),
            run_time=0.6
        )
        self.play(
            Write(topic_txt),
            run_time=0.8
        )
        self.play(
            FadeIn(emoji_txt, scale=0.5, shift=UP*0.2),
            run_time=0.5
        )

        # Pulsing effect on hook
        self.play(
            hook_txt.animate.scale(1.08),
            rate_func=there_and_back,
            run_time=0.5
        )

        # Hold
        self.wait({SCENE_SECS} - 2.8)
"""
    elif is_outro:
        scene_code = f"""
    def construct(self):
        self.camera.background_color = ManimColor("{slot_cfg['bg_color']}")

        # Star burst background shapes
        stars = VGroup(*[
            Star(n=5, outer_radius=0.3+i*0.1, color=ManimColor("{slot_cfg['accent1']}"), fill_opacity=0.2)
            .move_to([
                3.5*(-1 if i%2==0 else 1)*0.7,
                (i-4)*0.9,
                0
            ])
            for i in range(8)
        ])

        subscribe_txt = Text("SUBSCRIBE", font_size=110, color=ManimColor("{slot_cfg['accent1']}"), weight=BOLD)
        subscribe_txt.move_to(UP * 1.5)

        bell_txt = Text("🔔", font_size=100)
        bell_txt.next_to(subscribe_txt, DOWN, buff=0.2)

        cta_txt = Text("{cta}", font_size=60, color=WHITE, weight=BOLD)
        cta_txt.next_to(bell_txt, DOWN, buff=0.3)

        channel_txt = Text("{CHANNEL_NAME}", font_size=46, color=ManimColor("{slot_cfg['accent2']}"))
        channel_txt.to_edge(DOWN, buff=1.2)

        self.play(LaggedStart(
            *[FadeIn(s, scale=0.5) for s in stars],
            lag_ratio=0.1,
            run_time=0.6
        ))
        self.play(
            Write(subscribe_txt),
            run_time=0.7
        )
        self.play(
            FadeIn(bell_txt, scale=0.5, shift=DOWN*0.3),
            run_time=0.4
        )
        self.play(
            FadeIn(cta_txt, shift=UP*0.2),
            FadeIn(channel_txt),
            run_time=0.5
        )
        self.play(
            subscribe_txt.animate.scale(1.1),
            rate_func=there_and_back,
            run_time=0.6
        )
        self.wait({SCENE_SECS} - 2.8)
"""
    else:
        # Regular fact scene
        scene_code = f"""
    def construct(self):
        self.camera.background_color = ManimColor("{pal['bg']}")

        # ── Background decorations ──────────────────────────────
        # Large soft circle top-right
        bg_circle1 = Circle(radius=3.2, color=ManimColor("{pal['circle']}"),
                            fill_opacity=0.07, stroke_opacity=0)
        bg_circle1.move_to([2.5, 4, 0])

        # Large soft circle bottom-left
        bg_circle2 = Circle(radius=2.8, color=ManimColor("{pal['rect']}"),
                            fill_opacity=0.07, stroke_opacity=0)
        bg_circle2.move_to([-2.5, -4, 0])

        # Rounded rectangle accent strip left edge
        strip = RoundedRectangle(width=0.4, height=14, corner_radius=0.2,
                                  color=ManimColor("{pal['circle']}"), fill_opacity=0.3,
                                  stroke_opacity=0)
        strip.to_edge(LEFT, buff=0)

        self.add(bg_circle1, bg_circle2, strip)

        # ── Scene counter top-left ──────────────────────────────
        counter = Text("{scene_number}/{total_scenes}", font_size=36,
                       color=ManimColor("{pal['circle']}"), weight=BOLD)
        counter.to_corner(UL, buff=0.5)

        # ── Topic name top ──────────────────────────────────────
        topic_label = Text("{topic.upper()}", font_size=40,
                           color=WHITE)
        topic_label.set_opacity(0.7)
        topic_label.to_edge(UP, buff=0.4)

        # ── Emoji (large, center-top area) ─────────────────────
        emoji_obj = Text("{emoji}", font_size=120)
        emoji_obj.move_to(UP * 2.8)

        # ── Fact header — ALL CAPS big bold ────────────────────
        header_bg = RoundedRectangle(
            width=9, height=1.5, corner_radius=0.3,
            color=ManimColor("{pal['circle']}"), fill_opacity=1, stroke_opacity=0
        )
        header_bg.move_to(UP * 0.8)

        header_txt = Text("{fact_header}", font_size=70,
                          color=ManimColor("{pal['bg']}"), weight=BOLD)
        header_txt.move_to(header_bg.get_center())

        # ── Fact body lines ─────────────────────────────────────
        body_group = VGroup({body_lines_code}
        )
        body_group.arrange(DOWN, buff=0.15)
        body_group.move_to(DOWN * 0.8)

        # ── Progress bar at bottom ──────────────────────────────
        bar_bg = Rectangle(width=10, height=0.18,
                           color=WHITE, fill_opacity=0.2, stroke_opacity=0)
        bar_bg.to_edge(DOWN, buff=0.5)

        bar_fill = Rectangle(width=10 * {prog_frac:.3f}, height=0.18,
                             color=ManimColor("{pal['circle']}"),
                             fill_opacity=1, stroke_opacity=0)
        bar_fill.move_to(bar_bg.get_left(), aligned_edge=LEFT)

        # ── ANIMATIONS ─────────────────────────────────────────
        # Everything fades/slides in sequence
        self.play(
            FadeIn(counter),
            FadeIn(topic_label, shift=DOWN*0.2),
            run_time=0.3
        )
        self.play(
            FadeIn(emoji_obj, scale=0.4, shift=DOWN*0.3),
            run_time=0.5
        )
        self.play(
            FadeIn(header_bg, scale=0.8),
            run_time=0.3
        )
        self.play(
            Write(header_txt),
            run_time=0.5
        )
        self.play(
            LaggedStart(
                *[FadeIn(line, shift=RIGHT*0.3) for line in body_group],
                lag_ratio=0.2
            ),
            run_time=0.6
        )
        self.play(
            FadeIn(bar_bg),
            GrowFromEdge(bar_fill, LEFT),
            run_time=0.4
        )

        # Small pulse on header
        self.play(
            header_bg.animate.scale(1.03),
            header_txt.animate.scale(1.03),
            rate_func=there_and_back,
            run_time=0.4
        )

        # Hold rest of scene
        self.wait({SCENE_SECS} - 3.0)
"""

    # Full Manim script
    script = f'''from manim import *
config.pixel_width  = {VIDEO_WIDTH}
config.pixel_height = {VIDEO_HEIGHT}
config.frame_rate   = {FPS}

class {scene_name}(Scene):
{scene_code}
'''
    out_script.write_text(script)


def render_manim_scene(script_path: Path, scene_name: str, out_dir: Path) -> Path:
    """
    Run manim to render one scene. Returns path to rendered MP4.
    """
    render_dir = out_dir / "manim_scenes"

    run_cmd([
        "manim",
        str(script_path.resolve()),
        scene_name,
        "--format", "mp4",
        "--media_dir", str(render_dir.resolve()),
        "--output_file", scene_name,
        "-q", "m",
        "--disable_caching",
    ], f"Manim-{scene_name}")

    # Find the rendered file
    search_paths = [
        render_dir / "videos" / script_path.stem / f"{VIDEO_HEIGHT}p{FPS}" / f"{scene_name}.mp4",
        render_dir / "videos" / script_path.stem / "720p30" / f"{scene_name}.mp4",
        render_dir / f"{scene_name}.mp4",
    ]
    for p in search_paths:
        if p.exists():
            return p

    # Fallback — search recursively
    found = list(render_dir.rglob(f"{scene_name}.mp4"))
    if found:
        return found[0]

    raise FileNotFoundError(f"Manim output not found for {scene_name}")


def generate_all_scenes(data: dict, slot_cfg: dict, out_dir: Path) -> list[Path]:
    """
    Write + render all Manim scenes. Returns list of MP4 paths.
    """
    log("manim", f"Rendering {NUM_SCENES + 2} animated scenes...")
    scripts_dir = out_dir / "manim_scenes"
    rendered    = []
    total       = NUM_SCENES + 2   # intro + 8 scenes + outro

    # ── INTRO ─────────────────────────────────────────────────
    scene_name   = "IntroScene"
    script_path  = scripts_dir / f"{scene_name}.py"
    write_manim_scene(
        scene_number=0, topic=data["topic"],
        fact_header="", fact_body="", emoji="",
        is_intro=True, is_outro=False,
        hook=data["hook"], cta=data["cta"],
        slot_cfg=slot_cfg, total_scenes=NUM_SCENES,
        out_script=script_path, scene_name=scene_name,
    )
    mp4 = render_manim_scene(script_path, scene_name, out_dir)
    rendered.append(mp4)
    log("manim", f"  Intro done")

    # ── FACT SCENES ────────────────────────────────────────────
    for scene in data["scenes"]:
        n          = scene["scene_number"]
        scene_name = f"Scene{n:02d}"
        script_path = scripts_dir / f"{scene_name}.py"

        write_manim_scene(
            scene_number=n,
            topic=data["topic"],
            fact_header=scene["fact_header"],
            fact_body=scene["fact_body"],
            emoji=scene.get("emoji", "⭐"),
            is_intro=False, is_outro=False,
            hook=data["hook"], cta=data["cta"],
            slot_cfg=slot_cfg,
            total_scenes=NUM_SCENES,
            out_script=script_path,
            scene_name=scene_name,
        )
        mp4 = render_manim_scene(script_path, scene_name, out_dir)
        rendered.append(mp4)
        log("manim", f"  Scene {n}/{NUM_SCENES} done")

    # ── OUTRO ─────────────────────────────────────────────────
    scene_name  = "OutroScene"
    script_path = scripts_dir / f"{scene_name}.py"
    write_manim_scene(
        scene_number=NUM_SCENES+1, topic=data["topic"],
        fact_header="", fact_body="", emoji="",
        is_intro=False, is_outro=True,
        hook=data["hook"], cta=data["cta"],
        slot_cfg=slot_cfg, total_scenes=NUM_SCENES,
        out_script=script_path, scene_name=scene_name,
    )
    mp4 = render_manim_scene(script_path, scene_name, out_dir)
    rendered.append(mp4)
    log("manim", f"  Outro done")

    log("manim", f"All {len(rendered)} scenes rendered!")
    return rendered


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
            log("music", "Cached.")
            return path
    except Exception:
        pass
    run_cmd(["ffmpeg", "-y", "-f", "lavfi", "-i",
             "anullsrc=r=44100:cl=stereo", "-t", "90", str(path)])
    return path


# ─────────────────────────────────────────────────────────────
# STAGE 5 — ASSEMBLE FINAL VIDEO
# ─────────────────────────────────────────────────────────────

def assemble_video(scene_mp4s: list[Path], music: Path, slot_cfg: dict,
                   out_dir: Path) -> Path:
    log("video", "Assembling final Short...")

    # Concat all scene MP4s
    concat_txt = out_dir / "concat.txt"
    concat_txt.write_text("\n".join(f"file '{p.resolve()}'" for p in scene_mp4s))

    raw = out_dir / "raw.mp4"
    run_cmd([
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_txt),
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p",
        str(raw)
    ], "Concat")

    total_dur = get_duration(raw)

    date_str  = datetime.now().strftime("%Y%m%d")
    slot_name = slot_cfg["name"].replace(" ", "_").lower()
    final     = out_dir / f"short_{slot_name}_{date_str}.mp4"

    run_cmd([
        "ffmpeg", "-y",
        "-i", str(raw),
        "-stream_loop", "-1", "-i", str(music),
        "-filter_complex",
        (
            f"[1:a]volume={MUSIC_VOLUME},"
            f"atrim=0:{total_dur:.2f},"
            f"afade=t=in:st=0:d=0.8,"
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
# STAGE 6 — THUMBNAIL (FFmpeg — first scene frame + text)
# ─────────────────────────────────────────────────────────────

def make_thumbnail(final_video: Path, data: dict, slot_cfg: dict, out_dir: Path) -> Path:
    thumb_raw   = out_dir / "images" / "thumb_raw.jpg"
    thumb_final = out_dir / "images" / "thumb_final.jpg"
    (out_dir / "images").mkdir(exist_ok=True)

    # Extract frame at 3 seconds (mid-intro)
    run_cmd([
        "ffmpeg", "-y",
        "-ss", "3",
        "-i", str(final_video),
        "-vframes", "1",
        "-s", "1280x720",
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
        str(thumb_final)
    ], "ThumbText")

    return thumb_final


# ─────────────────────────────────────────────────────────────
# STAGE 7 — SEO METADATA
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
# STAGE 8 — YOUTUBE UPLOAD
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
            "title": seo_meta["title"], "description": seo_meta["description"],
            "tags": seo_meta["tags"], "categoryId": seo_meta["categoryId"],
            "defaultLanguage": seo_meta["defaultLanguage"],
            "defaultAudioLanguage": seo_meta["defaultAudioLanguage"],
        },
        "status": {
            "privacyStatus": "private", "publishAt": publish_at,
            "selfDeclaredMadeForKids": seo_meta["selfDeclaredMadeForKids"],
            "madeForKids": seo_meta["madeForKids"],
            "embeddable": True, "publicStatsViewable": True,
        },
    }

    media = MediaFileUpload(str(video_path), mimetype="video/mp4",
                            resumable=True, chunksize=5*1024*1024)
    req = yt.videos().insert(part="snippet,status", body=body, media_body=media)
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
                LANGUAGE_CODE: {"title": seo_meta["title"], "description": seo_meta["description"]}
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
    log("pipeline", f"   Format: Real 2D Animation · Manim · Music Only")
    log("pipeline", f"   Budget: $0.00")
    log("pipeline", "=" * 58)

    try:
        topic       = pick_topic(slot_cfg)
        data        = generate_script_and_seo(topic, slot_cfg)
        scene_mp4s  = generate_all_scenes(data, slot_cfg, out_dir)
        music       = get_music(slot)
        final_video = assemble_video(scene_mp4s, music, slot_cfg, out_dir)
        thumbnail   = make_thumbnail(final_video, data, slot_cfg, out_dir)
        seo_meta    = build_seo_meta(data)
        video_id    = youtube_upload(final_video, thumbnail, seo_meta, slot_cfg)

        elapsed = time.time() - start
        log("pipeline", "=" * 58)
        log("pipeline", f"   DONE in {elapsed/60:.1f} min")
        log("pipeline", f"   URL: youtube.com/shorts/{video_id}")
        log("pipeline", "=" * 58)

        Path("output").mkdir(exist_ok=True)
        with Path("output/upload_log.jsonl").open("a") as f:
            f.write(json.dumps({
                "date"     : datetime.now(US_EASTERN).isoformat(),
                "slot"     : slot, "niche": slot_cfg["name"], "topic": topic,
                "video_id" : video_id, "title": seo_meta["title"],
                "publish_at": get_publish_time_utc(slot_cfg), "cost_usd": 0.00,
            }) + "\n")

        return {"success": True, "video_id": video_id}

    except Exception as e:
        log("pipeline", f"ERROR: {e}")
        import traceback; traceback.print_exc()
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kids Shorts v5 — Real 2D Manim Animation")
    parser.add_argument("--slot", type=int, choices=[1, 2], required=True,
                        help="1=9AM Animal Facts, 2=6PM Dinosaur Facts (US Eastern)")
    run_slot(parser.parse_args().slot)
