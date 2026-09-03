import os
import math
from PIL import Image, ImageDraw, ImageFont

WIDTH = 800
HEIGHT = 420
BG_COLOR = (9, 13, 21)
BORDER_COLOR = (30, 41, 59)
BRAND_EMERALD = (16, 185, 129)
BRAND_CYAN = (6, 182, 212)
BRAND_VIOLET = (139, 92, 246)
BRAND_ROSE = (244, 63, 94)
BRAND_AMBER = (245, 158, 11)
TEXT_WHITE = (255, 255, 255)
TEXT_MUTED = (148, 163, 184)
TEXT_DIM = (100, 116, 139)

def get_font(size, bold=False):
    # Try system fonts, fallback to default
    font_paths = [
        "/System/Library/Fonts/SFProText-Bold.otf" if bold else "/System/Library/Fonts/SFProText-Regular.otf",
        "/System/Library/Fonts/HelveticaNeue.ttc",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    ]
    for p in font_paths:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except:
                pass
    return ImageFont.load_default()

font_title = get_font(28, bold=True)
font_subtitle = get_font(16, bold=True)
font_body = get_font(13, bold=False)
font_mono = get_font(12, bold=True)
font_small = get_font(10, bold=False)

def create_base_canvas():
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    draw = ImageDraw.Draw(img)
    # Subtle grid lines
    for x in range(0, WIDTH, 32):
        draw.line([(x, 0), (x, HEIGHT)], fill=(16, 23, 38))
    for y in range(0, HEIGHT, 32):
        draw.line([(0, y), (WIDTH, y)], fill=(16, 23, 38))
    # Border
    draw.rounded_rectangle([(4, 4), (WIDTH-5, HEIGHT-5)], radius=16, outline=BORDER_COLOR, width=2)
    # Top bar
    draw.rectangle([(5, 5), (WIDTH-6, 40)], fill=(14, 21, 35))
    draw.line([(5, 40), (WIDTH-6, 40)], fill=BORDER_COLOR, width=1)
    
    # Window dots
    draw.ellipse([(18, 18), (28, 28)], fill=(239, 68, 68))
    draw.ellipse([(36, 18), (46, 28)], fill=(234, 179, 8))
    draw.ellipse([(54, 18), (64, 28)], fill=(34, 197, 94))
    
    # Header tag
    draw.text((80, 16), "SHAREDCODE SOP // DATA & COMPUTE PLATFORM", fill=TEXT_MUTED, font=font_mono)
    return img, draw

frames = []

# --- SCENE 1: Intro & Value Proposition (8 frames) ---
for i in range(12):
    img, draw = create_base_canvas()
    
    # Title badge
    draw.rounded_rectangle([(60, 70), (220, 95)], radius=12, fill=(16, 185, 129, 30), outline=BRAND_EMERALD, width=1)
    draw.text((72, 75), "⚡ SOP ARCHITECTURE", fill=BRAND_EMERALD, font=font_mono)
    
    # Main Headline
    draw.text((60, 115), "One engine for data and compute.", fill=TEXT_WHITE, font=font_title)
    draw.text((60, 155), "Scalable Objects Persistence eliminates the multi-component database tax.", fill=TEXT_MUTED, font=font_body)
    
    # 3 Pill Cards
    cards = [
        ("ACID B-Tree", "Embedded storage kernel", BRAND_EMERALD),
        ("Swarm Compute", "Distributed work queues", BRAND_CYAN),
        ("Erasure Coding", "Zero-loss fault tolerance", BRAND_VIOLET)
    ]
    for idx, (title, desc, color) in enumerate(cards):
        cx = 60 + idx * 230
        draw.rounded_rectangle([(cx, 200), (cx + 215, 290)], radius=12, fill=(14, 21, 35), outline=color, width=1)
        draw.text((cx + 16, 218), title, fill=color, font=font_subtitle)
        draw.text((cx + 16, 248), desc, fill=TEXT_MUTED, font=font_small)
        
    # Footer
    draw.text((60, 340), "WITHOUT SOP: App → DB + Queue + Locks + Retries + Failover (6+ layers)", fill=BRAND_ROSE, font=font_mono)
    draw.text((60, 365), "WITH SOP:    App → SOP Unified Engine (<0.3ms latency, 0 glue code)", fill=BRAND_EMERALD, font=font_mono)
    
    frames.append(img)

# --- SCENE 2: Technical Demo Showcase (WASM / 0-Server) (12 frames) ---
for i in range(14):
    img, draw = create_base_canvas()
    
    draw.rounded_rectangle([(60, 65), (280, 90)], radius=12, fill=(6, 182, 212, 30), outline=BRAND_CYAN, width=1)
    draw.text((72, 70), "DEMO 1: ZERO-SERVER WASM ENGINE", fill=BRAND_CYAN, font=font_mono)
    
    draw.text((60, 105), "Running Go Storage Kernel Directly in Browser", fill=TEXT_WHITE, font=font_title)
    
    # Terminal display
    draw.rounded_rectangle([(60, 150), (740, 380)], radius=12, fill=(5, 8, 14), outline=BORDER_COLOR, width=1)
    draw.text((80, 170), ">>> [ACID Begin] Initialized Transaction TX-WASM-000842 (Local V8 Sandbox)", fill=BRAND_CYAN, font=font_mono)
    draw.text((80, 195), ">>> [Isolation] Acquired B-Tree latch in Snapshot Isolation mode", fill=TEXT_MUTED, font=font_mono)
    draw.text((80, 220), ">>> [Mutation] Debited $250,000.00 from acc:001 (Acme Treasury)", fill=BRAND_VIOLET, font=font_mono)
    draw.text((80, 245), ">>> [Mutation] Credited $250,000.00 to acc:002 (Starlight Fund)", fill=BRAND_VIOLET, font=font_mono)
    draw.text((80, 270), ">>> [Consistency] Invariant check Σ(balances) == $17,500,000.00 PASSED", fill=BRAND_EMERALD, font=font_mono)
    draw.text((80, 295), ">>> [Atomic Commit] WAL flushed to local segment in 118 µs (0.118 ms)", fill=BRAND_EMERALD, font=font_mono)
    draw.text((80, 325), "✓ 0 HTTP Requests | 0 Database Server Daemons | Sub-millisecond Execution", fill=TEXT_WHITE, font=font_mono)
    
    frames.append(img)

# --- SCENE 3: SOP Arena (Distributed Topology & Swarm) (12 frames) ---
for i in range(14):
    img, draw = create_base_canvas()
    
    draw.rounded_rectangle([(60, 65), (280, 90)], radius=12, fill=(139, 92, 246, 30), outline=BRAND_VIOLET, width=1)
    draw.text((72, 70), "DEMO 2: SOP ARENA SIMULATOR", fill=BRAND_VIOLET, font=font_mono)
    
    draw.text((60, 105), "Live Distributed Systems Survival Mission", fill=TEXT_WHITE, font=font_title)
    
    # Topology Canvas Box
    draw.rounded_rectangle([(60, 150), (740, 380)], radius=12, fill=(5, 8, 14), outline=BORDER_COLOR, width=1)
    
    # Draw topology nodes
    # Ingestion
    draw.rounded_rectangle([(90, 210), (200, 270)], radius=8, fill=(14, 21, 35), outline=BRAND_CYAN, width=1)
    draw.text((105, 225), "Ingestion API", fill=TEXT_WHITE, font=font_mono)
    draw.text((105, 245), "45,000 TPS", fill=BRAND_CYAN, font=font_small)
    
    # Coordinator
    draw.rounded_rectangle([(270, 195), (410, 285)], radius=10, fill=(16, 28, 48), outline=BRAND_EMERALD, width=2)
    draw.text((285, 215), "SOP KERNEL", fill=BRAND_EMERALD, font=font_subtitle)
    draw.text((285, 240), "OCC Coordinator", fill=TEXT_MUTED, font=font_small)
    draw.text((285, 258), "0-Master Swarm", fill=TEXT_MUTED, font=font_small)
    
    # Workers
    draw.rounded_rectangle([(480, 170), (580, 215)], radius=6, fill=(14, 21, 35), outline=BRAND_EMERALD, width=1)
    draw.text((492, 185), "Worker 01 [OK]", fill=BRAND_EMERALD, font=font_small)
    
    draw.rounded_rectangle([(480, 230), (580, 275)], radius=6, fill=(14, 21, 35), outline=BRAND_EMERALD, width=1)
    draw.text((492, 245), "Worker 02 [OK]", fill=BRAND_EMERALD, font=font_small)
    
    draw.rounded_rectangle([(480, 290), (580, 335)], radius=6, fill=(14, 21, 35), outline=BRAND_EMERALD, width=1)
    draw.text((492, 305), "Worker 03 [OK]", fill=BRAND_EMERALD, font=font_small)
    
    # Storage
    draw.rounded_rectangle([(630, 190), (720, 290)], radius=8, fill=(14, 21, 35), outline=BRAND_VIOLET, width=1)
    draw.text((642, 210), "B-Tree", fill=BRAND_VIOLET, font=font_mono)
    draw.text((642, 230), "Shard 1-4", fill=TEXT_MUTED, font=font_small)
    draw.text((642, 250), "Erasure", fill=TEXT_MUTED, font=font_small)
    draw.text((642, 268), "Parity OK", fill=BRAND_EMERALD, font=font_small)
    
    # Lines
    draw.line([(200, 240), (270, 240)], fill=BRAND_CYAN, width=2)
    draw.line([(410, 220), (480, 192)], fill=BRAND_EMERALD, width=1)
    draw.line([(410, 240), (480, 252)], fill=BRAND_EMERALD, width=1)
    draw.line([(410, 260), (480, 312)], fill=BRAND_EMERALD, width=1)
    draw.line([(580, 240), (630, 240)], fill=BRAND_VIOLET, width=1)
    
    frames.append(img)

# --- SCENE 4: Disaster Injected (Hardware Fault & Crash) (10 frames) ---
for i in range(12):
    img, draw = create_base_canvas()
    
    draw.rounded_rectangle([(60, 65), (280, 90)], radius=12, fill=(244, 63, 94, 30), outline=BRAND_ROSE, width=1)
    draw.text((72, 70), "CHAOS TEST: HARDWARE FAULT", fill=BRAND_ROSE, font=font_mono)
    
    draw.text((60, 105), "Simulating Node Crashes & Concurrent Storm", fill=TEXT_WHITE, font=font_title)
    
    draw.rounded_rectangle([(60, 150), (740, 380)], radius=12, fill=(25, 10, 15), outline=BRAND_ROSE, width=2)
    
    draw.text((90, 185), "🚨 STORAGE FAULT: Storage Shard #02 disk array offline!", fill=BRAND_ROSE, font=font_subtitle)
    draw.text((90, 225), "⚠️ WORKER CRASH: Agent Worker 03 terminated abruptly mid-transaction.", fill=BRAND_AMBER, font=font_subtitle)
    draw.text((90, 265), "⚡ TRANSACTION STORM: 75,000 TPS concurrent write collision.", fill=BRAND_ROSE, font=font_subtitle)
    
    draw.rounded_rectangle([(90, 305), (710, 355)], radius=8, fill=(14, 21, 35), outline=BORDER_COLOR, width=1)
    draw.text((110, 322), "Traditional Stack Result: Split-brain deadlock, lost tasks, cascading 500s", fill=TEXT_MUTED, font=font_mono)
    
    frames.append(img)

# --- SCENE 5: SOP Autonomous Self-Healing & Recovery (12 frames) ---
for i in range(14):
    img, draw = create_base_canvas()
    
    draw.rounded_rectangle([(60, 65), (280, 90)], radius=12, fill=(16, 185, 129, 30), outline=BRAND_EMERALD, width=1)
    draw.text((72, 70), "SOP AUTONOMOUS RECOVERY", fill=BRAND_EMERALD, font=font_mono)
    
    draw.text((60, 105), "Zero-Loss Erasure Coding & Swarm Redistribution", fill=TEXT_WHITE, font=font_title)
    
    draw.rounded_rectangle([(60, 150), (740, 380)], radius=12, fill=(8, 25, 18), outline=BRAND_EMERALD, width=2)
    
    draw.text((90, 180), "✓ REED-SOLOMON PARITY RECONSTRUCTION: Missing blocks rebuilt in-memory", fill=BRAND_EMERALD, font=font_subtitle)
    draw.text((90, 220), "✓ HEARTBEAT FAILOVER: 847 queued tasks redistributed to healthy workers in 12ms", fill=BRAND_CYAN, font=font_subtitle)
    draw.text((90, 260), "✓ OCC SERIALIZATION: 0 lock corruptions across concurrent storm", fill=BRAND_EMERALD, font=font_subtitle)
    
    draw.rounded_rectangle([(90, 305), (710, 355)], radius=8, fill=(14, 21, 35), outline=BRAND_EMERALD, width=1)
    draw.text((110, 322), "SYSTEM SURVIVED: 100.00% ACID Consistency | 0 Dropped Writes", fill=TEXT_WHITE, font=font_mono)
    
    frames.append(img)

# --- SCENE 6: AI Agent Workforce & Summary (10 frames) ---
for i in range(12):
    img, draw = create_base_canvas()
    
    draw.rounded_rectangle([(60, 65), (280, 90)], radius=12, fill=(139, 92, 246, 30), outline=BRAND_VIOLET, width=1)
    draw.text((72, 70), "REAL-TIME AI AGENT SWARMS", fill=BRAND_VIOLET, font=font_mono)
    
    draw.text((60, 105), "Persistent State for Distributed AI Workloads", fill=TEXT_WHITE, font=font_title)
    
    draw.rounded_rectangle([(60, 150), (740, 380)], radius=12, fill=(14, 21, 35), outline=BORDER_COLOR, width=1)
    
    draw.text((90, 180), "DATA + COMPUTE + COORDINATION + TRANSACTIONS → SOP", fill=BRAND_EMERALD, font=font_subtitle)
    draw.text((90, 215), "• Persistent Context: AI agent reasoning buffers commit atomically in <15ms", fill=TEXT_WHITE, font=font_body)
    draw.text((90, 245), "• Vector Search: 128-d cosine similarity evaluated locally in memory", fill=TEXT_WHITE, font=font_body)
    draw.text((90, 275), "• Resilient Swarm: Workers join and leave without orphan locks or dropped tasks", fill=TEXT_WHITE, font=font_body)
    draw.text((90, 320), "Ready to explore? Run both interactive demos on GitHub Pages:", fill=BRAND_CYAN, font=font_mono)
    draw.text((90, 345), "sharedcode.github.io/sop  •  sharedcode.github.io/sop-arena", fill=TEXT_WHITE, font=font_mono)
    
    frames.append(img)

# Save as optimized GIF
gif_path = "docs/assets/sop-demo.gif"
frames[0].save(
    gif_path,
    save_all=True,
    append_images=frames[1:],
    duration=250, # 250ms per frame
    loop=0,
    optimize=True
)
print(f"✓ Successfully generated {gif_path} ({os.path.getsize(gif_path)} bytes, {len(frames)} frames)")
