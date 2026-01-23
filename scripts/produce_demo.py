import os
import time
import sys
import subprocess
from pathlib import Path
from playwright.sync_api import sync_playwright

# Try to import audio libraries
try:
    from gtts import gTTS
    from mutagen.mp3 import MP3
    AUDIO_ENABLED = True
except ImportError:
    print("Warning: gTTS or mutagen not found. Audio generation disabled.")
    print("Run: pip install gTTS mutagen")
    AUDIO_ENABLED = False

class Director:
    def __init__(self, output_dir="demo_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.audio_dir = self.output_dir / "audio"
        self.audio_dir.mkdir(exist_ok=True)
        self.step_count = 0
        self.playlist = [] # List of (audio_file, duration, timestamp)

    def narrate(self, text, page=None):
        """Generates audio, plays it (simulated), and waits."""
        print(f"\n[NARRATOR]: {text}")
        self.step_count += 1
        
        if not AUDIO_ENABLED:
            time.sleep(2) # Read time
            return

        # Generate Audio
        filename = f"step_{self.step_count:03d}.mp3"
        filepath = self.audio_dir / filename
        
        tts = gTTS(text=text, lang='en', tld='co.uk') # British accent for gravitas
        tts.save(str(filepath))
        
        # Get duration
        try:
            audio = MP3(filepath)
            duration = audio.info.length
        except:
            duration = 3.0

        # "Play" it (Wait for the duration so video matches audio)
        # We add a slight padding for visual pacing
        time.sleep(duration + 0.5) 

def run_script():
    director = Director()
    
    # 1. Start the SOP Server (assuming it's running for this test, or we launch it)
    # For this demo script, we assume user ran it manually or we launch it background
    # subprocess.Popen(["./sop-httpserver", "demo_mode"]) 
    
    url = "http://localhost:8080"  # Adjust if needed

    with sync_playwright() as p:
        print("Action: Launching Browser...")
        browser = p.chromium.launch(headless=False) # Headless=False to see it
        
        # Create a context with video recording enabled
        context = browser.new_context(
            record_video_dir=str(director.output_dir),
            record_video_size={"width": 1280, "height": 720},
            viewport={"width": 1280, "height": 720}
        )
        
        page = context.new_page()
        
        # --- SCENE 1: Intro & Setup ---
        director.narrate("Welcome to the SOP Data Manager. Today, we will explore its capabilities, starting from the setup wizard.")
        
        page.goto(url)
        # Note: Selectors below are hypothetical based on typical structure, adjust to match your actual HTML IDs
        
        # Wait for load
        page.wait_for_load_state("networkidle")
        
        director.narrate("Upon first launch, the Setup Wizard guides us through configuring our environment.")
        
        # Example interaction: Clicking 'Next' or 'Start'
        # page.click("#start-wizard-btn") 
        
        # --- SCENE 2: AI Copilot ---
        director.narrate("Now, let's look at the AI Copilot interaction. This powerful tool brings generative AI directly to your data.")
        
        # Navigate to AI section
        # page.click("text=AI Copilot")
        
        director.narrate("We can execute sample prompts to query our data using natural language.")
        
        # Type a prompt
        prompt_input = page.locator("textarea[name='prompt']") # Adjust selector
        if prompt_input.count() > 0:
            prompt_text = "Analyze the sales trends for the last quarter and identify top performers."
            # Type slowly for effect
            prompt_input.type(prompt_text, delay=50) 
            
            director.narrate("As we type our query, the system prepares to analyze the underlying vector store.")
            
            # Click run
            # page.click("#run-btn")
            
            # Wait for result
            # page.wait_for_selector(".result-card")
            
            director.narrate("The results are returned instantly, providing actionable insights.")
        else:
            print("(!) Could not find prompt input - ensure server is running and selectors match.")

        # --- SCENE 3: eCommerce Demo ---
        director.narrate("Finally, let's see the eCommerce Demo in action, showcasing high-performance transaction handling.")
        
        # page.click("text=eCommerce Demo")
        # page.click("#run-simulation")
        
        director.narrate("The system handles thousands of concurrent transactions with ease, thanks to the underlying SOP architecture.")
        
        # Wait for visual effect
        time.sleep(3)

        director.narrate("Thank you for watching this demonstration of SOP Data Manager.")

        # Close context to save video
        context.close()
        browser.close()
        
    print(f"\n[DONE] Video saved to: {director.output_dir}")
    print("You can now merge the generated audio files with the video using a video editor.")

if __name__ == "__main__":
    run_script()
