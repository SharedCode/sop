import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_streaming_sets_big_data_size(page: Page, sop_server):
    # 1. Load the page
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")
    
    # 2. Open Add Store Modal
    page.locator("#btn-add-store").click()
    expect(page.locator("#add-store-modal")).to_be_visible()
    
    # 3. Enable Advanced Mode to see Data Size
    page.locator("#new-store-advanced-mode").check()
    expect(page.locator("#advanced-options-container")).to_be_visible()
    
    # 4. Verify initial Data Size is Small (0)
    data_size = page.locator("#new-store-data-size")
    expect(data_size).to_have_value("0")
    
    # 5. Check Streaming Mode
    page.locator("#new-store-is-streaming").check()
    
    # 6. Verify Data Size changed to Big (2)
    expect(data_size).to_have_value("2")
    
    print("Test passed!")
