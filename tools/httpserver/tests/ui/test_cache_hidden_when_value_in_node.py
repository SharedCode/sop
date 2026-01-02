import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_cache_controls_hidden_when_value_in_node_checked(page: Page, sop_server):
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")

    # Open Add Store modal
    add_btn = page.locator('button[title="Add new store"]')
    expect(add_btn).to_be_visible()
    add_btn.click()

    expect(page.locator('#add-store-modal')).to_be_visible()

    # Enable advanced mode (reveals ValueInNode + cache options)
    adv = page.locator('#new-store-advanced-mode')
    expect(adv).to_be_visible()
    adv.check()

    advanced_container = page.locator('#advanced-options-container')
    expect(advanced_container).to_be_visible()

    data_size = page.locator('#new-store-data-size')
    expect(data_size).to_be_visible()

    cache_container = page.locator('#cache-options-container')

    # Default is Small (0) -> cache controls should be hidden
    expect(data_size).to_have_value("0")
    expect(cache_container).to_be_hidden()

    # Select Medium (1) -> cache controls visible
    data_size.select_option("1")
    expect(cache_container).to_be_visible()

    # Select Small (0) -> hidden
    data_size.select_option("0")
    expect(cache_container).to_be_hidden()
    
    # Select Big (2) -> hidden
    data_size.select_option("2")
    expect(cache_container).to_be_hidden()
