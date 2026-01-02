import pytest
from playwright.sync_api import Page, expect
import time
import requests

SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"

def test_update_structure_empty_store(page: Page, sop_server):
    try:
        page.goto(SERVER_URL)
    except Exception as e:
        pytest.fail(f"Failed to load page: {e}")

    # Create a new empty store
    store_name = f"struct_test_{int(time.time())}"
    
    # Wait for Add Store button
    page.wait_for_selector("button[title='Add new store']", timeout=5000)
    page.click("button[title='Add new store']")
    
    page.fill("#new-store-name", store_name)
    page.click("#add-store-modal .btn-success")
    
    # Wait for store to appear and click it
    page.wait_for_selector(f"text={store_name}", timeout=5000)
    page.click(f"text={store_name}")
    
    # Click Edit Store
    page.click("#btn-edit-store")
    
    # 1. Check Data Size (was IsValueDataInNodeSegment)
    select_size = page.locator("#new-store-data-size")
    expect(select_size).to_be_enabled()
    # Select Medium (1)
    select_size.select_option("1")

    # 2. Check SlotLength
    input_slot = page.locator("#new-store-slot-length")
    expect(input_slot).to_be_enabled()
    input_slot.fill("500")

    # 3. Check IsUnique
    checkbox_unique = page.locator("#new-store-is-unique")
    expect(checkbox_unique).to_be_enabled()
    checkbox_unique.uncheck()
    
    # Save
    page.click("text=Update")
    
    # Verify success message or modal close
    expect(page.locator("#add-store-modal")).to_be_hidden()
    
    # Re-open Edit Store to verify persistence
    page.click("#btn-edit-store")
    
    expect(select_size).to_have_value("1")
    expect(input_slot).to_have_value("500")
    expect(checkbox_unique).not_to_be_checked()
    
    page.click("#add-store-modal .modal-footer button:has-text('Cancel')")

def test_update_structure_non_empty_store(page: Page, sop_server):
    page.goto(SERVER_URL)
    
    # Create a new store
    store_name = f"struct_locked_{int(time.time())}"
    page.wait_for_selector("button[title='Add new store']", timeout=5000)
    page.click("button[title='Add new store']")
    page.fill("#new-store-name", store_name)
    page.click("#add-store-modal .btn-success")
    
    # Wait for store to appear and click it
    page.wait_for_selector(f"text={store_name}", timeout=5000)
    page.click(f"text={store_name}")
    
    # Add an item to make it non-empty
    page.click("button[title='Add a new item to the store.']")
    page.wait_for_selector("#add-modal", state="visible")
    page.fill("#add-key-json", "\"key1\"")
    page.fill("#add-value-json", "\"value1\"")
    page.click("button[title='Add the new item to the store.']")

    # Refresh page to ensure fresh state and updated count
    page.reload()
    page.wait_for_selector(f"text={store_name}", timeout=5000)
    page.click(f"text={store_name}")
    
    # Click Edit Store
    page.click("#btn-edit-store")
    
    # Check if fields are DISABLED
    expect(page.locator("#new-store-data-size")).to_be_disabled()
    expect(page.locator("#new-store-slot-length")).to_be_disabled()
    expect(page.locator("#new-store-is-unique")).to_be_disabled()
    
    page.click("#add-store-modal .modal-footer button:has-text('Cancel')")
