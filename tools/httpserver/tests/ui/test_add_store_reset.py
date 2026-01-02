import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import signal
import sys

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"

def test_add_store_reset_after_edit(page: Page, sop_server):
    """
    Verifies that the 'Add Store' form is reset to a clean state
    after previously opening the 'Edit Store' modal.
    """
    
    # Dialog handler to accept alerts
    page.on("dialog", lambda dialog: dialog.accept())

    # 1. Load the page
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")

    # 2. Create a new store to have something to edit
    test_store_name = f"reset_test_{int(time.time())}"
    test_desc = "Description for reset test"

    add_btn = page.locator("button[title='Add new store']")
    expect(add_btn).to_be_visible()
    add_btn.click()

    expect(page.locator("#add-store-modal")).to_be_visible()

    page.fill("#new-store-name", test_store_name)
    page.fill("#new-store-description", test_desc)
    page.select_option("#new-store-key-type", "string")
    page.select_option("#new-store-value-type", "string")
    
    # Add seed data to make it non-empty (optional, but good for realism)
    page.fill("#new-store-seed-key", "key1")
    page.fill("#new-store-seed-value-simple", "val1")

    page.click("#add-store-modal .modal-footer .btn-success") # Add button

    # Wait for store to appear and be selected
    page.reload()
    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=30000)
    store_item.click()

    # 3. Open Edit Store Modal
    edit_btn = page.locator("button[onclick='showEditStore()']")
    expect(edit_btn).to_be_visible()
    edit_btn.click()
    
    expect(page.locator("#add-store-modal")).to_be_visible()
    expect(page.locator("#add-store-modal .modal-header span").first).to_have_text("Edit Store Metadata")
    
    # Verify it is populated
    expect(page.locator("#new-store-name")).to_have_value(test_store_name)
    expect(page.locator("#new-store-description")).to_have_value(test_desc)

    # 4. Close the modal (Cancel)
    # The close button is a span with an onclick handler, not a class .close
    page.click("#add-store-modal .modal-header span[onclick='closeAddStoreModal()']")
    expect(page.locator("#add-store-modal")).not_to_be_visible()

    # 5. Open Add Store Modal again
    add_btn.click()
    expect(page.locator("#add-store-modal")).to_be_visible()
    expect(page.locator("#add-store-modal .modal-header span").first).to_have_text("Add New Store")

    # 6. Verify fields are CLEARED
    expect(page.locator("#new-store-name")).to_have_value("")
    expect(page.locator("#new-store-description")).to_have_value("")
    expect(page.locator("#new-store-key-type")).to_have_value("string")
    expect(page.locator("#new-store-value-type")).to_have_value("string")
    
    # Switch to Map type to check buttons
    page.select_option("#new-store-key-type", "map")
    
    # Verify Add Buttons are ENABLED
    add_key_btn = page.locator("button[onclick='addKeyFieldRow()']")
    expect(add_key_btn).to_be_enabled()
    
    add_index_btn = page.locator("button[onclick='addIndexFieldRow()']")
    expect(add_index_btn).to_be_enabled()
    
    # Verify buttons are reset
    save_btn = page.locator("#add-store-modal .modal-footer .btn-success")
    expect(save_btn).to_have_text("Add")
    expect(save_btn).to_be_enabled()

    # Verify Unlock Admin button is hidden
    expect(page.locator("#btn-unlock-admin")).not_to_be_visible()

