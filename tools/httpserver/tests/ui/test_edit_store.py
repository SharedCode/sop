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

def test_edit_store_workflow(page: Page, sop_server):
    # Listen for console logs
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))
    page.on("pageerror", lambda exc: print(f"BROWSER ERROR: {exc}\nSTACK: {exc.stack}\nLOC: {getattr(exc, 'location', 'unknown')}"))
    
    # Dialog handler
    dialog_mode = {"mode": "default"}
    def handle_dialog(dialog):
        print(f"Dialog triggered: {dialog.type}")
        if dialog_mode["mode"] == "unlock_admin":
            if dialog.type == "prompt":
                dialog.accept("root") # Assuming 'root' is the password
            elif dialog.type == "alert":
                dialog.accept()
        else:
            dialog.dismiss()
    
    page.on("dialog", handle_dialog)

    # 1. Load the page
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")

    # 2. Create a new store to ensure clean state
    test_store_name = f"test_store_{int(time.time())}"

    # Wait for the button to be visible
    add_btn = page.locator("button[title='Add new store']")
    expect(add_btn).to_be_visible()
    add_btn.click()

    expect(page.locator("#add-store-modal")).to_be_visible()

    page.fill("#new-store-name", test_store_name)
    page.fill("#new-store-description", "Initial Description")
    page.select_option("#new-store-key-type", "string")
    page.select_option("#new-store-value-type", "string")

    # Cache controls are only applicable when Value data is NOT stored in-node.
    # Enable Advanced options and set Data Size to Medium (1) for this test.
    page.check("#new-store-advanced-mode")
    page.select_option("#new-store-data-size", "1")

    # Set initial seed data
    page.fill("#new-store-seed-key", "initial_key")
    page.fill("#new-store-seed-value-simple", "initial_value")

    page.click("#add-store-modal .modal-footer .btn-success")

    # Wait a bit for server to process
    page.wait_for_timeout(2000)
    
    # Reload page to force list update
    page.reload()

    # Wait for store to appear in list and be selected
    # The UI selects it automatically. Check if it appears in the sidebar.
    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=30000)
    store_item.click() # Ensure it is selected

    # 3. Open Edit Store Modal
    # Wait for the "Edit Store" button to be enabled/visible
    edit_btn = page.locator("button[onclick='showEditStore()']")
    expect(edit_btn).to_be_visible()
    edit_btn.click()
    expect(page.locator("#add-store-modal")).to_be_visible()
    expect(page.locator("#add-store-modal .modal-header span").first).to_have_text("Edit Store Metadata")

    # 4. Verify Fields State (Populated Store)
    # Unlock button should NOT be visible for primitive key store
    expect(page.locator("#btn-unlock-admin")).not_to_be_visible()

    # Key Type is only enabled if store is empty. Since we added seed data, it should be disabled.
    expect(page.locator("#new-store-key-type")).to_be_disabled()

    # Value Type should be disabled (Structural)
    expect(page.locator("#new-store-value-type")).to_be_disabled()
    
    # Streaming Checkbox should be disabled (Structural)
    expect(page.locator("#new-store-is-streaming")).to_be_disabled()

    # Data Size should be disabled (Structural)
    expect(page.locator("#new-store-data-size")).to_be_disabled()

    # Cache options should be visible and Cache Duration should be enabled (Config)
    expect(page.locator("#cache-options-container")).to_be_visible()
    expect(page.locator("#new-store-cache-duration")).to_be_enabled()

    # 5. Update Cache Duration and Save
    page.fill("#new-store-cache-duration", "60")
    page.click("#add-store-modal .modal-footer .btn-success") # Update button

    # Wait for success alert
    # The dialog handler will accept the alert.
    # We can verify by reopening the modal and checking the value.
    page.wait_for_timeout(2000)
    
    # Reopen Edit Store
    edit_btn.click()
    expect(page.locator("#add-store-modal")).to_be_visible()
    
    # Check if value persisted
    expect(page.locator("#new-store-cache-duration")).to_have_value("60")

def test_edit_empty_store_workflow(page: Page, sop_server):
    # Listen for console logs
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))
    
    # Dialog handler
    dialog_mode = {"mode": "default"}
    def handle_dialog(dialog):
        print(f"Dialog triggered: {dialog.type}")
        if dialog_mode["mode"] == "unlock_admin":
            if dialog.type == "prompt":
                dialog.accept("root")
            elif dialog.type == "alert":
                dialog.accept()
        else:
            dialog.dismiss()

    page.on("dialog", handle_dialog)

    # 1. Load the page
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")

    # 2. Create a new EMPTY store
    test_store_name = f"empty_store_{int(time.time())}"

    add_btn = page.locator("button[title='Add new store']")
    expect(add_btn).to_be_visible()
    add_btn.click()

    page.fill("#new-store-name", test_store_name)
    page.select_option("#new-store-key-type", "string")
    
    # Do NOT fill seed data
    
    page.click("#add-store-modal .modal-footer .btn-success")

    # Wait a bit for server to process
    page.wait_for_timeout(2000)
    
    # Reload page to force list update
    page.reload()

    # Wait for store to appear in list and be selected
    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=30000)
    store_item.click()

    # 3. Open Edit Store Modal
    edit_btn = page.locator("button[onclick='showEditStore()']")
    expect(edit_btn).to_be_visible()
    edit_btn.click()
    
    # 4. Verify Fields State (Empty Store)
    # Unlock button should NOT be visible (already unlocked)
    expect(page.locator("#btn-unlock-admin")).not_to_be_visible()

    # Verify fields are unlocked for EMPTY store
    expect(page.locator("#new-store-key-type")).to_be_enabled()
    expect(page.locator("#new-store-value-type")).to_be_enabled()
    expect(page.locator("#new-store-is-streaming")).to_be_enabled()
    expect(page.locator("#new-store-cache-duration")).to_be_enabled()
    expect(page.locator("#new-store-data-size")).to_be_enabled()
