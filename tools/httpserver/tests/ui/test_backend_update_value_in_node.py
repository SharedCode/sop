import pytest
import requests
import time
import json

SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"

def test_backend_update_value_in_node(sop_server):
    # 1. Create empty store
    store_name = f"backend_test_{int(time.time())}"
    payload = {
        "database": "TestDB",
        "store": store_name,
        "key_type": "string",
        "value_type": "string",
        "description": "Test Store",
        "slot_length": 100,
        "is_unique": True,
        "is_value_data_in_node_segment": True
    }
    res = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    assert res.status_code == 200, f"Create failed: {res.text}"

    # 2. Update IsValueDataInNodeSegment to False (Allowed for empty store)
    # Note: The API uses 'dataSize' (0=Small/InNode, 1=Medium/Separate, 2=Big/Separate+Persisted)
    update_payload = {
        "database": "TestDB",
        "storeName": store_name,
        "dataSize": 1, # Medium -> IsValueInNode = False
        "description": "Updated Description",
        "keyType": "string",
        "valueType": "string",
        "slotLength": 100,
        "isUnique": True
    }
    res = requests.post(f"{SERVER_URL}/api/store/update", json=update_payload)
    assert res.status_code == 200, f"Update failed: {res.text}"

    # Verify change
    res = requests.get(f"{SERVER_URL}/api/store/info", params={"database": "TestDB", "name": store_name})
    if res.status_code != 200:
        print(f"Get Info Failed: {res.text}")
    assert res.status_code == 200
    info = res.json()
    assert info["isValueInNode"] == False

    # 3. Add item to make it non-empty
    item_payload = {
        "database": "TestDB",
        "store": store_name,
        "key": "key1",
        "value": "val1"
    }
    res = requests.post(f"{SERVER_URL}/api/store/item/add", json=item_payload)
    assert res.status_code == 200

    # 4. Try to update IsValueDataInNodeSegment back to True (Should fail)
    # update_payload["isValueInNode"] = True # Ignored
    update_payload["dataSize"] = 0 # Small -> IsValueInNode = True
    res = requests.post(f"{SERVER_URL}/api/store/update", json=update_payload)
    assert res.status_code == 400
    assert "Structural fields (SlotLength, IsUnique, Data Size) cannot be changed for non-empty stores" in res.text
