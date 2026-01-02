import pytest
import subprocess
import os
import time
import json
import tempfile
import shutil

# Calculate path to server binary relative to this file
# tools/httpserver/tests/ui/conftest.py -> tools/httpserver/sop_server
SERVER_BINARY = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../sop_server"))

@pytest.fixture(scope="module")
def sop_server():
    """Starts the SOP HTTP server for testing with a temporary database."""
    
    # Create a temporary directory for the database
    temp_dir = tempfile.mkdtemp(prefix="sop_test_db_")
    
    # Create a temporary config file
    config_data = {
        "port": 8080,
        "pageSize": 40,
        "databases": [
            {
                "name": "TestDB",
                "path": temp_dir,
                "mode": "standalone"
            }
        ]
    }
    
    config_file_path = os.path.join(temp_dir, "test_config.json")
    with open(config_file_path, "w") as f:
        json.dump(config_data, f)
        
    print(f"Starting server with config: {config_file_path}")
    print(f"Database path: {temp_dir}")

    # Ensure server binary exists
    if not os.path.exists(SERVER_BINARY):
        # Try to build it
        print("Building sop_server...")
        try:
            subprocess.run(["go", "build", "-o", SERVER_BINARY, "."], cwd=os.path.dirname(SERVER_BINARY), check=True)
        except subprocess.CalledProcessError as e:
            pytest.fail(f"Failed to build server: {e}")

    # Start the server process
    process = subprocess.Popen(
        [SERVER_BINARY, "-config", config_file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for server to be ready
    # Simple sleep is flaky, but sufficient for now. 
    # Ideally we should poll the health endpoint.
    time.sleep(2)
    
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        print(f"Server failed to start:\nSTDOUT: {stdout}\nSTDERR: {stderr}")
        pytest.fail("Server failed to start")
        
    print("Server is ready!")
    yield process
    
    print("Stopping server...")
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        
    # Cleanup
    shutil.rmtree(temp_dir)
