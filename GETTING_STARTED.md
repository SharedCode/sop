# Getting Started with SOP

Welcome to **Scalable Objects Persistence (SOP)**! This guide will take you from downloading the software to building your first application.

## 1. Download & Installation

### Step 1: Download the Bundle
Go to the [Releases Page](https://github.com/SharedCode/sop/releases) and download the **Platform Bundle** for your operating system:

*   **macOS (Apple Silicon)**: `sop-bundle-macos-arm64.zip`
*   **macOS (Intel)**: `sop-bundle-macos-amd64.zip`
*   **Linux**: `sop-bundle-linux-amd64.zip`
*   **Windows**: `sop-bundle-windows-amd64.zip`

### Step 2: Extract & Run
Unzip the downloaded file. You will see a folder structure like this:

```text
sop-bundle/
├── sop-manager          # The Database Server & UI
├── libs/                # Shared libraries (for C/Rust)
├── python/              # Python package (.whl)
├── java/                # Java library (.jar)
└── dotnet/              # C# package (.nupkg)
```

**Start the Server:**
Open a terminal in this folder and run:

**macOS / Linux:**
```bash
chmod +x sop-manager
./sop-manager
```

**Windows:**
Double-click `sop-manager.exe` or run it from PowerShell.

---

## 2. First Run & Setup

Once the server is running, open your browser to: **[http://localhost:8080](http://localhost:8080)**

### Quick Start (Demo)
For a step-by-step walkthrough of the **Setup Wizard** and features, please see the **[SOP Demo Walkthrough](DEMO_GUIDE.md)**.

### The Setup Wizard
On your first visit, you will see the **Setup Wizard**.

1.  **Database Engine**:
    *   **Standalone**: Best for local development. Data is stored in a local folder.
    *   **Clustered**: For distributed environments. Requires a Redis connection string.

2.  **Initialize Database**:
    *   **Populate Demo Data**: Check this box! It will create a sample E-commerce database (Users, Products, Orders) so you can explore the features immediately.

3.  Click **"Initialize Database"**.

### Explore the Data
*   **Browse Stores**: Click on "user", "product", or "order" in the sidebar to see the data.
*   **AI Copilot**: Click the chat icon (bottom-left). Try asking:
    *   *"Show me the top 5 most expensive products"*
    *   *"Find all users who live in New York"*

---

## 3. Developing with SOP

Now that your server is running, you can write code to interact with it. SOP is **polyglot**, meaning you can access the same data from Go, Python, C#, or Java.

### Python
1.  **Install**:
    ```bash
    pip install python/sop-*.whl
    ```
2.  **Code**:
    ```python
    from sop.store import StoreFactory

    # Connect to the local server
    factory = StoreFactory()
    store = factory.get_store("user")

    # Add a user
    store.add("user_999", {"name": "Alice", "age": 30})
    
    # Get a user
    user = store.get("user_999")
    print(user)
    ```

### C# / .NET
1.  **Install**:
    Add the `.nupkg` to your project or local feed.
    ```bash
    dotnet add package Sop --source ./dotnet
    ```
2.  **Code**:
    ```csharp
    using Sop;

    var factory = new StoreFactory();
    var store = factory.GetStore<string, User>("user");

    store.Add("user_999", new User { Name = "Alice", Age = 30 });
    ```

### Java
1.  **Install**:
    Add the `.jar` to your classpath.
2.  **Code**:
    ```java
    import sop.StoreFactory;
    import sop.Store;

    StoreFactory factory = new StoreFactory();
    Store<String, User> store = factory.getStore("user");

    store.add("user_999", new User("Alice", 30));
    ```

---

## 4. Next Steps

*   **[Architecture Guide](ARCHITECTURE.md)**: Learn how SOP works under the hood.
*   **[Workflows](WORKFLOWS.md)**: Best practices for scaling from local dev to production swarms.
*   **[AI Copilot Guide](ai/README.md)**: Learn how to build AI-powered applications with SOP.
