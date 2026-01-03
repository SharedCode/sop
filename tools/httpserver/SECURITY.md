
## Running Modes & Security

The SOP Data Manager is designed to be flexible, serving as both a lightweight internal tool and a secure public API.

### 1. Internal / WebUI Mode (Default)
This is the default mode, optimized for running inside a secure network (e.g., local development, VPN, or behind a corporate firewall).

- **Configuration**: `EnableRestAuth = false` (Default)
- **Behavior**: 
    - The WebUI can communicate freely with the backend.
    - The `/api/macros/execute` endpoint is open and does not require authentication.
    - **Ideal for**: Developers, internal admin panels, and trusted environments.

### 2. Public / Edge Mode (Secure)
Use this mode when deploying the server to a public environment (e.g., Kubernetes Ingress, Edge locations) where it is exposed to the internet.

- **Configuration**: 
    - Set `SOP_ENABLE_REST_AUTH=true` (or use flag `--enable-rest-auth`).
    - Set `SOP_ROOT_PASSWORD` to a strong secret.
- **Behavior**:
    - The `/api/macros/execute` endpoint requires an `Authorization: Bearer <SOP_ROOT_PASSWORD>` header.
    - Requests without the header or with an invalid token will be rejected (HTTP 401).
    - **Ideal for**: Remote programmatic access, mobile app backends, and public-facing APIs.

### Configuration Flags
| Flag | Env Variable | Description |
|------|--------------|-------------|
| `--enable-rest-auth` | `SOP_ENABLE_REST_AUTH` | Set to `true` to enforce Bearer token auth on REST endpoints. |
| (Config File) | `SOP_ROOT_PASSWORD` | The password used for Admin operations and REST API Bearer token. |

### Example: Running in Secure Mode
```bash
export SOP_ROOT_PASSWORD="my-secret-token"
export SOP_ENABLE_REST_AUTH="true"
./sop_server --port 8080
```

### Example: Calling the Secure API
```bash
curl -X POST http://localhost:8080/api/macros/execute \
  -H "Authorization: Bearer my-secret-token" \
  -H "Content-Type: application/json" \
  -d '{"name": "my_macro", "args": {"param": "value"}}'
```
