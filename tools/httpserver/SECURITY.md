## Running Modes & Security
The SOP Data Manager is designed to be flexible, serving as both a lightweight internal tool and a secure public API.

### Phase 1 Auth Notes
The current V1 implementation uses a small token-auth facade built on top of `Config.Users` and the setup-created root user:

- `POST /api/auth/login` accepts JSON or OAuth2-style form credentials.
- The response returns `access_token` and `token_type: Bearer`.
- Protected endpoints require `Authorization: Bearer <token>`.
- `session_token_ttl_minutes` controls token lifetime (default: 60 minutes) so tokens renew on a predictable cadence.
- The facade is intentionally abstract so a later B-tree-backed or encrypted token implementation can be swapped in without changing the HTTP contract.

### 1. Internal / WebUI Mode (Default)
This is the default mode, optimized for running inside a secure network (e.g., local development, VPN, or behind a corporate firewall).

- **Configuration**: `EnableRestAuth = false` (Default)
- **Behavior**: 
    - The WebUI can communicate freely with the backend.
    - The `/api/scripts/execute` endpoint is open and does not require authentication.
    - **Ideal for**: Developers, internal admin panels, and trusted environments.

### 2. Public / Edge Mode (Secure)
Use this mode when deploying the server to a public environment (e.g., Kubernetes Ingress, Edge locations) where it is exposed to the internet.

- **Configuration**:
    - Set `enable_rest_auth=true` in config or use `--enable-rest-auth` to require bearer auth on protected REST endpoints.
    - Use the configured root user from `Config.Users` for V1 login.
    - Optionally set `session_token_ttl_minutes` to control the token renewal window.
- **Behavior**:
    - The `/api/scripts/execute` endpoint requires an `Authorization: Bearer <SOP_ROOT_PASSWORD>` header.
    - Requests without the header or with an invalid token will be rejected (HTTP 401).
    - **Ideal for**: Remote programmatic access, mobile app backends, and public-facing APIs.

### Configuration Flags
| Flag | Env Variable | Description |
|------|--------------|-------------|
| `--enable-rest-auth` | `SOP_ENABLE_REST_AUTH` | Set to `true` to enforce Bearer token auth on REST endpoints. |
| (Config File) | `SOP_ROOT_PASSWORD` | The password used for Admin operations and REST API Bearer token. |

### Example: Running in Secure Mode
```bash
export SOP_ENABLE_REST_AUTH="true"
./sop_server --port 8080
```

### Example: Calling the Secure API
```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "username=root" \
  --data-urlencode "password=your-password"

curl -X POST http://localhost:8080/api/scripts/execute \
  -H "Authorization: Bearer <access_token_from_login>" \
  -H "Content-Type: application/json" \
  -d '{"name": "my_script", "args": {"param": "value"}}'
```
