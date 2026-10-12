# Hosting the verification barrier: assessment

Status: an assessment and plan, plus one small implemented step that is not merged yet: a single-tenant HTTP mode for `sop-mcp-server` (see "What was built and checked"). No barrier endpoint is deployed anywhere. What is already running in Azure is a different service, described in the first section.

## What runs today, and what does not

| Service | Where it runs | What it does | Evidence |
| --- | --- | --- | --- |
| Joltrin HTTP server (`tools/httpserver`) | Azure Container Apps, one replica, East US | The Data Manager, the site's billing API and the OIDC and local sign-in. | `infra/azure`, `.github/workflows/deploy-azure.yml`. Its health check, `/metrics` and the v5.11.1 build were checked by hand against the live URL. |
| Verification barrier (`verify`, `tools/mcpserver`, `tools/a2aagent`) | Not hosted. It runs on the user's machine over MCP stdio, as an A2A server the user starts, and as WebAssembly in the browser demo. | Checks a proposed step against the recorded trace and the runbook's rules. | `cmd/sop-mcp-server`, `cmd/sop-a2a-agent`, `demo-agents`. |

The two are separate programs. The hosted server does not run the barrier, and the barrier demo does not call the hosted server.

## What already exists that a hosted barrier could reuse

Implemented and covered by tests in the repository:

- **The decision logic.** `verify` evaluates preconditions and safety rules, refuses a runbook whose rollback can dead-end, and checks and commits a step in one call under a lock (`CheckAndCommitIdempotent`). It has no knowledge of transports.
- **Two network-capable front ends.** `mcpserver.New` returns an `mcp-go` server, and `mcp-go` v1.1.1 includes a streamable HTTP server (`server.NewStreamableHTTPServer`), though the released `sop-mcp-server` only serves stdio. `tools/a2aagent` already serves A2A JSON-RPC over HTTP, with an optional shared bearer token, request timeouts, and a warning when it listens beyond loopback with no token (`cmd/sop-a2a-agent/main.go`). That command serves only the built-in `db-maintenance` example. It has no option to load your own runbooks, which the MCP server does through `SOP_RUNBOOKS`.
- **Input checks.** Workflow, step, trace and idempotency IDs are validated (`runbookstore.CheckID`), and an empty trace id is refused so callers cannot share one by accident.
- **Idempotent retries.** `idempotency_key` returns the original outcome for a retried call.
- **Observability, opt in.** `SOP_METRICS_ADDR`, `SOP_DECISION_LOG` and `SOP_RETRY_LIMIT` (PR #537) give Prometheus counters, a duration histogram and a JSON line per decision.
- **Pieces in `governance` and `tools/httpserver`.** A hash-chained in-memory audit recorder, an identity provider registry, a tier rate limiter used by the billing handler, a request body limit, and OIDC and bearer token sign-in. None of them is wired to the barrier.
- **Deployment machinery.** The Bicep modules, the build, scan and push workflow, and an approval-gated production environment.

## What a hosted barrier would be missing

These are gaps found by reading the code, not guesses:

1. **No client identity or authorization.** The A2A server has one optional shared token. Any holder can run any workflow. There is no per-client workflow access list.
2. **No tenant isolation, and a real hazard because of it.** `runbookstore.Store` keeps workflows and traces in one global namespace, and `trace_id` is chosen by the caller. On a shared endpoint, a second client who guesses or reuses a trace id could commit steps into the first client's trace and establish prerequisites that client never met. A hosted version must scope every trace and workflow by authenticated client.
3. **No durable trace state.** Traces and idempotency keys are in memory, and each trace keeps at most 256 idempotency keys (`maxIdempotencyKeysPerTrace` in `verify`). A restart forgets all of it. The failure direction is safe, since a lost trace starts empty and blocks steps that need a prerequisite, but agents would have to redo steps and retried calls could run again.
4. **No hosted MCP transport in the released binary, and no graceful shutdown anywhere under `cmd` or `tools`** (no `Shutdown` call and no signal handling). The A2A agent and the HTTP server both stop abruptly. The new HTTP mode below is the only code that shuts down gracefully.
5. **No rate limiting on the barrier,** and no audit sink wired to the barrier's decisions beyond the optional log.
6. **A trust boundary that hosting does not remove.** A committed step in the trace means the barrier let it proceed. It does not mean the action happened in the world. A hosted barrier still depends on the caller to perform, and report honestly, the steps whose states others rely on. Evidence-based states, where a step's state comes from a tool result the barrier can check, are the stronger design and need more work.

## What was built and checked

One change, local and not merged: `sop-mcp-server http` serves the same barrier over MCP streamable HTTP. It is deliberately small and single tenant.

- **Starts safe.** It listens on `127.0.0.1:8088` by default. On any other address it refuses to start unless `SOP_MCP_TOKEN` is at least 16 characters.
- **Auth and limits.** The MCP endpoint (`/mcp`) needs `Authorization: Bearer <token>`, compared in constant time on SHA-256 digests. `/healthz` needs no token. Request bodies are capped at 1 MiB, and the server sets read-header, read and idle timeouts.
- **Stops cleanly.** On SIGINT or SIGTERM it stops accepting requests and gives the ones in flight up to 10 seconds.
- **Same engine.** It calls `mcpserver.New` with the same store and options as the stdio server, so decisions are unchanged, and it works with `SOP_RUNBOOKS`, memory and the metrics from #537.
- **Single tenant, on purpose.** Everyone with the token shares one trace namespace. A test (`TestHTTPModeIsSingleTenant`) records that, so adding per-client traces has to change it deliberately.

What was checked:

- Go tests (`cmd/sop-mcp-server/httpmode_test.go`, run with `-race`): the open-listen refusal, 401 for a missing, wrong or wrongly formed token, a real MCP client blocked and then allowed over HTTP, the body cap, and shutdown on cancel. Breaking the token check made the auth test fail, so the test does test it.
- The built binary: 200 from `/healthz`, 401 without a token, 200 with it, a clean exit on SIGTERM, and exit 2 when started on `0.0.0.0` with no token.
- A container image (`docker/Dockerfile.barrier`, about 10.7 MB, distroless, non-root) deployed with `deploy/barrier/k8s.yaml` to a local `kind` cluster. The first attempt failed with `CreateContainerConfigError` because Kubernetes cannot verify that a named user is non-root, so the manifest now sets a numeric user. After that the pod became ready, `/healthz` returned 200, a call without the token returned 401, and the `drop_prod_db` sequence was blocked, blocked, allowed, allowed, allowed through a port-forward. After deleting the pod, `drop_prod_db` on the old trace was blocked again, which confirms that a restart makes the barrier stricter and not looser.

What this does not show: it was run once, by hand, on a laptop cluster. No workflow builds or tests the image, nothing is deployed, state is still in memory, and there is no per-client identity, no tenant isolation and no rate limit.

## Measured decision cost

`go test -bench` on one laptop (Apple M4, but the Go toolchain here is `darwin/amd64`, so it ran under translation and the numbers are probably pessimistic), three runs each. Single machine, in process, no network, no load test. The spread between runs is large, so read these as orders of magnitude.

| Benchmark | Per call |
| --- | --- |
| Check a step that is allowed (`CheckSafety`) | 53 to 92 ns, no allocations |
| Check a step that is blocked | 0.56 to 1.3 µs, 5 allocations |
| Check and commit a whole run (`CheckAndCommit`) | 8.8 to 15 µs |
| Replay a retried call by idempotency key | about 16 ns |
| Reachability check when a runbook is registered | 65 to 133 µs |
| `execute_step` through the in-process MCP client, blocked | 142 to 206 µs |
| `execute_step`, allowed, with memory on | 90 to 148 µs |

These show the decision itself is cheap compared with any network hop. They do not describe latency or throughput of a deployed service.

## Can it be hosted with a thin HTTP API and no change to the semantics?

Yes for the decision semantics, and the evidence is in the code: `verify` and the handlers in `mcpserver` do not depend on stdio, and `mcpserver.New` can be handed to `mcp-go`'s streamable HTTP server. The work is around the engine, not inside it. The tenant scoping in item 2 above changes how a trace is keyed. It does not change what the engine decides for a given trace, and it needs tests that prove two clients with the same `trace_id` do not affect each other.

## Options

| | A. Self-hosted | B. Managed service | C. Hybrid |
| --- | --- | --- | --- |
| What | Customers run the server next to their agents. | Customers call a Joltrin endpoint that holds their workflows and traces. | Customers run the barrier locally. An optional hosted control plane stores runbooks, audit and metrics. |
| Works today | Mostly. stdio and A2A over HTTP exist. | No. | No. |
| Customer value | Data and runbooks stay in their network. | Nothing to run. | Local latency and control, shared governance. |
| Joltrin effort | Low. HTTP transport, a container image, docs, graceful shutdown. | High. Tenancy, durable storage, billing, support. | Medium to high. Needs A first. |
| Security risk to Joltrin | Low. Customers own the runtime. | High. A tenant isolation bug exposes or corrupts another customer's state. | Medium. Only runbooks and audit leave the customer. |
| Operational burden | The customer's. | Joltrin's, including incidents and backups. | Split. |

**Recommendation.** Build A first (its first step now exists locally, see above), and run one single-tenant reference instance of it ourselves. A single-tenant deployment avoids the isolation hazard and is enough to operate the real barrier honestly, for example as the endpoint Joltrin's own agent examples and CI call. Consider C later if customers ask for shared audit. Do not offer B until tenant isolation and durable state have their own design review and tests.

## Minimal architecture for the recommendation

- **Process.** One Go binary built from the existing `mcpserver` and `a2aagent` packages, serving MCP over streamable HTTP and A2A over HTTP at a versioned path (`/v1/...`), with `/healthz` and `/metrics`, server timeouts, request body limits, a per-request deadline, and graceful shutdown on SIGTERM.
- **Authentication.** Bearer tokens, one per client, stored hashed and loaded from a secret store (Key Vault, as the existing app does). TLS ends at the Container Apps ingress with plain HTTP disallowed, as `container-app.bicep` already sets for the existing app.
- **Authorization.** Each token maps to a client id and a list of workflows it may call. Trace keys become `clientID/trace_id` inside the store, so clients cannot see or write each other's traces even in a later multi-client deployment.
- **Persistence.** Append each committed step to a durable log and rebuild traces from it on start. The first choice is a file on the Azure Files share the existing app already mounts, which fits the project's own storage engine. That choice needs a test of fsync and locking behaviour on that share before it is trusted.
- **Observability.** Reuse the counters and decision log from #537, send stderr to the Log Analytics workspace the environment already has, and add alerts for no replica, a burst of 5xx and a burst of blocked-by-auth responses.
- **Limits.** A per-client rate limit, with the existing tier rate limiter as the starting point, and the Container App's CPU and memory caps.
- **Deployment and rollback.** A second Container App in the existing environment, one replica, deployed by a workflow modelled on `deploy-azure.yml` with the same approval gate. Rollback is activating the previous revision. The rollback path has not been exercised and must be before launch.
- **Backup.** Snapshots of the file share on a schedule, with one restore rehearsed.

## Rough cost, with assumptions

From Azure's public retail price API for East US consumption pricing, checked while writing this: vCPU $0.000024 per second active and $0.000003 idle, memory $0.000003 per GiB-second, requests $0.40 per million. One always-on replica with 0.5 vCPU and 1 GiB for a 30.4 day month (about 2.63 million seconds):

- All idle: roughly $4 for vCPU and $8 for memory, about $12.
- All active: roughly $32 for vCPU and $8 for memory, about $40.

Both are before the monthly free grants and exclude Log Analytics ingestion, the container registry, storage and egress, which I did not price. A small instance with low traffic should sit near the low end, but that is an expectation, not a measurement.

## Code changes needed

| Change | Size |
| --- | --- |
| Streamable HTTP transport with timeouts, body limits and graceful shutdown | Done locally (`sop-mcp-server http`). Per-request deadlines are not added. |
| Token auth, per-client workflow access list, client-scoped trace keys | Medium |
| Durable trace and idempotency log with replay on start | Medium |
| Tests for client isolation, crash and restart, concurrent commits on one trace, and auth failures | Medium |
| Per-client rate limit and audit events for auth and block decisions | Small to medium |
| Bicep module, workflow, alerts, runbook for incidents and rollback | Medium |

## Production-readiness checklist for a hosted barrier

"Implemented" means code and tests in the repository. "Deployed and checked" means seen working against the live Azure app. Everything else is a requirement that is not met.

| # | Requirement | Status | Evidence or gap |
| --- | --- | --- | --- |
| 1 | Functional correctness | Implemented | `verify` tests and the MCP and A2A tests. |
| 2 | Unit and integration tests | Partly | In-process MCP client tests, and HTTP mode tests over a real listener. No test against a deployed endpoint, and the kind run was manual. |
| 3 | Race detection | Implemented | `go.yml` runs `-race` on `verify`, `mcpserver`, `a2aagent` and `runbookstore`. |
| 4 | Input validation | Partly | IDs are validated for MCP, and HTTP mode caps bodies at 1 MiB. The A2A path has not been audited for the same limits. |
| 5 | Authentication | Partly | One shared bearer token: optional on the A2A agent, required off loopback in the new HTTP mode. No per-client identity. |
| 6 | Authorization | Not implemented | No per-client workflow access. |
| 7 | Tenant isolation | Not implemented | One global trace namespace. |
| 8 | Persistent state and concurrency | Partly | Atomic check and commit exists. Traces are in memory only. |
| 9 | Idempotency and retries | Partly | Implemented, but the keys are lost on restart. |
| 10 | Network security | Deployed and checked, for the existing app only | TLS only ingress in `container-app.bicep`. Not applicable to a barrier that is not deployed. |
| 11 | Secrets management | Partly | Key Vault references for the existing app. Not designed for barrier tokens. |
| 12 | Dependency and vulnerability scanning | Implemented | govulncheck, Trivy, CodeQL and Dependabot run in CI. |
| 13 | Health checks | Deployed and checked for the existing app. Implemented for HTTP mode | Probes in Bicep and a 200 from the live URL. HTTP mode serves `/healthz`, tested, and used by the readiness and liveness probes on kind. |
| 14 | Latency and error metrics | Implemented, deployed for the existing app | Histogram on the HTTP server's `/metrics`. Barrier metrics (#537) are opt in and not deployed. |
| 15 | Alerting | Deployed for the existing app, not exercised | Five Azure Monitor alerts exist. None has been seen to fire. |
| 16 | Structured audit logs | Partly | Decision log from #537. The hash-chained audit recorder in `governance` is not wired to the barrier. |
| 17 | Resource limits and rate limiting | Not implemented for the barrier | CPU and memory caps exist for the existing app. |
| 18 | Deployment and rollback | Partly | Approval-gated deploy workflow exists for the existing app. A Kubernetes manifest for the barrier ran once on kind. Rollback has not been rehearsed for either. |
| 19 | Backup and recovery | Not verified | A snapshot policy for the file share has not been checked. |
| 20 | Incident response and operating docs | Not implemented | No runbook for the barrier. |

## What would make a claim like "operates a production barrier" defensible

All of the following, with evidence kept in the repository: the single-tenant instance is deployed and answering real calls from Joltrin's own agent examples; items 5, 6, 8, 15 and 17 above are met; at least four weeks of recorded uptime and latency; an alert has been seen to fire and was handled; a restore has been rehearsed. Until then, the accurate statement is that the barrier is implemented, tested and demonstrated, and that a different Go service is deployed and monitored in Azure.
