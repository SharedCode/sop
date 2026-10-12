# Incidents and alert tests

A dated log for the live Azure app (`joltrin-app`, one replica) and the release pipeline. Times are UTC. Each entry says what was observed, what was done, and what was not measured. There is no availability history before the scheduled check in `.github/workflows/uptime.yml` started, so no entry claims uptime.

## 2026-10-11: v5.11.0 release build failed

- **Detected:** the `Release Joltrin Engine` run for the `v5.11.0` tag failed at the Linux and Windows library build. The `Release MCP server binaries` run for the same tag also failed.
- **Cause:** `go.work` had moved to Go 1.26.9 in the earlier security update, and `bindings/Dockerfile.build` still pinned `golang:1.26.8`. The error was `go.work requires go >= 1.26.9 (running go 1.26.8; GOTOOLCHAIN=local)` at `go mod download`. Only the release build used that Dockerfile, so ordinary CI did not catch it.
- **Fix:** bumped the pin and the version files to 5.11.1 (PR #536) and released `v5.11.1`. The `v5.11.0` tag was left in place, because moving a published tag is worse than a skipped version. Both release workflows passed for `v5.11.1`.
- **Impact:** no release was published for `v5.11.0`. Nothing was running from it.
- **Not done:** no automated check yet that every Go version pin agrees. A follow-up could compare `go.work`, the `go.mod` files and the Dockerfiles in CI.

## 2026-10-11 to 12: a deploy run waited about 15 hours at the approval gate

- **Observed:** a `Deploy to Azure Container Apps` run (started 09:09 UTC on the 11th, after a security update merged) finished the infrastructure and image steps and then waited on the `production` approval for about 15 hours. The infrastructure step had created revision `joltrin-app--0000017` with the placeholder image, which never became ready.
- **What kept serving:** revision `joltrin-app--0000016` stayed healthy and answered requests (checked by hand near the end of the wait). I did not monitor the whole window, so there is no availability data for it.
- **Action:** the run was stale, so it was cancelled and a fresh run on the latest commit was approved. Its revision became healthy and took all traffic within about two minutes of approval.
- **Lesson:** the gate did its job. The infrastructure step briefly leaves a revision that cannot become ready, and a run that has waited this long should be replaced, not approved.

## 2026-10-12 03:33 to 03:38: alert path test

- **What was tested:** that a metric alert on the app reaches the action group. A temporary rule (`joltrin-alert-test`, `Requests >= 1` over 5 minutes) was created on the app with the same action group as the real alerts, and a few requests were sent to the site.
- **Result:** Azure recorded the alert as `Fired` at 03:37:23, about 3.5 minutes after the rule was created, with notifications not suppressed. The rule was deleted at 03:38. The five real alerts were left unchanged.
- **Not tested:** that the notification email arrived (check the inbox of the address on the action group), and that the real CPU, memory, restart, 5xx and no-replica alerts fire on their own conditions. None of those five has fired.

## Not yet exercised

- Rollback to an earlier image tag (the commands are in `infra/azure/README.md`).
- Restore of the data share.
- The real 5xx and no-replica alerts.
