# Runbook order lab

A small local experiment on the five-step workflow from `examples/verify_barrier`. It lists every order the verifier accepts that reaches `rollback_complete`, and shows what a naive order does.

This is a lab, not a feature. It makes no performance claim beyond the numbers recorded below, measured on one machine.

## Run it

```sh
go run ./examples/runbook_order_lab
go test ./examples/runbook_order_lab -race -count=1 -v
go test ./examples/runbook_order_lab -run xxx -bench . -benchtime=2s
```

Every candidate step goes through `verify.CheckAndCommit`. The lab does not decide safety itself. An order is listed only when the verifier accepts each step in it.

## What it shows

- Three orders reach `rollback_complete`. The shortest is three steps: `take_backup -> validate_backup -> restore_from_backup`.
- The naive order that tries `drop_prod_db` first is blocked once by the safety rule, then the other three steps run.

## Recorded run

Machine: darwin/amd64 under Rosetta on an arm64 Mac, go1.26.9.

| Check | Result |
| --- | --- |
| `TestAcceptedOrdersShortestFirst`, `TestNaiveOrderIsBlockedByVerifier` with `-race` | pass |
| `BenchmarkAcceptedOrders` | about 8.4 µs/op, 11.6 KB/op, 163 allocs/op |

These numbers describe this workload on this machine. They are not a general measure of the verifier.

## Limits

- Five steps and one safety rule. A larger workflow would show whether enumeration cost grows past useful limits, and this lab does not test that.
- The enumeration is exhaustive, so it is only practical for small workflows.
