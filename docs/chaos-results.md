# Chaos results

Raw output of `go run ./cmd/chaos`, appended run by run. Local kind cluster (1 node, colima on Apple silicon), MongoDB 8.0, 3 members, default `electionTimeoutMillis` (10 s). See the README for what the numbers mean.

### demo — mode `graceful`, driver `default`, 3 rounds (2026-11-24)

| Round | Failed member | New primary | Client majority-write gap | Failed attempts | Election (server, from operator) |
|---|---|---|---|---|---|
| 1 | demo-0 | demo-1 | 0.53s | 0 | n/a (clean step-down) |
| 2 | demo-1 | demo-0 | 0.53s | 0 | n/a (clean step-down) |
| 3 | demo-0 | demo-1 | 0.55s | 0 | n/a (clean step-down) |

**p50 0.53s · p95 0.53s · max 0.55s**

### demo — mode `force`, driver `default`, 3 rounds (2026-11-24)

| Round | Failed member | New primary | Client majority-write gap | Failed attempts | Election (server, from operator) |
|---|---|---|---|---|---|
| 1 | demo-1 | demo-0 | 0.12s | 0 | n/a (clean step-down) |
| 2 | demo-0 | demo-1 | 0.53s | 0 | n/a (clean step-down) |
| 3 | demo-1 | demo-0 | 0.07s | 0 | n/a (clean step-down) |

**p50 0.12s · p95 0.12s · max 0.53s**

### demo — mode `freeze`, driver `default`, 3 rounds (2026-11-24)

| Round | Failed member | New primary | Client majority-write gap | Failed attempts | Election (server, from operator) |
|---|---|---|---|---|---|
| 1 | demo-1 | demo-2 | 38.07s | 2 | 9.8s |
| 2 | demo-2 | demo-1 | 37.93s | 2 | 9.8s |
| 3 | demo-1 | demo-0 | 38.14s | 2 | 11.4s |

**p50 38.07s · p95 38.07s · max 38.14s**

### demo — mode `freeze`, driver `tuned`, 3 rounds (2026-11-24)

| Round | Failed member | New primary | Client majority-write gap | Failed attempts | Election (server, from operator) |
|---|---|---|---|---|---|
| 1 | demo-0 | demo-2 | 19.14s | 23 | 10.3s |
| 2 | demo-2 | demo-0 | 22.78s | 23 | 11.7s |
| 3 | demo-0 | demo-1 | 21.23s | 18 | 12.7s |

**p50 21.23s · p95 21.23s · max 22.78s**

