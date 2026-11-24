# Roadmap

A Kubernetes operator in Go that runs a MongoDB replica set: it bootstraps the set, survives a primary failure, takes scheduled backups with a tested restore, and exposes SLO metrics. Everything runs locally on a kind cluster: no cloud account is needed.

## Local setup

```sh
colima start --cpu 4 --memory 8      # Docker runtime
kind create cluster --name mongo-op  # local Kubernetes
make install                         # CRDs into the cluster
make run                             # controller on your machine, against kind
kubectl apply -k config/samples/     # a 3-member MongoReplicaSet
```

## Stages

Each stage ends with something you can demo and a short entry in `docs/decisions.md`.

| # | Stage | Done when | Pedro writes |
|---|---|---|---|
| 1 | **API**: `MongoReplicaSet` spec (members, version, storage size, backup schedule) and status (phase, primary, members ready, last backup) | `kubectl get mongoreplicasets` shows phase and primary | the spec/status fields and validation markers |
| 2 | **Reconcile: StatefulSet + headless Service**, a PVC per member, owner references | 3 pods with stable DNS names, data on PVCs | the reconcile loop |
| 3 | **Bootstrap**: `rs.initiate` once all members answer; idempotent on restart | `rs.status()` shows 1 primary + 2 secondaries; restarting the controller changes nothing | the init logic and the idempotency check |
| 4 | **Health and status**: poll `hello`/`replSetGetStatus`, write the primary and lag into status, emit Kubernetes events | `kubectl describe` shows the current primary and replication lag | the status mapping |
| 5 | **Failover**: detect a lost primary, confirm election settles, protect quorum with a PodDisruptionBudget and anti-affinity | killing the primary pod → a new primary, recorded in status and events | the failover detection and the PDB / anti-affinity choices |
| 6 | **Backup and restore**: CronJob with `mongodump` to a local PVC (or MinIO in-cluster), plus a restore Job; a test that writes data, backs up, wipes, restores and verifies | the restore test passes in CI | the restore verification |
| 7 | **SLO metrics**: Prometheus metrics (replica set available, election count, time to new primary, replication lag, backup age) and an SLO doc | a Grafana panel or `curl /metrics` shows them | choosing the SLIs and targets |
| 8 | **Chaos test**: a Go test that kills the primary N times and measures time to a writable primary (p50/p95) | the numbers are in the README | the test and the analysis |
| 9 | **Publish**: README with the architecture, the decisions (durability vs latency: write concern `majority`, journaling, election timeout) and the chaos results; CI on GitHub Actions with kind | public repo, green CI | the README decisions |

## Interview story it gives

Why durability and latency trade off (write concern, read preference, election timeout); what happens when a replica set loses quorum; why replicas must not share a failure domain (the Itaú leader-election lesson); how you'd measure recovery and defend an SLO.

## Status (2026-11-24)

| # | Stage | Status |
|---|---|---|
| 1 | API | ✅ spec/status, CEL rule for odd members, print columns, short name `mrs` |
| 2 | StatefulSet + headless Service + PVCs | ✅ |
| 3 | Bootstrap | ✅ idempotent `rs.initiate`, Ready in ~30 s |
| 4 | Health and status | ✅ parallel `replSetGetStatus`, per-member state and lag, events |
| 5 | Failover protection | ✅ PDB (majority), anti-affinity, one-member reconfig, step-down before removing the primary; scale 3 → 5 → 3 tested |
| 6 | Backup and restore | ✅ CronJob from a secondary; `hack/backup-restore-test.sh` 500/500 restored |
| 7 | SLO metrics | ✅ six `mongors_*` metrics; election measured from mongo's clocks |
| 8 | Chaos test | ✅ `cmd/chaos` graceful / force / freeze, default vs tuned driver; results in `docs/chaos-results.md` |
| 9 | Publish | ⏳ README done, CI workflow `chaos.yml` written; repo not pushed yet |

Next: keyfile auth, `electionTimeoutMillis` in the spec, in-cluster deployment in CI.
