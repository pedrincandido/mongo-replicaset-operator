# mongo-replicaset-operator

A Kubernetes operator, written in Go, that runs a MongoDB replica set. It does four things:
- Bootstraps the set.
- Grows and shrinks it one member at a time.
- Takes scheduled backups, which are checked by an automated restore test.
- Measures how long the cluster goes without a primary.

A chaos tool kills, freezes and steps down the primary, and reports what a client actually sees.

Everything runs locally on a kind cluster, and nothing needs a cloud account.

```console
$ kubectl get mrs
NAME   PHASE   PRIMARY                                       READY   MEMBERS   FAILOVERS   AGE
demo   Ready   demo-1.demo.default.svc.cluster.local:27017   3       3         18          25m
```

## Quick start

```sh
colima start --cpu 4 --memory 8          # or any Docker runtime
kind create cluster --name mongo-op
make install                             # CRD
go run ./cmd/main.go --metrics-bind-address=:8090 --metrics-secure=false &
kubectl apply -k config/samples/         # 3 members, backups every 30 min
kubectl get mrs -w                       # Pending → Initializing → Ready, ~30 s

hack/backup-restore-test.sh demo                                  # write → back up → drop → restore → verify
go run ./cmd/chaos --name demo --rounds 3 --mode freeze --driver tuned
```

```yaml
apiVersion: db.pedrincandido.dev/v1alpha1
kind: MongoReplicaSet
metadata:
  name: demo
spec:
  members: 3            # odd only, 3–7 (CEL validation)
  version: "8.0"
  storageSize: 1Gi
  backup:
    schedule: "*/30 * * * *"
    retain: 3
```

## What it creates

| Object | Why |
|---|---|
| Headless `Service` | Stable DNS per member (`demo-0.demo.<ns>.svc`). It publishes not-ready addresses, because members must resolve each other **before** `rs.initiate`, and that happens before they pass the readiness probe. |
| `StatefulSet` | One PVC per member. `Parallel` pod management, since the set forms after all members answer, not one by one. Pod anti-affinity is preferred, not required, so it still schedules on a single-node kind cluster. |
| `PodDisruptionBudget` | `minAvailable` = majority, so a node drain or cluster upgrade can never take away the quorum the primary needs. |
| `CronJob` + backup PVC | `mongodump` from a secondary (`readPreference=secondaryPreferred`), gzip archive, keeps the newest *N*. The PVC has no owner reference, so backups outlive the replica set they came from. |

## How the reconcile loop works

1. Make sure the objects above match the spec.
2. Read `replSetGetStatus` from **every ready member in parallel**, with a 4 s timeout each.
3. If the set is not initiated and every member answers, run `rs.initiate` once. It is idempotent: an already initiated set counts as success.
4. If the spec changed, add or remove **one** member per pass, always on the primary. To remove the primary, the operator asks it to step down first. On scale-down, the StatefulSet keeps its pods until the set has dropped them.
5. Write status, conditions (`Initialized`, `Available`), events and Prometheus metrics.

The operator **never chooses the primary**. MongoDB elects it. The operator protects the conditions for a healthy election: an odd member count, a majority kept up, and members spread across nodes. It also measures the result.

It talks to mongod through `mongosh` over the Kubernetes exec API (`internal/mongoexec`), not over the pod network. The same code works from a laptop against kind and in-cluster, with no port-forwarding.

## Results

All results come from a local kind cluster (1 node), MongoDB 8.0, 3 members and the default `electionTimeoutMillis` of 10 s. The full tables are in [`docs/chaos-results.md`](docs/chaos-results.md).

A writer inside a surviving member inserts with `writeConcern: majority` every 50 ms, using the replica set connection string. The **client gap** is the longest time between two successful writes. The **election** is measured from MongoDB's own clocks: the new primary's `electionDate` minus the last heartbeat received from the old one.

| Failure | Election (server) | Client gap, default driver | Client gap, tuned driver |
|---|---|---|---|
| Clean pod delete (SIGTERM → step-down) | step-down, no gap | **~0.5 s**, 0 failed writes | — |
| Pod delete, grace 0 | step-down, no gap | ~0.1–0.5 s | — |
| **Hung primary** (mongod frozen with SIGSTOP) | **9.8–12.7 s** | **~38 s** | **~21 s** |

What the numbers say:
- **A clean shutdown is almost free.** mongod steps down on SIGTERM, and retryable writes hide the switch. Even deleting a pod with grace 0 lands here, because the kubelet still sends SIGTERM.
- **A hung primary is the real failure.** The process stops answering but its TCP connections stay open. The other members wait for missed heartbeats and `electionTimeoutMillis`, so the election takes about 10–12 s.
- **Most of the client's outage is the driver, not the election.** With driver defaults, writes stalled for ~38 s, against an ~11 s election. A write to the hung primary waits on an open socket, and the driver's monitor is slow to mark the server unknown. Adding `socketTimeoutMS`, `heartbeatFrequencyMS=1000` and `connectTimeoutMS=2000` on the client cut the gap to ~21 s.
- The rest of that gap comes from the election timeout itself. Lowering `electionTimeoutMillis` would shorten it, at the cost of more false elections on a slow network. It isn't exposed in the spec yet; see below.

Backups: `hack/backup-restore-test.sh` writes 500 documents, runs the CronJob, drops the database, restores the newest archive with a one-off Job, and checks the count (**500/500 restored**). A backup counts only once it has been restored.

## Metrics (SLIs)

Served on the manager's `/metrics`:

| Metric | Use |
|---|---|
| `mongors_primary_available` | Availability SLI: is there a primary taking writes? |
| `mongors_failover_duration_seconds` (histogram) | Election time, from MongoDB's clocks |
| `mongors_primary_changes_total` | Failover rate |
| `mongors_replication_lag_seconds{member}` | Durability risk: how much a failover could roll back |
| `mongors_ready_members` | Headroom before losing quorum |
| `mongors_last_backup_timestamp_seconds` | Backup freshness (RPO) |

An example SLO: *a primary is available 99.9% of minutes per 30 days, and the last good backup is under 1 hour old*. Alert on burn rate, not on single failovers.

## Decisions and trade-offs

- **Durability vs latency.** The test client writes with `w: majority`. A write acknowledged by the majority survives a failover; `w: 1` is faster but can be rolled back when a lagging secondary becomes primary. Replication lag is exported for that reason.
- **One member per reconfig.** MongoDB's safe-reconfig rules allow one voting-member change at a time. Bigger jumps can leave two configurations with two different majorities.
- **Odd member counts only**, enforced by CEL in the CRD. An even count adds a voter without adding failure tolerance.
- **Status reads in parallel.** The first version asked members one by one; a single hung member then delayed detection by a full timeout. The chaos test found this.
- **The election is measured with MongoDB's clocks.** The first version measured from the operator's polling and missed every election faster than a poll.
- **Failure injection had to be real.** Deleting a pod, even with grace 0, is a clean step-down. A deny-all NetworkPolicy did not cut the already open replication connections in this kind setup. Freezing mongod with SIGSTOP from the node's PID namespace reproduces a hung node. This is kind only, through `docker exec` into the node.

## Not done yet

- Authentication: keyfile auth and SCRAM users. Today the set runs without auth, for a local lab.
- `electionTimeoutMillis` and member priorities in the spec.
- Running the operator in-cluster: the image builds with `make docker-build`, but it is not wired into CI.
- Point-in-time restore from the oplog; the backups are logical snapshots only.

## Layout

```
api/v1alpha1/          MongoReplicaSet types and validation
internal/controller/   reconcile loop, child objects, metrics
internal/mongoexec/    mongosh over the Kubernetes exec API
cmd/main.go            manager
cmd/chaos/             failure injection and measurement
hack/                  backup → restore verification
docs/chaos-results.md  raw results
```

## License

Apache 2.0.
