# Thumbnail Generation Service

Scans a file system for video files and generates thumbnail preview images for them.

The work is split into **four decoupled services** connected by **Redpanda** (Kafka-wire-compatible
broker): an **orchestrator** that issues commands, a **scanner** that discovers videos on demand, a
**generator** that produces thumbnails, and a **sync-service** that replicates finished thumbnails to
their destination. This models the real problem shape — a fast, I/O-bound scan feeding a slow,
CPU-bound generation step, followed by a delivery step — and lets the parts scale, fail, and be
controlled independently. Nothing scans automatically: the scanner, generator, and sync-service are
long-lived daemons that sit idle until commanded / fed work.

---

## How it works

All messages flow through **Redpanda** — every service only produces to or consumes from a topic;
no service talks to another directly. The broker is the hub in the middle, holding four topics:

```
  orchestrator        scanner-service          generator-service          sync-service
  (CLI, one-shot)     (daemon)                 (daemon)                   (daemon)
        │             ▲       │                ▲    │      │              ▲
        │ produce     │ scan- │ produce        │    │ scan │ produce      │ consume
        │ scan-cmds   │ cmds  │ video-jobs     │    │ cmds │ thumbnail-   │ thumbnail-
        │             │       │ + scan-events  │    │      │ ready        │ ready
        ▼             │       ▼                │    ▼      ▼              │
  ╔═════════════════════════════════════════════════════════════════════╪═══════╗
  ║                            R E D P A N D A                            │       ║
  ║  [ scan-commands ]   [ video-jobs ]   [ scan-events ]   [ thumbnail-ready ]   ║
  ╚══════════════════════════════════════════════════════════════════════════════╝
                                              │                            │
                                      ffmpeg → thumbnail            "video X synced"
                                              ▼                            ▼
                            thumbnails (mirror-tree) + videoDb.jsonl   videoDb row: synced=true
```

Who produces and consumes each topic:

| Topic | Produced by | Consumed by |
|---|---|---|
| `scan-commands` | orchestrator | scanner (`StartScan`/`StopScan`), generator (`Pause`/`Resume`) |
| `video-jobs` | scanner | generator |
| `scan-events` | scanner | orchestrator / observers |
| `thumbnail-ready` | generator | sync-service |

So a typical scan is: orchestrator **produces** `StartScan` → scanner **consumes** it, walks the
directory, and **produces** `VideoJob`s (and `ScanStarted`/`ScanCompleted` events) → generator
**consumes** the jobs, makes thumbnails, writes a `videoDb.jsonl` row (`synced: false`), and
**produces** a `ThumbnailReady` event → sync-service **consumes** it, replicates the thumbnail, and
flips that row to `synced: true`. No service ever calls another — Redpanda sits between every pair.

1. **orchestrator-service** is a thin CLI and the control plane. It resolves a `--tenant <id>` to a
   scan root via the tenant catalog, then publishes one command onto the `scan-commands` topic and
   exits: `StartScan` (with the resolved path, `scanId`, and `tenantId`), `StopScan`,
   `PauseGenerator`, `ResumeGenerator`. This is where scheduling/automation would live in production.
2. **scanner-service** is a long-lived daemon consuming `scan-commands`. On `StartScan` it walks the
   given directory (iteratively, depth-guarded), detects videos by extension, and produces one
   `VideoJob` per video onto `video-jobs` — tagged with the `scanId`. It runs one scan at a time
   (FIFO queue), can be cancelled mid-scan (`StopScan`), and emits `ScanStarted`/`ScanCompleted`/
   `ScanCancelled` on `scan-events` for the orchestrator to observe.
3. **Redpanda** holds the queues. The broker provides backpressure (the consumer pulls at its own
   pace), durability, and at-least-once delivery — the scanner never holds the whole work-set in memory.
4. **generator-service** is a long-lived daemon consuming `video-jobs`. For each job it runs ffmpeg to
   extract a thumbnail, with a per-job timeout (kill on hang), bounded retries with backoff, and
   idempotent skipping if the thumbnail already exists. It records the outcome (carrying the `scanId`)
   as a `videoDb.jsonl` row with `synced: false`, and — when a thumbnail actually exists (generated or
   skipped) — produces a `ThumbnailReady` event for the sync-service. It also consumes `scan-commands`
   to honour `Pause`/`Resume`. (A `failed` job produces nothing, so it emits no ready event.)
5. **sync-service** is a long-lived daemon consuming `thumbnail-ready`. For each event it "syncs" the
   thumbnail to its destination (a stand-in that logs `video X synced` — in production an rsync / scp /
   S3 upload to the server that serves the previews), then flips that video's `videoDb.jsonl` row to
   `synced: true`. Same write-ahead commit as the generator: the offset commits only after the sync +
   DB update succeed, and re-syncing is idempotent, so a crash mid-sync just redelivers.

### Key principles

- **Command-driven, nothing automatic** — daemons start idle and act only on orchestrator commands.
  You can scan a tenant at runtime, scan a different one, queue scans, stop, or pause/resume —
  without restarting a process.
- **Multi-tenant** — every scan is tenant-scoped; the orchestrator resolves `--tenant <id>` to a path
  from its catalog, and the `tenantId` flows through jobs/events/results for per-tenant output and
  tracking. Operators work with logical tenants, never raw paths.
- **Command vs. event separation** — `scan-commands` carries imperatives (orchestrator → services);
  `scan-events` carries facts (scanner → orchestrator). Commands and events never share a topic.
- **Decoupled producer/consumer** — scan and generation run in parallel; the first video is being
  processed while the scan is still walking. The slow generator scales horizontally — run more
  instances and the broker splits the work across them, no code change (see [Scaling](#scaling-the-generator)).
- **Write-ahead commit** — a video-job offset is committed only *after* the job reaches a terminal
  state (`autoCommit: false`). A crash mid-work redelivers the message, so no thumbnail is silently lost.
- **Resilient by default** — permission errors, broken symlinks, TOCTOU races, ffmpeg hangs, corrupt
  videos, and poison messages are all handled and logged; one bad entry never aborts the run.
- **Everything injected (DI)** — the file system, the ffmpeg launcher, the logger, and the result
  sink are all interfaces wired through an awilix container, so each piece is swappable and unit-
  testable with fakes (no real I/O in tests).
- **Idempotent / resumable** — re-running skips videos whose thumbnail already exists (`--force` /
  `GENERATE_FORCE=true` to override).

---

## Project structure (pnpm monorepo)

```
packages/
├── domain/        # @thumbnailer/domain   — pure interfaces (IFileSystem, IProcess, ILogger) + test fakes
├── contracts/     # @thumbnailer/contracts — shared schemas/builders (VideoJob, ThumbnailResult,
│                  #                            ScanCommand, ScanEvent, topic names) via zod
└── core/          # @thumbnailer/core      — createService() + infra providers (config, logger, kafka,
                   #                            kafkaUtils, signals, commands) wired into one DI container
apps/
├── orchestrator-service/ # CLI: TenantResolver + parseArgs → CommandPublisher → publishes a ScanCommand
├── scanner-service/      # daemon: CommandConsumer → ScanManager → FileScanner / JobProducer / EventProducer
├── generator-service/    # daemon: VideoJobConsumer → ThumbnailGenerator → ThumbnailStore + ResultRepository (videoDb)
└── sync-service/         # daemon: ThumbnailReadyConsumer → SyncTarget (replicate) → VideoDb (mark synced)
```

- **domain** — the two abstract dependencies from the task (`IFileSystem`,
  `startThumbnailProcess`/`IProcess`) plus a structured `ILogger`, as interfaces only. Fakes live
  here too and are reused across the services' tests.
- **contracts** — the data shapes shared between services: `VideoJob` (the work message),
  `ThumbnailResult` (the per-video outcome record), `ScanCommand` / `ScanEvent` (control plane), and
  the topic-name constants. Command names live in one place (`SCAN_COMMAND` constants + `scanCommands`
  builders) so no service hand-writes a magic string. Validated with zod, so a malformed message is
  rejected, not silently mis-processed.
- **core** — cross-cutting infrastructure as DI providers, registered in one `createService()` call.
  Each service then registers only its own domain classes; everything (logger, config, kafka client,
  lifecycle/shutdown) arrives via constructor injection. The interesting domain logic — commit
  semantics, timeout/kill/retry, the scan queue — stays visible in `apps/*`, not hidden in `core`.
- **orchestrator-service** — a one-shot CLI and control plane. `TenantResolver` maps `--tenant <id>`
  to a scan root (from `tenants.json`), `parseArgs` turns the sub-command into a typed `ScanCommand`,
  and `CommandPublisher` sends it. No daemon, no business logic — just the control surface.
- **scanner-service** — a daemon. `CommandConsumer` routes commands to `ScanManager`, which runs scans
  one at a time (FIFO queue) and is cancellable; `FileScanner` does the iterative walk, `JobProducer`
  emits `VideoJob`s, `EventProducer` emits lifecycle events.
- **generator-service** — a daemon. `VideoJobConsumer` consumes work + control commands;
  `ThumbnailGenerator` owns the resilient ffmpeg run; `ThumbnailStore` writes the image and
  `ResultRepository` writes the `videoDb.jsonl` row; on success it emits a `ThumbnailReady` event.
- **sync-service** — a daemon. `ThumbnailReadyConsumer` consumes `thumbnail-ready`; `SyncTarget`
  replicates the thumbnail (log-only here, rsync/S3 in production); `VideoDb` flips the row's `synced`
  flag. Both `SyncTarget` and `VideoDb` are interfaces, so the destination and the store are swappable.

---

## The video DB (`videoDb.jsonl` — the production DB equivalent)

There's one shared store, the **video DB**, holding one row per video — its metadata, the terminal
generation `status`, and whether its thumbnail has been **synced**. It lives at the repo root
(`videoDb.jsonl`, beside `tenants.json`) so two services share it: the **generator** INSERTs rows, the
**sync-service** UPDATEs the `synced` flag.

```json
{"scanId":"s-1","tenantId":"tenant-1","videoPath":"/data/clip.mov","outputPath":"/out/tenant-1/data/clip.mov.jpg","size":3422636,"discoveredAt":1781208138506,"format":"jpg","status":"generated","attempts":1,"processedAt":1781208139348,"synced":true,"syncedAt":1781208140002}
{"scanId":"s-1","tenantId":"tenant-1","videoPath":"/data/bad.mp4","outputPath":"/out/tenant-1/data/bad.mp4.jpg","size":262,"discoveredAt":1781208138485,"format":"jpg","status":"failed","attempts":3,"error":"exited with code 234","processedAt":1781208139037,"synced":false}
```

Each row carries the video's metadata, the `scanId` + `tenantId` (so rows group back to their scan and
tenant), the terminal `status` (`generated` | `skipped` | `failed`), and the **sync lifecycle**:

- The generator writes every row with `synced: false`.
- For a row whose thumbnail exists (`generated` / `skipped`) it emits a `ThumbnailReady` event.
- The sync-service consumes that event, replicates the thumbnail, and flips the row to
  `synced: true` (stamping `syncedAt`). A `failed` row produced no thumbnail, so it's never synced.

This sits behind two interfaces — **`IResultRepository`** (generator-side INSERT, `save()`) and
**`IVideoDb`** (sync-side UPDATE, `markSynced()`) — over a JSONL implementation. **In production this
is one Postgres/MySQL table**: `save()` is a row `INSERT`, `markSynced()` is
`UPDATE videos SET synced=true WHERE path=?`. Swapping the JSONL classes for DB ones touches neither
consumer nor their tests. JSONL is used here because it gives the same per-row durability without
standing up a database for the assessment.

Thumbnails themselves mirror the source tree under `OUTPUT_ROOT/<tenantId>/`
(e.g. `/data/movies/clip.mov` for `tenant-1` → `OUTPUT_ROOT/tenant-1/data/movies/clip.mov.jpg`) so
neither two videos nor two tenants can ever collide on the same output path.

### Why a separate sync-service?

Replication is its own concern with its own failure modes (network, destination down, slow transfers)
and its own scaling needs (more sync workers when the destination is the bottleneck) — independent of
how fast thumbnails are generated. Putting it behind its own topic means the generator never blocks on
a slow upload, the two scale separately, and other consumers (a search indexer, a CDN warmer) can
react to the same `ThumbnailReady` event without touching the generator. That's the whole point of the
decoupled, event-driven shape: the producer announces a fact and is done.

---

## Requirements

- Node.js >= 20
- pnpm (`npm i -g pnpm`)
- Docker (for Redpanda)
- ffmpeg on `PATH` (`brew install ffmpeg`). Without it, a placeholder file is written instead of a
  real image so the pipeline still runs end-to-end.

## Setup

```bash
pnpm install
cp .env.example .env
```

## Run

There are two steps: **bring the daemons up** (they start idle), then **drive them with the
orchestrator**.

**1. Start infra + both daemons:**

```bash
pnpm up        # infra:up + create topics + run scanner, generator & sync daemons
```

Or step by step:

```bash
pnpm infra:up        # start Redpanda + Console, waits until healthy
pnpm topic:create    # create video-jobs / scan-commands / scan-events / thumbnail-ready topics (idempotent)
pnpm services        # run scanner + generator + sync daemons (no watch)
```

At this point all three daemons are connected and **idle** — the scanner logs `waiting for commands`,
the generator `waiting for jobs`, the sync-service `waiting for ready thumbnails`. Nothing happens
until you tell it to.

**2. Drive the pipeline with the orchestrator** (in another terminal):

```bash
pnpm scan:start --tenant tenant-1          # scan tenant-1's directory
pnpm scan:start --tenant tenant-1 --id s-1 # explicit scanId for correlation
pnpm scan:stop                             # cancel the running scan + clear the queue
pnpm scan:stop --id s-1                    # cancel/drop a specific scan
pnpm scan:stop --tenant tenant-1           # route the stop to tenant-1's owning scanner (scanner pools)
pnpm gen:pause                             # pause thumbnail generation (jobs queue up)
pnpm gen:resume                            # resume generation
```

Every scan is **tenant-scoped**: you pass a `--tenant <id>`, and the orchestrator resolves the
directory from the tenant catalog (see [Tenants](#tenants) below) — raw paths are not accepted, since
the scan root is a tenant configuration detail, not a CLI argument. Issue `scan:start` again for a
different tenant at any time; a scan already running is queued behind the current one (FIFO).

Watch progress in the **Redpanda Console** (http://localhost:8086) or in the daemon logs. Thumbnails
land under `OUTPUT_ROOT` (mirror-tree), every outcome is written as a `videoDb.jsonl` row, and the
sync-service logs `🔄 video synced — …` then flips that row's `synced` flag to `true`.

> **Use `pnpm services` to run the daemons**, not `pnpm dev`. `pnpm dev` runs them under
> `node --watch` for live code reload while developing; for actually running the pipeline use
> `pnpm services` (no watch).

## Tenants

The system is **multi-tenant**: every scan runs for a tenant, and a `tenantId` flows through the whole
pipeline (jobs, events, results) for isolation and tracking. The orchestrator owns the tenant catalog
([tenants.json](./tenants.json)) — it maps a logical tenant id to a scan root:

```json
{
  "tenant-1": { "scanRoot": "~/Downloads" },
  "tenant-2": { "scanRoot": "~/Desktop" }
}
```

`scanRoot` accepts absolute, relative, or `~`-prefixed paths. The catalog is the orchestrator's
control-plane knowledge — the scanner and generator never see the tenant→path mapping, only the
resolved path plus the `tenantId`.

What the `tenantId` buys you:

- **Resolution** — `pnpm scan:start --tenant tenant-1` → orchestrator looks up `~/Downloads` and
  publishes `StartScan { tenantId, scanId, scanRoot }`.
- **Per-tenant output** — thumbnails land under `OUTPUT_ROOT/<tenantId>/…` (mirror-tree inside), so
  tenants never collide on disk.
- **Per-tenant tracking** — every `videoDb.jsonl` row carries the `tenantId`, so rows group back
  to their tenant.

In production the catalog would be a database table; the path it points at would be the real
production `IFileSystem` root, not a local directory. The configurable `TENANTS_FILE` env var points
at the catalog (default: repo-root `tenants.json`).

## Scaling the generator

The scanner is fast (it only reads file-system metadata); the generator is slow (one ffmpeg run per
video, seconds each). So the generator is the bottleneck, and it's where you scale out. Because all
generators join the same Kafka consumer group (`thumbnail-generators`), the broker **partitions the
work across them automatically** — no code change, no internal thread pool. This is why the generator
is intentionally a simple one-job-at-a-time consumer: parallelism is a deployment concern, not a code
concern.

Two things matter:

1. **The `video-jobs` topic needs N partitions.** A partition is the unit of parallelism: each is
   consumed by exactly one generator in the group. With the default 1 partition, only one generator
   ever gets messages (the rest sit idle). For N parallel generators, create the topic with N
   partitions:
   ```bash
   docker exec thumbnailer-redpanda rpk topic create video-jobs -p 4 -X brokers=localhost:9092
   ```
2. **Run the generator process N times.** Same command, more instances — the broker assigns each a
   share of the partitions:
   ```bash
   # in N separate terminals (or backgrounded), from the repo root:
   pnpm generate
   ```

Measured locally: scanning `~/Desktop` produced 175 video jobs. With **one** generator the queue
backed up (lag ~90, generator trailing the scan); with **three** generators sharing 4 partitions the
175 jobs were split (40 / 51 / 84) and the lag stayed at ~0 — the generators kept up with the scan.
Scaling is horizontal and requires no code change.

### How the work is sharded

A `VideoJob` is produced with the **video path as the message key** (`JobProducer` sends
`{ key: job.path, … }`). Kafka picks the partition from the key:

```
partition = murmur2(key) & 0x7fffffff % partitionCount
```

i.e. a hash of the path, modulo the number of partitions. This is consistent hashing / sharding, and
the key choice is deliberate:

- **Even distribution** — paths are diverse, so the hash spreads jobs roughly evenly across
  partitions (and therefore across generators).
- **Idempotency** — the same path always hashes to the same partition → the same generator, so a
  re-scan or redelivery of a video is handled by the consumer that already wrote (or skipped) it.
- **Per-key ordering** — Kafka preserves order within a partition, so all jobs for one path stay ordered.

A poor key would break this — e.g. keying by `tenantId` would send a whole tenant's videos to one
partition (one generator), defeating the balance. The path is the right shard key.

Gotchas:

- **Partition count is fixed up front.** Changing it re-shuffles the key→partition mapping
  (`hash % 4` ≠ `hash % 8`), so pick it for peak generator count and leave it.
- **`rpk topic delete` is async.** To re-create with a different partition count, wait for the topic
  to actually disappear first, or `create` silently no-ops with `TOPIC_ALREADY_EXISTS` and you keep
  the old partition count.

> **In production this is Kubernetes.** The generator becomes a `Deployment` with `replicas: N`
> (or an HPA that auto-scales on Kafka consumer lag), and the topic is provisioned with enough
> partitions to match peak replica count. The local "run it N times" above is just the dev-time
> equivalent of bumping replicas.

### Horizontal scaling — every service

The generator is the obvious bottleneck, but the same broker-backed model lets **every** service scale
out. They don't all scale the *same way*, though — the unit of parallelism differs per service, and
that difference is the interesting part:

| Service | Scales by | Unit of parallelism | How |
|---|---|---|---|
| **generator** | consumer group on `video-jobs` | partition (one generator per partition) | run N instances; N ≤ `video-jobs` partitions (above) |
| **sync** | consumer group on `thumbnail-ready` | partition (one syncer per partition) | identical to the generator — run N instances; N ≤ `thumbnail-ready` partitions |
| **scanner** | consumer group on `scan-commands`, keyed by `tenantId` | a tenant (one walk at a time per owning instance) | run N instances; N ≤ `scan-commands` partitions — the broker assigns each scanner a subset of tenants |
| **orchestrator** | — (stateless) | — | nothing to scale: it publishes one command and exits |

**Generator and sync are the easy case.** Both are plain consumer-group members on a partitioned
topic, so they scale the exact same way: add partitions to the topic, run more instances, the broker
splits the work. To parallelise sync the same way the generator does:

```bash
# give thumbnail-ready N partitions, then run N syncers
docker exec thumbnailer-redpanda rpk topic create thumbnail-ready -p 4 -X brokers=localhost:9092
pnpm sync   # in N terminals / N replicas
```

Because `ThumbnailReady` is keyed by `videoPath` (same shard key as the job), a video's
generate-then-sync work lands on a consistent partition end to end, and re-syncing is idempotent — so
sync can scale and recover with the same guarantees as the generator.

**The scanner is the interesting case** — worth calling out because it shows scaling isn't
one-size-fits. A scan is a single directory walk; one walk doesn't split across partitions the way a
stream of independent jobs does. So you don't scale the scanner by throwing more consumers at *one*
scan — you scale it by **distributing tenants across instances**, and the broker does that for you.

This is **implemented**: the orchestrator keys every `scan-commands` message by `tenantId`
(`CommandPublisher`), and all scanners share one consumer group (`SCANNER_GROUP_ID=scanners`). So when
`scan-commands` has N partitions, the broker assigns each scanner a subset of partitions → a subset of
tenants. `StartScan tenant-1` lands on whichever scanner owns `tenant-1`'s partition; `tenant-2` may
land on a different one — the two tenants scan **concurrently**, on different instances, with zero
duplication (the broker guarantees exactly one owner per partition) and automatic failover (a dead
scanner's tenants get reassigned to the survivors). To scale it out:

```bash
# give scan-commands N partitions, then run N scanners (one consumer group)
docker exec thumbnailer-redpanda rpk topic create scan-commands -p 4 -X brokers=localhost:9092
pnpm scan   # in N terminals / N replicas
```

`StopScan` is keyed by `tenantId` too (`pnpm scan:stop --tenant tenant-1`), so a stop lands on the
**same** scanner that owns — and is running — that tenant's scan. (Without `--tenant`, stop keys by
`scanId`/type and may reach a different instance — fine for the single-scanner default, but use
`--tenant` when running a scanner pool.)

Why partitioning over a "broadcast + local `OWNED_TENANTS` filter" config: the broker gives you
exactly-one-owner, no-gaps, and failover **for free** — the same machinery the generator and sync
already use — so all three services scale identically and no service hand-maintains a tenant→instance
map. (If you ever need to *pin* a specific tenant to specific hardware — e.g. its data is on one mount
— a dedicated scanner pool with its own `SCANNER_GROUP_ID` is the escape hatch; the catalog assigns
roots to pools.)

> **Caveat — `PauseGenerator`/`ResumeGenerator` reach one generator, not all.** They flow on
> `scan-commands` keyed by command `type`, so with several generators in one group only the partition
> owner receives them. That's consumer-group semantics, not a regression from this change. If you need
> pause/resume to fan out to *every* generator, give each generator instance a unique group id (turning
> that topic into pub/sub for them) — a separate concern from scanner sharding.

The throughput *within* a single scan is bounded by disk, not CPU — so the scanner's scaling axis is
**breadth** (more tenants in parallel), while the generator/sync axis is **depth** (more workers on one
stream). The broker makes both work without any service knowing about the others.

> **In production (Kubernetes):** generator and sync are `Deployment`s with HPAs on consumer lag;
> the scanner is a `Deployment` (or a `StatefulSet` / `Job` per tenant tier) sized to the number of
> tenants scanning concurrently, not to one scan's size; the orchestrator is a `CronJob` or an
> on-demand `Job` that fires a command and exits. Each scales on its own signal.

### Sharding by user within a tenant (next level — not yet implemented)

Keying `scan-commands` by `tenantId` evens out the *number of tenants* per scanner, but not the
*amount of work*: if `tenant-A` has 2M users and `tenant-B` has 10k, both still land on one partition
→ one scanner. `tenant-A` becomes a **hot partition** — a single scanner grinds through 200× the work
while the rest idle. Partitioning by tenant balances tenant *count*, but work is what saturates a node.

The fix is the **same hash-mod-partitions machinery, one level finer**: make the shard key
`tenantId:userId` (or a `tenantId:userBucket = hash(userId) % 256` to cap message volume). Now
`tenant-A`'s 2M users hash across *all* partitions → all scanners; no single instance owns a whole giant
tenant. Sharding becomes **hierarchical** — tenant → user → (already) video — and each level is just a
finer shard key on the same Kafka partitioning. Rule of thumb: **shard on whatever is your hot-spot.**
Uneven tenants → drop the key from tenant down to user.

This changes the unit of work from "scan a tenant" to "scan a user" (its `scanRoot` resolved per user
from a `users` catalog), so the fan-out has to come from somewhere. Two ways:

| Approach | How | Trade-off |
|---|---|---|
| **Orchestrator fan-out** | the control plane lists a tenant's users and publishes one `StartScan` per user (or per bucket), keyed by `tenantId:userId` | orchestrator must know every user (a DB query); millions of messages are fine for Kafka but the control plane gets "fat" |
| **Two-stage (recommended)** | `StartScan tenant-A` goes to a *discovery* scanner that lists the tenant's users and republishes per-user `ScanUser` commands back onto the topic (keyed by user) | the fan-out is itself distributed and crash-resilient; the control plane stays thin — it's the same producer/consumer pattern, applied recursively |

The two-stage approach keeps the broker doing the distribution: discovery is a cheap directory-list, and
the heavy per-user scans then shard across the pool with the same exactly-one-owner + failover
guarantees as everything else.

### Batch scanning (production add-on — micro-batching the trigger)

Today a command triggers a scan **immediately** — right for interactive, on-demand scans. But once the
control plane fans out millions of per-user commands (above), one-command-one-scan-now becomes *chatty*:
every tiny scan carries its own `ScanStarted`/`ScanCompleted` events, its own commit, its own overhead —
more coordination than work.

A small logic change fixes it: the scanner **accumulates** commands and flushes them as one batch when
**either** of two thresholds trips first —

- **time**: every N seconds (e.g. 5s), so a rarely-active tenant never waits forever, and
- **count**: once X commands pile up (e.g. 500 users), so a busy tenant doesn't build an unbounded batch.

This is the classic debounce/window pattern (the same idea as a Kafka producer's `linger.ms` +
`batch.size`, or a DB's batched `INSERT`). It's a **trade-off, not a free win**: a bigger batch / longer
window means better throughput (less per-scan overhead) but higher latency before any one scan starts —
so it's a production knob, defaulting to "act immediately" for the interactive case and tuned up for
mass nightly scans. Crucially it **does not change the sharding** (the key is still `tenantId:userId`);
it only changes *when* a scanner acts, not *where* the work lands — so it composes cleanly with
hierarchical sharding above.

## Endpoints

- **Redpanda Console** (topics, messages, consumer-group lag): http://localhost:8086
- **Kafka API**: `localhost:29092`

## Commands

```bash
# infra + daemons
pnpm up              # infra + topics + all daemons
pnpm services        # scanner + generator + sync daemons (no watch — use this to run)
pnpm scan            # scanner daemon only
pnpm generate        # generator daemon only
pnpm sync            # sync daemon only
pnpm dev             # all daemons in watch mode (development only)

# orchestration (control the running daemons)
pnpm scan:start --tenant <id> [--id <id>]   # StartScan (path from tenants.json)
pnpm scan:stop [--tenant <id>] [--id <id>]  # StopScan (--tenant routes to the owning scanner)
pnpm gen:pause                              # PauseGenerator
pnpm gen:resume                             # ResumeGenerator

# tooling
pnpm test            # run all tests
pnpm type-check      # type-check all packages
pnpm infra:up        # start Redpanda + Console
pnpm infra:down      # stop Redpanda
pnpm infra:logs      # follow Redpanda logs
pnpm topic:create    # create the topics
```

If a consumer group ever gets wedged (e.g. after killing daemons hard), reset it:

```bash
docker exec thumbnailer-redpanda rpk group delete scanners thumbnail-generators thumbnail-syncers -X brokers=localhost:9092
```

## Configuration

All config is environment-driven and validated against a zod schema at startup (fails fast with a
readable error). See [.env.example](./.env.example) for the full list. Most-used settings:

| Variable | Service | Description |
|---|---|---|
| `KAFKA_BROKERS` | all | Redpanda broker address (default `localhost:29092`) |
| `TENANTS_FILE` | orchestrator | Path to the tenant catalog (default repo-root `tenants.json`) |
| `SCANNER_GROUP_ID` | scanner | Consumer group for the scanner (must differ from the others') |
| `GENERATOR_GROUP_ID` | generator | Consumer group for the generator |
| `SYNC_GROUP_ID` | sync | Consumer group for the sync-service |
| `VIDEO_EXTENSIONS` | scanner | Comma-separated extensions to treat as video |
| `SCAN_MAX_DEPTH` | scanner | Max directory depth (cheap cycle guard) |
| `OUTPUT_ROOT` | generator | Where thumbnails are written (`OUTPUT_ROOT/<tenantId>/`, mirror-tree) |
| `VIDEO_DB_PATH` | generator, sync | Shared video DB (JSONL) path (default repo-root `videoDb.jsonl`) |
| `THUMBNAIL_FORMAT` | generator | Output image format (`jpg`, `png`, …) |
| `GENERATE_TIMEOUT_MS` | generator | Per-job timeout before the process is killed |
| `GENERATE_MAX_RETRIES` | generator | Retries on failure (total attempts = retries + 1) |
| `GENERATE_FORCE` | generator | Regenerate even if a thumbnail already exists |
| `SYNC_DELAY_MS` | sync | Simulated per-thumbnail transfer latency (ms, default 0) |

## Tests

```bash
pnpm test
```

Unit tests cover:

- **file-system walk** — deep trees, symlink cycles, permission/TOCTOU errors
- **video detection** — extension allowlist, empty/unreadable skips
- **scan orchestration** — `ScanManager` FIFO queue, run-one-at-a-time, cancel mid-scan,
  `scanId`/`tenantId` propagation; command parsing (`parseArgs`); command/event schema validation
- **tenants** — `TenantResolver` (resolve, `~` expansion, unknown-tenant error, catalog load/validate)
- **thumbnail generation** — timeout→kill (no zombie), retry, launch failure, idempotent skip,
  per-tenant output path
- **Kafka semantics** — write-ahead commit / redeliver-on-crash, poison-message handling,
  pause/resume control
- **result recording** — every status written, `scanId` + `tenantId` carried through

Everything runs with in-memory fakes — no real Kafka, ffmpeg, or disk.
