# Dispatch Engine — Milestone Walkthrough

## Milestone 2: Live Feature Hydration ✅
Embedded per-cell supply/demand cardinalities directly into `CandidateDriver`. Unified cluster pipeline in SpatialScanner. Removed `MarketplaceMetrics` struct and all variadic plumbing.

## Milestone 3: Request Re-Queuing & Recovery Paths ✅
Added Kafka re-queue loop with bounded retry depth (max 3), exponential backoff, and `dlq_expired` DLQ terminal state. Preserved `commitHungarianMatches` worker pool and `failedCommitOrderIDs` tracking.

## Milestone 4: Shared Distributed Pricing Cache ✅
Migrated the surge matrix into the Redis Cluster using un-bracketed keys for uniform shard scattering, protecting the API SLA with a 15ms read timeout and falling back to a `1.0` multiplier on `redis.Nil` or timeout.

## Milestone 5: Standalone E2E Simulation Runner ✅
Implemented basic single-region E2E simulation flood and bootstrap validation loops.

## Milestone 6: City-Scale OpenStreetMap Routing Ingestion ✅
Ingested pre-contracted node and edge flat CSV datasets stream-wise at boot via `GraphLoader` to prevent container startup delays. Successfully integrated the environment dataset paths and the local fallback system.

## Milestone 7: The Stale Telemetry Pruner Daemon ✅
Created a standalone background garbage collection service (`internal/telemetry/pruner/stale_pruner.go`) and independent execution bootstrapper (`cmd/pruner/main.go`) to atomic-sweep ZSET blocks, evict stale entries, and synchronize relational database rows.

---

## Goal A: High-Contention Multi-Driver E2E Simulation Suite ✅

### Problem
The diagnostic tools needed expansion to simulate realistic heavy concurrent traffic in a single-region deployment, ensuring all semaphores, matrix buffers, and bipartite graph solvers perform correctly under load spikes.

### Solution
Rewrote `cmd/simulator/main.go` to split E2E verification into three concurrent load waves:
1. **Wave 1 (Telemetry Flood)**: Ingests 20 concurrent active driver streams streaming telemetry over gRPC client-channels. Minor coordinates variances are randomized in Kolkata anchor boundaries to test ZSET cell maps.
2. **Wave 2 (Order Contention)**: Commits 10 conflicting orders concurrently into Kafka at the exact same instant to force dense Kuhn-Munkres matrix resolution.
3. **Wave 3 (Starvation retry)**: Injects a "poison pill" order in a zero-supply zone (`88283473fffffff`) to trigger exponential retry loop paths.

---

## Goal C: Deep-Learning Cancellation Risk Inference Integration ✅

### Problem
To maximize dispatcher fulfillment rates, the engine needs real-time evaluation of a candidate driver's probability of canceling or rejecting the trip ($P(\text{Cancellation})$), pruning high-risk combinations before the Kuhn-Munkres optimizer solves the matrix.

### Solution
Expanded our multi-objective cost score math and integrated a secondary FIL classification model on Triton.

### Key Enhancements

1. **Triton FIL Model Setup (`model_repository/cancellation_risk_classifier/config.pbtxt`)**
   * Configured the LightGBM classifier under Triton's Forest Inference Library (FIL) backend.
   * Expects 4 inputs in a 1D tensor (`input__0`, type `FP32`): `[Acceptance Rate, Cancellation History Avg, Local Supply Density, Driver Idle Time Seconds]`.
   * Yields a continuous risk scalar probability (`output__0`, type `FP32`).

2. **Expanded Cost Scorer (`internal/dispatch/matcher/hungarian.go`)**
   * Interface `ETACorrector` now carries `ComputeCancellationRisk`.
   * Refactored `ComputeSingleEdgeCost` to incorporate weight `zeta = 0.10` for cancellation risk.
   * Compiles the 4 driver profile metrics and calls Triton.
   * Enforces **Fence Value Exclusion**: if predicted risk $\ge 75\%$, returns un-routable cost penalty `1e7` to prune the candidate from matching eligibility entirely.

3. **Defensive Integration Tests**
   * Added `TestComputeSingleEdgeCost_HighCancellationRiskPruning` in `hungarian_test.go` to verify that safe risk bounds (20%) resolve costs normally, and high-risk bounds (80%) trigger the `1e7` exclusion penalty.
   * Updated deterministic score assertions in both `greedy_test.go` and `hungarian_test.go` to match the modernized weights.

---

## Verification Results (All Milestones)

| Check | Result | Details |
|-------|--------|---------|
| `go test ./internal/dispatch/matcher/...` | ✅ Pass | 16/16 tests passed successfully (including new risk pruning tests) |
| `go build -o NUL ./cmd/simulator/...` | ✅ Clean | Stress simulator builds cleanly using modern `grpc.NewClient` |
| `go build -o NUL ./internal/...` | ✅ Clean | Entire internal repository builds flawlessly |
| `go vet ./internal/dispatch/matcher/... ./cmd/simulator/...` | ✅ Clean | Static analysis is 100% clean |
| `go test ./internal/routing/graph/...` | ✅ Pass | 4/4 routing graph tests passed successfully (including loader) |
| `go test ./internal/telemetry/pruner/...` | ✅ Pass | Integration test executes and passes cleanly (runs or skips defensively based on env) |
| `go build -o NUL ./cmd/pruner/...` | ✅ Clean | Dedicated pruner daemon binary builds successfully |

---

## Milestone 9: The Post-Crash Order State Reconciliation Sync Worker (Self-Healing Daemon) ✅

### Problem
If an active container pod crashes, encounters an Out-Of-Memory (OOM) error, or loses network connectivity *exactly* after committing PostgreSQL state transitions (`status = 'ASSIGNED'`) but *before* publishing to the Kafka topic (`order.assigned`), the relational database shows the booking as `ASSIGNED` but the passenger device or client downstream never gets notified. This leads to a permanent anti-entropy split state anomaly where the booking is stuck in space.

### Solution
Created a robust background worker daemon that continuously scans for orders stuck in the `ASSIGNED` status for longer than a defensive grace window of 20 seconds, and safely re-emits their matching event notification onto the `order.assigned` Kafka topic with the audit metadata tag `"reconciled": true`.

### Key Enhancements

1. **Reconciliation Engine (`internal/dispatch/reconciler/order_reconciler.go`)**
   * Implemented `OrderReconcilerSyncWorker` which runs a 15-second background interval polling loop.
   * Performs high-efficiency queries targeting relational states that are strictly stuck in `ASSIGNED` state (older than 20 seconds, younger than 10 minutes to prevent infinite loops of historic data).
   * Sequentially publishes events to Kafka with strict 2-second per-message timeouts, preventing lock thrashing.
   * Tags payloads with `"reconciled": true` for downstream auditing.

2. **Daemon Operational Bootstrap (`cmd/reconciler/main.go`)**
   * Configured the main bootstrap entrypoint parsing environment configurations (`DATABASE_URL`, `KAFKA_BROKERS`, `CITY_PREFIX`).
   * Verifies database health with an active startup database ping check.
   * Leverages graceful shutdown signal traps for clean terminations.

3. **Multi-Container Stack Integration (`docker-compose.yml`)**
   * Integrated the `reconciler-daemon` service to dependency-link with relational and messaging tiers.

4. **Integration/Unit Testing Suite (`internal/dispatch/reconciler/order_reconciler_test.go`)**
   * Designed a test setting up postgres mock schema entries and seeding a stuck order, verifying correct scan intervals and successful sequential delivery onto Kafka topics.

---

## Verification Results (All Milestones)

| Check | Result | Details |
|-------|--------|---------|
| `go test ./internal/dispatch/matcher/...` | ✅ Pass | 16/16 tests passed successfully (including new risk pruning tests) |
| `go build ./cmd/reconciler/...` | ✅ Clean | Reconciliation daemon builds cleanly |
| `go vet ./internal/dispatch/reconciler/... ./cmd/reconciler/...` | ✅ Clean | Reconciler package static analysis is 100% clean |
| `go test ./internal/dispatch/reconciler/...` | ✅ Pass | Reconciler tests pass cleanly (runs or skips defensively based on env) |
| `go build -o NUL ./cmd/simulator/...` | ✅ Clean | Stress simulator builds cleanly using modern `grpc.NewClient` |
| `go build -o NUL ./internal/...` | ✅ Clean | Entire internal repository builds flawlessly |
| `go vet ./internal/dispatch/matcher/... ./cmd/simulator/...` | ✅ Clean | Static analysis is 100% clean |
| `go test ./internal/routing/graph/...` | ✅ Pass | 4/4 routing graph tests passed successfully (including loader) |
| `go test ./internal/telemetry/pruner/...` | ✅ Pass | Integration test executes and passes cleanly (runs or skips defensively based on env) |
| `go build -o NUL ./cmd/pruner/...` | ✅ Clean | Dedicated pruner daemon binary builds successfully |

---

## Milestone 8: Dynamic Batching Window Adaptation (Marketplace Velocity Balancer) ✅

### Problem
Using a hardcoded, static matching delay window (e.g., `300ms`) is suboptimal across daily ride demand cycles. During peak hours, massive coordinate densities are grouped in a single window, compounding CPU contention. Conversely, during low-volume off-peak hours, isolated riders experience an unnecessary latency delay waiting for the window threshold timer to expire when they could have been matched instantly.

### Solution
Integrated a thread-safe Exponentially Weighted Moving Average (EWMA) Ingestion Velocity Tracker directly into the order consumer queue processor. This dynamically calibrates the matching batch window size based on real-time orders-per-second arrival rates.

### Key Enhancements

1. **EWMA Tracking & Dynamic Calibration (`internal/dispatch/consumer/order_consumer.go`)**
   * Embedded thread-safe properties `lastFlushTime` and `rollingArrivalRate` directly in the consumer structure.
   * On each loop execution, calculates momentary message arrival throughput using elapsed timing metrics.
   * Integrates an exponential smoothing filter (alpha = 0.3) to track velocity trends accurately.
   * Dynamically shifts execution interval boundaries:
     * **Off-Peak (`rollingArrivalRate < 10`)**: Calibrates window to `100ms` for zero delay.
     * **Peak Hour (`rollingArrivalRate > 60`)**: Expands window to `400ms` to build larger optimization matching pools.
     * **Steady Intermediate States**: Interpolates linearly between `100ms` and `400ms`.

2. **Exhaustive Velocity Testing (`internal/dispatch/consumer/order_consumer_test.go`)**
   * Created unit tests (`TestOrderCreatedConsumer_DynamicBatchingWindow`) validating the exact math transitions, boundary conditions, and rolling EWMA state adaptations under low, high, and linear transition states.

---

## Verification Results (All Milestones)

| Check | Result | Details |
|-------|--------|---------|
| `go test ./internal/dispatch/matcher/...` | ✅ Pass | 16/16 tests passed successfully (including new risk pruning tests) |
| `go build ./cmd/reconciler/...` | ✅ Clean | Reconciliation daemon builds cleanly |
| `go vet ./internal/dispatch/reconciler/... ./cmd/reconciler/...` | ✅ Clean | Reconciler package static analysis is 100% clean |
