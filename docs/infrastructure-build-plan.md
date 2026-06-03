# Firstwork Operational Intelligence Platform - Infrastructure Build Plan

Status: Execution plan draft  
Audience: Platform engineering, data engineering, SRE, security, product engineering

## 1) Build goals

- Stand up reliable multi-tenant event and analytics infrastructure
- Provide low-latency operational state serving
- Enable rules, segments, workflows, and agent actions on top of trusted state
- Keep full historical replay capability for audit and AI

## 2) Target cloud footprint (AWS)

## 2.1 Core services

- **Event backbone:** AWS MSK (Kafka)
- **Event archive:** S3 (Parquet)
- **Operational analytics store:** ClickHouse (Cloud preferred; self-managed fallback)
- **Metadata registry:** DynamoDB
- **Semantic/operational API:** ECS/Fargate or EKS services
- **Workflow/agent orchestration:** Existing workflow systems + orchestration services
- **Observability:** CloudWatch + OpenTelemetry pipeline + centralized logs/metrics/traces
- **Secrets and keys:** AWS Secrets Manager + KMS

### Ingestion mode decision (adopted)

- **Default mode:** `Sources -> Kafka -> (parallel) ClickHouse + S3`
- **Reason:** best balance of low-latency operations, resilience, and replayability
- **Robustness requirement:** reliability control plane with idempotency, checkpoints, reconciliation, and replay
- **Optional compliance mode:** `Sources -> Kafka -> S3 -> ClickHouse` for archive-first workflows

Trade-off summary:

- Parallel mode: lower latency and better decoupling, requires reconciliation controls
- S3-anchored mode: simpler archive-serving parity, higher latency and higher S3 listing sensitivity

## 2.2 Environment topology

Use isolated environments with separate infra stacks:

- `dev` (integration and contract testing)
- `stage` (production-like scale and chaos testing)
- `prod` (live workload)

Recommended account model:

- Separate AWS account per environment (or strict account-level boundaries by OU)

## 3) Infrastructure as Code structure

Use Terraform (or Pulumi) with module boundaries:

```text
infra/
  modules/
    networking/
    msk/
    s3-event-lake/
    clickhouse/
    ingestion-control-plane/
    dynamodb-registry/
    semantic-api/
    observability/
    iam-security/
  envs/
    dev/
    stage/
    prod/
```

Design rules:

- No manual console-only changes for production resources
- Remote state with locking
- CI policy checks and plan approval gates

## 4) Implementation phases

## Phase 0 - Foundation and guardrails

Deliverables:

- VPC, private subnets, security groups, NAT strategy
- IAM baseline (least privilege), KMS keys, secret management
- CI/CD for infrastructure plans and applies
- Shared tagging and cost allocation policies

Exit criteria:

- All core modules deploy in `dev` from CI
- Security baseline checks pass

## Phase 1 - Event backbone

Deliverables:

- MSK cluster with topic standards and ACL strategy
- Schema registry and event contract pipeline
- Producer SDK conventions for all source products
- DLQ topics and consumer retry policy

Exit criteria:

- At least 2 source systems publishing canonical events
- Consumer lag dashboards and alerting active

## Phase 2 - Dual sink ingestion

Deliverables:

- Kafka->ClickHouse ingestion path (Kafka engine or ClickPipes)
- Kafka->S3 sink in Parquet
- S3 partitioning by tenant/year/month/day
- Lifecycle management and storage class transitions

Exit criteria:

- End-to-end events visible in ClickHouse and S3 for same tenant/time windows
- Data quality checks (row counts, schema drift, freshness) active

## Phase 3 - Ingestion reliability control plane

Deliverables:

- Idempotency contract (`event_id`) and de-dup strategy
- Checkpoint store for each ingestion stage (DynamoDB/ClickHouse control tables)
- Reconciliation jobs (`Kafka vs S3 vs ClickHouse`) by tenant/time bucket
- Replay service (Kafka/S3 -> ClickHouse)
- Quarantine and DLQ routing workflows

Exit criteria:

- Automated repair for selected failure scenarios validated
- Replay and reconciliation runbooks executed in stage

## Phase 4 - ClickHouse core serving layer

Deliverables:

- Replicated raw event tables
- Materialized views for state and aggregate projections
- Query users/roles and row-level tenant isolation strategy
- Backup and restore runbooks

ClickHouse design constraints to enforce:

- Follow `schema-pk-plan-before-creation` and `schema-pk-cardinality-order`
- Follow `schema-partition-low-cardinality` and `schema-partition-lifecycle`
- Follow `insert-batch-size` and `insert-mutation-avoid-update`
- Prefer incremental MVs per `query-mv-incremental`

Exit criteria:

- p95 state query under defined threshold on stage-like data volume
- Projection freshness SLA tracked and alertable

## Phase 5 - Semantic and operational API layer

Deliverables:

- GraphQL endpoint generated from ontology definitions
- Semantic compiler (GraphQL AST -> semantic AST -> SQL)
- Tenant authorization and field-level policies
- Query cost controls and caching for hot paths

Exit criteria:

- Core worker/compliance queries stable for product integration
- API-level audit logs linked to query executions

## Phase 6 - Rule, segment, and workflow activation

Deliverables:

- Rule registry and evaluator service
- Segment engine using composable rules
- Workflow trigger adapter integration
- Idempotent action execution contracts

Exit criteria:

- At least 3 production-like rules functioning end-to-end
- Trigger-to-action latency and success metrics available

## Phase 7 - Agent action plane (controlled rollout)

Deliverables:

- Agent registry, capabilities map, policy gates
- Action sandbox and dry-run mode
- Human-in-the-loop escalation path for sensitive actions

Exit criteria:

- Full action traceability (intent -> decision -> action -> outcome)
- Safety controls validated with staged fault tests

## 5) ClickHouse deployment decision framework

## Option A: ClickHouse Cloud (default)

Use when:

- Team wants lower operational burden
- Fast time-to-value is a priority
- Managed scaling/backups are preferred

Trade-offs:

- Less low-level control
- Vendor-managed operational model

## Option B: Self-managed ClickHouse

Use when:

- Strict hosting constraints require in-account control
- Team has mature SRE bandwidth for ClickHouse operations

Trade-offs:

- Higher operational complexity
- Need to own upgrades, backups, and cluster tuning

Recommendation:

- Start with Option A unless a compliance or platform requirement blocks it.

## 6) Capacity and performance planning baseline

Plan with explicit scale tests around:

- Ingestion throughput (events/sec and MB/sec)
- Projection lag under burst traffic
- Query concurrency for operational APIs
- Backfill throughput from S3

Initial operational guardrails:

- Backpressure and rate limits at producer and consumer tiers
- Strict query timeouts and limits in semantic API
- Separate compute pools for ingestion/projection vs user queries (if required by load)

## 7) Security, compliance, and governance checklist

- Encryption at rest and in transit for all data planes
- Mandatory tenant scoping in every query path
- Immutable audit trail for workflow and agent decisions
- PII classification and retention policies mapped to legal requirements
- Break-glass operational access with just-in-time approvals

## 7.1 Reliability configuration profile (parallel + robust DLQ)

Recommended baseline at 30-40 GB/day:

- Producer settings: `acks=all`, idempotent producers enabled
- Partitioning key: `tenant_id + entity_id`
- DLQ classes: validation errors, parse errors, transformation errors, sink errors
- Checkpoint commit interval: 5-15s
- Reconciliation cadence: every 5-15 minutes by tenant/time window
- Replay trigger: automatic on mismatch thresholds and manual on demand

Repair and correctness profile:

- Idempotent replays by immutable `event_id`
- Quarantine prefixes/topics for poison payloads
- Retry with capped exponential backoff for transient failures

## 8) Observability and SLOs

Track:

- Kafka publish success, lag, DLQ rate
- S3 sink success and partition completeness
- ClickHouse insert errors, merge pressure, query latency
- Projection freshness and state staleness
- Reconciliation mismatch rate and replay queue depth
- Rule evaluation latency and action execution success

Suggested SLOs:

- Event ingestion availability: 99.9%+
- State freshness p95: under 60s
- Operational API p95: under 1s for top workflows
- Workflow trigger success: 99%+

## 9) Risk register and mitigations

1. **Schema drift across source systems**  
   Mitigation: schema registry compatibility gates + contract CI checks.

2. **Hot tenants causing noisy-neighbor effects**  
   Mitigation: tenant-aware partitioning, quotas, and workload isolation strategy.

3. **Late/out-of-order events corrupting state**  
   Mitigation: versioned state model + deterministic projection logic.

4. **Automation safety incidents**  
   Mitigation: policy gates, dry-run mode, human approval for high-impact actions.

5. **Data divergence across sinks in parallel mode**  
   Mitigation: scheduled reconciliation, mismatch alerts, and auto replay/repair.

6. **Cost growth from retention and ad hoc queries**  
   Mitigation: lifecycle tiering, query governance, aggregate table strategy.

## 10) Immediate execution backlog

1. Finalize canonical event schema and producer SDK contracts
2. Stand up `dev` MSK + dual sinks (ClickHouse and S3)
3. Implement first reliability controls (checkpoints, reconciliation, replay)
4. Implement first ClickHouse raw + worker_state projection pipeline
5. Expose first GraphQL worker query from semantic API
6. Implement first rules:
   - workers stuck > 48 hours
   - expiring license in 30 days
7. Wire workflow actions and full audit logging

## 11) Definition of done for MVP infrastructure

- Events from ATS + onboarding + verification flowing end-to-end
- Worker state projection available and trusted
- Rule-to-workflow loop actively executing with observability
- Replay from Kafka/S3 proven in non-production and documented
- Reconciliation and mismatch-repair cycle validated
- Security review and operational runbooks complete
