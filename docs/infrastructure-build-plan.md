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

- **Default mode:** `Sources -> Kafka -> S3 -> S3Queue -> ClickHouse`
- **Reason:** better S3/ClickHouse data consistency and simpler replay/backfill
- **Optional fast lane:** `Sources -> Kafka -> ClickHouse` for selected low-latency workloads

Trade-off summary:

- S3-anchored mode: stronger consistency, higher latency
- Parallel fan-out mode: lower latency, requires reconciliation jobs

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

## Phase 2 - S3 raw event lake

Deliverables:

- Kafka-to-S3 sink in Parquet
- Partitioning by tenant/year/month/day
- Lifecycle management and storage class transitions
- Replay tooling from S3 into downstream systems
- Sub-60s sink profile: `rotate.interval.ms=10000-15000`, object size target `8-32 MB`, ready-prefix write pattern

Exit criteria:

- End-to-end event replay works for selected tenant/date slices
- Data quality checks (row counts, schema drift, freshness) active

## Phase 3 - ClickHouse core serving layer

Deliverables:

- S3Queue source tables and ingestion MVs
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

## Phase 4 - Semantic and operational API layer

Deliverables:

- GraphQL endpoint generated from ontology definitions
- Semantic compiler (GraphQL AST -> semantic AST -> SQL)
- Tenant authorization and field-level policies
- Query cost controls and caching for hot paths

Exit criteria:

- Core worker/compliance queries stable for product integration
- API-level audit logs linked to query executions

## Phase 5 - Rule, segment, and workflow activation

Deliverables:

- Rule registry and evaluator service
- Segment engine using composable rules
- Workflow trigger adapter integration
- Idempotent action execution contracts

Exit criteria:

- At least 3 production-like rules functioning end-to-end
- Trigger-to-action latency and success metrics available

## Phase 6 - Agent action plane (controlled rollout)

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

## 7.1 Sub-60s S3Queue configuration profile

Recommended baseline at 30-40 GB/day:

- Kafka -> S3 roll interval: 10-15s
- S3 file size: 8-32 MB
- S3Queue polling: 1-2s
- S3Queue processing threads: 4-8 per shard
- Keep ingest MV logic lightweight

Latency budget target:

- file roll/finalize: 10-20s
- S3Queue discovery: 1-3s
- parse + raw insert: 5-15s
- raw -> state MV: 5-15s
- total p95: 25-50s

## 8) Observability and SLOs

Track:

- Kafka publish success, lag, DLQ rate
- S3 sink success and partition completeness
- ClickHouse insert errors, merge pressure, query latency
- Projection freshness and state staleness
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

5. **Cost growth from retention and ad hoc queries**  
   Mitigation: lifecycle tiering, query governance, aggregate table strategy.

## 10) Immediate execution backlog

1. Finalize canonical event schema and producer SDK contracts
2. Stand up `dev` MSK + S3 sink + sample producer/consumer
3. Implement first S3Queue -> ClickHouse raw + worker_state projection pipeline
4. Expose first GraphQL worker query from semantic API
5. Implement first rules:
   - workers stuck > 48 hours
   - expiring license in 30 days
6. Wire workflow actions and full audit logging

## 11) Definition of done for MVP infrastructure

- Events from ATS + onboarding + verification flowing end-to-end
- Worker state projection available and trusted
- Rule-to-workflow loop actively executing with observability
- Replay from S3 proven in non-production and documented
- Security review and operational runbooks complete

