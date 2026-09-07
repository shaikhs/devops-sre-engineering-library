# Apache Kafka Quick Guide

## Fastest Path from Beginner to Production Understanding

This guide condenses the concepts in *Learning Apache Kafka, 2nd Edition* into a practical reference. The book is from the Kafka 0.8.x/ZooKeeper era, so the fundamentals remain useful but many commands and operational details must be mapped to the Kafka version currently deployed.

> **Modernization warning:** The book uses ZooKeeper, older producer/consumer APIs, and older command-line syntax. Modern Kafka may use KRaft, current Java clients, Confluent Platform components, Kafka Connect, Schema Registry, ksqlDB, and different configuration names. Always verify commands against the deployed version.

---

# 1. Kafka in One Minute

Kafka is a distributed, durable, partitioned event log.

```text
Producer → Topic → Partition → Broker log → Consumer group → Downstream system
```

Kafka is useful when:

- Many producers generate high-volume events.
- Multiple independent consumers need the same data.
- Consumers must process at different speeds.
- Events must be retained and replayed.
- Systems need decoupling instead of point-to-point integrations.
- Real-time and batch consumers need the same source stream.

Typical use cases:

- Log aggregation
- Clickstream tracking
- Metrics and activity streams
- Fraud and security-event detection
- Stream processing
- Event-driven integration
- Data-lake and warehouse ingestion
- Replicated commit logs
- Real-time recommendations and alerts

Kafka is not simply a faster queue. It is a retained, ordered, replayable log with scalable consumers.

---

# 2. The Core Objects

| Object | Meaning | Key operational point |
|---|---|---|
| Producer | Publishes records | Controls key, partitioning, batching, retries, and acknowledgements |
| Topic | Named event stream | Has partitions, retention, security, and ownership |
| Partition | Ordered append-only log | Ordering exists within one partition |
| Offset | Record position | Enables consumer progress and replay |
| Broker | Kafka server | Stores partitions and serves client requests |
| Leader | Active replica | Handles reads and writes for a partition |
| Follower | Replicating replica | Copies the leader’s log |
| ISR | In-sync replicas | Eligible for safe failover |
| Consumer | Reads records | Pulls data and processes it |
| Consumer group | Cooperative consumers | Each partition is assigned to one group member |
| Controller/metadata quorum | Cluster coordination | KRaft is modern; ZooKeeper is historical in this book |

## The ordering rule

Kafka guarantees order **within a partition**, not across a topic. Use a stable key when related records must remain ordered.

Examples:

- `account_id` for account events
- `payment_id` for payment lifecycle events
- `customer_id` for customer-specific ordering
- `order_id` for order state transitions

The key must also distribute load. A single extremely active key can create a hot partition.

## The replay rule

Kafka does not delete a record when one consumer reads it. Records remain according to retention or compaction policy. Different consumer groups can read the same topic independently and can replay data while the offset is still retained.

---

# 3. How Kafka Stores Data

Each partition is an immutable ordered log made of segment files.

```text
Topic: payments
├── Partition 0: offset 0 → 1 → 2 → 3 → ...
├── Partition 1: offset 0 → 1 → 2 → 3 → ...
└── Partition 2: offset 0 → 1 → 2 → 3 → ...
```

Records are appended to the active segment. Offsets are sequential within each partition. Kafka uses sequential I/O, filesystem caching, batching, and retention to support high throughput.

## Retention modes

### Time-based retention

Delete records older than the configured time.

### Size-based retention

Delete older segments when a topic exceeds its storage limit.

### Log compaction

Keep the latest value for each key. This is useful for current-state topics such as account configuration or customer profile state.

Compaction is asynchronous. It does not mean that old records disappear immediately.

## Compression

Compression reduces network and disk usage at the cost of CPU. Select a codec based on measured throughput, latency, CPU, and compatibility requirements. Compression is most effective when producers batch records.

---

# 4. Cluster Architecture

A Kafka cluster can be:

1. One node with one broker — development only.
2. One node with multiple brokers — learning/testing only.
3. Multiple nodes with multiple brokers — production-style topology.

A production cluster should distribute brokers across independent failure domains.

## Broker responsibilities

- Accept producer writes
- Serve consumer fetches
- Store partition segments
- Replicate follower data
- Participate in leader elections
- Expose metrics and administrative APIs
- Enforce authentication and authorization

## Replication model

For each partition:

```text
Leader ← Followers
```

The leader handles client traffic. Followers copy the leader. The ISR contains replicas sufficiently caught up to be considered safe failover candidates.

If a leader fails, an eligible ISR member can become the new leader. Replication factor, ISR health, acknowledgement policy, and failure-domain placement determine actual durability.

## Rack and zone awareness

Replicas should be spread across racks, availability zones, or data centers. A replication factor of three is not enough if all three copies are in the same failure domain.

---

# 5. Topic and Partition Design

Define these before creating a production topic:

- Business owner
- Producer applications
- Consumer groups
- Message key and ordering requirement
- Expected records/sec and bytes/sec
- Partition count
- Replication factor
- Retention policy
- Compaction policy
- Schema and compatibility mode
- Security classification
- ACL/RBAC policy
- Retry/dead-letter behavior
- DR replication policy
- RPO/RTO

## Partition count

Partitions provide:

- Producer parallelism
- Consumer parallelism
- Distribution across brokers
- Ordering boundaries

Too few partitions limit throughput and consumer concurrency. Too many partitions increase metadata, open files, leader-election work, recovery time, and rebalance complexity.

Choose partitions from measured producer/consumer capacity, not a random large number.

## Partition calculation

Start with:

```text
Required partitions ≈ target throughput ÷ measured throughput per partition
```

Then validate with:

- Peak traffic
- Burst factor
- Consumer processing speed
- Number of consumer groups
- Recovery time
- Key skew
- Broker and disk limits
- Growth forecast

Changing partition count can affect key-to-partition mapping and ordering expectations. Treat it as a design change, not a routine resize.

---

# 6. Setting Up a Learning Cluster

The book demonstrates Java installation, Kafka download, ZooKeeper startup, broker startup, topic creation, console production, and console consumption.

Modern learning sequence:

1. Choose a supported Kafka distribution/version.
2. Use a local KRaft development mode or an approved container image.
3. Start a single-node development broker.
4. Create a topic with explicit partitions and replication.
5. Produce messages.
6. Consume from the beginning.
7. Create a second consumer group.
8. Stop/restart the broker.
9. Observe offsets, replay, and recovery.
10. Repeat with multiple brokers in a lab.

Typical current-style commands vary by distribution, but the workflow is conceptually:

```bash
# Create a topic; exact flags vary by Kafka version
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic payments \
  --partitions 3 --replication-factor 1

# Describe the topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic payments

# Produce records
kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic payments

# Consume from the beginning
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments --from-beginning \
  --group payments-test
```

Do not use a single broker, replication factor one, or temporary directories as a production design.

---

# 7. Producer Quick Guide

A producer:

1. Connects to a bootstrap broker.
2. Requests metadata for the topic.
3. Finds the leader for the target partition.
4. Serializes the key and value.
5. Batches and compresses records where configured.
6. Sends records to the partition leader.
7. Receives an acknowledgement or error.

## Producer design decisions

| Decision | Trade-off |
|---|---|
| Stable key | Preserves per-key ordering but can create hot partitions |
| Batching | Improves throughput but may increase latency |
| Compression | Reduces network/disk but uses CPU |
| Retries | Handles transient errors but can create duplicates or retry storms |
| Strong acknowledgements | Improves durability but may increase latency |
| Idempotence | Reduces duplicate writes for supported failure cases |
| Large buffers | Absorb bursts but increase memory pressure |

Important settings to understand:

- `acks`
- `enable.idempotence`
- `retries`
- `delivery.timeout.ms`
- `batch.size`
- `linger.ms`
- `compression.type`
- `max.in.flight.requests.per.connection`
- Serialization configuration
- Client and request timeouts

A network error does not prove that a record was not committed. Design downstream processing for duplicate-safe behavior.

## Producer failure checklist

```text
Check error class → metadata/leader health → TLS/auth/ACL → retries/timeouts
→ broker capacity → network → partition/key behavior → downstream event correctness
```

---

# 8. Consumer Quick Guide

A consumer uses a pull model:

1. Join a consumer group.
2. Receive partition assignments.
3. Fetch records from the partition leader.
4. Process records.
5. Commit offsets according to the chosen strategy.
6. Rejoin/rebalance after failure or membership changes.

## Consumer group rules

- One partition is assigned to at most one active consumer in a group.
- Multiple groups can independently consume the same topic.
- Consumers greater than partitions leave some consumers idle.
- Rebalances occur when group membership or partition assignment changes.
- Lag is the distance between the latest available offset and the committed/processed offset.

## Offset strategies

### Commit before processing

Lower duplicate risk but possible data loss after a crash.

### Process before commit

At-least-once behavior; duplicate processing is possible, so the downstream operation must be idempotent.

### Transactional processing

Requires coordinated transactional design. Do not call a Kafka setting alone “end-to-end exactly once” when the workflow includes an external database or payment switch.

## Consumer settings to understand

- `group.id`
- `enable.auto.commit`
- `auto.offset.reset`
- `max.poll.interval.ms`
- `max.poll.records`
- `session.timeout.ms`
- `heartbeat.interval.ms`
- Fetch sizes and wait times
- Assignment strategy
- Security and deserialization settings

## Consumer lag checklist

```text
Identify group/topic/partition → compare produce and consume rates
→ check rebalances and poll delays → inspect processing latency
→ check downstream dependencies → check broker fetch latency
→ mitigate safely → replay or dead-letter poison records → verify business completion
```

---

# 9. Delivery Semantics

| Semantic | Behavior | Main risk |
|---|---|---|
| At-most-once | Commit before processing | Data may be lost |
| At-least-once | Process before commit | Records may be duplicated |
| Exactly-once | Coordinated transactional flow | Higher complexity and limited scope |

For payment or financial events, use:

- Stable event IDs
- Idempotency keys
- Deduplication
- Outbox or transactional source patterns
- Clear transaction state machines
- Retry topics and dead-letter topics
- Reconciliation against the system of record
- Immutable audit trails

Kafka delivery semantics do not automatically guarantee a single business effect in an external system.

---

# 10. Kafka Integrations

## Storm / stream processing

The book describes Storm as a distributed real-time processing system:

- **Spout:** Produces or reads a stream.
- **Bolt:** Processes and emits streams.
- **Tuple:** Data unit.
- **Topology:** Graph of processing components.
- **Worker/executor:** Runtime execution units.

Kafka integrates with stream processors through a Kafka source/spout. The modern equivalent may be Kafka Streams, ksqlDB, Flink, or Spark Structured Streaming, depending on the use case.

## Hadoop / batch processing

Kafka can feed batch systems and data lakes. A consumer reads from Kafka and writes to HDFS or another storage system. Offsets must be stored so failed jobs can restart from the correct position.

The integration pattern is:

```text
Kafka event stream → batch consumer → HDFS/data lake → MapReduce/analytics
```

The lesson is not to confuse real-time processing with batch processing. Kafka can be the common event source while different consumers apply different latency and processing models.

## Modern integration options

- Kafka Connect for standard source/sink integrations
- Kafka Streams for application-embedded stream processing
- ksqlDB for SQL-based stream processing
- Flink for stateful distributed processing
- Spark Structured Streaming for analytics pipelines
- Schema Registry for governed serialization

---

# 11. Kafka Operations

## Daily operational checks

- Broker availability
- Controller/metadata health
- Offline partitions
- Under-replicated partitions
- ISR shrink/expand rate
- Leader imbalance
- Disk utilization and latency
- Network throughput and errors
- JVM heap and GC pauses
- Produce/fetch request latency
- Producer errors and retries
- Consumer lag
- Rebalance rate
- Connector task failures
- Schema Registry health
- Replication or mirror lag

## Adding brokers

Adding a broker does not automatically move existing partition data. The operational sequence is:

1. Provision and secure the broker.
2. Join it to the cluster.
3. Verify monitoring and capacity.
4. Generate a partition reassignment plan.
5. Review placement and failure domains.
6. Apply reassignment in controlled batches.
7. Throttle recovery if client traffic is affected.
8. Monitor ISR, disk, network, and latency.
9. Confirm balanced replicas and leaders.

## Controlled broker shutdown

Before maintenance:

- Confirm ISR health.
- Move leadership safely where supported.
- Confirm no critical partition loses its safe replica set.
- Drain or stop one failure-domain unit at a time.
- Monitor client errors, lag, and leader movement.

## Partition reassignment

Partition reassignment can generate high disk and network traffic. Use a reviewed plan, controlled throttles, change-window communication, and a rollback/recovery procedure. Do not rebalance the entire cluster blindly during peak payment traffic.

## Preferred leader balancing

Broker failures and maintenance can leave leaders unevenly distributed. Check leader and replica skew, then use version-appropriate balancing tools or controlled reassignment. Balance must not compromise failure-domain placement.

## Mirroring and cross-cluster replication

Define:

- Source and destination clusters
- Topic allowlist
- Direction of replication
- Replication lag objective
- Offset translation behavior
- Schema replication
- Security credentials
- Failover and failback
- Duplicate/replay handling
- Conflict policy

Cluster Linking and MirrorMaker 2 are not interchangeable in every environment. Select based on version, platform support, semantics, and the team’s ability to operate the failover path.

---

# 12. L3 Troubleshooting Matrix

| Symptom | First checks | Likely causes |
|---|---|---|
| Producer timeout | Broker/leader, auth, network, retries | Broker saturation, leader issue, TLS/ACL, network |
| ISR shrink | Replica fetch, disk, network, GC | Slow follower, disk latency, packet loss, pauses |
| Offline partition | Controller, leaders, replicas | Broker/site failure, unavailable ISR |
| Consumer lag | Lag by partition, processing time | Slow consumer, downstream dependency, rebalance, hot partition |
| Duplicate event | Event ID, offset, commit timing | Retry ambiguity, crash before commit, non-idempotent consumer |
| Connector retry loop | Task logs, target system, DLQ | Bad record, schema, target outage, credentials |
| Disk full | Topic growth, retention, replicas | Retention, reassignment, replica duplication, runaway producer |
| High latency | Request, disk, network, downstream | Saturation, GC, WAN, database/payment switch |
| Broker healthy, app failing | End-to-end trace | Schema, consumer, connector, downstream, business logic |

## Root-cause method

1. Establish user and business impact.
2. Freeze risky changes.
3. Capture timeline and evidence.
4. Scope affected topics, partitions, consumers, sites, and workflows.
5. Form multiple hypotheses.
6. Use metrics/logs/tests to eliminate hypotheses.
7. Mitigate safely.
8. Validate the business outcome.
9. Perform RCA and implement permanent controls.

---

# 13. Security Essentials

Use layered security:

- TLS for encryption in transit
- Mutual TLS or SASL/Kerberos for identity
- ACLs or Confluent RBAC for authorization
- Network segmentation and private listeners
- Encryption at rest and key management
- Secret and certificate rotation
- Separate operator, producer, consumer, Connect, and Schema Registry identities
- Audit logs for access and configuration
- PII/payment-data classification
- Restricted break-glass access
- Secure backups and DR credentials

A producer should write only to required topics. A consumer should read only required topics and groups. Operators should not automatically receive payment payload access.

---

# 14. Capacity Planning Cheat Sheet

```text
Ingress bytes/sec = records/sec × average record size
Broker write load ≈ ingress × replication factor
Retention storage ≈ ingress × retention time × replication ÷ compression ratio
Consumer network load ≈ ingress × number of consumer groups
```

Also account for:

- Traffic bursts
- Record-size distribution
- Consumer replay
- Replication catch-up
- Cross-DC bandwidth
- Disk recovery speed
- Growth forecast
- Failure-domain headroom
- Producer and consumer CPU
- Schema/serialization overhead

Measure with a load test. Do not treat formulas as final sizing.

---

# 15. Modern Confluent Platform Map

| Book concept | Current interview translation |
|---|---|
| ZooKeeper coordination | Explain historical behavior, then discuss KRaft/current supported mode |
| Old high-level consumer API | Current consumer group, poll, assignment, rebalance, and offset model |
| `broker-list` | `bootstrap.servers` and current client configuration |
| Manual producer/consumer | Secure, schema-aware, observable application clients |
| Basic replication | ISR, rack awareness, failure domains, RPO/RTO, and recovery testing |
| Storm integration | Kafka Streams, ksqlDB, Flink, or Spark where appropriate |
| Hadoop integration | Kafka Connect, data lake, warehouse, and governed batch pipelines |
| Manual topic tools | Automated, reviewed, version-controlled platform operations |

Never present a 2015 command as current production guidance without checking the deployed version.

---

# 16. 30-Minute Learning Plan

## Minutes 0–5: Mental model

Explain producer, topic, partition, broker, consumer group, offset, leader, follower, and ISR.

## Minutes 5–10: Storage and ordering

Explain append-only logs, retention, compaction, keys, partitions, and ordering boundaries.

## Minutes 10–15: Reliability

Explain replication, ISR, leader failover, acknowledgements, retries, idempotence, and delivery semantics.

## Minutes 15–20: Consumers

Explain consumer groups, assignment, rebalances, commits, lag, replay, and idempotent downstream processing.

## Minutes 20–25: Operations

Explain metrics, under-replicated partitions, lag, disk, reassignment, broker maintenance, and cross-cluster replication.

## Minutes 25–30: Production scenario

Trace one event end to end:

```text
Producer → topic/key → partition leader → replication
→ consumer group → downstream system → offset commit
→ audit/reconciliation → replay or recovery behavior
```

If you can explain that flow under normal operation and failure, you understand Kafka beyond memorized definitions.

---

# Final Checklist

Before calling yourself interview-ready, confirm that you can explain:

- Why Kafka is used and when it is not the right choice
- Topic, partition, offset, broker, leader, follower, ISR, and group
- Ordering and hot-partition trade-offs
- Replication and failure-domain placement
- Producer batching, compression, retries, acknowledgements, and idempotence
- Consumer groups, rebalances, offset commits, replay, and lag
- At-most-once, at-least-once, and exactly-once limitations
- Log retention versus compaction
- Broker failure and under-replication diagnosis
- Partition reassignment and adding brokers
- Kafka Connect, Schema Registry, stream processing, and batch integration
- Cross-cluster replication and DR
- TLS, SASL/Kerberos, ACLs/RBAC, secrets, and audit
- Capacity sizing from TPS, bytes, retention, replicas, and consumers
- Modern KRaft and version-specific command differences
- A complete incident response from customer impact to permanent fix
