<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Experimental uniform2 assignor

`uniform2` is separate from `uniform`; the latter's implementation and the default
assignor list are unchanged. This branch starts at upstream/trunk `5ce7e34250`.

## Enable

Broker configuration:

```properties
group.consumer.assignors=uniform,range,uniform2
group.consumer.uniform2.rack.aware.enable=true
```

Consumer configuration:

```properties
group.protocol=consumer
group.remote.assignor=uniform2
client.rack=az-a
```

Rack awareness defaults to false. Every member must have a nonempty rack before
locality is used. The disabled path does not read member racks or replica racks,
create rack groups, build the locality graph, or execute locality optimization.
These guarantees concern the assignor; existing coordinator metadata hashing is unchanged.
The incomplete-member-rack path does not read replica racks. Unknown replica
racks impose no locality preference. Replica racks are alternatives: RF=2 in
three AZs does not require an intersection shared by every partition.

The broker configuration and new selectable assignor are experimental public
configuration changes; an upstream proposal should resolve the KIP requirement
before submission. No wire protocol or persisted record schema is added.

## Objectives and conflicts

The algorithm minimizes the following tuple lexicographically:

1. Sum of squared total partition counts over members.
2. Sum of squared partition counts over all topic/member subscriptions.
3. Number of partitions assigned outside their replica-rack sets (when enabled).
4. Number of previously assigned partitions changing owner.

Coverage, unique ownership, and subscription eligibility are hard constraints.
Only topics subscribed to by at least one current member are assigned. Missing
subscribed topic metadata raises `PartitionAssignorException`.

Balance is measured among eligible members. For homogeneous subscriptions, both
total counts and each topic's counts differ by at most one. For heterogeneous
subscriptions, these objectives can conflict. For example, if member A can read
X and Y, member B can only read Y, and both topics have ten partitions, total
balance requires A to own X and B to own Y. Even Y distribution would instead
produce total loads of 15 and 5. The stated priority chooses total balance.
Similarly, unavoidable remote reads do not justify violating balance.

Minimum movement is exact *among assignments with optimal preceding objectives*.
An unchanged already optimal assignment is retained. Rebalancing an old
concentrated assignment or reacting to changed rack metadata can necessarily
move partitions even when member IDs have not changed. New partitions and
partitions whose owners left have no avoidable movement cost.

## Representation and algorithm

Members and topics are ordered deterministically. The same implementation handles
both subscription types; it does not branch on `GroupSpec.subscriptionType()`.

The count graph has topic supply nodes, member nodes, and a sink. Each subscription
has a convex edge whose flow is that member's partition count for that topic.
Member-to-sink edges express total balance. The final cost on a subscription edge
is zero up to the number of eligible previously owned partitions, and one above
it. This compactly expresses maximum retention without partition/member edges.

Two cheap certificates avoid solving balance unnecessarily:

- Reuse a complete previous assignment if it already attains both absolute balance
  lower bounds and, when enabled, has no known remote partitions.
- Seed floor quotas for every topic and distribute remainder quotas by total load.
  If the resulting total counts also attain their absolute lower bound, retain
  the full floor/ceiling ranges as the optimal face. Otherwise optimize the
  actual constrained balance objectives.

The general balance solver uses integral convex cost scaling. Marginal costs for
squared loads are `2 * count + 1`; an edge represents an entire count range.
Scaling costs by `|V| + 1` and finishing at epsilon one rules out negative-cost
residual cycles. Exact residual shortest-path potentials then identify the
zero-cost residual capacities. Restricting subsequent stages to those capacities
preserves the whole optimal face, rather than freezing a single arbitrary set
of counts. Separate stages avoid large weighted objectives and precision loss.

When enabled, the locality graph groups each topic's partitions by replica-rack
set and original owner. Each owner bucket can retain partitions directly or
release them to the rack-set hub. The hub can route them to any subscriber;
nonlocal destinations cost one. Topic/member and member/sink count ranges are
copied from the compact balance graph. A blocking-flow feasibility pass first
tries using only local edges. If that fails, cost scaling finds the minimum
unavoidable remote count and preserves its optimal face.

When racks are disabled and all topic counts are fixed by the balance bounds,
materialization alone achieves maximum retention and the movement stage is skipped.
Otherwise the final movement stage uses primal-dual shortest paths and blocking flows.
It routes free retained capacity before paid transfers, including transfer chains
and exchanges needed under heterogeneous subscriptions. Every augmented path
has zero reduced cost and feasible potentials certify minimum cost at completion.
Traversals use explicit arrays rather than recursion, avoiding stack overflow
on long transfer chains. Materialization retains concrete old partitions first,
then deterministically fills the remaining counts. Input assignments are not
mutated; unchanged member assignments are reused.

Count-graph space is O(partitions + subscriptions + members). With racks it also
includes original-owner buckets and subscriber edges per distinct replica-rack
set. In a three-AZ RF=2 cluster there are at most three such nonempty sets per
topic. Arbitrarily many distinct replica-rack sets can increase this substantially;
this is not a constant-time or worst-case linear-time optimizer.

## Coordinator triggers

Existing topic metadata hashes include replica racks and already trigger refresh
on partition metadata updates. The opt-in integration additionally:

- bumps the group epoch for a member rack change when the effective assignor is
  rack-aware `uniform2`;
- exposes broker rack changes through `CoordinatorMetadataDelta`, invalidating
  topic hash entries and requesting refresh for rack-aware `uniform2` groups
  whose members all have racks.

This covers broker registration/rack changes even without a topic delta. The
added broker-rack check and group scan are skipped when no configured assignor
has the feature enabled. Built-in `uniform2` instances are constructed per
configuration, avoiding shared mutable configuration between coordinators.

## Correctness verification

`Uniform2AssignorTest` includes a seeded exhaustive oracle, independent of the
flow implementation. It enumerates every eligible assignment for small random
groups and compares the entire objective tuple, including minimum movement.
Each seed also checks input iteration-order independence and fixed-point
stability. Default: 1,000 seeds for each rack mode.

Topology fuzzing repeatedly changes member count, subscriptions, topic counts,
partition counts, member racks, and RF=2 replica-rack sets. It checks coverage,
unique ownership, eligibility, fixed points, and both homogeneous balance bounds.
Default: 100 seeds x 12 changes x two rack modes (2,400 rounds).

Focused cases cover concentrated old assignments, RF=2 realignment, incomplete
rack metadata, missing topics, configuration parsing, and the coordinator's
member-rack and broker-rack refresh triggers.

```bash
./gradlew :group-coordinator:test --tests '*Uniform2AssignorTest' \
  --tests '*GroupCoordinatorConfigTest' --tests '*GroupMetadataManagerTest.testUniform2*'
./gradlew :coordinator-common:test --tests '*KRaftCoordinatorMetadataDeltaTest'
```

For a larger exhaustive run, use a Gradle init script to set the test JVM property:

```groovy
allprojects {
    tasks.withType(Test).configureEach {
        systemProperty 'uniform2.fuzz.trials', '100000'
    }
}
```

Pass it with `./gradlew -I /path/to/fuzz.gradle ...`. The development validation
uses 100,000 seeds per rack mode (200,000 exhaustive cases).

## Initial performance measurements

`ServerSideAssignorBenchmark` accepts `UNIFORM2`. Its rack-aware fixture now
supplies RF=2 replica-rack sets across three racks, rather than just assigning
racks to members. The benchmark returns the assignment so JMH consumes it.

```bash
./gradlew :jmh-benchmarks:shadowJar
java -jar jmh-benchmarks/build/libs/kafka-jmh-benchmarks-*-all.jar \
  ServerSideAssignorBenchmark \
  -p memberCount=1000 -p topicCount=10 -p partitionsToMemberRatio=10 \
  -p isRackAware=false,true -p subscriptionType=HOMOGENEOUS,HETEROGENEOUS \
  -p assignorType=UNIFORM,UNIFORM2 -p assignmentType=FULL,INCREMENTAL \
  -f 1 -wi 3 -i 5 -w 1s -r 1s -prof gc -rf json -rff uniform2-jmh.json
```

Measurements on this macOS host using Temurin 21.0.9, one fork, three 1-second
warmups, five 1-second measurements, 1 GiB fixed heap, and the GC profiler.
Each case has 1,000 members, 10 topics, and 10,000 partitions. Values are mean
milliseconds per assignment; allocations are MiB per assignment.

| Assignment | Subscriptions | Racks | uniform ms | uniform2 ms | uniform2 MiB |
|---|---|---|---:|---:|---:|
| full | homogeneous | false | 0.210 | 0.822 | 4.91 |
| full | homogeneous | true | 0.212 | 3.296 | 13.26 |
| full | heterogeneous | false | 0.612 | 0.360 | 2.01 |
| full | heterogeneous | true | 0.651 | 0.999 | 4.06 |
| incremental | homogeneous | false | 0.096 | 1.585 | 5.11 |
| incremental | homogeneous | true | 0.094 | 6.661 | 20.10 |
| incremental | heterogeneous | false | 0.301 | 0.606 | 1.86 |
| incremental | heterogeneous | true | 0.309 | 1.750 | 5.37 |

The old uniform assignor does not provide the same per-topic/rack guarantees.
The homogeneous incremental case is substantially slower and allocates more
than uniform. Exact optimization has a cost; these results do not claim uniform2
matches the existing homogeneous implementation’s latency or allocation rate.
The heterogeneous JMH fixture uses five subscription buckets. Arbitrary overlapping
subscriptions and many distinct replica-rack sets can be more expensive. These
are local microbenchmarks, not a production latency SLA or a full parameter sweep.

Larger-group measurements use 10,000 members, 10 topics, and 100,000 partitions,
a 2 GiB fixed heap, two 1-second warmups, and three 1-second measurements. These
shorter runs have wider confidence intervals, especially the homogeneous cases.

| Assignment | Subscriptions | Racks | uniform2 ms | uniform2 MiB |
|---|---|---|---:|---:|
| full | homogeneous | false | 13.25 | 50.90 |
| full | homogeneous | true | 55.96 | 137.21 |
| full | heterogeneous | false | 4.57 | 20.70 |
| full | heterogeneous | true | 11.88 | 41.51 |
| incremental | homogeneous | false | 28.50 | 54.72 |
| incremental | homogeneous | true | 107.84 | 203.99 |
| incremental | heterogeneous | false | 8.13 | 18.80 |
| incremental | heterogeneous | true | 25.67 | 54.37 |

Initial implementation results: `build/uniform2/jmh-final.json`. Individual JMH JSON runs are
preserved alongside it, including reruns of the rack-disabled incremental cases
after the fixed-count shortcut. The JSON retains JMH confidence intervals and
allocation statistics.

Final validation used Homebrew OpenJDK 25.0.1: 757 group-coordinator tests and four
coordinator metadata tests passed, including 200,000 exhaustive fuzz cases, 2,400
topology-change rounds, and 512-member transfer-chain tests in both rack modes.
Changed modules passed Spotless and main/test Checkstyle checks. Earlier validation
also passed on Temurin 21.0.9, the runtime used for these benchmarks. This does not
include a full Kafka system/integration test run or the complete JMH parameter grid.

## Primitive storage optimization

The follow-up performance work keeps the same objective hierarchy and solver
algorithms. An allocation profile of the previous implementation identified graph
edge/arc objects, adjacency-list backing arrays, Dijkstra queue entries, and
partition hash sets as the largest allocation sites.

The optimized representation uses:

- Exact-sized parallel primitive arrays for edge endpoints, bounds, flows, costs,
  and supplies. Residual arc IDs encode the edge and direction in one integer.
- CSR adjacency arrays built only when a solver needs them. Arc traversal retains
  insertion order, preserving deterministic tie breaking.
- An indexed primitive heap with at most one entry per node. Dijkstra distances
  and heap storage are reused across shortest-path iterations. Active queues use
  integer ring buffers rather than boxed member IDs.
- Primitive subscriber arrays and counting scatters for replica-rack/original-owner
  groups. These replace boxed partition lists and one map entry/object per owner
  bucket. Replica-rack sets are copied only for a newly encountered rack pattern.
- Sorted primitive partition storage at the API boundary. Contiguous assignments
  use the existing `RangeSet`; noncontiguous assignments use `Uniform2PartitionSet`.
  Each noncontiguous set owns its slice, avoiding retention of an entire old topic
  array when individual members reuse their assignments. Integer boxing remains
  available through the required `Set<Integer>` API.

The exhaustive assignment oracle and topology fuzz tests continue to validate
balance, locality, and exact stickiness. Dedicated randomized tests compare the
indexed heap with `TreeSet`, and partition-set behavior with `HashSet`, including
equality, hashing, iteration, defensive copying, and unsupported mutations.

Paired JMH results for the saved pre-optimization jar and the primitive-storage
implementation are recorded below. Both use Temurin 21.0.9, one fork, three
500 ms warmup iterations, five 500 ms measurement iterations, a 2 GiB fixed heap,
and the GC profiler. These settings differ from the earlier measurements above;
use each paired comparison rather than comparing across benchmark configurations.

All 24 paired cases improved: 1.93–4.71x faster with 63.6–73.1% less allocation.
These are comparisons against the preceding **uniform2** implementation, not the
original uniform assignor. The balance/locality/movement objective hierarchy is unchanged.

### 1,000 members, 10 topics, 10,000 partitions

| Assignment | Subscriptions | Racks | Before ms | After ms | Before MiB | After MiB |
|---|---|---|---:|---:|---:|---:|
| full | homogeneous | false | 0.855 | 0.328 | 4.91 | 1.56 |
| full | homogeneous | true | 3.124 | 1.315 | 13.26 | 3.88 |
| full | heterogeneous | false | 0.338 | 0.171 | 2.04 | 0.64 |
| full | heterogeneous | true | 0.958 | 0.497 | 4.06 | 1.36 |
| incremental | homogeneous | false | 1.358 | 0.587 | 5.11 | 1.81 |
| incremental | homogeneous | true | 6.090 | 2.666 | 20.10 | 6.47 |
| incremental | heterogeneous | false | 0.558 | 0.238 | 1.86 | 0.67 |
| incremental | heterogeneous | true | 1.616 | 0.649 | 5.35 | 1.89 |

### 1,000 members, 100 topics, 10,000 partitions

| Assignment | Subscriptions | Racks | Before ms | After ms | Before MiB | After MiB |
|---|---|---|---:|---:|---:|---:|
| full | homogeneous | false | 8.843 | 2.807 | 25.60 | 7.29 |
| full | homogeneous | true | 41.233 | 14.301 | 105.27 | 28.29 |
| full | heterogeneous | false | 3.837 | 0.859 | 7.60 | 2.28 |
| full | heterogeneous | true | 9.563 | 3.025 | 23.93 | 6.83 |
| incremental | homogeneous | false | 17.542 | 6.695 | 28.50 | 10.38 |
| incremental | homogeneous | true | 98.591 | 48.094 | 121.90 | 36.89 |
| incremental | heterogeneous | false | 4.812 | 1.712 | 8.81 | 3.12 |
| incremental | heterogeneous | true | 11.602 | 4.521 | 31.53 | 10.11 |

### 10,000 members, 10 topics, 100,000 partitions

| Assignment | Subscriptions | Racks | Before ms | After ms | Before MiB | After MiB |
|---|---|---|---:|---:|---:|---:|
| full | homogeneous | false | 10.250 | 3.775 | 50.90 | 16.38 |
| full | homogeneous | true | 47.312 | 17.657 | 137.21 | 39.13 |
| full | heterogeneous | false | 3.641 | 1.637 | 20.70 | 6.28 |
| full | heterogeneous | true | 10.476 | 4.938 | 41.51 | 13.16 |
| incremental | homogeneous | false | 23.147 | 4.917 | 52.21 | 18.21 |
| incremental | homogeneous | true | 93.573 | 46.235 | 203.99 | 67.49 |
| incremental | heterogeneous | false | 6.944 | 2.628 | 18.65 | 6.58 |
| incremental | heterogeneous | true | 23.679 | 8.448 | 53.98 | 18.45 |

Raw paired JMH runs, with confidence intervals and GC metrics:
`build/uniform2/primitive/{before,after}-{1000,10000}.json`.
A compact paired summary is in `build/uniform2/primitive/comparison.json`.

Final primitive-storage validation on JDK 25 passed 761 group-coordinator tests
and four metadata tests. This includes 200,000 exhaustive assignment cases,
2,400 topology-change rounds, the 512-member transfer chains, 100,000 randomized
heap operations, and 1,000 randomized partition-set contract cases. Spotless and
main/test Checkstyle checks passed for the affected modules.

The measurements cover homogeneous and bucketed heterogeneous subscriptions.
They do not establish worst-case latency for arbitrary subscription graphs or
replica-rack patterns, nor replace a full Kafka system-test run. The 100-topic
rack-aware homogeneous incremental case still takes about 48 ms; further gains
there would require reducing solver work as well as storage costs.


## Singleton remainder optimization and mixed topic sizes

The balance seed now selects the lowest-load subscriber with a linear scan when
only one remainder partition must be placed. This includes single-partition
topics. The scan retains the heap's ascending subscriber-index tie break and
therefore leaves the seeded counts, objective hierarchy, and stickiness unchanged.
For larger remainders the indexed heap remains in use.

A bulk heap-construction experiment was also measured, but removed after a
regression in the equal-size homogeneous full-assignment case. A better asymptotic
heap construction bound did not reliably improve this workload: repeated inserts
can already be cheap when subscriber loads are ordered.

The existing server-side JMH benchmark now accepts `topicSizeDistribution=MIXED`
(default `EQUAL`). The mixed fixture interleaves four singleton topics with each
larger topic and distributes the remaining partition budget using repeating
weights 1, 2, 3, 4. With 1,000 members, 100 topics, and ratio 10, there are 80
singleton topics and five topics each of size 199, 397, 595, and 793: exactly
10,000 partitions. Both metadata partition counts and the assignor's describer
reflect these sizes. Stable topic IDs allow reproducible paired runs.

Validation after the isolated scan change: 761 selected group-coordinator tests
passed, including 200,000 exhaustive oracle comparisons, 2,400 topology-change
rounds, and both 512-member transfer-chain cases. Formatting and Checkstyle passed
for the changed modules. The rack gate and exact movement solver are unchanged.

Paired measurements below use JDK 21.0.9, one fork, three 500 ms warmup
iterations, five 500 ms measurement iterations, a fixed 2 GiB heap, and the GC
profiler. Raw results are in `build/uniform2/next/before.json` and `scan.json`;
`after.json` records the discarded bulk-heap experiment. All cases use 1,000
members, 100 topics, and 10,000 partitions.

| Assignment | Subscriptions | Racks | Topic sizes | Before ms | Scan ms | Time reduction |
|---|---|---|---|---:|---:|---:|
| full | homogeneous | false | equal | 2.957 | 3.038 | -2.7% |
| full | homogeneous | false | mixed | 2.943 | 2.830 | +3.9% |
| full | heterogeneous | false | equal | 0.881 | 0.886 | -0.6% |
| full | heterogeneous | false | mixed | 0.745 | 0.748 | -0.4% |
| full | homogeneous | true | equal | 14.262 | 14.626 | -2.6% |
| full | homogeneous | true | mixed | 12.003 | 11.134 | +7.2% |
| full | heterogeneous | true | equal | 3.175 | 3.210 | -1.1% |
| full | heterogeneous | true | mixed | 2.212 | 2.176 | +1.6% |
| incremental | homogeneous | false | equal | 6.906 | 6.728 | +2.6% |
| incremental | homogeneous | false | mixed | 7.653 | 6.819 | +10.9% |
| incremental | heterogeneous | false | equal | 1.765 | 1.736 | +1.6% |
| incremental | heterogeneous | false | mixed | 1.380 | 1.331 | +3.5% |
| incremental | homogeneous | true | equal | 47.824 | 48.242 | -0.9% |
| incremental | homogeneous | true | mixed | 42.752 | 43.323 | -1.3% |
| incremental | heterogeneous | true | equal | 4.523 | 4.568 | -1.0% |
| incremental | heterogeneous | true | mixed | 3.695 | 3.535 | +4.3% |

The short runs above are exploratory: most confidence intervals overlap, and
allocation is essentially unchanged. A longer confirmation of the apparent
mixed-topic homogeneous incremental improvement (racks disabled), using two
forks and five one-second warmup/measurement iterations each, measured
6.838 ± 0.090 ms before and 6.572 ± 0.132 ms with the scan
(JMH reported error). Use this confirmation rather than the short-run percentage
for that workload. Raw results are `confirm-before.json` and `confirm-scan.json`.
