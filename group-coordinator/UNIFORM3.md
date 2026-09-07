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

# Experimental uniform3 assignor

`uniform3` is an independent incremental design. It never calls `Uniform2Assignor`
or `Uniform2AssignmentGraph`; only the immutable partition-set representation is
shared. Its exact objective hierarchy is total load squares, per-topic load squares,
remote partitions, then changed eligible previous owners.

**Historical comparison:** the measurements and cross-assignor comparisons below
were made when uniform2 used this same objective order. Uniform2 now enforces
per-topic floor/ceiling bounds before optimizing total load. Uniform3 retains its
total-first order, so heterogeneous assignments can now have different objective
values. Current differential topology tests compare the two only for homogeneous
subscriptions; each assignor retains its own exhaustive oracle.

## Enable

```properties
group.consumer.assignors=uniform,range,uniform2,uniform3
group.consumer.uniform3.rack.aware.enable=true
```

Select `group.remote.assignor=uniform3` in consumers using the new protocol.
Defaults remain uniform/range. Rack optimization is opt-in and requires every
member to provide a nonempty rack. The disabled path does not read member or
replica racks and does not allocate rack candidate maps, rack buckets, or a rack
graph. Coordinator member-rack and broker-rack refresh handling covers uniform3.

## Algorithm

1. Read valid previous ownership into primitive arrays. Subscriptions and members
   determine legal destinations; invalid/deleted/unsubscribed ownership is dropped.
2. Derive absolute floor/ceiling balance bounds and necessary movement bounds.
   Direct repairs remove topic excess and remote ownership, place released/new
   partitions, and transfer partitions from donors to eligible recipients.
   Candidate heaps prioritize topic deficits, total load, and member ID.
3. A complete assignment reaching both absolute balance minima, zero known remote
   assignments, and a necessary movement lower bound is already certified optimal.
   No graph is necessary in that case.
4. Otherwise complete a feasible assignment and construct a residual graph seeded
   with the current assignment. With racks disabled, topic/member counts are
   sufficient. With racks enabled, retain/release edges preserve concrete ownership
   through groups of equal replica-rack sets and original owners.
5. Find a lexicographically negative residual cycle using queue-based label
   correction. Augment the entire improving capacity range of that cycle, found
   by binary search on its nondecreasing convex marginal cost. Repeat until no
   improving cycle remains. All four cost coordinates are separate integers;
   there are no weighted approximations or calls to another solver.
6. Materialize partitions deterministically, retaining original partitions first.

The general residual-cycle phase is part of uniform3 itself, not a delegation to
uniform2. It handles transfer chains, rack exchanges, and heterogeneous cases
where absolute floor/ceiling targets are infeasible.

## Why the certificates are sufficient

A complete assignment attaining the global total floor/ceiling bounds and every
topic's subscriber floor/ceiling bounds attains the independent absolute lower
bounds of the first two objectives. Zero known remote partitions is the absolute
lower bound of locality. For movement, count each independent necessity:

- Total excess above the ceiling, or total floor deficits that cannot be filled
  by unowned partitions.
- For each topic, excess above its ceiling, or floor deficits that cannot be
  filled by its unowned partitions; sum these per-topic necessities.
- Eligible old owners that are now remote (when certifying zero remote reads).

The maximum of these bounds is a valid movement lower bound. They must not be
added together because the same move can satisfy several necessities. If direct
repair uses more moves, or cannot attain an absolute balance/locality bound, the
residual-cycle phase resolves the full constrained problem.

For the circulation phase, flow conservation preserves feasibility. Each
augmentation strictly decreases the finite integer lexicographic objective.
For separable convex costs, absence of a negative residual cycle certifies
optimality: any better feasible flow would decompose into residual cycles, at
least one of which must improve the objective. Exact tuple comparison extends
this argument to the four priorities. Equal-cost traversal ties are deterministic.

## Expected tradeoff

Starting with ownership can avoid rebuilding and reoptimizing a large network
for small changes. However, cycle cancellation can need many repairs on difficult
instances; it is not a uniformly faster replacement for cost scaling. Completing
an unfinished greedy seed can also scan all subscribers for each remaining
partition. The implementation therefore needs measurements on constrained and
rack-changing workloads, not only simple joins.

## Comparison method

The server-side JMH benchmark now supports UNIFORM3. For incremental comparisons,
both algorithms receive the same initial assignment generated once by uniform2
in trial setup; setup is outside the timed operation. This is benchmark fixture
construction only: uniform3 itself has no dependency on the uniform2 solver.
Stable topic IDs, identical group metadata, and the same rack settings are used.
The MIXED shape includes 80 singleton topics and 20 larger topics for the
100-topic, 10,000-partition case.

## Validation

The selected coordinator suite passes 778 tests. Both assignors are checked
against 200,000 exhaustive small assignments each. Uniform3 also runs 2,400
changing-topology rounds and compares all four objective coordinates against
uniform2 on the same input, in addition to coverage, subscriptions, uniqueness,
determinism, fixed-point behavior, RF=2 locality, missing-rack gating, and the
512-member transfer chain. Coordinator configuration and rack-refresh integration
tests run for both assignors. Module Checkstyle, Spotless, and the enabled
SpotBugs checks pass on JDK 25.0.1.

The direct-repair certificate is sufficient, not necessary: a candidate can be
optimal without reaching the independent absolute bounds. Such a case is checked
by uniform3's own residual solver. No timeout or approximation is imposed inside
the assignor. External benchmark process budgets are reported as timeouts, not
converted into latency measurements or treated as successful assignments.

## Initial experiment and cycle-detection revision

An initial matrix exposed three cases that exceeded a 45-second process budget.
Mixed-topic heterogeneous incremental assignment measured about 129 ms with racks
disabled and 2,883 ms with racks enabled. The original detector waited for a
predecessor-depth threshold proportional to graph size before extracting a cycle.
The revised implementation probes short predecessor cycles periodically, while
retaining the general detector for long cycles. This preserves the objective and
optimality certificate; every candidate cycle is checked before augmentation.
A thread dump from a separate overlong comparison also located the main thread
in residual label correction. Early detection improves several difficult cases,
but does not remove the general solver's poor-case behavior.

Initial raw results are in `build/uniform3/matrix/`; the final comparison is in
`build/uniform3/final/`. Use the final results below when judging this version.

## Final JMH results

Temurin 21.0.9, one fork, three 500 ms warmup iterations, five 500 ms measurement
iterations, fixed 2 GiB heap, GC profiler, ratio 10 partitions per member.
The algorithm order alternates across paired cases. No builds/tests ran during
timed measurements. RACK_CHANGE keeps membership/subscriptions fixed and rotates
all consumer racks by one AZ after generating the common previous assignment.
Each individual JMH process has an external 20-second budget, including startup,
setup, warmup, and measurement; a timeout is not a 20-second assignment latency.

| Members | Topics | Sizes | Change | Subscriptions | Racks | uniform2 ms | uniform3 ms | uniform2 MiB | uniform3 MiB | Speed ratio (2/3) |
|---:|---:|---|---|---|---|---:|---:|---:|---:|---:|
| 1000 | 100 | mixed | incremental | homogeneous | true | 41.886 | 55.450 | 29.80 | 29.14 | 0.76× |
| 1000 | 100 | mixed | incremental | heterogeneous | true | 3.632 | 10.011 | 7.21 | 6.72 | 0.36× |
| 1000 | 100 | mixed | incremental | heterogeneous | false | 1.402 | 2.967 | 2.64 | 3.18 | 0.47× |
| 1000 | 100 | equal | full | homogeneous | false | 3.081 | 3.308 | 7.29 | 6.31 | 0.93× |
| 1000 | 100 | equal | full | heterogeneous | false | 0.865 | 0.942 | 2.28 | 2.03 | 0.92× |
| 1000 | 100 | equal | full | homogeneous | true | 15.347 | 6.041 | 28.29 | 9.07 | 2.54× |
| 1000 | 100 | equal | full | heterogeneous | true | 3.071 | 1.523 | 6.80 | 2.32 | 2.02× |
| 1000 | 100 | equal | incremental | homogeneous | false | 7.127 | 3.077 | 10.41 | 8.56 | 2.32× |
| 1000 | 100 | equal | incremental | heterogeneous | false | 1.774 | 0.992 | 3.14 | 2.53 | 1.79× |
| 1000 | 100 | equal | incremental | homogeneous | true | 49.199 | 172.415 | 36.88 | 36.52 | 0.29× |
| 1000 | 100 | equal | incremental | heterogeneous | true | 4.539 | 1.430 | 10.08 | 2.76 | 3.17× |
| 1000 | 100 | mixed | full | homogeneous | false | 2.812 | 3.419 | 7.29 | 6.32 | 0.82× |
| 1000 | 100 | mixed | full | heterogeneous | false | 0.746 | 0.983 | 1.96 | 1.78 | 0.76× |
| 1000 | 100 | mixed | full | homogeneous | true | 10.948 | 6.243 | 21.20 | 9.07 | 1.75× |
| 1000 | 100 | mixed | full | heterogeneous | true | 2.115 | timeout | 5.07 | — | — |
| 1000 | 100 | mixed | incremental | homogeneous | false | 6.773 | 3.006 | 10.38 | 8.56 | 2.25× |
| 10000 | 10 | equal | incremental | homogeneous | false | 5.387 | 5.440 | 20.88 | 16.70 | 0.99× |
| 10000 | 10 | equal | incremental | heterogeneous | false | 2.673 | 2.390 | 6.58 | 6.14 | 1.12× |
| 10000 | 10 | equal | incremental | homogeneous | true | 44.243 | 9.394 | 64.67 | 22.33 | 4.71× |
| 10000 | 10 | equal | incremental | heterogeneous | true | 8.760 | 4.453 | 18.45 | 7.53 | 1.97× |
| 1000 | 100 | equal | rack_change | homogeneous | true | 48.752 | 123.091 | 37.14 | 34.77 | 0.40× |
| 1000 | 100 | equal | rack_change | heterogeneous | true | 10.246 | timeout | 10.15 | — | — |
| 1000 | 100 | mixed | rack_change | homogeneous | true | 39.660 | timeout | 30.05 | — | — |
| 1000 | 100 | mixed | rack_change | heterogeneous | true | 6.851 | timeout | 7.44 | — | — |

Across 24 paired workloads, uniform3 has a lower mean in 10, a higher mean in
10, and exceeds the process budget in 4. These counts describe point
estimates, not statistical significance; short measurements have uncertainty,
which is retained in the raw JMH JSON. Uniform2 completes every workload.

The decisive finding is the performance distribution, not the number of wins.
Certified direct repairs are often substantially faster and allocate less, but
constrained cases can be much slower or exceed the budget. The independent cycle
solver is not a suitable general replacement for uniform2 in its current form.
There is no fallback masking these results.

For all 20 workloads where uniform3 completed, a separate untimed check verified
coverage, subscription eligibility, unique ownership, deterministic repetition,
and equality of the complete four-coordinate objective tuple against uniform2.
The four timed-out workloads have no completed large-case correctness result;
small exhaustive and changing-topology tests still cover the algorithm generally.
The per-workload checks and movement counts are in `final/correctness.json`.

The code is on local branch `codex/uniform3`, based on the published uniform2
commit `612237ecb3`. Uniform2's assignor and graph implementations are unchanged.
The configurable built-in registration uses separate instances; the shared
configuration cleanup catch is narrowed to RuntimeException because its checked
reflection exception is already handled inside the loop, satisfying SpotBugs.
