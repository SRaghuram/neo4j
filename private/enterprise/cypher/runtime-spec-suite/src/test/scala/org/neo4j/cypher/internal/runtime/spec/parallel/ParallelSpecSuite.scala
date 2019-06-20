/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.MorselRuntime.{MORSEL, PARALLEL}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.runtime.spec.morsel.{ParallelInputTestBase, SchedulerTracerTestBase}
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._

object ParallelRuntimeSpecSuite {
  val SIZE_HINT = 1000
}

// INPUT
class ParallelRuntimeInputTest extends ParallelInputTestBase(PARALLEL)

// ALL NODE SCAN
class ParallelRuntimeAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeAllNodeScanNoFusingTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeAllNodeScanStressTest extends AllNodeScanStressTestBase(PARALLEL)

// NODE BY ID SEEK
class ParallelRuntimeNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeByIdSeekStressTest extends NodeByIdSeekStressTestBase(PARALLEL)

// NODE COUNT FROM COUNT STORE
class ParallelNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

// RELATIONSHIP COUNT FROM COUNT STORE
class ParallelRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

// LABEL SCAN
class ParallelRuntimeLabelScanTest extends LabelScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeLabelScanNoFusingTest extends LabelScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeLabelScanStressTest extends LabelScanStressTestBase(PARALLEL)

// INDEX SEEK
class ParallelRuntimeNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]

class ParallelRuntimeIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(PARALLEL)
class ParallelRuntimeIndexSeekExactStressTest extends IndexSeekExactStressTest(PARALLEL)

// INDEX SCAN
class ParallelRuntimeNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexScanStressTest extends IndexScanStressTestBase(PARALLEL)

// INDEX CONTAINS SCAN
class ParallelRuntimeNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexContainsScanStressTest extends IndexContainsScanStressTestBase(PARALLEL)

// ARGUMENT
class ParallelRuntimeArgumentTest extends ArgumentTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeArgumentNoFusingTest extends ArgumentTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeArgumentStressTest extends ArgumentStressTestBase(PARALLEL)

// APPLY
class ParallelRuntimeApplyStressTest extends ApplyStressTestBase(PARALLEL)

// EXPAND
class ParallelRuntimeExpandAllTest extends ExpandAllTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeExpandAllTestNoFusing extends ExpandAllTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeExpandStressTest extends ExpandStressTestBase(PARALLEL)

// PROJECTION
class ParallelRuntimeProjectionTest extends ProjectionTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeProjectionNoFusingTest extends ProjectionTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProjectionStressTest extends ProjectionStressTestBase(PARALLEL)

// FILTER
class ParallelRuntimeFilterTest extends FilterTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeFilterNoFusingTest extends FilterTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeFilterStressTest extends FilterStressTestBase(PARALLEL)

// LIMIT
class ParallelRuntimeLimitTest extends LimitTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeLimitNoFusingTest extends LimitTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

// UNWIND
class ParallelRuntimeUnwindTest extends UnwindTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeUnwindNoFusingTest extends UnwindTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeUnwindStressTest extends UnwindStressTestBase(PARALLEL)

// SORT
class ParallelRuntimeSortTest extends SortTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)

// AGGREGATION
class ParallelRuntimeParallelAggregationTest extends AggregationTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeAggregationStressTest extends AggregationStressTestBase(PARALLEL)

// NODE HASH JOIN
class ParallelRuntimeNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)

// REACTIVE
class ParallelRuntimeReactiveParallelTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeReactiveParallelNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)
class ParallelRuntimeReactiveParallelStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.PARALLEL, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size
class ParallelRuntimeReactiveParallelNoFusingStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size

// GENERAL
class ParallelRuntimeMiscTest extends MiscTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeNoFusingMiscTest extends MiscTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)
class ParallelRuntimeSchedulerTracerTest extends SchedulerTracerTestBase(PARALLEL)

// WORKLOAD
class ParallelRuntimeWorkloadTest extends WorkloadTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNoFusingWorkloadTest extends WorkloadTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

// PROFILE
class ParallelRuntimeProfileNoFusingRowsTest extends ProfileRowsTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileNoFusingTimeTest extends ProfileTimeTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

