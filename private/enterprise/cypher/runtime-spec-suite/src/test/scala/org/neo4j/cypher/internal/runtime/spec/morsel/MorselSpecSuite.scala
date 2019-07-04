/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.MorselRuntime.MORSEL
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE.{SINGLE_THREADED, SINGLE_THREADED_NO_FUSING}
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._

object MorselSpecSuite {
  val SIZE_HINT = 1000
}

// INPUT
class MorselInputTest extends InputTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)

// ALL NODE SCAN
class MorselAllNodeScanTest extends AllNodeScanTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselAllNodeScanNoFusingTest extends AllNodeScanTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// NODE BY ID SEEK
class MorselNodeByIdSeekTest extends NodeByIdSeekTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// RELATIONSHIP BY ID SEEK
class MorselDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// NODE COUNT FROM COUNT STORE
class MorselNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(SINGLE_THREADED, MORSEL)
class MorselNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(SINGLE_THREADED_NO_FUSING, MORSEL)

// RELATIONSHIP COUNT FROM COUNT STORE
class MorselRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(SINGLE_THREADED, MORSEL)
class MorselRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(SINGLE_THREADED_NO_FUSING, MORSEL)

// LABEL SCAN
class MorselLabelScanTest extends LabelScanTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselLabelScanNoFusingTest extends LabelScanTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// INDEX SEEK
class MorselNodeIndexSeekTest extends NodeIndexSeekTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]

class MorselNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]

// INDEX SCAN
class MorselNodeIndexScanTest extends NodeIndexScanTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// INDEX CONTAINS SCAN
class MorselNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// ARGUMENT
class MorselArgumentTest extends ArgumentTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselArgumentNoFusingTest extends ArgumentTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// APPLY
class MorselApplyStressTest extends ApplyStressTestBase(MORSEL)

// EXPAND
class MorselExpandAllTest extends ExpandAllTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselExpandAllTestNoFusing extends ExpandAllTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// PROJECTION
class MorselProjectionTest extends ProjectionTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselProjectionNoFusingTest extends ProjectionTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// FILTER
class MorselFilterTest extends FilterTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselFilterNoFusingTest extends FilterTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// LIMIT
class MorselLimitTest extends LimitTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselLimitNoFusingTest extends LimitTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// DISTINCT
class MorselDistinctTest extends DistinctTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselDistinctNoFusingTest extends DistinctTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// UNWIND
class MorselUnwindTest extends UnwindTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselUnwindNoFusingTest extends UnwindTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// SORT
class MorselSortTest extends SortTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)

// AGGREGATION
class MorselSingleThreadedAggregationTest extends AggregationTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)

// NODE HASH JOIN
class MorselNodeHashJoinTest extends NodeHashJoinTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)

// PROVIDED ORDER
class MorselSingleThreadedProvidedOrderTest extends ProvidedOrderTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselSingleThreadedNoFusingProvidedOrderTest extends ProvidedOrderTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// REACTIVE
class MorselReactiveSingleThreadedTest extends ReactiveResultTestBase(SINGLE_THREADED, MORSEL)
class MorselReactiveSingleThreadedNoFusingTest extends ReactiveResultTestBase(SINGLE_THREADED_NO_FUSING, MORSEL)
class MorselReactiveParallelStressTest
  extends ReactiveResultStressTestBase(SINGLE_THREADED, MORSEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size
class MorselReactiveParallelNoFusingStressTest
  extends ReactiveResultStressTestBase(SINGLE_THREADED_NO_FUSING, MORSEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size

// GENERAL
class MorselSingleThreadedTest extends MiscTestBase(SINGLE_THREADED, MORSEL)
class MorselSingleThreadedNoFusingTest extends MiscTestBase(SINGLE_THREADED_NO_FUSING, MORSEL)
class MorselSchedulerTracerTest extends SchedulerTracerTestBase(MORSEL)

// WORKLOAD
class MorselWorkloadTest extends WorkloadTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselNoFusingWorkloadTest extends WorkloadTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)

// PROFILE
class MorselProfileNoFusingRowsTest extends ProfileRowsTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)
class MorselProfileRowsTest extends ProfileRowsTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
class MorselProfileNoFusingTimeTest extends ProfileTimeTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)
class MorselProfileNoFusingDbHitsTest extends MorselDbHitsTestBase(SINGLE_THREADED_NO_FUSING, MORSEL, SIZE_HINT)
class MorselProfileDbHitsTest extends MorselDbHitsTestBase(SINGLE_THREADED, MORSEL, SIZE_HINT)
