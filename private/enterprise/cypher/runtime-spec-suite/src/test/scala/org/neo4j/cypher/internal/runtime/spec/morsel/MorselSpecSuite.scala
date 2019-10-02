/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.MorselRuntime.MORSEL
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE.{FUSING, NO_FUSING}
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.slotted.WithSlotsMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, LogicalQueryBuilder}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile

object MorselSpecSuite {
  val SIZE_HINT = 1000
}

// INPUT
class MorselInputTest extends InputTestBase(FUSING, MORSEL, SIZE_HINT)

// ALL NODE SCAN
class MorselAllNodeScanTest extends AllNodeScanTestBase(FUSING, MORSEL, SIZE_HINT)
                            with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class MorselAllNodeScanNoFusingTest extends AllNodeScanTestBase(NO_FUSING, MORSEL, SIZE_HINT)
                                    with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]

// NODE BY ID SEEK
class MorselNodeByIdSeekTest extends NodeByIdSeekTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// RELATIONSHIP BY ID SEEK
class MorselDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// UNDIRECTED RELATIONSHIP BY ID SEEK
class MorselUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// NODE COUNT FROM COUNT STORE
class MorselNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(FUSING, MORSEL)
class MorselNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(NO_FUSING, MORSEL)

// RELATIONSHIP COUNT FROM COUNT STORE
class MorselRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(FUSING, MORSEL)
class MorselRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(NO_FUSING, MORSEL)

// LABEL SCAN
class MorselLabelScanTest extends LabelScanTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselLabelScanNoFusingTest extends LabelScanTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// INDEX SEEK
class MorselNodeIndexSeekTest extends NodeIndexSeekTestBase(FUSING, MORSEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]

class MorselNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(NO_FUSING, MORSEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]

// INDEX SCAN
class MorselNodeIndexScanTest extends NodeIndexScanTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// INDEX CONTAINS SCAN
class MorselNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// INDEX ENDS WITH SCAN
class MorselNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// ARGUMENT
class MorselArgumentTest extends ArgumentTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselArgumentNoFusingTest extends ArgumentTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// APPLY
class MorselApplyStressTest extends ApplyStressTestBase(ENTERPRISE.FUSING, MORSEL)
class MorselApplyNoFusingStressTest extends ApplyStressTestBase(ENTERPRISE.NO_FUSING, MORSEL)

// EXPAND
class MorselExpandAllTest extends ExpandAllTestBase(FUSING, MORSEL, SIZE_HINT)
                          with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class MorselExpandAllTestNoFusing extends ExpandAllTestBase(NO_FUSING, MORSEL, SIZE_HINT)
                                  with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class MorselExpandIntoTest extends ExpandIntoTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselExpandIntoTestNoFusing extends ExpandIntoTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// VAR EXPAND
class MorselVarLengthExpandTest extends VarLengthExpandTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// PROJECT ENDPOINTS
class MorselProjectEndpointsTest extends ProjectEndpointsTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselProjectEndpointsTestNoFusing extends ProjectEndpointsTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// PROJECTION
class MorselProjectionTest extends ProjectionTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselProjectionNoFusingTest extends ProjectionTestBase(NO_FUSING, MORSEL, SIZE_HINT)
class MorselCachePropertiesTest extends CachePropertiesTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselCachePropertiesNoFusingTest extends CachePropertiesTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// FILTER
class MorselFilterTest extends FilterTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselFilterNoFusingTest extends FilterTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// LIMIT
class MorselLimitTest extends LimitTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselLimitNoFusingTest extends LimitTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// DISTINCT
class MorselDistinctTest extends DistinctTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselDistinctNoFusingTest extends DistinctTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// UNWIND
class MorselUnwindTest extends UnwindTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselUnwindNoFusingTest extends UnwindTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// SORT
class MorselSortTest extends SortTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselSortNoFusingTest extends SortTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// TOP
class MorselTopTest extends TopTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselTopNoFusingTest extends TopTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// AGGREGATION
class MorselAggregationTest extends AggregationTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselAggregationNoFusingTest extends AggregationTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// NODE HASH JOIN
class MorselNodeHashJoinTest extends NodeHashJoinTestBase(FUSING, MORSEL, SIZE_HINT)

// PROVIDED ORDER
class MorselProvidedOrderTest extends ProvidedOrderTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselNoFusingProvidedOrderTest extends ProvidedOrderTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// REACTIVE
class MorselReactiveResultsTest extends ReactiveResultTestBase(FUSING, MORSEL)
class MorselReactiveResultsNoFusingTest extends ReactiveResultTestBase(NO_FUSING, MORSEL)
class MorselReactiveResultsStressTest
  extends ReactiveResultStressTestBase(FUSING, MORSEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size
class MorselReactiveResultsNoFusingStressTest
  extends ReactiveResultStressTestBase(NO_FUSING, MORSEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)//TODO this test is slow, hence the reduced size

// OPTIONAL
class MorselOptionalTest extends OptionalTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselOptionalNoFusingTest extends OptionalTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// CARTESIAN PRODUCT
class MorselCartesianProductTest extends CartesianProductTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselCartesianProductNoFusingTest extends CartesianProductTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// GENERAL
class MorselMiscTest extends MiscTestBase(FUSING, MORSEL)
class MorselMiscNoFusingTest extends MiscTestBase(NO_FUSING, MORSEL)
class MorselSchedulerTracerTest extends SchedulerTracerTestBase(MORSEL)
class MorselMemoryManagementTest extends MemoryManagementTestBase(FUSING, MORSEL)
                                 with WithSlotsMemoryManagementTestBase
class MorselMemoryManagementNoFusingTest extends MemoryManagementTestBase(NO_FUSING, MORSEL)
                                         with WithSlotsMemoryManagementTestBase
class MorselMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(FUSING, MORSEL)
class MorselSubscriberErrorTest extends SubscriberErrorTestBase(FUSING, MORSEL)

// SLOTTED PIPE FALLBACK OPERATOR
class MorselSlottedPipeFallbackTest extends SlottedPipeFallbackTestBase(FUSING, MORSEL, SIZE_HINT)

// WORKLOAD
class MorselWorkloadTest extends WorkloadTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselNoFusingWorkloadTest extends WorkloadTestBase(NO_FUSING, MORSEL, SIZE_HINT)

// PROFILE
class MorselProfileNoFusingRowsTest extends ProfileRowsTestBase(NO_FUSING, MORSEL, SIZE_HINT)
class MorselProfileRowsTest extends ProfileRowsTestBase(FUSING, MORSEL, SIZE_HINT)
class MorselProfileNoFusingTimeTest extends ProfileTimeTestBase(NO_FUSING, MORSEL, SIZE_HINT)
class MorselProfileNoTimeTest extends ProfileNoTimeTestBase(FUSING, MORSEL, SIZE_HINT) {
  //this test differs in Morsel and Parallel since we fuse differently
  test("should partially profile time if fused pipelines and non-fused pipelines co-exist") {
    // given
    circleGraph(SIZE_HINT, "X")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .sort(Seq(Ascending("c")))
      .aggregation(Seq("x AS x"), Seq("count(*) AS c"))
      .filter("x.prop = null")
      .expand("(x)-->(y)")
      .nodeByLabelScan("x", "X")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should not be(OperatorProfile.NO_DATA) // produce results - not fused
    queryProfile.operatorProfile(1).time() should not be(OperatorProfile.NO_DATA) // sort - not fused
    queryProfile.operatorProfile(2).time() should not be(OperatorProfile.NO_DATA) // aggregation - not fused
    queryProfile.operatorProfile(3).time() should be(OperatorProfile.NO_DATA) // filter - fused
    queryProfile.operatorProfile(4).time() should be(OperatorProfile.NO_DATA) // expand - fused
    queryProfile.operatorProfile(5).time() should be(OperatorProfile.NO_DATA) // node by label scan - fused
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }
}
class MorselProfileNoFusingDbHitsTest extends MorselDbHitsTestBase(NO_FUSING, MORSEL, SIZE_HINT)
class MorselProfileDbHitsTest extends MorselDbHitsTestBase(FUSING, MORSEL, SIZE_HINT)
