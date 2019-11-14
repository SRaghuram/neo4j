/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE.{FUSING, NO_FUSING}
import org.neo4j.cypher.internal.runtime.spec.morsel.MorselSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.slotted.WithSlotsMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile

object MorselSpecSuite {
  val SIZE_HINT = 1000
}

trait MorselSpecSuite extends AssertFusingSucceeded {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>
}

// INPUT
class MorselInputTest extends InputTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// ALL NODE SCAN
class MorselAllNodeScanTest extends AllNodeScanTestBase(FUSING, PIPELINED, SIZE_HINT)
                            with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                            with MorselSpecSuite
class MorselAllNodeScanNoFusingTest extends AllNodeScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                    with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                    with MorselSpecSuite

// NODE BY ID SEEK
class MorselNodeByIdSeekTest extends NodeByIdSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// RELATIONSHIP BY ID SEEK
class MorselDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// UNDIRECTED RELATIONSHIP BY ID SEEK
class MorselUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// NODE COUNT FROM COUNT STORE
class MorselNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(FUSING, PIPELINED) with MorselSpecSuite
class MorselNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(NO_FUSING, PIPELINED) with MorselSpecSuite

// RELATIONSHIP COUNT FROM COUNT STORE
class MorselRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(FUSING, PIPELINED) with MorselSpecSuite
class MorselRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(NO_FUSING, PIPELINED) with MorselSpecSuite

// LABEL SCAN
class MorselLabelScanTest extends LabelScanTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselLabelScanNoFusingTest extends LabelScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// INDEX SEEK
class MorselNodeIndexSeekTest extends NodeIndexSeekTestBase(FUSING, PIPELINED, SIZE_HINT)
                              with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                              with ArrayIndexSupport[EnterpriseRuntimeContext]
                              with MorselSpecSuite

class MorselNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                      with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                                      with ArrayIndexSupport[EnterpriseRuntimeContext]
                                      with MorselSpecSuite

// INDEX SCAN
class MorselNodeIndexScanTest extends NodeIndexScanTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// INDEX CONTAINS SCAN
class MorselNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// INDEX ENDS WITH SCAN
class MorselNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// ARGUMENT
class MorselArgumentTest extends ArgumentTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselArgumentNoFusingTest extends ArgumentTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// EXPAND ALL
class MorselExpandAllTest extends ExpandAllTestBase(FUSING, PIPELINED, SIZE_HINT)
                          with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                          with MorselSpecSuite
class MorselExpandAllTestNoFusing extends ExpandAllTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                  with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                  with MorselSpecSuite

// EXPAND INTO
class MorselExpandIntoTest extends ExpandIntoTestBase(FUSING, PIPELINED, SIZE_HINT)
                           with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                           with MorselSpecSuite
class MorselExpandIntoTestNoFusing extends ExpandIntoTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                   with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                   with MorselSpecSuite

// OPTIONAL EXPAND ALL
class MorselOptionalExpandAllTest extends OptionalExpandAllTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselOptionalExpandAllTestNoFusing extends OptionalExpandAllTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// OPTIONAL EXPAND INTO
class MorselOptionalExpandIntoTest extends OptionalExpandIntoTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselOptionalExpandIntoTestNoFusing extends OptionalExpandIntoTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// VAR EXPAND
class MorselVarLengthExpandTest extends VarLengthExpandTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// PROJECT ENDPOINTS
class MorselProjectEndpointsTest extends ProjectEndpointsTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselProjectEndpointsTestNoFusing extends ProjectEndpointsTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// PROJECTION
class MorselProjectionTest extends ProjectionTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselProjectionNoFusingTest extends ProjectionTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselCachePropertiesTest extends CachePropertiesTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselCachePropertiesNoFusingTest extends CachePropertiesTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// FILTER
class MorselFilterTest extends FilterTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselFilterNoFusingTest extends FilterTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// LIMIT
class MorselLimitTest extends LimitTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselLimitNoFusingTest extends LimitTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// DISTINCT
class MorselDistinctTest extends DistinctTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselDistinctNoFusingTest extends DistinctTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// UNWIND
class MorselUnwindTest extends UnwindTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselUnwindNoFusingTest extends UnwindTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// SORT
class MorselSortTest extends SortTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselSortNoFusingTest extends SortTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// TOP
class MorselTopTest extends TopTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselTopNoFusingTest extends TopTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// AGGREGATION
class MorselAggregationTest extends AggregationTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselAggregationNoFusingTest extends AggregationTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// NODE HASH JOIN
class MorselNodeHashJoinTest extends NodeHashJoinTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselNodeHashJoinNoFusingTest extends NodeHashJoinTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// PROVIDED ORDER
class MorselProvidedOrderTest extends ProvidedOrderTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselNoFusingProvidedOrderTest extends ProvidedOrderTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// REACTIVE
class MorselReactiveResultsTest extends ReactiveResultTestBase(FUSING, PIPELINED) with MorselSpecSuite
class MorselReactiveResultsNoFusingTest extends ReactiveResultTestBase(NO_FUSING, PIPELINED) with MorselSpecSuite
class MorselReactiveResultsStressTest
  extends ReactiveResultStressTestBase(FUSING, PIPELINED,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with MorselSpecSuite
class MorselReactiveResultsNoFusingStressTest
  extends ReactiveResultStressTestBase(NO_FUSING, PIPELINED,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with MorselSpecSuite

// OPTIONAL
class MorselOptionalTest extends OptionalTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselOptionalNoFusingTest extends OptionalTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// CARTESIAN PRODUCT
class MorselCartesianProductTest extends CartesianProductTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselCartesianProductNoFusingTest extends CartesianProductTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// GENERAL
class MorselMiscTest extends MiscTestBase(FUSING, PIPELINED) with MorselSpecSuite
class MorselMiscNoFusingTest extends MiscTestBase(NO_FUSING, PIPELINED) with MorselSpecSuite
class MorselExpressionTest extends ExpressionTestBase(FUSING, PIPELINED)
                           with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
class MorselExpressionNoFusingTest extends ExpressionTestBase(NO_FUSING, PIPELINED)
                                   with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
class MorselFusingNotificationTest extends MorselFusingNotificationTestBase(FUSING, PIPELINED) // not MorselSpecSuite, since we expect fusing to fail
class MorselSchedulerTracerTest extends SchedulerTracerTestBase(PIPELINED) with MorselSpecSuite
class MorselMemoryManagementTest extends MemoryManagementTestBase(FUSING, PIPELINED)
                                 with WithSlotsMemoryManagementTestBase
                                 with MorselSpecSuite
class MorselMemoryManagementNoFusingTest extends MemoryManagementTestBase(NO_FUSING, PIPELINED)
                                         with WithSlotsMemoryManagementTestBase
                                         with MorselSpecSuite
class MorselMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(FUSING, PIPELINED) with MorselSpecSuite
class MorselSubscriberErrorTest extends SubscriberErrorTestBase(FUSING, PIPELINED) with MorselSpecSuite

// SLOTTED PIPE FALLBACK OPERATOR
class MorselSlottedPipeFallbackTest extends SlottedPipeFallbackTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// WORKLOAD with MorselSpecSuite
class MorselWorkloadTest extends WorkloadTestBase(FUSING, PIPELINED, SIZE_HINT)
class MorselNoFusingWorkloadTest extends WorkloadTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite

// PROFILE
class MorselProfileNoFusingRowsTest extends ProfileRowsTestBase(NO_FUSING, PIPELINED, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with MorselSpecSuite
class MorselProfileRowsTest extends ProfileRowsTestBase(FUSING, PIPELINED, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with MorselSpecSuite
class MorselProfileNoFusingTimeTest extends ProfileTimeTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite
class MorselProfileNoTimeTest extends ProfileNoTimeTestBase(FUSING, PIPELINED, SIZE_HINT) with MorselSpecSuite {
  //this test differs in Morsel and Parallel since we fuse differently
  test("should partially profile time if fused pipelines and non-fused pipelines co-exist") {
    given { circleGraph(SIZE_HINT, "X") }

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
class MorselProfileNoFusingDbHitsTest extends MorselDbHitsTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                      with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
                                      with MorselSpecSuite
class MorselProfileDbHitsTest extends MorselDbHitsTestBase(FUSING, PIPELINED, SIZE_HINT)
                              with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
                              with MorselSpecSuite
