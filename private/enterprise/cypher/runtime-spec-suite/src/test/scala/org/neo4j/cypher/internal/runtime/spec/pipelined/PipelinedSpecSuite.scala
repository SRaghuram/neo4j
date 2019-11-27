/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE.{FUSING, NO_FUSING}
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.slotted.WithSlotsMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile

object PipelinedSpecSuite {
  val SIZE_HINT = 1000
}

trait PipelinedSpecSuite extends AssertFusingSucceeded {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>
}

// INPUT
class PipelinedInputTest extends InputTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// ALL NODE SCAN
class PipelinedAllNodeScanTest extends AllNodeScanTestBase(FUSING, PIPELINED, SIZE_HINT)
                            with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                            with PipelinedSpecSuite
class PipelinedAllNodeScanNoFusingTest extends AllNodeScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                    with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                    with PipelinedSpecSuite

// NODE BY ID SEEK
class PipelinedNodeByIdSeekTest extends NodeByIdSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// RELATIONSHIP BY ID SEEK
class PipelinedDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// UNDIRECTED RELATIONSHIP BY ID SEEK
class PipelinedUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// NODE COUNT FROM COUNT STORE
class PipelinedNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(NO_FUSING, PIPELINED) with PipelinedSpecSuite

// RELATIONSHIP COUNT FROM COUNT STORE
class PipelinedRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(NO_FUSING, PIPELINED) with PipelinedSpecSuite

// LABEL SCAN
class PipelinedLabelScanTest extends LabelScanTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLabelScanNoFusingTest extends LabelScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// INDEX SEEK
class PipelinedNodeIndexSeekTest extends NodeIndexSeekTestBase(FUSING, PIPELINED, SIZE_HINT)
                              with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                              with ArrayIndexSupport[EnterpriseRuntimeContext]
                              with PipelinedSpecSuite

class PipelinedNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                      with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                                      with ArrayIndexSupport[EnterpriseRuntimeContext]
                                      with PipelinedSpecSuite

// INDEX SCAN
class PipelinedNodeIndexScanTest extends NodeIndexScanTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// INDEX CONTAINS SCAN
class PipelinedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// INDEX ENDS WITH SCAN
class PipelinedNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// ARGUMENT
class PipelinedArgumentTest extends ArgumentTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedArgumentNoFusingTest extends ArgumentTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// EXPAND ALL
class PipelinedExpandAllTest extends ExpandAllTestBase(FUSING, PIPELINED, SIZE_HINT)
                          with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                          with PipelinedSpecSuite
class PipelinedExpandAllTestNoFusing extends ExpandAllTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                  with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                  with PipelinedSpecSuite

// EXPAND INTO
class PipelinedExpandIntoTest extends ExpandIntoTestBase(FUSING, PIPELINED, SIZE_HINT)
                           with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                           with PipelinedSpecSuite
class PipelinedExpandIntoTestNoFusing extends ExpandIntoTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                   with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                   with PipelinedSpecSuite

// OPTIONAL EXPAND ALL
class PipelinedOptionalExpandAllTest extends OptionalExpandAllTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedOptionalExpandAllTestNoFusing extends OptionalExpandAllTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// OPTIONAL EXPAND INTO
class PipelinedOptionalExpandIntoTest extends OptionalExpandIntoTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedOptionalExpandIntoTestNoFusing extends OptionalExpandIntoTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// VAR EXPAND
class PipelinedVarLengthExpandTest extends VarLengthExpandTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PRUNING VAR EXPAND
class PipelinedPruningVarLengthExpandTest extends PruningVarLengthExpandTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedPruningNoFusingVarLengthExpandTest extends PruningVarLengthExpandTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PROJECT ENDPOINTS
class PipelinedProjectEndpointsTest extends ProjectEndpointsTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedProjectEndpointsTestNoFusing extends ProjectEndpointsTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PROJECTION
class PipelinedProjectionTest extends ProjectionTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedProjectionNoFusingTest extends ProjectionTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedCachePropertiesTest extends CachePropertiesTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedCachePropertiesNoFusingTest extends CachePropertiesTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// FILTER
class PipelinedFilterTest extends FilterTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedFilterNoFusingTest extends FilterTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// LIMIT
class PipelinedLimitTest extends LimitTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLimitNoFusingTest extends LimitTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// DISTINCT
class PipelinedDistinctTest extends DistinctTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedDistinctNoFusingTest extends DistinctTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// UNWIND
class PipelinedUnwindTest extends UnwindTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedUnwindNoFusingTest extends UnwindTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// SORT
class PipelinedSortTest extends SortTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSortNoFusingTest extends SortTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// TOP
class PipelinedTopTest extends TopTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedTopNoFusingTest extends TopTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// AGGREGATION
class PipelinedAggregationTest extends AggregationTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedAggregationNoFusingTest extends AggregationTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// NODE HASH JOIN
class PipelinedNodeHashJoinTest extends NodeHashJoinTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNodeHashJoinNoFusingTest extends NodeHashJoinTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PROVIDED ORDER
class PipelinedProvidedOrderTest extends ProvidedOrderTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNoFusingProvidedOrderTest extends ProvidedOrderTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// REACTIVE
class PipelinedReactiveResultsTest extends ReactiveResultTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedReactiveResultsNoFusingTest extends ReactiveResultTestBase(NO_FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedReactiveResultsStressTest
  extends ReactiveResultStressTestBase(FUSING, PIPELINED,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with PipelinedSpecSuite
class PipelinedReactiveResultsNoFusingStressTest
  extends ReactiveResultStressTestBase(NO_FUSING, PIPELINED,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with PipelinedSpecSuite

// OPTIONAL
class PipelinedOptionalTest extends OptionalTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedOptionalNoFusingTest extends OptionalTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// CARTESIAN PRODUCT
class PipelinedCartesianProductTest extends CartesianProductTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedCartesianProductNoFusingTest extends CartesianProductTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// GENERAL
class PipelinedMiscTest extends MiscTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedMiscNoFusingTest extends MiscTestBase(NO_FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedExpressionTest extends ExpressionTestBase(FUSING, PIPELINED)
                           with ThreadUnsafeExpressionTests[EnterpriseRuntimeContext]
                           with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
class PipelinedExpressionNoFusingTest extends ExpressionTestBase(NO_FUSING, PIPELINED)
                                   with ThreadUnsafeExpressionTests[EnterpriseRuntimeContext]
                                   with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
class PipelinedFusingNotificationTest extends PipelinedFusingNotificationTestBase(FUSING, PIPELINED) // not PipelinedSpecSuite, since we expect fusing to fail
class PipelinedSchedulerTracerTest extends SchedulerTracerTestBase(PIPELINED) with PipelinedSpecSuite
class PipelinedMemoryManagementTest extends MemoryManagementTestBase(FUSING, PIPELINED)
                                 with WithSlotsMemoryManagementTestBase
                                 with PipelinedSpecSuite
class PipelinedMemoryManagementNoFusingTest extends MemoryManagementTestBase(NO_FUSING, PIPELINED)
                                         with WithSlotsMemoryManagementTestBase
                                         with PipelinedSpecSuite
class PipelinedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedSubscriberErrorTest extends SubscriberErrorTestBase(FUSING, PIPELINED) with PipelinedSpecSuite

// SLOTTED PIPE FALLBACK OPERATOR
class PipelinedSlottedPipeFallbackTest extends SlottedPipeFallbackTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// WORKLOAD with PipelinedSpecSuite
class PipelinedWorkloadTest extends WorkloadTestBase(FUSING, PIPELINED, SIZE_HINT)
class PipelinedNoFusingWorkloadTest extends WorkloadTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PROFILE
class PipelinedProfileNoFusingRowsTest extends ProfileRowsTestBase(NO_FUSING, PIPELINED, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with PipelinedSpecSuite
class PipelinedProfileRowsTest extends ProfileRowsTestBase(FUSING, PIPELINED, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with PipelinedSpecSuite
class PipelinedProfileNoFusingTimeTest extends ProfileTimeTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedProfileNoTimeTest extends ProfileNoTimeTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite {
  //this test differs in Pipelined and Parallel since we fuse differently
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
class PipelinedProfileNoFusingDbHitsTest extends PipelinedDbHitsTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                      with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
                                      with PipelinedSpecSuite
class PipelinedProfileDbHitsTest extends PipelinedDbHitsTestBase(FUSING, PIPELINED, SIZE_HINT)
                              with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
                              with PipelinedSpecSuite {

  override protected def canFuseOverPipelines: Boolean = true
}
