/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.PipelinedRuntime.PARALLEL
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.pipelined._
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile

object ParallelRuntimeSpecSuite {
  val SIZE_HINT = 1000
}

trait ParallelRuntimeSpecSuite extends TimeLimitedCypherTest with AssertFusingSucceeded {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>
}

// INPUT
class ParallelRuntimeInputTest extends ParallelInputTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeInputTestNoFusing extends ParallelInputTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// ALL NODE SCAN
class ParallelRuntimeAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
                                     with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                     with ParallelRuntimeSpecSuite
class ParallelRuntimeAllNodeScanNoFusingTest extends AllNodeScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
                                             with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                             with ParallelRuntimeSpecSuite
class ParallelRuntimeAllNodeScanStressTest extends AllNodeScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeAllNodeScanNoFusingStressTest extends AllNodeScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// NODE BY ID SEEK
class ParallelRuntimeNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeByIdSeekStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeByIdSeekNoFusingStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// RELATIONSHIP BY ID SEEK
class ParallelRuntimeDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// UNDIRECTED RELATIONSHIP BY ID SEEK
class ParallelRuntimeUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// NODE COUNT FROM COUNT STORE
class ParallelNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// RELATIONSHIP COUNT FROM COUNT STORE
class ParallelRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// LABEL SCAN
class ParallelRuntimeLabelScanTest extends LabelScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeLabelScanNoFusingTest extends LabelScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeLabelScanStressTest extends LabelScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeLabelScanNoFusingStressTest extends LabelScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// INDEX SEEK
class ParallelRuntimeNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                       with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                                       with ArrayIndexSupport[EnterpriseRuntimeContext]
class ParallelRuntimeNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                                               with ArrayIndexSupport[EnterpriseRuntimeContext]

class ParallelRuntimeIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.FUSING,PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexSeekRangeNoFusingStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexSeekExactStressTest extends IndexSeekExactStressTest(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexSeekExactNoFusingStressTest extends IndexSeekExactStressTest(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// INDEX SCAN
class ParallelRuntimeNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexScanNoFusingStressTest extends IndexScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexScanStressTest extends IndexScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// INDEX CONTAINS SCAN
class ParallelRuntimeNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexContainsScanStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexContainsScanNoFusingStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// INDEX ENDS WITH SCAN
class ParallelRuntimeNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexEndsWithScanStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeIndexEndsWithScanNoFusingStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// ARGUMENT
class ParallelRuntimeArgumentTest extends ArgumentTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeArgumentNoFusingTest extends ArgumentTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeArgumentStressTest extends ArgumentStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeArgumentNoFusingStressTest extends ArgumentStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// APPLY
class ParallelRuntimeApplyStressTest extends ApplyStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeApplyNoFusingStressTest extends ApplyStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// EXPAND ALL
class ParallelRuntimeExpandAllTest extends ExpandAllTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                   with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandAllTestNoFusing extends ExpandAllTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandAllStressTest extends ExpandAllStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeExpandAllNoFusingStressTest extends ExpandAllStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// EXPAND INTO
class ParallelRuntimeExpandIntoTest extends ExpandIntoTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
                                    with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext] with ParallelRuntimeSpecSuite
class ParallelRuntimeExpandIntoTestNoFusing extends ExpandIntoTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
                                            with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext] with ParallelRuntimeSpecSuite

// OPTIONAL EXPAND ALL
class ParallelRuntimeOptionalExpandAllTest extends OptionalExpandAllTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite
class ParallelRuntimeOptionalExpandAllTestNoFusing extends OptionalExpandAllTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite

// OPTIONAL EXPAND INTO
class ParallelRuntimeOptionalExpandIntoTest extends OptionalExpandIntoTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite
class ParallelRuntimeOptionalExpandIntoTestNoFusing extends OptionalExpandIntoTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite

// VAR EXPAND
class ParallelRuntimeVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeVarExpandStressTest extends VarExpandStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeVarExpandNoFusingStressTest extends VarExpandStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// PRUNING VAR EXPAND
class ParallelRuntimePruningVarLengthExpandTest extends PruningVarLengthExpandTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimePruningNoFusingVarLengthExpandTest extends PruningVarLengthExpandTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// PROJECTION
class ParallelRuntimeProjectionTest extends ProjectionTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeProjectionNoFusingTest extends ProjectionTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeProjectionStressTest extends ProjectionStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeProjectionNoFusingStressTest extends ProjectionStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeCachePropertiesTest extends CachePropertiesTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeCachePropertiesNoFusingTest extends CachePropertiesTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// FILTER
class ParallelRuntimeFilterTest extends FilterTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeFilterNoFusingTest extends FilterTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeFilterStressTest extends FilterStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeFilterNoFusingStressTest extends FilterStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

//Misc Expressions
class ParallelRuntimeExpressionStressTest extends ExpressionStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeExpressionNoFusingStressTest extends ExpressionStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// LIMIT
class ParallelRuntimeLimitTest extends LimitTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeLimitNoFusingTest extends LimitTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// DISTINCT
class ParallelRuntimeDistinctTest extends DistinctTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDistinctStressTest extends DistinctStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDistinctNoFusingStressTest extends DistinctStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// UNWIND
class ParallelRuntimeUnwindTest extends UnwindTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeUnwindNoFusingTest extends UnwindTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeUnwindStressTest extends UnwindStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUnwindNoFusingStressTest extends UnwindStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// SORT
class ParallelRuntimeSortTest extends SortTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeSortNoFusingTest extends SortTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// TOP
class ParallelRuntimeTopTest extends TopTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeTopNoFusingTest extends TopTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// AGGREGATION
class ParallelRuntimeAggregationTest extends AggregationTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeAggregationNoFusingTest extends AggregationTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeAggregationStressTest extends AggregationStressTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// NODE HASH JOIN
class ParallelRuntimeNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeHashJoinNoFusingTest extends NodeHashJoinTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// REACTIVE
class ParallelRuntimeReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeReactiveResultsNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeReactiveResultsStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with ParallelRuntimeSpecSuite// this test is slow, hence the reduced size
class ParallelRuntimeReactiveNoFusingStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with ParallelRuntimeSpecSuite// this test is slow, hence the reduced size

// OPTIONAL
class ParallelRuntimeOptionalTest extends OptionalTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeOptionalNoFusingTest extends OptionalTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite


// CARTESIAN PRODUCT
class ParallelRuntimeCartesianProductTest extends CartesianProductTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeCartesianProductNoFusingTest extends CartesianProductTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// GENERAL
class ParallelRuntimeMiscTest extends MiscTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingMiscTest extends MiscTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeExpressionTest extends ExpressionTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingExpressionTest extends ExpressionTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelFusingNotificationTest extends PipelinedFusingNotificationTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest // not ParallelRuntimeSpecSuite, since we expect fusing to fail
class ParallelRuntimeSchedulerTracerTest extends SchedulerTracerTestBase(PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeSubscriberErrorTest extends SubscriberErrorTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// WORKLOAD
class ParallelRuntimeWorkloadTest extends WorkloadTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingWorkloadTest extends WorkloadTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// ERROR HANDLING
class ParallelErrorHandlingTest extends ParallelErrorHandlingTestBase(PARALLEL) with ParallelRuntimeSpecSuite

// PROFILE
class ParallelRuntimeProfileNoFusingRowsTest extends ProfileRowsTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with ParallelRuntimeSpecSuite
class ParallelRuntimeProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with ParallelRuntimeSpecSuite
class ParallelRuntimeProfileNoFusingTimeTest extends ProfileTimeTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeProfileNoTimeTest extends ProfileNoTimeTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite {
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
    queryProfile.operatorProfile(4).time() should not be(OperatorProfile.NO_DATA) // expand - fused, but the prepare output time is attributed here
    queryProfile.operatorProfile(5).time() should not be(OperatorProfile.NO_DATA) // node by label scan - not fused
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }
}
class ParallelRuntimeProfileNoFusingDbHitsTest extends PipelinedDbHitsTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
