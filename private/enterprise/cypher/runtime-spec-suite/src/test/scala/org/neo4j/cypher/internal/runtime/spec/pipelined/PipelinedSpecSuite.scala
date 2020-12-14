/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import java.lang.System.lineSeparator

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.debug.SaveGeneratedSource
import org.neo4j.cypher.internal.runtime.spec.CompiledExpressionsTestBase
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE.MORSEL_SIZE
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE.WITH_MORSEL_SIZE
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedDynamicLimitPropagationTest.CONFIGURED_MORSEL_SIZE
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedSpecSuite.FUSING
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedSpecSuite.NO_FUSING
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.slotted.WithSlotsMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.WorkloadTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AntiConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArgumentTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArrayIndexSupport
import org.neo4j.cypher.internal.runtime.spec.tests.AssertSameNodeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CachePropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CreateTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EmptyResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EnterpriseNodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExhaustiveLimitTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionWithTxStateChangesTests
import org.neo4j.cypher.internal.runtime.spec.tests.FilterTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.InputTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LeftOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LenientCreateRelationshipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetAntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetSelectOrAntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetSelectOrSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LetSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LimitTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LockNodesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryDeallocationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MiscTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MultiNodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedLeftOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NestedPlanExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexContainsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexEndsWithScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexPointDistanceSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexSeekRangeAndCompositeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexStartsWithSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonParallelProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonParallelProfileTimeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonParallelProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalFailureTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedAggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OrderedDistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialSortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialTop1TestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PartialTopNTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProcedureCallDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProcedureCallTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileMemoryTrackingDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfilePageCacheStatsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileTimeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectEndpointsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipTypeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RightOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RollupApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SelectOrAntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SelectOrSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SetPropertyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ShortestPathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SkipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SlottedPipeFallbackTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubscriberErrorTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ThreadUnsafeExpressionTests
import org.neo4j.cypher.internal.runtime.spec.tests.TopTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TriadicSelectionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UndirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnwindTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UserDefinedAggregationSupport
import org.neo4j.cypher.internal.runtime.spec.tests.ValueHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.VarLengthExpandTestBase
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.scalatest.Outcome

object PipelinedSpecSuite {
  val SIZE_HINT = 1000

  val FUSING: Edition[EnterpriseRuntimeContext] = ENTERPRISE.WITH_FUSING(ENTERPRISE.DEFAULT)
  val NO_FUSING: Edition[EnterpriseRuntimeContext] = ENTERPRISE.WITH_NO_FUSING(ENTERPRISE.DEFAULT)
}

trait PipelinedDebugGeneratedSource extends SaveGeneratedSource {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>

  /**
   * To debug generated source code for fused pipelines in pipelined runtime:
   * Set this flag to true, and set a breakpoint in [[org.neo4j.cypher.internal.PipelinedRuntime.PipelinedExecutionPlan.run]].
   * When you reach the breakpoint you should be able to find the generated source in `[maven-module]/target/generated-test-sources/cypher`.
   *
   * (If you are using IntelliJ make sure to "Mark Directory as" "Generated Sources Root".
   *  You may also need to remove the `target` folder from excluded directories in the module settings for it to show up in the Project view.)
   *
   * NOTE: You need to set your run/debug configuration working directory to the directory of the Maven module containing your test.
   *       (If you are using IntelliJ this is probably the default unless you have changed your run/debug configuration template)
   *
   * See [[SaveGeneratedSource]] for more details.
   */
  val saveGeneratedSourceEnabled: Boolean = DebugSupport.DEBUG_GENERATED_SOURCE_CODE

  // Only enable this if you want to inspect the generated source files after the test run. Otherwise they will be deleted automatically.
  override val keepSourceFilesAfterTestFinishes: Boolean = true

  override val debugOptions: CypherDebugOptions =
    if (saveGeneratedSourceEnabled) CypherDebugOptions.default.withOptionEnabled(CypherDebugOption.generateJavaSource)
    else CypherDebugOptions.default
}

trait PipelinedSpecSuite extends AssertFusingSucceeded with PipelinedDebugGeneratedSource {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>

  abstract override def withFixture(test: NoArgTest): Outcome = {
    withClue(s"Failed with MORSEL_SIZE = $MORSEL_SIZE${lineSeparator()}")(super.withFixture(test))
  }
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

// RELATIONSHIP TYPE SCAN
class PipelinedRelationshipTypeScanTest extends RelationshipTypeScanTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedRelationshipTypeScanNoFusingTest extends RelationshipTypeScanTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite


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
                                 with EnterpriseNodeIndexSeekTestBase[EnterpriseRuntimeContext]
                                 with PipelinedSpecSuite

class PipelinedNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                         with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                                         with ArrayIndexSupport[EnterpriseRuntimeContext]
                                         with EnterpriseNodeIndexSeekTestBase[EnterpriseRuntimeContext]
                                         with PipelinedSpecSuite
class PipelinedRuntimeNodeIndexStartsWithSeekTest extends NodeIndexStartsWithSeekTestBase(FUSING, PIPELINED, SIZE_HINT)
class PipelinedRuntimeNodeIndexStartsWithSeekNoFusingTest extends NodeIndexStartsWithSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT)

class PipelinedPointDistanceSeekTest extends NodeIndexPointDistanceSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedPointDistanceSeekNoFusingTest extends NodeIndexPointDistanceSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

class PipelinedMultiNodeIndexSeekTest extends MultiNodeIndexSeekTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedMultiNodeIndexSeekNoFusingTest extends MultiNodeIndexSeekTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedMultiNodeIndexSeekRewriterTest extends MultiNodeIndexSeekRewriterTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedMultiNodeIndexSeekRewriterNoFusingTest extends MultiNodeIndexSeekRewriterTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedAssertSameNodeRewriterTest extends AssertSameNodeRewriterTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedAssertSameNodeRewriterNoFusingTest extends AssertSameNodeRewriterTestBase(NO_FUSING, PIPELINED) with PipelinedSpecSuite

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

// SKIP
class PipelinedSkipTest extends SkipTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSkipNoFusingTest extends SkipTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// DISTINCT
class PipelinedDistinctTest extends DistinctTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedDistinctNoFusingTest extends DistinctTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// ORDERED DISTINCT
class PipelinedOrderedDistinctTest extends OrderedDistinctTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedOrderedDistinctNoFusingTest extends OrderedDistinctTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// UNWIND
class PipelinedUnwindTest extends UnwindTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedUnwindNoFusingTest extends UnwindTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// SORT
class PipelinedSortTest extends SortTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSortNoFusingTest extends SortTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PARTIAL SORT
class PipelinedPartialSortTest extends PartialSortTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedPartialSortNoFusingTest extends PartialSortTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// TOP
class PipelinedTopTest extends TopTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedTopNoFusingTest extends TopTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PARTIAL TOP N
class PipelinedPartialTopNTest extends PartialTopNTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedPartialTopNTopNoFusingTest extends PartialTopNTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PARTIAL TOP 1
class PipelinedPartialTop1Test extends PartialTop1TestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedPartialTop1TopNoFusingTest extends PartialTop1TestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// AGGREGATION
class PipelinedAggregationTest extends AggregationTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite with UserDefinedAggregationSupport[EnterpriseRuntimeContext]
class PipelinedAggregationNoFusingTest extends AggregationTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite with UserDefinedAggregationSupport[EnterpriseRuntimeContext]

// ORDERED AGGREGATION
class PipelinedOrderedAggregationTest extends OrderedAggregationTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedOrderedAggregationNoFusingTest extends OrderedAggregationTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// NODE HASH JOIN
class PipelinedNodeHashJoinTest extends NodeHashJoinTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNodeHashJoinNoFusingTest extends NodeHashJoinTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// NODE RIGHT OUTER HASH JOIN
class PipelinedNodeRightOuterHashJoinTest extends RightOuterHashJoinTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedNodeRightOuterHashJoinNoFusingTest extends RightOuterHashJoinTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// NODE LEFT OUTER HASH JOIN
class PipelinedNodeLeftOuterHashJoinTest extends LeftOuterHashJoinTestBase(FUSING, PIPELINED, SIZE_HINT)
                                         with NestedLeftOuterHashJoinTestBase[EnterpriseRuntimeContext]
                                         with PipelinedSpecSuite
class PipelinedNodeLeftOuterHashJoinNoFusingTest extends LeftOuterHashJoinTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
                                                 with NestedLeftOuterHashJoinTestBase[EnterpriseRuntimeContext]
                                                 with PipelinedSpecSuite

// VALUE HASH JOIN
class PipelinedValueHashJoinTest extends ValueHashJoinTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedValueHashJoinNoFusingTest extends ValueHashJoinTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PROVIDED ORDER
class PipelinedProvidedOrderTest extends ProvidedOrderTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
                                 with NonParallelProvidedOrderTestBase[EnterpriseRuntimeContext]
                                 with CartesianProductProvidedOrderTestBase[EnterpriseRuntimeContext]
class PipelinedNoFusingProvidedOrderTest extends ProvidedOrderTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
                                         with NonParallelProvidedOrderTestBase[EnterpriseRuntimeContext]
                                         with CartesianProductProvidedOrderTestBase[EnterpriseRuntimeContext]


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
                            with OptionalFailureTestBase[EnterpriseRuntimeContext]
class PipelinedOptionalNoFusingTest extends OptionalTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
                                    with OptionalFailureTestBase[EnterpriseRuntimeContext]

// CARTESIAN PRODUCT
class PipelinedCartesianProductTest extends CartesianProductTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedCartesianProductNoFusingTest extends CartesianProductTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// APPLY
class PipelinedApplyTest extends ApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedApplyNoFusingTest extends ApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PROCEDURE CALL
class PipelinedProcedureCallTest extends ProcedureCallTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedProcedureCallNoFusingTest extends ProcedureCallTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// SHORTEST PATH
class PipelinedShortestPathTest extends ShortestPathTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedShortestPathNoFusingTest extends ShortestPathTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// UNION
class PipelinedUnionTest extends UnionTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedUnionNoFusingTest extends UnionTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// SEMI APPLY
class PipelinedSemiApplyTest extends SemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSemiApplyNoFusingTest extends SemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// ANTI SEMI APPLY
class PipelinedAntiSemiApplyTest extends AntiSemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedAntiSemiApplyNoFusingTest extends AntiSemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

class PipelinedSemiApplyRewriterTest extends SemiApplyRewriterTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSemiApplyRewriterNoFusingTest extends SemiApplyRewriterTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// LET SEMI APPLY
class PipelinedLetSemiApplyTest extends LetSemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLetSemiApplyNoFusingTest extends LetSemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// LET ANTI SEMI APPLY
class PipelinedLetAntiSemiApplyTest extends LetAntiSemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLetAntiSemiApplyNoFusingTest extends LetAntiSemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// LET SEMI APPLY
class PipelinedLetSelectOrAntiSemiApplyTest extends LetSelectOrAntiSemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLetSelectOrAntiSemiApplyNoFusingTest extends LetSelectOrAntiSemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// SELECT OR SEMI APPLY
class PipelinedSelectOrSemiApplyTest extends SelectOrSemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSelectOrSemiApplyNoFusingTest extends SelectOrSemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSelectOrAntiSemiApplyTest extends SelectOrAntiSemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSelectOrAntiSemiApplyNoFusingTest extends SelectOrAntiSemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// LET SELECT OR SEMI APPLY
class PipelinedLetSelectOrSemiApplyTest extends LetSelectOrSemiApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLetSelectOrSemiApplyNoFusingTest extends LetSelectOrSemiApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

//CONDITIONAL APPLY
class PipelinedConditionalApplyTest extends ConditionalApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedConditionalApplyNoFusingTest extends ConditionalApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

//ANTI CONDITIONAL APPLY
class PipelinedAntiConditionalApplyTest extends AntiConditionalApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedAntiConditionalApplyNoFusingTest extends AntiConditionalApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// ROLLUP APPLY
class PipelinedRollupApplyTest extends RollupApplyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedRollupApplyNoFusingTest extends RollupApplyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// ROLLUP APPLY REWRITER
class PipelinedRollupApplyRewriterTest extends RollupApplyRewriterTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedRollupApplyRewriterNoFusingTest extends RollupApplyRewriterTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// EMPTY RESULT
class PipelinedEmptyResultTest extends EmptyResultTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedEmptyResultNoFusingTest extends EmptyResultTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// TRIADIC SELECTION
class PipelinedTriadicSelectionTest extends TriadicSelectionTestBase(FUSING, PIPELINED, SIZE_HINT)
class PipelinedTriadicSelectionNoFusingTest extends TriadicSelectionTestBase(NO_FUSING, PIPELINED, SIZE_HINT)

// ASSERT SAME NODE
class PipelinedAssertSameNodeTest extends AssertSameNodeTestBase(FUSING, PIPELINED, SIZE_HINT)
class PipelinedAssertSameNodeNoFusingTest extends AssertSameNodeTestBase(NO_FUSING, PIPELINED, SIZE_HINT)

//CREATE
class PipelinedCreateTest extends CreateTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedCreateNoFusingTest extends CreateTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLenientCreateRelationshipTest extends LenientCreateRelationshipTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedLenientCreateRelationshipNoFusingTest extends LenientCreateRelationshipTestBase(NO_FUSING, PIPELINED) with PipelinedSpecSuite

//SET PROPERTY
class PipelinedSetPropertyTestBase extends SetPropertyTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSetPropertyNoFusingTest extends SetPropertyTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

//EXHAUSTIVE LIMIT
class PipelinedExhaustiveLimitTest extends ExhaustiveLimitTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedExhaustiveLimitNoFusingTest extends ExhaustiveLimitTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// GENERAL
class PipelinedMiscTest extends MiscTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedMiscNoFusingTest extends MiscTestBase(NO_FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedExpressionTest extends ExpressionTestBase(FUSING, PIPELINED)
                              with ThreadUnsafeExpressionTests[EnterpriseRuntimeContext]
                              with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
                              with PipelinedSpecSuite
class PipelinedExpressionNoFusingTest extends ExpressionTestBase(NO_FUSING, PIPELINED)
                                      with ThreadUnsafeExpressionTests[EnterpriseRuntimeContext]
                                      with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
                                      with PipelinedSpecSuite
class PipelinedFusingNotificationTest extends PipelinedFusingNotificationTestBase(FUSING, PIPELINED) // not PipelinedSpecSuite, since we expect fusing to fail
class PipelinedSchedulerTracerTest extends SchedulerTracerTestBase(PIPELINED) with PipelinedSpecSuite
class PipelinedMemoryManagementTest extends MemoryManagementTestBase(FUSING, PIPELINED)
                                    with WithSlotsMemoryManagementTestBase
                                    with PipelinedSpecSuite
class PipelinedMemoryManagementNoFusingTest extends MemoryManagementTestBase(NO_FUSING, PIPELINED)
                                            with WithSlotsMemoryManagementTestBase
                                            with PipelinedSpecSuite
class PipelinedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(FUSING, PIPELINED) with PipelinedSpecSuite
class PipelinedMemoryDeallocationTest extends MemoryDeallocationTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedMemoryDeallocationNoFusingTest extends MemoryDeallocationTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedSubscriberErrorTest extends SubscriberErrorTestBase(FUSING, PIPELINED) with PipelinedSpecSuite

// SLOTTED PIPE FALLBACK OPERATOR
class PipelinedSlottedPipeFallbackTest extends SlottedPipeFallbackTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// WORKLOAD with PipelinedSpecSuite
class PipelinedWorkloadTest extends WorkloadTestBase(FUSING, PIPELINED, SIZE_HINT)
class PipelinedWorkloadNoFusingTest extends WorkloadTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

// PROFILE
class PipelinedProfileRowsNoFusingTest extends ProfileRowsTestBase(NO_FUSING, PIPELINED, SIZE_HINT, MORSEL_SIZE) with PipelinedSpecSuite
                                       with NonParallelProfileRowsTestBase[EnterpriseRuntimeContext]
class PipelinedProfileRowsTest extends ProfileRowsTestBase(FUSING, PIPELINED, SIZE_HINT, MORSEL_SIZE) with PipelinedSpecSuite
                               with NonParallelProfileRowsTestBase[EnterpriseRuntimeContext]
class PipelinedProfileTimeNoFusingTest extends ProfileTimeTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
                                       with NonParallelProfileTimeTestBase[EnterpriseRuntimeContext]
class PipelinedProfileNoTimeTest extends ProfileNoTimeTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite {
  //this test differs in Pipelined and Parallel since we fuse differently
  test("should profile time of both fused and non-fused pipelines when they co-exist") {
    given { circleGraph(SIZE_HINT, "X") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .sort(Seq(Ascending("c")))
      .aggregation(Seq("x AS x"), Seq("count(*) AS c"))
      .filter("x.prop = null")
      .expand("(x)-->(y)")
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should not be OperatorProfile.NO_DATA // produce results - not fused
    queryProfile.operatorProfile(1).time() should not be OperatorProfile.NO_DATA // sort - not fused
    queryProfile.operatorProfile(2).time() should not be OperatorProfile.NO_DATA // aggregation - not fused
    queryProfile.operatorProfile(3).time() should be(OperatorProfile.NO_DATA) // filter - fused
    queryProfile.operatorProfile(4).time() should be(OperatorProfile.NO_DATA) // expand - fused
    queryProfile.operatorProfile(5).time() should not be OperatorProfile.NO_DATA // node by label scan - fused
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }
}
class PipelinedProfileDbHitsNoFusingTest extends PipelinedDbHitsTestBase(NO_FUSING, PIPELINED, SIZE_HINT, canFuseOverPipelines = false)
                                         with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
                                         with NestedPlanDbHitsTestBase[EnterpriseRuntimeContext]
                                         with PipelinedSpecSuite
class PipelinedProfileDbHitsTest extends PipelinedDbHitsTestBase(FUSING, PIPELINED, SIZE_HINT, canFuseOverPipelines = true)
                                 with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
                                 with PipelinedSpecSuite

class PipelinedProfileMemoryNoFusingTest extends ProfileMemoryTestBase(NO_FUSING, PIPELINED)
                                         with ProfilePipelinedNoFusingMemoryTestBase
class PipelinedProfileMemoryTest extends ProfileMemoryTestBase(FUSING, PIPELINED)
                                 with ProfilePipelinedMemoryTestBase
class PipelinedProfileMemoryTrackingDisabledNoFusingTest extends ProfileMemoryTrackingDisabledTestBase(NO_FUSING, PIPELINED, SIZE_HINT)
class PipelinedProfileMemoryTrackingDisabledTest extends ProfileMemoryTrackingDisabledTestBase(FUSING, PIPELINED, SIZE_HINT)

class PipelinedProfilePageCacheStatsNoFusingTest extends ProfilePageCacheStatsTestBase(canFuseOverPipelines = false, NO_FUSING, PIPELINED)
class PipelinedProfilePageCacheStatsTest extends ProfilePageCacheStatsTestBase(canFuseOverPipelines = true, FUSING, PIPELINED)

class PipelinedNestedPlanExpressionTest extends NestedPlanExpressionTestBase(FUSING, PIPELINED, SIZE_HINT)
class PipelinedNestedPlanExpressionNoFusingTest extends NestedPlanExpressionTestBase(NO_FUSING, PIPELINED, SIZE_HINT)

class PipelinedCompiledExpressionsTest extends CompiledExpressionsTestBase(FUSING, PIPELINED)
class PipelinedCompiledExpressionsNoFusingTest extends CompiledExpressionsTestBase(NO_FUSING, PIPELINED)

class PipelinedDebugTest extends PipelinedDebugTestBase(FUSING, PIPELINED) with AssertFusingSucceeded

/**
 * This test is pipelined only, there is no reason to run in other runtimes
 */
class PipelinedDynamicLimitPropagationTest extends RuntimeTestSuite[EnterpriseRuntimeContext](WITH_MORSEL_SIZE(CONFIGURED_MORSEL_SIZE), PIPELINED) {
  test("limit should propagate upstream") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(25)
      .nonFuseable()
      .input(variables = Seq("x"))
      .build()
    val input = inputColumns(2, 50, identity).stream()

    // when
    consume(execute(logicalQuery, runtime, input))

    // then
    input.hasMore shouldBe true
  }

  test("should propagate upstream with multiple limits, smaller first") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .limit(25)
      .unwind("[1, 2, 3] AS y")
      .limit(CONFIGURED_MORSEL_SIZE)
      .nonFuseable()
      .input(variables = Seq("x"))
      .build()
    val input = inputColumns(2, 50, identity).stream()

    // when
    consume(execute(logicalQuery, runtime, input))

    // then
    input.hasMore shouldBe false
  }

  test("should propagate upstream with multiple limits, bigger first") {
    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .limit(CONFIGURED_MORSEL_SIZE)
      .unwind("[1, 2, 3] AS y")
      .limit(25)
      .nonFuseable()
      .input(variables = Seq("x"))
      .build()
    val input = inputColumns(2, 50, identity).stream()

    // when
    consume(execute(logicalQuery, runtime, input))

    // then
    input.hasMore shouldBe true
  }
}

// LOCK NODES
class PipelinedLockNodesTest extends LockNodesTestBase(FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite
class PipelinedLockNodesNoFusingTest extends LockNodesTestBase(NO_FUSING, PIPELINED, SIZE_HINT) with PipelinedSpecSuite

object PipelinedDynamicLimitPropagationTest {
  val CONFIGURED_MORSEL_SIZE: Int = 100
}
