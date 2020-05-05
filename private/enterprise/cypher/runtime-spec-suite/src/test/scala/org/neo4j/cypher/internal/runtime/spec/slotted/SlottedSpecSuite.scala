/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.SlottedRuntime
import org.neo4j.cypher.internal.runtime.spec.CompiledExpressionsTestBase
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.interpreted.LegacyDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.slotted.SlottedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.AggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArgumentTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArrayIndexSupport
import org.neo4j.cypher.internal.runtime.spec.tests.CacheFromCursorTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CachePropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ConditionalApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionWithTxStateChangesTests
import org.neo4j.cypher.internal.runtime.spec.tests.FilterTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.FullSupportMemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.FullSupportProfileMemoryTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.InputTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.InputWithMaterializedEntitiesTest
import org.neo4j.cypher.internal.runtime.spec.tests.LabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LeftOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LimitTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MiscTestBase
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
import org.neo4j.cypher.internal.runtime.spec.tests.NodeLockingUniqueIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NonParallelProfileRowsTestBase
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
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectEndpointsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RightOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RollupApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ShortestPathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SkipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubscriberErrorTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ThreadUnsafeExpressionTests
import org.neo4j.cypher.internal.runtime.spec.tests.TopTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UndirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnwindTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ValueHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.VarLengthExpandTestBase

object SlottedSpecSuite {
  val SIZE_HINT = 200
}

class SlottedAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                             with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class SlottedNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedAggregationTest extends AggregationTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedOrderedAggregationTest extends OrderedAggregationTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedExpandAllTest extends ExpandAllTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class SlottedExpandIntoTest extends ExpandIntoTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                            with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class SlottedOptionalExpandAllTest extends OptionalExpandAllTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedOptionalExpandIntoTest extends OptionalExpandIntoTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedVarExpandAllTest extends VarLengthExpandTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedPruningVarExpandTest extends PruningVarLengthExpandTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedProjectEndpointsTest extends ProjectEndpointsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedLabelScanTest extends LabelScanTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                               with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
                               with ArrayIndexSupport[EnterpriseRuntimeContext]
class SlottedNodeIndexStartsWithSeekTest extends NodeIndexStartsWithSeekTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexPointDistanceSeekTest extends NodeIndexPointDistanceSeekTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedInputTest extends InputTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedInputWithMaterializedEntitiesTest extends InputWithMaterializedEntitiesTest(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedPartialSortTest extends PartialSortTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedTopTest extends TopTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedSortTest extends SortTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedPartialTopNTest extends PartialTopNTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedPartialTop1Test extends PartialTop1TestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedFilterTest extends FilterTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedArgumentTest extends ArgumentTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedProjectionTest extends ProjectionTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedCachePropertiesTest extends CachePropertiesTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedUnwindTest extends UnwindTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedDistinctTest extends DistinctTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedOrderedDistinctTest extends OrderedDistinctTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedLimitTest extends LimitTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedSkipTest extends SkipTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedValueHashJoinTest extends ValueHashJoinTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedRightOuterHashJoinTest extends RightOuterHashJoinTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedLeftOuterHashJoinTest extends LeftOuterHashJoinTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedReactiveResultsStressTest extends ReactiveResultStressTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedMiscTest extends MiscTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedProvidedOrderTest extends ProvidedOrderTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                               with CartesianProductProvidedOrderTestBase[EnterpriseRuntimeContext]
class SlottedProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT, 1)
                             with NonParallelProfileRowsTestBase[EnterpriseRuntimeContext]
class SlottedProfileDbHitsTest extends LegacyDbHitsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                               with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
                               with NestedPlanDbHitsTestBase[EnterpriseRuntimeContext]
class SlottedProfilePageCacheStatsTest extends ProfilePageCacheStatsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedProfileMemoryTest extends ProfileMemoryTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
                               with FullSupportProfileMemoryTestBase[EnterpriseRuntimeContext]
                               with ProfileSlottedMemoryTestBase
class SlottedProfileMemoryTrackingDisabledTest extends ProfileMemoryTrackingDisabledTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedOptionalTest extends OptionalTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                          with OptionalFailureTestBase[EnterpriseRuntimeContext]
class SlottedMemoryManagementTest extends MemoryManagementTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
                                  with FullSupportMemoryManagementTestBase[EnterpriseRuntimeContext]
                                  with SlottedMemoryManagementTestBase
class SlottedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedSubscriberErrorTest extends SubscriberErrorTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedCartesianProductTest extends CartesianProductTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedApplyTest extends ApplyTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedExpressionTest extends ExpressionTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
                            with ThreadUnsafeExpressionTests[EnterpriseRuntimeContext]
                            with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
class SlottedProcedureCallTest extends ProcedureCallTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedShortestPathTest extends ShortestPathTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedUnionTest extends UnionTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedSemiApplyTest extends SemiApplyTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedAntiSemiApplyTest extends AntiSemiApplyTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedConditionalApplyTest extends ConditionalApplyTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedCompiledExpressionsTest extends CompiledExpressionsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)

class SlottedNestedPlanExpressionTest extends NestedPlanExpressionTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedRollupApplyTest extends RollupApplyTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedCacheFromCursorTest extends CacheFromCursorTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)

