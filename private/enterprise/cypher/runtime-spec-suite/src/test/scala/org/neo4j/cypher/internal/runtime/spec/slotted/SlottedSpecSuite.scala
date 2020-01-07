/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.interpreted.LegacyDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.slotted.SlottedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, SlottedRuntime}

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
class SlottedNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedValueHashJoinTest extends ValueHashJoinTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedReactiveResultsStressTest extends ReactiveResultStressTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedMiscTest extends MiscTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedProvidedOrderTest extends ProvidedOrderTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                               with CartesianProductProvidedOrderTestBase[EnterpriseRuntimeContext]
class SlottedProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT, 1)
                             with ProcedureCallRowsTestBase[EnterpriseRuntimeContext]
class SlottedProfileDbHitsTest extends LegacyDbHitsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
                               with ProcedureCallDbHitsTestBase[EnterpriseRuntimeContext]
class SlottedProfilePageCacheStatsTest extends ProfilePageCacheStatsTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedOptionalTest extends OptionalTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedMemoryManagementTest extends MemoryManagementTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
                                  with FullSupportMemoryManagementTestBase[EnterpriseRuntimeContext]
                                  with SlottedMemoryManagementTestBase
class SlottedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedSubscriberErrorTest extends SubscriberErrorTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
class SlottedCartesianProductTest extends CartesianProductTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedExpressionTest extends ExpressionTestBase(ENTERPRISE.DEFAULT, SlottedRuntime)
                            with ThreadUnsafeExpressionTests[EnterpriseRuntimeContext]
                            with ExpressionWithTxStateChangesTests[EnterpriseRuntimeContext]
class SlottedProcedureCallTest extends ProcedureCallTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
class SlottedShortestPathTest extends ShortestPathTestBase(ENTERPRISE.DEFAULT, SlottedRuntime, SIZE_HINT)
