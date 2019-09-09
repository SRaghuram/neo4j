/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

class SlottedAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.FUSING, SlottedRuntime)
class SlottedRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.FUSING, SlottedRuntime)
class SlottedAggregationTest extends AggregationTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedOrderedAggregationTest extends OrderedAggregationTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedExpandAllTest extends ExpandAllTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class SlottedVarExpandAllTest extends VarLengthExpandTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedLabelScanTest extends LabelScanTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                               with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
                               with ArrayIndexSupport[EnterpriseRuntimeContext]
class SlottedInputTest extends InputTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedInputWithMaterializedEntitiesTest extends InputWithMaterializedEntitiesTest(ENTERPRISE.FUSING, SlottedRuntime)
class SlottedPartialSortTest extends PartialSortTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedTopTest extends TopTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedSortTest extends SortTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedPartialTopNTest extends PartialTopNTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedPartialTop1Test extends PartialTop1TestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedFilterTest extends FilterTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedArgumentTest extends ArgumentTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedProjectionTest extends ProjectionTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedCachePropertiesTest extends CachePropertiesTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedUnwindTest extends UnwindTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedDistinctTest extends DistinctTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedOrderedDistinctTest extends OrderedDistinctTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedLimitTest extends LimitTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.FUSING, SlottedRuntime)
class SlottedReactiveResultsStressTest extends ReactiveResultStressTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedMiscTest extends MiscTestBase(ENTERPRISE.FUSING, SlottedRuntime)
class SlottedProvidedOrderTest extends ProvidedOrderTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedProfileDbHitsTest extends LegacyDbHitsTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedProfilePageCacheStatsTest extends ProfilePageCacheStatsTestBase(ENTERPRISE.FUSING, SlottedRuntime)
class SlottedOptionalTest extends OptionalTestBase(ENTERPRISE.FUSING, SlottedRuntime, SIZE_HINT)
class SlottedMemoryManagementTest extends MemoryManagementTestBase(ENTERPRISE.FUSING, SlottedRuntime)
                                  with FullSupportMemoryManagementTestBase[EnterpriseRuntimeContext]
                                  with SlottedMemoryManagementTestBase
class SlottedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.FUSING, SlottedRuntime)
class SlottedSubscriberErrorTest extends SubscriberErrorTestBase(ENTERPRISE.FUSING, SlottedRuntime)
