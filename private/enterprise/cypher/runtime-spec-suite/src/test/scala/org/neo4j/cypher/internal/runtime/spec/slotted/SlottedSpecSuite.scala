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

class SlottedAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
class SlottedRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
class SlottedAggregationTest extends AggregationTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedOrderedAggregationTest extends OrderedAggregationTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedExpandAllTest extends ExpandAllTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class SlottedVarExpandAllTest extends VarLengthExpandTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedLabelScanTest extends LabelScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                               with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
                               with ArrayIndexSupport[EnterpriseRuntimeContext]
class SlottedInputTest extends InputTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedInputWithNoDbAccessTest extends InputWithNoDbAccessTest(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
class SlottedPartialSortTest extends PartialSortTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedSortTest extends SortTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedPartialTopNTest extends PartialTopNTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedPartialTop1Test extends PartialTop1TestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedFilterTest extends FilterTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedArgumentTest extends ArgumentTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedProjectionTest extends ProjectionTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedUnwindTest extends UnwindTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedDistinctTest extends DistinctTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedOrderedDistinctTest extends OrderedDistinctTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedLimitTest extends LimitTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
class SlottedReactiveResultsStressTest extends ReactiveResultStressTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedMiscTest extends MiscTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
class SlottedProvidedOrderTest extends ProvidedOrderTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedProfileDbHitsTest extends LegacyDbHitsTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedProfilePageCacheStatsTest extends ProfilePageCacheStatsTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
class SlottedOptionalTest extends OptionalTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedMemoryManagementTest extends MemoryManagementTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
                                  with FullSupportMemoryManagementTestBase[EnterpriseRuntimeContext]
                                  with SlottedMemoryManagementTestBase
class SlottedMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime)
