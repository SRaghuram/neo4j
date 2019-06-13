/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.slotted.SlottedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests._
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, SlottedRuntime}

object SlottedSpecSuite {
  val SIZE_HINT = 200
}

class SlottedAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedAggregationTest extends AggregationTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedOrderedAggregationTest extends OrderedAggregationTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedExpandAllTest extends ExpandAllTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
                           with ExpandAllWithOptionalTestBase[EnterpriseRuntimeContext]
class SlottedLabelScanTest extends LabelScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                               with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
class SlottedInputTest extends InputTestBase(ENTERPRISE.SINGLE_THREADED, SlottedRuntime, SIZE_HINT)
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
