/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE_SINGLE_THREAD
import org.neo4j.cypher.internal.runtime.spec.slotted.SlottedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.{AggregationTestBase, AllNodeScanTestBase, ArgumentTestBase, ExpandAllTestBase, ExpandAllWithOptionalTestBase, FilterTestBase, InputTestBase, LabelScanTestBase, NodeIndexContainsScanTestBase, NodeIndexScanTestBase, NodeIndexSeekRangeAndCompositeTestBase, NodeIndexSeekTestBase, NodeLockingUniqueIndexSeekTestBase, PartialSortTestBase, PartialTop1TestBase, PartialTopNTestBase}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, SlottedRuntime}

object SlottedSpecSuite {
  val SIZE_HINT = 200
}

class SlottedAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedAggregationTest extends AggregationTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedExpandAllTest extends ExpandAllTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
                           with ExpandAllWithOptionalTestBase[EnterpriseRuntimeContext]
class SlottedLabelScanTest extends LabelScanTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                               with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
class SlottedInputTest extends InputTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedPartialSortTest extends PartialSortTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedPartialTopNTest extends PartialTopNTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedPartialTop1Test extends PartialTop1TestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedFilterTest extends FilterTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
class SlottedArgumentTest extends ArgumentTestBase(ENTERPRISE_SINGLE_THREAD, SlottedRuntime, SIZE_HINT)
