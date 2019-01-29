/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE_EDITION
import org.neo4j.cypher.internal.runtime.spec.slotted.SlottedSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.tests.{AggregationTestBase, AllNodeScanTestBase, ExpandAllTestBase, ExpandAllWithOptionalTestBase, InputTestBase, LabelScanTestBase, NodeIndexScanTestBase, NodeIndexSeekRangeAndCompositeTestBase, NodeIndexSeekTestBase, NodeLockingUniqueIndexSeekTestBase}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, SlottedRuntime}

object SlottedSpecSuite {
  val SIZE_HINT = 200
}

class SlottedAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_EDITION, SlottedRuntime, SIZE_HINT)
class SlottedAggregationTest extends AggregationTestBase(ENTERPRISE_EDITION, SlottedRuntime, SIZE_HINT)
class SlottedExpandAllTest extends ExpandAllTestBase(ENTERPRISE_EDITION, SlottedRuntime, SIZE_HINT)
                           with ExpandAllWithOptionalTestBase[EnterpriseRuntimeContext]
class SlottedLabelScanTest extends LabelScanTestBase(ENTERPRISE_EDITION, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE_EDITION, SlottedRuntime, SIZE_HINT)
class SlottedNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE_EDITION, SlottedRuntime, SIZE_HINT)
                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                               with NodeLockingUniqueIndexSeekTestBase[EnterpriseRuntimeContext]
class SlottedInputTest extends InputTestBase(ENTERPRISE_EDITION, SlottedRuntime, SIZE_HINT)
