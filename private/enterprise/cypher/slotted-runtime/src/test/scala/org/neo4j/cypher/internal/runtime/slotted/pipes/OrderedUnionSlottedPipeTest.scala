/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.slotted.Ascending
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContextOrdering
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OrderedUnionSlottedPipeTest extends CypherFunSuite {
  test("Close should close RHS and LHS.") {
    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)

    val lhs = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11), Map("a" -> 25)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("a" -> 20), Map("a" -> 21), Map("a" -> 26)), slots)
    val mapping = SlottedPipeMapper.computeUnionRowMapping(slots, slots)
    val pipe = OrderedUnionSlottedPipe(lhs, rhs, slots, mapping, mapping, SlottedExecutionContextOrdering.asComparator(List(Ascending(slots("a")))))()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next()
    result.close()

    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
  }
}
