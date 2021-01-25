/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionSlottedPipeTest extends CypherFunSuite {
  test("Close should close RHS and LHS.") {
    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)

    val lhs = FakeSlottedPipe(Seq(Map("a" -> 10), Map("a" -> 11)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("b" -> 20), Map("b" -> 21)), slots)
    val mapping = SlottedPipeMapper.computeUnionRowMapping(slots, slots)
    val pipe = UnionSlottedPipe(lhs, rhs, slots, mapping, mapping)()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next()
    result.close()

    lhs.wasClosed shouldBe true
    rhs.wasClosed shouldBe true
  }
}
