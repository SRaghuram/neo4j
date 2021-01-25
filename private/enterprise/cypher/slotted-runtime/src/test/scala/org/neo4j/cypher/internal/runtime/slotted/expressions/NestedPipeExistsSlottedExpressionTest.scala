/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.slotted.pipes.FakeSlottedPipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NestedPipeExistsSlottedExpressionTest extends CypherFunSuite {
  test("Should close pipe results.") {
    // given
    val state = QueryStateHelper.empty
    val outerSlots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val innerSlots = SlotConfiguration.empty.newLong("a", nullable = false, CTNode)
    val input = FakeSlottedPipe(Seq(Map("a"->10),Map("a"->11)), innerSlots)
    val npee = NestedPipeExistsSlottedExpression(input, innerSlots, Array(), Id(0))
    // when
    val outerRow = FakeSlottedPipe(Seq(Map("x" -> 42)), outerSlots).createResults(state).next()
    npee.apply(outerRow, state)
    // then
    input.wasClosed shouldBe true
  }
}
