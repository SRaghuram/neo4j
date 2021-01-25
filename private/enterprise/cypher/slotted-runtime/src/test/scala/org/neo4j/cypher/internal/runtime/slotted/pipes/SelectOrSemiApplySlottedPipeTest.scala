/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class SelectOrSemiApplySlottedPipeTest extends CypherFunSuite {
  test("Each row should immediately close RHS. Exhaust should close LHS.") {
    val monitor = QueryStateHelper.trackClosedMonitor
    val resourceManager = new ResourceManager(monitor)
    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
    val lhs = FakeSlottedPipe(Seq(Map("a"->10),Map("a"->11)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("b"->20),Map("b"->21)), slots)
    val pipe = SelectOrSemiApplySlottedPipe(lhs, rhs, Literal(Values.booleanValue(false)), negated = false, slots)()
    val result = pipe.createResults(QueryStateHelper.emptyWithResourceManager(resourceManager))
    result.next() // First row
    lhs.wasClosed shouldBe false
    rhs.currentIterator.wasClosed shouldBe true

    result.next() // Second row
    result.hasNext shouldBe false // Make sure to exhaust
    lhs.wasClosed shouldBe true
    rhs.currentIterator.wasClosed shouldBe true
  }
}
