/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ApplySlottedPipeTest extends CypherFunSuite {
  test("Close should close current RHS and LHS.") {
    val slots = SlotConfiguration.empty
      .newLong("a", nullable = false, CTNode)
      .newLong("b", nullable = false, CTNode)

    val lhs = FakeSlottedPipe(Seq(Map("a"->10),Map("a"->11)), slots)
    val rhs = FakeSlottedPipe(Seq(Map("b"->20),Map("b"->21)), slots)
    val pipe = ApplySlottedPipe(lhs, rhs)()
    val result = pipe.createResults(QueryStateHelper.empty)
    result.next() // First row
    val firstRhs = rhs.currentIterator
    result.next() // Second row
    result.next() // Third row. First RHS should be exhausted and closed by now
    lhs.wasClosed shouldBe false
    firstRhs.wasClosed shouldBe true

    val secondRhs = rhs.currentIterator
    result.next() // Fourth row
    result.hasNext shouldBe false // Make sure to exhaust
    lhs.wasClosed shouldBe true
    secondRhs.wasClosed shouldBe true
  }
}
