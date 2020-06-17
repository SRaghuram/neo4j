/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.FakeEntityTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeTestSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.expressions.ReferenceFromSlot
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RollUpApplySlottedPipeTest extends CypherFunSuite with PipeTestSupport with FakeEntityTestSupport {
  private val slots = SlotConfiguration
    .empty
    .newReference("a", nullable = true, CTNumber)
    .newReference("x", nullable = false, CTList(CTNumber)) // NOTE: This has to be last since that is the order in which the slots are assumed to be allocated

  private val collectionRefSlotOffset = slots.getReferenceOffsetFor("x")

  test("should set the QueryState when calling down to the RHS") {
    // given
    val lhs = createLhs(1)
    val rhs = mock[Pipe]
    when(rhs.createResults(any())).then(new Answer[Iterator[CypherRow]] {
      override def answer(invocation: InvocationOnMock) = {
        val state = invocation.getArguments.apply(0).asInstanceOf[QueryState]
        state.initialContext should not be empty
        Iterator.empty
      }
    })
    val pipe = RollUpApplySlottedPipe(lhs, rhs, collectionRefSlotOffset,
      identifierToCollect = "y" -> ReferenceFromSlot(0), slots)()

    // when
    pipe.createResults(QueryStateHelper.empty).toList

    // then should not throw exception
  }

  private def createLhs(data: Any*) = {
    val lhsData = data.map(v => Map[Any, Any]("a" -> v))
    FakeSlottedPipe(lhsData, slots)
  }
}

