/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class ForeachSlottedPipe(lhs: Pipe, rhs: Pipe, innerVariableSlot: Slot, expression: Expression)
                             (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe with ListSupport {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setVariableFun = makeSetValueInSlotFunctionFor(innerVariableSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    input.map {
      outerContext =>
        val values = makeTraversable(expression(outerContext, state)).iterator()
        while (values.hasNext) {
          setVariableFun(outerContext, values.next()) // A slot for the variable has been allocated on the outer context
          val innerState = state.withInitialContext(outerContext)
          rhs.createResults(innerState).length // exhaust the iterator, in case there's a merge read increasing cardinality inside the foreach
        }
        outerContext
    }
  }
}
