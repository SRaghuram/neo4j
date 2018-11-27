/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.Slot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ListSupport}
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.JavaConverters._

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
  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.map {
      (outerContext) =>
        val values = makeTraversable(expression(outerContext, state))
        values.iterator().asScala.foreach { v =>
          setVariableFun(outerContext, v) // A slot for the variable has been allocated on the outer context
          val innerState = state.withInitialContext(outerContext)
          rhs.createResults(innerState).length // exhaust the iterator, in case there's a merge read increasing cardinality inside the foreach
        }
        outerContext
    }
  }
}
