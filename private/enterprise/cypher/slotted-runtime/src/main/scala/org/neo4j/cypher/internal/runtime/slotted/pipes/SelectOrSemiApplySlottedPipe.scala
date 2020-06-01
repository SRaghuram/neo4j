/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

case class SelectOrSemiApplySlottedPipe(lhs: Pipe,
                                        rhs: Pipe,
                                        predicate: Expression,
                                        negated: Boolean,
                                        slots: SlotConfiguration)
                                       (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe {

  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {
   input.filter {
      row =>
        (predicate.apply(row, state) eq Values.TRUE) || {
          val rhsState = state.withInitialContext(row)
          val innerResult = rhs.createResults(rhsState)
          if (negated) !innerResult.hasNext else innerResult.hasNext
        }
    }.flatMap {
      row =>
        val output = SlottedRow(slots)
        row.copyTo(output)
        Iterator.single(output)
    }
  }
}
