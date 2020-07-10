/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.virtual.VirtualValues

case class RollUpApplySlottedPipe(lhs: Pipe, rhs: Pipe,
                                  collectionRefSlotOffset: Int,
                                  identifierToCollect: (String, Expression),
                                  slots: SlotConfiguration)
                                 (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) {

  private val getValueToCollectFunction = {
    val expression: Expression = identifierToCollect._2
    state: QueryState => (ctx: CypherRow) => expression(ctx, state)
  }

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    input.map {
      ctx =>
        val innerState = state.withInitialContext(ctx)
        val innerResults: Iterator[CypherRow] = rhs.createResults(innerState)
        val collection = VirtualValues.list(innerResults.map(getValueToCollectFunction(state)).toArray: _*)
        ctx.setRefAt(collectionRefSlotOffset, collection)
        ctx
    }
  }
}
