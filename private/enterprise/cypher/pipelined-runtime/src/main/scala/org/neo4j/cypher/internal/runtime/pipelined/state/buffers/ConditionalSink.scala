/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.values.storable.Values

/**
 * A conditional sink delegates to one of two inner sinks depending on a predicate.
 *
 *             .--onTrue->
 *            /
 * -INCOMING->
 *            \
 *             -onFalse->
 *
 * @param predicate The predicate to evaluate.
 * @param onTrue The sink where data is put if predicate is `true`
 * @param onFalse The sink where data is put if predicate is `false`
 * @param queryState used for evaluating the predicate.
 */
class ConditionalSink(predicate: Expression,
                      onTrue: Sink[Morsel],
                      onFalse: Sink[Morsel],
                      queryState: PipelinedQueryState) extends Sink[Morsel] {

  override def toString: String = s"${getClass.getSimpleName}"

  override def put(morsel: Morsel, resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- $morsel")
    }
    val expressionState = queryState.queryStateForExpressionEvaluation(resources)
    if (morsel.hasData) {
      val onTrueMorsel = morsel.filteringShallowCopy()
      val onFalseMorsel = morsel.filteringShallowCopy()
      val readCursor = morsel.readCursor()
      while (readCursor.next()) {
        if (predicate.apply(readCursor, expressionState) eq Values.TRUE) {
          onFalseMorsel.cancelRow(readCursor.row)
        } else {
          onTrueMorsel.cancelRow(readCursor.row)
        }
      }
      //NOTE: morsel has the slot configuration of the lhs, so if the lhs slot configuration is not a
      //      strict prefix of the rhs slot configuration things might break in horrible ways
      onTrue.put(onTrueMorsel, resources)
      onFalse.put(onFalseMorsel, resources)
    }
  }

  override def canPut: Boolean = onTrue.canPut && onFalse.canPut
}
