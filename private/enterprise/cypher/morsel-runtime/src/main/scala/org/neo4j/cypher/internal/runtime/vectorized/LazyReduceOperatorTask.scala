/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext


abstract class LazyReduceOperatorTask(messageQueue: util.Queue[MorselExecutionContext], collector: LazyReduceCollector) extends ContinuableOperatorTask {

  private var processedMorsels = 0

  // TODO we don't really need outputRow. It's an unnecessary empty morsel.
  override def operate(outputRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       cursors: ExpressionCursors): Unit = {
    // Outer loop until trySetTaskDone succeeds
    do {
      // Inner loop until there is currently no more data
      var currentRow = messageQueue.poll()
      while (currentRow != null) {
        processedMorsels += 1
        operateSingleMorsel(context, state, currentRow)
        currentRow = messageQueue.poll()
      }
    } while(!collector.trySetTaskDone(this, processedMorsels))
  }

  /**
    * Process a single morsel. This function is supposed to have side effects.
    * @param currentRow the morsel
    */
  def operateSingleMorsel(context: QueryContext,
                          state: QueryState,
                          currentRow: MorselExecutionContext): Unit

  override val canContinue: Boolean = false
}
