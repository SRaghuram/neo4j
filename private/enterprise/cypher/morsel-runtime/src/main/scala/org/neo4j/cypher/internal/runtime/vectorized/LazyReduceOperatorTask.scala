/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import java.util

import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext

import scala.collection.mutable.ArrayBuffer

abstract class LazyReduceOperatorTask(messageQueue: util.Queue[MorselExecutionContext], collector: LazyReduceCollector) {

  private var processedMorsels = ArrayBuffer[MorselExecutionContext]()

  /**
    * Operates on all available morsels from the queue.
    *
    * @return all processed morsels
    */
  def operate(context: QueryContext,
              state: QueryState,
              cursors: ExpressionCursors): IndexedSeq[MorselExecutionContext] = {
    // Outer loop until trySetTaskDone succeeds
    do {
      // Inner loop until there is currently no more data
      var currentRow = messageQueue.poll()
      while (currentRow != null) {
        operateSingleMorsel(context, state, currentRow)
        processedMorsels += currentRow
        currentRow = messageQueue.poll()
      }
    } while (!collector.trySetTaskDone(this, processedMorsels.length))
    processedMorsels
  }

  /**
    * Process a single morsel. This function is supposed to have side effects, or change the morsel in place.
    *
    * @param currentRow the morsel
    */
  def operateSingleMorsel(context: QueryContext,
                          state: QueryState,
                          currentRow: MorselExecutionContext): Unit
}
