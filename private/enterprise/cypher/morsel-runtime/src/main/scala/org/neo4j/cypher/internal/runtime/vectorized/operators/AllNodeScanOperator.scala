/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeCursor

class AllNodeScanOperator(offset: Int, argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext): ContinuableOperatorTask = {
    val nodeCursor = queryContext.transactionalContext.cursors.allocateNodeCursor()
    queryContext.transactionalContext.dataRead.allNodesScan(nodeCursor)
    new OTask(nodeCursor, inputMorsel)
  }

  class OTask(var nodeCursor: NodeCursor, argument: MorselExecutionContext) extends ContinuableOperatorTask {

    var cursorHasMore = true

    override def operate(currentRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {

      while (currentRow.hasMoreRows && cursorHasMore) {
        cursorHasMore = nodeCursor.next()
        if (cursorHasMore) {
          currentRow.copyFrom(argument, argumentSize.nLongs, argumentSize.nReferences)
          currentRow.setLongAt(offset, nodeCursor.nodeReference())
          currentRow.moveToNextRow()
        }
      }

      currentRow.finishedWriting()

      if (!cursorHasMore) {
        if (nodeCursor != null) {
          nodeCursor.close()
          nodeCursor = null
        }
      }
    }

    override def canContinue: Boolean = cursorHasMore
  }
}
