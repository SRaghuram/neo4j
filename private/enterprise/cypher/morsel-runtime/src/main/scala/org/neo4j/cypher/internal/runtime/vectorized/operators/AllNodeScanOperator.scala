/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeCursor

class AllNodeScanOperator(val workIdentity: WorkIdentity,
                          offset: Int,
                          argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    cursors: ExpressionCursors): ContinuableOperatorTask = {
    new OTask(inputMorsel)
  }

  class OTask(val inputRow: MorselExecutionContext) extends StreamingContinuableOperatorTask {

    var nodeCursor: NodeCursor = _

    protected override def initializeInnerLoop(inputRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): AutoCloseable = {
      nodeCursor = context.transactionalContext.cursors.allocateNodeCursor()
      context.transactionalContext.dataRead.allNodesScan(nodeCursor)
      nodeCursor
    }

    override def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      while (outputRow.hasMoreRows && nodeCursor.next()) {
        outputRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, nodeCursor.nodeReference())
        outputRow.moveToNextRow()
      }
    }
  }
}
