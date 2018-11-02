/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyLabel, ExpressionCursors}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor

class LabelScanOperator(offset: Int, label: LazyLabel, argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperator[NodeLabelIndexCursor](offset) {

  override def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext, cursors: ExpressionCursors): ContinuableOperatorTask = {
    val cursor = context.transactionalContext.cursors.allocateNodeLabelIndexCursor()
    val read = context.transactionalContext.dataRead
    val labelId = label.getOptId(context)
    read.nodeLabelScan(labelId.get.id, cursor)
    new OTask(cursor)
  }

  class OTask(nodeCursor: NodeLabelIndexCursor) extends ContinuableOperatorTask {

    var hasMore = false
    override def operate(currentRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit = {
      hasMore = iterate(currentRow, nodeCursor, argumentSize)
    }

    override def canContinue: Boolean = hasMore
  }
}
