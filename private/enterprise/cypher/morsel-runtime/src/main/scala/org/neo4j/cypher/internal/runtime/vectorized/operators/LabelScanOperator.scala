/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor

class LabelScanOperator(val workIdentity: WorkIdentity,
                        offset: Int,
                        label: LazyLabel,
                        argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperator[NodeLabelIndexCursor](offset) {

  override def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext, cursors: ExpressionCursors): ContinuableOperatorTask = {
    new OTask(inputMorsel)
  }

  class OTask(val inputRow: MorselExecutionContext) extends StreamingContinuableOperatorTask {

    private var cursor: NodeLabelIndexCursor = _

    override protected def initializeInnerLoop(inputRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): AutoCloseable = {
      cursor = context.transactionalContext.cursors.allocateNodeLabelIndexCursor()
      val read = context.transactionalContext.dataRead
      val maybeLabelId = label.getOptId(context)
      if (maybeLabelId.isDefined)
        read.nodeLabelScan(maybeLabelId.get.id, cursor)
      cursor
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputRow, outputRow, cursor, argumentSize)
    }
  }
}
