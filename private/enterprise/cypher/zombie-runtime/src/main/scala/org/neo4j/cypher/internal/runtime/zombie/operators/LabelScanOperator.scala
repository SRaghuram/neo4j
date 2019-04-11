/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor

class LabelScanOperator(val workIdentity: WorkIdentity,
                        offset: Int,
                        label: LazyLabel,
                        argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperator[NodeLabelIndexCursor](offset) {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    // Single threaded scan
    IndexedSeq(new SingleThreadedScanTask(inputMorsel.nextCopy))
  }

  /**
    * A [[SingleThreadedScanTask]] will iterate over all inputRows and do a full scan for each of them.
    *
    * @param inputMorsel the input row, pointing to the beginning of the input morsel
    */
  class SingleThreadedScanTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "LabelScanSerialTask"

    private var cursor: NodeLabelIndexCursor = _

    override protected def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean = {
      label.getOptId(context) match {
        case Some(id) =>
          cursor = resources.cursorPools.nodeLabelIndexCursorPool.allocate()
          val read = context.transactionalContext.dataRead
          read.nodeLabelScan(id, cursor)
          true

        case _ =>
          false // No data to iterate
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      while (outputRow.isValidRow && cursor.next()) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, cursor.nodeReference())
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeLabelIndexCursorPool.free(cursor)
      cursor = null
    }
  }
}
