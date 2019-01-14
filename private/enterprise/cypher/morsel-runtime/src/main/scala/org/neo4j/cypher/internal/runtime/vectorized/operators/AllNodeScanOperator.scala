/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Scan

class AllNodeScanOperator(val workIdentity: WorkIdentity,
                          offset: Int,
                          argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    resources: QueryResources): IndexedSeq[ContinuableOperatorTask] = {

    if (state.singeThreaded) {
      // Single threaded scan
      IndexedSeq(new SingleThreadedScanTask(inputMorsel))
    } else {
      // Parallel scan
      val scan = queryContext.transactionalContext.dataRead.allNodesScan()
      val tasks = new Array[ContinuableOperatorTask](state.numberOfWorkers)
      for (i <- 0 until state.numberOfWorkers) {
        // Each task gets its own cursor which is reuses until it's done.
        val cursor = resources.cursorPools.nodeCursorPool.allocate()
        val rowForTask = inputMorsel.shallowCopy()
        tasks(i) = new ParallelScanTask(rowForTask, scan, cursor, state.morselSize)
      }
      tasks
    }
  }

  /**
    * A [[SingleThreadedScanTask]] will iterate over all inputRows and do a full scan for each of them.
    *
    * @param inputRow the input row, pointing to the beginning of the input morsel
    */
  class SingleThreadedScanTask(val inputRow: MorselExecutionContext) extends StreamingContinuableOperatorTask {
    private var cursor: NodeCursor = _

    override protected def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean = {
      cursor = resources.cursorPools.nodeCursorPool.allocate()
      context.transactionalContext.dataRead.allNodesScan(cursor)
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      while (outputRow.isValidRow && cursor.next()) {
        outputRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, cursor.nodeReference())
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeCursorPool.free(cursor)
      cursor = null
    }
  }

  /**
    * A [[ParallelScanTask]] reserves new batches from the Scan, until there are no more batches. It competes for these batches with other
    * concurrently running [[ParallelScanTask]]s.
    *
    * For each batch, it process all the nodes and combines them with each input row.
    */
  class ParallelScanTask(val inputRow: MorselExecutionContext,
                         scan: Scan[NodeCursor],
                         val cursor: NodeCursor,
                         val batchSizeHint: Int) extends ContinuableOperatorTask {

    private var _canContinue: Boolean = true
    private var deferredRow: Boolean = false

    /**
      * These 2 lines make sure that the first call to [[next]] is correct.
      */
    scan.reserveBatch(cursor, batchSizeHint)
    inputRow.setToAfterLastRow()

    override def operate(outputRow: MorselExecutionContext, context: QueryContext, queryState: QueryState, resources: QueryResources): Unit = {
      while (next(queryState) && outputRow.isValidRow) {
        outputRow.copyFrom(inputRow, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(offset, cursor.nodeReference())
        outputRow.moveToNextRow()
      }

      if (!outputRow.isValidRow && _canContinue) {
        deferredRow = true
      }

      outputRow.finishedWriting()
    }


    private def next(queryState: QueryState): Boolean = {
      while (true) {
        if (deferredRow) {
          deferredRow = false
          return true
        } else if (inputRow.hasNextRow) {
          inputRow.moveToNextRow()
          return true
        } else if (cursor.next()) {
          inputRow.resetToBeforeFirstRow()
        } else if (scan.reserveBatch(cursor, batchSizeHint)) {
          // Do nothing
        } else {
          // We ran out of work
          cursor.close()
          _canContinue = false
          return false
        }
      }

      throw new IllegalStateException("Unreachable code")
    }

    override def canContinue: Boolean = _canContinue
  }

}
