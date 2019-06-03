/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.internal.kernel.api.{IndexOrder, IndexReadSession, NodeValueIndexCursor}

class NodeIndexScanOperator(val workIdentity: WorkIdentity,
                            nodeOffset: Int,
                            properties: Array[SlottedIndexedProperty],
                            queryIndexId: Int,
                            indexOrder: IndexOrder,
                            argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, properties) {

  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    val indexSession = state.queryIndexes(queryIndexId)
    IndexedSeq(new OTask(inputMorsel.nextCopy, indexSession))
  }

  class OTask(val inputMorsel: MorselExecutionContext, index: IndexReadSession) extends InputLoopTask {

    override def workIdentity: WorkIdentity = NodeIndexScanOperator.this.workIdentity

    private var cursor: NodeValueIndexCursor = _

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): Boolean = {

      cursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
      val read = context.transactionalContext.dataRead
      read.nodeIndexScan(index, cursor, indexOrder, needsValues)
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputMorsel, outputRow, cursor, argumentSize)
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeValueIndexCursorPool.free(cursor)
      cursor = null
    }
  }
}
