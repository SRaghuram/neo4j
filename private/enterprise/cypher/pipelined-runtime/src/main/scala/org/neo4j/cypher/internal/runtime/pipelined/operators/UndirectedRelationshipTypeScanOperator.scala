/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel.UNKNOWN
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyType
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.schema.IndexOrder

class UndirectedRelationshipTypeScanOperator(val workIdentity: WorkIdentity,
                                             relOffset: Int,
                                             startOffset: Int,
                                             typ: LazyType,
                                             endOffset: Int,
                                             argumentSize: SlotConfiguration.Size)
  extends StreamingOperator {

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    singletonIndexedSeq(new UndirectedTypeScanTask(inputMorsel.nextCopy))
  }

  class UndirectedTypeScanTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = UndirectedRelationshipTypeScanOperator.this.workIdentity

    override def toString: String = "UndirectedRelationshipTypeScanTask"

    private var typeIndexCursor: RelationshipTypeIndexCursor = _
    private var relationshipCursor: RelationshipScanCursor = _
    private var forwardDirection = true

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      val id = typ.getId(state.queryContext)
      if (id == UNKNOWN) false
      else {
        typeIndexCursor = resources.cursorPools.relationshipTypeIndexCursorPool.allocateAndTrace()
        relationshipCursor = resources.cursorPools.relationshipScanCursorPool.allocateAndTrace()
        val read = state.queryContext.transactionalContext.dataRead
        read.relationshipTypeScan(id, typeIndexCursor, IndexOrder.NONE)
        true
      }
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      val read = state.queryContext.transactionalContext.dataRead
      while (outputRow.onValidRow() &&  (!forwardDirection || typeIndexCursor.next())) {
        val currentRel = typeIndexCursor.relationshipReference()
        outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setLongAt(relOffset, currentRel)
        if (forwardDirection) {
          read.singleRelationship(currentRel, relationshipCursor)
          require(relationshipCursor.next())
          outputRow.setLongAt(startOffset, relationshipCursor.sourceNodeReference())
          outputRow.setLongAt(endOffset, relationshipCursor.targetNodeReference())
          forwardDirection = false
        } else {
          outputRow.setLongAt(startOffset, relationshipCursor.targetNodeReference())
          outputRow.setLongAt(endOffset, relationshipCursor.sourceNodeReference())
          forwardDirection = true
        }
        outputRow.next()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (typeIndexCursor != null) {
        typeIndexCursor.setTracer(event)
      }
      if (relationshipCursor != null) {
        relationshipCursor.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.relationshipTypeIndexCursorPool.free(typeIndexCursor)
      typeIndexCursor = null
      resources.cursorPools.relationshipScanCursorPool.free(relationshipCursor)
      relationshipCursor = null
    }
  }
}


