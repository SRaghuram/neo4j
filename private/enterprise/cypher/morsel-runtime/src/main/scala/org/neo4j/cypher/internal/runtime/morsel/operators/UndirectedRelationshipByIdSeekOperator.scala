/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.UndirectedRelationshipByIdSeekOperator.asId
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.internal.kernel.api.{IndexReadSession, RelationshipScanCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue

class UndirectedRelationshipByIdSeekOperator(val workIdentity: WorkIdentity,
                                           relationship: Int,
                                           startNode: Int,
                                           endNode: Int,
                                           relId: SeekArgs,
                                           argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

      IndexedSeq(new UndirectedRelationshipByIdTask(inputMorsel.nextCopy))
  }


  class UndirectedRelationshipByIdTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "UndirectedRelationshipByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _
    private var cursor: RelationshipScanCursor = _
    private var progress = true

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      cursor = resources.cursorPools.relationshipScanCursorPool.allocate()
      val queryState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      ids = relId.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = UndirectedRelationshipByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      while (outputRow.isValidRow && (!progress || ids.hasNext)) {
        if (progress) {
          val nextId = asId(ids.next())
          if (nextId >= 0L) {
            context.transactionalContext.dataRead.singleRelationship(nextId, cursor)
            if (cursor.next()) {
              outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
              outputRow.setLongAt(relationship, nextId)
              outputRow.setLongAt(startNode, cursor.sourceNodeReference())
              outputRow.setLongAt(endNode, cursor.targetNodeReference())
              outputRow.moveToNextRow()
              progress = false
            }
          }
        } else {
          outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
          outputRow.setLongAt(relationship, cursor.relationshipReference())
          outputRow.setLongAt(startNode, cursor.targetNodeReference())
          outputRow.setLongAt(endNode, cursor.sourceNodeReference())
          outputRow.moveToNextRow()
          progress = true
        }
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.relationshipScanCursorPool.free(cursor)
      cursor = null
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit =  {
      if (cursor != null) {
        cursor.setTracer(event)
      }
    }
  }
}

object UndirectedRelationshipByIdSeekOperator {
  def asId(value: AnyValue): Long = value match {
    case d:IntegralValue => d.longValue()
    case _ => -1L
  }

  val asIdMethod: Method = method[UndirectedRelationshipByIdSeekOperator, Long, AnyValue]("asId")
}
