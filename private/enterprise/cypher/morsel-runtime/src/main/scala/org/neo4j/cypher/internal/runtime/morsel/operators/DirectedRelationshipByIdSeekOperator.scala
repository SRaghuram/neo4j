/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.Method
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.DirectedRelationshipByIdSeekOperator.asId
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.internal.kernel.api.{IndexReadSession, RelationshipScanCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue

class DirectedRelationshipByIdSeekOperator(val workIdentity: WorkIdentity,
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

      IndexedSeq(new DirectedRelationshipByIdTask(inputMorsel.nextCopy))
  }


  class DirectedRelationshipByIdTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = " DirectedRelationshipByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _
    private var cursor: RelationshipScanCursor = _

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
                                         resources.expressionVariables(state.nExpressionSlots))
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      ids = relId.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = DirectedRelationshipByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      while (outputRow.isValidRow && ids.hasNext) {
        val nextId = asId(ids.next())
        val read = context.transactionalContext.dataRead
        if (nextId >= 0L) {
          read.singleRelationship(nextId, cursor)
          if (cursor.next()) {
            outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
            outputRow.setLongAt(relationship, nextId)
            outputRow.setLongAt(startNode, cursor.sourceNodeReference())
            outputRow.setLongAt(endNode, cursor.targetNodeReference())
            outputRow.moveToNextRow()
          }
        }
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.relationshipScanCursorPool.free(cursor)
      cursor = null
    }
  }
}

object DirectedRelationshipByIdSeekOperator {
  def asId(value: AnyValue): Long = value match {
    case d:IntegralValue => d.longValue()
    case _ => -1L
  }

  val asIdMethod: Method = method[DirectedRelationshipByIdSeekOperator, Long, AnyValue]("asId")
}

