/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SeekArgs
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue


class NodeByIdSeekOperator(val workIdentity: WorkIdentity,
                           offset: Int,
                           nodeIdsExpr: SeekArgs,
                           argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

      IndexedSeq(new NodeByIdTask(inputMorsel.nextCopy))
  }


  class NodeByIdTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "NodeByIdTask"

    private var ids: java.util.Iterator[AnyValue] = _


    /**
      * Initialize the inner loop for the current input row.
      *
      * @return true iff the inner loop might result it output rows
      */
    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots),
                                         state.subscriber)
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      ids = nodeIdsExpr.expressions(initExecutionContext, queryState).iterator()
      true
    }

    override def workIdentity: WorkIdentity = NodeByIdSeekOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      while (outputRow.isValidRow && ids.hasNext) {
        val nextId = asId(ids.next())
        if (nextId >= 0L && context.transactionalContext.dataRead.nodeExists(nextId)) {
          outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
          outputRow.setLongAt(offset, nextId)
          outputRow.moveToNextRow()
        }
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
     //nothing to do here
    }

    private def asId(value: AnyValue): Long = value match {
      case d:IntegralValue => d.longValue()
      case _ => -1L
    }
  }
}


