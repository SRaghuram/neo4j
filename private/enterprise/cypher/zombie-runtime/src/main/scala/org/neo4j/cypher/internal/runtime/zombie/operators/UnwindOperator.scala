/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => InterpretedQueryState}
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue

class UnwindOperator(val workIdentity: WorkIdentity,
                     collection: Expression,
                     offset: Int)
  extends StreamingOperator with ListSupport {

  override def nextTasks(context: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    IndexedSeq(new OTask(inputMorsel.nextCopy))
  }

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    private var unwoundValues: java.util.Iterator[AnyValue] = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): Boolean = {

      val queryState = new InterpretedQueryState(context,
                                                 resources = null,
                                                 params = state.params,
                                                 resources.expressionCursors,
                                                 Array.empty[IndexReadSession],
                                                 resources.expressionVariables(state.nExpressionSlots))

      val value = collection(inputMorsel, queryState)
      unwoundValues = makeTraversable(value).iterator
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {
      while (unwoundValues.hasNext && outputRow.isValidRow) {
        val thisValue = unwoundValues.next()
        outputRow.copyFrom(inputMorsel)
        outputRow.setRefAt(offset, thisValue)
        outputRow.moveToNextRow()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      unwoundValues = null
    }

    override def canContinue: Boolean = unwoundValues != null || inputMorsel.isValidRow
  }
}
