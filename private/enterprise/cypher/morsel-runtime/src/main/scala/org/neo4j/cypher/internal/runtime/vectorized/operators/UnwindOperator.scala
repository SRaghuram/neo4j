/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => InterpretedQueryState}
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue

class UnwindOperator(val workIdentity: WorkIdentity,
                     collection: Expression,
                     offset: Int)
  extends StreamingOperator with ListSupport {

  override def init(context: QueryContext,
                    state: QueryState,
                    inputRow: MorselExecutionContext,
                    resources: QueryResources): ContinuableOperatorTask = {
    new OTask(inputRow, null)
  }

  class OTask(var inputRow: MorselExecutionContext,
              var unwoundValues: java.util.Iterator[AnyValue]
             ) extends ContinuableOperatorTask {

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      val queryState = new InterpretedQueryState(context,
                                                 resources = null,
                                                 params = state.params,
                                                 resources.expressionCursors,
                                                 Array.empty[IndexReadSession])

      while (inputRow.hasMoreRows && outputRow.hasMoreRows) {
        if (unwoundValues == null) {
          val value = collection(inputRow, queryState)
          unwoundValues = makeTraversable(value).iterator
        }

        while (unwoundValues.hasNext && outputRow.hasMoreRows) {
          val thisValue = unwoundValues.next()
          outputRow.copyFrom(inputRow)
          outputRow.setRefAt(offset, thisValue)
          outputRow.moveToNextRow()
        }

        if (!unwoundValues.hasNext) {
          inputRow.moveToNextRow()
          unwoundValues = null
        }
      }

      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = unwoundValues != null || inputRow.hasMoreRows
  }
}
