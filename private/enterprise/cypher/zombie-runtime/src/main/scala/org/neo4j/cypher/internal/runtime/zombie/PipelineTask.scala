/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.zombie.operators.{ContinuableInputOperatorTask, ContinuableOperatorTask, StatelessOperator}
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}

/**
  * The [[Task]] of executing a [[ExecutablePipeline]] once.
  *
  * @param start  task for executing the start operator
  * @param state  the current QueryState
  */
case class PipelineTask(start: ContinuableInputOperatorTask,
                        middleOperators: Seq[StatelessOperator],
                        produceResult: ContinuableOperatorTask,
                        queryContext: QueryContext,
                        state: QueryState,
                        pipeline: ExecutablePipeline)
  extends Task[QueryResources] {

  override final def executeWorkUnit(resources: QueryResources, output: MorselExecutionContext): Unit = {
    try {
      state.transactionBinder.bindToThread(queryContext.transactionalContext.transaction)
      doExecuteWorkUnit(resources, output)
    } finally {
      state.transactionBinder.unbindFromThread()
    }
  }

  private def doExecuteWorkUnit(resources: QueryResources,
                                output: MorselExecutionContext): Unit = {
    start.operate(output, queryContext, state, resources)
    for (op <- middleOperators) {
      output.resetToFirstRow()
      op.operate(output, queryContext, state, resources)
    }
    if (produceResult != null) {
      output.resetToFirstRow()
      produceResult.operate(output, queryContext, state, resources)
    }
  }

  override def workId: Int = -1

  override def workDescription: String = "not implemented"

  override def canContinue: Boolean = start.canContinue || (produceResult != null && produceResult.canContinue)
}
