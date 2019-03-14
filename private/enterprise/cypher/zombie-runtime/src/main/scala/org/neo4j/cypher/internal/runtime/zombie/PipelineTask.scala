/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.zombie.operators.{ContinuableOperatorTask, OperatorTask}

/**
  * The [[Task]] of executing a [[ExecutablePipeline]] once.
  *
  * @param startTask  task for executing the start operator
  * @param state  the current QueryState
  */
case class PipelineTask(startTask: ContinuableOperatorTask,
                        middleTasks: Seq[OperatorTask],
                        produceResult: ContinuableOperatorTask,
                        queryContext: QueryContext,
                        state: QueryState,
                        pipelineState: PipelineState)
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
    startTask.operate(output, queryContext, state, resources)
    for (op <- middleTasks) {
      output.resetToFirstRow()
      op.operate(output, queryContext, state, resources)
    }
    if (produceResult != null) {
      output.resetToFirstRow()
      produceResult.operate(output, queryContext, state, resources)
    }
  }

  def close(): Unit = {
    startTask.close(pipelineState)
  }

  override def workId: Int = pipelineState.pipeline.workId

  override def workDescription: String = pipelineState.pipeline.workDescription

  override def canContinue: Boolean = startTask.canContinue || (produceResult != null && produceResult.canContinue)
}
