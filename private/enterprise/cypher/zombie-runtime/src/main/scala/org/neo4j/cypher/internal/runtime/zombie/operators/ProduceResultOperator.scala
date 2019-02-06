/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ArrayResultExecutionContextFactory
import org.neo4j.internal.kernel.api.IndexReadSession

/**
  * This operator implements both [[StreamingOperator]] and [[ContinuableOperator]] because it
  * can occur both as the start of a pipeline, and as the final operator of a pipeline.
  *
  * @param workIdentity
  * @param slots
  * @param columns
  */
class ProduceResultOperator(val workIdentity: WorkIdentity,
                            slots: SlotConfiguration,
                            columns: Seq[(String, Expression)]) extends StreamingOperator with ContinuableOperator {

  override def toString: String = "ProduceResult"

  override def init(context: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    resources: QueryResources): IndexedSeq[ContinuableInputOperatorTask] =
    Array(new InputOTask(inputMorsel))

  override def init(context: QueryContext,
                    state: QueryState,
                    resources: QueryResources): ContinuableOperatorTask =
    new OutputOTask()

  class InputOTask(val inputMorsel: MorselExecutionContext) extends OTask() with ContinuableInputOperatorTask {

    override def canContinue: Boolean = false // will be true sometimes for reactive results

    override def operate(outputIgnore: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      produceOutput(inputMorsel, context, state, resources)
    }
  }

  class OutputOTask() extends OTask() {

    override def canContinue: Boolean = false // will be true sometimes for reactive results

    override def operate(output: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {
      produceOutput(output, context, state, resources)
    }
  }

  abstract class OTask() extends ContinuableOperatorTask {

    override def toString: String = "ProduceResultTask"

    override def canContinue: Boolean = false // will be true sometimes for reactive results

    protected def produceOutput(output: MorselExecutionContext,
                                context: QueryContext,
                                state: QueryState,
                                resources: QueryResources): Unit = {
      val resultFactory = ArrayResultExecutionContextFactory(columns)
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession])

      // Loop over the rows of the morsel and call the visitor for each one
      while (output.isValidRow) {
        val arrayRow = resultFactory.newResult(output, queryState, queryState.prePopulateResults)
        state.visitor.visit(arrayRow)
        output.moveToNextRow()
      }
    }
  }
}
