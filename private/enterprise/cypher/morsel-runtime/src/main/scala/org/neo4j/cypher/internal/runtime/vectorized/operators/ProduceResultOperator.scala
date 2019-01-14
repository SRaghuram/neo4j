/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import java.util

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.ArrayResultExecutionContextFactory
import org.neo4j.cypher.internal.runtime.vectorized.QueryResources
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.IndexReadSession

class ProduceResultOperator(val workIdentity: WorkIdentity, slots: SlotConfiguration, columns: Seq[(String, Expression)]) extends LazyReduceOperator {

  override def init(context: QueryContext,
                    state: QueryState,
                    messageQueue: util.Queue[MorselExecutionContext],
                    collector: LazyReduceCollector,
                    resources: QueryResources): LazyReduceOperatorTask = new OTask(messageQueue, collector, resources)

  class OTask(messageQueue: util.Queue[MorselExecutionContext], collector: LazyReduceCollector, resources: QueryResources)
    extends LazyReduceOperatorTask(messageQueue, collector) {

    override def operateSingleMorsel(context: QueryContext,
                                     state: QueryState,
                                     currentRow: MorselExecutionContext): Unit = {
      val resultFactory = ArrayResultExecutionContextFactory(columns)
      val queryState = new OldQueryState(context,
        resources = null,
        params = state.params,
        resources.expressionCursors,
        Array.empty[IndexReadSession])

      // Loop over the rows of the morsel and call the visitor for each one
      while (currentRow.isValidRow) {
        val arrayRow = resultFactory.newResult(currentRow, queryState, queryState.prePopulateResults)
        state.visitor.visit(arrayRow)
        currentRow.moveToNextRow()
      }
    }
  }

}
