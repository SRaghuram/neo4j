/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.internal.kernel.api.IndexReadSession

/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperatorNoGrouping(val workIdentity: WorkIdentity,
                                          aggregations: Array[AggregationOffsets]) extends EagerReduceOperator {

  override def init(queryContext: QueryContext,
                    state: QueryState,
                    inputMorsels: Seq[MorselExecutionContext],
                    resources: QueryResources): ContinuableOperatorTask =
    new OTask(inputMorsels.toArray)

  class OTask(inputMorsels: Array[MorselExecutionContext]) extends ContinuableOperatorTask {

    override def operate(currentRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      val queryState = new OldQueryState(context,
        resources = null,
        params = state.params,
        resources.expressionCursors,
        Array.empty[IndexReadSession])

      val reducers = aggregations.map(_.createReducer)

      //Go through the morsels and collect the output from the map step
      //and reduce the values
      var i = 0
      while (i < inputMorsels.length) {
        val currentInputRow = inputMorsels(i)
        var j = 0
        while (j < aggregations.length) {
          reducers(j).apply(currentInputRow, queryState)
          j += 1
        }
        i += 1
      }

      //Write the reduced value to output
      i = 0
      while (i < aggregations.length) {
        currentRow.setRefAt(aggregations(i).reducerOutputSlot, reducers(i).result(queryState))
        i += 1
      }
      currentRow.moveToNextRow()
      currentRow.finishedWriting()
    }

    // This operator will never continue since it will always write a single row
    override def canContinue: Boolean = false
  }
}
