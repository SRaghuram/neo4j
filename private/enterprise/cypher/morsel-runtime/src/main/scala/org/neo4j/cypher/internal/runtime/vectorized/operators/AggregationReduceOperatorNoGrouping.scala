/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.vectorized._

/*
Responsible for reducing the output of AggregationMapperOperatorNoGrouping
 */
class AggregationReduceOperatorNoGrouping(val workIdentity: WorkIdentity,
                                          aggregations: Array[AggregationOffsets]) extends EagerReduceOperator {

  override def init(queryContext: QueryContext, state: QueryState, inputMorsels: Seq[MorselExecutionContext], cursors: ExpressionCursors): ContinuableOperatorTask =
    new OTask(inputMorsels.toArray)

  class OTask(inputMorsels: Array[MorselExecutionContext]) extends ContinuableOperatorTask {

    override def operate(currentRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit = {

      val incomingSlots = aggregations.map(_.mapperOutputSlot)
      val reducers = aggregations.map(_.aggregation.createAggregationReducer)

      //Go through the morsels and collect the output from the map step
      //and reduce the values
      var i = 0
      while (i < inputMorsels.length) {
        val currentInputRow = inputMorsels(i)
        var j = 0
        while (j < aggregations.length) {
          reducers(j).reduce(currentInputRow.getRefAt(incomingSlots(j)))
          j += 1
        }
        i += 1
      }

      //Write the reduced value to output
      i = 0
      while (i < aggregations.length) {
        currentRow.setRefAt(aggregations(i).reducerOutputSlot, reducers(i).result)
        i += 1
      }
      currentRow.moveToNextRow()
      currentRow.finishedWriting()
    }

    // This operator will never continue since it will always write a single row
    override def canContinue: Boolean = false
  }
}
