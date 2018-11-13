/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._


/*
Responsible for aggregating the data coming from a single morsel. This is equivalent to the map
step of map-reduce. Each thread performs it its local aggregation on the data local to it. In
the subsequent reduce steps these local aggregations are merged into a single global aggregate.
 */
class AggregationMapperOperatorNoGrouping(aggregations: Array[AggregationOffsets]) extends StatelessOperator {

  override def operate(currentRow: MorselExecutionContext, context: QueryContext, state: QueryState, cursors: ExpressionCursors): Unit = {

    val aggregationMappers = aggregations.map(_.aggregation.createAggregationMapper)
    val queryState = new OldQueryState(context, resources = null, params = state.params, cursors, Array())

    //loop over the entire morsel and apply the aggregation
    while (currentRow.hasMoreRows) {
      var accCount = 0
      while (accCount < aggregations.length) {
        aggregationMappers(accCount).map(currentRow, queryState)
        accCount += 1
      }
      currentRow.moveToNextRow()
    }

    //Write the local aggregation value to the morsel in order for the
    //reducer to pick it up later
    var i = 0
    currentRow.resetToFirstRow()
    while (i < aggregations.length) {
      val aggregation = aggregations(i)
      currentRow.setRefAt(aggregation.mapperOutputSlot, aggregationMappers(i).result)
      i += 1
    }
    currentRow.moveToNextRow()
    currentRow.finishedWriting()

  }
}
