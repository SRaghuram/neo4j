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
import org.neo4j.values.storable.Values


/*
Responsible for aggregating the data coming from a single morsel. This is equivalent to the map
step of map-reduce. Each thread performs it its local aggregation on the data local to it. In
the subsequent reduce steps these local aggregations are merged into a single global aggregate.
 */
class AggregationMapperOperatorNoGrouping(val workIdentity: WorkIdentity,
                                          aggregations: Array[AggregationOffsets]) extends StatelessOperator {

  override def operate(currentRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {

    val aggregationMappers = aggregations.map(_.createMapper)
    val queryState = new OldQueryState(context,
                                       resources = null,
                                       params = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession],
                                       resources.expressionVariables(state.nExpressionSlots))

    val hasInput = currentRow.isValidRow

    //loop over the entire morsel and apply the aggregation
    while (currentRow.isValidRow) {
      var accCount = 0
      while (accCount < aggregations.length) {
        aggregationMappers(accCount).apply(currentRow, queryState)
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
      val localAggregationValue = if (hasInput) aggregationMappers(i).result(queryState) else Values.NO_VALUE
      currentRow.setRefAt(aggregation.mapperOutputSlot, localAggregationValue)
      i += 1
    }
    currentRow.moveToNextRow()
    currentRow.finishedWriting()
  }
}
