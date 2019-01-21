/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.expressions.AggregationMapper
import org.neo4j.internal.kernel.api.IndexReadSession

import scala.collection.mutable


/*
Responsible for aggregating the data coming from a single morsel. This is equivalent to the map
step of map-reduce. Each thread performs it its local aggregation on the data local to it. In
the subsequent reduce steps these local aggregations are merged into a single global aggregate.
 */
class AggregationMapperOperator(val workIdentity: WorkIdentity,
                                aggregations: Array[AggregationOffsets],
                                groupings: GroupingExpression) extends StatelessOperator {
  override def operate(currentRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {

    val result = mutable.LinkedHashMap[groupings.KeyType, Array[(Int,AggregationMapper)]]()

    val queryState = new OldQueryState(context,
                                       resources = null,
                                       params = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession])

    //loop over the entire morsel and apply the aggregation
    while (currentRow.isValidRow) {
      val groupingValue = groupings.computeGroupingKey(currentRow, queryState)
      val functions = result
        .getOrElseUpdate(groupingValue, aggregations.map(a => a.mapperOutputSlot -> a.aggregation.createAggregationMapper))
      functions.foreach(f => f._2.map(currentRow, queryState))
      currentRow.moveToNextRow()
    }

    //reuse and reset morsel context
    currentRow.resetToFirstRow()
    result.foreach {
      case (key, aggregator) =>
        groupings.project(currentRow, key)
        var i = 0
        while (i < aggregations.length) {
          val (offset, mapper) = aggregator(i)
          currentRow.setRefAt(offset, mapper.result)
          i += 1
        }
        currentRow.moveToNextRow()
    }
    currentRow.finishedWriting()
  }
}
