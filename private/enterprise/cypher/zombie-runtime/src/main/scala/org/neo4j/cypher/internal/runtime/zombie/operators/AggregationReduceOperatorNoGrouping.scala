/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.zombie.aggregators.{AggregatingAccumulator, Aggregator, Updater}

/**
  * Operator which streams aggregated data, built by [[AggregationMapperOperatorNoGrouping]] and [[AggregatingAccumulator]].
  */
class AggregationReduceOperatorNoGrouping(val argumentStateMapId: ArgumentStateMapId,
                                          val workIdentity: WorkIdentity,
                                          aggregations: Array[Aggregator],
                                          reducerOutputSlots: Array[Int])
  extends Operator
     with ReduceOperatorState[Array[Updater], AggregatingAccumulator] {

  override def createState(argumentStateCreator: ArgumentStateMapCreator): ReduceOperatorState[Array[Updater], AggregatingAccumulator] = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatingAccumulator.Factory(aggregations))
    this
  }

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         input: AggregatingAccumulator,
                         resources: QueryResources
                        ): IndexedSeq[ContinuableOperatorTaskWithAccumulator[Array[Updater], AggregatingAccumulator]] = {
    Array(new OTask(input))
  }

  class OTask(override val accumulator: AggregatingAccumulator) extends ContinuableOperatorTaskWithAccumulator[Array[Updater], AggregatingAccumulator] {

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      var i = 0
      while (i < aggregations.length) {
        outputRow.setRefAt(reducerOutputSlots(i), accumulator.result(i))
        i += 1
      }
      outputRow.moveToNextRow()
      outputRow.finishedWriting()
    }

    // This operator will never continue since it will always write a single row
    override def canContinue: Boolean = false
  }
}
