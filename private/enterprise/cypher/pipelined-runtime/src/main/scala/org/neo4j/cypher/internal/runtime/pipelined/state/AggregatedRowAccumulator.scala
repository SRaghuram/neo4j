/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StandardReducer
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

/**
 * Wraps [[AggregatedRow]] as a [[MorselAccumulator]]. Used for Aggregations without grouping.
 */
case class AggregatedRowAccumulator(override val argumentRowId: Long,
                                    override val argumentRowIdsForReducers: Array[Long],
                                    argumentRow: MorselRow,
                                    aggregatorRow: AggregatedRow) extends MorselAccumulator[AnyRef] {

  override def update(data: AnyRef, resources: QueryResources): Unit = {}

  override def shallowSize: Long = AggregatedRowAccumulator.SHALLOW_SIZE
}

object AggregatedRowAccumulator {
  private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[AggregatedRowAccumulator])
  class Factory(aggregators: Array[Aggregator],
                memoryTracker: MemoryTracker,
                numberOfWorkers: Int) extends ArgumentStateFactory[AggregatedRowAccumulator] {
    private[this] val needToApplyUpdates = !Aggregator.allDirect(aggregators)

    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long]): AggregatedRowAccumulator = {
      var i = 0
      val reducers = new Array[StandardReducer](aggregators.length)
      while (i < reducers.length) {
        reducers(i) = aggregators(i).newStandardReducer(memoryTracker)
        i += 1
      }
      AggregatedRowAccumulator(
        argumentRowId,
        argumentRowIdsForReducers,
        argumentMorsel.snapshot(),
        if (needToApplyUpdates) {
          new StandardAggregators(reducers)
        } else {
          new StandardDirectAggregators(reducers)
        })
    }

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): AggregatedRowAccumulator =
      AggregatedRowAccumulator(
        argumentRowId,
        argumentRowIdsForReducers,
        argumentMorsel.snapshot(),
        new ConcurrentAggregators(aggregators.map(_.newConcurrentReducer), numberOfWorkers))
  }
}
