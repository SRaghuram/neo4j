/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.memory.MemoryTracker

/**
 * Wraps [[AggregatedRow]] as a [[MorselAccumulator]]. Used for Aggregations without grouping.
 */
case class AggregatedRowAccumulator(override val argumentRowId: Long,
                                    override val argumentRowIdsForReducers: Array[Long],
                                    argumentRow: MorselRow,
                                    aggregatorRow: AggregatedRow) extends MorselAccumulator[AnyRef] {

  override def update(data: AnyRef, resources: QueryResources): Unit = {}
}

object AggregatedRowAccumulator {
  class Factory(aggregators: Array[Aggregator],
                memoryTracker: MemoryTracker,
                numberOfWorkers: Int) extends ArgumentStateFactory[AggregatedRowAccumulator] {
    private[this] val needToApplyUpdates = !Aggregator.allDirect(aggregators)

    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long]): AggregatedRowAccumulator =
      AggregatedRowAccumulator(
        argumentRowId,
        argumentRowIdsForReducers,
        argumentMorsel.snapshot(),
        if (needToApplyUpdates)
          new StandardAggregators(aggregators.map(_.newStandardReducer(memoryTracker)))
        else
          new StandardDirectAggregators(aggregators.map(_.newStandardReducer(memoryTracker)))
      )

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
