/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

/**
  * Morsel buffer that group incoming rows by argumentRowId by delegating to an `ArgumentStateMap`.
  *
  * @param reducePlanId id of the reduce plan owning this argument state map
  */
class MorselArgumentStateBuffer[ACC <: MorselAccumulator](tracker: QueryCompletionTracker,
                                                          downstreamArgumentReducers: Seq[Id],
                                                          workCancellers: Seq[Id],
                                                          argumentStateMaps: ArgumentStateMaps,
                                                          val reducePlanId: Id
                                  ) extends ArgumentCountUpdater(tracker, argumentStateMaps)
                                       with Sink[MorselExecutionContext]
                                       with Source[Iterable[ACC]] {

  private val argumentStateMap = argumentStateMaps(reducePlanId).asInstanceOf[ArgumentStateMap[ACC]]

  def initiate(morsel: MorselExecutionContext): Unit = {
    for (argumentRowId <- morsel.allArgumentRowIdsFor(argumentStateMap.argumentSlotOffset)) {
      argumentStateMap.initiate(argumentRowId)
      tracker.increment()
    }
  }

  def decrement(morsel: MorselExecutionContext): Unit = {
    for (argumentRowId <- morsel.allArgumentRowIdsFor(argumentStateMap.argumentSlotOffset)) {
      argumentStateMap.decrement(argumentRowId)
    }
  }

  override def put(morsel: MorselExecutionContext): Unit = {
    if (morsel.hasData) {
      morsel.resetToFirstRow()
      argumentStateMap.update(morsel, (acc, morselView) => acc.update(morselView))
    }
  }

  override def hasData: Boolean = argumentStateMap.hasCompleted

  override def take(): Iterable[ACC] = {
    val accumulators = argumentStateMap.takeCompleted()
    if (accumulators.nonEmpty) {
      incrementArgumentCounts(downstreamArgumentReducers, accumulators.map(_.argumentRowId))
      accumulators
    } else null
  }

  def filterCancelledArguments(accumulators: Iterable[ACC]): Boolean = {
    false
  }

  def close(accumulators: Iterable[ACC]): Unit = {
    decrementArgumentCounts(downstreamArgumentReducers, accumulators.map(_.argumentRowId))
    accumulators.foreach(acc => {
      tracker.decrement()
    })
  }
}

