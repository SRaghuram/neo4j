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
  * Helper class for updating argument counts.
  *
  * @param tracker tracker of the progress for this query
  * @param downstreamArgumentReducers Ids of downstream logical plans that reduce morsels in this buffer
  * @param argumentStateMaps the ArgumentStateMap attribute for all logical plans
  */
abstract class ArgumentCountUpdater(tracker: QueryCompletionTracker,
                                    downstreamArgumentReducers: Seq[Id],
                                    argumentStateMaps: ArgumentStateMaps) {

  protected def initiateArgumentCounts(downstreamArgumentReducersToInitiate: Seq[Id],
                                       morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- downstreamArgumentReducersToInitiate
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.initiate(argumentId)
  }

  protected def incrementArgumentCounts(argumentRowIds: Iterable[Long]): Unit = {
    for {
      reducePlanId <- downstreamArgumentReducers
      asm = argumentStateMaps(reducePlanId)
      argumentId <- argumentRowIds
    } asm.increment(argumentId)
    tracker.increment()
  }

  protected def decrementArgumentCounts(argumentRowIds: Iterable[Long]): Unit = {
    for {
      reducePlanId <- downstreamArgumentReducers
      asm = argumentStateMaps(reducePlanId)
      argumentId <- argumentRowIds
    } asm.decrement(argumentId)
    tracker.decrement()
  }

  protected def incrementArgumentCounts(morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- downstreamArgumentReducers
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.increment(argumentId)
    tracker.increment()
  }

  protected def decrementArgumentCounts(morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- downstreamArgumentReducers
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.decrement(argumentId)
    tracker.decrement()
  }
}
