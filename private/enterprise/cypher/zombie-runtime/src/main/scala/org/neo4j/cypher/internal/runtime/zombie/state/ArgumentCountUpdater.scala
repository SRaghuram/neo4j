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
  * @param argumentStateMaps the ArgumentStateMap attribute for all logical plans
  */
abstract class ArgumentCountUpdater(tracker: QueryCompletionTracker,
                                    argumentStateMaps: ArgumentStateMaps) {

  protected def initiateArgumentStates(argumentStatePlans: Seq[Id],
                                       morsel: MorselExecutionContext): Unit = {
    for {
      planId <- argumentStatePlans
      asm = argumentStateMaps(planId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.initiate(argumentId)
  }

  protected def incrementArgumentCounts(argumentStatePlans: Seq[Id],
                                        argumentRowIds: Iterable[Long]): Unit = {
    for {
      reducePlanId <- argumentStatePlans
      asm = argumentStateMaps(reducePlanId)
      argumentId <- argumentRowIds
    } asm.increment(argumentId)
  }

  protected def decrementArgumentCounts(argumentStatePlans: Seq[Id],
                                        argumentRowIds: Iterable[Long]): Unit = {
    for {
      reducePlanId <- argumentStatePlans
      asm = argumentStateMaps(reducePlanId)
      argumentId <- argumentRowIds
    } asm.decrement(argumentId)
  }

  protected def incrementArgumentCounts(argumentStatePlans: Seq[Id],
                                        morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- argumentStatePlans
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.increment(argumentId)
  }

  protected def decrementArgumentCounts(argumentStatePlans: Seq[Id],
                                        morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- argumentStatePlans
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.decrement(argumentId)
  }
}
