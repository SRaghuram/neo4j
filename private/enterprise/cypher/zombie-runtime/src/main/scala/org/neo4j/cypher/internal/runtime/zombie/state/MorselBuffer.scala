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
  * Morsel buffer which adds reference counting of arguments to the regular buffer semantics.
  *
  * @param tracker tracker of the progress for this query
  * @param counters Ids of downstream logical plans which need to reference count the morsels in this buffer
  * @param argumentStateMaps the ArgumentStateMap attribute for all logical plans
  * @param inner inner buffer to delegate real buffer work to
  */
class MorselBuffer(tracker: Tracker,
                   counters: Seq[Id],
                   argumentStateMaps: ArgumentStateMaps,
                   inner: Buffer[MorselExecutionContext]
                  ) extends Consumable[MorselParallelizer] {

  override def hasData: Boolean = inner.hasData

  def produce(morsel: MorselExecutionContext): Unit = {
    morsel.resetToFirstRow()
    incrementCounters(morsel)
    inner.produce(morsel)
  }

  override def consume(): MorselParallelizer = {
    val morsel = inner.consume()
    if (morsel == null)
      null
    else new Parallelizer(morsel)
  }

  /**
    * Decrement reference counters attached to `morsel`.
    */
  def close(morsel: MorselExecutionContext): Unit = decrementCounters(morsel)

  private def incrementCounters(morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- counters
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.increment(argumentId)
    tracker.increment()
    morsel.setCounters(counters)
  }

  private def decrementCounters(morsel: MorselExecutionContext): Unit = {
    for {
      reducePlanId <- morsel.getAndClearCounters()
      asm = argumentStateMaps(reducePlanId)
      argumentId <- morsel.allArgumentRowIdsFor(asm.argumentSlotOffset)
    } asm.decrement(argumentId)
    tracker.decrement()
  }

  class Parallelizer(original: MorselExecutionContext) extends MorselParallelizer {
    private var usedOriginal = false
    override def nextCopy: MorselExecutionContext = {
      if (!usedOriginal) {
        usedOriginal = true
        original
      } else {
        val copy = original.shallowCopy()
        incrementCounters(copy)
        copy
      }
    }
  }
}
