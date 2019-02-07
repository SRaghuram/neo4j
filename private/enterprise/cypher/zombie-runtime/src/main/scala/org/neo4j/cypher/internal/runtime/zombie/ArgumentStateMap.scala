/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.runtime.morsel.MorselExecutionContext
import org.neo4j.cypher.internal.v4_0.util.attribution.{Attribute, Id}

/**
  * Accumulator of morsels. Has internal state which it updates using provided morsel.
  */
trait MorselAccumulator {

  /**
    * Update internal state using the provided morsel.
    */
  def update(morsel: MorselExecutionContext)
}

/**
  * Maps every argument to one `MorselAccumulator`/`T`.
  */
trait ArgumentStateMap[T <: MorselAccumulator] {

  /**
    * Update the MorselAccumulator related to `argument` and decrement
    * the argument counter. Also remove the [[owningPlanId]] counter
    * from `morsel`.
    */
  def updateAndConsume(morsel: MorselExecutionContext)

  /**
    * Consume the MorselAccumulators of all complete arguments. The MorselAccumulators will
    * be removed from the ArgumentStateMap and cannot be consumed or modified after this call.
    */
  def consumeComplete(): Iterator[T]

  /**
    * Increment the argument counter for `argument`.
    */
  def increment(argument: Long): Unit

  /**
    * Decrement the argument counter for `argument`.
    */
  def decrement(argument: Long): Unit

  /**
    * Plan which owns this argument state map.
    */
  def owningPlanId: Id

  /**
    * Slot offset of the argument slot.
    */
  def argumentSlotOffset: Int
}

class ArgumentStateMaps() extends Attribute[ArgumentStateMap[_ <: MorselAccumulator]]

object ArgumentStateMap {

  /**
    * For each argument row id at `argumentSlotOffset`, create a view over `morsel`
    * displaying only the rows derived from this argument rows, and execute `f` on this view.
    */
  def foreachArgument(argumentSlotOffset: Int,
                      morsel: MorselExecutionContext,
                      f: (Long, MorselExecutionContext) => Unit): Unit = {

    while (morsel.isValidRow) {
      var arg = morsel.getLongAt(argumentSlotOffset)
      val start: Int = morsel.getCurrentRow
      while (morsel.isValidRow && morsel.getLongAt(argumentSlotOffset) == arg) {
        morsel.moveToNextRow()
      }
      val end: Int = morsel.getCurrentRow
      val view = morsel.view(start, end)
      f(arg, view)
    }
  }
}
