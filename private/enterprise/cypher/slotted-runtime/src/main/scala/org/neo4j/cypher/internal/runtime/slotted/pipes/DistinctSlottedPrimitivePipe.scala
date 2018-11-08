/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util

import org.eclipse.collections.impl.factory.Sets
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{Slot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils
import org.opencypher.v9_0.util.attribution.Id

import scala.collection.immutable

case class DistinctSlottedPrimitivePipe(source: Pipe,
                                        slots: SlotConfiguration,
                                        primitiveSlots: Array[Int],
                                        projections: Map[Slot, Expression])
                                       (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setValuesInOutput: immutable.Iterable[(ExecutionContext, QueryState, ExecutionContext) => Unit] = projections.map {
    case (slot, expression) =>
      val f = SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor(slot)
      (incomingContext: ExecutionContext, state: QueryState, outgoingContext: ExecutionContext) =>
        f(outgoingContext, expression(incomingContext, state))
  }

  projections.values.foreach(_.registerOwningPipe(this))

  //===========================================================================
  // Runtime code
  //===========================================================================
  protected def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    new PrefetchingIterator[ExecutionContext] {
      private val seen = Sets.mutable.empty[Key]()

      override def produceNext(): Option[ExecutionContext] = {
        while (input.nonEmpty) {
          val next: ExecutionContext = input.next()

          // Create key array
          val keys = buildKey(next)

          if (seen.add(new Key(keys))) {
            // Found something! Set it as the next element to yield, and exit
            val outgoing = SlottedExecutionContext(slots)
            outgoing.setLinenumber(next.getLinenumber)
            for (setter <- setValuesInOutput) {
              setter(next, state, outgoing)
            }

            return Some(outgoing)
          }
        }

        None
      }
    }
  }

  private def buildKey(next: ExecutionContext): Array[Long] = {
    val keys = new Array[Long](primitiveSlots.length)
    var i = 0
    while (i < primitiveSlots.length) {
      keys(i) = next.getLongAt(primitiveSlots(i))
      i += 1
    }
    keys
  }
}

/**
  * This little class is here to make sure we have the expected behaviour of our primitive arrays.
  * In the JVM, long[] are do not have reasonable hashcode or equal()
  */
class Key(val inner: Array[Long]) {

  override val hashCode: Int = util.Arrays.hashCode(inner)

  override def equals(other: Any): Boolean =
    if (other == null || getClass() != other.getClass())
      false
    else {
      val otherKey = other.asInstanceOf[Key]

      if (otherKey eq this)
        return true

      util.Arrays.hashCode(inner) == util.Arrays.hashCode(inner) && util.Arrays.equals(inner, otherKey.inner)
    }

  override def toString: String = util.Arrays.toString(inner)
}
