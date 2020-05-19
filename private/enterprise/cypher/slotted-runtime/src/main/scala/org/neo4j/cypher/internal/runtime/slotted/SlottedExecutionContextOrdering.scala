/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import java.util.Comparator

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues

object SlottedExecutionContextOrdering {
  def comparator(order: ColumnOrder): scala.Ordering[ReadableRow] = order.slot match {
    case LongSlot(offset, true, _) =>
      new scala.Ordering[ReadableRow] {
        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case LongSlot(offset, false, _) =>
      new scala.Ordering[ReadableRow] {
        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new scala.Ordering[ReadableRow] {
        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offset)
          val bVal = b.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }
  }

  def asComparator(orderBy: Seq[ColumnOrder]): Comparator[ReadableRow] = {
    val comparators = orderBy.map(SlottedExecutionContextOrdering.comparator)

    // For size 1 and 2 the overhead of doing the foreach is measurable
    if (comparators.size == 1) {
      return comparators.head
    }
    if (comparators.size == 2) {
      val first = comparators.head
      val second = comparators.last
      return (a, b) => {
        val i = first.compare(a, b)
        if (i == 0) {
          second.compare(a, b)
        } else {
          i
        }
      }
    }

    // For larger ORDER BY the overhead is negligible
    new Comparator[ReadableRow] {
      override def compare(a: ReadableRow, b: ReadableRow): Int = {
        for (comparator <- comparators) {
          val i = comparator.compare(a, b)
          if (i != 0) {
            return i;
          }
        }
        0
      }
    }
  }
}

sealed trait ColumnOrder {
  def slot: Slot

  def compareValues(a: AnyValue, b: AnyValue): Int
  def compareLongs(a: Long, b: Long): Int
  def compareNullableLongs(a: Long, b: Long): Int
}

case class Ascending(slot: Slot) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(a, b)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(a, b)
  override def compareNullableLongs(a: Long, b: Long): Int = java.lang.Long.compareUnsigned(a, b)
}

case class Descending(slot: Slot) extends ColumnOrder {
  override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(b, a)
  override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(b, a)
  override def compareNullableLongs(a: Long, b: Long): Int = java.lang.Long.compareUnsigned(b, a)
}
