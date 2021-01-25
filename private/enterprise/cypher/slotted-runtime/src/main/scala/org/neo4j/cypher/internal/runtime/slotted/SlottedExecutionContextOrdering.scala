/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import java.util.Comparator

import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues

object SlottedExecutionContextOrdering {
  def comparator(order: ColumnOrder): Comparator[ReadableRow] = order.slot match {
    case LongSlot(offset, true, _) =>
      new Comparator[ReadableRow] {
        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case LongSlot(offset, false, _) =>
      new Comparator[ReadableRow] {
        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new Comparator[ReadableRow] {
        override def compare(a: ReadableRow, b: ReadableRow): Int = {
          val aVal = a.getRefAt(offset)
          val bVal = b.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }
  }

  def asComparator(orderBy: Seq[ColumnOrder]): Comparator[ReadableRow] =
    composeComparator[ReadableRow](SlottedExecutionContextOrdering.comparator)(orderBy)

  def composeComparator[T](singleComparatorCreator: ColumnOrder => Comparator[T])(orderBy: Seq[ColumnOrder]): Comparator[T] = {
    val size = orderBy.size

    // For size 1 and 2 the overhead of doing the foreach is measurable
    if (size == 1) {
      singleComparatorCreator(orderBy.head)
    }
    else if (size == 2) {
      val first = singleComparatorCreator(orderBy.head)
      val second = singleComparatorCreator(orderBy.last)
      (a, b) => {
        val i = first.compare(a, b)
        if (i == 0) {
          second.compare(a, b)
        } else {
          i
        }
      }
    }
    else {
      val comparators = new Array[Comparator[T]](size)
      var i = 0
      orderBy.foreach { columnOrder =>
        comparators(i) = singleComparatorCreator(columnOrder)
        i += 1
      }

      // For larger ORDER BY the overhead is negligible
      new Comparator[T] {
        override def compare(a: T, b: T): Int = {
          var c = 0
          while (c < comparators.length) {
            val i = comparators(c).compare(a, b)
            if (i != 0) {
              return i;
            }
            c += 1
          }
          0
        }
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
