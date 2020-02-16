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
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues

object SlottedExecutionContextOrdering {
  def comparator(order: ColumnOrder): scala.Ordering[CypherRow] = order.slot match {
    case LongSlot(offset, true, _) =>
      new scala.Ordering[CypherRow] {
        override def compare(a: CypherRow, b: CypherRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareNullableLongs(aVal, bVal)
        }
      }

    case LongSlot(offset, false, _) =>
      new scala.Ordering[CypherRow] {
        override def compare(a: CypherRow, b: CypherRow): Int = {
          val aVal = a.getLongAt(offset)
          val bVal = b.getLongAt(offset)
          order.compareLongs(aVal, bVal)
        }
      }

    case RefSlot(offset, _, _) =>
      new scala.Ordering[CypherRow] {
        override def compare(a: CypherRow, b: CypherRow): Int = {
          val aVal = a.getRefAt(offset)
          val bVal = b.getRefAt(offset)
          order.compareValues(aVal, bVal)
        }
      }
  }

  def asComparator(orderBy: Seq[ColumnOrder]): Comparator[CypherRow] =
    orderBy.map(SlottedExecutionContextOrdering.comparator)
      .reduceLeft[Comparator[CypherRow]]((a, b) => a.thenComparing(b))
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
