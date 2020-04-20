/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values.COMPARATOR

import scala.annotation.tailrec
import scala.collection.mutable

abstract class ManyNodeValueIndexCursor extends DefaultCloseListenable with NodeValueIndexCursor {

  private[this] var closed = false
  protected var current: NodeValueIndexCursor = _

  protected def cursors: Array[NodeValueIndexCursor]

  override def numberOfProperties(): Int = current.numberOfProperties()

  override def propertyKey(offset: Int): Int = current.propertyKey(offset)

  override def hasValue: Boolean = current.hasValue

  override def propertyValue(offset: Int): Value = current.propertyValue(offset)

  override def node(cursor: NodeCursor): Unit = current.node(cursor)

  override def nodeReference(): Long = current.nodeReference()

  override def score(): Float = current.score()

  override def setTracer(tracer: KernelReadTracer): Unit = {
    if (current != null) {
      current.setTracer(tracer)
    }
    cursors.foreach(_.setTracer(tracer))
  }

  override def removeTracer(): Unit = {
    if (current != null) {
      current.removeTracer()
    }
    cursors.foreach(_.removeTracer())
  }

  override def close(): Unit = {
    closed = true
    closeInternal()
    val listener = closeListener
    if (listener != null) listener.onClosed(this)
  }

  override def closeInternal(): Unit = {
    if (current != null) {
      current.close()
    }
    cursors.foreach(_.close())
  }

  override def isClosed: Boolean = closed
}

object ManyNodeValueIndexCursor {
  private val ASCENDING: Ordering[NodeValueIndexCursor] =
    (x: NodeValueIndexCursor, y: NodeValueIndexCursor) => -COMPARATOR.compare(x.propertyValue(0), y.propertyValue(0))
  private val DESCENDING: Ordering[NodeValueIndexCursor] =
    (x: NodeValueIndexCursor, y: NodeValueIndexCursor) => COMPARATOR.compare(x.propertyValue(0), y.propertyValue(0))

  def ascending(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = new MergeSortCursor(cursors, ASCENDING)
  def descending(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = new MergeSortCursor(cursors, DESCENDING)
  def unordered(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = new UnorderedCursor(cursors)
}

private class UnorderedCursor(override val cursors: Array[NodeValueIndexCursor]) extends ManyNodeValueIndexCursor {
  private[this] var index = 0
  current = if (cursors.nonEmpty) cursors.head else null

  @tailrec
  override final def next(): Boolean = {
    if (current != null && current.next()) true
    else {
      if (index < cursors.length - 1) {
        index += 1
        current = cursors(index)
        next()
      } else false
    }
  }
}

/**
  * NOTE: this assumes that cursors internally are correctly sorted which is guaranteed by the index.
  */
private class MergeSortCursor(override val cursors: Array[NodeValueIndexCursor], ordering: Ordering[NodeValueIndexCursor] ) extends ManyNodeValueIndexCursor {
  private[this] val queue: mutable.PriorityQueue[NodeValueIndexCursor] = mutable.PriorityQueue.empty[NodeValueIndexCursor](ordering)

  if (AssertionRunner.isAssertionsEnabled) {
    //we must have values in order to keep items sorted
    require(cursors.forall(_.hasValue))
  }

  cursors.foreach(c => {
    if (c.next()) {
      queue.enqueue(c)
    }
  })

  override def next(): Boolean = {
    if (current != null && current.next()) {
      queue.enqueue(current)
    }

    if (queue.isEmpty) {
      if (current != null) {
        current.close()
      }
      current = null
      false
    }
    else {
      current = queue.dequeue()
      true
    }
  }
}