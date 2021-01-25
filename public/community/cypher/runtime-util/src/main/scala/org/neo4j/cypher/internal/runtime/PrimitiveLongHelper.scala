/*
 * Copyright (c) "Neo4j"
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

object PrimitiveLongHelper {
  def map[T](in: ClosingLongIterator, f: Long => T): ClosingIterator[T] = new ClosingIterator[T] {
    override def innerHasNext: Boolean = in.hasNext

    override def next(): T = f(in.next())

    override protected[this] def closeMore(): Unit = in.close()
  }

  def mapPrimitive(in: ClosingLongIterator, f: Long => Long): ClosingLongIterator = new ClosingLongIterator {
    override def innerHasNext: Boolean = in.hasNext

    override def next(): Long = f(in.next())

    override def close(): Unit = in.close()
  }
}
