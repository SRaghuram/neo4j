/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.logical.plans

object ProvidedOrder {
  val empty: ProvidedOrder = ProvidedOrder(Seq.empty[ColumnOrder])
}

/**
  * A LogicalPlan can guarantee to provide its results in a particular order. This class
  * is uses for the purpose of conveying the information of which order the reuslts are in,
  * if they are in any defined order.
  * @param columns a sequence of columns with sort direction
  */
case class ProvidedOrder(columns: Seq[ColumnOrder]) {

  /**
    * Returns a new provided order where the order columns of this are concatenated with
    * the order columns of the other provided order. Example:
    * [n.foo ASC, n.bar DESC].followedBy([n.baz ASC]) = [n.foo ASC, n.bar DESC, n.baz ASC]
    *
    * If this is empty, then the returned provided order will also be empty, regardless of the
    * given nextOrder.
    */
  def followedBy(nextOrder: ProvidedOrder): ProvidedOrder = {
    if (this.columns.isEmpty) {
      this
    } else {
      ProvidedOrder(columns ++ nextOrder.columns)
    }
  }

  /**
    * Trim provided order up until a sort column that matches any of the given args.
    */
  def upToExcluding(args: Set[String]): ProvidedOrder = {
    val trimmed = columns.foldLeft((false,Seq.empty[ColumnOrder])) {
      case (acc, _) if acc._1 => acc
      case (acc, col) if args.contains(col.id) => (true, acc._2)
      case (acc, col) => (acc._1, acc._2 :+ col)
    }
    ProvidedOrder(trimmed._2)
  }
}
