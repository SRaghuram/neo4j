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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.{ExecutionContext, IsNoValue}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

import scala.collection.mutable.ListBuffer

case class OptionalExpandIntoPipe(source: Pipe, fromName: String, relName: String, toName: String,
                                  dir: SemanticDirection, types: RelationshipTypes, predicate: Option[Expression])
                                 (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with CachingExpandInto {
  private final val CACHE_SIZE = 100000

  predicate.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new RelationshipsCache(CACHE_SIZE)

    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case fromNode: NodeValue =>
            val toNode = getRowNode(row, toName)

            toNode match {
              case IsNoValue() =>
                row.set(relName, Values.NO_VALUE)
                Iterator.single(row)
              case n: NodeValue =>
                val relationships = relCache.get(fromNode, n, dir)
                  .getOrElse(findRelationships(state, fromNode, n, relCache, dir, types.types(state.query)))

                val it = relationships.toIterator
                val filteredRows = ListBuffer.empty[ExecutionContext]
                while (it.hasNext) {
                  val candidateRow = executionContextFactory.copyWith(row, relName, it.next())
                  if (predicate.forall(p => p(candidateRow, state) eq Values.TRUE)){
                    filteredRows.append(candidateRow)
                  }
                }

                if (filteredRows.isEmpty) {
                  row.set(relName, Values.NO_VALUE)
                  Iterator.single(row)
                }
                else filteredRows
            }

          case IsNoValue() =>
            row.set(relName, Values.NO_VALUE)
            Iterator(row)
        }
    }
  }
}
