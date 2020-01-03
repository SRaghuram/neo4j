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

import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.CursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe.{getRowNode, relationshipIterator}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, IsNoValue, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.helpers.{CachingExpandInto, RelationshipSelectionCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}

import scala.collection.Iterator

/**
 * Expand when both end-points are known, find all relationships of the given
 * type in the given direction between the two end-points.
 *
 * This is done by checking both nodes and starts from any non-dense node of the two.
 * If both nodes are dense, we find the degree of each and expand from the smaller of the two
 *
 * This pipe also caches relationship information between nodes for the duration of the query
 */
case class ExpandIntoPipe(source: Pipe,
                          fromName: String,
                          relName: String,
                          toName: String,
                          dir: SemanticDirection,
                          lazyTypes: RelationshipTypes)
                          (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {
  self =>
  private val kernelDirection = dir match {
    case SemanticDirection.OUTGOING => Direction.OUTGOING
    case SemanticDirection.INCOMING => Direction.INCOMING
    case SemanticDirection.BOTH => Direction.BOTH
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val query = state.query

    val expandInto = new CachingExpandInto(query.transactionalContext.dataRead, kernelDirection)
    val nodeCursor = query.nodeCursor()
    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        fromNode match {
          case fromNode: NodeValue =>
            val toNode = getRowNode(row, toName)
            toNode match {
              case IsNoValue() => Iterator.empty
              case n: NodeValue =>
                val groupCursor = query.groupCursor()
                val traversalCursor = query.traversalCursor()
                val relationships = relationshipIterator(expandInto.connectingRelationships(nodeCursor,
                                                                                            groupCursor,
                                                                                            traversalCursor,
                                                                                            fromNode.id(),
                                                                                            lazyTypes.types(query),
                                                                                            n.id()), query)
                if (relationships.isEmpty) Iterator.empty
                else relationships.map(r => executionContextFactory.copyWith(row, relName, r))
              case value => throw new ParameterWrongTypeException(
                s"Expected to find a node at '$fromName' but found $value instead")
            }

          case IsNoValue() => Iterator.empty
        }
    }
  }
}

object ExpandIntoPipe {

  def relationshipIterator(cursor: RelationshipSelectionCursor,
                           query: QueryContext): Iterator[RelationshipValue] = {
    new CursorIterator[RelationshipValue] {

      override protected def fetchNext(): RelationshipValue = {
        if (cursor.next()) {
          query.relationshipById(cursor.relationshipReference(), cursor.sourceNodeReference(), cursor.targetNodeReference(),
                           cursor.`type`())
        } else {
          null
        }
      }

      override protected def close(): Unit = cursor.close()
    }
  }

  @inline
  def getRowNode(row: ExecutionContext, col: String): AnyValue = {
    row.getByName(col) match {
      case n: NodeValue => n
      case IsNoValue() => NO_VALUE
      case value => throw new ParameterWrongTypeException(s"Expected to find a node at '$col' but found $value instead")
    }
  }
}


