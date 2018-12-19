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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.matching

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.{KeyToken, UnresolvedProperty}
import org.neo4j.cypher.internal.runtime.interpreted.commands.{RelatedTo, SingleNode}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.values.virtual.NodeValue

import scala.collection.Map

class PatternNode(key: String, val labels: Seq[KeyToken] = Seq.empty, val properties: Map[KeyToken, Expression] = Map.empty)
  extends PatternElement(key) {

  def this(node: SingleNode) = {
    this(node.name, node.labels, node.properties.map {
      case (k, e) => (UnresolvedProperty(k), e)
    })
  }

  def canUseThis(graphNodeId: Long, state: QueryState, ctx: ExecutionContext): Boolean =
    nodeHasLabels(graphNodeId, state) &&
    nodeHasProperties(graphNodeId, ctx, state)

  val relationships = scala.collection.mutable.Set[PatternRelationship]()

  def getPRels(history: Seq[MatchingPair]): Seq[PatternRelationship] = relationships.filterNot(r => history.exists(_.matches(r))).toIndexedSeq

  def getGraphRelationships(node: NodeValue, pRel: PatternRelationship, state: QueryState, f: => ExecutionContext): Seq[GraphRelationship] =
    pRel.getGraphRelationships(this, node, state, f)

  def relateTo(key: String, other: PatternNode, relType: Seq[String], dir: SemanticDirection,
               props: Map[String, Expression] = Map.empty): PatternRelationship = {
    val relProps = props.map { case (k,v) => UnresolvedProperty(k)->v }.toMap
    val rel = new PatternRelationship(key, this, other, relType, relProps, dir)
    relationships.add(rel)
    other.relationships.add(rel)
    rel
  }

  def relateTo(key: String, other: PatternNode, r: RelatedTo): PatternRelationship =
    relateTo(key, other, r.relTypes, r.direction, r.properties)

  def relateViaVariableLengthPathTo(pathName: String,
                                    end: PatternNode,
                                    minHops: Option[Int],
                                    maxHops: Option[Int],
                                    relType: Seq[String],
                                    dir: SemanticDirection,
                                    collectionOfRels: Option[String],
                                    props: Map[String, Expression] = Map.empty): PatternRelationship = {
    val relProps = props.map { case (k,v) => UnresolvedProperty(k)->v }.toMap
    val rel = new VariableLengthPatternRelationship(pathName, this, end, collectionOfRels, minHops, maxHops, relType, relProps, dir)
    relationships.add(rel)
    end.relationships.add(rel)
    rel
  }

  override def toString = String.format("PatternNode[key=%s]", key)

  def traverse[T](shouldFollow: (PatternElement) => Boolean,
                  visitNode: (PatternNode, T) => T,
                  visitRelationship: (PatternRelationship, T) => T,
                  data: T,
                  path: Seq[PatternElement]) {
    if (!path.contains(this)) {
      val moreData = visitNode(this, data)

      relationships.
        filter(shouldFollow).
        foreach(r => r.traverse(shouldFollow, visitNode, visitRelationship, moreData, this, path :+ this))
    }
  }

  private def nodeHasLabels(graphNodeId: Long, state: QueryState): Boolean = {
    val expectedLabels: Seq[Option[Int]] = labels.map(_.getOptId(state.query))

    expectedLabels.forall {
      case None          => false
      case Some(labelId) => state.query.isLabelSetOnNode(labelId, graphNodeId, state.cursors.nodeCursor)
    }
  }

  private def nodeHasProperties(graphNodeId: Long, execCtx: ExecutionContext, state: QueryState): Boolean =
    properties.forall {
    case (token, expression) =>
      val propertyId = token.getOptId(state.query)
      if (propertyId.isEmpty) false // The property doesn't exist in the graph
      else {
        val value = state.query.nodeOps.getProperty(graphNodeId, propertyId.get, state.cursors.nodeCursor, state.cursors.propertyCursor)
        val expectedValue = expression(execCtx, state)
        value == expectedValue
      }
  }
}
