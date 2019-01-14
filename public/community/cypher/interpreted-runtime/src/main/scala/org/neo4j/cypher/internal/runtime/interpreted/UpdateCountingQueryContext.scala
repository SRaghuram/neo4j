/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.planner.v4_0.spi.IdempotentResult
import org.neo4j.cypher.internal.runtime.{Operations, QueryContext, QueryStatistics, _}
import org.neo4j.internal.kernel.api.{IndexReference, NodeCursor, RelationshipScanCursor}
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection

class UpdateCountingQueryContext(inner: QueryContext) extends DelegatingQueryContext(inner) {

  private val nodesCreated = new Counter
  private val relationshipsCreated = new Counter
  private val propertiesSet = new Counter
  private val nodesDeleted = new Counter
  private val relationshipsDeleted = new Counter
  private val labelsAdded = new Counter
  private val labelsRemoved = new Counter
  private val indexesAdded = new Counter
  private val indexesRemoved = new Counter
  private val uniqueConstraintsAdded = new Counter
  private val uniqueConstraintsRemoved = new Counter
  private val propertyExistenceConstraintsAdded = new Counter
  private val propertyExistenceConstraintsRemoved = new Counter
  private val nodekeyConstraintsAdded = new Counter
  private val nodekeyConstraintsRemoved = new Counter

  def getStatistics = QueryStatistics(
    nodesCreated = nodesCreated.count,
    relationshipsCreated = relationshipsCreated.count,
    propertiesSet = propertiesSet.count,
    nodesDeleted = nodesDeleted.count,
    labelsAdded = labelsAdded.count,
    labelsRemoved = labelsRemoved.count,
    relationshipsDeleted = relationshipsDeleted.count,
    indexesAdded = indexesAdded.count,
    indexesRemoved = indexesRemoved.count,
    uniqueConstraintsAdded = uniqueConstraintsAdded.count,
    uniqueConstraintsRemoved = uniqueConstraintsRemoved.count,
    existenceConstraintsAdded = propertyExistenceConstraintsAdded.count,
    existenceConstraintsRemoved = propertyExistenceConstraintsRemoved.count,
    nodekeyConstraintsAdded = nodekeyConstraintsAdded.count,
    nodekeyConstraintsRemoved = nodekeyConstraintsRemoved.count)

  override def getOptStatistics = Some(getStatistics)

  override def createNode(labels: Array[Int]) = {
    nodesCreated.increase()
    labelsAdded.increase(labels.length)
    inner.createNode(labels)
  }

  override def createNodeId(labels: Array[Int]) = {
    nodesCreated.increase()
    labelsAdded.increase(labels.length)
    inner.createNodeId(labels)
  }

  override def nodeOps: NodeOperations =
    new CountingOps[NodeValue, NodeCursor](inner.nodeOps, nodesDeleted) with NodeOperations

  override def relationshipOps: RelationshipOperations =
    new CountingOps[RelationshipValue, RelationshipScanCursor](inner.relationshipOps, relationshipsDeleted) with RelationshipOperations

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = {
    val added = inner.setLabelsOnNode(node, labelIds)
    labelsAdded.increase(added)
    added
  }

  override def createRelationship(start: Long, end: Long, relType: Int) = {
    relationshipsCreated.increase()
    inner.createRelationship(start, end, relType)
  }

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = {
    val removed = inner.removeLabelsFromNode(node, labelIds)
    labelsRemoved.increase(removed)
    removed
  }

  override def addIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): IdempotentResult[IndexReference] = {
    val result = inner.addIndexRule(labelId, propertyKeyIds)
    result.ifCreated { indexesAdded.increase() }
    result
  }

  override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit = {
    inner.dropIndexRule(labelId, propertyKeyIds)
    indexesRemoved.increase()
  }

  override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Boolean = {
    val result = inner.createNodeKeyConstraint(labelId, propertyKeyIds)
    if ( result ) nodekeyConstraintsAdded.increase()
    result
  }

  override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit = {
    inner.dropNodeKeyConstraint(labelId, propertyKeyIds)
    nodekeyConstraintsRemoved.increase()
  }

  override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Boolean = {
    val result = inner.createUniqueConstraint(labelId, propertyKeyIds)
    if ( result ) uniqueConstraintsAdded.increase()
    result
  }

  override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit = {
    inner.dropUniqueConstraint(labelId, propertyKeyIds)
    uniqueConstraintsRemoved.increase()
  }

  override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Boolean = {
    val result = inner.createNodePropertyExistenceConstraint(labelId, propertyKeyId)
    if ( result ) propertyExistenceConstraintsAdded.increase()
    result
  }

  override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit = {
    inner.dropNodePropertyExistenceConstraint(labelId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Boolean = {
    val result = inner.createRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId)
    if ( result ) propertyExistenceConstraintsAdded.increase()
    result
  }

  override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int) {
    inner.dropRelationshipPropertyExistenceConstraint(relTypeId, propertyKeyId)
    propertyExistenceConstraintsRemoved.increase()
  }

  override def nodeGetDegree(node: Long, dir: SemanticDirection, nodeCursor: NodeCursor): Int = super.nodeGetDegree(node, dir, nodeCursor)

  override def detachDeleteNode(node: Long): Int = {
    nodesDeleted.increase()
    val count = inner.detachDeleteNode(node)
    relationshipsDeleted.increase(count)
    count
  }

  class Counter {
    val counter: AtomicInteger = new AtomicInteger()

    def count: Int = counter.get()

    def increase(amount: Int = 1) {
      counter.addAndGet(amount)
    }
  }

  private class CountingOps[T, CURSOR](inner: Operations[T, CURSOR], deletes: Counter)
    extends DelegatingOperations[T, CURSOR](inner) {

    override def delete(id: Long) {
      deletes.increase()
      inner.delete(id)
    }

    override def removeProperty(id: Long, propertyKeyId: Int) {
      propertiesSet.increase()
      inner.removeProperty(id, propertyKeyId)
    }

    override def setProperty(id: Long, propertyKeyId: Int, value: Value) {
      propertiesSet.increase()
      inner.setProperty(id, propertyKeyId, value)
    }
  }
}
