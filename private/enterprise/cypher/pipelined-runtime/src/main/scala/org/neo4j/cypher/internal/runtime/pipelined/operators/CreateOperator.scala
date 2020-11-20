/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.createNode
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.createRelationship
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.handleMissingNode
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.setNodeProperties
import org.neo4j.cypher.internal.runtime.pipelined.operators.CreateOperator.setRelationshipProperties
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateNodeSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateRelationshipSlottedCommand
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.MapValue

class CreateOperator(val workIdentity: WorkIdentity,
                     nodes: Array[CreateNodeSlottedCommand],
                     relationships: Array[CreateRelationshipSlottedCommand],
                     lenientCreateRelationship: Boolean) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    val write = state.query.transactionalContext.dataWrite
    val tokenWrite = state.query.transactionalContext.transaction.tokenWrite()

    val cursor: MorselFullCursor = morsel.fullCursor()
    while (cursor.next()) {
      var i = 0
      while (i < nodes.length) {
        val command = nodes(i)
        val labelIds = command.labels.map(_.getOrCreateId(state.query)).toArray
        val nodeId = createNode(labelIds, write, resources.queryStatisticsTracker)
        command.properties.foreach(p => setNodeProperties(nodeId, p(cursor, queryState), tokenWrite, write, resources.queryStatisticsTracker))
        cursor.setLongAt(command.idOffset, nodeId)
        i += 1
      }
      i = 0
      while (i < relationships.length) {
        val command = relationships(i)
        val startNodeId = command.startNodeIdGetter.applyAsLong(cursor)
        val endNodeId = command.endNodeIdGetter.applyAsLong(cursor)
        val typeId = command.relType.getOrCreateType(state.query)
        val relationshipId = if (startNodeId == NO_SUCH_NODE) {
          handleMissingNode(command.relName, command.startName, lenientCreateRelationship)
        } else if (endNodeId == NO_SUCH_NODE) {
          handleMissingNode(command.relName, command.endName, lenientCreateRelationship)
        } else {
          createRelationship(startNodeId, typeId, endNodeId, write, resources.queryStatisticsTracker)
        }
        command.properties.foreach(p => setRelationshipProperties(relationshipId, p(cursor, queryState), tokenWrite, write, resources.queryStatisticsTracker))
        cursor.setLongAt(command.relIdOffset, relationshipId)
        i += 1
      }
    }
  }
}

object CreateOperator {
  def createNode(labels: Array[Int],
                 write: Write,
                 queryStatisticsTracker: MutableQueryStatistics): Long = {
    val nodeId = write.nodeCreate()
    queryStatisticsTracker.createNode()
    var i = 0
    while (i < labels.length) {
      write.nodeAddLabel(nodeId, labels(i))
      queryStatisticsTracker.addLabel()
      i += 1
    }
    nodeId
  }

  def setNodeProperties(node: Long,
                        properties: AnyValue,
                        tokenWrite: TokenWrite,
                        write: Write,
                        queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(properties).foreach((k: String, v: AnyValue) => {
      if (!(v eq NO_VALUE)) {
        val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(k)
        write.nodeSetProperty(node, propertyKeyId, makeValueNeoSafe(v))
        queryStatisticsTracker.setProperty()
      }
    })
  }

  def handleMissingNode(relName: String, nodeName: String, lenientCreateRelationship: Boolean): Long =
    if (lenientCreateRelationship) NO_SUCH_RELATIONSHIP
    else throw new InternalException(LenientCreateRelationship.errorMsg(relName, nodeName))

  def createRelationship(source: Long,
                         typ: Int,
                         target: Long,
                         write: Write,
                         queryStatisticsTracker: MutableQueryStatistics): Long = {
      val relId = write.relationshipCreate(source, typ, target)
      queryStatisticsTracker.createRelationship()
      relId
  }

  def setRelationshipProperties(relationship: Long,
                                properties: AnyValue,
                                tokenWrite: TokenWrite,
                                write: Write,
                                queryStatisticsTracker: MutableQueryStatistics): Unit = {
    if (relationship != NO_SUCH_RELATIONSHIP) {
      safeCastToMap(properties).foreach((k: String, v: AnyValue) => {
        if (!(v eq NO_VALUE)) {
          val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(k)
          write.relationshipSetProperty(relationship, propertyKeyId, makeValueNeoSafe(v))
          queryStatisticsTracker.setProperty()
        }
      })
    }
  }

  private def safeCastToMap(value: AnyValue): MapValue = value match {
    case mapValue: MapValue => mapValue
    case _ =>
      throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
  }
}


