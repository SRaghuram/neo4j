/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetOperation
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.Token
import org.neo4j.internal.kernel.api.Write
import org.neo4j.token.api.TokenConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SetOperator(val workIdentity: WorkIdentity,
                  setOperation: SetOperation) extends StatelessOperator {

  var event: OperatorProfileEvent = _
  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    val cursor: MorselFullCursor = morsel.fullCursor()
    var setCount = 0L
    while (cursor.next()) {
      val nbrOfSetProperties = setOperation.set(cursor, queryState)
      setCount += nbrOfSetProperties
      resources.queryStatisticsTracker.setProperties(nbrOfSetProperties)
    }

    // write::nodeSetProperty and write::relationshipSetProperty uses an internal property cursor
    // to get the previous value of the property.
    if (event != null) {
      event.dbHits(setCount)
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    this.event = event
  }
}

object SetOperator {

  def addNodeProperties(nodeId: Long,
                        propertiesMap: AnyValue,
                        token: Token,
                        write: Write,
                        queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(propertiesMap)
      .foreach {
        case (k: String, v: AnyValue) if !(v eq NO_VALUE) => setNodeProperty(nodeId, k, v, token, write, queryStatisticsTracker)
        case _ =>
      }
  }

  def setNodeProperty(nodeId: Long,
                      propertyKey: String,
                      propertyValue: AnyValue,
                      token: Token,
                      write: Write,
                      queryStatisticsTracker: MutableQueryStatistics) : Unit = {
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      val propertyToken = token.propertyKey(propertyKey)
      if (propertyToken != TokenConstants.NO_TOKEN && !(write.nodeRemoveProperty(nodeId, propertyToken) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      val propertyKeyId = token.propertyKeyGetOrCreateForName(propertyKey)
      translateException(token, write.nodeSetProperty(nodeId, propertyKeyId, safeValue))
      queryStatisticsTracker.setProperty()
    }
  }

  def setNodeProperty(nodeId: Long,
                      propertyToken: Int,
                      propertyValue: AnyValue,
                      token: Token,
                      write: Write,
                      queryStatisticsTracker: MutableQueryStatistics): Unit = {
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      if (!(write.nodeRemoveProperty(nodeId, propertyToken) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      translateException(token, write.nodeSetProperty(nodeId, propertyToken, safeValue))
      queryStatisticsTracker.setProperty()
    }
  }

  def setNodePropertiesFromMap(nodeId: Long,
                               propertiesMap: AnyValue,
                               removeOtherProps: Boolean,
                               token: Token,
                               write: Write,
                               read: Read,
                               queryStatisticsTracker: MutableQueryStatistics,
                               nodeCursor: NodeCursor,
                               relCursor: RelationshipScanCursor,
                               propertyCursor: PropertyCursor): Unit = {

    val propValueMap = transformToPropertyMap(propertiesMap, token, read, nodeCursor, relCursor, propertyCursor)
    propValueMap.foreach{case (k, v) => setNodeProperty(nodeId, k, v, token, write, queryStatisticsTracker)}

    //delete remaining properties
    if (removeOtherProps) {
      read.singleNode(nodeId, nodeCursor)
      if (nodeCursor.next()) {
        val setKeys = propValueMap.keySet

        nodeCursor.properties(propertyCursor)
        val propsToRemove = ArrayBuffer[Int]()
        while (propertyCursor.next()) {
          if (!setKeys.contains(propertyCursor.propertyKey)) {
            propsToRemove.append(propertyCursor.propertyKey())
          }
        }

        propsToRemove.foreach(prop => {
          write.nodeRemoveProperty(nodeId, prop)
          queryStatisticsTracker.setProperty()
        })
      }
    }
  }

  def addRelationshipProperties(relationshipId: Long,
                                propertiesMap: AnyValue,
                                token: Token,
                                write: Write,
                                queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(propertiesMap)
      .foreach {
        case (k: String, v: AnyValue) if !(v eq NO_VALUE) => setRelationshipProperty(relationshipId, k, v, token, write, queryStatisticsTracker)
        case _ =>
      }
  }

  def setRelationshipProperty(relationshipId: Long,
                              propertyKey: String,
                              propertyValue: AnyValue,
                              token: Token,
                              write: Write,
                              queryStatisticsTracker: MutableQueryStatistics): Unit = {
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      val propertyToken = token.propertyKey(propertyKey)
      if (propertyToken != TokenConstants.NO_TOKEN && !(write.relationshipRemoveProperty(relationshipId, propertyToken) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      val propertyKeyId = token.propertyKeyGetOrCreateForName(propertyKey)
      translateException(token, write.relationshipSetProperty(relationshipId, propertyKeyId, safeValue))
      queryStatisticsTracker.setProperty()
    }
  }

  def setRelationshipProperty(relationshipId: Long,
                              propertyKeyId: Int,
                              propertyValue: AnyValue,
                              token: Token,
                              write: Write,
                              queryStatisticsTracker: MutableQueryStatistics): Unit = {
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      if (!(write.relationshipRemoveProperty(relationshipId, propertyKeyId) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      translateException(token, write.relationshipSetProperty(relationshipId, propertyKeyId, safeValue))
      queryStatisticsTracker.setProperty()
    }
  }

  def setRelationshipPropertiesFromMap(relationShipId: Long,
                                       propertiesMap: AnyValue,
                                       removeOtherProps: Boolean,
                                       token: Token,
                                       write: Write,
                                       read: Read,
                                       queryStatisticsTracker: MutableQueryStatistics,
                                       nodeCursor: NodeCursor,
                                       relCursor: RelationshipScanCursor ,
                                       propertyCursor: PropertyCursor): Unit = {

    val propValueMap = transformToPropertyMap(propertiesMap, token, read, nodeCursor, relCursor, propertyCursor)
    propValueMap.foreach{case (k, v) => setRelationshipProperty(relationShipId, k, v, token, write, queryStatisticsTracker)}

    //delete remaining properties
    if (removeOtherProps) {
      read.singleRelationship(relationShipId, relCursor)
      if (relCursor.next()) {
        val setKeys = propValueMap.keySet

        relCursor.properties(propertyCursor)
        val propsToRemove = ArrayBuffer[Int]()
        while (propertyCursor.next()) {
          if (!setKeys.contains(propertyCursor.propertyKey)) {
            propsToRemove.append(propertyCursor.propertyKey())
          }
        }

        propsToRemove.foreach(prop => {
          write.relationshipRemoveProperty(relationShipId, prop)
          queryStatisticsTracker.setProperty()
        })
      }
    }
  }

  private def safeCastToMap(value: AnyValue): MapValue = value match {
    case mapValue: MapValue => mapValue
    case _ =>
      throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
  }

  private def transformToPropertyMap(value: AnyValue,
                                     token: Token,
                                     read: Read,
                                     nodeCursor: NodeCursor,
                                     relCursor: RelationshipScanCursor,
                                     propertyCursor: PropertyCursor): mutable.Map[Int, AnyValue] = value match {
    case mapValue: MapValue =>
      val map = mutable.Map[Int, AnyValue]()
      mapValue.foreach {
        case (k: String, v: AnyValue) =>
          val propertyKey = if (v == Values.NO_VALUE) token.propertyKey(k) else token.propertyKeyGetOrCreateForName(k)
          if (propertyKey != TokenConstants.NO_TOKEN) {
            map.put(propertyKey, v)
          }
      }
      map
    case vNode: VirtualNodeValue =>
      read.singleNode(vNode.id(), nodeCursor)
      if (nodeCursor.next()) {
        nodeCursor.properties(propertyCursor)
        createMap(propertyCursor)
      } else {
        mutable.Map.empty
      }
    case vRelation: VirtualRelationshipValue =>
      read.singleRelationship(vRelation.id(), relCursor)
      if (relCursor.next()) {
        relCursor.properties(propertyCursor)
        createMap(propertyCursor)
      } else {
        mutable.Map.empty
      }
    case _ =>
      throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
  }

  def createMap(propertyCursor: PropertyCursor): mutable.Map[Int, AnyValue] = {
    val map = mutable.Map[Int, AnyValue]()
    while(propertyCursor.next()) {
      map.put(propertyCursor.propertyKey(), propertyCursor.propertyValue())
    }
    map
  }
}