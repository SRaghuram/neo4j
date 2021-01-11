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
import org.neo4j.values.storable.Value
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
                      queryStatisticsTracker: MutableQueryStatistics): Unit = {
    val safeValue: Value = makeValueNeoSafe(propertyValue)
    val propertyId = getPropertyKey(propertyKey, safeValue, token)
    setNodeProperty(nodeId, propertyId, propertyValue, token, write, queryStatisticsTracker)
  }

  private def setNodeProperty(nodeId: Long,
                      propertyId: Int,
                      propertyValue: AnyValue,
                      token: Token,
                      write: Write,
                      queryStatisticsTracker: MutableQueryStatistics): Unit = {
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      if (!(write.nodeRemoveProperty(nodeId, propertyId) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      translateException(token, write.nodeSetProperty(nodeId, propertyId, safeValue))
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
                               propertyCursor: PropertyCursor,
                               expression: String): Unit = {
    val propValueMap = transformToPropertyValueMap(propertiesMap, token, read, nodeCursor, relCursor, propertyCursor, expression)
    propValueMap.foreach { case (k, v) => setNodeProperty(nodeId, k, v, token, write, queryStatisticsTracker) }

    //delete remaining properties
    if (removeOtherProps) {
      read.singleNode(nodeId, nodeCursor)
      if (nodeCursor.next()) {
        nodeCursor.properties(propertyCursor)
        removeProperties(propertyCursor, propValueMap, queryStatisticsTracker, write.nodeRemoveProperty(nodeId, _))
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
    val propertyId = getPropertyKey(propertyKey, safeValue, token)
    setRelationshipProperty(relationshipId, propertyId, safeValue, token, write, queryStatisticsTracker)
  }

  private def setRelationshipProperty(relationshipId: Long,
                              propertyId: Int,
                              safeValue: Value,
                              token: Token,
                              write: Write,
                              queryStatisticsTracker: MutableQueryStatistics): Unit = {
    if (safeValue == Values.NO_VALUE) {
      if (!(write.relationshipRemoveProperty(relationshipId, propertyId) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      translateException(token, write.relationshipSetProperty(relationshipId, propertyId, safeValue))
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
                                       relCursor: RelationshipScanCursor,
                                       propertyCursor: PropertyCursor,
                                       expression: String): Unit = {
    val propValueMap = transformToPropertyValueMap(propertiesMap, token, read, nodeCursor, relCursor, propertyCursor, expression)
    propValueMap.foreach { case (propertyId, value) => setRelationshipProperty(relationShipId, propertyId, makeValueNeoSafe(value), token, write, queryStatisticsTracker) }

    //delete remaining properties
    if (removeOtherProps) {
      read.singleRelationship(relationShipId, relCursor)
      if (relCursor.next()) {
        relCursor.properties(propertyCursor)
        removeProperties(propertyCursor, propValueMap, queryStatisticsTracker, write.relationshipRemoveProperty(relationShipId, _))
      }
    }
  }

  def removeProperties(propertyCursor: PropertyCursor,
                       propertiesToKeep: mutable.Map[Int, AnyValue],
                       queryStatisticsTracker: MutableQueryStatistics,
                       removeFunc: Int => Unit): Unit = {
    val propertyKeysToKeep = propertiesToKeep.keySet
    val propsToRemove = ArrayBuffer[Int]()
    while (propertyCursor.next()) {
      if (!propertyKeysToKeep.contains(propertyCursor.propertyKey)) {
        propsToRemove.append(propertyCursor.propertyKey())
      }
    }

    propsToRemove.foreach(prop => {
      removeFunc(prop)
      queryStatisticsTracker.setProperty()
    })
  }

  private def safeCastToMap(value: AnyValue): MapValue = value match {
    case mapValue: MapValue => mapValue
    case _ =>
      throw new CypherTypeException(s"Parameter provided for setting properties is not a Map, instead got $value")
  }

  private def transformToPropertyValueMap(value: AnyValue,
                                          token: Token,
                                          read: Read,
                                          nodeCursor: NodeCursor,
                                          relCursor: RelationshipScanCursor,
                                          propertyCursor: PropertyCursor,
                                          expression: String): mutable.Map[Int, AnyValue] = value match {
    case mapValue: MapValue =>
      val map = mutable.Map[Int, AnyValue]()
      mapValue.foreach {
        case (propertyName: String, value: AnyValue) =>
          val propertyKey = getPropertyKey(propertyName, makeValueNeoSafe(value), token)
          if (propertyKey != TokenConstants.NO_TOKEN) {
            map += (propertyKey -> value)
          }
      }
      map
    case vNode: VirtualNodeValue =>
      read.singleNode(vNode.id(), nodeCursor)
      if (nodeCursor.next()) {
        nodeCursor.properties(propertyCursor)
        createPropertyValueMap(propertyCursor)
      } else {
        mutable.Map.empty
      }
    case vRelation: VirtualRelationshipValue =>
      read.singleRelationship(vRelation.id(), relCursor)
      if (relCursor.next()) {
        relCursor.properties(propertyCursor)
        createPropertyValueMap(propertyCursor)
      } else {
        mutable.Map.empty
      }
    case _ =>
      throw new CypherTypeException(s"Expected $expression to be a map, but it was :`$value`")
  }

  private def getPropertyKey(propertyName: String, value: Value, token: Token): Int = {
    if (value == Values.NO_VALUE) {
      token.propertyKey(propertyName)
    } else {
      token.propertyKeyGetOrCreateForName(propertyName)
    }
  }

  private def createPropertyValueMap(propertyCursor: PropertyCursor): mutable.Map[Int, AnyValue] = {
    val map = mutable.Map[Int, AnyValue]()
    while (propertyCursor.next()) {
      map += (propertyCursor.propertyKey() -> propertyCursor.propertyValue())
    }
    map
  }
}