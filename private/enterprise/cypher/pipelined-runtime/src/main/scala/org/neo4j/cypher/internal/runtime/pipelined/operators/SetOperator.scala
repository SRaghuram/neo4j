/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.fail
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.instanceOf
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.SetOperation
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_WRITE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.LOCKS
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.TOKEN
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.dbHit
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.Locks
import org.neo4j.internal.kernel.api.Token
import org.neo4j.internal.kernel.api.Write
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

class SetOperator(val workIdentity: WorkIdentity,
                  setOperation: SetOperation) extends StatelessOperator {

  var event: OperatorProfileEvent = _
  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    val cursor: MorselFullCursor = morsel.fullCursor()
    var setCount = 0
    while (cursor.next()) {
      setCount += 1
      val sss = setOperation.set(cursor, queryState)
      resources.queryStatisticsTracker.setProperties(sss)
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
    val propertyKeyId = token.propertyKeyGetOrCreateForName(propertyKey)
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      if (!(write.nodeRemoveProperty(nodeId, propertyKeyId) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      translateException(token, write.nodeSetProperty(nodeId, propertyKeyId, safeValue))
      queryStatisticsTracker.setProperty()
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
    val propertyKeyId = token.propertyKeyGetOrCreateForName(propertyKey)
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

  private def safeCastToMap(value: AnyValue): MapValue = value match {
    case mapValue: MapValue => mapValue
    case _ =>
      throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
  }
}

class SetPropertyOperatorTemplate(override val inner: OperatorTaskTemplate,
                                  override val id: Id,
                                  entity: () => IntermediateExpression,
                                  key: String,
                                  value: () => IntermediateExpression)(protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  private var entityValue: IntermediateExpression = _
  private var relationshipProperty: IntermediateExpression = _
  private var nodeProperty: IntermediateExpression = _

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    if (entityValue == null) {
      entityValue = entity()
    }
    if (relationshipProperty == null) {
      // We need two separate IntermediateExpressions to make sure the property value is evaluated
      // at the correct place. If the "value()" contains a "oneTime"-expression, it would otherwise
      // end up with only evaluating the expression in the first condition.
      relationshipProperty = value()
      nodeProperty = value()
    }

    val entityValueVar = codeGen.namer.nextVariableName("entity")
    val relProp = nullCheckIfRequired(relationshipProperty)
    val nodeProp = nullCheckIfRequired(nodeProperty)

    block(
      declareAndAssign(typeRefOf[AnyValue], entityValueVar, nullCheckIfRequired(entityValue)),
      ifElse(instanceOf[VirtualNodeValue](load(entityValueVar)))(
        setProperty(
          isNode = true,
          invoke(cast[VirtualNodeValue](load(entityValueVar)), method[VirtualNodeValue, Long]("id")),
          nodeProp
        )
      )(ifElse(instanceOf[VirtualRelationshipValue](load(entityValueVar)))
      (
        setProperty(
          isNode = false,
          invoke(cast[VirtualRelationshipValue](load(entityValueVar)), method[VirtualRelationshipValue, Long]("id")),
          relProp
        )
      )(
        block(
          condition(notEqual(load(entityValueVar), noValue))(
            fail(newInstance(constructor[InvalidArgumentException, String], constant("Expected to set property on a node or a relationship.")))
          )
        )
      )),
      dbHit(loadField(executionEventField)),
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile),
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq(entityValue, relationshipProperty, nodeProperty)

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V)

  override def genFields: Seq[Field] = Seq(DATA_WRITE, TOKEN, LOCKS)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false

  private def setProperty(isNode: Boolean, id: IntermediateRepresentation, propValue: IntermediateRepresentation): IntermediateRepresentation = {
    val errorVar = codeGen.namer.nextVariableName("errorN")
    val acquireLockFunction = if (isNode) "acquireExclusiveNodeLock" else "acquireExclusiveRelationshipLock"
    val releaseLockFunction = if (isNode) "releaseExclusiveNodeLock" else "releaseExclusiveRelationshipLock"
    val setFunction = if (isNode) "setNodeProperty" else "setRelationshipProperty"

    block(
      invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](acquireLockFunction), arrayOf[Long](id)),
      IntermediateRepresentation.tryCatch[Exception](errorVar)
        (block(
          invokeStatic(
            method[SetOperator, Unit, Long, String, AnyValue, Token, Write, MutableQueryStatistics](setFunction),
            id,
            constant(key),
            propValue,
            loadField(TOKEN),
            loadField(DATA_WRITE),
            QUERY_STATS_TRACKER,
          ),
          invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](releaseLockFunction), arrayOf[Long](id)),
        ))
        (block(
          invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](releaseLockFunction), arrayOf[Long](id)),
          fail(load(errorVar))
        ))
    )
  }
}