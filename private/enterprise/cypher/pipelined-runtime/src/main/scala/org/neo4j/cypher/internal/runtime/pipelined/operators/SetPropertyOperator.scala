/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_WRITE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.TOKEN_WRITE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

class SetPropertyOperator(val workIdentity: WorkIdentity,
                          entity: commands.expressions.Expression,
                          propertyKey: String,
                          propertyValue: commands.expressions.Expression) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    val write = state.query.transactionalContext.dataWrite
    val tokenWrite = state.query.transactionalContext.transaction.tokenWrite()

    val cursor: MorselFullCursor = morsel.fullCursor()
    while (cursor.next()) {
      val resolvedEntity = entity.apply(cursor, queryState)

      SetPropertyOperator.setProperty(
        resolvedEntity,
        propertyKey,
        propertyValue.apply(cursor, queryState),
        tokenWrite,
        write,
        resources.queryStatisticsTracker
      )
    }
  }
}

object SetPropertyOperator {
  def addNodeProperties(nodeId: Long,
                        propertiesMap: AnyValue,
                        tokenWrite: TokenWrite,
                        write: Write,
                        queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(propertiesMap)
      .foreach {
        case (k: String, v: AnyValue) if !(v eq NO_VALUE) => setNodeProperty(nodeId, k, v, tokenWrite, write, queryStatisticsTracker)
        case _ =>
      }
  }

  def setNodeProperty(nodeId: Long,
                      propertyKey: String,
                      propertyValue: AnyValue,
                      tokenWrite: TokenWrite,
                      write: Write,
                      queryStatisticsTracker: MutableQueryStatistics): Unit = {
    val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(propertyKey)
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      if (!(write.nodeRemoveProperty(nodeId, propertyKeyId) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      write.nodeSetProperty(nodeId, propertyKeyId, safeValue)
      queryStatisticsTracker.setProperty()
    }
  }

  def addRelationshipProperties(relationshipId: Long,
                                propertiesMap: AnyValue,
                                tokenWrite: TokenWrite,
                                write: Write,
                                queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(propertiesMap)
      .foreach {
        case (k: String, v: AnyValue) if !(v eq NO_VALUE) => setRelationshipProperty(relationshipId, k, v, tokenWrite, write, queryStatisticsTracker)
        case _ =>
      }
  }

  def setRelationshipProperty(relationshipId: Long,
                              propertyKey: String,
                              propertyValue: AnyValue,
                              tokenWrite: TokenWrite,
                              write: Write,
                              queryStatisticsTracker: MutableQueryStatistics): Unit = {
    val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(propertyKey)
    val safeValue = makeValueNeoSafe(propertyValue)
    if (safeValue == Values.NO_VALUE) {
      if (!(write.relationshipRemoveProperty(relationshipId, propertyKeyId) eq Values.NO_VALUE)) {
        queryStatisticsTracker.setProperty()
      }
    } else {
      write.relationshipSetProperty(relationshipId, propertyKeyId, safeValue)
      queryStatisticsTracker.setProperty()
    }
  }

  def setProperty(entity: AnyValue,
                  propertyKey: String,
                  propertyValue: AnyValue,
                  tokenWrite: TokenWrite,
                  write: Write,
                  queryStatisticsTracker: MutableQueryStatistics): Unit = {
    entity match {
      case node: VirtualNodeValue => SetPropertyOperator.setNodeProperty(
        node.id(),
        propertyKey,
        propertyValue,
        tokenWrite,
        write,
        queryStatisticsTracker
      )
      case relationship: VirtualRelationshipValue => setRelationshipProperty(
        relationship.id(),
        propertyKey,
        propertyValue,
        tokenWrite,
        write,
        queryStatisticsTracker
      )
      case IsNoValue() => // Do nothing
      case _ => throw new InvalidArgumentException(
        s"Expected to set property on a node or a relationship, but got $entity")
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
  private var propertyValue: IntermediateExpression = _

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    if (entityValue == null) {
      entityValue = entity()
    }
    if (propertyValue == null) {
      propertyValue = value()
    }

    val propertyValueVar = codeGen.namer.nextVariableName("start")
    val entityValueVar = codeGen.namer.nextVariableName("end")

    block(
      declareAndAssign(typeRefOf[AnyValue], propertyValueVar, nullCheckIfRequired(propertyValue)),
      declareAndAssign(typeRefOf[AnyValue], entityValueVar, nullCheckIfRequired(entityValue)),
      invokeStatic(
        method[SetPropertyOperator, Unit, AnyValue, String, AnyValue, TokenWrite, Write, MutableQueryStatistics]("setProperty"),
        load(entityValueVar),
        constant(key),
        load(propertyValueVar),
        loadField(TOKEN_WRITE),
        loadField(DATA_WRITE),
        QUERY_STATS_TRACKER
      ),
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile),
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq(entityValue, propertyValue)

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V)

  override def genFields: Seq[Field] = Seq(DATA_WRITE, TOKEN_WRITE)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false
}