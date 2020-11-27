/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
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
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.SetPropertyOperator.setRelationshipProperty
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.values.AnyValue
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

      resolvedEntity match {
        case node: VirtualNodeValue => SetPropertyOperator.setNodeProperty(
          node.id(),
          propertyKey,
          propertyValue.apply(cursor, queryState),
          tokenWrite,
          write,
          resources.queryStatisticsTracker
        )
        case relationship: VirtualRelationshipValue => setRelationshipProperty(
          relationship.id(),
          propertyKey,
          propertyValue.apply(cursor, queryState),
          tokenWrite,
          write,
          resources.queryStatisticsTracker
        )
        case _ => throw new InvalidArgumentException(
          s"The expression $entity should have been a node or a relationship, but got $resolvedEntity")
      }
    }
  }
}

object SetPropertyOperator {
  def setNodeProperties(nodeId: Long,
                        propertiesMap: AnyValue,
                      tokenWrite: TokenWrite,
                      write: Write,
                      queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(propertiesMap).foreach((k: String, v: AnyValue) => setNodeProperty(nodeId, k, v, tokenWrite, write, queryStatisticsTracker))
  }

  def setNodeProperty(nodeId: Long,
                      propertyKey: String,
                      propertyValue: AnyValue,
                      tokenWrite: TokenWrite,
                      write: Write,
                      queryStatisticsTracker: MutableQueryStatistics): Unit = {
    if (!(propertyValue eq NO_VALUE)) {
      val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(propertyKey)
      write.nodeSetProperty(nodeId, propertyKeyId, makeValueNeoSafe(propertyValue))
      queryStatisticsTracker.setProperty()
    }
  }

  def setRelationshipProperties(relationshipId: Long,
                                propertiesMap: AnyValue,
                                tokenWrite: TokenWrite,
                                write: Write,
                                queryStatisticsTracker: MutableQueryStatistics): Unit = {
    safeCastToMap(propertiesMap).foreach((k: String, v: AnyValue) => setRelationshipProperty(relationshipId, k, v, tokenWrite, write, queryStatisticsTracker))
  }

  def setRelationshipProperty(relationshipId: Long,
                              propertyKey: String,
                              propertyValue: AnyValue,
                              tokenWrite: TokenWrite,
                              write: Write,
                              queryStatisticsTracker: MutableQueryStatistics): Unit = {
    if (!(propertyValue eq NO_VALUE)) {
      val propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName(propertyKey)
      write.relationshipSetProperty(relationshipId, propertyKeyId, makeValueNeoSafe(propertyValue))
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
  private var propertyValue: IntermediateExpression = _

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    if(entityValue == null) {
      entityValue = entity()
    }
    if(propertyValue == null) {
      propertyValue = value()
    }

    val propertyValueVar = codeGen.namer.nextVariableName("start")
    val entityValueVar = codeGen.namer.nextVariableName("end")

    block(
      declareAndAssign(typeRefOf[AnyValue], propertyValueVar, propertyValue.ir),
      declareAndAssign(typeRefOf[AnyValue], entityValueVar, entityValue.ir),
      ifElse(instanceOf[VirtualNodeValue](load(entityValueVar)))(
        invokeStatic(
          method[SetPropertyOperator, Unit, Long, String, AnyValue, TokenWrite, Write, MutableQueryStatistics]("setNodeProperty"),
          invoke(cast[VirtualNodeValue](load(entityValueVar)), method[VirtualNodeValue, Long]("id")),
          constant(key),
          load(propertyValueVar),
          loadField(TOKEN_WRITE),
          loadField(DATA_WRITE),
          QUERY_STATS_TRACKER
        )
      )(block(
        ifElse(instanceOf[VirtualRelationshipValue](load(entityValueVar)))
        (invokeStatic(
          method[SetPropertyOperator, Unit, Long, String, AnyValue, TokenWrite, Write, MutableQueryStatistics]("setRelationshipProperty"),
          invoke(cast[VirtualRelationshipValue](load(entityValueVar)), method[VirtualRelationshipValue, Long]("id")),
          constant(key),
          load(propertyValueVar),
          loadField(TOKEN_WRITE),
          loadField(DATA_WRITE),
          QUERY_STATS_TRACKER
        ))
        (fail(newInstance(constructor[IllegalStateException, String], constant(s"Properties can only be set on nodes or relationships."))))
      )),
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