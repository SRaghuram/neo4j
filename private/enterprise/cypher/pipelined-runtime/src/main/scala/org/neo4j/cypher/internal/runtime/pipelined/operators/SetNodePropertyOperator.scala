/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
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
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.TokenWrite
import org.neo4j.internal.kernel.api.Write
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.NodeReference
import org.neo4j.values.virtual.NodeValue

class SetNodePropertyOperator(val workIdentity: WorkIdentity,
                              idName: String,
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
      val nodeOption = cursor.getByName(idName) match {
        case n: NodeValue => Some(n.id())
        case l: NodeReference => Some(l.id())
        case IsNoValue() => None
        case x => throw new InternalException(s"Expected to find a node at '$idName' but found instead: $x")
      }

      nodeOption.map(node => SetPropertyOperator.setNodeProperty(
        node,
        propertyKey,
        propertyValue.apply(cursor, queryState),
        tokenWrite,
        write,
        resources.queryStatisticsTracker
      ))
    }
  }
}

class SetNodePropertyOperatorTemplate(override val inner: OperatorTaskTemplate,
                                      override val id: Id,
                                      offset: Int,
                                      key: String,
                                      value: () => IntermediateExpression)(protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  private var propertyValue: IntermediateExpression = _

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    if (propertyValue == null) {
      propertyValue = value()
    }

    val entityId = codeGen.namer.nextVariableName("entity")

    block(
      declareAndAssign(typeRefOf[Long], entityId, codeGen.getLongAt(offset)),
      condition(notEqual(load(entityId), constant(-1L)))(
        invokeStatic(
          method[SetPropertyOperator, Unit, Long, String, AnyValue, TokenWrite, Write, MutableQueryStatistics]("setNodeProperty"),
          load(entityId),
          constant(key),
          nullCheckIfRequired(propertyValue),
          loadField(TOKEN_WRITE),
          loadField(DATA_WRITE),
          QUERY_STATS_TRACKER
        )),
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile),
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq(propertyValue)

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V)

  override def genFields: Seq[Field] = Seq(DATA_WRITE, TOKEN_WRITE)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false
}