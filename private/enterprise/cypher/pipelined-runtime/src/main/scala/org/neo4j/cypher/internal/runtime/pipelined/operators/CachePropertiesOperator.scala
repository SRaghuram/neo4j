/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

class CachePropertiesOperator(val workIdentity: WorkIdentity,
                              val properties: Array[commands.expressions.Expression]) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    val cursor = morsel.readCursor()
    while (cursor.next()) {
      var i = 0
      while (i < properties.length) {
        properties(i)(cursor, queryState)
        i += 1
      }
    }
  }
}

class CachePropertiesOperatorTemplate(override val inner: OperatorTaskTemplate,
                                      override val id: Id,
                                      propertyOps: Seq[Expression])(protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  private var properties: Seq[IntermediateExpression] = _

  override def genOperate: IntermediateRepresentation = {
    if(properties == null) {
      properties =
        propertyOps.map(p => codeGen.compileExpression(p, id)
          .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $p"))
        )
    }
    val irs = properties.map(_.ir) ++ Seq(inner.genOperateWithExpressions, conditionallyProfileRow(innerCannotContinue, id, doProfile))
    block(irs:_*)
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = properties

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genFields: Seq[Field] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false
}

