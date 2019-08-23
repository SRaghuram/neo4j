/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.internal.kernel.api.IndexReadSession

class CachePropertiesOperator(val workIdentity: WorkIdentity,
                              val properties: Array[commands.expressions.Expression]) extends StatelessOperator {

  override def operate(currentRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {

    val queryState = new OldQueryState(context,
                                       resources = null,
                                       params = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession],
                                       resources.expressionVariables(state.nExpressionSlots),
                                       state.subscriber,
                                       NoMemoryTracker)

    while (currentRow.isValidRow) {
      var i = 0
      while (i < properties.length) {
        properties(i)(currentRow, queryState)
        i += 1
      }
      currentRow.moveToNextRow()
    }
  }
}

class CachePropertiesOperatorTemplate(override val inner: OperatorTaskTemplate,
                                      override val id: Id,
                                      propertyOps: Seq[Expression])(codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  private var properties: Seq[IntermediateExpression] = _

  override def genOperate: IntermediateRepresentation = {
    properties =
      propertyOps.map(p => codeGen.intermediateCompileExpression(p)
        .getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $p"))
      )
    val irs = properties.map(_.ir) ++ Seq(profileRow(id), inner.genOperateWithExpressions)
    block(irs:_*)
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = properties

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genFields: Seq[Field] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

