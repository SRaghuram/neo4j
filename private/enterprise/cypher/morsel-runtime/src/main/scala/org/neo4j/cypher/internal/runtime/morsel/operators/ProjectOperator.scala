/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.{NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession

class ProjectOperator(val workIdentity: WorkIdentity,
                      val projectionOps: CommandProjection) extends StatelessOperator {

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
      projectionOps.project(currentRow, queryState)
      currentRow.moveToNextRow()
    }
  }
}

class ProjectOperatorTemplate(override val inner: OperatorTaskTemplate,
                              override val id: Id,
                              projectionOps: Map[String, Expression])(codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  private var projections: IntermediateExpression = _

  override def genOperate: IntermediateRepresentation = {
    projections = codeGen.intermediateCompileProjection(projectionOps).getOrElse(throw new CantCompileQueryException())
    block(
      projections.ir,
      profileRow(id),
      inner.genOperate
      )
  }

  override def genLocalVariables: Seq[LocalVariable] = {
    projections.variables ++ inner.genLocalVariables
  }

  override def genFields: Seq[Field] = {
    projections.fields ++ inner.genFields
  }

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

