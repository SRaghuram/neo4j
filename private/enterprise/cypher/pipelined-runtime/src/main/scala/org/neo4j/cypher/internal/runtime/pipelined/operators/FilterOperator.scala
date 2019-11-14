/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.storable.Values

/**
  * Takes an input morsel and compacts all rows to the beginning of it, only keeping the rows that match a predicate
  */
class FilterOperator(val workIdentity: WorkIdentity,
                     predicate: Expression) extends StatelessOperator {

  override def operate(readingRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState,
                       resources: QueryResources): Unit = {

    val writingRow = readingRow.shallowCopy()
    val queryState = new OldQueryState(context,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)

    while (readingRow.isValidRow) {
      val matches = predicate(readingRow, queryState) eq Values.TRUE
      if (matches) {
        writingRow.copyFrom(readingRow)
        writingRow.moveToNextRow()
      }
      readingRow.moveToNextRow()
    }

    // We need to set validRows of the provided context
    // to the current row of the local writing context
    readingRow.finishedWritingUsing(writingRow)
  }
}

class FilterOperatorTemplate(val inner: OperatorTaskTemplate,
                             override val id: Id,
                             generatePredicate: () => IntermediateExpression)
                            (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  private var predicate: IntermediateExpression = _

  override def genExpressions: Seq[IntermediateExpression] = Seq(predicate)

  override def genOperate: IntermediateRepresentation = {
    if (predicate != null) {
      throw new IllegalStateException("genOperate must be called first!!")
    }
    predicate = generatePredicate()

    condition(equal(nullCheckIfRequired(predicate), trueValue)) (
      block(
        profileRow(id),
        inner.genOperateWithExpressions
      )
    )
  }

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genFields: Seq[Field] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}
