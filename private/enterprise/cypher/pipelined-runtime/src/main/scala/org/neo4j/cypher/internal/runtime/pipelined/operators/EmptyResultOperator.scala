/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Takes an input morsel and compacts all rows to the beginning of it, only keeping the rows that match a predicate
 */
class EmptyResultOperator(val workIdentity: WorkIdentity) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val writeCursor = morsel.writeCursor(onFirstRow = true)
    writeCursor.truncate()
  }
}

class EmptyResultOperatorTemplate(val inner: OperatorTaskTemplate,
                                  override val id: Id)
                                 (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    // Do nothing. We still need to call genOperate on inner since other generation methods where we cannot block
    // inner recursion depend on it having been called first
    inner.genOperateWithExpressions // Recurse into inner but discard the generated code
    noop()
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty[IntermediateExpression]

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty[LocalVariable]

  override def genFields: Seq[Field] = Seq.empty[Field]

  override def genCanContinue: Option[IntermediateRepresentation] = Some(constant(false))

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override protected def isHead: Boolean = false
}
