/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.labeledLoop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.self
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTER_LOOP_LABEL_NAME
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_MOVE_TO_NEXT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

class ArgumentOperator(val workIdentity: WorkIdentity,
                       argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def toString: String = "Argument"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    IndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: MorselCypherRow) extends ContinuableOperatorTaskWithMorsel {

    override def workIdentity: WorkIdentity = ArgumentOperator.this.workIdentity

    override def toString: String = "ArgumentTask"

    override def operate(outputRow: MorselCypherRow,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      while (outputRow.isValidRow && inputMorsel.isValidRow) {
        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        inputMorsel.moveToNextRow()
        outputRow.moveToNextRow()
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = false

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
    override protected def closeCursors(resources: QueryResources): Unit = {}
  }
}

class ArgumentOperatorTaskTemplate(override val inner: OperatorTaskTemplate,
                                   override val id: Id,
                                   innermost: DelegateOperatorTaskTemplate,
                                   argumentSize: SlotConfiguration.Size,
                                   final override protected val isHead: Boolean = true)
                                  (protected val codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {

  override protected def scopeId: String = "argument" + id.x

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genInit: IntermediateRepresentation = inner.genInit

  override protected def genOperateHead: IntermediateRepresentation = {
    block(
      labeledLoop(OUTER_LOOP_LABEL_NAME, and(invoke(self(), method[ContinuableOperatorTask, Boolean]("canContinue")), innermost.predicate))(
        block(
          codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
          inner.genOperateWithExpressions,
          // Else if no inner operator can proceed we move to the next input row
          doIfInnerCantContinue(block(INPUT_ROW_MOVE_TO_NEXT,  profileRow(id))),
          innermost.resetCachedPropertyVariables
        )
      )
    )
  }

  override protected def genOperateMiddle: IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile Input as middle operator")
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty[IntermediateExpression]

  override def genFields: Seq[Field] = Seq.empty[Field]

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty[LocalVariable]

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, INPUT_ROW_IS_VALID)).orElse(Some(INPUT_ROW_IS_VALID))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}
