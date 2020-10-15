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
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.self
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NEXT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

class ArgumentOperator(val workIdentity: WorkIdentity,
                       argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def toString: String = "Argument"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] =
    singletonIndexedSeq(new OTask(inputMorsel.nextCopy))

  class OTask(val inputMorsel: Morsel) extends ContinuableOperatorTaskWithMorsel {

    override def workIdentity: WorkIdentity = ArgumentOperator.this.workIdentity

    override def toString: String = "ArgumentTask"

    override def operate(outputMorsel: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = {

      val inputCursor = inputMorsel.readCursor()
      val outputCursor = outputMorsel.writeCursor()
      while (outputCursor.next() && inputCursor.next()) {
        outputCursor.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
      }
      outputCursor.truncate()
    }

    override def canContinue: Boolean = false

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
    override protected def closeCursors(resources: QueryResources): Unit = {}
  }
}

class ArgumentOperatorTaskTemplate(override val inner: OperatorTaskTemplate,
                                   override val id: Id,
                                   innermost: DelegateOperatorTaskTemplate,
                                   argumentSize: SlotConfiguration.Size)
                                  (protected val codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {

  // Argument does not support fusing over pipelines, so it is always gonna be the head operator
  override protected val isHead: Boolean = true

  override protected def scopeId: String = "argument" + id.x

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genInit: IntermediateRepresentation = inner.genInit

  override protected def genOperateHead(profile: Boolean): IntermediateRepresentation = {
    block(
      genAdvanceOnCancelledRow,
      loop(and(invoke(self(), method[ContinuableOperatorTask, Boolean]("canContinue")), innermost.predicate))(
        block(
          innermost.resetBelowLimitAndAdvanceToNextArgument,
          codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
          inner.genOperateWithExpressions(profile),
          // Else if no inner operator can proceed we move to the next input row
          doIfInnerCantContinue(block(invokeSideEffect(INPUT_CURSOR, NEXT),  profileRow(id, profile))),
          innermost.resetCachedPropertyVariables
        )
      )
      )
  }

  override protected def genOperateMiddle(profile: Boolean): IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile Input as middle operator")
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty[IntermediateExpression]

  override def genFields: Seq[Field] = Seq.empty[Field]

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty[LocalVariable]

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, INPUT_ROW_IS_VALID)).orElse(Some(INPUT_ROW_IS_VALID))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genClearStateOnCancelledRow: IntermediateRepresentation = noop()
}
