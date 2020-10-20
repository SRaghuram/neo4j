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
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NEXT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStreamArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

/**
 * Operator that preserves order by using an ordered input buffer.
 */
class PreserveOrderOperator(val argumentStateMapId: ArgumentStateMapId,
                            val workIdentity: WorkIdentity)
                            (val id: Id = Id.INVALID_ID) extends Operator with DataInputOperatorState[MorselData] {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(argumentStateMapId, new ArgumentStreamArgumentStateBuffer.Factory(stateFactory, id), ordered = true)
    this
  }

  override def toString: String = "PreserveOrderOperator"

  override def nextTasks(state: PipelinedQueryState,
                         input: MorselData,
                         argumentStateMaps: ArgumentStateMap.ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    singletonIndexedSeq(new OTask(input))
  }

  class OTask(val morselData: MorselData) extends ContinuableOperatorTaskWithMorselData {

    private val inputCursor: MorselReadCursor = morselData.readCursor()

    /*
     * We only copy data here, ordering is handled by the input buffer.
     */
    override def operate(output: Morsel, state: PipelinedQueryState, resources: QueryResources): Unit = {
      val outputCursor = output.writeCursor()
      while (outputCursor.next() && inputCursor.next()) {
        outputCursor.copyAllFrom(inputCursor)
      }
      outputCursor.truncate()
    }

    override def canContinue: Boolean = inputCursor.hasNext

    override protected def closeCursors(resources: QueryResources): Unit = {}

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

    override def workIdentity: WorkIdentity = PreserveOrderOperator.this.workIdentity
  }
}

class PreserveOrderOperatorTaskTemplate(override val inner: OperatorTaskTemplate,
                                        override val id: Id,
                                        innermost: DelegateOperatorTaskTemplate)
                                       (protected val codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselDataTemplate {

  override protected def genOperateHead: IntermediateRepresentation = {
    block(
      genAdvanceOnCancelledRow,
      loop(and(invoke(self(), method[ContinuableOperatorTask, Boolean]("canContinue")), innermost.predicate))(
        block(
          innermost.resetBelowLimitAndAdvanceToNextArgument,
          codeGen.copyFromInput(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.inputSlotConfiguration.numberOfReferences),
          inner.genOperateWithExpressions,
          // Else if no inner operator can proceed we move to the next input row
          doIfInnerCantContinue(block(invokeSideEffect(INPUT_CURSOR, NEXT),  profileRow(id))),
          innermost.resetCachedPropertyVariables
        )
      )
    )
  }

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genFields: Seq[Field] = super.genFields

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, INPUT_ROW_IS_VALID)).orElse(Some(INPUT_ROW_IS_VALID))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = true

  override protected def genOperateMiddle: IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile PreserveOrderOperator as middle operator")
  }

  override protected def genClearStateOnCancelledRow: IntermediateRepresentation = noop()
}