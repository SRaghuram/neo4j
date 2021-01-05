/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

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
import org.neo4j.cypher.internal.macros.AssertMacros
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
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactoryFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException

/**
 * Operator that inputs [[MorselData]] from a buffer.
 */
abstract class InputFromBufferWithAsmOperator[S <: ArgumentState](val argumentStateMapId: ArgumentStateMapId,
                                                                  val argumentStateFactoryFactory: ArgumentStateFactoryFactory[S],
                                                                  val ordered: Boolean,
                                                                  val id: Id = Id.INVALID_ID) extends Operator with OperatorState {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    argumentStateCreator.createArgumentStateMap(
      argumentStateMapId,
      argumentStateFactoryFactory.createFactory(stateFactory, id.x),
      stateFactory.newMemoryTracker(id.x),
      ordered
    )
    this
  }
}

/**
 * Operator that inputs [[MorselData]] from a buffer.
 */
class InputMorselDataFromBufferOperator[S <: ArgumentState](val operatorName: String,
                                                            val workIdentity: WorkIdentity,
                                                            argumentStateMapId: ArgumentStateMapId,
                                                            argumentStateFactoryFactory: ArgumentStateFactoryFactory[S],
                                                            ordered: Boolean)
                                                           (id: Id = Id.INVALID_ID)
  extends InputFromBufferWithAsmOperator(argumentStateMapId, argumentStateFactoryFactory, ordered, id) with DataInputOperatorState[MorselData] {

  override def toString: String = s"InputMorselDataFromBuffer($operatorName)"

  override def nextTasks(state: PipelinedQueryState,
                         input: MorselData,
                         argumentStateMaps: ArgumentStateMap.ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    singletonIndexedSeq(new InputFromBufferOperatorTask(workIdentity, input.readCursor()) with ContinuableOperatorTaskWithMorselData {
      override val morselData: MorselData = input
    })
  }
}

/**
 * Operator that inputs a single [[org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator]] from a buffer.
 */
class InputSingleAccumulatorFromMorselArgumentStateBufferOperator[S <: ArgumentState](val operatorName: String,
                                                                                      val workIdentity: WorkIdentity,
                                                                                      argumentStateMapId: ArgumentStateMapId,
                                                                                      argumentStateFactoryFactory: ArgumentStateFactoryFactory[S],
                                                                                      ordered: Boolean)
                                                                                     (id: Id = Id.INVALID_ID)
  extends InputFromBufferWithAsmOperator(argumentStateMapId, argumentStateFactoryFactory, ordered, id)
    with AccumulatorsInputOperatorState[Morsel, ArgumentStateBuffer] {

  override def accumulatorsPerTask(morselSize: Int): Int = 1

  override def toString: String = s"InputMorselDataFromBuffer($operatorName)"

  override def nextTasks(state: PipelinedQueryState,
                         input: IndexedSeq[ArgumentStateBuffer],
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithAccumulators[Morsel, ArgumentStateBuffer]] = {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(input.size == 1)
    singletonIndexedSeq(new InputFromBufferOperatorTask(workIdentity, input.head.readCursor())
      with ContinuableOperatorTaskWithAccumulators[Morsel, ArgumentStateBuffer] {
      override val accumulators: IndexedSeq[ArgumentStateBuffer] = input
    })
  }
}

/**
 * Serial operator that inputs [[Morsel]]s from an eager buffer (that needs an argument state map to track progress)
 */
class InputMorselFromEagerBufferOperator[S <: ArgumentState](val operatorName: String,
                                                             val workIdentity: WorkIdentity,
                                                             argumentStateMapId: ArgumentStateMapId,
                                                             argumentStateFactoryFactory: ArgumentStateFactoryFactory[S],
                                                             ordered: Boolean)
                                                            (id: Id = Id.INVALID_ID)
  extends InputFromBufferWithAsmOperator(argumentStateMapId, argumentStateFactoryFactory, ordered, id) with MorselInputOperatorState {

  override def toString: String = s"InputMorselFromEagerBuffer($operatorName)"

  override def nextTasks(state: PipelinedQueryState,
                         inputMorselParallelizer: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources,
                         argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    // TODO: Support parallelism parameter

    val input = inputMorselParallelizer.nextCopy
    singletonIndexedSeq(new InputFromBufferOperatorTask(workIdentity, input.readCursor()) with ContinuableOperatorTaskWithMorsel {
      override val inputMorsel: Morsel = input
    })
  }
}

abstract class InputFromBufferOperatorTask(override val workIdentity: WorkIdentity, val inputCursor: MorselReadCursor) extends ContinuableOperatorTask {

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
}

abstract class InputFromBufferOperatorTaskTemplate(override val inner: OperatorTaskTemplate,
                                                   override val id: Id,
                                                   innermost: DelegateOperatorTaskTemplate)
                                                  (protected val codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskTemplate {
  override protected def scopeId: String = "inputFromBuffer" + id.x

  override protected def genOperateHead: IntermediateRepresentation = {
    block(
      genAdvanceOnCancelledRow,
      loop(and(invoke(self[ContinuableOperatorTask], method[ContinuableOperatorTask, Boolean]("canContinue")), innermost.predicate))(
        block(
          innermost.resetBelowLimitAndAdvanceToNextArgument,
          codeGen.copyFromInput(Math.min(codeGen.inputSlotConfiguration.numberOfLongs, codeGen.slots.numberOfLongs),
                                Math.min(codeGen.inputSlotConfiguration.numberOfReferences, codeGen.slots.numberOfReferences)),
          inner.genOperateWithExpressions,
          // Else if no inner operator can proceed we move to the next input row
          doIfInnerCantContinue(block(invokeSideEffect(INPUT_CURSOR, NEXT),  profileRow(id, doProfile))),
          innermost.resetCachedPropertyVariables
        )
      )
    )
  }

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  // NOTE: genFields is implemented by specialized traits and should not be overridden here

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, INPUT_ROW_IS_VALID)).orElse(Some(INPUT_ROW_IS_VALID))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = true

  override protected def genOperateMiddle: IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile InputFromBufferOperator as middle operator")
  }

  override protected def genClearStateOnCancelledRow: IntermediateRepresentation = noop()
}

class InputMorselFromEagerBufferOperatorTaskTemplate(inner: OperatorTaskTemplate, id: Id, innermost: DelegateOperatorTaskTemplate)
                                                    (codeGen: OperatorExpressionCompiler)
  extends InputFromBufferOperatorTaskTemplate(inner, id, innermost)(codeGen) with ContinuableOperatorTaskWithMorselTemplate

class InputMorselDataFromBufferOperatorTaskTemplate(inner: OperatorTaskTemplate, id: Id, innermost: DelegateOperatorTaskTemplate)
                                                   (codeGen: OperatorExpressionCompiler)
  extends InputFromBufferOperatorTaskTemplate(inner, id, innermost)(codeGen) with ContinuableOperatorTaskWithMorselDataTemplate

class InputSingleAccumulatorFromMorselArgumentStateBufferOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                                              id: Id,
                                                                              innermost: DelegateOperatorTaskTemplate)
                                                                             (codeGen: OperatorExpressionCompiler)
  extends InputFromBufferOperatorTaskTemplate(inner, id, innermost)(codeGen)
    with ContinuableOperatorTaskWithSingleAccumulatorTemplate