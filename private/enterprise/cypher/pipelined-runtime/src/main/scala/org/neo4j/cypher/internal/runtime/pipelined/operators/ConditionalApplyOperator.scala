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
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_MORSEL_FIELD
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NEXT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.values.storable.Values.NO_VALUE

/**
 * ConditionalApplyOperator is not really a full operator, most of the heavy lifting is done
 * by ConditionalSink. ConditionalApplyOperator is a variant of Argument which
 * sets all RHS-introduced identifiers to NO_VALUE on the LHS morsel.
 */
class ConditionalApplyOperator(val workIdentity: WorkIdentity,
                               lhsSlotConfig: SlotConfiguration,
                               rhsSlotConfig: SlotConfiguration) extends StreamingOperator {

  override def toString: String = "ConditionalApply"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    val morsel = inputMorsel.nextCopy
    val task =
      if (morsel.slots eq lhsSlotConfig) new ConditionalApplyLHSTask(morsel, workIdentity, lhsSlotConfig, rhsSlotConfig)
      else if (morsel.slots eq rhsSlotConfig) new ConditionalApplyRHSTask(morsel, workIdentity, rhsSlotConfig.size())
      else throw new IllegalStateException(s"Unknown slot configuration in UnionOperator. Got: ${morsel.slots}. LHS: $lhsSlotConfig. RHS: $rhsSlotConfig")

    singletonIndexedSeq(task)
  }
}

abstract class ConditionalApplyTask(val inputMorsel: Morsel,
                                    val workIdentity: WorkIdentity) extends ContinuableOperatorTaskWithMorsel {
  override def toString: String = "ConditionalApplyTask"
  override def canContinue: Boolean = false
  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  override protected def closeCursors(resources: QueryResources): Unit = {}
}

class ConditionalApplyLHSTask(inputMorsel: Morsel,
                              workIdentity: WorkIdentity,
                              lhsSlotConfig: SlotConfiguration,
                              rhsSlotConfig: SlotConfiguration) extends ConditionalApplyTask(inputMorsel, workIdentity){


  private val refsToCopy = computeRefsToCopy()

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val inputCursor = inputMorsel.readCursor()
    val outputCursor = outputMorsel.writeCursor()
    val lhsSize = lhsSlotConfig.size()
    val rhsSize = lhsSlotConfig.size()

    while (outputCursor.next() && inputCursor.next()) {
      outputCursor.copyFrom(inputCursor, lhsSize.nLongs, lhsSize.nReferences)
      var offset = lhsSize.nLongs
      while (offset < rhsSize.nLongs) {
        outputCursor.setLongAt(offset, -1)
        offset += 1
      }
      var i = 0
      while (i < refsToCopy.length) {
        outputCursor.setRefAt(refsToCopy(i), NO_VALUE)
        i += 1
      }
    }
    outputCursor.truncate()
  }

  private def computeRefsToCopy(): Array[Int] = {
    val refsToCopy = mutable.ArrayBuilder.make[Int]
    rhsSlotConfig.foreachSlotAndAliasesOrdered {
      case SlotWithKeyAndAliases(VariableSlotKey(_), RefSlot(offset, _, _), _) if offset >= lhsSlotConfig.size().nReferences =>
        refsToCopy += offset
      case _ => //Do nothing
    }
    refsToCopy.result()
  }
}

class ConditionalApplyRHSTask(inputMorsel: Morsel,
                              workIdentity: WorkIdentity,
                              rhsSize: Size) extends ConditionalApplyTask(inputMorsel, workIdentity) {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    val inputCursor = inputMorsel.readCursor()
    val outputCursor = outputMorsel.writeCursor()

    while (outputCursor.next() && inputCursor.next()) {
      outputCursor.copyFrom(inputCursor, rhsSize.nLongs, rhsSize.nReferences)
    }
    outputCursor.truncate()
  }
}

class ConditionalOperatorTaskTemplate(override val inner: OperatorTaskTemplate,
                                      override val id: Id,
                                      innermost: DelegateOperatorTaskTemplate,
                                      lhsSize: SlotConfiguration.Size,
                                      rhsSize: SlotConfiguration.Size)
                                     (protected val codeGen: OperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {

  override protected val isHead: Boolean = true

  override protected def scopeId: String = "conditionalApply" + id.x

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genInit: IntermediateRepresentation = inner.genInit
  override protected def genOperateHead: IntermediateRepresentation = {
    block(
      genAdvanceOnCancelledRow,
      loop(and(invoke(self(), method[ContinuableOperatorTask, Boolean]("canContinue")), innermost.predicate))(
        block(
          innermost.resetBelowLimitAndAdvanceToNextArgument,
          ifElse(invokeStatic(
            method[ConditionalOperatorTaskTemplate, Boolean, Morsel, Int, Int]("compareSize"),
            loadField(INPUT_MORSEL_FIELD), constant(lhsSize.nLongs), constant(lhsSize.nReferences))){
            block(
              block((0 until lhsSize.nLongs).map(offset => codeGen.setLongAt(offset, codeGen.getLongFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block((0 until lhsSize.nReferences).map(offset => codeGen.setRefAt(offset, codeGen.getRefFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block((lhsSize.nLongs until rhsSize.nLongs).map(offset => codeGen.setLongAt(offset, constant(-1L))): _*),
              block((lhsSize.nReferences until rhsSize.nReferences).map(offset => codeGen.setRefAt(offset, noValue)): _*)
            )
          }{
            block(
              block((0 until rhsSize.nLongs).map(offset => codeGen.setLongAt(offset, codeGen.getLongFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block((0 until rhsSize.nReferences).map(offset => codeGen.setRefAt(offset, codeGen.getRefFromExecutionContext(offset, INPUT_CURSOR))): _*)
            )
          },
          inner.genOperateWithExpressions,
          // Else if no inner operator can proceed we move to the next input row
          doIfInnerCantContinue(block(invokeSideEffect(INPUT_CURSOR, NEXT),  profileRow(id))),
          innermost.resetCachedPropertyVariables
        )
      )
    )
  }

  override protected def genOperateMiddle: IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile ConditionalApply as middle operator")
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty[IntermediateExpression]

  override def genFields: Seq[Field] = Seq(INPUT_MORSEL_FIELD)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty[LocalVariable]

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, INPUT_ROW_IS_VALID)).orElse(Some(INPUT_ROW_IS_VALID))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genClearStateOnCancelledRow: IntermediateRepresentation = noop()
}

object ConditionalOperatorTaskTemplate {
  def compareSize(a: Morsel, nLongs: Int, nReferences: Int): Boolean = {
    val size = a.slots.size()
    nLongs == size.nLongs && nReferences == size.nReferences
  }
}

