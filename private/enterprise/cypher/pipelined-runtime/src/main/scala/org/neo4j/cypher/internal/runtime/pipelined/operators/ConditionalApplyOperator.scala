/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.fail
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.self
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_MORSEL
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

import scala.collection.mutable

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
      if (morsel.slots eq lhsSlotConfig) new ConditionalApplyLHSTask(morsel, workIdentity, lhsSlotConfig, rhsSlotConfig, computeRefsToNullOnLHS())
      else if (morsel.slots eq rhsSlotConfig) new ConditionalApplyRHSTask(morsel, workIdentity, rhsSlotConfig.size())
      else throw new IllegalStateException(s"Unknown slot configuration in UnionOperator. Got: ${morsel.slots}. LHS: $lhsSlotConfig. RHS: $rhsSlotConfig")

    singletonIndexedSeq(task)
  }

  //This computes the reference on the RHS that are not on the lhs that needs to be nulled.
  //NOTE: we should not include cached properties here since that will break cached property lookup
  private def computeRefsToNullOnLHS(): Array[Int] = {
    val refsToCopy = mutable.ArrayBuilder.make[Int]
    rhsSlotConfig.foreachSlotAndAliasesOrdered {
      case SlotWithKeyAndAliases(VariableSlotKey(_), RefSlot(offset, _, _), _) if offset >= lhsSlotConfig.size().nReferences =>
        refsToCopy += offset
      case _ => //Do nothing
    }
    refsToCopy.result()
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
                              rhsSlotConfig: SlotConfiguration,
                              refsToNull: Array[Int]) extends ConditionalApplyTask(inputMorsel, workIdentity){

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val inputCursor = inputMorsel.readCursor()
    val outputCursor = outputMorsel.writeCursor()
    val lhsSize = lhsSlotConfig.size()
    val rhsSize = rhsSlotConfig.size()

    while (outputCursor.next() && inputCursor.next()) {
      outputCursor.copyFrom(inputCursor, lhsSize.nLongs, lhsSize.nReferences)
      var offset = lhsSize.nLongs
      while (offset < rhsSize.nLongs) {
        outputCursor.setLongAt(offset, -1)
        offset += 1
      }
      var i = 0
      while (i < refsToNull.length) {
        outputCursor.setRefAt(refsToNull(i), NO_VALUE)
        i += 1
      }
    }
    outputCursor.truncate()
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
                                      lhsSlots: SlotConfiguration,
                                      rhsSlots: SlotConfiguration)
                                     (protected val codeGen: BinaryOperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {

  private val lhsSlotsConfigsFused: StaticField = staticConstant[SlotConfiguration](codeGen.namer.nextVariableName("lhsSlotConfig"), lhsSlots)
  private val rhsSlotsConfigsFused: StaticField = staticConstant[SlotConfiguration](codeGen.namer.nextVariableName("rhsSlotConfig"), rhsSlots)

  override protected val isHead: Boolean = true

  override protected def scopeId: String = "conditionalApply" + id.x

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)


  override def genInit: IntermediateRepresentation = inner.genInit
  /**
   * {{{
   *   boolean fromLHS;
   *   if ( (this.inputMorsel().slots()) == (lhsSlotConfig) )
   *       fromLHS = true;
   *   else if ( (this.inputMorsel().slots()) == (rhsSlotConfig) )
   *       fromLHS = false;
   *   else
   *       throw new java.lang.IllegalStateException( "Unknown slot configuration in ConditionalApplyOperator." );
   *   while(this.canContinue()) {
   *     if (!fromLhs) {
   *        //copy over all data and all cached properties
   *     } else {
   *       //copy over all data that is on the lhs
   *       //all data is on the rhs but not in lhs is set to NO_VALUE or -1
   *       //(note don't touch cached properties that are only on the RHS)
   *     }
   * }}}
   */
  override protected def genOperateHead(profile: Boolean): IntermediateRepresentation = {
    val inputSlotConfig = invoke(INPUT_MORSEL, method[Morsel, SlotConfiguration]("slots"))
    val (refsToCopy, cachedPropertiesToCopy) = computeWhatToCopy()
    val (refsToCopyLhs, refsToCopyRhs) = refsToCopy.partition(_ < lhsSlots.size().nReferences)
    val (cachedPropertiesToCopyLhs, _) = cachedPropertiesToCopy.partition(_ < lhsSlots.size().nReferences)
    block(
      declare[Boolean](codeGen.fromLHSName),
      ifElse(equal(inputSlotConfig, getStatic(lhsSlotsConfigsFused))) {
        assign(codeGen.fromLHSName, constant(true))
      } {
        ifElse(equal(inputSlotConfig, getStatic(rhsSlotsConfigsFused))) {
          assign(codeGen.fromLHSName, constant(false))
        } {
          fail(newInstance(constructor[IllegalStateException, String], constant("Unknown slot configuration in ConditionalApplyOperator.")))
        }
      },
      genAdvanceOnCancelledRow,
      loop(and(invoke(self(), method[ContinuableOperatorTask, Boolean]("canContinue")), innermost.predicate))(
        block(
          innermost.resetBelowLimitAndAdvanceToNextArgument,
          ifElse(not(load(codeGen.fromLHSName))) {
            block(
              block((0 until rhsSlots.size().nLongs).map(offset => codeGen.setLongAt(offset, codeGen.getLongFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block(refsToCopy.map(offset => codeGen.setRefAt(offset, codeGen.getRefFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block(cachedPropertiesToCopy.map(offset => codeGen.setCachedPropertyAt(offset, codeGen.getCachedPropertyFromExecutionContext(offset, INPUT_CURSOR))): _*)
            )
          } {
            block(
              block((0 until lhsSlots.size().nLongs).map(offset => codeGen.setLongAt(offset, codeGen.getLongFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block(refsToCopyLhs.map(offset => codeGen.setRefAt(offset, codeGen.getRefFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block(cachedPropertiesToCopyLhs.map(offset => codeGen.setRefAt(offset, codeGen.getCachedPropertyFromExecutionContext(offset, INPUT_CURSOR))): _*),
              block((lhsSlots.size().nLongs until rhsSlots.size().nLongs).map(offset => codeGen.setLongAt(offset, constant(-1L))): _*),
              block(refsToCopyRhs.map(offset => codeGen.setRefAt(offset, noValue)): _*)
            )
          },
          inner.genOperateWithExpressions(profile),
          // Else if no inner operator can proceed we move to the next input row
          doIfInnerCantContinue(block(invokeSideEffect(INPUT_CURSOR, NEXT),  profileRow(id, profile))),
          innermost.resetCachedPropertyVariables
        )
      )
    )
  }

  override protected def genOperateMiddle(profile: Boolean): IntermediateRepresentation = {
    throw new CantCompileQueryException("Cannot compile ConditionalApply as middle operator")
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty[IntermediateExpression]

  override def genFields: Seq[Field] = Seq(INPUT_MORSEL_FIELD, lhsSlotsConfigsFused, rhsSlotsConfigsFused)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty[LocalVariable]

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, INPUT_ROW_IS_VALID)).orElse(Some(INPUT_ROW_IS_VALID))
  }

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genClearStateOnCancelledRow: IntermediateRepresentation = noop()

  private def computeWhatToCopy(): (Array[Int], Array[Int]) = {
    val refsToCopy = mutable.ArrayBuilder.make[Int]
    val cachedPropertiesToCopy = mutable.ArrayBuilder.make[Int]
    rhsSlots.foreachSlotAndAliasesOrdered {
      case SlotWithKeyAndAliases(VariableSlotKey(_), RefSlot(offset, _, _), _) =>
        refsToCopy += offset
      case SlotWithKeyAndAliases(CachedPropertySlotKey(cnp), _, _) =>
        val offset = rhsSlots.getCachedPropertyOffsetFor(cnp)
        cachedPropertiesToCopy += offset
      case _ =>
    }
    (refsToCopy.result(), cachedPropertiesToCopy.result())
  }
}

object ConditionalOperatorTaskTemplate {
  def compareSize(a: Morsel, nLongs: Int, nReferences: Int): Boolean = {
    val size = a.slots.size()
    nLongs == size.nLongs && nReferences == size.nReferences
  }
}

