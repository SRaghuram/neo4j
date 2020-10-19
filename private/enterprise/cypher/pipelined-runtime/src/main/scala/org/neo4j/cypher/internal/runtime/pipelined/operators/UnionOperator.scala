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
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_MORSEL
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_ROW_IS_VALID
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NEXT
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.RowMapping
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.UnionSlotMapping
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.InternalException
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

class UnionOperator(val workIdentity: WorkIdentity,
                    lhsSlotConfig: SlotConfiguration,
                    rhsSlotConfig: SlotConfiguration,
                    lhsMapping: RowMapping,
                    rhsMapping: RowMapping
                   ) extends StreamingOperator {

  override def toString: String = "Union"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    val morsel = inputMorsel.nextCopy

    val rowMapping =
      if (morsel.slots eq lhsSlotConfig) lhsMapping
      else if (morsel.slots eq rhsSlotConfig) rhsMapping
      else throw new IllegalStateException(s"Unknown slot configuration in UnionOperator. Got: ${morsel.slots}. LHS: $lhsSlotConfig. RHS: $rhsSlotConfig")

    singletonIndexedSeq(new UnionTask(morsel,
      workIdentity,
      rowMapping))
  }

}

class UnionTask(val inputMorsel: Morsel,
                val workIdentity: WorkIdentity,
                rowMapping: RowMapping) extends ContinuableOperatorTaskWithMorsel {

  override def toString: String = "UnionTask"

  override protected def closeCursors(resources: QueryResources): Unit = {}

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    val inputCursor = inputMorsel.fullCursor(onFirstRow = false)
    val outputCursor = outputMorsel.fullCursor(onFirstRow = true)

    // We write one output row per input row.
    // Since the input morsel can at most have as many rows as the output morsel, we don't need to check `outputCursor.onValidRow()`.
    while (inputCursor.next()) {
      rowMapping.mapRows(inputCursor, outputCursor, state)
      outputCursor.next()
    }
    outputCursor.truncate()
  }

  override def canContinue: Boolean = false

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}



class UnionOperatorTemplate(val inner: OperatorTaskTemplate,
                            override val id: Id,
                            innermost: DelegateOperatorTaskTemplate,
                            lhsSlotConfig: SlotConfiguration,
                            rhsSlotConfig: SlotConfiguration,
                            lhsMapping: Iterable[UnionSlotMapping],
                            rhsMapping: Iterable[UnionSlotMapping])
                           (protected val codeGen: BinaryOperatorExpressionCompiler) extends ContinuableOperatorTaskWithMorselTemplate {


  // Union does not support fusing over pipelines, so it is always gonna be the head operator
  override protected val isHead: Boolean = true

  private val lhsSlotsConfigsFused: StaticField = staticConstant[SlotConfiguration](codeGen.namer.nextVariableName("lhsSlotConfig"), lhsSlotConfig)
  private val rhsSlotsConfigsFused: StaticField = staticConstant[SlotConfiguration](codeGen.namer.nextVariableName("rhsSlotConfig"), rhsSlotConfig)
  private val canContinue = field[Boolean](codeGen.namer.nextVariableName("canContinue"))

  override protected def scopeId: String = "union" + id.x

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genFields: Seq[Field] = Seq(lhsSlotsConfigsFused, rhsSlotsConfigsFused, canContinue)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genInit: IntermediateRepresentation = inner.genInit

  /**
   * {{{
   *   boolean fromLHS;
   *   if ( (this.inputMorsel().slots()) == (lhsSlotConfig) )
   *       fromLHS = true;
   *   else if ( (this.inputMorsel().slots()) == (rhsSlotConfig) )
   *       fromLHS = false;
   *   else
   *       throw new java.lang.IllegalStateException( "Unknown slot configuration in UnionOperator." );
   *   this.canContinue = this.inputMorsel.onValidRow
   *    << genLoop >>
   * }}}
   */
  override protected def genOperateHead: IntermediateRepresentation = {
    val inputSlotConfig = invoke(INPUT_MORSEL, method[Morsel, SlotConfiguration]("slots"))
    block(
      declare[Boolean](codeGen.fromLHSName),
      ifElse(equal(inputSlotConfig, getStatic(lhsSlotsConfigsFused))) {
        assign(codeGen.fromLHSName, constant(true))
      } {
        ifElse(equal(inputSlotConfig, getStatic(rhsSlotsConfigsFused))) {
          assign(codeGen.fromLHSName, constant(false))
        } {
          fail(newInstance(constructor[IllegalStateException, String], constant("Unknown slot configuration in UnionOperator.")))
        }
      },
      genAdvanceOnCancelledRow,
      setField(canContinue, INPUT_ROW_IS_VALID),
      genLoop(doProfile)
    )
  }
  /**
   * {{{
   *    while( (this.canContinue) && ((served) < (demand)) )
   *    {
   *        if ( fromLHS )
   *            // copy from LHS
   *        else
   *            // copy from RHS
   *        << inner.genOperate >>
   *        this.canContinue = this.inputCursor.next();
   *    }
   * }}}
   */
  private def genLoop(profile: Boolean): IntermediateRepresentation = {
    loop(and(loadField(canContinue), innermost.predicate)) {
      block(
        innermost.resetBelowLimitAndAdvanceToNextArgument,
        ifElse(load(codeGen.fromLHSName)) {
          copySlots(lhsMapping)
        } {
          copySlots(rhsMapping)
        },
        inner.genOperateWithExpressions,
        // Else if no inner operator can proceed we move to the next input row
        doIfInnerCantContinue(
          innermost.setUnlessPastLimit(canContinue,
            block(profileRow(id, profile), invoke(INPUT_CURSOR, NEXT)))),
        innermost.resetCachedPropertyVariables
      )
    }
  }

  private def copySlots(slotMapping: Iterable[UnionSlotMapping]): IntermediateRepresentation = {
    val ops = slotMapping.map {
      case SlottedPipeMapper.CopyLongSlot(sourceOffset, targetOffset) =>
        codeGen.setLongAt(targetOffset, codeGen.getLongFromExecutionContext(sourceOffset, INPUT_CURSOR))
      case SlottedPipeMapper.CopyRefSlot(sourceOffset, targetOffset) =>
        codeGen.setRefAt(targetOffset, codeGen.getRefFromExecutionContext(sourceOffset, INPUT_CURSOR))
      case SlottedPipeMapper.CopyCachedProperty(sourceOffset, targetOffset) =>
        codeGen.setCachedPropertyAt(targetOffset, codeGen.getCachedPropertyFromExecutionContext(sourceOffset, INPUT_CURSOR))
      case SlottedPipeMapper.ProjectLongToRefSlot(sourceSlot, targetOffset) =>
        codeGen.setRefAt(targetOffset, getFromSlot(sourceSlot))
    }

    block(ops.toSeq: _*)
  }

  private def nodeFromSlot(offset: Int): IntermediateRepresentation = {
    invoke(DB_ACCESS, method[DbAccess, NodeValue, Long]("nodeById"), codeGen.getLongFromExecutionContext(offset, INPUT_CURSOR))
  }

  private def relFromSlot(offset: Int): IntermediateRepresentation = {
    invoke(DB_ACCESS, method[DbAccess, RelationshipValue, Long]("relationshipById"), codeGen.getLongFromExecutionContext(offset, INPUT_CURSOR))
  }

  private def getFromSlot(slot: LongSlot): IntermediateRepresentation = slot match {
    case LongSlot(offset, true, symbols.CTNode) =>
      ternary(equal(codeGen.getLongAt(offset), constant(-1L)), noValue, nodeFromSlot(offset))
    case LongSlot(offset, false, symbols.CTNode) =>
      nodeFromSlot(offset)
    case LongSlot(offset, true, symbols.CTRelationship) =>
      ternary(equal(codeGen.getLongAt(offset), constant(-1L)), noValue, relFromSlot(offset))
    case LongSlot(offset, false, symbols.CTRelationship) =>
      relFromSlot(offset)

    case _ =>
      throw new InternalException(s"Do not know how to project $slot")
  }

  override protected def genOperateMiddle: IntermediateRepresentation = throw new CantCompileQueryException("Cannot compile Union as middle operator")

  override def genCanContinue: Option[IntermediateRepresentation] = {
    inner.genCanContinue.map(or(_, loadField(canContinue))).orElse(Some(loadField(canContinue)))
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genClearStateOnCancelledRow: IntermediateRepresentation = noop()
}
