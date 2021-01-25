/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arraySet
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newArray
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateGroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.BaseDistinctOperatorTaskTemplate.seen
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentSlotOffsetFieldName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgument
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgumentSlotOffset
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.peekState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.removeState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.FilterStateWithIsLast
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.pipes.DistinctSlottedPrimitivePipe.buildGroupingValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.LongArray
import org.neo4j.values.storable.Values

class DistinctPrimitiveOperatorTask[S <: DistinctState](argumentStateMap: ArgumentStateMap[S],
                                                        val workIdentity: WorkIdentity,
                                                        primitiveSlots: Array[Int],
                                                        groupings: GroupingExpression) extends OperatorTask {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    argumentStateMap.filterWithSideEffect[S](outputMorsel,
      (distinctState, _) => FilterStateWithIsLast(distinctState, isLast = false),
      (distinctState, row) => {
        val groupingValue = buildGroupingValue(row, primitiveSlots)
        if (distinctState.seen(groupingValue)) {
          val groupingKey = groupings.computeGroupingKey(row, queryState)
          groupings.project(row, groupingKey)
          true
        } else {
          false
        }
      })
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}

class DistinctPrimitiveOperator(argumentStateMapId: ArgumentStateMapId,
                                val workIdentity: WorkIdentity,
                                primitiveSlots: Array[Int],
                                groupings: GroupingExpression)
                               (val id: Id = Id.INVALID_ID) extends MemoryTrackingMiddleOperator(id.x) {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources,
                          memoryTracker: MemoryTracker): OperatorTask = {
    new DistinctPrimitiveOperatorTask(
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, DistinctStateFactory, memoryTracker),
      workIdentity,
      primitiveSlots,
      groupings
    )
  }
}

abstract class BaseDistinctPrimitiveOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                         override val id: Id,
                                                         primitiveSlots: Array[Int],
                                                         groupings: Seq[(Slot, Expression)],
                                                         codeGen: OperatorExpressionCompiler)
  extends OperatorTaskTemplate {

  private var groupingExpression: IntermediateGroupingExpression = _

  protected val distinctStateField: InstanceField = field[DistinctState](codeGen.namer.nextVariableName("distinctState"), initializeState)
  protected val memoryTracker: IntermediateRepresentation = codeGen.registerMemoryTracker(id)
  protected val primitiveSlotsField: StaticField = staticConstant[Array[Int]]("primitiveSlots", primitiveSlots)

  protected def initializeState: IntermediateRepresentation
  protected def beginOperate: IntermediateRepresentation
  protected def createState: IntermediateRepresentation
  protected def genMoreFields: Seq[Field]

  override def genInit: IntermediateRepresentation = inner.genInit
  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
  override protected def genOperate: IntermediateRepresentation = {
    val compiled = groupings.map {
      case (s, e) =>
        s -> codeGen.compileExpression(e, id).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $e"))
    }
    val groupingKeyAsArrayVar = codeGen.namer.nextVariableName("groupingKeyAsArray")
    //note that this key ir read later also by project
    val keyVar = codeGen.namer.nextVariableName("groupingKey")
    groupingExpression = codeGen.compileGroupingExpression(compiled, keyVar, id)

    /**
     * val groupingKeyAsArray = buildGroupingKey
     * if (distinctState.seen(groupingKeyAsArray)) {
     * val key = [computeKey]
     * [project key]
     * <<inner.generate>>
     * }
     */
    block(
      beginOperate,
      declareAndAssign(typeRefOf[LongArray], groupingKeyAsArrayVar, buildGroupingKey),
      condition(or(innerCanContinue,
        seen(loadField(distinctStateField), load[LongArray](groupingKeyAsArrayVar)))) {
        block(
          declareAndAssign(typeRefOf[AnyValue], keyVar, nullCheckIfRequired(groupingExpression.computeKey)),
          nullCheckIfRequired(groupingExpression.projectKey),
          inner.genOperateWithExpressions,
          conditionallyProfileRow(innerCannotContinue, id, doProfile)
        )
      })
  }

  override def genCreateState: IntermediateRepresentation = block(inner.genCreateState, createState)
  override def genExpressions: Seq[IntermediateExpression] = Seq(groupingExpression.computeKey, groupingExpression.projectKey)
  override def genFields: Seq[Field] = Seq(distinctStateField, primitiveSlotsField) ++ genMoreFields
  override def genLocalVariables: Seq[LocalVariable] = Seq.empty
  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue
  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  /**
   * {{{
   *     val keys = new Array[Long](slots.length)
   *     keys(i) = row.getLongAt(slots(i))
   *     ...
   *     Values.longArray(keys)
   * }}}
   */
  private def buildGroupingKey: IntermediateRepresentation = {
    val statements =
      declareAndAssign(typeRefOf[Array[Long]], "keys", newArray(typeRefOf[Long], primitiveSlots.length)) +:
        primitiveSlots.zipWithIndex.map {
          case (slot, i) => arraySet(load[Array[Long]]("keys"), constant(i), codeGen.getLongAt(slot))
        } :+
        invokeStatic(method[Values, LongArray, Array[Long]]("longArray"), load[Array[Long]]("keys"))
    block(statements: _*)
  }
}

class SerialTopLevelDistinctPrimitiveOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                          id: Id,
                                                          argumentStateMapId: ArgumentStateMapId,
                                                          primitiveSlots: Array[Int],
                                                          groupings: Seq[(Slot, Expression)])
                                                         (protected val codeGen: OperatorExpressionCompiler)
  extends BaseDistinctPrimitiveOperatorTaskTemplate(inner, id, primitiveSlots, groupings, codeGen) {
  override protected def initializeState: IntermediateRepresentation = peekState[DistinctState](argumentStateMapId)
  override protected def beginOperate: IntermediateRepresentation = noop()
  override protected def createState: IntermediateRepresentation = invoke(loadField(distinctStateField),
    method[DistinctState, Unit, MemoryTracker]("setMemoryTracker"), memoryTracker)
  override protected def genMoreFields: Seq[Field] = Seq.empty
  override protected def isHead: Boolean = false
}

class SerialDistinctOnRhsOfApplyPrimitiveOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                              id: Id,
                                                              argumentStateMapId: ArgumentStateMapId,
                                                              primitiveSlots: Array[Int],
                                                              groupings: Seq[(Slot, Expression)])
                                                             (val codeGen: OperatorExpressionCompiler)
  extends BaseDistinctPrimitiveOperatorTaskTemplate(inner, id, primitiveSlots, groupings, codeGen) {
  private val argumentMaps: InstanceField = field[ArgumentStateMaps](codeGen.namer.nextVariableName("stateMaps"),
    load(ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER))
  private val localArgument = codeGen.namer.nextVariableName("argument")

  override protected def initializeState: IntermediateRepresentation = constant(null)
  override protected def beginOperate: IntermediateRepresentation =
    block(
      declareAndAssign(typeRefOf[Long], argumentVarName(argumentStateMapId), getArgument(argumentStateMapId)),
      condition(not(equal(load[Long](argumentVarName(argumentStateMapId)), load[Long](localArgument)))) {
        block(
          condition(IntermediateRepresentation.isNotNull(loadField(distinctStateField))){
            removeState(loadField(argumentMaps), argumentStateMapId, load[Long](localArgument))
          },
          assign(localArgument, load[Long](argumentVarName(argumentStateMapId))),
          setField(distinctStateField, cast[DistinctState](OperatorCodeGenHelperTemplates.fetchState(loadField(argumentMaps), argumentStateMapId))),
          invoke(loadField(distinctStateField),
            method[DistinctState, Unit, MemoryTracker]("setMemoryTracker"), memoryTracker)
        )
      })

  override def genOperateEnter: IntermediateRepresentation = {
    block(
      declareAndAssign(typeRefOf[Long], localArgument, constant(-1L)),
      inner.genOperateEnter
    )
  }
  override def createState: IntermediateRepresentation = noop()
  override def genMoreFields: Seq[Field] =
    Seq(argumentMaps, field[Int](argumentSlotOffsetFieldName(argumentStateMapId), getArgumentSlotOffset(argumentStateMapId)))

  override protected def isHead: Boolean = false
}

