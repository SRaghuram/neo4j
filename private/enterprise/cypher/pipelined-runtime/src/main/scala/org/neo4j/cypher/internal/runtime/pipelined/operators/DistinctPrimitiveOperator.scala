/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newArray
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentSlotOffsetFieldName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.fetchState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgument
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgumentSlotOffset
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.peekState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.setMemoryTracker
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelDistinctOperatorTaskTemplate.seen
import org.neo4j.cypher.internal.runtime.pipelined.operators.SerialTopLevelDistinctPrimitiveOperatorTaskTemplate.buildGroupingKey
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
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
      (distinctState, _) => distinctState,
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
    new DistinctPrimitiveOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId, new DistinctStateFactory(memoryTracker)), workIdentity, primitiveSlots, groupings)
  }
}

class SerialTopLevelDistinctPrimitiveOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                          override val id: Id,
                                                          argumentStateMapId: ArgumentStateMapId,
                                                          primitiveSlots: Array[Int],
                                                          groupings: Seq[(Slot, Expression)])
                                                         (protected val codeGen: OperatorExpressionCompiler)
  extends OperatorTaskTemplate {

  private var groupingExpression: IntermediateGroupingExpression = _

  private val distinctStateField = field[DistinctState](codeGen.namer.nextVariableName("distinctState"),
    peekState[DistinctState](argumentStateMapId))
  private val memoryTrackerField = field[MemoryTracker](codeGen.namer.nextVariableName("memoryTracker"))
  private val primitiveSlotsField = staticConstant[Array[Int]]("primitiveSlots", primitiveSlots)

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)


  override protected def genOperate: IntermediateRepresentation = {
    val compiled = groupings.map {
      case (s, e) =>
        s -> codeGen.compileExpression(e).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $e"))
    }

    val groupingKeyAsArrayVar = codeGen.namer.nextVariableName("groupingKeyAsArray")

    //note that this key ir read later also by project
    val keyVar = codeGen.namer.nextVariableName("groupingKey")
    groupingExpression = codeGen.compileGroupingExpression(compiled, keyVar)

    /**
     * val groupingKeyAsArray = buildGroupingKey
     * if (distinctState.seen(groupingKeyAsArray)) {
     *    val key = [computeKey]
     *   [project key]
     *   <<inner.generate>>
     * }
     */
    block(
      declareAndAssign(typeRefOf[LongArray], groupingKeyAsArrayVar, buildGroupingKey(primitiveSlots, codeGen)),
      condition(or(innerCanContinue,
        seen(loadField(distinctStateField), load(groupingKeyAsArrayVar)))) {
        block(
          declareAndAssign(typeRefOf[AnyValue], keyVar, nullCheckIfRequired(groupingExpression.computeKey)),
          nullCheckIfRequired(groupingExpression.projectKey),
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(profileRow(id))
        )
      })
  }

  override def genCreateState: IntermediateRepresentation = block(
    setMemoryTracker(memoryTrackerField, id.x),
    invoke(loadField(distinctStateField),
      method[DistinctState, Unit, MemoryTracker]("setMemoryTracker"),
      loadField(memoryTrackerField)),
    inner.genCreateState)

  override def genExpressions: Seq[IntermediateExpression] = Seq(groupingExpression.computeKey, groupingExpression.projectKey)

  override def genFields: Seq[Field] = Seq(distinctStateField, memoryTrackerField, primitiveSlotsField)

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

object SerialTopLevelDistinctPrimitiveOperatorTaskTemplate {
  /**
   * {{{
   *     val keys = new Array[Long](slots.length)
   *     keys(i) = row.getLongAt(slots(i))
   *     ...
   *     Values.longArray(keys)
   * }}}
   */
  private def buildGroupingKey(primitiveSlots: Array[Int], codeGen: OperatorExpressionCompiler): IntermediateRepresentation = {
    val statements =
      declareAndAssign(typeRefOf[Array[Long]], "keys", newArray(typeRefOf[Long], primitiveSlots.length)) +:
        primitiveSlots.zipWithIndex.map {
          case (slot, i) => arraySet(load("keys"), constant(i), codeGen.getLongAt(slot))
        } :+
        invokeStatic(method[Values, LongArray, Array[Long]]("longArray"), load("keys"))
    block(statements: _*)
  }

  class SerialDistinctOnRhsOfApplyPrimitiveOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                                override val id: Id,
                                                                argumentStateMapId: ArgumentStateMapId,
                                                                primitiveSlots: Array[Int],
                                                                groupings: Seq[(Slot, Expression)])
                                                               (val codeGen: OperatorExpressionCompiler)
    extends OperatorTaskTemplate {
    private val argumentMaps: InstanceField = field[ArgumentStateMaps](codeGen.namer.nextVariableName("stateMaps"),
      load(ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER.name))
    private val localArgument = codeGen.namer.nextVariableName("argument")
    private var groupingExpression: IntermediateGroupingExpression = _

    private val distinctStateField = field[DistinctState](codeGen.namer.nextVariableName("distinctState"),
      peekState[DistinctState](argumentStateMapId))
    private val memoryTrackerField = field[MemoryTracker](codeGen.namer.nextVariableName("memoryTracker"))
    private val primitiveSlotsField = staticConstant[Array[Int]]("primitiveSlots", primitiveSlots)

    override def genInit: IntermediateRepresentation = inner.genInit

    override def genExpressions: Seq[IntermediateExpression] = Seq(groupingExpression.computeKey, groupingExpression.projectKey)

    override def genFields: Seq[Field] = Seq(primitiveSlotsField, argumentMaps, memoryTrackerField, distinctStateField, field[Int](argumentSlotOffsetFieldName(argumentStateMapId), getArgumentSlotOffset(argumentStateMapId)))

    override def genOperateEnter: IntermediateRepresentation = {
      block(
        declareAndAssign(typeRefOf[Long], localArgument, constant(-1L)),
        inner.genOperateEnter
      )
    }

    /**
     * {{{
     *   val key = [computeKey]
     *   if (localArgument != inputCursor.getLongAt(argSlot)) {
     *     localArgument = inputCursor.getLongAt(argSlot)
     *     this.distinctState = [get new state from ArgumentStateMap]
     *   }
     *   val groupingKeyAsArray = buildGroupingKey
     *   if (distinctState.seen(groupingKeyAsArray)) {
     *     val key = [computeKey]
     *     [project key]
     *     <<inner.generate>>
     *   }
     * }}}
     */
    override def genOperate: IntermediateRepresentation = {
      val compiled = groupings.map {
        case (s, e) =>
          s -> codeGen.compileExpression(e).getOrElse(throw new CantCompileQueryException(s"The expression compiler could not compile $e"))
      }

      val groupingKeyAsArrayVar = codeGen.namer.nextVariableName("groupingKeyAsArray")

      //note that this key ir read later also by project
      val keyVar = codeGen.namer.nextVariableName("groupingKey")
      groupingExpression = codeGen.compileGroupingExpression(compiled, keyVar)

      block(
        declareAndAssign(typeRefOf[Long], argumentVarName(argumentStateMapId), getArgument(argumentStateMapId)),
        declareAndAssign(typeRefOf[LongArray], groupingKeyAsArrayVar, buildGroupingKey(primitiveSlots, codeGen)),
        condition(not(equal(load(argumentVarName(argumentStateMapId)), load(localArgument)))) {
          block(
            assign(localArgument, load(argumentVarName(argumentStateMapId))),
            setField(distinctStateField, cast[DistinctState](fetchState(loadField(argumentMaps), argumentStateMapId))),
            invoke(loadField(distinctStateField),
              method[DistinctState, Unit, MemoryTracker]("setMemoryTracker"),
              loadField(memoryTrackerField))
          )
        },

        condition(or(innerCanContinue, seen(loadField(distinctStateField), load(groupingKeyAsArrayVar)))) {
          block(
            declareAndAssign(typeRefOf[AnyValue], keyVar, nullCheckIfRequired(groupingExpression.computeKey)),
            nullCheckIfRequired(groupingExpression.projectKey),
            inner.genOperateWithExpressions,
            doIfInnerCantContinue(profileRow(id))
          )
        })
    }

    override def genOperateExit: IntermediateRepresentation = {
      block(
        condition(isNotNull(loadField(distinctStateField))) {
          IntermediateRepresentation.noop()
        },
        inner.genOperateExit)
    }

    override def genLocalVariables: Seq[LocalVariable] = Seq.empty

    override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

    override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

    override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

    override def genCreateState: IntermediateRepresentation = block(
      setMemoryTracker(memoryTrackerField, id.x),
      inner.genCreateState)
  }
}
