/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctSinglePrimitiveOperator.ConcurrentDistinctSinglePrimitiveState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctSinglePrimitiveOperator.DistinctSinglePrimitiveState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctSinglePrimitiveOperator.DistinctSinglePrimitiveStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctSinglePrimitiveOperator.StandardDistinctSinglePrimitiveState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentSlotOffsetFieldName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgument
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgumentSlotOffset
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.peekState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.removeState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.FilterStateWithIsLast
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.InternalException
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

class DistinctSinglePrimitiveOperatorTask[S <: DistinctSinglePrimitiveState](argumentStateMap: ArgumentStateMap[S],
                                                                             val workIdentity: WorkIdentity,
                                                                             setInSlot: (WritableRow, AnyValue) => Unit,
                                                                             offset: Int,
                                                                             expression: commands.expressions.Expression) extends OperatorTask {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    argumentStateMap.filterWithSideEffect[S](outputMorsel,
      (distinctState, _) => FilterStateWithIsLast(distinctState, isLast = false),
      (distinctState, row) => {
        val key = row.getLongAt(offset)
        if (distinctState.seen(key)) {
          val outputValue = expression(row, state)
          setInSlot(row, outputValue)
          true
        } else {
          false
        }
      })
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}

class DistinctSinglePrimitiveOperator(argumentStateMapId: ArgumentStateMapId,
                                      val workIdentity: WorkIdentity,
                                      toSlot: Slot,
                                      offset: Int,
                                      expression: commands.expressions.Expression)
                                     (val id: Id = Id.INVALID_ID) extends MemoryTrackingMiddleOperator(id.x) {

  private val setInSlot: (WritableRow, AnyValue) => Unit = makeSetValueInSlotFunctionFor(toSlot)

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources,
                          memoryTracker: MemoryTracker): OperatorTask = {
    new DistinctSinglePrimitiveOperatorTask(
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new DistinctSinglePrimitiveStateFactory, memoryTracker),
      workIdentity,
      setInSlot,
      offset,
      expression)
  }
}

object DistinctSinglePrimitiveOperator {
  class DistinctSinglePrimitiveStateFactory extends ArgumentStateFactory[DistinctSinglePrimitiveState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): DistinctSinglePrimitiveState = {
      new StandardDistinctSinglePrimitiveState(argumentRowId, argumentRowIdsForReducers, memoryTracker)
    }

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctSinglePrimitiveState = {
      new ConcurrentDistinctSinglePrimitiveState(argumentRowId, argumentRowIdsForReducers)
    }

    override def completeOnConstruction: Boolean = true
  }

  trait DistinctSinglePrimitiveState extends ArgumentState  {
    def seen(key: Long): Boolean
    /**
     * We need this method instead of having the memoryTracked as a constructor parameter because the Factory
     * needs be statically known for the fused code. This allows us to share the DistinctState between fused and non-fused code.
     */
    def setMemoryTracker(memoryTracker: MemoryTracker): Unit
  }

  class StandardDistinctSinglePrimitiveState(override val argumentRowId: Long, override val argumentRowIdsForReducers: Array[Long])
    extends DistinctSinglePrimitiveState {

    def this(argumentRowId: Long, argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker) = {
      this(argumentRowId, argumentRowIdsForReducers)
      setMemoryTracker(memoryTracker)
    }

    private var seenSet: HeapTrackingLongHashSet = _

    // This is called from generated code by genCreateState
    override def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {
      if (seenSet == null) {
        seenSet = HeapTrackingCollections.newLongSet(memoryTracker)
      }
    }

    override def seen(key: Long): Boolean =
      seenSet.add(key)

    override def close(): Unit = {
      seenSet.close()
      super.close()
    }

    override def toString: String = s"StandardDistinctSinglePrimitiveState($argumentRowId)"

    override def shallowSize: Long = StandardDistinctSinglePrimitiveState.SHALLOW_SIZE
  }

  object StandardDistinctSinglePrimitiveState {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[StandardDistinctSinglePrimitiveState])
  }

  class ConcurrentDistinctSinglePrimitiveState(override val argumentRowId: Long, override val argumentRowIdsForReducers: Array[Long])
    extends DistinctSinglePrimitiveState {

    private val seenSet = ConcurrentHashMap.newKeySet[Long]()

    // This is called from generated code by genCreateState
    override def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {}

    override def seen(key: Long): Boolean =
      seenSet.add(key)

    override def toString: String = s"ConcurrentDistinctSinglePrimitiveState($argumentRowId)"

    override def shallowSize: Long = ConcurrentDistinctSinglePrimitiveState.SHALLOW_SIZE
  }

  object ConcurrentDistinctSinglePrimitiveState {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[ConcurrentDistinctSinglePrimitiveState])
  }
}

object DistinctSinglePrimitiveState {

  object DistinctStateFactory extends ArgumentStateFactory[DistinctSinglePrimitiveState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): DistinctSinglePrimitiveState = {
      new StandardDistinctSinglePrimitiveState(argumentRowId, argumentRowIdsForReducers, memoryTracker)
    }

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): DistinctSinglePrimitiveState = {
      new ConcurrentDistinctSinglePrimitiveState(argumentRowId, argumentRowIdsForReducers)

    }
  }
}

abstract class BaseDistinctSinglePrimitiveOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                                override val id: Id,
                                                                toSlot: Slot,
                                                                offset: Int,
                                                                generateExpression: () => IntermediateExpression,
                                                                codeGen: OperatorExpressionCompiler)
  extends OperatorTaskTemplate {

  private var expression: IntermediateExpression = _
  protected val distinctStateField: InstanceField = field[DistinctSinglePrimitiveState](codeGen.namer.nextVariableName("distinctState"), initializeState)
  protected val memoryTracker: IntermediateRepresentation = codeGen.registerMemoryTracker(id)

  protected def initializeState: IntermediateRepresentation
  protected def beginOperate: IntermediateRepresentation
  protected def createState: IntermediateRepresentation
  protected def genMoreFields: Seq[Field]

  override def genInit: IntermediateRepresentation = inner.genInit
  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
  override protected def genOperate: IntermediateRepresentation = {
    if (expression == null) {
      expression = generateExpression()
    }
    val keyVar = codeGen.namer.nextVariableName("groupingKey")
    /**
     * val key = context.getLongAt(offset)
     * if (innerCanContinue || distinctState.seen(key)) {
     *    context.setLongAt(outOffset, ((VirtualNodeValue) expression).id());
     *   <<inner.generate>>
     * }
     */
    block(
      beginOperate,
      declareAndAssign(typeRefOf[Long], keyVar, codeGen.getLongAt(offset)),
      condition(or(innerCanContinue, seen(loadField(distinctStateField), load(keyVar)))) {
        block(
          computeProjection,
          inner.genOperateWithExpressions,
          conditionallyProfileRow(innerCannotContinue, id, doProfile)
        )
      })
  }

  override def genCreateState: IntermediateRepresentation = block(inner.genCreateState, createState)
  override def genExpressions: Seq[IntermediateExpression] = Seq(expression)
  override def genFields: Seq[Field] = distinctStateField +: genMoreFields
  override def genLocalVariables: Seq[LocalVariable] = Seq.empty
  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue
  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  private def computeProjection: IntermediateRepresentation = {
    toSlot match {
      case LongSlot(offset, _, CTNode) =>
        codeGen.setLongAt(offset, invoke(cast[VirtualNodeValue](nullCheckIfRequired(expression)),
          method[VirtualNodeValue, Long]("id")))
      case LongSlot(offset, _, CTRelationship) =>
        codeGen.setLongAt(offset, invoke(cast[VirtualRelationshipValue](nullCheckIfRequired(expression)),
          method[VirtualRelationshipValue, Long]("id")))
      case RefSlot(offset, _, _) =>
        codeGen.setRefAt(offset, nullCheckIfRequired(expression))
      case slot =>
        throw new InternalException(s"Do not know how to make setter for slot $slot")
    }
  }
  private def seen(state: IntermediateRepresentation, key: IntermediateRepresentation): IntermediateRepresentation =
    invoke(state, method[DistinctSinglePrimitiveState, Boolean, Long]("seen"), key)
}

class SerialTopLevelDistinctSinglePrimitiveOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                                id: Id,
                                                                argumentStateMapId: ArgumentStateMapId,
                                                                toSlot: Slot,
                                                                offset: Int,
                                                                generateExpression: () => IntermediateExpression)
                                                               (protected val codeGen: OperatorExpressionCompiler)
  extends BaseDistinctSinglePrimitiveOperatorTaskTemplate(inner, id, toSlot, offset, generateExpression, codeGen) {

  override protected def initializeState: IntermediateRepresentation = peekState[DistinctSinglePrimitiveState](argumentStateMapId)
  override protected def beginOperate: IntermediateRepresentation = noop()
  override protected def createState: IntermediateRepresentation =
      invoke(loadField(distinctStateField),
        method[DistinctSinglePrimitiveState, Unit, MemoryTracker]("setMemoryTracker"), memoryTracker)

  override protected def genMoreFields: Seq[Field] = Seq.empty
  override protected def isHead: Boolean = false
}

class SerialDistinctOnRhsOfApplySinglePrimitiveOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                                    id: Id,
                                                                    argumentStateMapId: ArgumentStateMapId,
                                                                    toSlot: Slot,
                                                                    offset: Int,
                                                                    generateExpression: () => IntermediateExpression)
                                                                   (val codeGen: OperatorExpressionCompiler)
  extends BaseDistinctSinglePrimitiveOperatorTaskTemplate(inner, id, toSlot, offset, generateExpression, codeGen) {
  private val argumentMaps: InstanceField = field[ArgumentStateMaps](codeGen.namer.nextVariableName("stateMaps"),
    load(ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER.name))
  private val localArgument = codeGen.namer.nextVariableName("argument")

  override def genMoreFields: Seq[Field] = Seq(argumentMaps, field[Int](argumentSlotOffsetFieldName(argumentStateMapId), getArgumentSlotOffset(argumentStateMapId)))
  override def genOperateEnter: IntermediateRepresentation = {
    block(
      declareAndAssign(typeRefOf[Long], localArgument, constant(-1L)),
      inner.genOperateEnter
    )
  }
  override def createState: IntermediateRepresentation = noop()
  override protected def initializeState: IntermediateRepresentation = constant(null)
  override protected def beginOperate: IntermediateRepresentation =
    block(
      declareAndAssign(typeRefOf[Long], argumentVarName(argumentStateMapId), getArgument(argumentStateMapId)),
      condition(not(equal(load(argumentVarName(argumentStateMapId)), load(localArgument)))) {
        block(
          condition(IntermediateRepresentation.isNotNull(loadField(distinctStateField))){
            removeState(loadField(argumentMaps), argumentStateMapId, load(localArgument))
          },
          assign(localArgument, load(argumentVarName(argumentStateMapId))),
          setField(distinctStateField, cast[DistinctSinglePrimitiveState](OperatorCodeGenHelperTemplates.fetchState(loadField(argumentMaps), argumentStateMapId))),
          invoke(loadField(distinctStateField),
            method[DistinctSinglePrimitiveState, Unit, MemoryTracker]("setMemoryTracker"), memoryTracker)
        )
      })
  override protected def isHead: Boolean = false
}