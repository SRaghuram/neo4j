/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.BaseDistinctOperatorTaskTemplate.seen
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.ConcurrentDistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.DistinctStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctOperator.StandardDistinctState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentSlotOffsetFieldName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.fetchState
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
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

/**
 * The distinct operator, for Cypher like
 *
 *   MATCH ...
 *   WITH DISTINCT a, b
 *   ...
 *
 * or
 *
 *   UNWIND someList AS x
 *   RETURN DISTINCT x
 */

class DistinctOperatorTask[S <: DistinctState](argumentStateMap: ArgumentStateMap[S],
                                               val workIdentity: WorkIdentity,
                                               groupings: GroupingExpression) extends OperatorTask {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    argumentStateMap.filterWithSideEffect[S](outputMorsel,
      (distinctState, _) => FilterStateWithIsLast(distinctState, isLast = false),
      (distinctState, row) => {
        val groupingKey = groupings.computeGroupingKey(row, queryState)
        if (distinctState.seen(groupingKey)) {
          groupings.project(row, groupingKey)
          true
        } else {
          false
        }
      })
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}

class DistinctOperator(argumentStateMapId: ArgumentStateMapId,
                       val workIdentity: WorkIdentity,
                       groupings: GroupingExpression)
                      (val id: Id = Id.INVALID_ID) extends MemoryTrackingMiddleOperator(id.x) {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources,
                          memoryTracker: MemoryTracker): OperatorTask = {
    new DistinctOperatorTask(
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, DistinctStateFactory, memoryTracker),
      workIdentity,
      groupings
    )
  }
}

object DistinctOperator {

  object DistinctStateFactory extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): DistinctState = {
      new StandardDistinctState(argumentRowId, argumentRowIdsForReducers, memoryTracker)
    }

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): DistinctState = {
      new ConcurrentDistinctState(argumentRowId, argumentRowIdsForReducers, EmptyMemoryTracker.INSTANCE)
    }

    override def completeOnConstruction: Boolean = true
  }

  trait DistinctState extends ArgumentState {
    def seen(key: AnyValue): Boolean
    /**
     * We need this method instead of having the memoryTracked as a constructor parameter because the Factory
     * needs be statically known for the fused code. This allows us to share the DistinctState between fused and non-fused code.
     */
    def setMemoryTracker(memoryTracker: MemoryTracker): Unit
  }

  class StandardDistinctState(override val argumentRowId: Long,
                              override val argumentRowIdsForReducers: Array[Long])
    extends DistinctState {

    def this(argumentRowId: Long, argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker) = {
      this(argumentRowId, argumentRowIdsForReducers)
      setMemoryTracker(memoryTracker)
    }

    private var seenSet: DistinctSet[AnyValue] = _

    // This is called from generated code by genCreateState
    override def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {
      if (seenSet == null) {
        seenSet = DistinctSet.createDistinctSet[AnyValue](memoryTracker)
      }
    }

    override def seen(key: AnyValue): Boolean =
      seenSet.add(key)

    override def close(): Unit = {
      seenSet.close()
      super.close()
    }

    override def toString: String = s"StandardDistinctState($argumentRowId)"

    override final def shallowSize: Long = StandardDistinctState.SHALLOW_SIZE
  }

  object StandardDistinctState {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[StandardDistinctState])
  }

  class ConcurrentDistinctState(override val argumentRowId: Long, override val argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker)
    extends DistinctState {

    private val seenSet = ConcurrentHashMap.newKeySet[AnyValue]()

    // This is called from generated code by genCreateState
    override def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {}

    override def seen(key: AnyValue): Boolean =
      seenSet.add(key)

    override def toString: String = s"ConcurrentDistinctState($argumentRowId)"

    override final def shallowSize: Long = ConcurrentDistinctState.SHALLOW_SIZE
  }

  object ConcurrentDistinctState {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[ConcurrentDistinctState])
  }
}

object DistinctOperatorState {
  object DistinctStateFactory extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long], memoryTracker: MemoryTracker): DistinctState = {
      new StandardDistinctState(argumentRowId, argumentRowIdsForReducers, memoryTracker)
    }

    override def newConcurrentArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long]): DistinctState = {
      new ConcurrentDistinctState(argumentRowId, argumentRowIdsForReducers, EmptyMemoryTracker.INSTANCE)
    }
  }
}

abstract class BaseDistinctOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                                override val id: Id,
                                                groupings: Seq[(Slot, Expression)],
                                                val codeGen: OperatorExpressionCompiler)
  extends OperatorTaskTemplate {

  private var groupingExpression: IntermediateGroupingExpression = _
  protected val distinctStateField: InstanceField = field[DistinctState](codeGen.namer.nextVariableName("distinctState"), initializeState)
  protected val memoryTracker: IntermediateRepresentation = codeGen.registerMemoryTracker(id)

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
    //note that this key ir read later also by project
    val keyVar = codeGen.namer.nextVariableName("groupingKey")
    groupingExpression = codeGen.compileGroupingExpression(compiled, keyVar, id)

    /**
     * val key = [computeKey]
     * if (distinctState.seen(key)) {
     *   [project key]
     *   <<inner.generate>>
     * }
     */
    block(
      beginOperate,
      declareAndAssign(typeRefOf[AnyValue], keyVar, nullCheckIfRequired(groupingExpression.computeKey)),
      condition(or(innerCanContinue, seen(loadField(distinctStateField), load[AnyValue](keyVar)))) {
        block(
          nullCheckIfRequired(groupingExpression.projectKey),
          inner.genOperateWithExpressions,
          conditionallyProfileRow(innerCannotContinue, id, doProfile)
        )
      })
  }

  override def genCreateState: IntermediateRepresentation = block(inner.genCreateState, createState)
  override def genExpressions: Seq[IntermediateExpression] = Seq(groupingExpression.computeKey, groupingExpression.projectKey)
  override def genFields: Seq[Field] = distinctStateField +: genMoreFields
  override def genLocalVariables: Seq[LocalVariable] = Seq.empty
  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue
  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

object BaseDistinctOperatorTaskTemplate {
  def seen(state: IntermediateRepresentation, key: IntermediateRepresentation): IntermediateRepresentation =
    invoke(state, method[DistinctState, Boolean, AnyValue]("seen"), key)
}

class SerialTopLevelDistinctOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                 id: Id,
                                                 argumentStateMapId: ArgumentStateMapId,
                                                 groupings: Seq[(Slot, Expression)])
                                                (codeGen: OperatorExpressionCompiler)
  extends BaseDistinctOperatorTaskTemplate(inner, id, groupings, codeGen) {
  override protected def initializeState: IntermediateRepresentation =  peekState[DistinctState](argumentStateMapId)
  override protected def beginOperate: IntermediateRepresentation = noop()
  override protected def createState: IntermediateRepresentation =
    invoke(loadField(distinctStateField),
      method[DistinctState, Unit, MemoryTracker]("setMemoryTracker"), memoryTracker)
  override protected def genMoreFields: Seq[Field] = Seq.empty

  override protected def isHead: Boolean = false
}

class SerialDistinctOnRhsOfApplyOperatorTaskTemplate(override val inner: OperatorTaskTemplate,
                                                     override val id: Id,
                                                     argumentStateMapId: ArgumentStateMapId,
                                                     groupings: Seq[(Slot, Expression)])
                                                    (codeGen: OperatorExpressionCompiler)
  extends BaseDistinctOperatorTaskTemplate(inner, id, groupings, codeGen) {
  private val argumentMaps: InstanceField = field[ArgumentStateMaps](codeGen.namer.nextVariableName("stateMaps"),
    load(ARGUMENT_STATE_MAPS_CONSTRUCTOR_PARAMETER))
  private val localArgument = codeGen.namer.nextVariableName("argument")

  override def genOperateEnter: IntermediateRepresentation = {
    block(
      declareAndAssign(typeRefOf[Long], localArgument, constant(-1L)),
      inner.genOperateEnter
    )
  }
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
          setField(distinctStateField, cast[DistinctState](fetchState(loadField(argumentMaps), argumentStateMapId))),
          invoke(loadField(distinctStateField),
            method[DistinctState, Unit, MemoryTracker]("setMemoryTracker"), memoryTracker)
        )
      })

  override def createState: IntermediateRepresentation = noop()

  override def genMoreFields: Seq[Field] =
   Seq(argumentMaps, field[Int](argumentSlotOffsetFieldName(argumentStateMapId), getArgumentSlotOffset(argumentStateMapId)))

  override protected def isHead: Boolean = false
}