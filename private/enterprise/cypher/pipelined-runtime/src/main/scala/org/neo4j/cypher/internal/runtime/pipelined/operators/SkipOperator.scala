/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.ConcurrentCountingState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.StandardCountingState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.evaluateCountValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getArgument
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.SkipOperator.SkipStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

object SkipOperator {

  trait NonCancellableState {
    self: CountingState =>
    def isCancelled: Boolean = false
  }

  class SkipStateFactory(count: Long) extends ArgumentStateFactory[CountingState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): CountingState =
      new StandardCountingState(argumentRowId, count, argumentRowIdsForReducers) with NonCancellableState

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): CountingState =
      new ConcurrentCountingState(argumentRowId, count, argumentRowIdsForReducers) with NonCancellableState
  }
}

class SkipOperator(argumentStateMapId: ArgumentStateMapId,
                   val workIdentity: WorkIdentity,
                   countExpression: Expression)(id: Id = Id.INVALID_ID) extends MemoryTrackingMiddleOperator(id.x) {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources,
                          memoryTracker: MemoryTracker): OperatorTask = {
    val skip = evaluateCountValue(state, resources, countExpression)
    new SkipOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
                                                                     new SkipStateFactory(skip), memoryTracker))
  }

  class SkipOperatorTask(asm: ArgumentStateMap[CountingState]) extends OperatorTask {

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      //nothing to do here
    }

    override def operate(output: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = {
      asm.skip(output, (state, nRows) => state.reserve(nRows))
    }

    override def workIdentity: WorkIdentity = SkipOperator.this.workIdentity
  }
}

class SerialTopLevelSkipOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                             id: Id,
                                             innermost: DelegateOperatorTaskTemplate,
                                             argumentStateMapId: ArgumentStateMapId,
                                             generateCountExpression: () => IntermediateExpression)
                                            (codeGen: OperatorExpressionCompiler)
  extends SerialTopLevelCountingOperatorTaskTemplate(inner, id, innermost, argumentStateMapId, generateCountExpression, codeGen) {

  override def genOperate: IntermediateRepresentation = {
    block(
      ifElse(equal(load(countLeftVar), constant(0)))(block(
        inner.genOperateWithExpressions,
        conditionallyProfileRow(innerCannotContinue, id, doProfile)
      ))(
        doIfInnerCantContinue(assign(countLeftVar, subtract(load(countLeftVar), constant(1))))
      )
    )
  }

  override protected def isHead: Boolean = false
}

object SerialSkipState {

  // This is used by fused skip in a serial pipeline, i.e. only safe to use in single-threaded execution or by a serial pipeline in parallel execution
  object SerialSkipStateFactory extends ArgumentStateFactory[SerialCountingState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): SerialCountingState =
      new StandardSerialSkipState(argumentRowId, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): SerialCountingState =
    // NOTE: This is actually _not_ threadsafe and only safe to use in a serial pipeline!
      new VolatileSerialSkipState(argumentRowId, argumentRowIdsForReducers)
  }

  class StandardSerialSkipState(override val argumentRowId: Long,
                                override val argumentRowIdsForReducers: Array[Long]) extends SerialCountingState {

    private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"StandardSerialTopLevelSkipState($argumentRowId, countLeft=$countLeft)"

    override def shallowSize: Long = StandardSerialSkipState.SHALLOW_SIZE
  }

  object StandardSerialSkipState {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[StandardSerialSkipState])
  }

  /**
    * The SerialTopLevelSkipState intended for use with `SerialTopLevelSkipOperatorTaskTemplate` when used
    * in `ParallelRuntime`. It provides thread-safe calls of `isCancelled`, while all other methods have
    * to be accessed in serial.
    */
  class VolatileSerialSkipState(override val argumentRowId: Long,
                                override val argumentRowIdsForReducers: Array[Long]) extends SerialCountingState {

    @volatile private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"VolatileSerialTopLevelSkipState($argumentRowId, countLeft=$countLeft)"

    override def shallowSize = VolatileSerialSkipState.SHALLOW_SIZE
  }

  object VolatileSerialSkipState {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[VolatileSerialSkipState])
  }
}

class SerialSkipOnRhsOfApplyOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                 id: Id,
                                                 argumentStateMapId: ArgumentStateMapId,
                                                 generateCountExpression: () => IntermediateExpression)
                                                (codeGen: OperatorExpressionCompiler)
  extends SerialCountingOperatorOnRhsOfApplyOperatorTaskTemplate(inner, id, argumentStateMapId, generateCountExpression, codeGen) {

  override protected def beginOperate: IntermediateRepresentation =
    declareAndAssign(typeRefOf[Long], argumentVarName(argumentStateMapId), getArgument(argumentStateMapId))

  override protected def innerOperate: IntermediateRepresentation = {
    ifElse(equal(load(countLeftVar), constant(0)))(
      block(
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          block(
            profileRow(id, doProfile)
          )
        )))(
      doIfInnerCantContinue(assign(countLeftVar, subtract(load(countLeftVar), constant(1))))
    )
  }

  override protected def isHead: Boolean = false
}
