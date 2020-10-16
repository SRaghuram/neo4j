/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThan
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.codegen.api.LocalVariable
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.LimitOperator.LimitState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_COUNTER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_MORSEL
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SHOULD_BREAK
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.belowLimitVarName
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRows
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.pipelined.state.FilterStateWithIsLast
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

object LimitOperator {

  trait LimitState extends CountingState with WorkCanceller {
    self: CountingState =>
    def isCancelled: Boolean = getCount <= 0
    def remaining: Long = getCount
  }

  class LimitStateFactory(count: Long) extends ArgumentStateFactory[LimitState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): LimitState =
      new StandardCountingState(argumentRowId, count, argumentRowIdsForReducers) with LimitState

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): LimitState =
      new ConcurrentCountingState(argumentRowId, count, argumentRowIdsForReducers) with LimitState

    override def completeOnConstruction: Boolean = true
  }
}

/**
 * Limit the number of rows to `countExpression` per argument.
 */
class LimitOperator(argumentStateMapId: ArgumentStateMapId,
                    val workIdentity: WorkIdentity,
                    countExpression: Expression) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator, stateFactory: StateFactory, state: PipelinedQueryState, resources: QueryResources): OperatorTask = {
    val limit = evaluateCountValue(state, resources, countExpression)
    new LimitOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
      new LimitOperator.LimitStateFactory(limit)))
  }

  class LimitOperatorTask(argumentStateMap: ArgumentStateMap[LimitState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = LimitOperator.this.workIdentity

    override def operate(outputMorsel: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = {

      argumentStateMap.filterWithSideEffect[FilterState](outputMorsel,
        (rowCount, nRows) => {
          val reserved = rowCount.reserve(nRows)
          FilterStateWithIsLast(new FilterState(reserved), rowCount.isCancelled)
        },
        (x, _) => x.next())
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  }

  /**
   * Filter state for the rows from one argumentRowId within one morsel.
   */
  class FilterState(var countLeft: Long) {
    def next(): Boolean = {
      if (countLeft > 0) {
        countLeft -= 1
        true
      } else
        false
    }
  }

}

/**
 * This is a (common) special case used when not nested under an apply, so we do not need to worry about the argument state map.
 * It also needs to be run either single threaded execution or in a serial pipeline (i.e. the final produce pipeline) in parallel execution,
 * since it does not synchronize the limit count between tasks.
 */
class SerialTopLevelLimitOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                              id: Id,
                                              innermost: DelegateOperatorTaskTemplate,
                                              argumentStateMapId: ArgumentStateMapId,
                                              generateCountExpression: () => IntermediateExpression)
                                             (codeGen: OperatorExpressionCompiler)
  extends SerialTopLevelCountingOperatorTaskTemplate(inner, id, innermost, argumentStateMapId, generateCountExpression, codeGen) {

  override def genLocalVariables: Seq[LocalVariable] = super.genLocalVariables :+ SHOULD_BREAK

  override def genOperate: IntermediateRepresentation = {
    block(
      condition(greaterThan(load(countLeftVar), constant(0))) (
        block(
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
              assign(countLeftVar, subtract(load(countLeftVar), constant(1)))))
        ) ,
      doIfInnerCantContinue(
        condition(equal(load(countLeftVar), constant(0)))(
          assign(SHOULD_BREAK, constant(true)))
        )
      )
  }

  override protected def howMuchToReserve: IntermediateRepresentation = {
    if (innermost.shouldWriteToContext) {
      // Use the available output morsel rows to determine our maximum chunk of the total limit
      invoke(OUTPUT_MORSEL, method[Morsel, Int]("numberOfRows"))
    } else if (innermost.shouldCheckOutputCounter) {
      // Use the output counter to determine our maximum chunk of the total limit
      load(OUTPUT_COUNTER)
    } else {
      // We do not seem to have any bound on the output of this task (i.e. we are the final produce result pipeline task)
      // Reserve as much as we can get
      constant(Int.MaxValue)
    }
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      profileRows(id, cast[Long](subtract(load(reservedVar), load(countLeftVar)))),
      super.genOperateExit
    )
  }
}

/**
 * Assumes that this is run on the RHS of an Apply. It works by controlling a local variable `belowLimit` which is set here
 * once the limit has been reached for a given argument. All leaf operators then respects that and will check this variable before calling
 * `next` on its cursor. Something like `this.canContinue = belowLimit && cursor.next`, which will force the iteration to go into the
 * initialize inner loop state where we take the next item of the input cursor and also set `belowLimit` back to `true`
 */
class SerialLimitOnRhsOfApplyOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                                  id: Id,
                                                  argumentStateMapId: ArgumentStateMapId,
                                                  generateCountExpression: () => IntermediateExpression)
                                                 (codeGen: OperatorExpressionCompiler)
  extends SerialCountingOperatorOnRhsOfApplyOperatorTaskTemplate(inner, id, argumentStateMapId, generateCountExpression, codeGen) {

  override protected def beginOperate: IntermediateRepresentation = noop()
  override protected def innerOperate: IntermediateRepresentation = {
    block(
      condition(greaterThan(load(countLeftVar), constant(0)))(
        block(
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
            block(
              profileRow(id),
              assign(countLeftVar, subtract(load(countLeftVar), constant(1)))
            )
          ))
      ),
      condition(equal(load(countLeftVar), constant(0)))(
        assign(belowLimitVarName(argumentStateMapId), constant(false))
      )
    )
  }

}

object SerialTopLevelLimitOperatorTaskTemplate {

  // This is used by fused limit in a serial pipeline, i.e. only safe to use in single-threaded execution or by a serial pipeline in parallel execution
  object SerialLimitStateFactory extends ArgumentStateFactory[LimitState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): LimitState =
      new StandardSerialLimitState(argumentRowId, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): LimitState =
    // NOTE: This is actually _not_ threadsafe and only safe to use in a serial pipeline!
      new VolatileSerialLimitState(argumentRowId, argumentRowIdsForReducers)

    override def completeOnConstruction: Boolean = true
  }

  class StandardSerialLimitState(override val argumentRowId: Long,
                                 override val argumentRowIdsForReducers: Array[Long]) extends SerialCountingState with LimitState {

    private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"StandardSerialTopLevelLimitState($argumentRowId, countLeft=$countLeft)"
    override def isCancelled: Boolean = getCount == 0
  }

  /**
   * The SerialTopLevelLimitState intended for use with `SerialTopLevelLimitOperatorTaskTemplate` when used
   * in `ParallelRuntime`. It provides thread-safe calls of `isCancelled`, while all other methods have
   * to be accessed in serial.
   */
  class VolatileSerialLimitState(override val argumentRowId: Long,
                                 override val argumentRowIdsForReducers: Array[Long]) extends SerialCountingState with LimitState {

    @volatile private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"VolatileSerialTopLevelLimitState($argumentRowId, countLeft=$countLeft)"
    override def isCancelled: Boolean = getCount == 0
  }
}
