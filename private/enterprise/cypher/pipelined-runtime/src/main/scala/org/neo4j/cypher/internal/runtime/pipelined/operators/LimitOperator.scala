/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThan
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.ConcurrentCountingState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.StandardCountingState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.evaluateCountValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_COUNTER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_MORSEL
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.SHOULD_BREAK
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRows
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

object LimitOperator {

  trait CancellableState {
    self: CountingState =>
    def isCancelled: Boolean = getCount <= 0
  }

  class LimitStateFactory(count: Long) extends ArgumentStateFactory[CountingState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): CountingState =
      new StandardCountingState(argumentRowId, count, argumentRowIdsForReducers) with CancellableState

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): CountingState =
      new ConcurrentCountingState(argumentRowId, count, argumentRowIdsForReducers) with CancellableState
  }
}

/**
 * Limit the number of rows to `countExpression` per argument.
 */
class LimitOperator(argumentStateMapId: ArgumentStateMapId,
                    val workIdentity: WorkIdentity,
                    countExpression: Expression) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator, stateFactory: StateFactory, queryContext: QueryContext, state: QueryState, resources: QueryResources): OperatorTask = {
    val limit = evaluateCountValue(queryContext, state, resources, countExpression)
    new LimitOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
      new LimitOperator.LimitStateFactory(limit)))
  }

  class LimitOperatorTask(argumentStateMap: ArgumentStateMap[CountingState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = LimitOperator.this.workIdentity

    override def operate(outputMorsel: Morsel,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      argumentStateMap.filterWithSideEffect[FilterState](outputMorsel,
        (rowCount, nRows) => new FilterState(rowCount.reserve(nRows)),
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

  override def genOperate: IntermediateRepresentation = {
    block(
      condition(greaterThan(load(countLeftVar), constant(0))) (
        block(
          inner.genOperateWithExpressions,
          doIfInnerCantContinue(
              assign(countLeftVar, subtract(load(countLeftVar), constant(1)))))
        ),
      doIfInnerCantContinue(
        condition(equal(load(countLeftVar), constant(0)))(
          setField(SHOULD_BREAK, constant(true)))
        )
      )
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      invoke(loadField(countingStateField),
             method[SerialTopLevelCountingState, Unit, Int]("update"),
             subtract(load(reservedVar), load(countLeftVar))),
      profileRows(id, subtract(load(reservedVar), load(countLeftVar))),
      condition(greaterThan(load(reservedVar), constant(0)))(
        setField(SHOULD_BREAK, constant(false))
        ),
      inner.genOperateExit
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
}

object SerialTopLevelLimitOperatorTaskTemplate {

  // This is used by fused limit in a serial pipeline, i.e. only safe to use in single-threaded execution or by a serial pipeline in parallel execution
  object SerialTopLevelLimitStateFactory extends ArgumentStateFactory[SerialTopLevelCountingState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): SerialTopLevelCountingState =
      new StandardSerialTopLevelLimitState(argumentRowId, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): SerialTopLevelCountingState =
    // NOTE: This is actually _not_ threadsafe and only safe to use in a serial pipeline!
      new VolatileSerialTopLevelLimitState(argumentRowId, argumentRowIdsForReducers)
  }

  class StandardSerialTopLevelLimitState(override val argumentRowId: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends SerialTopLevelCountingState {

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
  class VolatileSerialTopLevelLimitState(override val argumentRowId: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends SerialTopLevelCountingState {

    @volatile private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"VolatileSerialTopLevelLimitState($argumentRowId, countLeft=$countLeft)"
    override def isCancelled: Boolean = getCount == 0
  }
}
