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
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRows
import org.neo4j.cypher.internal.runtime.pipelined.operators.SkipOperator.SkipStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

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
                   countExpression: Expression) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          queryContext: QueryContext,
                          state: QueryState,
                          resources: QueryResources): OperatorTask = {
    val skip = evaluateCountValue(queryContext, state, resources, countExpression)
    new SkipOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
                                                                     new SkipStateFactory(skip)))
  }

  class SkipOperatorTask(asm: ArgumentStateMap[CountingState]) extends OperatorTask {

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      //nothing to do here
    }

    override def operate(output: Morsel,
                         context: QueryContext,
                         state: QueryState,
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


  override protected def howMuchToReserve: IntermediateRepresentation = constant(Int.MaxValue)

  override def genOperate: IntermediateRepresentation = {
      IntermediateRepresentation.ifElse(equal(load(countLeftVar), constant(0)))(inner.genOperateWithExpressions)(
        doIfInnerCantContinue(
          block(
            if (innermost.shouldCheckOutputCounter) OperatorCodeGenHelperTemplates.UPDATE_OUTPUT_COUNTER else noop(),
            assign(countLeftVar, subtract(load(countLeftVar), constant(1))),
            )
        )
      )
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      invoke(loadField(countingStateField), method[SerialTopLevelCountingState, Unit, Int]("update"),
             subtract(load(reservedVar), load(countLeftVar))),
      profileRows(id,subtract(load(reservedVar), load(countLeftVar))),
      inner.genOperateExit)
  }
}

object SerialTopLevelSkipOperatorTaskTemplate {

  // This is used by fused skip in a serial pipeline, i.e. only safe to use in single-threaded execution or by a serial pipeline in parallel execution
  object SerialTopLevelSkipStateFactory extends ArgumentStateFactory[SerialTopLevelCountingState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): SerialTopLevelCountingState =
      new StandardSerialTopLevelSkipState(argumentRowId, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): SerialTopLevelCountingState =
    // NOTE: This is actually _not_ threadsafe and only safe to use in a serial pipeline!
      new VolatileSerialTopLevelSkipState(argumentRowId, argumentRowIdsForReducers)
  }

  class StandardSerialTopLevelSkipState(override val argumentRowId: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends SerialTopLevelCountingState {

    private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"StandardSerialTopLevelSkipState($argumentRowId, countLeft=$countLeft)"
    override def isCancelled: Boolean = false
  }

  /**
    * The SerialTopLevelSkipState intended for use with `SerialTopLevelSkipOperatorTaskTemplate` when used
    * in `ParallelRuntime`. It provides thread-safe calls of `isCancelled`, while all other methods have
    * to be accessed in serial.
    */
  class VolatileSerialTopLevelSkipState(override val argumentRowId: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends SerialTopLevelCountingState {

    @volatile private var countLeft: Long = -1L

    override protected def getCount: Long = countLeft
    override protected def setCount(count: Long): Unit = countLeft = count
    override def toString: String = s"VolatileSerialTopLevelSkipState($argumentRowId, countLeft=$countLeft)"
    override def isCancelled: Boolean = false

  }
}