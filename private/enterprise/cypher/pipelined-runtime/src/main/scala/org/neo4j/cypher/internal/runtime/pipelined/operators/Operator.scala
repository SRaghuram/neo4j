/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.NonFatalCypherError
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.SchedulingInputException
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeOperator.updateProfileEvent
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatorAndMorsel
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.pipelined.tracing.WorkUnitEvent
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.memory.Measurable

/**
 * Input to use for starting an operator task.
 */
trait OperatorInput {

  /**
   * Take the next input morsel
   *
   * @return the input morsel, or `null` if no input is available
   */
  def takeMorsel(): MorselParallelizer

  /**
   * Take the next input accumulator
   *
   * @param n the maximum number of accumulators to take
   * @return the input accumulator, or `null` if no input is available
   */
  def takeAccumulators[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](n: Int): IndexedSeq[ACC]

  /**
   * Take the next input accumulator from the LHS and morsel from the RHS.
   *
   * @return the input accumulator and the morsel, or `null` if no input is available
   */
  def takeAccumulatorAndMorsel[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]](): AccumulatorAndMorsel[DATA, ACC]

  /**
   * Take the next data.
   * @return the next data to work on or `null` if no input is available
   */
  def takeData[DATA <: AnyRef](): DATA
}

/**
 * Closer for ContinuableOperatorTasks.
 */
trait OperatorCloser {

  /**
   * Close input morsel.
   */
  def closeMorsel(morsel: Morsel): Unit

  /**
   * Close input data.
   */
  def closeData[DATA <: AnyRef](data: DATA): Unit

  /**
   * Close input accumulators.
   */
  def closeAccumulators(accumulators: IndexedSeq[MorselAccumulator[_]]): Unit

  /**
    * Close input morsel and accumulator.
    */
  def closeMorselAndAccumulatorTask(morsel: Morsel, accumulator: MorselAccumulator[_]): Unit

  /**
   * Remove all rows related to cancelled argumentRowIds from `morsel`.
   *
   * @return `true` if the morsel is completely empty after cancellations
   */
  def filterCancelledArguments(morsel: Morsel): Boolean

  /**
   * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
   *
   * @return `true` if the accumulator was removed
   */
  def filterCancelledArguments(accumulator: MorselAccumulator[_]): Boolean

  /**
   * Remove all rows related to cancelled argumentRowIds from `morsel`.
   * Remove the state of the accumulator, if it is related to a cancelled argumentRowId.
   *
   * @param morsel the input morsel
   * @param accumulator the accumulator
   * @return `true` iff both the morsel and the accumulator are cancelled
   */
  def filterCancelledArguments(morsel: Morsel, accumulator: MorselAccumulator[_]): Boolean
}

/**
 * A executable morsel operator.
 */
trait Operator extends HasWorkIdentity {

  /**
   * Create a new execution state for this operator.
   *
   * @param argumentStateCreator creator used to construct a argumentStateMap for this operator state
   * @param stateFactory The state factory for the ExecutionState.
   *                      This is used e.g. to create buffers, or in many places just to access the memory tracker
   * @return the new execution state for this operator.
   */
  def createState(argumentStateCreator: ArgumentStateMapCreator,
                  stateFactory: StateFactory,
                  state: PipelinedQueryState,
                  resources: QueryResources): OperatorState
}

/**
 * The execution state of an operator. One instance of this is created for every query execution.
 */
trait OperatorState {

  /**
   * Initialize new tasks for this operator. This code path let's operators create
   * multiple output rows for each row in `inputMorsel`.
   *
   * TODO: ArgumentStateMaps are currently needed for fused operators that has argument state, to lookup its ArgumentStateMap in the CompiledTask constructor.
   *       When we solve how to instantiate generated classes from generated code we would like to be able to remove it from this interface,
   *       by generating an OperatorState class that can create instances of a generated OperatorTask class.
   *       It can then pass the correct ArgumentStateMap directly as a constructor parameter.
   */
  def nextTasks(state: PipelinedQueryState,
                operatorInput: OperatorInput,
                parallelism: Int,
                resources: QueryResources,
                argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask]
}

/**
 * The execution state of a reduce operator. One instance of this is created for every query execution.
 */
trait ReduceOperatorState[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]] extends OperatorState {

  def accumulatorsPerTask(morselSize: Int): Int

  final override def nextTasks(state: PipelinedQueryState,
                               operatorInput: OperatorInput,
                               parallelism: Int,
                               resources: QueryResources,
                               argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithAccumulators[DATA, ACC]] = {
    val input = operatorInput.takeAccumulators[DATA, ACC](accumulatorsPerTask(state.morselSize))
    if (input != null) {
      try {
        nextTasks(state, input, resources)
      } catch {
        case NonFatalCypherError(t) =>
          throw SchedulingInputException(input, t)
      }
    } else {
      null
    }
  }

  /**
   * Initialize new tasks for this operator.
   */
  def nextTasks(state: PipelinedQueryState,
                input: IndexedSeq[ACC],
                resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[DATA, ACC]]
}

/**
 * A streaming operator is initialized with an input, to produce 0-n [[ContinuableOperatorTask]].
 */
trait StreamingOperator extends Operator with OperatorState {

  final override def nextTasks(state: PipelinedQueryState,
                               operatorInput: OperatorInput,
                               parallelism: Int,
                               resources: QueryResources,
                               argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
    val input = operatorInput.takeMorsel()
    if (input != null) {
      try {
        nextTasks(state, input, parallelism, resources, argumentStateMaps)
      } catch {
        case NonFatalCypherError(t) =>
          throw SchedulingInputException(input, t)
      }
    } else {
      null
    }
  }

  /**
   * Initialize new tasks for this operator. This code path let's operators create
   * multiple output rows for each row in `inputMorsel`.
   */
  protected def nextTasks(state: PipelinedQueryState,
                          inputMorsel: MorselParallelizer,
                          parallelism: Int,
                          resources: QueryResources,
                          argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel]

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = this
}

trait MiddleOperator extends HasWorkIdentity {
  def createTask(argumentStateCreator: ArgumentStateMapCreator,
                 stateFactory: StateFactory,
                 state: PipelinedQueryState,
                 resources: QueryResources): OperatorTask
}

trait StatelessOperator extends MiddleOperator with OperatorTask {
  final override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                                stateFactory: StateFactory,
                                state: PipelinedQueryState,
                                resources: QueryResources): OperatorTask = this

  // stateless operators by definition do not hold cursors
  final override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}

/**
 * Operator related task.
 */
trait OperatorTask extends HasWorkIdentity {

  def operateWithProfile(output: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources,
                         queryProfiler: QueryProfiler): Unit = {

    val operatorExecutionEvent = queryProfiler.executeOperator(workIdentity.workId)
    resources.setKernelTracer(operatorExecutionEvent)
    if (state.doProfile) {
      resources.profileInformation = new InterpretedProfileInformation
    }
    setExecutionEvent(operatorExecutionEvent)
    try {
      operate(output, state, resources)
      if (operatorExecutionEvent != null) {
        operatorExecutionEvent.rows(output.numberOfRows)
        updateProfileEvent(operatorExecutionEvent, resources.profileInformation)
      }
    } finally {
      setExecutionEvent(null)
      resources.setKernelTracer(null)
      if (operatorExecutionEvent != null) {
        operatorExecutionEvent.close()
      }
    }
  }

  def setExecutionEvent(event: OperatorProfileEvent): Unit

  def operate(output: Morsel,
              state: PipelinedQueryState,
              resources: QueryResources): Unit
}

/**
 * Operator task which might require several operate calls to be fully executed.
 */
trait ContinuableOperatorTask extends OperatorTask with Measurable {
  def canContinue: Boolean
  def close(operatorCloser: OperatorCloser, resources: QueryResources): Unit = {
    // NOTE: we have to close cursors before closing the input to make sure that all cursors
    // are freed before the query is completed
    closeCursors(resources)
    closeInput(operatorCloser)
  }
  protected def closeInput(operatorCloser: OperatorCloser): Unit
  protected def closeCursors(resources: QueryResources): Unit
  def producingWorkUnitEvent: WorkUnitEvent

  /**
   * Remove everything related to cancelled argumentRowIds from to the task's input.
   *
   * @return `true` if the task has become obsolete.
   */
  def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean
}

trait ContinuableOperatorTaskWithMorselData extends ContinuableOperatorTask {
  val morselData: MorselData

  override protected def closeInput(operatorCloser: OperatorCloser): Unit = operatorCloser.closeData(morselData)

  override final def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = false

  override final def producingWorkUnitEvent: WorkUnitEvent = null

  override def estimatedHeapUsage: Long = morselData.morsels.map(_.estimatedHeapUsage).sum
}

trait ContinuableOperatorTaskWithMorsel extends ContinuableOperatorTask {
  val inputMorsel: Morsel

  override protected def closeInput(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeMorsel(inputMorsel)
  }

  override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    operatorCloser.filterCancelledArguments(inputMorsel)
  }

  override def producingWorkUnitEvent: WorkUnitEvent = inputMorsel.producingWorkUnitEvent

  override def estimatedHeapUsage: Long = inputMorsel.estimatedHeapUsage
}

trait ContinuableOperatorTaskWithAccumulators[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]] extends ContinuableOperatorTask {
  val accumulators: IndexedSeq[ACC]

  override protected def closeInput(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeAccumulators(accumulators)
  }

  override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    var cancelled = true
    var i = 0
    while (i < accumulators.size) {
      val accumulator = accumulators(i)
      cancelled = cancelled && operatorCloser.filterCancelledArguments(accumulator)
      i += 1
    }
    cancelled
  }

  override def producingWorkUnitEvent: WorkUnitEvent = null

  // These operators have no cursors
  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  override protected def closeCursors(resources: QueryResources): Unit = {}

  // Since we track memory separately on the ArgumentStates in ArgumentStateMaps, we can disregard any size here.
  override def estimatedHeapUsage: Long = 0
}

trait ContinuableOperatorTaskWithMorselAndAccumulator[DATA <: AnyRef, ACC <: MorselAccumulator[DATA]]
  extends ContinuableOperatorTaskWithMorsel {
  val accumulator: ACC

  override protected def closeInput(operatorCloser: OperatorCloser): Unit = {
    operatorCloser.closeMorselAndAccumulatorTask(inputMorsel, accumulator)
  }

  override def filterCancelledArguments(operatorCloser: OperatorCloser): Boolean = {
    operatorCloser.filterCancelledArguments(inputMorsel, accumulator)
  }

  override def producingWorkUnitEvent: WorkUnitEvent = inputMorsel.producingWorkUnitEvent

  // These operators have no cursors
  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}
