/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.EmptyResultOperator.EmptyResultAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Sink
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

/**
 * Reducing operator which observes all input morsels until it
 * has seen all, and then outputs an empty morsel.
 * Like all reducing operators, [[EmptyResultOperator]]
 * collects and streams grouped by argument rows ids.
 */
case class EmptyResultOperator(workIdentity: WorkIdentity) {

  override def toString: String = "EmptyResult"

  def mapper(argumentSlotOffset: Int, outputBufferId: BufferId): EmptyResultMapperOperator =
    new EmptyResultMapperOperator(argumentSlotOffset, outputBufferId)

  def reducer(argumentStateMapId: ArgumentStateMapId, operatorId: Id): EmptyResultReduceOperator =
    new EmptyResultReduceOperator(argumentStateMapId)(operatorId)

  class EmptyResultMapperOperator(argumentSlotOffset: Int,
                                  outputBufferId: BufferId) extends OutputOperator {

    override def workIdentity: WorkIdentity = EmptyResultOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = Some(outputBufferId)

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState =
      new State(executionState.getSink[IndexedSeq[PerArgument[Morsel]]](outputBufferId))

    class State(sink: Sink[IndexedSeq[PerArgument[Morsel]]]) extends OutputOperatorState {

      override def workIdentity: WorkIdentity = EmptyResultOperator.this.workIdentity
      override def trackTime: Boolean = true

      override def prepareOutput(morsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): EmptyResultOutput = {
        val perArguments = ArgumentStateMap.map(argumentSlotOffset, morsel, argumentMorsel => argumentMorsel)
        new EmptyResultOutput(perArguments, sink)
      }
    }

    class EmptyResultOutput(perArguments: IndexedSeq[PerArgument[Morsel]],
                       sink: Sink[IndexedSeq[PerArgument[Morsel]]]) extends PreparedOutput {
      override def produce(resources: QueryResources): Unit = {
        sink.put(perArguments, resources)
      }
    }
  }

  /**
   * Operator which streams sorted and limited data, built by [[EmptyResultMapperOperator]] and [[EmptyResultAccumulator]].
   */
  class EmptyResultReduceOperator(argumentStateMapId: ArgumentStateMapId)
                                 (val id: Id = Id.INVALID_ID)
    extends Operator
    with AccumulatorsInputOperatorState[Morsel, MorselAccumulator[Morsel]] {

    override def workIdentity: WorkIdentity = EmptyResultOperator.this.workIdentity

    override def accumulatorsPerTask(morselSize: Int): Int = morselSize

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): AccumulatorsInputOperatorState[Morsel, MorselAccumulator[Morsel]] = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new EmptyResultOperator.Factory(memoryTracker))
      this
    }

    override def nextTasks(input: IndexedSeq[MorselAccumulator[Morsel]],
                           resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[Morsel, MorselAccumulator[Morsel]]] = {
      singletonIndexedSeq(new OTask(input))
    }

    class OTask(override val accumulators: IndexedSeq[MorselAccumulator[Morsel]]) extends ContinuableOperatorTaskWithAccumulators[Morsel, MorselAccumulator[Morsel]] {

      override def workIdentity: WorkIdentity = EmptyResultOperator.this.workIdentity

      override def toString: String = "EmptyResultTask"

      override def operate(outputMorsel: Morsel,
                           state: PipelinedQueryState,
                           resources: QueryResources): Unit = {
        outputMorsel.writeCursor().truncate()
      }

      override def canContinue: Boolean = false
    }

  }

}

object EmptyResultOperator {

  class Factory(memoryTracker: MemoryTracker) extends ArgumentStateFactory[MorselAccumulator[Morsel]] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): MorselAccumulator[Morsel] =
      new EmptyResultAccumulator(argumentRowId, argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): MorselAccumulator[Morsel] =
      new EmptyResultAccumulator(argumentRowId, argumentRowIdsForReducers)
  }

  /**
   * MorselAccumulator that does nothing and does not keep any data
   */
  class EmptyResultAccumulator(override val argumentRowId: Long,
                               override val argumentRowIdsForReducers: Array[Long]) extends MorselAccumulator[Morsel] {
    override def update(data: Morsel, resources: QueryResources): Unit = {}

    //override def shallowSize: Long = EmptyResultAccumulator.SHALLOW_SIZE // TODO: Enable ASM memory tracking
  }

  object EmptyResultAccumulator {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[EmptyResultAccumulator])
  }
}


