/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.CartesianProductOperator.LHSMorsel
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.ArgumentStateBuffer
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselAttachBuffer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

class CartesianProductOperator(val workIdentity: WorkIdentity,
                               lhsArgumentStateMapId: ArgumentStateMapId,
                               rhsArgumentStateMapId: ArgumentStateMapId,
                               argumentSize: SlotConfiguration.Size)
                              (val id: Id = Id.INVALID_ID)
  extends Operator with AccumulatorsAndMorselInputOperatorState[Morsel, LHSMorsel, Morsel] {

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    argumentStateCreator.createArgumentStateMap(lhsArgumentStateMapId, new LHSMorsel.Factory(stateFactory), memoryTracker)
    argumentStateCreator.createArgumentStateMap(rhsArgumentStateMapId, new ArgumentStateBuffer.Factory(stateFactory, id), memoryTracker)
    this
  }

  override def nextTasks(input: Buffers.AccumulatorAndPayload[Morsel, LHSMorsel, Morsel]): IndexedSeq[ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, LHSMorsel]] =
    Array(new OTask(input.acc, input.payload))

  // Extending InputLoopTask first to get the correct producingWorkUnitEvent implementation
  class OTask(override val accumulator: LHSMorsel, rhsMorsel: Morsel)
    extends InputLoopTask(accumulator.lhsMorsel)
    with ContinuableOperatorTaskWithMorselAndAccumulator[Morsel, LHSMorsel] {

    private val rhsInputCursor: MorselReadCursor = rhsMorsel.readCursor()

    override def workIdentity: WorkIdentity = CartesianProductOperator.this.workIdentity

    private val lhsSlots = inputMorsel.slots

    override def toString: String = "CartesianProductTask"

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      rhsInputCursor.setToStart()
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && rhsInputCursor.hasNext) {
        rhsInputCursor.next()
        outputRow.copyFrom(inputCursor)  // lhs
        outputRow.copyFromOffset(rhsInputCursor,
          sourceLongOffset = argumentSize.nLongs, // Skip over arguments since they should be identical to lhsCtx
          sourceRefOffset = argumentSize.nReferences,
          targetLongOffset = lhsSlots.numberOfLongs,
          targetRefOffset = lhsSlots.numberOfReferences)
        outputRow.next()
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}
  }

}

object CartesianProductOperator {

  class LHSMorsel(override val argumentRowId: Long,
                  val lhsMorsel: Morsel,
                  override val argumentRowIdsForReducers: Array[Long])
    extends MorselAccumulator[Morsel] {

    override def update(morsel: Morsel, resources: QueryResources): Unit =
      throw new IllegalStateException("LHSMorsel is complete on construction, and cannot be further updated.")

    override def toString: String = {
      s"LHSMorsel(argumentRowId=$argumentRowId)"
    }

    override final def shallowSize: Long = LHSMorsel.SHALLOW_SIZE
  }

  object LHSMorsel {
    private final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[LHSMorsel])

    /**
     * This Factory creates [[LHSMorsel]] instances by detaching the morsel which is attached to the argument morsel. It has been attached in
     * the [[MorselAttachBuffer]].
     */
    class Factory(stateFactory: StateFactory) extends ArgumentStateFactory[LHSMorsel] {
      override def newStandardArgumentState(argumentRowId: Long,
                                            argumentMorsel: MorselReadCursor,
                                            argumentRowIdsForReducers: Array[Long],
                                            memoryTracker: MemoryTracker): LHSMorsel =
        new LHSMorsel(argumentRowId, argumentMorsel.morsel.detach(), argumentRowIdsForReducers)

      override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor,
                                              argumentRowIdsForReducers: Array[Long]): LHSMorsel =
        new LHSMorsel(argumentRowId, argumentMorsel.morsel.detach(), argumentRowIdsForReducers)

      override def completeOnConstruction: Boolean = true
    }
  }
}
