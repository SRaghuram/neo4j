/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.ConcurrentHashMap

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctSinglePrimitiveOperator.DistinctSinglePrimitiveState
import org.neo4j.cypher.internal.runtime.pipelined.operators.DistinctSinglePrimitiveOperator.DistinctSinglePrimitiveStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

class DistinctSinglePrimitiveOperatorTask[S <: DistinctSinglePrimitiveState](argumentStateMap: ArgumentStateMap[S],
                                                                             val workIdentity: WorkIdentity,
                                                                             setInSlot: (WritableRow, AnyValue) => Unit,
                                                                             offset: Int,
                                                                             expression: commands.expressions.Expression) extends OperatorTask {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    argumentStateMap.filterWithSideEffect[S](outputMorsel,
      (distinctState, _) => distinctState,
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
    new DistinctSinglePrimitiveOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
      new DistinctSinglePrimitiveStateFactory(memoryTracker)), workIdentity, setInSlot, offset, expression)
  }
}

object DistinctSinglePrimitiveOperator {
  class DistinctSinglePrimitiveStateFactory(memoryTracker: MemoryTracker) extends ArgumentStateFactory[DistinctSinglePrimitiveState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctSinglePrimitiveState = {
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
  }

  class ConcurrentDistinctSinglePrimitiveState(override val argumentRowId: Long, override val argumentRowIdsForReducers: Array[Long])
    extends DistinctSinglePrimitiveState {

    private val seenSet = ConcurrentHashMap.newKeySet[Long]()

    // This is called from generated code by genCreateState
    override def setMemoryTracker(memoryTracker: MemoryTracker): Unit = {}

    override def seen(key: Long): Boolean =
      seenSet.add(key)

    override def toString: String = s"ConcurrentDistinctSinglePrimitiveState($argumentRowId)"
  }
}
