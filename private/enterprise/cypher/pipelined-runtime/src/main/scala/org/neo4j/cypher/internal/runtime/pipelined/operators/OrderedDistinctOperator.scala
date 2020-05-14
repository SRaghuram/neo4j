/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OrderedDistinctOperator.AbstractOrderedDistinctState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.memory.MemoryTracker

class OrderedDistinctOperator(argumentStateMapId: ArgumentStateMapId,
                              val workIdentity: WorkIdentity,
                              orderedGroupings: GroupingExpression,
                              unorderedGroupings: GroupingExpression)
                             (val id: Id = Id.INVALID_ID) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources): OperatorTask = {
    val memoryTracker = stateFactory.newMemoryTracker(id.x)
    new OrderedDistinctOperatorTask(
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new OrderedDistinctStateFactory(memoryTracker)),
      workIdentity)
  }

  class OrderedDistinctStateFactory(memoryTracker: MemoryTracker) extends ArgumentStateFactory[OrderedDistinctState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): OrderedDistinctState =
      new OrderedDistinctState(argumentRowId, argumentRowIdsForReducers, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): OrderedDistinctState =
      throw new IllegalStateException("OrderedDistinct is not supported in parallel")

    override def completeOnConstruction: Boolean = true
  }

  class OrderedDistinctState(override val argumentRowId: Long,
                             override val argumentRowIdsForReducers: Array[Long],
                             memoryTracker: MemoryTracker) extends AbstractOrderedDistinctState {

    private var prevOrderedGroupingKey: orderedGroupings.KeyType = _
    private var seen: DistinctSet[unorderedGroupings.KeyType] = _

    override def filterOrProject(row: ReadWriteRow, queryState: QueryState): Boolean = {
      val orderedGroupingKey = orderedGroupings.computeGroupingKey(row, queryState)
      if (orderedGroupingKey != prevOrderedGroupingKey) {
        if (seen != null) {
          seen.close()
        }
        seen = DistinctSet.createDistinctSet[unorderedGroupings.KeyType](memoryTracker)
        prevOrderedGroupingKey = orderedGroupingKey
      }

      val unorderedGroupingKey = unorderedGroupings.computeGroupingKey(row, queryState)
      if (seen.add(unorderedGroupingKey)) {
        orderedGroupings.project(row, orderedGroupingKey)
        unorderedGroupings.project(row, unorderedGroupingKey)
        true
      } else {
        false
      }
    }

    override def close(): Unit = {
      seen.close()
      seen = null
      super.close()
    }
    override def toString: String = s"OrderedDistinctState($argumentRowId)"
  }
}

object OrderedDistinctOperator {
  trait AbstractOrderedDistinctState extends ArgumentState {
    def filterOrProject(row: ReadWriteRow, queryState: QueryState): Boolean
  }
}

class OrderedDistinctOperatorTask[S <: AbstractOrderedDistinctState](argumentStateMap: ArgumentStateMap[S], val workIdentity: WorkIdentity) extends OperatorTask {

  override def operate(outputMorsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)

    argumentStateMap.filterWithSideEffect[S](outputMorsel,
      (distinctState, _) => distinctState,
      (distinctState, row) => distinctState.filterOrProject(row, queryState))
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
}
