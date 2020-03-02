/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

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
class DistinctOperator(argumentStateMapId: ArgumentStateMapId,
                       val workIdentity: WorkIdentity,
                       groupings: GroupingExpression)
                      (val id: Id = Id.INVALID_ID) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources): OperatorTask = {

    new DistinctOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId, new DistinctStateFactory(stateFactory.memoryTracker)))
  }

  class DistinctOperatorTask(argumentStateMap: ArgumentStateMap[DistinctState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = DistinctOperator.this.workIdentity

    override def operate(outputMorsel: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = {

      val queryState = state.queryStateForExpressionEvaluation(resources)

      argumentStateMap.filterWithSideEffect[DistinctState](outputMorsel,
        (distinctState, _) => distinctState,
        (distinctState, row) => distinctState.filterOrProject(row, queryState))
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  }

  class DistinctStateFactory(memoryTracker: QueryMemoryTracker) extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState =
      new DistinctState(argumentRowId, new util.HashSet[groupings.KeyType](), argumentRowIdsForReducers, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): DistinctState =
      new DistinctState(argumentRowId, ConcurrentHashMap.newKeySet[groupings.KeyType](), argumentRowIdsForReducers, memoryTracker)

    override def completeOnConstruction: Boolean = true
  }

  class DistinctState(override val argumentRowId: Long,
                      seen: util.Set[groupings.KeyType],
                      override val argumentRowIdsForReducers: Array[Long],
                      memoryTracker: QueryMemoryTracker) extends ArgumentState {

    def filterOrProject(row: ReadWriteRow, queryState: QueryState): Boolean = {
      val groupingKey = groupings.computeGroupingKey(row, queryState)
      if (seen.add(groupingKey)) {
        // Note: this allocation is currently never de-allocated
        memoryTracker.allocated(groupingKey, id.x)
        groupings.project(row, groupingKey)
        true
      } else {
        false
      }
    }
    override def toString: String = s"DistinctState($argumentRowId, concurrent=${seen.getClass.getPackageName.contains("concurrent")})"
  }
}
