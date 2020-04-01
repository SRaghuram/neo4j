/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id

class AllOrderedDistinctOperator(argumentStateMapId: ArgumentStateMapId,
                                 val workIdentity: WorkIdentity,
                                 groupings: GroupingExpression)
                                (val id: Id = Id.INVALID_ID) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources): OperatorTask =
    new DistinctOperatorTask(
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AllOrderedDistinctStateFactory(stateFactory.memoryTracker), ordered = false),
      workIdentity)

  class AllOrderedDistinctStateFactory(memoryTracker: QueryMemoryTracker) extends ArgumentStateFactory[AllOrderedDistinctState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): AllOrderedDistinctState =
      new AllOrderedDistinctState(argumentRowId, argumentRowIdsForReducers, memoryTracker)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): AllOrderedDistinctState =
      throw new IllegalStateException("OrderedDistinct is not supported in parallel")

    override def completeOnConstruction: Boolean = true
  }

  class AllOrderedDistinctState(override val argumentRowId: Long,
                                override val argumentRowIdsForReducers: Array[Long],
                                memoryTracker: QueryMemoryTracker) extends DistinctOperatorState {

    private var prevGroupingKey: groupings.KeyType = _

    override def filterOrProject(row: ReadWriteRow, queryState: QueryState): Boolean = {
      val groupingKey = groupings.computeGroupingKey(row, queryState)
      if (groupingKey != prevGroupingKey) {
        groupings.project(row, groupingKey)
        prevGroupingKey = groupingKey
        true
      } else {
        false
      }
    }
    override def toString: String = s"AllOrderedDistinctState($argumentRowId)"
  }
}
