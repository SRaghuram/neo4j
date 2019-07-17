/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentState, ArgumentStateFactory}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.internal.kernel.api.IndexReadSession

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
                       groupings: GroupingExpression) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          queryContext: QueryContext,
                          state: QueryState,
                          resources: QueryResources): OperatorTask = {

    new DistinctOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId, DistinctStateFactory))
  }

  class DistinctOperatorTask(argumentStateMap: ArgumentStateMap[DistinctState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = DistinctOperator.this.workIdentity

    override def operate(output: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots),
                                         state.subscriber)

      argumentStateMap.filter[DistinctState](output,
                                             (distinctState, _) => distinctState,
                                             (distinctState, morsel) => distinctState.filterOrProject(morsel, queryState))
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
  }

  object DistinctStateFactory extends ArgumentStateFactory[DistinctState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext): DistinctState =
      new DistinctState(argumentRowId, new util.HashSet[groupings.KeyType]())

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselExecutionContext): DistinctState =
      new DistinctState(argumentRowId, ConcurrentHashMap.newKeySet[groupings.KeyType]())
  }

  class DistinctState(override val argumentRowId: Long,
                      seen: util.Set[groupings.KeyType]) extends ArgumentState {

    def filterOrProject(row: MorselExecutionContext, queryState: OldQueryState): Boolean = {
      val groupingKey = groupings.computeGroupingKey(row, queryState)
      if (seen.add(groupingKey)) {
        groupings.project(row, groupingKey)
        true
      } else {
        false
      }
    }
    override def toString: String = s"DistinctState($argumentRowId, concurrent=${seen.getClass.getPackageName.contains("concurrent")})"
  }
}
