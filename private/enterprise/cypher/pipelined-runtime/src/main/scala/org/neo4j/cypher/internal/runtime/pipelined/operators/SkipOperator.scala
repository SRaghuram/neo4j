/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.ConcurrentCountingState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.StandardCountingState
import org.neo4j.cypher.internal.runtime.pipelined.operators.CountingState.evaluateCountValue
import org.neo4j.cypher.internal.runtime.pipelined.operators.SkipOperator.SkipStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

object SkipOperator {
  class SkipStateFactory(count: Long) extends ArgumentStateFactory[CountingState] {
    override def newStandardArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): CountingState =
      new StandardCountingState(argumentRowId, count, argumentRowIdsForReducers) {
        override def isCancelled: Boolean = false
      }

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): CountingState =
      new ConcurrentCountingState(argumentRowId, count, argumentRowIdsForReducers) {
        override def isCancelled: Boolean = false
      }
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
      asm.filterWithSideEffect[SkipFilterState](output,
                                            (rowCount, nRows) => new SkipFilterState(rowCount.reserve(nRows)),
                                            (x, _) => x.next())
    }

    override def workIdentity: WorkIdentity = SkipOperator.this.workIdentity

    /**
      * Filter state for the rows from one argumentRowId within one morsel.
      */
    class SkipFilterState(var countLeft: Long) {
      def next(): Boolean = {
        if (countLeft > 0) {
          countLeft -= 1
          false
        } else {
          true
        }
      }
    }
  }
}
