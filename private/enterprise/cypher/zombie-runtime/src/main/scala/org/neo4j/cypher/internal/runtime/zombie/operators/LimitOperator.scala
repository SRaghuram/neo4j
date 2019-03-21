/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.morsel.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.zombie._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.storable.NumberValue
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}

/**
  * Limit the number of rows to `countExpression` per argument.
  */
class LimitOperator(val planId: Id,
                    val workIdentity: WorkIdentity,
                    countExpression: Expression) extends MiddleOperator {

  override def createState(argumentStateCreator: ArgumentStateCreator,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): OperatorTask = {

    val queryState = new OldQueryState(queryContext,
                                       resources = null,
                                       params = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession],
                                       resources.expressionVariables(state.nExpressionSlots))

    val count = countExpression(ExecutionContext.empty, queryState).asInstanceOf[NumberValue].longValue()

    new LimitOperatorState(argumentStateCreator.createArgumentStateMap(planId,
                                                                       argumentRowId => new LimitState(argumentRowId,
                                                                                                       count)))
  }

  class LimitOperatorState(argumentStateMap: ArgumentStateMap[LimitState]) extends OperatorTask {

    override def operate(output: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      argumentStateMap.filter[FilterState](output,
                                           (rowCount, nRows) => new FilterState(rowCount.reserve(nRows)),
                                           (x, morsel) => x.next())
    }
  }

  /**
    * Filter state for the rows from one argumentRowId within one morsel.
    */
  class FilterState(var countLeft: Long) {
    def next(): Boolean = {
      if (countLeft > 0) {
        countLeft -= 1
        true
      } else
        false
    }
  }

  /**
    * Query-write row count for the rows from one argumentRowId.
    */
  class LimitState(override val argumentRowId: Long, countTotal: Long) extends WorkCanceller {
    private var countLeft = countTotal

    def reserve(wanted: Long): Long = {
      val got = math.min(countLeft, wanted)
      countLeft -= got
      got
    }

    override def isCancelled: Boolean = countLeft == 0

    override def toString: String = s"LimitState($argumentRowId, countLeft=$countLeft)"
  }
}
