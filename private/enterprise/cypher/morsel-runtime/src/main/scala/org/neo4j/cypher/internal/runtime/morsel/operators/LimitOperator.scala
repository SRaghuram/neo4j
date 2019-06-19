/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.{ArgumentStateFactory, WorkCanceller}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.internal.kernel.api.{IndexReadSession, KernelReadTracer}
import org.neo4j.values.storable.NumberValue

/**
  * Limit the number of rows to `countExpression` per argument.
  */
class LimitOperator(argumentStateMapId: ArgumentStateMapId,
                    val workIdentity: WorkIdentity,
                    countExpression: Expression) extends MiddleOperator {

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          queryContext: QueryContext,
                          state: QueryState,
                          resources: QueryResources): OperatorTask = {

    val queryState = new OldQueryState(queryContext,
                                       resources = null,
                                       params = state.params,
                                       resources.expressionCursors,
                                       Array.empty[IndexReadSession],
                                       resources.expressionVariables(state.nExpressionSlots),
                                       state.subscriber)

    val count = countExpression(ExecutionContext.empty, queryState).asInstanceOf[NumberValue].longValue()

    new LimitOperatorTask(argumentStateCreator.createArgumentStateMap(argumentStateMapId,
                                                                      new LimitStateFactory(count)))
  }

  class LimitOperatorTask(argumentStateMap: ArgumentStateMap[LimitState]) extends OperatorTask {

    override def workIdentity: WorkIdentity = LimitOperator.this.workIdentity

    override def operate(output: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      argumentStateMap.filter[FilterState](output,
                                           (rowCount, nRows) => new FilterState(rowCount.reserve(nRows)),
                                           (x, morsel) => x.next())
    }

    override def setTracer(tracer: KernelReadTracer): Unit = {}
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

  class LimitStateFactory(count: Long) extends ArgumentStateFactory[LimitState] {
    override def newStandardArgumentState(argumentRowId: Long): LimitState =
      new StandardLimitState(argumentRowId, count)

    override def newConcurrentArgumentState(argumentRowId: Long): LimitState =
      new ConcurrentLimitState(argumentRowId, count)
  }

  /**
    * Query-wide row count for the rows from one argumentRowId.
    */
  abstract class LimitState extends WorkCanceller {
    def reserve(wanted: Long): Long
  }

  class StandardLimitState(override val argumentRowId: Long, countTotal: Long) extends LimitState {
    private var countLeft = countTotal

    def reserve(wanted: Long): Long = {
      val got = math.min(countLeft, wanted)
      countLeft -= got
      got
    }

    override def isCancelled: Boolean = countLeft == 0

    override def toString: String = s"StandardLimitState($argumentRowId, countLeft=$countLeft)"
  }

  class ConcurrentLimitState(override val argumentRowId: Long, countTotal: Long) extends LimitState {
    private val countLeft = new AtomicLong(countTotal)

    def reserve(wanted: Long): Long = {
      if (countLeft.get() <= 0) {
        0L
      } else {
        val newCountLeft = countLeft.addAndGet(-wanted)
        debug("newCountLeft:"+newCountLeft)
        if (newCountLeft >= 0) {
          wanted
        } else {
          math.max(0L, wanted + newCountLeft)
        }
      }
    }

    override def isCancelled: Boolean = countLeft.get() <= 0

    override def toString: String = s"ConcurrentLimitState($argumentRowId, countLeft=${countLeft.get()})"
  }
}
