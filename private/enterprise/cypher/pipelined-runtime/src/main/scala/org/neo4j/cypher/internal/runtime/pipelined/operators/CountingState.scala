/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.WorkCanceller
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.FloatingPointValue

object CountingState {
  def evaluateCountValue(queryContext: QueryContext,
                         state: QueryState,
                         resources: QueryResources,
                         countExpression: Expression): Long = {
    val queryState = new SlottedQueryState(queryContext,
                                           resources = null,
                                           params = state.params,
                                           resources.expressionCursors,
                                           Array.empty[IndexReadSession],
                                           resources.expressionVariables(state.nExpressionSlots),
                                           state.subscriber,
                                           NoMemoryTracker)

    val countValue = countExpression(CypherRow.empty, queryState)
    evaluateCountValue(countValue)
  }

  def evaluateCountValue(countValue: AnyValue): Long = {
    val countNumber = NumericHelper.asNumber(countValue)
    if (countNumber.isInstanceOf[FloatingPointValue]) {
      val count = countNumber.doubleValue()
      throw new InvalidArgumentException(s"Invalid input. '$count' is not a valid value. Must be a non-negative integer.")
    }
    val count = countNumber.longValue()

    if (count < 0) {
      throw new InvalidArgumentException(s"Invalid input. '$count' is not a valid value. Must be a non-negative integer.")
    }
    count
  }

  abstract class StandardCountingState(override val argumentRowId: Long,
                                       countTotal: Long,
                                       override val argumentRowIdsForReducers: Array[Long]) extends CountingState {
    protected var countLeft: Long = countTotal

    override def reserve(wanted: Long): Long = {
      val got = math.min(countLeft, wanted)
      countLeft -= got
      got
    }
    override def getCount: Long = countLeft
    override def toString: String = s"StandardCountingState($argumentRowId, countLeft=$countLeft)"
  }

  abstract class ConcurrentCountingState(override val argumentRowId: Long,
                                         countTotal: Long,
                                         override val argumentRowIdsForReducers: Array[Long]) extends CountingState {
    private val countLeft = new AtomicLong(countTotal)

    def reserve(wanted: Long): Long = {
      if (countLeft.get() <= 0) {
        0L
      } else {
        val newCountLeft = countLeft.addAndGet(-wanted)
        if (newCountLeft >= 0) {
          wanted
        } else {
          math.max(0L, wanted + newCountLeft)
        }
      }
    }
    override def getCount: Long = countLeft.get()

    override def toString: String = s"ConcurrentCountingState($argumentRowId, countLeft=${countLeft.get()})"
  }
}

/**
  * Query-wide row count for the rows from one argumentRowId.
  */
abstract class CountingState extends WorkCanceller {
  def reserve(wanted: Long): Long

  protected def getCount: Long
}
