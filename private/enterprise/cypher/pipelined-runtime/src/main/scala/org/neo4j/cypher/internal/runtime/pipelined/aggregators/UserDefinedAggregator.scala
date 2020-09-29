/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue

case class UserDefinedAggregator(callToken: Int, allowed: Array[String]) extends Aggregator {
  override def newStandardReducer(memoryTracker: MemoryTracker): StandardReducer = new UserDefinedStandardReducer(callToken, allowed)
  override def newConcurrentReducer: Reducer = throw new UnsupportedOperationException

  override val standardShallowSize: Long =
    HeapEstimator.shallowSizeOfInstance(classOf[UserDefinedStandardReducer])
}

object UserDefinedAggregator {
  def apply(signature: UserFunctionSignature): UserDefinedAggregator = new UserDefinedAggregator(signature.id, signature.allowed)
}

class UserDefinedStandardReducer(callToken: Int, allowed: Array[String]) extends DirectStandardReducer {

  private var aggregator: org.neo4j.cypher.internal.runtime.UserDefinedAggregator = _
  // Reducer
  override def newUpdater(): Updater = this

  // Updater
  override def initialize(state: QueryState): Unit = {
    if (aggregator == null) {
      aggregator = state.query.aggregateFunction(callToken, allowed)
    }
  }

  override def add(values: Array[AnyValue]): Unit = aggregator.update(values)
  override def add(value: AnyValue): Unit = aggregator.update(IndexedSeq(value))
  override def result: AnyValue = aggregator.result
}










