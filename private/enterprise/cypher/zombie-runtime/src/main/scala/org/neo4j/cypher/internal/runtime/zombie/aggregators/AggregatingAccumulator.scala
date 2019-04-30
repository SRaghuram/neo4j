/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.aggregators

import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator}
import org.neo4j.values.AnyValue

/**
  * Accumulator that compacts input data using some [[Reducer]]s.
  */
class AggregatingAccumulator(override val argumentRowId: Long,
                             reducers: Array[Reducer]) extends MorselAccumulator[Array[Updater]] {

  override def update(data: Array[Updater]): Unit = {
    var i = 0
    while (i < data.length) {
      reducers(i).update(data(i))
      i += 1
    }
  }

  /**
    * Return the result of the reducer at constructor offset `offset`.
    */
  def result(offset: Int): AnyValue = reducers(offset).result
}

object AggregatingAccumulator {

  class Factory(aggregators: Array[Aggregator]) extends ArgumentStateFactory[AggregatingAccumulator] {
    override def newStandardArgumentState(argumentRowId: Long): AggregatingAccumulator =
      new AggregatingAccumulator(argumentRowId, aggregators.map(_.newStandardReducer))

    override def newConcurrentArgumentState(argumentRowId: Long): AggregatingAccumulator =
      new AggregatingAccumulator(argumentRowId, aggregators.map(_.newConcurrentReducer))
  }
}

/**
  * Computational parallel primitive which allows aggregating data
  * over many input rows. Creates [[Updater]], which performs initial
  * parts of the computations that can be done without synchronization.
  *
  * Also creates [[Reducer]], which performs the final parts of the
  * aggregations. In the reducer synchronization is required.
  */
trait Aggregator {
  def newUpdater: Updater
  def newStandardReducer: Reducer
  def newConcurrentReducer: Reducer
}

/**
  * Performs the initial parts of an aggregation that can be done
  * without synchronization.
  */
trait Updater {
  def update(value: AnyValue): Unit
}

/**
  * Performs the final parts of an aggregation that migth require
  * synchronization.
  */
trait Reducer {
  def update(updater: Updater): Unit
  def result: AnyValue
}
