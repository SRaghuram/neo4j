/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.morsel.operators.AggregationOffsets
import org.neo4j.cypher.internal.runtime.zombie.state.ArgumentStateMap.{ArgumentStateFactory, MorselAccumulator}
import org.neo4j.util.Preconditions
import org.neo4j.values.AnyValue

abstract class AggregatingAccumulator extends MorselAccumulator

/**
  * TODO: write me
  */
class StandardAggregatingAccumulator(override val argumentRowId: Long,
                                     expressions: Array[Expression],
                                     aggregators: Array[Aggregator]) extends AggregatingAccumulator {

  Preconditions.checkArgument(expressions.length == aggregators.length,
                              "Expected one expression per aggregator, but got %d expressions and %d aggregators".format(expressions.length, aggregators.length))

  override def update(morsel: MorselExecutionContext): Unit = {
    var i = 0
    while (i < expressions.length) {
//      expressions(i).apply(morsel, )
      i += 1
    }
  }
}

object AggregatingAccumulator {

  class Factory(aggregations: Array[AggregationOffsets]) extends ArgumentStateFactory[AggregatingAccumulator] {
    override def newStandardArgumentState(argumentRowId: Long): AggregatingAccumulator = ???

    override def newConcurrentArgumentState(argumentRowId: Long): AggregatingAccumulator = ???
  }
}

trait Aggregator {
  def updater: Updater
}

trait Updater {
  def update(value: AnyValue): Unit
  def applyToAggregator(): Unit
}
