/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.aggregators

import java.util.concurrent.ThreadLocalRandom

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

trait AggregatorTest {

  /**
   * Override to specify how to perform the aggregation
   */
  def runAggregation(values: Seq[Array[AnyValue]]): AnyValue

  /**
   * Helper method for the case when the aggregation is on a single value
   */
  def runSingleAggregation(values: Seq[AnyValue]): AnyValue = runAggregation(values.map(Array[AnyValue](_)))

  private val random: ThreadLocalRandom = ThreadLocalRandom.current()
  private val randomValues: RandomValues = RandomValues.create(random)

  private def runAggregator(reducer: Reducer, values: Seq[Array[AnyValue]]): AnyValue = {
    val upperBound = Math.max(2, values.size / 10)
    val groups = values.grouped(random.nextInt(1, upperBound))
    val updaters = (0 until random.nextInt(2, 5)).map(_ => reducer.newUpdater())
    var i = 0
    for (group <- groups) {
      val updater = updaters(i)
      group.foreach(g => updater.add(g))
      updater.applyUpdates()
      i = (i+1) % updaters.length
    }
    reducer.result
  }

  def runAggregationFunction(getFunction: Array[Expression] => AggregationFunction, values: Seq[Array[AnyValue]]): AnyValue = {
    val inputSize = if (values.isEmpty) 0 else values.head.length
    val args = (0 until inputSize).map(i => Variable(s"x$i"))
    val func = getFunction(args.toArray)
    for (value <- values) {
      val rows = value.zipWithIndex.map {
        case (v, i) => s"x$i" -> v
      }
      func.apply(CypherRow.from(rows:_*), null)
    }
    func.result(null)
  }

  def runStandardAggregator(aggregator: Aggregator, values: Seq[Array[AnyValue]]): AnyValue =
    runAggregator(aggregator.newStandardReducer(EmptyMemoryTracker.INSTANCE), values)

  def runConcurrentAggregator(aggregator: Aggregator, values: Seq[Array[AnyValue]]): AnyValue =
    runAggregator(aggregator.newConcurrentReducer, values)

  protected val randomInts: Seq[Int] = random.ints(50, 0, 15).toArray
  protected val randomIntValues: Seq[Value] = randomInts.map(Values.intValue)
  protected val randomIntValuesWithNulls: Seq[Value] = randomIntValues.take(25) ++ Seq.fill(7)(Values.NO_VALUE) ++ randomIntValues.drop(25)

  protected val randomDurations: Seq[DurationValue] = {
    val a = Seq.fill(30)(randomValues.nextDuration())
    val b = Seq.fill(20)(a(random.nextInt(a.size)))
    a ++ b
  }
  protected val randomDurationsWithNulls: Seq[Value] = randomDurations.take(25) ++ Seq.fill(7)(Values.NO_VALUE) ++ randomDurations.drop(25)
}

trait StandardAggregatorTest extends AggregatorTest {
  def runAggregation(values: Seq[Array[AnyValue]]): AnyValue = runStandardAggregator(aggregator, values)
  def aggregator: Aggregator
}
trait ConcurrentAggregatorTest extends AggregatorTest {
  def runAggregation(values: Seq[Array[AnyValue]]): AnyValue = runConcurrentAggregator(aggregator, values)
  def aggregator: Aggregator
}
trait FunctionAggregatorTest extends AggregatorTest {
  def runAggregation(values: Seq[Array[AnyValue]]): AnyValue = runAggregationFunction(getAggregationFunction, values)
  def getAggregationFunction(e: Expression): AggregationFunction
  def getAggregationFunction(e: Array[Expression]): AggregationFunction = getAggregationFunction(e.head)
}
