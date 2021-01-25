/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class RingBufferTest extends CypherFunSuite {

  test("ring buffer should produce correctly when empty") {
    val ringBuffer = new RingBuffer(10)
    val points = ArrayBuffer[DataPoint]()
    ringBuffer.consume(points += _)
    points.length should equal(0)
  }

  test("ring buffer should produce correctly with partially filled") {
    val ringBuffer = new RingBuffer(10)
    val points = ArrayBuffer[DataPoint]()
    ringBuffer.produce(dataPointFor(1, 1))
    ringBuffer.consume(points += _)
    points.length should equal(1)
  }

  test("ring buffer should produce correctly when full") {
    val ringBufferBitSize = 10
    val ringBuffer = new RingBuffer(ringBufferBitSize)

    val expectedPoints: Seq[DataPoint] = dataPointsFor(0, ringBuffer.size, 1)
    val points = ArrayBuffer[DataPoint]()

    expectedPoints.foreach(point => ringBuffer.produce(point))

    ringBuffer.consume(points += _)

    points.toList should equal(expectedPoints)
  }

  test("ring buffer should be reusable after consuming full buffer") {
    val ringBufferBitSize = 10
    val ringBuffer = new RingBuffer(ringBufferBitSize)

    val expectedPoints: Seq[DataPoint] = dataPointsFor(0, ringBuffer.size, 1)
    val points = ArrayBuffer[DataPoint]()

    expectedPoints.foreach(point => ringBuffer.produce(point))
    ringBuffer.consume(points += _)
    points.toList should equal(expectedPoints)

    points.clear()

    ringBuffer.consume(points += _)
    points.toList should equal(Nil)

    expectedPoints.foreach(point => ringBuffer.produce(point))
    ringBuffer.consume(points += _)
    points.toList should equal(expectedPoints)
  }

  test("ring buffer should error when overfilling") {
    val ringBufferBitSize = 10
    val ringBuffer = new RingBuffer(ringBufferBitSize)

    val expectedPoints: Seq[DataPoint] = dataPointsFor(0, ringBuffer.size, 1)

    expectedPoints.foreach(point => ringBuffer.produce(point))

    an[Exception] should be thrownBy ringBuffer.produce(dataPointFor(1, 1))
  }

  // HELPERS

  private def dataPointsFor(min: Int, max: Int, threadId: Int): Seq[DataPoint] = (min until max).map(i => dataPointFor(i, threadId))

  private def dataPointFor(i: Int, threadId: Int) = DataPoint(i, Some(0), 0, 0, 0, threadId, 0, 0, NOP)

}
