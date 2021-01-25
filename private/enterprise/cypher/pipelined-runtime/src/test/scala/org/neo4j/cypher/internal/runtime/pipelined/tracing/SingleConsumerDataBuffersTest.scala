/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.mutable.ArrayBuffer

class SingleConsumerDataBuffersTest extends CypherFunSuite {

  test("should not lose data points when concurrently written and consumed") {
    val innerWriter = new CollectingDataPointWriter
    val dataBuffers = new SingleConsumerDataBuffers(ringBufferBitSize = 4)

    val random = ThreadLocalRandom.current()
    val maxPointsPerThread = 16
    val produceThreadCount = 32
    val latch = new CountDownLatch(produceThreadCount)
    val producers = (0 until produceThreadCount)
      .map(threadId => {
        val min = threadId * maxPointsPerThread
        val max = min + 1 + random.nextInt(maxPointsPerThread - 1)
        new ProduceThread(min, max, threadId, latch, dataBuffers)
      })
    val consumer = new ConsumeThread(latch, dataBuffers, innerWriter)
    producers.foreach(_.start)
    consumer.start()

    val isFinished = latch.await(10, TimeUnit.SECONDS)

    if (!isFinished)
      fail("Test threads did not finish on time")

    producers.foreach(_.join)
    consumer.join()

    val expectedPoints = producers.flatMap(thread => dataPointsFor(thread.min, thread.max, thread.threadId))

    innerWriter.points.size should equal(expectedPoints.size)

    val resultSet = innerWriter.points.toSet
    val expectSet = expectedPoints.toSet
    val intersect = resultSet.intersect(expectSet)

    resultSet -- intersect should be(empty) // Unwanted result data points
    expectSet -- intersect should be(empty) // Missing result data points
  }

  // HELPERS

  private def dataPointsFor(min: Int, max: Int, threadId: Int): Seq[DataPoint] = (min until max).map(i => dataPointFor(i, threadId))

  private def dataPointFor(i: Int, threadId: Int) = DataPoint(i, Some(0), 0, 0, 0, threadId, 0, 0, NOP)

  class CollectingDataPointWriter extends DataPointFlusher {
    val points: ArrayBuffer[DataPoint] = ArrayBuffer[DataPoint]()

    override def write(dataPoint: DataPoint): Unit = points += dataPoint

    override def flush(): Unit = {}

    override def close(): Unit = {}
  }

  class ProduceThread(val min: Int,
                      val max: Int,
                      val threadId: Int,
                      val latch: CountDownLatch,
                      val dataPointWriter: DataPointWriter) extends Thread {

    override def run(): Unit = {
      dataPointsFor(min, max, threadId).foreach(dataPointWriter.write)
      latch.countDown()
    }
  }

  class ConsumeThread(latch: CountDownLatch,
                      dataBuffers: SingleConsumerDataBuffers,
                      inner: DataPointFlusher) extends Thread {

    override def run(): Unit = {
      while (latch.getCount > 0) {
        dataBuffers.consume(inner)
      }
      dataBuffers.consume(inner)
    }
  }

}
