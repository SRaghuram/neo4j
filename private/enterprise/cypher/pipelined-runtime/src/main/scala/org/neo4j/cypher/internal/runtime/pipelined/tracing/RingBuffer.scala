/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.tracing

object RingBuffer {
  val defaultBitSize: Int = 10 // buffer size 1024
  val defaultMaxRetries: Int = 1000
}

/**
  * Single producer, single consumer ring buffer.
  *
  * This means that the ring buffer is safe to use with one thread producing values, and another one consuming them.
  *
  * @param bitSize size of the ring buffer in number of bits
  * @param maxRetries number of retries to attempt if the buffer is full
  */
class RingBuffer(bitSize: Int = RingBuffer.defaultBitSize,
                 private val maxRetries: Int = RingBuffer.defaultMaxRetries) {

  @volatile private var produceCount: Int = 0
  @volatile private var consumeCount: Int = 0

  val size: Int = 1 << bitSize
  private val mask: Int = size - 1
  private val buffer = new Array[DataPoint](size)

  /**
    * Produce (add) one additional DataPoint into this RingBuffer. Not Thread-safe.
    *
    * @param dp the DataPoint to produce
    */
  def produce(dp: DataPoint): Unit = {
    var claimed = -1
    val snapshotProduce = produceCount
    var retries = 0
    while (claimed == -1) {
      val snapshotConsume = consumeCount
      if (snapshotProduce - size < snapshotConsume) {
        claimed = snapshotProduce & mask
        buffer(claimed) = dp
        produceCount += 1
      } else {
        retries += 1
        if (retries < maxRetries) {
          Thread.sleep(1)
        } else {
          // full buffer can prevent query execution from making progress
          throw new RuntimeException("Exceeded max retries")
        }
      }
    }
  }

  /**
    * Consume all available DataPoints from this RingBuffer. Not thread-safe.
    */
  def consume(f: DataPoint => Unit): Unit = {
    var snapshotConsume = consumeCount
    val snapshotProduce = produceCount
    while (snapshotConsume < snapshotProduce) {
      f(buffer(snapshotConsume & mask))
      snapshotConsume += 1
    }
    consumeCount = snapshotConsume
  }
}
