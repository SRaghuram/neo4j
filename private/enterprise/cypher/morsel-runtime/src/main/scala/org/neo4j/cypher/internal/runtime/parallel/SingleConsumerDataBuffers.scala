/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import scala.collection.mutable.ArrayBuffer

/**
  * DataPointWriter which buffers data in a thread-safe way, to later be consumed by a single thread.
  */
class SingleConsumerDataBuffers(ringBufferBitSize: Int = RingBuffer.defaultBitSize,
                                ringBufferMaxRetries: Int = RingBuffer.defaultMaxRetries) extends DataPointWriter {

  private val MAX_CLIENT_THREADS: Int = 1024
  private val buffersByThread: Array[RingBuffer] =
    (0 until MAX_CLIENT_THREADS).map(_ => new RingBuffer(ringBufferBitSize, ringBufferMaxRetries)).toArray

  private var t0: Long = -1
  private var theConsumer: Thread = _

  override def write(dataPoint: DataPoint): Unit = {
    if (dataPoint.executionThreadId < MAX_CLIENT_THREADS) {
      buffersByThread(dataPoint.executionThreadId.toInt).produce(dataPoint)
    } else {
      throw new IllegalArgumentException(s"Thread ID exceeded maximum: ${dataPoint.executionThreadId} > ${MAX_CLIENT_THREADS - 1}")
    }
  }

  def consume(dataPointWriter: DataPointFlusher): Unit = {

    if (theConsumer == null)
      theConsumer = Thread.currentThread()
    else if (theConsumer != Thread.currentThread())
      throw new IllegalStateException(
        s"""Tried to consume SingleConsumerThreadSafeDataBuffers from wrong thread
          |  wanted thread: $theConsumer
          |  but got thread: ${Thread.currentThread()}
        """.stripMargin)

    val dataByProducerThread: Array[ArrayBuffer[DataPoint]] =
      for (threadBuffer <- buffersByThread) yield {
        val dataPoints = new ArrayBuffer[DataPoint]()
        threadBuffer.consume(dataPoint => dataPoints += dataPoint)
        dataPoints
      }

    val threadsWithData = dataByProducerThread.filter(_.nonEmpty)

    if (threadsWithData.nonEmpty) {
      if (t0 == -1) {
        t0 = threadsWithData.map(_.head.scheduledTime).min
      }

      for {
        threadData <- dataByProducerThread
        dataPoint <- threadData
      } dataPointWriter.write(dataPoint.withTimeZero(t0))

      dataPointWriter.flush()
    }
  }
}
