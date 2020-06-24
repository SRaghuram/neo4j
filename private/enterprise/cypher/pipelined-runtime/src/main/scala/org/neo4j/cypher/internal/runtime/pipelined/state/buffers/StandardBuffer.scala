/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import java.util

import org.neo4j.cypher.internal.runtime.MemoizingMeasurable
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.memory.MemoryTracker

import scala.collection.JavaConverters.asJavaIteratorConverter
import scala.collection.mutable.ArrayBuffer

/**
 * Implementation of a standard non-Thread-safe buffer of elements of type T.
 */
class StandardBuffer[T <: AnyRef] extends Buffer[T] {
  private val data = new ArrayBuffer[T]

  override def put(t: T, resources: QueryResources): Unit = {
    data.append(t)
  }

  override def canPut: Boolean = data.size < Buffer.MAX_SIZE_HINT

  override def hasData: Boolean = data.nonEmpty

  override def take(): T = {
    if (data.isEmpty) null.asInstanceOf[T]
    else data.remove(0)
  }

  override def foreach(f: T => Unit): Unit = {
    data.foreach(f)
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb ++= s"${getClass.getSimpleName}("
    data.foreach(t => {
      sb ++= t.toString
      sb += ','
    })
    sb += ')'
    sb.result()
  }

  override def iterator: util.Iterator[T] = data.iterator.asJava
}

class MemoryTrackingStandardBuffer[T <: MemoizingMeasurable](memoryTracker: MemoryTracker) extends StandardBuffer[T] {
  override def put(t: T, resources: QueryResources): Unit = {
    memoryTracker.allocateHeap(t.estimatedHeapUsageWithCache)
    super.put(t, resources)
  }

  override def take(): T = {
    val t = super.take()
    if (t != null) {
      memoryTracker.releaseHeap(t.estimatedHeapUsageWithCache)
      t.clearCachedEstimatedHeapUsage()
    }
    t
  }

  override def close(): Unit = {
    super.close()
    super.foreach(t => {
      memoryTracker.releaseHeap(t.estimatedHeapUsageWithCache)
      t.clearCachedEstimatedHeapUsage()
    })
  }
}
