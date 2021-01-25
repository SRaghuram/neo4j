/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util.concurrent.atomic.AtomicLong

/**
 * [[IdAllocator]] allocates batches of ids.
 */
trait IdAllocator {
  /**
   * Allocate a batch of `nIds` consecutive ids.
   *
   * @return return the first id in the batch.
   */
  def allocateIdBatch(nIds: Int): Long
}

/**
 * Not thread-safe implementation of [[IdAllocator]].
 */
class StandardIdAllocator extends IdAllocator {
  private var nextFreeId = 0L

  override def allocateIdBatch(nIds: Int): Long = {
    val firstId = nextFreeId
    nextFreeId += nIds
    firstId
  }
}

/**
 * Concurrent implementation of [[IdAllocator]].
 */
class ConcurrentIdAllocator extends IdAllocator {
  private val nextFreeId = new AtomicLong(0)

  override def allocateIdBatch(nIds: Int): Long = nextFreeId.getAndAdd(nIds)
}
