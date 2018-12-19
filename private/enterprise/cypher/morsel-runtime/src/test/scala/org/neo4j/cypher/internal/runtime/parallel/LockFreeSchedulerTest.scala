/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

class LockFreeSchedulerTest extends SchedulerTest {

  private val threadFactory = new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r)
  }

  override def newScheduler(maxConcurrency: Int): Scheduler[Resource.type] =
    new LockFreeScheduler(threadFactory, maxConcurrency, Duration(1, TimeUnit.SECONDS), () => Resource)
}
