/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.scheduling

import java.util.concurrent.ExecutorService
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.duration.Duration

class SimpleSchedulerTest extends SchedulerTest {
   private var _service: ExecutorService = _

  override def newScheduler(maxConcurrency: Int): Scheduler[Resource.type] = {
    val service: ExecutorService = Executors.newFixedThreadPool(maxConcurrency)
    _service = service
    new SimpleScheduler(service, Duration(1, TimeUnit.SECONDS), () => Resource, maxConcurrency)
  }

  override def shutDown(): Unit = {
    if (_service != null) {
      _service.shutdown()
    }
  }
}
