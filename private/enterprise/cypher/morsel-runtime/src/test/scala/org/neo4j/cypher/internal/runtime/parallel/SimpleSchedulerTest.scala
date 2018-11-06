/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.duration.Duration

class SimpleSchedulerTest extends SchedulerTest {
  override def newScheduler(maxConcurrency: Int): Scheduler[Resource.type] =
    new SimpleScheduler(Executors.newFixedThreadPool(maxConcurrency), Duration(1, TimeUnit.SECONDS), () => Resource)
}
