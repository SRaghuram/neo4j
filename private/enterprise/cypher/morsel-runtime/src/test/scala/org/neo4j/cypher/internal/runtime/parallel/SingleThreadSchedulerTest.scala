/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

class SingleThreadSchedulerTest extends SchedulerTest {
  override def newScheduler(maxConcurrency: Int): Scheduler[Resource.type] = new SingleThreadScheduler(() => Resource)

  override def shutDown(): Unit = {} // Nothing to do
}
