/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.runtime.morsel.execution.{LiveCounts, QueryResources}
import org.neo4j.kernel.lifecycle.Lifecycle

/**
  * Get the resources for a worker by its id. The resources are bound to a Database, while a worker can work for different databases in a DBMS.
  */
class WorkerResourceProvider(numberOfWorkers: Int,
                             newWorkerResources: () => QueryResources) extends Lifecycle {
  private val queryResourcesForWorkers = Array.fill(numberOfWorkers)(newWorkerResources())

  /**
    * Get the resources for the worker with the given id.
    */
  def resourcesForWorker(workerId: Int): QueryResources = queryResourcesForWorkers(workerId)

  /**
    * Assert that all resources are released
    */
  def assertAllReleased(): Unit = {
    val liveCounts = new LiveCounts()
    for (q <- queryResourcesForWorkers) {
      q.cursorPools.collectLiveCounts(liveCounts)
    }
    liveCounts.assertAllReleased()
  }

  override def init(): Unit = {}

  override def start(): Unit = {}

  override def stop(): Unit = {}

  // This is called on database.stop()
  override def shutdown(): Unit = {
    var i = 0
    while (i < queryResourcesForWorkers.length) {
      queryResourcesForWorkers(i).close()
      i += 1
    }
  }
}
