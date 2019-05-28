/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.morsel.execution._

/**
  * Manages [[Worker]]s and checks that all worker related resources are released.
  */
abstract class WorkerManager(val numberOfWorkers: Int,
                             val queryManager: QueryManager,
                             queryResourceFactory: () => QueryResources) {

  protected val workers: Array[Worker] =
    (for (workerId <- 0 until numberOfWorkers) yield {
      new Worker(workerId, queryManager, LazyScheduling, queryResourceFactory())
    }).toArray

  def assertAllReleased(): Unit = {
    val liveCounts = new LiveCounts()
    for (w <- workers) {
      w.collectCursorLiveCounts(liveCounts)
    }
    liveCounts.assertAllReleased()

    for (worker <- workers) {
      if (worker.sleeper.isActive) {
        throw new RuntimeResourceLeakException(s"$worker is ACTIVE even though all resources should be released!")
      }
    }
  }
}
