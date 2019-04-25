/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.mockito.Mockito.{never, times, verify}
import org.neo4j.cypher.internal.runtime.morsel.QueryResources
import org.neo4j.cypher.internal.runtime.zombie.execution.{ExecutingQuery, QueryManager, SchedulingPolicy}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class WorkerTest extends CypherFunSuite {

  test("should not reset sleeper when no queries") {
    val ATTEMPTS = 100
    val queryManager: QueryManager =
      new QueryManager {
        private val countDown = new CountDown[ExecutingQuery](ATTEMPTS, null)
        override def nextQueryToWorkOn(workerId: Int): ExecutingQuery = countDown.next()
      }
    val schedulingPolicy = new SchedulingPolicy {
      override def nextTask(executingQuery: ExecutingQuery,
                            queryResources: QueryResources): PipelineTask = null
    }
    val sleeper = mock[Sleeper]

    val worker = new Worker(1, queryManager, schedulingPolicy, null, sleeper)
    worker.run()

    verify(sleeper, never()).reset()
    verify(sleeper, times(ATTEMPTS)).reportIdle()
  }

  test("should not reset sleeper when query has with no work") {
    val ATTEMPTS = 100
    val queryManager: QueryManager =
      new QueryManager {
        override def nextQueryToWorkOn(workerId: Int): ExecutingQuery = mock[ExecutingQuery]
      }

    val schedulingPolicy: SchedulingPolicy =
      new SchedulingPolicy {
        private val countDown = new CountDown[PipelineTask](ATTEMPTS, null)
        override def nextTask(executingQuery: ExecutingQuery,
                              queryResources: QueryResources): PipelineTask = countDown.next()
      }

    val sleeper = mock[Sleeper]

    val worker = new Worker(1, queryManager, schedulingPolicy, null, sleeper)
    worker.run()

    verify(sleeper, never()).reset()
    verify(sleeper, times(ATTEMPTS)).reportIdle()
  }

  class NoMoreAttemptsException() extends IllegalArgumentException

  class CountDown[T](var count: Int, val t: T) {
    def next(): T = {
      count -= 1
      if (count == 0)
        Thread.currentThread().interrupt()
      t
    }
  }
}
