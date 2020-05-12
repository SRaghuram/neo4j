/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManager
import org.neo4j.cypher.internal.runtime.pipelined.WorkerResourceProvider
import org.neo4j.cypher.internal.runtime.pipelined.Worker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.Cursor
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.memory.EmptyMemoryTracker

import scala.util.Random

class FixedWorkersQueryExecutorTest extends CypherFunSuite {

  test("assertAllReleased should pass initially") {
    // given
    val executor = new RandomExecutor

    // then
    executor.assertAllReleased()
  }

  test("assertAllReleased should throw on leaked cursors") {
    def assertFor[T <: Cursor](cursorName: String, pool: CursorPools => CursorPool[T]):Unit = {
      // given
      val executor = new RandomExecutor()

      // when
      val p = pool(executor.randomCursorPools())
      p.allocateAndTrace()

      // then
      withClue(cursorName+"Cursor: ") {
        intercept[RuntimeResourceLeakException] {
          executor.assertAllReleased()
        }
      }
    }

    assertFor("node", _.nodeCursorPool)
    assertFor("relationshipTraversal", _.relationshipTraversalCursorPool)
    assertFor("nodeLabelIndex", _.nodeLabelIndexCursorPool)
    assertFor("nodeValueIndex", _.nodeValueIndexCursorPool)
  }

  test("assertAllReleased should pass on freed cursors") {
    def assertFor[T <: Cursor](cursorName: String, pool: CursorPools => CursorPool[T]):Unit = {
      // given
      val executor = new RandomExecutor()

      // when
      val p = pool(executor.randomCursorPools())
      val cursor = p.allocateAndTrace()
      p.free(cursor)

      // then
      withClue(cursorName+"Cursor: ") {
        executor.assertAllReleased()
      }
    }

    assertFor("node", _.nodeCursorPool)
    assertFor("relationshipTraversal", _.relationshipTraversalCursorPool)
    assertFor("nodeLabelIndex", _.nodeLabelIndexCursorPool)
    assertFor("nodeValueIndex", _.nodeValueIndexCursorPool)
  }

  test("assertAllReleased should pass on cursors acquired and released on different workers") {
    def assertFor[T <: Cursor](cursorName: String, pool: CursorPools => CursorPool[T]):Unit = {
      // given
      val executor = new RandomExecutor()

      // when
      val cursor = pool(executor.randomCursorPools()).allocateAndTrace()
      pool(executor.randomCursorPools()).free(cursor)

      // then
      withClue(cursorName+"Cursor: ") {
        executor.assertAllReleased()
      }
    }

    assertFor("node", _.nodeCursorPool)
    assertFor("relationshipTraversal", _.relationshipTraversalCursorPool)
    assertFor("nodeLabelIndex", _.nodeLabelIndexCursorPool)
    assertFor("nodeValueIndex", _.nodeValueIndexCursorPool)
  }

  test("assertAllReleased should throw on working worker") {
    // given
    val executor = new RandomExecutor()

    // when
    executor.randomWorker().sleeper.reportStartWorkUnit()

    // then
    intercept[RuntimeResourceLeakException] {
      executor.assertAllReleased()
    }
  }

  test("assertAllReleased should not throw on active worker, because that will cause flaky tests") {
    // given
    val executor = new RandomExecutor()

    // when
    val worker = executor.randomWorker()
    worker.sleeper.reportStartWorkUnit()
    worker.sleeper.reportStopWorkUnit()

    // then
    executor.assertAllReleased()
  }

  val N_WORKERS = 3
  class RandomExecutor extends FixedWorkersQueryExecutor(
    new WorkerResourceProvider(
      N_WORKERS,
      workerId => new QueryResources(mock[CursorFactory](RETURNS_DEEP_STUBS), PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE, workerId, N_WORKERS)),
    new WorkerManager(N_WORKERS, null)) {

    private val random = new Random()

    def randomCursorPools(): CursorPools =
      workerResourceProvider.resourcesForWorker(random.nextInt(workerManager.numberOfWorkers)).cursorPools

    def randomWorker(): Worker =
      workerManager.workers(random.nextInt(workerManager.numberOfWorkers))
  }
}
