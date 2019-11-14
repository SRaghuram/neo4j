/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.pipelined.{Worker, WorkerManager, WorkerResourceProvider}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.{Cursor, CursorFactory}

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
    assertFor("relationshipGroup", _.relationshipGroupCursorPool)
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
    assertFor("relationshipGroup", _.relationshipGroupCursorPool)
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
    assertFor("relationshipGroup", _.relationshipGroupCursorPool)
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

  class RandomExecutor extends FixedWorkersQueryExecutor(
    new WorkerResourceProvider(3, () => new QueryResources(mock[CursorFactory](RETURNS_DEEP_STUBS))),
    new WorkerManager(3, null)) {

    private val random = new Random()

    def randomCursorPools(): CursorPools =
      workerResourceProvider.resourcesForWorker(random.nextInt(workerManager.numberOfWorkers)).cursorPools

    def randomWorker(): Worker =
      workerManager.workers(random.nextInt(workerManager.numberOfWorkers))
  }
}
