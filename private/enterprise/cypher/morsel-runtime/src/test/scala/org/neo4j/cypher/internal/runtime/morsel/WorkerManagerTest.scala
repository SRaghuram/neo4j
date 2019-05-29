/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.mockito.Mockito.RETURNS_DEEP_STUBS
import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.morsel.execution._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.{Cursor, CursorFactory}

import scala.util.Random

class WorkerManagerTest extends CypherFunSuite {

  test("assertAllReleased should pass initially") {
    // given
    val workerManager = new RandomWorkerManager

    // then
    workerManager.assertAllReleased()
  }

  test("assertAllReleased should throw on leaked cursors") {
    def assertFor[T <: Cursor](cursorName: String, pool: CursorPools => CursorPool[T]):Unit = {
      // given
      val workerManager = new RandomWorkerManager()

      // when
      val p = pool(workerManager.randomWorkerCursorPools())
      p.allocate()

      // then
      withClue(cursorName+"Cursor: ") {
        intercept[RuntimeResourceLeakException] {
          workerManager.assertAllReleased()
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
      val workerManager = new RandomWorkerManager()

      // when
      val p = pool(workerManager.randomWorkerCursorPools())
      val cursor = p.allocate()
      p.free(cursor)

      // then
      withClue(cursorName+"Cursor: ") {
        workerManager.assertAllReleased()
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
      val workerManager = new RandomWorkerManager()

      // when
      val cursor = pool(workerManager.randomWorkerCursorPools()).allocate()
      pool(workerManager.randomWorkerCursorPools()).free(cursor)

      // then
      withClue(cursorName+"Cursor: ") {
        workerManager.assertAllReleased()
      }
    }

    assertFor("node", _.nodeCursorPool)
    assertFor("relationshipTraversal", _.relationshipTraversalCursorPool)
    assertFor("relationshipGroup", _.relationshipGroupCursorPool)
    assertFor("nodeLabelIndex", _.nodeLabelIndexCursorPool)
    assertFor("nodeValueIndex", _.nodeValueIndexCursorPool)
  }

  test("assertAllReleased should throw on active worker") {
    // given
    val workerManager = new RandomWorkerManager()

    // when
    workerManager.randomWorker().sleeper.reportWork()

    // then
    intercept[RuntimeResourceLeakException] {
      workerManager.assertAllReleased()
    }
  }

  class RandomWorkerManager extends WorkerManager(3, null, () => new QueryResources(mock[CursorFactory](RETURNS_DEEP_STUBS))) {

    private val random = new Random()

    def randomWorkerCursorPools(): CursorPools =
      randomWorker().resources.cursorPools

    def randomWorker(): Worker = workers(random.nextInt(numberOfWorkers))
  }
}
