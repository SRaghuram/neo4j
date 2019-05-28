/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.internal.kernel.api._
import org.neo4j.io.IOUtils

class CursorPools(cursorFactory: CursorFactory) extends AutoCloseable {

  val nodeCursorPool = new CursorPool[NodeCursor](
    () => cursorFactory.allocateNodeCursor())
  val relationshipGroupCursorPool = new CursorPool[RelationshipGroupCursor](
    () => cursorFactory.allocateRelationshipGroupCursor())
  val relationshipTraversalCursorPool = new CursorPool[RelationshipTraversalCursor](
    () => cursorFactory.allocateRelationshipTraversalCursor())
  val nodeValueIndexCursorPool = new CursorPool[NodeValueIndexCursor](
    () => cursorFactory.allocateNodeValueIndexCursor())
  val nodeLabelIndexCursorPool = new CursorPool[NodeLabelIndexCursor](
    () => cursorFactory.allocateNodeLabelIndexCursor())

  override def close(): Unit = {
    IOUtils.closeAll(nodeCursorPool,
                     relationshipGroupCursorPool,
                     relationshipTraversalCursorPool,
                     nodeValueIndexCursorPool,
                     nodeLabelIndexCursorPool)
  }

  def collectLiveCounts(liveCounts: LiveCounts): Unit = {
    liveCounts.nodeCursorPool += nodeCursorPool.getLiveCount
    liveCounts.relationshipGroupCursorPool += relationshipGroupCursorPool.getLiveCount
    liveCounts.relationshipTraversalCursorPool += relationshipTraversalCursorPool.getLiveCount
    liveCounts.nodeValueIndexCursorPool += nodeValueIndexCursorPool.getLiveCount
    liveCounts.nodeLabelIndexCursorPool += nodeLabelIndexCursorPool.getLiveCount
  }
}

class LiveCounts(var nodeCursorPool: Long = 0,
                 var relationshipGroupCursorPool: Long = 0,
                 var relationshipTraversalCursorPool: Long = 0,
                 var nodeValueIndexCursorPool: Long = 0,
                 var nodeLabelIndexCursorPool: Long = 0) {

  def assertAllReleased(): Unit = {
    def reportLeak(liveCount: Long, poolName: String): Option[String] = {
      if (liveCount != 0) {
        Some(s"${poolName}s had a total live count of $liveCount")
      } else None
    }

    val resourceLeaks = Seq(
      reportLeak(nodeCursorPool, "nodeCursorPool"),
      reportLeak(relationshipGroupCursorPool, "relationshipGroupCursorPool"),
      reportLeak(relationshipTraversalCursorPool, "relationshipTraversalCursorPool"),
      reportLeak(nodeValueIndexCursorPool, "nodeValueIndexCursorPool"),
      reportLeak(nodeLabelIndexCursorPool, "nodeLabelIndexCursorPool")).flatten

    if (resourceLeaks.nonEmpty) {
      throw new RuntimeResourceLeakException(resourceLeaks.mkString(
        "Several cursors are live even though all cursors should have been released\n  ", "\n  ", "\n"))
    }
  }
}

class CursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR) extends AutoCloseable {

  @volatile private var liveCount: Long = 0L
  private var cached: CURSOR = _

  /**
    * Allocate a cursor of type `CURSOR`.
    */
  def allocate(): CURSOR = {
    liveCount += 1
    DebugSupport.logCursors(topStack().mkString("+ allocate\n        ", "\n        ", ""))
    if (cached != null) {
      val temp = cached
      cached = null.asInstanceOf[CURSOR]
      temp
    } else {
      cursorFactory()
    }
  }

  /**
    * Free the given cursor. NOOP if `null`.
    */
  def free(cursor: CURSOR): Unit = {
    DebugSupport.logCursors(topStack().mkString("+ free\n        ", "\n        ", ""))
    if (cursor != null) {
      liveCount -= 1
      if (cached != null)
        cached.close()
      cached = cursor
    }
  }

  override def close(): Unit = {
    if (cached != null)
      cached.close()
  }

  def getLiveCount: Long = liveCount

  private def topStack(): Seq[String] = {
    new Exception().getStackTrace.slice(4, 5).map(traceElement => "\tat "+traceElement)
  }
}
