/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.internal.kernel.api._
import org.neo4j.io.IOUtils
import org.neo4j.util.Preconditions

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
    def assertReleased(liveCount: Long, poolName: String): Unit = {
      if (liveCount != 0) {
        throw new RuntimeResourceLeakException(s"${poolName}s had a total live count of $liveCount, " +
                                                "even though all cursors should be released.")
      }
    }

    assertReleased(nodeCursorPool, "nodeCursorPool")
    assertReleased(relationshipGroupCursorPool, "relationshipGroupCursorPool")
    assertReleased(relationshipTraversalCursorPool, "relationshipTraversalCursorPool")
    assertReleased(nodeValueIndexCursorPool, "nodeValueIndexCursorPool")
    assertReleased(nodeLabelIndexCursorPool, "nodeLabelIndexCursorPool")
  }
}

class CursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR) extends AutoCloseable {

  @volatile private var liveCount: Long = 0L
  private var cached: CURSOR = _

  def allocate(): CURSOR = {
    liveCount += 1
    if (cached != null) {
      val temp = cached
      cached = null.asInstanceOf[CURSOR]
      temp
    } else {
      cursorFactory()
    }
  }

  def free(cursor: CURSOR): Unit = {
    liveCount -= 1
    if (cached != null)
      cached.close()
    cached = cursor
  }

  override def close(): Unit = {
    if (cached != null)
      cached.close()
  }

  def getLiveCount: Long = liveCount
}
