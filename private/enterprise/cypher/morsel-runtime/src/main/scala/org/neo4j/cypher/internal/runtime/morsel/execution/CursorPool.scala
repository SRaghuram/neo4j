/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.v4_0.util.AssertionRunner
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.{RelationshipDenseSelectionCursor, RelationshipSelectionCursor, RelationshipSparseSelectionCursor}
import org.neo4j.io.IOUtils

class CursorPools(cursorFactory: CursorFactory) extends CursorFactory with AutoCloseable {

  val nodeCursorPool: CursorPool[NodeCursor] = CursorPool[NodeCursor](
    () => cursorFactory.allocateNodeCursor())
  val relationshipGroupCursorPool: CursorPool[RelationshipGroupCursor] = CursorPool[RelationshipGroupCursor](
    () => cursorFactory.allocateRelationshipGroupCursor())
  val relationshipTraversalCursorPool: CursorPool[RelationshipTraversalCursor] = CursorPool[RelationshipTraversalCursor](
    () => cursorFactory.allocateRelationshipTraversalCursor())
  val relationshipScanCursorPool: CursorPool[RelationshipScanCursor] = CursorPool[RelationshipScanCursor](
    () => cursorFactory.allocateRelationshipScanCursor())
  val nodeValueIndexCursorPool: CursorPool[NodeValueIndexCursor] = CursorPool[NodeValueIndexCursor](
    () => cursorFactory.allocateNodeValueIndexCursor())
  val nodeLabelIndexCursorPool: CursorPool[NodeLabelIndexCursor] = CursorPool[NodeLabelIndexCursor](
    () => cursorFactory.allocateNodeLabelIndexCursor())

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    nodeCursorPool.setKernelTracer(tracer)
    relationshipGroupCursorPool.setKernelTracer(tracer)
    relationshipTraversalCursorPool.setKernelTracer(tracer)
    relationshipScanCursorPool.setKernelTracer(tracer)
    nodeValueIndexCursorPool.setKernelTracer(tracer)
    nodeLabelIndexCursorPool.setKernelTracer(tracer)
  }

  def free(selectionCursor: RelationshipSelectionCursor): Unit = selectionCursor match {
    case dense: RelationshipDenseSelectionCursor =>
      relationshipGroupCursorPool.forceFree(dense.groupCursor())
      relationshipTraversalCursorPool.forceFree(dense.traversalCursor())
    case sparse: RelationshipSparseSelectionCursor =>
      relationshipTraversalCursorPool.forceFree(sparse.traversalCursor())

    case _ => //either null or empty

  }

  override def close(): Unit = {
    IOUtils.closeAll(nodeCursorPool,
                     relationshipGroupCursorPool,
                     relationshipTraversalCursorPool,
                     relationshipScanCursorPool,
                     nodeValueIndexCursorPool,
                     nodeLabelIndexCursorPool)
  }

  def collectLiveCounts(liveCounts: LiveCounts): Unit = {
    liveCounts.nodeCursorPool += nodeCursorPool.getLiveCount
    liveCounts.relationshipGroupCursorPool += relationshipGroupCursorPool.getLiveCount
    liveCounts.relationshipTraversalCursorPool += relationshipTraversalCursorPool.getLiveCount
    liveCounts.relationshipScanCursorPool += relationshipScanCursorPool.getLiveCount
    liveCounts.nodeValueIndexCursorPool += nodeValueIndexCursorPool.getLiveCount
    liveCounts.nodeLabelIndexCursorPool += nodeLabelIndexCursorPool.getLiveCount
  }

  override def allocateNodeCursor(): NodeCursor = nodeCursorPool.allocate()

  override def allocateFullAccessNodeCursor(): NodeCursor = fail("FullAccessNodeCursor")

  override def allocateRelationshipScanCursor(): RelationshipScanCursor = relationshipScanCursorPool.allocate()

  override def allocateFullAccessRelationshipScanCursor(): RelationshipScanCursor = fail("FullAccessRelationshipScanCursor")

  override def allocateRelationshipTraversalCursor(): RelationshipTraversalCursor = relationshipTraversalCursorPool.allocate()

  override def allocatePropertyCursor(): PropertyCursor = fail("PropertyCursor")

  override def allocateFullAccessPropertyCursor(): PropertyCursor = fail("FullAccessPropertyCursor")

  override def allocateRelationshipGroupCursor(): RelationshipGroupCursor = relationshipGroupCursorPool.allocate()

  override def allocateNodeValueIndexCursor(): NodeValueIndexCursor = nodeValueIndexCursorPool.allocate()

  override def allocateNodeLabelIndexCursor(): NodeLabelIndexCursor = nodeLabelIndexCursorPool.allocate()

  override def allocateRelationshipIndexCursor(): RelationshipIndexCursor = fail("RelationshipIndexCursor")

  private def fail(cursor: String) = throw new IllegalStateException(s"This cursor pool doesn't support allocating $cursor")
}

class LiveCounts(var nodeCursorPool: Long = 0,
                 var relationshipGroupCursorPool: Long = 0,
                 var relationshipTraversalCursorPool: Long = 0,
                 var relationshipScanCursorPool: Long = 0,
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
      reportLeak(relationshipScanCursorPool, "relationshipScanCursorPool"),
      reportLeak(nodeValueIndexCursorPool, "nodeValueIndexCursorPool"),
      reportLeak(nodeLabelIndexCursorPool, "nodeLabelIndexCursorPool")).flatten

    if (resourceLeaks.nonEmpty) {
      throw new RuntimeResourceLeakException(resourceLeaks.mkString(
        "Cursors are live even though all cursors should have been released\n  ", "\n  ", "\n"))
    }
  }
}

class TrackingCursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR) extends CursorPool[CURSOR](cursorFactory) {
  @volatile private var liveCount: Long = 0L

  override def allocateAndTrace(): CURSOR = {
    liveCount += 1L
    super.allocateAndTrace()
  }

  override def allocate(): CURSOR = {
    liveCount += 1L
    super.allocate()
  }

  override def free(cursor: CURSOR): Unit = {
    if (cursor != null) {
      liveCount -= 1L
    }
    super.free(cursor)
  }

  override def forceFree(cursor: CURSOR): Unit = {
    liveCount -= 1L
    super.forceFree(cursor)
  }
  override def getLiveCount: Long = liveCount
}

class CursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR) extends AutoCloseable {
  private var cached: CURSOR = _
  private var tracer: KernelReadTracer = KernelReadTracer.NONE

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    this.tracer = tracer
  }

  /**
    * Allocate a cursor of type `CURSOR`.
    */
  def allocateAndTrace(): CURSOR = {
    val cursor = allocateCursor()
    cursor.setTracer(tracer)
    cursor
  }

  def allocate(): CURSOR = allocateCursor()

  private final def allocateCursor(): CURSOR = {
    if (DebugSupport.CURSORS.enabled) {
      DebugSupport.CURSORS.log(stackTraceSlice(2, 5).mkString("+ allocate\n        ", "\n        ", ""))
    }
    var cursor = cached
    if (cursor != null) {
      cached = null.asInstanceOf[CURSOR]
    } else {
      cursor = cursorFactory()
    }
    cursor
  }

  /**
    * Free the given cursor. NOOP if `null`.
    */
  def free(cursor: CURSOR): Unit = {
    freeCursor(cursor)
  }

  def forceFree(cursor: CURSOR): Unit = {
    freeCursor(cursor)
  }

  private def freeCursor(cursor: CURSOR): Unit = {
    if (cursor != null) {
      if (DebugSupport.CURSORS.enabled) {
        DebugSupport.CURSORS.log(stackTraceSlice(4, 5).mkString(s"+ free $cursor\n        ", "\n        ", ""))
      }
      cursor.setTracer(KernelReadTracer.NONE)
      //use local variable in order to avoid `cached()` multiple times
      val c = cached
      if (c != null)
        c.close()
      cached = cursor
    }
  }

  override def close(): Unit = {
    //use local variable in order to avoid `cached()` multiple times
    val c = cached
    if (c != null) {
      c.close()
      cached = null.asInstanceOf[CURSOR]
    }
  }

  def getLiveCount: Long = throw new UnsupportedOperationException("use TrackingCursorPool")

  /**
    * Collect a slice of the current stack trace.
    *
    * @param from first included stack trace frame, counting from the inner most nesting
    * @param to first excluded stack trace frame, counting from the inner most nesting
    */
  private def stackTraceSlice(from: Int, to: Int): Seq[String] = {
    new Exception().getStackTrace.slice(from, to).map(traceElement => "\tat "+traceElement)
  }
}

object CursorPool {

  def apply[CURSOR <: Cursor](cursorFactory: () => CURSOR): CursorPool[CURSOR] =
    if (AssertionRunner.isAssertionsEnabled) {
      new TrackingCursorPool(cursorFactory)
    } else {
      new CursorPool(cursorFactory)
    }
}