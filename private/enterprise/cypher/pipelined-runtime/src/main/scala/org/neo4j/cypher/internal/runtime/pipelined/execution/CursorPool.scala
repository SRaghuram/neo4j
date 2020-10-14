/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.RuntimeResourceLeakException
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.internal.helpers.Exceptions
import org.neo4j.internal.kernel.api.Cursor
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipIndexCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.io.IOUtils
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.memory.MemoryTracker

class CursorPools(cursorFactory: CursorFactory, pageCursorTracer: PageCursorTracer, memoryTracker: MemoryTracker) extends CursorFactory with AutoCloseable {

  val nodeCursorPool: CursorPool[NodeCursor] = CursorPool[NodeCursor](
    () => cursorFactory.allocateNodeCursor(pageCursorTracer))
  val relationshipTraversalCursorPool: CursorPool[RelationshipTraversalCursor] = CursorPool[RelationshipTraversalCursor](
    () => cursorFactory.allocateRelationshipTraversalCursor(pageCursorTracer))
  val relationshipScanCursorPool: CursorPool[RelationshipScanCursor] = CursorPool[RelationshipScanCursor](
    () => cursorFactory.allocateRelationshipScanCursor(pageCursorTracer))
  val nodeValueIndexCursorPool: CursorPool[NodeValueIndexCursor] = CursorPool[NodeValueIndexCursor](
    () => cursorFactory.allocateNodeValueIndexCursor(pageCursorTracer, memoryTracker))
  val nodeLabelIndexCursorPool: CursorPool[NodeLabelIndexCursor] = CursorPool[NodeLabelIndexCursor](
    () => cursorFactory.allocateNodeLabelIndexCursor(pageCursorTracer))
  val propertyCursorPool: CursorPool[PropertyCursor] = CursorPool[PropertyCursor](
    () => cursorFactory.allocatePropertyCursor(pageCursorTracer, memoryTracker))

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    nodeCursorPool.setKernelTracer(tracer)
    relationshipTraversalCursorPool.setKernelTracer(tracer)
    relationshipScanCursorPool.setKernelTracer(tracer)
    nodeValueIndexCursorPool.setKernelTracer(tracer)
    nodeLabelIndexCursorPool.setKernelTracer(tracer)
    propertyCursorPool.setKernelTracer(tracer)
  }

  override def close(): Unit = {
    IOUtils.closeAll(nodeCursorPool,
      relationshipTraversalCursorPool,
      relationshipScanCursorPool,
      nodeValueIndexCursorPool,
      nodeLabelIndexCursorPool,
      propertyCursorPool)
  }

  def collectLiveCounts(liveCounts: LiveCounts): Unit = {
    liveCounts.nodeCursorPool += nodeCursorPool.getLiveCount
    liveCounts.relationshipTraversalCursorPool += relationshipTraversalCursorPool.getLiveCount
    liveCounts.relationshipScanCursorPool += relationshipScanCursorPool.getLiveCount
    liveCounts.nodeValueIndexCursorPool += nodeValueIndexCursorPool.getLiveCount
    liveCounts.nodeLabelIndexCursorPool += nodeLabelIndexCursorPool.getLiveCount
    liveCounts.propertyCursorPool += propertyCursorPool.getLiveCount
  }

  override def allocateNodeCursor(cursorTracer: PageCursorTracer): NodeCursor = nodeCursorPool.allocate()

  override def allocateFullAccessNodeCursor(cursorTracer: PageCursorTracer): NodeCursor = fail("FullAccessNodeCursor")

  override def allocateRelationshipScanCursor(cursorTracer: PageCursorTracer): RelationshipScanCursor = relationshipScanCursorPool.allocate()

  override def allocateFullAccessRelationshipScanCursor(cursorTracer: PageCursorTracer): RelationshipScanCursor = fail("FullAccessRelationshipScanCursor")

  override def allocateRelationshipTraversalCursor(cursorTracer: PageCursorTracer): RelationshipTraversalCursor = relationshipTraversalCursorPool.allocate()

  override def allocateFullAccessRelationshipTraversalCursor(cursorTracer: PageCursorTracer): RelationshipTraversalCursor = fail("FullAccessRelationshipTraversalCursor")

  override def allocatePropertyCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): PropertyCursor = fail("PropertyCursor")

  override def allocateFullAccessPropertyCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): PropertyCursor = fail("FullAccessPropertyCursor")

  override def allocateNodeValueIndexCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): NodeValueIndexCursor = nodeValueIndexCursorPool.allocate()

  override def allocateFullAccessNodeValueIndexCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): NodeValueIndexCursor = fail("FullAccessNodeValueIndexCursor")

  override def allocateNodeLabelIndexCursor(cursorTracer: PageCursorTracer): NodeLabelIndexCursor = nodeLabelIndexCursorPool.allocate()

  override def allocateFullAccessNodeLabelIndexCursor(cursorTracer: PageCursorTracer): NodeLabelIndexCursor = fail("FullAccessNodeLabelIndexCursor")

  override def allocateRelationshipIndexCursor(cursorTracer: PageCursorTracer): RelationshipIndexCursor = fail("RelationshipIndexCursor")

  override def allocateRelationshipTypeIndexCursor(): RelationshipTypeIndexCursor = fail("RelationshipTypeIndexCursor")

  private def fail(cursor: String) = throw new IllegalStateException(s"This cursor pool doesn't support allocating $cursor")
}

class LiveCounts(var nodeCursorPool: Long = 0,
                 var relationshipGroupCursorPool: Long = 0,
                 var relationshipTraversalCursorPool: Long = 0,
                 var relationshipScanCursorPool: Long = 0,
                 var nodeValueIndexCursorPool: Long = 0,
                 var nodeLabelIndexCursorPool: Long = 0,
                 var propertyCursorPool: Long = 0) {

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
      reportLeak(propertyCursorPool, "propertyCursorPool"),
      reportLeak(nodeLabelIndexCursorPool, "nodeLabelIndexCursorPool")).flatten

    if (resourceLeaks.nonEmpty) {
      throw new RuntimeResourceLeakException(resourceLeaks.mkString(
        "Cursors are live even though all cursors should have been released\n  ", "\n  ", "\n"))
    }
  }
}

class TrackingBoundedArrayCursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR, size: Int) extends BoundedArrayCursorPool[CURSOR](cursorFactory, size) {
  @volatile private var liveCount: Long = 0L

  override def allocateAndTrace(): CURSOR = {
    liveCount += 1L
    if (DebugSupport.CURSORS.enabled) {
      DebugSupport.CURSORS.log(stackTraceSlice(2, 5).mkString("+ allocate\n        ", "\n        ", ""))
    }
    super.allocateAndTrace()
  }

  override def allocate(): CURSOR = {
    liveCount += 1L
    if (DebugSupport.CURSORS.enabled) {
      DebugSupport.CURSORS.log(stackTraceSlice(2, 5).mkString("+ allocate\n        ", "\n        ", ""))
    }
    super.allocate()
  }

  override def free(cursor: CURSOR): Unit = {
    if (cursor != null) {
      liveCount -= 1L
      if (DebugSupport.CURSORS.enabled) {
        DebugSupport.CURSORS.log(stackTraceSlice(4, 5).mkString( s"""+ free $cursor
        """, "\n        ", ""))
      }
    }
    super.free(cursor)
  }

  override def getLiveCount: Long = liveCount

  /**
   * Collect a slice of the current stack trace.
   *
   * @param from first included stack trace frame, counting from the inner most nesting
   * @param to first excluded stack trace frame, counting from the inner most nesting
   */
  private def stackTraceSlice(from: Int, to: Int): Seq[String] = {
    Exceptions.getPartialStackTrace(from, to).map(traceElement => "\tat " + traceElement)
  }
}

abstract class CursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR) extends AutoCloseable {

  def setKernelTracer(tracer: KernelReadTracer): Unit

  /**
   * Allocate and trace a cursor of type `CURSOR`.
   */
  def allocateAndTrace(): CURSOR

  /**
   * Allocate a cursor of type `CURSOR`.
   */
  def allocate(): CURSOR

  /**
   * Free the given cursor. NOOP if `null`.
   */
  def free(cursor: CURSOR): Unit

  def getLiveCount: Long
}

class BoundedArrayCursorPool[CURSOR <: Cursor](cursorFactory: () => CURSOR, private[this] val size: Int) extends CursorPool[CURSOR](cursorFactory) {
  private[this] var cached: Array[Cursor] = new Array[Cursor](size)
  private[this] var index: Int = 0
  private[this] var tracer: KernelReadTracer = _

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    this.tracer = tracer
  }

  /**
   * Allocate and trace a cursor of type `CURSOR`.
   */
  def allocateAndTrace(): CURSOR = {
    val cursor = allocateCursor()
    cursor.setTracer(tracer)
    cursor
  }

  /**
   * Allocate a cursor of type `CURSOR`.
   */
  def allocate(): CURSOR = allocateCursor()

  /**
   * Free the given cursor. NOOP if `null`.
   */
  def free(cursor: CURSOR): Unit = {
    if (cursor != null) {
      cursor.removeTracer()
      if (index < size) {
        cached(index) = cursor
        index += 1
      } else {
        // The pool is full and we have to discard one cursor.
        // We are expecting pools to be sized big enough so that this would be very rare.
        // Discarding the oldest cursor would probably be best, but to keep it simple, and avoid having to do
        // extra ring-buffer arithmetics in the common case, we just throw away the cursor being freed here instead.
        cursor.close()
      }
    }
  }

  private final def allocateCursor(): CURSOR = {
    if (index > 0) {
      index -= 1
      val cursor = cached(index)
      cached(index) = null.asInstanceOf[CURSOR]
      cursor.asInstanceOf[CURSOR]
    } else {
      cursorFactory()
    }
  }

  override def close(): Unit = {
    if (index > 0 ) {
      IOUtils.closeAll(cached: _*) // IOUtils.closeAll can handle nulls. We avoid creating another copy since this could be called when we are running low on memory.
      index = 0
    }
    cached = null.asInstanceOf[Array[Cursor]]
  }

  def getLiveCount: Long = throw new UnsupportedOperationException("use TrackingCursorPool")
}

object CursorPool {

  def apply[CURSOR <: Cursor](cursorFactory: () => CURSOR): CursorPool[CURSOR] =
    if (AssertionRunner.ASSERTIONS_ENABLED) {
      new TrackingBoundedArrayCursorPool[CURSOR](cursorFactory)
    } else {
      new BoundedArrayCursorPool(cursorFactory)
    }
}
