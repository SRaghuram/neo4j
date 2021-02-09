/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.io.IOUtils
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.kernel.impl.newapi.Cursors.EmptyTraversalCursor
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

class CursorPools(cursorFactory: CursorFactory, pageCursorTracer: PageCursorTracer, memoryTracker: MemoryTracker) extends CursorFactory with AutoCloseable {

  private[this] val scopedMemoryTracker = memoryTracker.getScopedMemoryTracker

  // Allocate all known cursor pools up-front to avoid having to null-check on each allocate call.
  // The sizes are fairly small, so this is not expected to be a problem.
  // But if this is demonstrated to affect query setup time too much, we could switch to lazy initialization,
  // or even better, let the compiler tell us which pools will be needed and how big they need to be.
  private[this] val _nodeCursorPool: CursorPool[NodeCursor] = CursorPool[NodeCursor](
    () => cursorFactory.allocateNodeCursor(pageCursorTracer), scopedMemoryTracker)
  private[this] val _relationshipTraversalCursorPool: CursorPool[RelationshipTraversalCursor] = CursorPool.relationshipTraversalPool(
    () => cursorFactory.allocateRelationshipTraversalCursor(pageCursorTracer), scopedMemoryTracker)
  private[this] val _relationshipScanCursorPool: CursorPool[RelationshipScanCursor] = CursorPool[RelationshipScanCursor](
    () => cursorFactory.allocateRelationshipScanCursor(pageCursorTracer), scopedMemoryTracker)
  private[this] val _relationshipTypeIndexCursorPool: CursorPool[RelationshipTypeIndexCursor] = CursorPool[RelationshipTypeIndexCursor](
    () => cursorFactory.allocateRelationshipTypeIndexCursor(pageCursorTracer), scopedMemoryTracker)
  private[this] val _nodeValueIndexCursorPool: CursorPool[NodeValueIndexCursor] = CursorPool[NodeValueIndexCursor](
    () => cursorFactory.allocateNodeValueIndexCursor(pageCursorTracer, memoryTracker), scopedMemoryTracker)
  private[this] val _relationshipValueIndexCursorPool: CursorPool[RelationshipValueIndexCursor] = CursorPool[RelationshipValueIndexCursor](
    () => cursorFactory.allocateRelationshipValueIndexCursor(pageCursorTracer, memoryTracker), scopedMemoryTracker)
  private[this] val _nodeLabelIndexCursorPool: CursorPool[NodeLabelIndexCursor] = CursorPool[NodeLabelIndexCursor](
    () => cursorFactory.allocateNodeLabelIndexCursor(pageCursorTracer), scopedMemoryTracker)
  private[this] val _propertyCursorPool: CursorPool[PropertyCursor] = CursorPool[PropertyCursor](
    () => cursorFactory.allocatePropertyCursor(pageCursorTracer, memoryTracker), scopedMemoryTracker)

  //---------------------
  // Accessors
  def nodeCursorPool: CursorPool[NodeCursor] = _nodeCursorPool
  def relationshipTraversalCursorPool: CursorPool[RelationshipTraversalCursor] = _relationshipTraversalCursorPool
  def relationshipScanCursorPool: CursorPool[RelationshipScanCursor] = _relationshipScanCursorPool
  def relationshipTypeIndexCursorPool: CursorPool[RelationshipTypeIndexCursor] = _relationshipTypeIndexCursorPool
  def relationshipValueIndexCursorPool: CursorPool[RelationshipValueIndexCursor] = _relationshipValueIndexCursorPool
  def nodeValueIndexCursorPool: CursorPool[NodeValueIndexCursor] = _nodeValueIndexCursorPool
  def nodeLabelIndexCursorPool: CursorPool[NodeLabelIndexCursor] = _nodeLabelIndexCursorPool
  def propertyCursorPool: CursorPool[PropertyCursor] = _propertyCursorPool
  //---------------------

  def setKernelTracer(tracer: KernelReadTracer): Unit = {
    _nodeCursorPool.setKernelTracer(tracer)
    _relationshipTraversalCursorPool.setKernelTracer(tracer)
    _relationshipScanCursorPool.setKernelTracer(tracer)
    _relationshipTypeIndexCursorPool.setKernelTracer(tracer)
    _relationshipValueIndexCursorPool.setKernelTracer(tracer)
    _nodeValueIndexCursorPool.setKernelTracer(tracer)
    _nodeLabelIndexCursorPool.setKernelTracer(tracer)
    _propertyCursorPool.setKernelTracer(tracer)
  }

  override def close(): Unit = {
    IOUtils.closeAll(_nodeCursorPool,
      _relationshipTraversalCursorPool,
      _relationshipScanCursorPool,
      _relationshipTypeIndexCursorPool,
      _relationshipValueIndexCursorPool,
      _nodeValueIndexCursorPool,
      _nodeLabelIndexCursorPool,
      _propertyCursorPool)
    scopedMemoryTracker.close()
  }

  def collectLiveCounts(liveCounts: LiveCounts): Unit = {
    liveCounts.nodeCursorPool += _nodeCursorPool.getLiveCount
    liveCounts.relationshipTraversalCursorPool += _relationshipTraversalCursorPool.getLiveCount
    liveCounts.relationshipScanCursorPool += _relationshipScanCursorPool.getLiveCount
    liveCounts.relationshipTypeIndexCursorPool += _relationshipTypeIndexCursorPool.getLiveCount
    liveCounts.relationshipValueIndexCursorPool += _relationshipValueIndexCursorPool.getLiveCount
    liveCounts.nodeValueIndexCursorPool += _nodeValueIndexCursorPool.getLiveCount
    liveCounts.nodeLabelIndexCursorPool += _nodeLabelIndexCursorPool.getLiveCount
    liveCounts.propertyCursorPool += _propertyCursorPool.getLiveCount
  }

  override def allocateNodeCursor(cursorTracer: PageCursorTracer): NodeCursor = _nodeCursorPool.allocate()

  override def allocateFullAccessNodeCursor(cursorTracer: PageCursorTracer): NodeCursor = fail("FullAccessNodeCursor")

  override def allocateRelationshipScanCursor(cursorTracer: PageCursorTracer): RelationshipScanCursor = _relationshipScanCursorPool.allocate()

  override def allocateFullAccessRelationshipScanCursor(cursorTracer: PageCursorTracer): RelationshipScanCursor = fail("FullAccessRelationshipScanCursor")

  override def allocateRelationshipTraversalCursor(cursorTracer: PageCursorTracer): RelationshipTraversalCursor = _relationshipTraversalCursorPool.allocate()

  override def allocateFullAccessRelationshipTraversalCursor(cursorTracer: PageCursorTracer): RelationshipTraversalCursor = fail("FullAccessRelationshipTraversalCursor")

  override def allocatePropertyCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): PropertyCursor = fail("PropertyCursor")

  override def allocateFullAccessPropertyCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): PropertyCursor = fail("FullAccessPropertyCursor")

  override def allocateNodeValueIndexCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): NodeValueIndexCursor = _nodeValueIndexCursorPool.allocate()

  override def allocateFullAccessNodeValueIndexCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): NodeValueIndexCursor = fail("FullAccessNodeValueIndexCursor")

  override def allocateNodeLabelIndexCursor(cursorTracer: PageCursorTracer): NodeLabelIndexCursor = _nodeLabelIndexCursorPool.allocate()

  override def allocateFullAccessNodeLabelIndexCursor(cursorTracer: PageCursorTracer): NodeLabelIndexCursor = fail("FullAccessNodeLabelIndexCursor")

  override def allocateRelationshipValueIndexCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): RelationshipValueIndexCursor =  _relationshipValueIndexCursorPool.allocate()

  override def allocateRelationshipTypeIndexCursor(cursorTracer: PageCursorTracer): RelationshipTypeIndexCursor = _relationshipTypeIndexCursorPool.allocate()

  override def allocateFullAccessRelationshipTypeIndexCursor(): RelationshipTypeIndexCursor = fail("FullAccessRelationshipTypeIndexCursor")

  private def fail(cursor: String) = throw new IllegalStateException(s"This cursor pool doesn't support allocating $cursor")
}

class LiveCounts(var nodeCursorPool: Long = 0,
                 var relationshipGroupCursorPool: Long = 0,
                 var relationshipTraversalCursorPool: Long = 0,
                 var relationshipScanCursorPool: Long = 0,
                 var relationshipTypeIndexCursorPool: Long = 0,
                 var relationshipValueIndexCursorPool: Long = 0,
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
      reportLeak(relationshipTypeIndexCursorPool, "relationshipTypeIndexCursorPool"),
      reportLeak(relationshipValueIndexCursorPool, "relationshipValueIndexCursorPool"),
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

trait CursorPool[CURSOR <: Cursor] extends AutoCloseable {

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

class BoundedArrayCursorPool[CURSOR <: Cursor](private[this] val cursorFactory: () => CURSOR, private[this] val size: Int) extends CursorPool[CURSOR] {
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
      if (index < size && cached != null) { // NOTE: The pool could already have been closed, e.g. if this is called from a CleanUpTask
        cached(index) = cursor
        index += 1
      } else {
        // The pool is full (or closed) and we have to discard one cursor.
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
    if (index > 0) {
      IOUtils.closeFirst(cached, index)
      index = 0
    }
    cached = null.asInstanceOf[Array[Cursor]]
  }

  def getLiveCount: Long = throw new UnsupportedOperationException("use TrackingCursorPool")
}

class RelationshipTraversalCursorPool(
  cursorFactory: () => RelationshipTraversalCursor,
  size: Int
) extends BoundedArrayCursorPool(cursorFactory, size) {
  override def free(cursor: RelationshipTraversalCursor): Unit = {
    if (!cursor.isInstanceOf[EmptyTraversalCursor]) {
      super.free(cursor)
    } else {
      // EmptyTraversalCursors are not re-usable and cause bugs if they end up in the cursor pool
      DebugSupport.CURSORS.log("Warning, tried to free EmptyTraversalCursor")
    }
  }
}

class TrackingRelationshipTraversalCursorPool(
  cursorFactory: () => RelationshipTraversalCursor,
  size: Int
) extends TrackingBoundedArrayCursorPool(cursorFactory, size) {
  override def free(cursor: RelationshipTraversalCursor): Unit = {
    if (!cursor.isInstanceOf[EmptyTraversalCursor]) {
      super.free(cursor)
    } else {
      // EmptyTraversalCursors are not re-usable and cause bugs if they end up in the cursor pool
      DebugSupport.CURSORS.log("Warning, tried to free EmptyTraversalCursor")
    }
  }
}

object CursorPool {

  final val DEFAULT_POOL_SIZE = 128
  private final val TRACKING_POOL_SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[TrackingBoundedArrayCursorPool[_]])
  private final val POOL_SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[BoundedArrayCursorPool[_]])
  private final val RT_TRACKING_POOL_SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[TrackingRelationshipTraversalCursorPool])
  private final val RT_POOL_SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[RelationshipTraversalCursorPool])

  def apply[CURSOR <: Cursor](cursorFactory: () => CURSOR, memoryTracker: MemoryTracker): CursorPool[CURSOR] =
    if (AssertionRunner.ASSERTIONS_ENABLED) {
      memoryTracker.allocateHeap(TRACKING_POOL_SHALLOW_SIZE + poolArrayHeap(DEFAULT_POOL_SIZE))
      new TrackingBoundedArrayCursorPool[CURSOR](cursorFactory, DEFAULT_POOL_SIZE)
    } else {
      memoryTracker.allocateHeap(POOL_SHALLOW_SIZE + poolArrayHeap(DEFAULT_POOL_SIZE))
      new BoundedArrayCursorPool(cursorFactory, DEFAULT_POOL_SIZE)
    }

  def relationshipTraversalPool(cursorFactory: () => RelationshipTraversalCursor, memoryTracker: MemoryTracker): CursorPool[RelationshipTraversalCursor] = {
    if (AssertionRunner.ASSERTIONS_ENABLED) {
      memoryTracker.allocateHeap(RT_TRACKING_POOL_SHALLOW_SIZE + poolArrayHeap(DEFAULT_POOL_SIZE))
      new TrackingRelationshipTraversalCursorPool(cursorFactory, DEFAULT_POOL_SIZE)
    } else {
      memoryTracker.allocateHeap(RT_POOL_SHALLOW_SIZE + poolArrayHeap(DEFAULT_POOL_SIZE))
      new RelationshipTraversalCursorPool(cursorFactory, DEFAULT_POOL_SIZE)
    }
  }

  private def poolArrayHeap(size: Int): Long = HeapEstimator.shallowSizeOfObjectArray(size)
}
