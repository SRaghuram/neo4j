/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipIndexCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker

class CursorPoolTest extends CypherFunSuite {
  private val poolSize: Int = CursorPool.DEFAULT_POOL_SIZE
  private val memoryTracker = new LocalMemoryTracker

  //   used simultaneously, expected create count
  Seq((poolSize/2,          poolSize/2),
      (poolSize,            poolSize),
      (poolSize+1,          poolSize+2),
      (poolSize*2,          poolSize*2 + poolSize)).foreach { case (n, expected) =>
    test(s"should not allocate more than $expected node cursors when using $n cursors simultaneously repeated twice") {
      // given
      val (pools, factory) = setupCursorPools

      // when
      val cursors1 = (1 to n).map(_ => pools.nodeCursorPool.allocate())
      cursors1.foreach(c => pools.nodeCursorPool.free(c))

      val cursors2 = (1 to n).map(_ => pools.nodeCursorPool.allocate())
      cursors2.foreach(c => pools.nodeCursorPool.free(c))

      // then
      factory.nodeCursorCount shouldEqual expected
      closeAndAssertAllReleased(pools)
    }

    test(s"should not allocate more than $expected relationship traversal cursors when using $n cursors simultaneously repeated twice") {
      // given
      val (pools, factory) = setupCursorPools

      // when
      val cursors1 = (1 to n).map(_ => pools.relationshipTraversalCursorPool.allocate())
      cursors1.foreach(c => pools.relationshipTraversalCursorPool.free(c))

      val cursors2 = (1 to n).map(_ => pools.relationshipTraversalCursorPool.allocate())
      cursors2.foreach(c => pools.relationshipTraversalCursorPool.free(c))

      // then
      factory.relationshipTraversalCursorCount shouldEqual expected
      closeAndAssertAllReleased(pools)
    }

    test(s"should not allocate more than $expected relationship scan cursors when using $n cursors simultaneously repeated twice") {
      // given
      val (pools, factory) = setupCursorPools

      // when
      val cursors1 = (1 to n).map(_ => pools.relationshipScanCursorPool.allocate())
      cursors1.foreach(c => pools.relationshipScanCursorPool.free(c))

      val cursors2 = (1 to n).map(_ => pools.relationshipScanCursorPool.allocate())
      cursors2.foreach(c => pools.relationshipScanCursorPool.free(c))

      // then
      factory.relationshipScanCursorCount shouldEqual expected
      closeAndAssertAllReleased(pools)
    }

    test(s"should not allocate more than $expected node value index cursors when using $n cursors simultaneously repeated twice") {
      // given
      val (pools, factory) = setupCursorPools

      // when
      val cursors1 = (1 to n).map(_ => pools.nodeValueIndexCursorPool.allocate())
      cursors1.foreach(c => pools.nodeValueIndexCursorPool.free(c))

      val cursors2 = (1 to n).map(_ => pools.nodeValueIndexCursorPool.allocate())
      cursors2.foreach(c => pools.nodeValueIndexCursorPool.free(c))

      // then
      factory.nodeValueIndexCursorCount shouldEqual expected
      closeAndAssertAllReleased(pools)
    }

    test(s"should not allocate more than $expected node label index cursors when using $n cursors simultaneously repeated twice") {
      // given
      val (pools, factory) = setupCursorPools

      // when
      val cursors1 = (1 to n).map(_ => pools.nodeLabelIndexCursorPool.allocate())
      cursors1.foreach(c => pools.nodeLabelIndexCursorPool.free(c))

      val cursors2 = (1 to n).map(_ => pools.nodeLabelIndexCursorPool.allocate())
      cursors2.foreach(c => pools.nodeLabelIndexCursorPool.free(c))

      // then
      factory.nodeLabelIndexCursorCount shouldEqual expected
      closeAndAssertAllReleased(pools)
    }

    test(s"should not allocate more than $expected property cursors when using $n cursors simultaneously repeated twice") {
      // given
      val (pools, factory) = setupCursorPools

      // when
      val cursors1 = (1 to n).map(_ => pools.propertyCursorPool.allocate())
      cursors1.foreach(c => pools.propertyCursorPool.free(c))

      val cursors2 = (1 to n).map(_ => pools.propertyCursorPool.allocate())
      cursors2.foreach(c => pools.propertyCursorPool.free(c))

      // then
      factory.propertyCursorCount shouldEqual expected
      closeAndAssertAllReleased(pools)
    }

  }

  private def setupCursorPools: (CursorPools, TestCursorFactory) = {
    val factory = new TestCursorFactory
    val pool = new CursorPools(factory, PageCursorTracer.NULL, memoryTracker)
    (pool, factory)
  }

  private def closeAndAssertAllReleased(cursorPools: CursorPools): Unit = {
    cursorPools.close()
    val liveCounts = new LiveCounts()
    cursorPools.collectLiveCounts(liveCounts)
    liveCounts.assertAllReleased()
    withClue("Leaking memory")(memoryTracker.estimatedHeapMemory shouldEqual 0)
  }

  class TestCursorFactory extends CursorFactory {
    var nodeCursorCount: Int = 0
    var relationshipTraversalCursorCount: Int = 0
    var relationshipScanCursorCount: Int = 0
    var nodeValueIndexCursorCount: Int = 0
    var nodeLabelIndexCursorCount: Int = 0
    var propertyCursorCount: Int = 0

    override def allocateNodeCursor(cursorTracer: PageCursorTracer): NodeCursor = {
      nodeCursorCount += 1
      mock[NodeCursor]
    }

    override def allocateRelationshipTraversalCursor(cursorTracer: PageCursorTracer): RelationshipTraversalCursor = {
      relationshipTraversalCursorCount += 1
      mock[RelationshipTraversalCursor]
    }

    override def allocateRelationshipScanCursor(cursorTracer: PageCursorTracer): RelationshipScanCursor = {
      relationshipScanCursorCount += 1
      mock[RelationshipScanCursor]
    }

    override def allocateNodeValueIndexCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): NodeValueIndexCursor = {
      nodeValueIndexCursorCount += 1
      mock[NodeValueIndexCursor]
    }

    override def allocateNodeLabelIndexCursor(cursorTracer: PageCursorTracer): NodeLabelIndexCursor = {
      nodeLabelIndexCursorCount += 1
      mock[NodeLabelIndexCursor]
    }

    override def allocatePropertyCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): PropertyCursor = {
      propertyCursorCount += 1
      mock[PropertyCursor]
    }

    //---- Unused ----
    override def allocateFullAccessNodeCursor(cursorTracer: PageCursorTracer): NodeCursor = ???

    override def allocateFullAccessRelationshipScanCursor(cursorTracer: PageCursorTracer): RelationshipScanCursor = ???

    override def allocateFullAccessRelationshipTraversalCursor(cursorTracer: PageCursorTracer): RelationshipTraversalCursor = ???

    override def allocateFullAccessPropertyCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): PropertyCursor = ???

    override def allocateFullAccessNodeValueIndexCursor(cursorTracer: PageCursorTracer, memoryTracker: MemoryTracker): NodeValueIndexCursor = ???

    override def allocateFullAccessNodeLabelIndexCursor(cursorTracer: PageCursorTracer): NodeLabelIndexCursor = ???

    override def allocateRelationshipIndexCursor(cursorTracer: PageCursorTracer): RelationshipIndexCursor = ???

    override def allocateRelationshipTypeIndexCursor(): RelationshipTypeIndexCursor = ???
  }
}

