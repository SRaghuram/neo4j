/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.spi.codegen.ir

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.planner.spi.KernelStatisticProvider
import org.neo4j.cypher.internal.runtime.compiled.codegen.Variable
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.{CodeGenType, NodeProjection}
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.{AcceptVisitor, ScanAllNodes, WhileLoop}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.logical.plans.{AllNodesScan, NodeHashJoin}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.internal.kernel.api.helpers.{StubNodeCursor, StubRead}
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.core.{EmbeddedProxySPI, NodeProxy}
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class CompiledProfilingTest extends CypherFunSuite with CodeGenSugar {

  test("should count db hits and rows") {
    // given
    val id1 = new Id(0)
    val id2 = new Id(1)

    val variable = Variable("name", CodeGenType.primitiveNode)
    val projectNode = NodeProjection(variable)
    val compiled = compile(Seq(WhileLoop(variable,
      ScanAllNodes("OP1"), AcceptVisitor("OP2", Map("n" -> projectNode)))),
      Seq("n"), Map("OP1" -> id1, "OP2" -> id2, "X" -> Id.INVALID_ID))

    val cursors = mock[CursorFactory]
    val dataRead = new StubRead
    val nodeCursor = new StubNodeCursor
    nodeCursor.withNode(1)
    nodeCursor.withNode(2)
    when(cursors.allocateNodeCursor()).thenReturn(nodeCursor)
    val entityAccessor = mock[EmbeddedProxySPI]
    val queryContext = mock[QueryContext]
    val transactionalContext = mock[TransactionalContextWrapper]
    when(queryContext.transactionalContext).thenReturn(transactionalContext.asInstanceOf[QueryTransactionalContext])
    when(transactionalContext.kernelStatisticProvider).thenReturn(new DelegatingKernelStatisticProvider(new DefaultPageCursorTracer))
    when(transactionalContext.cursors).thenReturn(cursors)
    when(transactionalContext.dataRead).thenReturn(dataRead)
    when(entityAccessor.newNodeProxy(anyLong())).thenReturn(mock[NodeProxy])
    when(queryContext.entityAccessor).thenReturn(entityAccessor)

    // when
    val tracer = new ProfilingTracer(transactionalContext.kernelStatisticProvider)
    newInstance(compiled, queryContext = queryContext, tracer = Some(tracer)).size

    // then
    tracer.dbHitsOf(id1) should equal(3)
    tracer.rowsOf(id2) should equal(2)
  }

  test("should profile hash join") {
    //given
    val database = new TestGraphDatabaseFactory().newImpermanentDatabase()
    try {
      val graphDb = new GraphDatabaseCypherService(database)
      val tx = graphDb.beginTransaction(Type.explicit, AnonymousContext.write())
      database.createNode()
      database.createNode()
      tx.success()
      tx.close()

      val lhs = AllNodesScan("a", Set.empty)
      val rhs = AllNodesScan("a", Set.empty)
      val join = NodeHashJoin(Set("a"), lhs, rhs)
      val projection = plans.Projection(join, Map("foo" -> literalInt(1)))
      val plan = plans.ProduceResult(projection, List("foo"))

      // when
      val result = compileAndProfile(plan, graphDb)
      val queryProfile = result.queryProfile()

      // then
      val hashJoin = queryProfile.operatorProfile(join.id.x)
      hashJoin.dbHits() should equal(0)
      hashJoin.rows() should equal(2)
    } finally {
      database.shutdown()
    }
  }

  class DelegatingKernelStatisticProvider(tracer: DefaultPageCursorTracer) extends KernelStatisticProvider {

    override def getPageCacheHits: Long = tracer.hits()

    override def getPageCacheMisses: Long = tracer.faults()
  }
}
