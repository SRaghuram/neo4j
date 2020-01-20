/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.spi.codegen.ir

import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito.when
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.profiling.KernelStatisticProvider
import org.neo4j.cypher.internal.profiling.ProfilingTracer
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.Variable
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.AcceptVisitor
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.ScanAllNodes
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.WhileLoop
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.NodeProjection
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor
import org.neo4j.internal.kernel.api.helpers.StubRead
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.core.NodeEntity
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.test.TestDatabaseManagementServiceBuilder

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
    when(cursors.allocateNodeCursor(any())).thenReturn(nodeCursor)
    val kernelTransaction = mock[KernelTransaction]
    val transaction = mock[InternalTransaction]
    val queryContext = mock[QueryContext]
    val transactionalContext = mock[TransactionalContextWrapper]
    transactionalContext.transaction
    when(queryContext.entityAccessor).thenReturn(transaction)
    when(queryContext.transactionalContext).thenReturn(transactionalContext.asInstanceOf[QueryTransactionalContext])
    when(transactionalContext.kernelStatisticProvider).thenReturn(new DelegatingKernelStatisticProvider(PageCursorTracer.NULL))
    when(transactionalContext.cursors).thenReturn(cursors)
    when(transactionalContext.dataRead).thenReturn(dataRead)
    when(transactionalContext.transaction).thenReturn(kernelTransaction)
    when(kernelTransaction.pageCursorTracer()).thenReturn(PageCursorTracer.NULL)
    when(transaction.newNodeEntity(anyLong())).thenReturn(mock[NodeEntity])

    // when
    val tracer = new ProfilingTracer(transactionalContext.kernelStatisticProvider)
    newInstance(compiled, queryContext = queryContext, tracer = Some(tracer)).size

    // then
    tracer.dbHitsOf(id1) should equal(3)
    tracer.rowsOf(id2) should equal(2)
  }

  test("should profile hash join") {
    val managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    //given
    val database = managementService.database(DEFAULT_DATABASE_NAME)
    try {
      val graphDb = new GraphDatabaseCypherService(database)
      val tx = graphDb.beginTransaction(Type.EXPLICIT, AnonymousContext.write())
      tx.createNode()
      tx.createNode()
      tx.commit()

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
      managementService.shutdown()
    }
  }

  class DelegatingKernelStatisticProvider(tracer: PageCursorTracer) extends KernelStatisticProvider {

    override def getPageCacheHits: Long = tracer.hits()

    override def getPageCacheMisses: Long = tracer.faults()
  }
}
