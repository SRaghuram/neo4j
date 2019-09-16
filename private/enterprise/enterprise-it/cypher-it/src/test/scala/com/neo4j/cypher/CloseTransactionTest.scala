/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import java.util

import org.neo4j.collection.RawIterator
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.GraphIcing
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs._
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.{CallableProcedure, Context, GlobalProcedures}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.{QueryExecution, QuerySubscriber}
import org.neo4j.procedure.Mode
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

class CloseTransactionTest extends CypherFunSuite with GraphIcing {

  private val runtimes = Seq("interpreted", "compiled")

  private var managementService: DatabaseManagementService = _
  private var db : GraphDatabaseService = _

  override protected def initTest(): Unit = {
    super.initTest()
    managementService = new TestDatabaseManagementServiceBuilder().impermanent().build()
    db = managementService.database(DEFAULT_DATABASE_NAME)
  }

  override protected def stopTest(): Unit = {
    managementService.shutdown()
    super.stopTest()
  }

  for (runtime <- runtimes) {

    test(s"should not leak transaction when closing the result for a query - runtime=$runtime") {
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.executeTransactionally(s"CYPHER runtime=$runtime return 1")
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      val execution = engine.execute(s"CYPHER runtime=$runtime return 1")
      execution.cancel()

      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a profile query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.executeTransactionally(s"CYPHER runtime=$runtime profile return 1")
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1").cancel()
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute("PROFILE return 1").cancel()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for an explain query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      db.executeTransactionally(s"CYPHER runtime=$runtime explain return 1")
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1").cancel()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when failing in pre-parsing - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      intercept[SyntaxException](engine.execute(s"CYPHER runtime=$runtime "))
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      // when
      db.executeTransactionally(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()")
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()").cancel()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for a profile procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      // when
      db.executeTransactionally(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()")
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()").cancel()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when closing the result for an explain procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      // when
      db.executeTransactionally(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()")
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").cancel()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a regular query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime return 1").asScala.length
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a profile query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime profile return 1").asScala.length
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for an explain query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      import scala.collection.JavaConverters._
      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime explain return 1").asScala.length
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      import scala.collection.JavaConverters._
      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()").asScala.length
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.executeAndConsume(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()")

      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for a profile procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      import scala.collection.JavaConverters._
      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()").asScala.length
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.executeAndConsume(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()")
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when consuming the whole iterator for an explain procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      import scala.collection.JavaConverters._
      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").asScala.length
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a regular query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime return 1").accept(consumerVisitor)
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime return 1").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a profile query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime profile return 1").accept(consumerVisitor)
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime profile return 1").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for an explain query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      // when
      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime explain return 1").accept(consumerVisitor)
      } finally {
        transaction.close()
      }
      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain return 1").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)
      } finally {
        transaction.close()
      }

      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.executeAndConsume(s"CYPHER runtime=$runtime CALL org.neo4j.bench.getAllNodes()")
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for a profile procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)
      } finally {
        transaction.close()
      }

      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.executeAndConsume(s"CYPHER runtime=$runtime profile CALL org.neo4j.bench.getAllNodes()")
      // then
      txBridge(service).hasTransaction shouldBe false
    }

    test(s"should not leak transaction when visiting the result for an explain procedure query - runtime=$runtime") {
      //given
      val service = new GraphDatabaseCypherService(db)
      val engine = createEngine(service)

      procedures(service).register(new AllNodesProcedure())
      txBridge(service).hasTransaction shouldBe false

      val transaction = db.beginTx()
      try {
        transaction.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").accept(consumerVisitor)
      } finally {
        transaction.close()
      }

      // then
      txBridge(service).hasTransaction shouldBe false

      // when
      engine.execute(s"CYPHER runtime=$runtime explain CALL org.neo4j.bench.getAllNodes()").consumeAll()
      // then
      txBridge(service).hasTransaction shouldBe false
    }
  }

  private val consumerVisitor = new ResultVisitor[RuntimeException] {
    override def visit(row: ResultRow): Boolean = true
  }

  private def txBridge(db: GraphDatabaseQueryService) = {
    db.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }

  private def procedures(db: GraphDatabaseQueryService) = {
    db.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
  }

  implicit class RichExecutionEngine(engine: ExecutionEngine) {

    def execute(query: String, f: QueryExecution => Unit = _ => ()): QueryExecution = {
      val transaction = db.beginTx()
      try {
        val result = engine.execute(query,
          VirtualValues.EMPTY_MAP,
          engine.queryService.transactionalContext(transaction.asInstanceOf[InternalTransaction], query = query -> Map()),
          profile = false,
          prePopulate = false,
          QuerySubscriber.DO_NOTHING_SUBSCRIBER)
        f(result)
        result
      } finally {
        transaction.close()
      }
    }

    def executeAndConsume(query: String): QueryExecution =
    {
      execute(query, result => result.consumeAll())
    }
  }

  class AllNodesProcedure extends CallableProcedure {

    import scala.collection.JavaConverters._

    private val results = Map[String, AnyRef]("node" -> Neo4jTypes.NTInteger)
    val procedureName = new QualifiedName(Array[String]("org", "neo4j", "bench"), "getAllNodes")
    val emptySignature: util.List[FieldSignature] = List.empty[FieldSignature].asJava
    val signature: ProcedureSignature = new ProcedureSignature(
      procedureName, paramSignature, resultSignature, Mode.READ, false, null, Array.empty,
      null, null, false, false, false)

    def paramSignature: util.List[FieldSignature] = List.empty[FieldSignature].asJava

    def resultSignature: util.List[FieldSignature] = results.keys.foldLeft(List.empty[FieldSignature]) { (fields, entry) =>
      fields :+ FieldSignature.outputField(entry, results(entry).asInstanceOf[Neo4jTypes.AnyType])
    }.asJava

    override def apply(context: Context,
                       objects: Array[AnyValue],
                       resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
      val ktx = context.kernelTransaction()
      val nodeBuffer = new ArrayBuffer[Long]()
      val cursor = ktx.cursors().allocateNodeCursor()
      ktx.dataRead().allNodesScan(cursor)
      while (cursor.next()) nodeBuffer.append(cursor.nodeReference())
      cursor.close()
      new RawIterator[Array[AnyValue], ProcedureException] {
        var index = 0

        override def next(): Array[AnyValue] = {
          val value = nodeBuffer(index)
          index += 1
          Array(Values.longValue(value))
        }

        override def hasNext: Boolean = {
          nodeBuffer.length < index
        }
      }
    }
  }

}
