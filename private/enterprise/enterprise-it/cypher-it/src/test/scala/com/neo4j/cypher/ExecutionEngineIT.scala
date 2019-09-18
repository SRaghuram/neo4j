/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import java.util

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.collection.RawIterator
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.GraphIcing
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs._
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.{CallableProcedure, Context}
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

class ExecutionEngineIT extends CypherFunSuite with GraphIcing {

  test("should close resources on TX rollback") {
    val managementService = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build()
    try {
      // given
      val db = managementService.database(DEFAULT_DATABASE_NAME)

      val tx = db.beginTx()

      tx.createNode()
      tx.createNode()

      // We need two node vars to have one non-pooled cursor
      val r = tx.execute("MATCH (n), (m) WHERE true RETURN n, m, n.name, m.name")
      r.next()
      tx.rollback() // This should close all cursors
    } finally {
      managementService.shutdown()
    }
  }

  test("should be possible to close compiled result after it is consumed") {
    val managementService = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent().build()
    // given
    val db = managementService.database(DEFAULT_DATABASE_NAME)
    try {

      // when
      val transaction = db.beginTx()
      val result = transaction.execute("CYPHER runtime=compiled MATCH (n) RETURN n")
      result.accept(new ResultVisitor[RuntimeException] {
        def visit(row: ResultRow) = true
      })

      result.close()
      transaction.close()

      // then
      // call to close actually worked
    }
    finally {
      managementService.shutdown()
    }
  }

  private implicit class RichDb(db: GraphDatabaseCypherService) {
    def planDescriptionForQuery(tx:InternalTransaction, query: String): ExecutionPlanDescription = {
      val res = tx.execute(query)
      res.resultAsString()
      res.getExecutionPlanDescription
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
      val nodes = nodeBuffer.iterator
      var count = 0
      new RawIterator[Array[AnyValue], ProcedureException] {
        override def next(): Array[AnyValue] = {
          count = count + 1
          Array(Values.longValue(nodes.next()))
        }

        override def hasNext: Boolean = {
          nodes.hasNext
        }
      }
    }
  }
}
