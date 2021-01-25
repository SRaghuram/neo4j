/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import java.util

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.collection.RawIterator
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.GraphIcing
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.CallableProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters.seqAsJavaListConverter
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
      val r = tx.execute("CYPHER runtime=slotted MATCH (n), (m) WHERE true RETURN n, m, n.name, m.name")
      // We use slotted runtime. Pipelined will execute too much on `r.next` and everything will be closed already
      r.next()
      tx.rollback() // This should close all cursors
    } finally {
      managementService.shutdown()
    }
  }

  class AllNodesProcedure extends CallableProcedure {

    private val results = Map[String, AnyRef]("node" -> Neo4jTypes.NTInteger)
    val procedureName = new QualifiedName(Array[String]("org", "neo4j", "bench"), "getAllNodes")
    val emptySignature: util.List[FieldSignature] = new util.ArrayList[FieldSignature]()
    val signature: ProcedureSignature = new ProcedureSignature(
      procedureName, paramSignature, resultSignature, Mode.READ, false, null, Array.empty,
      null, null, false, false, false, false, false)

    def paramSignature: util.List[FieldSignature] = new util.ArrayList[FieldSignature]()

    def resultSignature: util.List[FieldSignature] = results.keys.foldLeft(List.empty[FieldSignature]) { (fields, entry) =>
      fields :+ FieldSignature.outputField(entry, results(entry).asInstanceOf[Neo4jTypes.AnyType])
    }.asJava

    override def apply(context: Context,
                       objects: Array[AnyValue],
                       resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
      val ktx = context.internalTransaction().kernelTransaction()
      val nodeBuffer = new ArrayBuffer[Long]()
      val cursor = ktx.cursors().allocateNodeCursor( ktx.pageCursorTracer() )
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
