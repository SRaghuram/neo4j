/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.PrintWriter
import java.lang.Boolean.FALSE
import java.lang.Boolean.TRUE

import com.neo4j.cypher.RunWithConfigTestSupport
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.csv.reader.MissingEndQuoteException
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport

class LoadCsvWithQuotesAcceptanceTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport with CreateTempFileTestSupport {
  def csvUrls(f: PrintWriter => Unit) = Seq(
    createCSVTempFileURL(f),
    createGzipCSVTempFileURL(f),
    createZipCSVTempFileURL(f)
  )

  override protected def initTest(): Unit = ()

  override protected def stopTest(): Unit = ()

  test("import rows with messy quotes using legacy mode as default") {
    runWithConfig() { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 1',\"\\\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
          writer.println("'Quotes 3',\"\\\"\\\"\"")
          writer.println("'Quotes 4',\"\"\"\"\"\"")
      })
      for (url <- urls) {
        val result = executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        assert(result.toList === List(
          Map("line.x" -> ""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\"\""),
          Map("line.x" -> "\"\"")
        ))
      }
    }
  }

  test("import rows with messy quotes using legacy mode") {
    runWithConfig(GraphDatabaseSettings.csv_legacy_quote_escaping -> TRUE) { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 1',\"\\\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
          writer.println("'Quotes 3',\"\\\"\\\"\"")
          writer.println("'Quotes 4',\"\"\"\"\"\"")
      })
      for (url <- urls) {
        val result = executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        assert(result.toList === List(
          Map("line.x" -> ""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\"\""),
          Map("line.x" -> "\"\"")
        ))
      }
    }
  }

  test("import rows with messy quotes using rfc4180 mode") {
    runWithConfig(GraphDatabaseSettings.csv_legacy_quote_escaping -> FALSE) { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
          writer.println("'Quotes 4',\"\"\"\"\"\"")
          writer.println("'Quotes 5',\"\\\"\"\"")
      })
      for (url <- urls) {
        val result = executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        assert(result.toList === List(
          Map("line.x" -> ""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\"\""),
          Map("line.x" -> "\\\"")
        ))
      }
    }
  }

  test("fail to import rows with java quotes when in rfc4180 mode") {
    runWithConfig(GraphDatabaseSettings.csv_legacy_quote_escaping -> FALSE) { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 1',\"\\\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
      })
      for (url <- urls) {
        intercept[MissingEndQuoteException] {
          executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        }.getMessage should include("which started on line 2")
      }
    }
  }

  def executeWithCustomDb(db: GraphDatabaseCypherService, query: String): RewindableExecutionResult = {
    db.withTx(tx => RewindableExecutionResult(tx.execute(query)))
  }

}
