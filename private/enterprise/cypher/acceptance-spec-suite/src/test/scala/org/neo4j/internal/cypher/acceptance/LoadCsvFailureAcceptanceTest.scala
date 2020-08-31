/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.HttpServerTestSupport
import org.neo4j.cypher.HttpServerTestSupportBuilder
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.exceptions
import org.neo4j.exceptions.LoadCsvStatusWrapCypherException
import org.neo4j.exceptions.LoadExternalResourceException
import org.scalatest.BeforeAndAfterAll

class LoadCsvFailureAcceptanceTest extends ExecutionEngineFunSuite
                                   with BeforeAndAfterAll
                                   with CreateTempFileTestSupport {

  for (runtime <- Seq("interpreted", "slotted")) {
    test(s"correct error message with file name and line number on MERGE with $runtime") {
      createLabeledNode(Map("OrderId" -> "4", "field1" -> "REPLACE_ME"), "Order")

      val url = createCSVTempFileURL({
        writer =>
          writer.println("OrderId,field1")
          writer.println("4,hi")
          writer.println("5,yo")
          writer.println("6,bye")
      })

      val query =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId
           | MERGE (x:Order {OrderId:o.otherOrderId})
           | RETURN o, x""".stripMargin
      val e = intercept[LoadCsvStatusWrapCypherException](execute(query))
      e.getMessage should include regex """Cannot merge the following node because of null property value for 'OrderId': \((x)?:Order\s\{OrderId: null\}\).+csv' on line 2.+"""
    }

    test(s"correct error message for previous version with file name and line number on MERGE with $runtime") {
      createLabeledNode(Map("OrderId" -> "4", "field1" -> "REPLACE_ME"), "Order")

      val url = createCSVTempFileURL({
        writer =>
          writer.println("OrderId,field1")
          writer.println("4,hi")
          writer.println("5,yo")
          writer.println("6,bye")
      })

      val query =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId
           | MERGE (x:Order {OrderId:o.otherOrderId})
           | RETURN o, x""".stripMargin
      val e = intercept[LoadCsvStatusWrapCypherException](execute(query))
      e.getMessage should include regex """Cannot merge the following node because of null property value for 'OrderId': \((x)?:Order\s\{OrderId: null\}\).+csv' on line 2.+"""
    }

    test(s"correct error message with file name and line number on / by 0 with $runtime") {
      // given
      val url = createCSVTempFileURL({ writer =>
        writer.println("1")
        writer.println("2")
        writer.println("0")
        writer.println("3")
      })

      val queryText = s"CYPHER runtime=$runtime LOAD CSV FROM '$url' AS line CREATE ({name: 1/toInteger(line[0])})"
      val e = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(queryText))
      e.getMessage should include regex """/ by zero.+csv' on line 3.+"""
    }

    test(s"correct error message without file name and line number after aggregation with $runtime") {
      createLabeledNode(Map("OrderId" -> "3", "field1" -> "REPLACE_ME"), "Order")

      val url = createCSVTempFileURL({
        writer =>
          writer.println("OrderId,field1")
          writer.println("4,hi")
          writer.println("5,yo")
          writer.println("6,bye")
      })

      val query =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId
           | WITH count(o) as count
           | RETURN 1/count""".stripMargin
      val e = intercept[exceptions.ArithmeticException](execute(query))
      e.getMessage should include("/ by zero")
      e.getMessage should not include "' on line"
    }

    test(s"correct error message for previous version without file name and line number after aggregation with $runtime") {
      createLabeledNode(Map("OrderId" -> "3", "field1" -> "REPLACE_ME"), "Order")

      val url = createCSVTempFileURL({
        writer =>
          writer.println("OrderId,field1")
          writer.println("4,hi")
          writer.println("5,yo")
          writer.println("6,bye")
      })

      val query =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId
           | WITH count(o) as count
           | RETURN 1/count""".stripMargin
      val e = intercept[exceptions.ArithmeticException](execute(query))
      e.getMessage should include("/ by zero")
      e.getMessage should not include "' on line"
    }

    test(s"correct error message with file name and line number before aggregation with $runtime") {
      createLabeledNode(Map("OrderId" -> "4", "field1" -> "3"), "Order")

      val url = createCSVTempFileURL({
        writer =>
          writer.println("OrderId,field1")
          writer.println("4,0")
          writer.println("5,1")
          writer.println("6,3")
      })

      val query =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId AND o.field1 = 1/toInteger(row.field1)
           | WITH count(o) as count
           | RETURN count""".stripMargin
      val e = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query))
      e.getMessage should include regex """/ by zero.+csv' on line 2.+"""
    }

    test(s"correct error message for last line in file with $runtime") {
      createLabeledNode(Map("OrderId" -> "3", "field1" -> "REPLACE_ME"), "Order")
      // given
      val url = createCSVTempFileURL({ writer =>
        writer.println("OrderId,field1")
        writer.println("1,hola")
        writer.println("2,hello")
        writer.println("3,hej")
      })

      val queryText = s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
                         | MATCH (o:Order) WHERE o.OrderId = row.OrderId
                         | RETURN o, 1/row.field1""".stripMargin
      val e = intercept[LoadCsvStatusWrapCypherException](execute(queryText))
      e.getMessage should include regex """Cannot divide `Long` by `String`.+csv' on line 4 \(which is the last row in the file\).+"""
    }

    test(s"correct error message when DISTINCT with $runtime") {
      createLabeledNode(Map("OrderId" -> "4", "field1" -> "3"), "Order")
      createLabeledNode(Map("OrderId" -> "5", "field1" -> "0"), "Order")

      val url = createCSVTempFileURL({
        writer =>
          writer.println("OrderId")
          writer.println("4")
          writer.println("5")
          writer.println("6")
      })

      val query1 =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId AND 1/toInteger(o.field1) = 2
           | WITH DISTINCT o.OrderId AS order
           | RETURN order""".stripMargin
      val e1 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query1))
      e1.getMessage should include regex """/ by zero.+csv' on line 3.+"""

      val query2 =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId
           | WITH DISTINCT 1/toInteger(o.field1) AS f
           | RETURN f""".stripMargin
      val e2 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query2))
      e2.getMessage should include regex """/ by zero.+csv' on line 3.+"""

      val query3 =
        s"""CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId
           | WITH DISTINCT o
           | RETURN 1/toInteger(o.field1)""".stripMargin
      val e3 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query3))
      e3.getMessage should include regex """/ by zero.+csv' on line 3.+"""
    }

    test(s"correct error message for PROFILE with $runtime"){
      createLabeledNode(Map("OrderId" -> "4", "field1" -> "3"), "Order")
      createLabeledNode(Map("OrderId" -> "5", "field1" -> "0"), "Order")

      val url = createCSVTempFileURL({
        writer =>
          writer.println("OrderId,field1")
          writer.println("4,0")
          writer.println("5,1")
          writer.println("6,3")
      })

      // Without error
      val query1 =
        s"""CYPHER runtime=$runtime PROFILE LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId
           | RETURN o""".stripMargin
      execute(query1)

      // With error
      val query2 =
        s"""CYPHER runtime=$runtime PROFILE LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (o:Order) WHERE o.OrderId = row.OrderId AND o.field1 = 1/toInteger(row.field1)
           | RETURN o""".stripMargin
      val e2 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query2))
      e2.getMessage should include regex """/ by zero.+csv' on line 2.+"""
    }

    test(s"correct error message for non-existing file with $runtime") {
      // given
      createLabeledNode(Map("OrderId" -> "4", "field1" -> "3"), "Order")
      val url = createCSVTempFileURL("orders")({ writer =>
        writer.println("OrderId,field1")
        writer.println("5,1")
        writer.println("6,3")
        writer.println("4,0")
      })

      val urlOfUrls1 = createCSVTempFileURL("urls1")({ writer =>
        writer.println("notvalid.csv")
        writer.println(url)
      })

      val urlOfUrls2 = createCSVTempFileURL("urls2")({ writer =>
        writer.println(url)
        writer.println("notvalid.csv")
      })

      val queryText1 =
        s"""
           |CYPHER runtime=$runtime LOAD CSV FROM '$urlOfUrls1' as url
           |LOAD CSV WITH HEADERS FROM url[0] AS row
           |MATCH (o:Order) WHERE o.OrderId = row.OrderId AND o.field1 = 1/toInteger(row.field1)
           |CREATE (o)-[:KNOWS]->()
         """.stripMargin
      val e1 = intercept[LoadCsvStatusWrapCypherException](execute(queryText1))
      e1.getMessage should include regex """Invalid URL 'notvalid\.csv': no protocol: notvalid\.csv.+urls1\d+\.csv' on line 1.+"""
      assert(e1.getCause.isInstanceOf[LoadExternalResourceException])

      val queryText2 =
        s"""
           |CYPHER runtime=$runtime LOAD CSV FROM '$urlOfUrls2' as url
           |LOAD CSV WITH HEADERS FROM url[0] AS row
           |MATCH (o:Order) WHERE o.OrderId = row.OrderId AND o.field1 = 1/toInteger(row.field1)
           |CREATE (o)-[:KNOWS]->()
         """.stripMargin
      val e2 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(queryText2))
      e2.getMessage should include regex """/ by zero.+orders\d+\.csv' on line 4 \(which is the last row in the file\).+"""
    }

    test(s"correct error message for two files with $runtime"){
      createLabeledNode(Map("OrderId" -> "4", "field1" -> "1"), "Order")
      createLabeledNode(Map("OrderId" -> "5", "field1" -> "2"), "Order")

      val url1 = createCSVTempFileURL("orders-")({ writer =>
        writer.println("OrderId,field1")
        writer.println("4,1")
        writer.println("5,2")
        writer.println("6,0")
      })
      val url2 = createCSVTempFileURL("fields-")({ writer =>
        writer.println("OrderId,field1")
        writer.println("4,1")
        writer.println("5,0")
        writer.println("6,3")
      })

      val query1 =
        s"""
           |CYPHER runtime=$runtime UNWIND ["$url1", "$url2"] as url
           |LOAD CSV WITH HEADERS FROM url AS row
           |MATCH (o:Order) WHERE o.OrderId <> row.OrderId AND o.field1 = 1/toInteger(row.field1)
           |RETURN o
         """.stripMargin

      val e1 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query1))
      e1.getMessage should include regex """/ by zero.+orders-\d+\.csv' on line 4 \(which is the last row in the file\).+"""

      val query2 =
        s"""
           |CYPHER runtime=$runtime UNWIND ["$url1", "$url2"] as url
           |LOAD CSV WITH HEADERS FROM url AS row
           |MATCH (o:Order) WHERE o.OrderId = row.OrderId AND o.field1 = 1/toInteger(row.field1)
           |RETURN o
         """.stripMargin

      val e2 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query2))
      e2.getMessage should include regex """/ by zero.+fields-\d+\.csv' on line 3.+"""

      val query3 =
        s"""
           |CYPHER runtime=$runtime LOAD CSV WITH HEADERS FROM '$url1' as row1
           |MATCH (o:Order) WHERE o.OrderId = row1.OrderId
           |WITH o.OrderId AS order
           |LOAD CSV WITH HEADERS FROM '$url2' as row2
           |MATCH (x:Order) WHERE x.OrderId = order AND x.field1 = 1/toInteger(row2.field1)
           |RETURN x
        """.stripMargin
      val e3 = intercept[exceptions.LoadCsvStatusWrapCypherException](execute(query3))
      e3.getMessage should include regex """/ by zero.+fields-\d+\.csv' on line 3.+"""
    }
  }

  private val CSV_DATA_CONTENT = "1,1,1\n2,2,2\n3,3,3\n".getBytes
  private val CSV_PATH = "/test.csv"
  private val CSV_COOKIE_PATH = "/cookie_test.csv"
  private val CSV_REDIRECT_PATH = "/redirect_test.csv"
  private val MAGIC_COOKIE = "neoCookie=Magic"
  private var httpServer: HttpServerTestSupport = _
  private var port = -1

  override def beforeAll() {
    val  builder = new HttpServerTestSupportBuilder()
    builder.onPathReplyWithData(CSV_PATH, CSV_DATA_CONTENT)

    builder.onPathReplyWithData(CSV_COOKIE_PATH, CSV_DATA_CONTENT)
    builder.onPathReplyOnlyWhen(CSV_COOKIE_PATH, HttpServerTestSupport.hasCookie(MAGIC_COOKIE))

    builder.onPathRedirectTo(CSV_REDIRECT_PATH, CSV_COOKIE_PATH)
    builder.onPathTransformResponse(CSV_REDIRECT_PATH, HttpServerTestSupport.setCookie(MAGIC_COOKIE))

    httpServer = builder.build()
    httpServer.start()
    port = httpServer.boundInfo.getPort
    assert(port > 0)
  }

  override def afterAll() {
    httpServer.stop()
  }
}
