/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.io.File
import java.io.PrintWriter
import java.lang.Boolean.FALSE
import java.net.URL
import java.net.URLConnection
import java.net.URLStreamHandler
import java.net.URLStreamHandlerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.Collections.emptyMap

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.HttpServerTestSupport
import org.neo4j.cypher.HttpServerTestSupportBuilder
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Configuration
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

class LoadCsvAcceptanceTest
  extends ExecutionEngineFunSuite
  with BeforeAndAfterAll
  with QueryStatisticsTestSupport
  with CreateTempFileTestSupport
  with CypherComparisonSupport
  with com.neo4j.cypher.RunWithConfigTestSupport
  with ResourceTracking {

  override protected def initTest(): Unit = {
    super.initTest()
    trackResources(graph)
  }

  private def csvUrls(f: PrintWriter => Unit): Seq[String] = Seq(
    createCSVTempFileURL(f),
    createGzipCSVTempFileURL(f),
    createZipCSVTempFileURL(f)
  )

  test("import three rows with headers and match from import using index hint") {
    // Given
    val urls = csvUrls({
      writer =>
        writer.println("USERID,OrderId,field1,field2")
        writer.println("1, '4', 1, '4'")
        writer.println("2, '5', 2, '5'")
        writer.println("3, '6', 3, '6'")
    })

    executeSingle(
      s"""LOAD CSV WITH HEADERS FROM '${urls.head}' AS row
         | CREATE (user:User{userID: row.USERID})
         | CREATE (order:Order{orderID: row.OrderId})
         | CREATE (user)-[acc:ORDERED]->(order)
         | RETURN count(*)""".stripMargin
    )

    resourceMonitor.assertClosedAndClear(1)

    graph.createIndex("User", "userID")

    // when & then
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"""LOAD CSV WITH HEADERS FROM '$url' AS row
           | MATCH (user:User{userID: row.USERID}) USING INDEX user:User(userID)
           | MATCH (order:Order{orderID: row.OrderId})
           | MATCH (user)-[acc:ORDERED]->(order)
           | SET acc.field1=row.field1,
           | acc.field2=row.field2
           | RETURN count(*); """.stripMargin,
        planComparisonStrategy = ComparePlansWithAssertion(_ should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("user"))))

      resourceMonitor.assertClosedAndClear(1)
      assertStats(result, propertiesWritten = 6)
      result.executionPlanDescription() should includeSomewhere.atLeastNTimes(1, aPlan("NodeIndexSeek").containingVariables("user"))
    }
  }

  test("should PROFILE LOAD CSV queries with") {
    // Given
    val urls: Seq[String] = csvUrls({
      writer =>
        // 7 unique nodes and 6 relationships
        writer.println("user1,user2")
        writer.println("Sangreal,tbaum")
        writer.println("adilfulara,emileifrem")
        writer.println("maheshksp,emileifrem")
        writer.println("Sangreal,pigmon")
        writer.println("adilfulara,maheshksp")
        writer.println("Sangreal,lucascaton")
    })

    for (url <- urls) {
      val query =
        s"""PROFILE
           |LOAD CSV WITH HEADERS FROM '$url' AS line
           |MERGE (u1:User {login: line.user1})
           |MERGE (u2:User {login: line.user2})
           |CREATE (u1)-[:FRIEND]->(u2)
           |""".stripMargin

      val result = executeWith(Configs.InterpretedAndSlotted, query)
      result.queryStatistics().nodesCreated should be(7)
      result.queryStatistics().relationshipsCreated should be(6)
      val list = result.toList
      list should be(empty)

      // Clean up for next loop iteration
      executeSingle("MATCH (n) DETACH DELETE n")
    }
  }

  test("should be able to use multiple index hints with load csv") {
    val startNodes = (0 to 9 map (i => createLabeledNode(Map("loginId" -> i.toString), "Login"))).toArray
    val endNodes = (0 to 9 map (i => createLabeledNode(Map("platformId" -> i.toString), "Permission"))).toArray

    for( a <- 0 to 9 ) {
      for( b <- 0 to 9) {
        relate( startNodes(a), endNodes(b), "prop" -> (10 * a + b))
      }
    }

    val urls = csvUrls({
      writer =>
        writer.println("USER_ID,PLATFORM")
        writer.println("1,5")
        writer.println("2,4")
        writer.println("3,4")
    })

    graph.createIndex("Permission", "platformId")
    graph.createIndex("Login", "loginId")

    val query =
      s"""
         |    LOAD CSV WITH HEADERS FROM '${urls.head}' AS line
         |    WITH line
         |    MATCH (l:Login {loginId: line.USER_ID}), (p:Permission {platformId: line.PLATFORM})
         |    USING INDEX l:Login(loginId)
         |    USING INDEX p:Permission(platformId)
         |    MATCH (l)-[r: REL]->(p)
         |    RETURN r.prop
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query,
      planComparisonStrategy = ComparePlansWithAssertion(_  should includeSomewhere.atLeastNTimes(2, aPlan("NodeIndexSeek"))))

    result.toSet should be(Set(Map("r.prop" -> 15), Map("r.prop" -> 24), Map("r.prop" -> 34)))
  }

  test("import should not be eager") {
    createNode(Map("OrderId" -> "4", "field1" -> "REPLACE_ME"))

    val url = createCSVTempFileURL({
      writer =>
        writer.println("OrderId,field1")
        writer.println("4,hi")
        writer.println("5,yo")
        writer.println("6,bye")
    })

    val result = executeWith(Configs.InterpretedAndSlotted,
      s"""LOAD CSV WITH HEADERS FROM '$url' AS row
         | WITH row.field1 as field, row.OrderId as order
         | MATCH (o) WHERE o.OrderId = order
         | SET o.field1 = field""".stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ should not( includeSomewhere.aPlan("Eager"))))

    resourceMonitor.assertClosedAndClear(1)
    assertStats(result, nodesCreated = 0, propertiesWritten = 1)
  }

  test("import three strings") {
    val urls = csvUrls({
      writer =>
        writer.println("'Foo'")
        writer.println("'Foo'")
        writer.println("'Foo'")
    })

    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
      resourceMonitor.assertClosedAndClear(1)
      assertStats(result, nodesCreated = 3, propertiesWritten = 3)
    }
  }

  test("should return correct linenumber") {
    val url = createCSVTempFileURL("neo")({
      writer =>
        writer.println("Foo")
        writer.println("Bar")
        writer.println("Baz")
    })

    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name, linenumber()")
    resourceMonitor.assertClosedAndClear(1)
    assertStats(result, nodesCreated = 3, propertiesWritten = 3)
    result.toList should equal(List(
      Map("linenumber()" -> 1, "a.name" -> "Foo"),
      Map("linenumber()" -> 2, "a.name" -> "Bar"),
      Map("linenumber()" -> 3, "a.name" -> "Baz")))
  }

  test("should return no linenumber or filename after aggregation") {
    val url = createCSVTempFileURL("neo")({
      writer =>
        writer.println("Foo")
        writer.println("Bar")
        writer.println("Baz")
    })

    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line WITH count(line) as linecount RETURN linecount, linenumber(), file()")
    resourceMonitor.assertClosedAndClear(1)
    result.toList should equal(List(Map("linecount" -> 3, "linenumber()" -> null, "file()" -> null)))
  }

  test("should return no linenumber or filename without load csv") {
    val result = executeWith(Configs.InterpretedAndSlotted, "RETURN linenumber(), file()")
    result.toList should equal(List(Map("linenumber()" -> null, "file()" -> null)))
  }

  test("should return correct filename") {
    val path = Files.createTempFile("file",".csv")

    Files.write(path,"foo".getBytes)
    assert(Files.exists(path))

    val filePathForQuery = path.normalize().toUri
    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$filePathForQuery' AS line CREATE (a {name: line[0]}) RETURN a.name, file()")
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
    resourceMonitor.assertClosedAndClear(1)
    result.toList should equal(List(Map("a.name" -> "foo", "file()" -> filePathForQuery.getPath)))
  }

  test("should return correct filename for path including spaces") {
    val dirPath = Files.createTempDirectory("directory with spaces")
    val path = Files.createTempFile(dirPath,"file",".csv")

    Files.write(path,"foo".getBytes)
    assert(Files.exists(path))

    val filePathForQuery = path.normalize().toUri
    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$filePathForQuery' AS line CREATE (a {name: line[0]}) RETURN a.name, file()")
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
    resourceMonitor.assertClosedAndClear(1)
    result.toList should equal(List(Map("a.name" -> "foo", "file()" -> filePathForQuery.getPath)))
  }

  test("make sure to release all possible locks/references on input files") {
    val path = Files.createTempFile("file",".csv")

    Files.write(path,"foo".getBytes)
    assert(Files.exists(path))

    val filePathForQuery = path.normalize().toUri
    val result = execute(s"LOAD CSV FROM '$filePathForQuery' AS line CREATE (a {name: line[0]}) RETURN a.name")
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
    resourceMonitor.assertClosedAndClear(1)

    assert(Files.deleteIfExists(path))
  }

  test("import three numbers") {
    val urls = csvUrls({
      writer =>
        writer.println("1")
        writer.println("2")
        writer.println("3")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line CREATE (a {number: line[0]}) RETURN a.number")
      assertStats(result, nodesCreated = 3, propertiesWritten = 3)
      resourceMonitor.assertClosedAndClear(1)

      result.columnAs[Long]("a.number").toList === List("")
    }
  }

  test("import three rows numbers and strings") {
    val urls = csvUrls({
      writer =>
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
      assertStats(result, nodesCreated = 3, propertiesWritten = 3)
      resourceMonitor.assertClosedAndClear(1)
    }
  }

  test("import three rows with headers") {
    val urls = csvUrls({
      writer =>
        writer.println("id,name")
        writer.println("1, 'Aadvark'")
        writer.println("2, 'Babs'")
        writer.println("3, 'Cash'")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"LOAD CSV WITH HEADERS FROM '$url' AS line CREATE (a {id: line.id, name: line.name}) RETURN a.name"
      )

      resourceMonitor.assertClosedAndClear(1)
      assertStats(result, nodesCreated = 3, propertiesWritten = 6)
    }
  }

  test("import three rows with headers messy data") {

    val urls = csvUrls({
      writer =>
        writer.println("id,name,x")
        writer.println("1,'Aardvark',0")
        writer.println("2,'Babs'")
        writer.println("3,'Cash',1")
        writer.println("4,'Dice',\"\"")
        writer.println("5,'Emerald',")
    })
    for (url <- urls) {
      val result =executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
      resourceMonitor.assertClosedAndClear(1)
      assert(result.toList === List(
        Map("line.x" -> "0"),
        Map("line.x" -> null),
        Map("line.x" -> "1"),
        Map("line.x" -> ""),
        Map("line.x" -> null))
      )
    }
  }

  test("import three rows with headers messy data with predicate") {
    val urls = csvUrls({
      writer =>
        writer.println("id,name,x")
        writer.println("1,'Aardvark',0")
        writer.println("2,'Babs'")
        writer.println("3,'Cash',1")
        writer.println("4,'Dice',\"\"")
        writer.println("5,'Emerald',")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV WITH HEADERS FROM '$url' AS line WITH line WHERE line.x IS NOT NULL RETURN line.name")
      resourceMonitor.assertClosedAndClear(1)
      assert(result.toList === List(
        Map("line.name" -> "'Aardvark'"),
        Map("line.name" -> "'Cash'"),
        Map("line.name" -> "'Dice'"))
      )
    }
  }

  test("should handle quotes") {
    val urls = csvUrls({
      writer =>
        writer.println("String without quotes")
        writer.println("'String, with single quotes'")
        writer.println("\"String, with double quotes\"")
        writer.println( """"String with ""quotes"" in it"""")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line RETURN line as string").toList
      resourceMonitor.assertClosedAndClear(1)
      assert(result === List(
        Map("string" -> Seq("String without quotes")),
        Map("string" -> Seq("'String", " with single quotes'")),
        Map("string" -> Seq("String, with double quotes")),
        Map("string" -> Seq( """String with "quotes" in it"""))))
    }
  }

  test("should handle crlf line termination") {
    val urls = csvUrls({
      writer =>
        writer.print("1,'Aadvark',0\r\n")
        writer.print("2,'Babs'\r\n")
        writer.print("3,'Cash',1\r\n")
    })

    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line RETURN line")
      resourceMonitor.assertClosedAndClear(1)
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should handle lf line termination") {
    val urls = csvUrls({
      writer =>
        writer.print("1,'Aadvark',0\n")
        writer.print("2,'Babs'\n")
        writer.print("3,'Cash',1\n")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line RETURN line")
      resourceMonitor.assertClosedAndClear(1)
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should handle cr line termination") {
    val urls = csvUrls({
      writer =>
        writer.print("1,'Aadvark',0\r")
        writer.print("2,'Babs'\r")
        writer.print("3,'Cash',1\r")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line RETURN line")
      resourceMonitor.assertClosedAndClear(1)
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should handle custom field terminator") {
    val urls = csvUrls({
      writer =>
        writer.println("1;'Aadvark';0")
        writer.println("2;'Babs'")
        writer.println("3;'Cash';1")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line FIELDTERMINATOR ';' RETURN line")
      resourceMonitor.assertClosedAndClear(1)
      assert(result.toList === List(Map("line" -> Seq("1", "'Aadvark'", "0")), Map("line" -> Seq("2", "'Babs'")),
        Map("line" -> Seq("3", "'Cash'", "1"))))
    }
  }

  test("should open file containing strange chars with '") {
    val filename = ensureNoIllegalCharsInWindowsFilePath("cypher '%^&!@#_)(098.:,;[]{}\\~$*+-")
    val url = createCSVTempFileURL(filename)({
      writer =>
        writer.println("something")
    })

    val result = executeWith(Configs.InterpretedAndSlotted, "LOAD CSV FROM \"" + url + "\" AS line RETURN line as string").toList
    resourceMonitor.assertClosedAndClear(1)
    assert(result === List(Map("string" -> Seq("something"))))
  }

  test("should open file containing strange chars with \"") {
    val filename = ensureNoIllegalCharsInWindowsFilePath("cypher \"%^&!@#_)(098.:,;[]{}\\~$*+-")
    val url = createCSVTempFileURL(filename)({
      writer =>
        writer.println("something")
    })

    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line RETURN line as string").toList
    resourceMonitor.assertClosedAndClear(1)
    assert(result === List(Map("string" -> Seq("something"))))
  }

  test("empty file does not create anything") {
    val urls = csvUrls(writer => {})
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
      resourceMonitor.assertClosedAndClear(1)
      assertStats(result, nodesCreated = 0)
    }
  }

  test("should be able to open relative paths with dot") {
    val url = createCSVTempFileURL(filename = "cypher", dir = "./")(
      writer =>
        writer.println("something")
    ).cypherEscape

    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
    resourceMonitor.assertClosedAndClear(1)
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
  }

  test("should be able to open relative paths with dotdot") {
    val url = createCSVTempFileURL(filename = "cypher", dir = "../")(
      writer =>
        writer.println("something")
    ).cypherEscape

    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line CREATE (a {name: line[0]}) RETURN a.name")
    resourceMonitor.assertClosedAndClear(1)
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
  }

  test("should handle null keys in maps as result value") {
    val urls = csvUrls({
      writer =>
        writer.println("DEPARTMENT ID;DEPARTMENT NAME;")
        writer.println("010-1010;MFG Supplies;")
        writer.println("010-1011;Corporate Procurement;")
        writer.println("010-1015;MFG - Engineering HQ;")
    })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV WITH HEADERS FROM '$url' AS line FIELDTERMINATOR ';' RETURN *").toList
      resourceMonitor.assertClosedAndClear(1)
      assert(result === List(
        Map("line" -> Map("DEPARTMENT ID" -> "010-1010", "DEPARTMENT NAME" -> "MFG Supplies",
          null.asInstanceOf[String] -> null)),
        Map("line" -> Map("DEPARTMENT ID" -> "010-1011", "DEPARTMENT NAME" -> "Corporate Procurement",
          null.asInstanceOf[String] -> null)),
        Map("line" -> Map("DEPARTMENT ID" -> "010-1015", "DEPARTMENT NAME" -> "MFG - Engineering HQ",
          null.asInstanceOf[String] -> null))
      ))
    }
  }

  test("should handle returning null keys") {
    val urls = csvUrls({
      writer =>
        writer.println("DEPARTMENT ID;DEPARTMENT NAME;")
        writer.println("010-1010;MFG Supplies;")
        writer.println("010-1011;Corporate Procurement;")
        writer.println("010-1015;MFG - Engineering HQ;")
    })

    for (url <- urls) {
      //Using innerExecuteDeprecated because different versions has different ordering for keys
      val result =  executeSingle(s"LOAD CSV WITH HEADERS FROM '$url' AS line FIELDTERMINATOR ';' RETURN keys(line)").toList

      assert(result === List(
        Map("keys(line)" -> List(null, "DEPARTMENT ID", "DEPARTMENT NAME" )),
        Map("keys(line)" -> List(null, "DEPARTMENT ID", "DEPARTMENT NAME" )),
        Map("keys(line)" -> List(null, "DEPARTMENT ID", "DEPARTMENT NAME" ))
      ))
    }
  }

  test("should fail gracefully when loading missing file") {
    failWithError(Configs.InterpretedAndSlotted, "LOAD CSV FROM 'file:///./these_are_not_the_droids_you_are_looking_for.csv' AS line CREATE (a {name:line[0]})",
      List("Couldn't load the external resource at: file:/./these_are_not_the_droids_you_are_looking_for.csv"))
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should be able to download data from the web") {
    val url = s"http://127.0.0.1:$port/test.csv".cypherEscape

    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line RETURN count(line)")
    resourceMonitor.assertClosedAndClear(1)
    result.columnAs[Long]("count(line)").toList should equal(List(3))
  }

  test("should be able to download from a website when redirected and cookies are set") {
    val url = s"http://127.0.0.1:$port/redirect_test.csv".cypherEscape

    val result = executeWith(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '$url' AS line RETURN count(line)")
    resourceMonitor.assertClosedAndClear(1)
    result.columnAs[Long]("count(line)").toList should equal(List(3))
  }

  test("should fail gracefully when getting 404") {
    failWithError(Configs.InterpretedAndSlotted, s"LOAD CSV FROM 'http://127.0.0.1:$port/these_are_not_the_droids_you_are_looking_for/' AS line CREATE (a {name:line[0]})",
      List("Couldn't load the external resource at"))
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should fail gracefully when loading non existent (local) site") {
    failWithError(Configs.InterpretedAndSlotted, "LOAD CSV FROM 'http://127.0.0.1:9999/these_are_not_the_droids_you_are_looking_for/' AS line CREATE (a {name:line[0]})",
      List("Couldn't load the external resource at"))
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should reject URLs that are not valid") {

    failWithError(Configs.InterpretedAndSlotted, s"LOAD CSV FROM 'morsecorba://sos' AS line CREATE (a {name:line[0]})",
      List("Invalid URL 'morsecorba://sos': unknown protocol: morsecorba"))
    resourceMonitor.assertClosedAndClear(0)

    failWithError(Configs.InterpretedAndSlotted, s"LOAD CSV FROM '://' AS line CREATE (a {name:line[0]})",
      List("Invalid URL '://': no protocol: ://"))
    resourceMonitor.assertClosedAndClear(0)

    failWithError(Configs.InterpretedAndSlotted, s"LOAD CSV FROM 'foo.bar' AS line CREATE (a {name:line[0]})",
      List("Invalid URL 'foo.bar': no protocol: foo.bar"))
    resourceMonitor.assertClosedAndClear(0)

    failWithError(Configs.InterpretedAndSlotted, s"LOAD CSV FROM 'jar:file:///tmp/bar.jar' AS line CREATE (a {name:line[0]})",
      List("Invalid URL 'jar:file:///tmp/bar.jar': no !/ in spec"))
    resourceMonitor.assertClosedAndClear(0)

    failWithError(Configs.InterpretedAndSlotted, "LOAD CSV FROM 'file://./blah.csv' AS line CREATE (a {name:line[0]})",
      List("Cannot load from URL 'file://./blah.csv': file URL may not contain an authority section (i.e. it should be 'file:///')"))
    resourceMonitor.assertClosedAndClear(0)

    failWithError(Configs.InterpretedAndSlotted, "LOAD CSV FROM 'file:///tmp/blah.csv?q=foo' AS line CREATE (a {name:line[0]})",
      List("Cannot load from URL 'file:///tmp/blah.csv?q=foo': file URL may not contain a query component"))
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should deny URLs for blocked protocols") {
    failWithError(Configs.InterpretedAndSlotted, s"LOAD CSV FROM 'jar:file:///tmp/bar.jar!/blah/foo.csv' AS line CREATE (a {name:line[0]})",
      List("Cannot load from URL 'jar:file:///tmp/bar.jar!/blah/foo.csv': loading resources via protocol 'jar' is not permitted"))
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should fail for file urls if local file access disallowed") {
    val managementService = acceptanceTestDatabaseBuilder
      .setConfig(GraphDatabaseSettings.allow_file_urls, FALSE)
      .build()
    val db = managementService.database(DEFAULT_DATABASE_NAME)
    try {
      val transaction = db.beginTx()
      try {
        intercept[QueryExecutionException] {
          transaction.execute(s"LOAD CSV FROM 'file:///tmp/blah.csv' AS line CREATE (a {name:line[0]})", emptyMap())
        }.getMessage should endWith(": configuration property 'dbms.security.allow_csv_import_from_file_urls' is false")
      } finally {
        transaction.close()
      }
    } finally {
      managementService.shutdown()
    }
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should allow paths relative to authorized directory") {
    val dir = createTempDirectory("loadcsvroot")
    pathWrite(dir.resolve("tmp/blah.csv"))(
      writer =>
        writer.println("something")
    )

    val managementService = acceptanceTestDatabaseBuilder
      .setConfig(GraphDatabaseSettings.load_csv_file_url_root, dir)
      .build()

    val db = managementService.database(DEFAULT_DATABASE_NAME)

    trackResources(db)

    val tx = db.beginTx
    try {
      val result = tx.execute(s"LOAD CSV FROM 'file:///tmp/blah.csv' AS line RETURN line[0] AS field", emptyMap())
      result.asScala.map(_.asScala).toList should equal(List(Map("field" -> "something")))
      result.close()
    } finally {
      tx.close()
      managementService.shutdown()
    }
    resourceMonitor.assertClosedAndClear(1)
  }

  test("should restrict file urls to be rooted within an authorized directory") {
    val dir = createTempDirectory("loadcsvroot")

    val managementService = acceptanceTestDatabaseBuilder
      .setConfig(GraphDatabaseSettings.load_csv_file_url_root, dir)
      .build()
    val db = managementService.database(DEFAULT_DATABASE_NAME)

    trackResources(db)

    val transaction = db.beginTx()
    try {
      intercept[QueryExecutionException] {
        transaction.execute(s"LOAD CSV FROM 'file:///../foo.csv' AS line RETURN line[0] AS field", emptyMap()).asScala.size
      }.getMessage should endWith(" file URL points outside configured import directory").or(include("Couldn't load the external resource at"))
    } finally {
      transaction.close()
      managementService.shutdown()
    }
    resourceMonitor.assertClosedAndClear(0)
  }

  test("should apply protocol rules set at db construction") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("something")
    })

    URL.setURLStreamHandlerFactory(new URLStreamHandlerFactory {
      override def createURLStreamHandler(protocol: String): URLStreamHandler =
        if (protocol != "testproto")
          null
        else
          new URLStreamHandler {
            override def openConnection(u: URL): URLConnection = new URL(url).openConnection()
          }
    })

    val managementService = new TestEnterpriseDatabaseManagementServiceBuilder(acceptanceDbFolder)
      .setConfig(GraphDatabaseInternalSettings.cypher_enable_runtime_monitors, java.lang.Boolean.TRUE)
      .addURLAccessRule("testproto", (config: Configuration, url: URL) => url)
      .impermanent()
      .build()
    val db = managementService.database(DEFAULT_DATABASE_NAME)

    trackResources(db)

    val tx = db.beginTx
    try {
      val result = tx.execute(s"LOAD CSV FROM 'testproto://foo.bar' AS line RETURN line[0] AS field", emptyMap())
      result.asScala.map(_.asScala).toList should equal(List(Map("field" -> "something")))
      resourceMonitor.assertClosedAndClear(1)
    } finally {
      tx.close()
      managementService.shutdown()
    }
  }

  test("eager queries should be handled correctly") {
    val urls = csvUrls({
      writer =>
        writer.println("id,title,country,year")
        writer.println("1,Wall Street,USA,1987")
        writer.println("2,The American President,USA,1995")
        writer.println("3,The Shawshank Redemption,USA,1994")
    })
    for (url <- urls) {
      val query =
        s"""LOAD CSV WITH HEADERS FROM '$url' AS csvLine
           |MERGE (country:Country {name: csvLine.country})
           |CREATE (movie:Movie {id: toInteger(csvLine.id), title: csvLine.title, year:toInteger(csvLine.year)})
           |CREATE (movie)-[:MADE_IN]->(country)""".stripMargin
      executeSingle(query, Map.empty)
      resourceMonitor.assertClosedAndClear(1)

      //make sure three unique movies are created
      val result = executeWith(Configs.All, "match (m:Movie) return m.id AS id ORDER BY m.id").toList

      result should equal(List(Map("id" -> 1), Map("id" -> 2), Map("id" -> 3)))
      //empty database
      executeSingle("MATCH (n) DETACH DELETE n", Map.empty)
    }
  }

  test("should be able to use expression as url") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("'Foo'")
        writer.println("'Foo'")
        writer.println("'Foo'")
    }).cypherEscape
    val first = url.substring(0, url.length / 2)
    val second = url.substring(url.length / 2)
    createNode(Map("prop" -> second))

    val result = executeWith(Configs.InterpretedAndSlotted, s"MATCH (n) WITH n, '$first' as prefix LOAD CSV FROM prefix + n.prop AS line CREATE (a {name: line[0]}) RETURN a.name")
    resourceMonitor.assertClosedAndClear(1)
    assertStats(result, nodesCreated = 3, propertiesWritten = 3)
  }

  test("should not project too much when there is an aggregation on a with after load csv") {
    val url = createCSVTempFileURL({
      writer =>
        writer.println("10")
    }).cypherEscape
    val query  = s"""LOAD CSV FROM '$url' as row
                    |WITH row where row[0] = 10
                    |WITH distinct toInteger(row[0]) as data
                    |MERGE (c:City {data:data})
                    |RETURN count(*) as c""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    resourceMonitor.assertClosedAndClear(1)
    result.columnAs("c").toList should equal(List(0))
  }

  test("empty headers file should not throw") {
    val urls = csvUrls({ _ => {} })
    for (url <- urls) {
      val result = executeWith(Configs.InterpretedAndSlotted,
        s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN count(*)"
      )

      resourceMonitor.assertClosedAndClear(1)
      result.toList should equal(List(Map("count(*)" -> 0)))
    }
  }

  test("should give nice error message when overflowing the buffer") {
    runWithConfig(GraphDatabaseSettings.csv_buffer_size -> java.lang.Long.valueOf(1 * 1024 * 1024),
      GraphDatabaseInternalSettings.cypher_enable_runtime_monitors -> java.lang.Boolean.TRUE) { db =>

      trackResources(db)

      val longName  = "f"* 6000000
      val urls = csvUrls({
        writer =>
          writer.println("\"prop\"")
          writer.println(longName)
      })
      for (url <- urls) {
        //TODO this message should mention `dbms.import.csv.buffer_size` in 3.5
        val error = db.withTx( tx =>
          intercept[QueryExecutionException](tx.execute(
            s"""LOAD CSV WITH HEADERS FROM '$url' AS row
               |RETURN row.prop""".stripMargin).next().get("row.prop")))
        error.getMessage should startWith(
          """Tried to read a field larger than buffer size 1048576.""".stripMargin)
        resourceMonitor.assertClosedAndClear(1)
      }
    }
  }

  test("should be able to configure db to handle huge fields") {
    runWithConfig(GraphDatabaseSettings.csv_buffer_size -> java.lang.Long.valueOf(4 * 1024 * 1024),
      GraphDatabaseInternalSettings.cypher_enable_runtime_monitors -> java.lang.Boolean.TRUE)  { db =>

      trackResources(db)

      val longName  = "f"* 6000000
      val urls = csvUrls({
        writer =>
          writer.println("\"prop\"")
          writer.println(longName)
      })
      for (url <- urls) {
        db.withTx( tx => {
          val result = tx.execute(
            s"""LOAD CSV WITH HEADERS FROM '$url' AS row
               |RETURN row.prop""".stripMargin)
          result.next().get("row.prop") should equal(longName)
          resourceMonitor.assertClosedAndClear(1)
        })
      }
    }
  }

  test("periodic commit and distinct") {

    val urls = csvUrls({
      writer =>
       writer.println("foo")
       writer.println("bar")
       writer.println("baz")
    })
    for (url <- urls) {
      val query = s"USING PERIODIC COMMIT 2 LOAD CSV FROM '$url' AS row UNWIND [1, 2, 3, 1] AS i WITH DISTINCT i CREATE () RETURN i"

      executeSingle(query, Map.empty).toList should equal(List(Map("i" -> 1), Map("i" -> 2), Map("i" -> 3)))

      resourceMonitor.assertClosedAndClear(1)

      //empty database
      executeSingle("MATCH (n) DETACH DELETE n", Map.empty)
    }
  }

  test("periodic commit and top") {
    val urls = csvUrls({
      writer =>
        writer.println("foo")
        writer.println("bar")
        writer.println("baz")
    })
    for (url <- urls) {
      val query = s"USING PERIODIC COMMIT 2 LOAD CSV FROM '$url' AS row UNWIND [4, 2, 3, 1] AS i WITH i ORDER BY i LIMIT 9 CREATE () RETURN i"

      executeSingle(query, Map.empty).toList should equal(List(
        Map("i" -> 1), Map("i" -> 1), Map("i" -> 1),
        Map("i" -> 2), Map("i" -> 2), Map("i" -> 2),
        Map("i" -> 3), Map("i" -> 3), Map("i" -> 3)
      ))

      resourceMonitor.assertClosedAndClear(1)
      //empty database
      executeSingle("MATCH (n) DETACH DELETE n", Map.empty)
    }
  }

  test("periodic commit and grouped aggregation") {
    val urls = csvUrls({
      writer =>
        writer.println("foo")
        writer.println("bar")
        writer.println("baz")
    })
    for (url <- urls) {
      val query = s"USING PERIODIC COMMIT 2 LOAD CSV FROM '$url' AS row UNWIND [1, 2, 3] AS i CREATE () RETURN i, count(i)"

      executeSingle(query, Map.empty).toList should equal(List(
        Map("i" -> 1, "count(i)" -> 3), Map("i" -> 2, "count(i)" -> 3), Map("i" -> 3, "count(i)" -> 3)))

      resourceMonitor.assertClosedAndClear(1)

      //empty database
      executeSingle("MATCH (n) DETACH DELETE n", Map.empty)
    }
  }

  private def ensureNoIllegalCharsInWindowsFilePath(filename: String) = {
    // isWindows?
    if ('\\' == File.separatorChar) {
      // http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247%28v=vs.85%29.aspxs
      val illegalCharsInWindowsFilePath = "/?<>\\:*|\""
      // just replace the illegal chars with a 'a'
      illegalCharsInWindowsFilePath.foldLeft(filename)((current, c) => current.replace(c, 'a'))
    } else {
      filename
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

  private def acceptanceTestDatabaseBuilder = {
    new TestEnterpriseDatabaseManagementServiceBuilder(acceptanceDbFolder).impermanent()
      .setConfig(GraphDatabaseInternalSettings.cypher_enable_runtime_monitors, java.lang.Boolean.TRUE)
  }

  private def acceptanceDbFolder = {
    Path.of("target/test-data/acceptance-db")
  }
}
