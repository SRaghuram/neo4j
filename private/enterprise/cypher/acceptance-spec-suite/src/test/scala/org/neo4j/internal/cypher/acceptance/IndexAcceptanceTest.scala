/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.concurrent.TimeUnit

import org.neo4j.cypher._
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.graphdb.Label
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

import scala.collection.JavaConverters._

class IndexAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  // Create index

  test("should create index") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name)")
    awaitIndexesOnline()

    // THEN

    // get by definition
    val index = graph.getIndex("Person", Seq("name"))
    index.getName should be("Index on :Person (name)")

    // get by name
    val (label, properties) = getIndexByName("Index on :Person (name)")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named index") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    awaitIndexesOnline()

    // THEN

    // get by definition
    val index = graph.getIndex("Person", Seq("name"))
    index.getName should be("my_index")

    // get by name
    val (label, properties) = getIndexByName("my_index")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should do nothing when trying to create multiple indexes with same definition") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name)")
    awaitIndexesOnline()

    // THEN: nothing happens
    executeSingle("CREATE INDEX ON :Person(name)")
  }

  test("should do nothing when trying to create multiple named indexes with same name and definition") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    awaitIndexesOnline()

    // THEN: nothing happens
    executeSingle("CREATE INDEX my_index ON :Person(name)")
  }

  test("should fail to create multiple named indexes with different names but same definition") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX your_index ON :Person(name)")
      // THEN
    } should have message "There already exists an index :Person(name)."
  }

  test("should fail to create multiple named indexes with same name") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index ON :Person(age)")
      // THEN
    } should have message "There already exists an index called 'my_index'."
  }

  // Drop index

  test("should drop index by schema") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("Index on :Person (name)")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    getIndexName("Person", Seq("name")) should be(None)
  }

  test("should drop index by name") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("Index on :Person (name)")

    // WHEN
    executeSingle("DROP INDEX `Index on :Person (name)`")

    // THEN
    getIndexName("Person", Seq("name")) should be(None)
  }

  test("should drop named index by schema") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    getIndexName("Person", Seq("name")) should be(None)
  }

  test("should drop named index by name") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX my_index")

    // THEN
    getIndexName("Person", Seq("name")) should be(None)
  }

  test("should get error when trying to drop the same index twice") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("Index on :Person (name)")
    executeSingle("DROP INDEX ON :Person(name)")
    getIndexName("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on :Person(name): No such INDEX ON :Person(name)."
  }

  test("should get error when trying to drop the same named index twice") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("my_index")
    executeSingle("DROP INDEX my_index")
    getIndexName("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "No such INDEX my_index." // TODO not as nice error message
  }

  test("should get error when trying to drop non-existing index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on :Person(name): No such INDEX ON :Person(name)."
  }

  test("should get error when trying to drop non-existing named index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "No such INDEX my_index." // TODO not as nice error message
  }

  private def awaitIndexesOnline(): Unit = {
    val tx = graph.getGraphDatabaseService.beginTx()
    try {
      graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
      tx.commit()
    } finally {
      tx.close()
    }
  }

  private def getIndexByName(name: String): (String, Seq[String]) = {
    val tx = graph.getGraphDatabaseService.beginTx()
    var label: String = null
    var properties: Seq[String] = null
    try {
      val index = graph.schema().getIndexByName(name)
      label = index.getLabel.name()
      properties = index.getPropertyKeys.asScala.toList
      tx.commit()
    } finally {
      tx.close()
    }
    (label, properties)
  }

  private def getIndexName(label: String, properties: Seq[String]): Option[String] = {
    val tx = graph.getGraphDatabaseService.beginTx()
    var name: Option[String] = None
    try {
      val maybeIndex = graph.schema().getIndexes(Label.label(label)).asScala.find(index => index.getPropertyKeys.asScala.toList == properties.toList)
      if (maybeIndex.isDefined) {
        name = Some(maybeIndex.get.getName)
      }
      tx.commit()
    } finally {
      tx.close()
    }
    name
  }
}
