/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

class IndexAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  // Create index

  test("should create index") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getIndex("Person", Seq("name")).getName should be("Index on :Person (name)")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("Index on :Person (name)")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named index") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getIndex("Person", Seq("name")).getName should be("my_index")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("my_index")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should do nothing when trying to create multiple indexes with same definition") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    // THEN: nothing happens
    executeSingle("CREATE INDEX ON :Person(name)")
  }

  test("should do nothing when trying to create multiple named indexes with same name and definition") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    graph.awaitIndexesOnline()

    // THEN: nothing happens
    executeSingle("CREATE INDEX my_index ON :Person(name)")
  }

  test("should fail to create multiple named indexes with different names but same definition") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX your_index ON :Person(name)")
      // THEN
    } should have message "There already exists an index :Person(name)."
  }

  test("should fail to create multiple named indexes with same name") {
    // WHEN
    executeSingle("CREATE INDEX my_index ON :Person(name)")
    graph.awaitIndexesOnline()

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
    graph.getMaybeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop index by name") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("Index on :Person (name)")

    // WHEN
    executeSingle("DROP INDEX `Index on :Person (name)`")

    // THEN
    graph.getMaybeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named index by schema") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    graph.getMaybeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named index by name") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX my_index")

    // THEN
    graph.getMaybeIndex("Person", Seq("name")) should be(None)
  }

  test("should get error when trying to drop the same index twice") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("Index on :Person (name)")
    executeSingle("DROP INDEX ON :Person(name)")
    graph.getMaybeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on :Person(name): No such index :Person(name)."
  }

  test("should get error when trying to drop the same named index twice") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("my_index")
    executeSingle("DROP INDEX my_index")
    graph.getMaybeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "No such index my_index." // TODO not as nice error message
  }

  test("should get error when trying to drop non-existing index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on :Person(name): No such index :Person(name)."
  }

  test("should get error when trying to drop non-existing named index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "No such index my_index." // TODO not as nice error message
  }

  // Create constraint

  test("should create node key constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Node key constraint on :Person (name)")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("Node key constraint on :Person (name)")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create composite node key constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name, n.age) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name", "age")).getName should be("Node key constraint on :Person (name,age)")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("Node key constraint on :Person (name,age)")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should create unique property constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Uniqueness constraint on :Person (name)")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("Uniqueness constraint on :Person (name)")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create node property existence constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Property existence constraint on :Person (name)")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("Property existence constraint on :Person (name)")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create relationship property existence constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON ()-[r:HasPet]-()  ASSERT EXISTS (r.since)")

    // THEN

    // get by definition
    graph.getRelationshipConstraint("HasPet", "since").getName should be("Property existence constraint on ()-[:HasPet]-() (since)")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("Property existence constraint on ()-[:HasPet]-() (since)")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should create named node key constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("my_constraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named composite node key constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.name, n.age) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name", "age")).getName should be("my_constraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("my_constraint")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should create named unique property constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("my_constraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named node property existence constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN

    // get by definition
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("my_constraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named relationship property existence constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON ()-[r:HasPet]-()  ASSERT EXISTS (r.since)")

    // THEN

    // get by definition
    graph.getRelationshipConstraint("HasPet", "since").getName should be("my_constraint")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("my_constraint")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should do nothing when trying to create multiple constraints with same definition") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN: nothing happens
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")
  }

  test("should do nothing when trying to create multiple named constraints with same name and definition") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN: nothing happens
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT EXISTS (n.name)")
  }

  test("should fail to create multiple named constraints with different name and same definition") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT EXISTS (n.name)")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT your_constraint ON (n:Person) ASSERT EXISTS (n.name)")
      // THEN
    } should have message "Constraint already exists: CONSTRAINT ON ( person:Person ) ASSERT exists(person.name)"
  }

  test("should fail to create multiple named constraints with same name") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT EXISTS (n.name)")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT EXISTS (n.age)")
      // THEN
    } should have message "There already exists a constraint called 'my_constraint'."
  }

  // Drop constraint

  test("should drop node key constraint by schema") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Node key constraint on :Person (name)")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop composite node key constraint by schema") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name", "age")
    graph.getNodeConstraint("Person", Seq("name", "age")).getName should be("Node key constraint on :Person (name,age)")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name, n.age) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name", "age")) should be(None)
  }

  test("should drop unique property constraint by schema") {
    // GIVEN
    graph.createUniqueConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Uniqueness constraint on :Person (name)")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop node property existence constraint by schema") {
    // GIVEN
    graph.createNodeExistenceConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Property existence constraint on :Person (name)")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop relationship property existence constraint by schema") {
    // GIVEN
    graph.createRelationshipExistenceConstraint("HasPet", "since")
    graph.getRelationshipConstraint("HasPet", "since").getName should be("Property existence constraint on ()-[:HasPet]-() (since)")

    // WHEN
    executeSingle("DROP CONSTRAINT ON ()-[r:HasPet]-()  ASSERT EXISTS (r.since)")

    // THEN
    graph.getMaybeRelationshipConstraint("HasPet", "since") should be(None)
  }

  test("should drop node key constraint by name") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Node key constraint on :Person (name)")

    // WHEN
    executeSingle("DROP CONSTRAINT `Node key constraint on :Person (name)`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop composite node key constraint by name") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name", "age")
    graph.getNodeConstraint("Person", Seq("name", "age")).getName should be("Node key constraint on :Person (name,age)")

    // WHEN
    executeSingle("DROP CONSTRAINT `Node key constraint on :Person (name,age)`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name", "age")) should be(None)
  }

  test("should drop unique property constraint by name") {
    // GIVEN
    graph.createUniqueConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Uniqueness constraint on :Person (name)")

    // WHEN
    executeSingle("DROP CONSTRAINT `Uniqueness constraint on :Person (name)`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop node property existence constraint by name") {
    // GIVEN
    graph.createNodeExistenceConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Property existence constraint on :Person (name)")

    // WHEN
    executeSingle("DROP CONSTRAINT `Property existence constraint on :Person (name)`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop relationship property existence constraint by name") {
    // GIVEN
    graph.createRelationshipExistenceConstraint("HasPet", "since")
    graph.getRelationshipConstraint("HasPet", "since").getName should be("Property existence constraint on ()-[:HasPet]-() (since)")

    // WHEN
    executeSingle("DROP CONSTRAINT `Property existence constraint on ()-[:HasPet]-() (since)`")

    // THEN
    graph.getMaybeRelationshipConstraint("HasPet", "since") should be(None)
  }

  test("should drop named node key constraint by schema") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("my_constraint", "Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop named node property existence constraint by schema") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("my_constraint", "Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop named unique property constraint by name") {
    // GIVEN
    graph.createUniqueConstraintWithName("my_constraint", "Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // WHEN
    executeSingle("DROP CONSTRAINT my_constraint")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop named relationship property existence constraint by name") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("my_constraint", "HasPet", "since")
    graph.getRelationshipConstraint("HasPet", "since").getName should be("my_constraint")

    // WHEN
    executeSingle("DROP CONSTRAINT my_constraint")

    // THEN
    graph.getMaybeRelationshipConstraint("HasPet", "since") should be(None)
  }

  test("should get error when trying to drop the same constraint twice") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("Node key constraint on :Person (name)")
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
      // THEN
    } should have message "No such constraint :Person(name)." // TODO not as nice error message as for dropping non-existing index
  }

  test("should get error when trying to drop the same named constraint twice") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("my_constraint", "Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")
    executeSingle("DROP CONSTRAINT my_constraint")
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT my_constraint")
      // THEN
    } should have message "No such constraint my_constraint." // TODO not as nice error message as for dropping non-existing index
  }

  test("should get error when trying to drop non-existing constraint") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
      // THEN
    } should have message "No such constraint :Person(name)." // TODO not as nice error message as for dropping non-existing index
  }

  test("should get error when trying to drop non-existing named constraint") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT my_constraint")
      // THEN
    } should have message "No such constraint my_constraint." // TODO not as nice error message as for dropping non-existing index
  }
}
