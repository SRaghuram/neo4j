/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.exceptions.CypherExecutionException

//noinspection RedundantDefaultArgument
// Disable warnings for redundant default argument since its used for clarification of the `assertStats` when nothing should have happened
class DropSchemaCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for dropping indexes and constraints */

  // Drop index

  test("should drop index by schema") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop node index by name") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    executeSingle("DROP INDEX `index_5c0607ad`")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop relationship property index by name") {
    // GIVEN
    graph.createRelationshipIndex("Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")

    // WHEN
    executeSingle("DROP INDEX `index_1d6349b4`")

    // THEN
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named index by schema") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named node index by name") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX my_index")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named relationship property index by name") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX my_index")

    // THEN
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)
  }

  test("should drop node index by name if exists") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    val result = executeSingle("DROP INDEX `index_5c0607ad` IF EXISTS")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
    assertStats(result, indexesRemoved = 1)
  }

  test("should drop relationship property index by name if exists") {
    // GIVEN
    graph.createRelationshipIndex("Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")

    // WHEN
    val result = executeSingle("DROP INDEX `index_1d6349b4` IF EXISTS")

    // THEN
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)
    assertStats(result, indexesRemoved = 1)
  }

  test("should drop non-existent index by name if exists") {
    // WHEN
    val result = executeSingle("DROP INDEX `notexistint` IF EXISTS")

    // THEN
    assertStats(result, indexesRemoved = 0)
  }

  test("should get error when trying to drop the same index twice") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")
    executeSingle("DROP INDEX ON :Person(name)")
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on (:Person {name}). There is no such index."
  }

  test("should get error when trying to drop the same named node index twice") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("my_index")
    executeSingle("DROP INDEX my_index")
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  test("should get error when trying to drop the same named relationship property index twice") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("my_index")
    executeSingle("DROP INDEX my_index")
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  test("should get error when trying to drop non-existing index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on (:Person {name}). There is no such index."
  }

  test("should get error when trying to drop non-existing named index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  test("should fail to drop relationship property index by (node) schema") {
    // GIVEN
    graph.createRelationshipIndex("Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")

    // WHEN
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on (:Person {name}). There is no such index."

    // THEN
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")
  }

  test("should fail when dropping index when only constraint exists") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", "Label", "prop1")
    graph.createUniqueConstraintWithName("mine2", "Label", "prop2")
    graph.createNodeExistenceConstraintWithName("mine3", "Label", "prop3")
    graph.createRelationshipExistenceConstraintWithName("mine4", "Label", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint (backed by index)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Label(prop1)")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: (:Label {prop1})"

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine1")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: `mine1`"

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine1 IF EXISTS")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: `mine1`"

    // Uniqueness constraint (backed by index)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Label(prop2)")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: (:Label {prop2})"

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine2")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: `mine2`"

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine2 IF EXISTS")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: `mine2`"

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Label(prop3)")
      // THEN
    } should have message "Unable to drop index on (:Label {prop3}). There is no such index."

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine3")
      // THEN
    } should have message "Unable to drop index called `mine3`. There is no such index."

    // THEN no error
    val resN = executeSingle("DROP INDEX mine3 IF EXISTS")
    assertStats(resN, indexesRemoved = 0)

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Label(prop4)")
      // THEN
    } should have message "Unable to drop index on (:Label {prop4}). There is no such index."

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine4")
      // THEN
    } should have message "Unable to drop index called `mine4`. There is no such index."

    // THEN no error
    val resR = executeSingle("DROP INDEX mine4 IF EXISTS")
    assertStats(resR, indexesRemoved = 0)
  }

  // Drop constraint

  test("should drop node key constraint by schema") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_9b73711d")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop composite node key constraint by schema") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name", "age")
    graph.getNodeConstraint("Person", Seq("name", "age")).getName should be("constraint_c11599ca")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name, n.age) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name", "age")) should be(None)
  }

  test("should drop unique property constraint by schema") {
    // GIVEN
    graph.createUniqueConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_e26b1a8b")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop node property existence constraint by schema") {
    // GIVEN
    graph.createNodeExistenceConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_6ced8351")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop relationship property existence constraint by schema") {
    // GIVEN
    graph.createRelationshipExistenceConstraint("HasPet", "since")
    graph.getRelationshipConstraint("HasPet", "since").getName should be("constraint_6c4e7adb")

    // WHEN
    executeSingle("DROP CONSTRAINT ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

    // THEN
    graph.getMaybeRelationshipConstraint("HasPet", "since") should be(None)
  }

  test("should drop node key constraint by name") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_9b73711d")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_9b73711d`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop node key constraint by name if exists") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_9b73711d")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_9b73711d` IF EXISTS")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
  }

  test("should drop composite node key constraint by name") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name", "age")
    graph.getNodeConstraint("Person", Seq("name", "age")).getName should be("constraint_c11599ca")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_c11599ca`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name", "age")) should be(None)
  }

  test("should drop unique property constraint by name") {
    // GIVEN
    graph.createUniqueConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_e26b1a8b")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_e26b1a8b`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop unique property constraint by name if exists") {
    // GIVEN
    graph.createUniqueConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_e26b1a8b")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_e26b1a8b` IF EXISTS")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
  }

  test("should drop node property existence constraint by name") {
    // GIVEN
    graph.createNodeExistenceConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_6ced8351")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_6ced8351`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
  }

  test("should drop node property existence constraint by name if exists") {
    // GIVEN
    graph.createNodeExistenceConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_6ced8351")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_6ced8351` IF EXISTS")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
  }

  test("should drop relationship property existence constraint by name") {
    // GIVEN
    graph.createRelationshipExistenceConstraint("HasPet", "since")
    graph.getRelationshipConstraint("HasPet", "since").getName should be("constraint_6c4e7adb")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_6c4e7adb`")

    // THEN
    graph.getMaybeRelationshipConstraint("HasPet", "since") should be(None)
  }

  test("should drop relationship property existence constraint by name if exists") {
    // GIVEN
    graph.createRelationshipExistenceConstraint("HasPet", "since")
    graph.getRelationshipConstraint("HasPet", "since").getName should be("constraint_6c4e7adb")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_6c4e7adb` IF EXISTS")

    // THEN
    graph.getMaybeRelationshipConstraint("HasPet", "since") should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
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

  test("should do nothing when trying to drop non-existing constraint by name") {
    // WHEN
    val result = executeSingle("DROP CONSTRAINT myNonExistingConstraint IF EXISTS")

    // THEN
    assertStats(result, namedConstraintsRemoved = 0)
  }

  test("should get error when trying to drop the same constraint twice") {
    // GIVEN
    graph.createNodeKeyConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_9b73711d")
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."
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
    } should have message "Unable to drop constraint `my_constraint`: No such constraint my_constraint."

    // THEN no error
    executeSingle("DROP CONSTRAINT my_constraint IF EXISTS")
  }

  test("should get error when trying to drop non-existing constraint") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."
  }

  test("should get error when trying to drop non-existing named constraint") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT my_constraint")
      // THEN
    } should have message "Unable to drop constraint `my_constraint`: No such constraint my_constraint."
  }

  test("should be able to drop correct (node key) constraint by schema when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("nodeKey", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    graph.getConstraintSchemaByName("existence") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("nodeKey") should equal(("Label", Seq("prop")))
    } should have message "No constraint found with the name 'nodeKey'."
  }

  test("should be able to drop correct (existence) constraint by schema when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("nodeKey", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)")

    // THEN
    graph.getConstraintSchemaByName("nodeKey") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("existence") should equal(("Label", Seq("prop")))
    } should have message "No constraint found with the name 'existence'."
  }

  test("should be able to drop correct (node key) constraint by name when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("nodeKey", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")

    // WHEN
    executeSingle("DROP CONSTRAINT nodeKey")

    // THEN
    graph.getConstraintSchemaByName("existence") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("nodeKey")
    } should have message "No constraint found with the name 'nodeKey'."
  }

  test("should be able to drop correct (existence) constraint by name when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("nodeKey", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")

    // WHEN
    executeSingle("DROP CONSTRAINT existence")

    // THEN
    graph.getConstraintSchemaByName("nodeKey") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("existence")
    } should have message "No constraint found with the name 'existence'."
  }

  test("should be able to drop correct (uniqueness) constraint by schema when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName("uniqueness", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")
    graph.getNodeConstraint("Label", Seq("prop"))

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    graph.getConstraintSchemaByName("existence") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("uniqueness") should equal(("Label", Seq("prop")))
    } should have message "No constraint found with the name 'uniqueness'."
  }

  test("should be able to drop correct (existence) constraint by schema when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName("uniqueness", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")

    // WHEN
    executeSingle("DROP CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)")

    // THEN
    graph.getConstraintSchemaByName("uniqueness") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("existence") should equal(("Label", Seq("prop")))
    } should have message "No constraint found with the name 'existence'."
  }

  test("should be able to drop correct (uniqueness) constraint by name when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName("uniqueness", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")

    // WHEN
    executeSingle("DROP CONSTRAINT uniqueness")

    // THEN
    graph.getConstraintSchemaByName("existence") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("uniqueness")
    } should have message "No constraint found with the name 'uniqueness'."
  }

  test("should be able to drop correct (existence) constraint by name when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName("uniqueness", "Label", "prop")
    graph.createNodeExistenceConstraintWithName("existence", "Label", "prop")

    // WHEN
    executeSingle("DROP CONSTRAINT existence")

    // THEN
    graph.getConstraintSchemaByName("uniqueness") should equal(("Label", Seq("prop")))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName("existence")
    } should have message "No constraint found with the name 'existence'."
  }

  test("should fail when dropping constraint when only node index exists") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Person", "name")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON ()-[n:Person]-() ASSERT EXISTS (n.name)")
      // THEN
    } should have message "Unable to drop constraint on -[:Person {name}]-: No such constraint -[:Person {name}]-."

    // Drop by name
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT my_index")
      // THEN
    } should have message "Unable to drop constraint `my_index`: No such constraint my_index."

    // Drop by name IF EXISTS
    // THEN no error
    val res = executeSingle("DROP CONSTRAINT my_index IF EXISTS")
    assertStats(res, namedConstraintsRemoved = 0)
  }

  test("should fail when dropping constraint when only relationship property index exists") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Person", "name")
    graph.awaitIndexesOnline()

    // Node key constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."

    // Uniqueness constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."

    // Node property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")
      // THEN
    } should have message "Unable to drop constraint on (:Person {name}): No such constraint (:Person {name})."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON ()-[n:Person]-() ASSERT EXISTS (n.name)")
      // THEN
    } should have message "Unable to drop constraint on -[:Person {name}]-: No such constraint -[:Person {name}]-."

    // Drop by name
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT my_index")
      // THEN
    } should have message "Unable to drop constraint `my_index`: No such constraint my_index."

    // Drop by name IF EXISTS
    // THEN no error
    val res = executeSingle("DROP CONSTRAINT my_index IF EXISTS")
    assertStats(res, namedConstraintsRemoved = 0)
  }
}
