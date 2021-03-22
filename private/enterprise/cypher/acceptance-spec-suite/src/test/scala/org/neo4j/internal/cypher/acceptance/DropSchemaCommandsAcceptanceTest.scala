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
    graph.createNodeIndex(label, prop)
    graph.getMaybeNodeIndex(label, Seq(prop)).isDefined should be(true)

    // WHEN
    executeSingle(s"DROP INDEX ON :$label($prop)")

    // THEN
    graph.getMaybeNodeIndex(label, Seq(prop)) should be(None)
  }

  test("should drop node index by name") {
    // GIVEN
    graph.createNodeIndex(stableEntity, stableProp)
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")

    // WHEN
    executeSingle("DROP INDEX `index_5ac9113e`")

    // THEN
    graph.getMaybeNodeIndex(stableEntity, Seq(stableProp)) should be(None)
  }

  test("should drop relationship property index by name") {
    // GIVEN
    graph.createRelationshipIndex(stableEntity, stableProp)
    graph.getRelIndex(stableEntity, Seq(stableProp)).getName should be("index_a84ff04f")

    // WHEN
    executeSingle("DROP INDEX `index_a84ff04f`")

    // THEN
    graph.getMaybeRelIndex(stableEntity, Seq(stableProp)) should be(None)
  }

  test("should drop named index by schema") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, label, prop)
    graph.getNodeIndex(label, Seq(prop)).getName should be(indexName)

    // WHEN
    executeSingle(s"DROP INDEX ON :$label($prop)")

    // THEN
    graph.getMaybeNodeIndex(label, Seq(prop)) should be(None)
  }

  test("should drop named node index by name") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, label, prop)
    graph.getNodeIndex(label, Seq(prop)).getName should be(indexName)

    // WHEN
    executeSingle(s"DROP INDEX $indexName")

    // THEN
    graph.getMaybeNodeIndex(label, Seq(prop)) should be(None)
  }

  test("should drop named relationship property index by name") {
    // GIVEN
    graph.createRelationshipIndexWithName(indexName, label, prop)
    graph.getRelIndex(label, Seq(prop)).getName should be(indexName)

    // WHEN
    executeSingle(s"DROP INDEX $indexName")

    // THEN
    graph.getMaybeRelIndex(label, Seq(prop)) should be(None)
  }

  test("should drop node index by name if exists") {
    // GIVEN
    graph.createNodeIndex(stableEntity, stableProp)
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")

    // WHEN
    val result = executeSingle("DROP INDEX `index_5ac9113e` IF EXISTS")

    // THEN
    graph.getMaybeNodeIndex(stableEntity, Seq(stableProp)) should be(None)
    assertStats(result, indexesRemoved = 1)
  }

  test("should drop relationship property index by name if exists") {
    // GIVEN
    graph.createRelationshipIndex(stableEntity, stableProp)
    graph.getRelIndex(stableEntity, Seq(stableProp)).getName should be("index_a84ff04f")

    // WHEN
    val result = executeSingle("DROP INDEX `index_a84ff04f` IF EXISTS")

    // THEN
    graph.getMaybeRelIndex(stableEntity, Seq(stableProp)) should be(None)
    assertStats(result, indexesRemoved = 1)
  }

  test("should drop non-existent index by name if exists") {
    // WHEN
    val result = executeSingle("DROP INDEX `nonExistingIndex` IF EXISTS")

    // THEN
    assertStats(result, indexesRemoved = 0)
  }

  test("should get error when trying to drop the same index twice") {
    // GIVEN
    graph.createNodeIndex(label, prop)
    graph.getMaybeNodeIndex(label, Seq(prop)).isDefined should be(true)
    executeSingle(s"DROP INDEX ON :$label($prop)")
    graph.getMaybeNodeIndex(label, Seq(prop)).isDefined should be(false)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP INDEX ON :$label($prop)")
      // THEN
    } should have message s"Unable to drop index on (:$label {$prop}). There is no such index."
  }

  test("should get error when trying to drop the same named node index twice") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, label, prop)
    graph.getNodeIndex(label, Seq(prop)).getName should be(indexName)
    executeSingle(s"DROP INDEX $indexName")
    graph.getMaybeNodeIndex(label, Seq(prop)) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP INDEX $indexName")
      // THEN
    } should have message s"Unable to drop index called `$indexName`. There is no such index."
  }

  test("should get error when trying to drop the same named relationship property index twice") {
    // GIVEN
    graph.createRelationshipIndexWithName(indexName, label, prop)
    graph.getRelIndex(label, Seq(prop)).getName should be(indexName)
    executeSingle(s"DROP INDEX $indexName")
    graph.getMaybeRelIndex(label, Seq(prop)) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP INDEX $indexName")
      // THEN
    } should have message s"Unable to drop index called `$indexName`. There is no such index."
  }

  test("should get error when trying to drop non-existing index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP INDEX ON :$label($prop)")
      // THEN
    } should have message s"Unable to drop index on (:$label {$prop}). There is no such index."
  }

  test("should get error when trying to drop non-existing named index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX nonExistingIndex")
      // THEN
    } should have message "Unable to drop index called `nonExistingIndex`. There is no such index."
  }

  test("should fail to drop relationship property index by (node) schema") {
    // GIVEN
    graph.createRelationshipIndex(label, prop)
    graph.getMaybeRelIndex(label, Seq(prop)).isDefined should be(true)

    // WHEN
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP INDEX ON :$label($prop)")
      // THEN
    } should have message s"Unable to drop index on (:$label {$prop}). There is no such index."

    // THEN
    graph.getMaybeRelIndex(label, Seq(prop)).isDefined should be(true)
  }

  test("should fail when dropping index when only constraint exists") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", label, prop)
    graph.createUniqueConstraintWithName("mine2", label, prop2)
    graph.createNodeExistenceConstraintWithName("mine3", label, prop3)
    graph.createRelationshipExistenceConstraintWithName("mine4", label, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint (backed by index)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP INDEX ON :$label($prop)")
      // THEN
    } should have message s"Unable to drop index: Index belongs to constraint: (:$label {$prop})"

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
      executeSingle(s"DROP INDEX ON :$label($prop2)")
      // THEN
    } should have message s"Unable to drop index: Index belongs to constraint: (:$label {$prop2})"

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
      executeSingle(s"DROP INDEX ON :$label($prop3)")
      // THEN
    } should have message s"Unable to drop index on (:$label {$prop3}). There is no such index."

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
      executeSingle(s"DROP INDEX ON :$label($prop4)")
      // THEN
    } should have message s"Unable to drop index on (:$label {$prop4}). There is no such index."

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
    graph.createNodeKeyConstraint(label, prop)
    graph.getMaybeNodeConstraint(label, Seq(prop)).isDefined should be(true)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint(label, Seq(prop)) should be(None)
  }

  test("should drop composite node key constraint by schema") {
    // GIVEN
    graph.createNodeKeyConstraint(label, prop, prop2)
    graph.getMaybeNodeConstraint(label, Seq(prop, prop2)).isDefined should be(true)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop, n.$prop2) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint(label, Seq(prop, prop2)) should be(None)
  }

  test("should drop unique property constraint by schema") {
    // GIVEN
    graph.createUniqueConstraint(label, prop)
    graph.getMaybeNodeConstraint(label, Seq(prop)).isDefined should be(true)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS UNIQUE")

    // THEN
    graph.getMaybeNodeConstraint(label, Seq(prop)) should be(None)
  }

  test("should drop node property existence constraint by schema") {
    // GIVEN
    graph.createNodeExistenceConstraint(label, prop)
    graph.getMaybeNodeConstraint(label, Seq(prop)).isDefined should be(true)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$prop)")

    // THEN
    graph.getMaybeNodeConstraint(label, Seq(prop)) should be(None)
  }

  test("should drop relationship property existence constraint by schema") {
    // GIVEN
    graph.createRelationshipExistenceConstraint(relType, prop)
    graph.getMaybeRelationshipConstraint(relType, prop).isDefined should be(true)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON ()-[r:$relType]-() ASSERT EXISTS (r.$prop)")

    // THEN
    graph.getMaybeRelationshipConstraint(relType, prop) should be(None)
  }

  test("should drop node key constraint by name") {
    // GIVEN
    graph.createNodeKeyConstraint(stableEntity, stableProp)
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_6127a33a")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_6127a33a`")

    // THEN
    graph.getMaybeNodeConstraint(stableEntity, Seq(stableProp)) should be(None)
  }

  test("should drop node key constraint by name if exists") {
    // GIVEN
    graph.createNodeKeyConstraint(stableEntity, stableProp)
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_6127a33a")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_6127a33a` IF EXISTS")

    // THEN
    graph.getMaybeNodeConstraint(stableEntity, Seq(stableProp)) should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
  }

  test("should drop composite node key constraint by name") {
    // GIVEN
    graph.createNodeKeyConstraint(stableEntity, stableProp, stableProp2)
    graph.getNodeConstraint(stableEntity, Seq(stableProp, stableProp2)).getName should be("constraint_53005a54")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_53005a54`")

    // THEN
    graph.getMaybeNodeConstraint(stableEntity, Seq(stableProp, stableProp2)) should be(None)
  }

  test("should drop unique property constraint by name") {
    // GIVEN
    graph.createUniqueConstraint(stableEntity, stableProp)
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_f454d6c5")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_f454d6c5`")

    // THEN
    graph.getMaybeNodeConstraint(stableEntity, Seq(stableProp)) should be(None)
  }

  test("should drop unique property constraint by name if exists") {
    // GIVEN
    graph.createUniqueConstraint(stableEntity, stableProp)
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_f454d6c5")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_f454d6c5` IF EXISTS")

    // THEN
    graph.getMaybeNodeConstraint(stableEntity, Seq(stableProp)) should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
  }

  test("should drop node property existence constraint by name") {
    // GIVEN
    graph.createNodeExistenceConstraint(stableEntity, stableProp)
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_2da781f0")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_2da781f0`")

    // THEN
    graph.getMaybeNodeConstraint(stableEntity, Seq(stableProp)) should be(None)
  }

  test("should drop node property existence constraint by name if exists") {
    // GIVEN
    graph.createNodeExistenceConstraint(stableEntity, stableProp)
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_2da781f0")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_2da781f0` IF EXISTS")

    // THEN
    graph.getMaybeNodeConstraint(stableEntity, Seq(stableProp)) should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
  }

  test("should drop relationship property existence constraint by name") {
    // GIVEN
    graph.createRelationshipExistenceConstraint(stableEntity, stableProp)
    graph.getRelationshipConstraint(stableEntity, stableProp).getName should be("constraint_782743d3")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_782743d3`")

    // THEN
    graph.getMaybeRelationshipConstraint(stableEntity, stableProp) should be(None)
  }

  test("should drop relationship property existence constraint by name if exists") {
    // GIVEN
    graph.createRelationshipExistenceConstraint(stableEntity, stableProp)
    graph.getRelationshipConstraint(stableEntity, stableProp).getName should be("constraint_782743d3")

    // WHEN
    val result = executeSingle("DROP CONSTRAINT `constraint_782743d3` IF EXISTS")

    // THEN
    graph.getMaybeRelationshipConstraint(stableEntity, stableProp) should be(None)
    assertStats(result, namedConstraintsRemoved = 1)
  }

  test("should drop named node key constraint by schema") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY")

    // THEN
    graph.getMaybeNodeConstraint(label, Seq(prop)) should be(None)
  }

  test("should drop named node property existence constraint by schema") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, label, prop)
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$prop)")

    // THEN
    graph.getMaybeNodeConstraint(label, Seq(prop)) should be(None)
  }

  test("should drop named unique property constraint by name") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // WHEN
    executeSingle(s"DROP CONSTRAINT $constraintName")

    // THEN
    graph.getMaybeNodeConstraint(label, Seq(prop)) should be(None)
  }

  test("should drop named relationship property existence constraint by name") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName(constraintName, relType, prop)
    graph.getRelationshipConstraint(relType, prop).getName should be(constraintName)

    // WHEN
    executeSingle(s"DROP CONSTRAINT $constraintName")

    // THEN
    graph.getMaybeRelationshipConstraint(relType, prop) should be(None)
  }

  test("should do nothing when trying to drop non-existing constraint by name") {
    // WHEN
    val result = executeSingle("DROP CONSTRAINT nonExistingConstraint IF EXISTS")

    // THEN
    assertStats(result, namedConstraintsRemoved = 0)
  }

  test("should get error when trying to drop the same constraint twice") {
    // GIVEN
    graph.createNodeKeyConstraint(label, prop)
    graph.getMaybeNodeConstraint(label, Seq(prop)).isDefined should be(true)
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY")
    graph.getMaybeNodeConstraint(label, Seq(prop)).isDefined should be(false)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."
  }

  test("should get error when trying to drop the same named constraint twice") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, label, prop)
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)
    executeSingle(s"DROP CONSTRAINT $constraintName")
    graph.getMaybeNodeConstraint(label, Seq(prop)) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT $constraintName")
      // THEN
    } should have message s"Unable to drop constraint `$constraintName`: No such constraint $constraintName."

    // THEN no error
    executeSingle(s"DROP CONSTRAINT $constraintName IF EXISTS")
  }

  test("should get error when trying to drop non-existing constraint") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS UNIQUE")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."
  }

  test("should get error when trying to drop non-existing named constraint") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT nonExistingConstraint")
      // THEN
    } should have message "Unable to drop constraint `nonExistingConstraint`: No such constraint nonExistingConstraint."
  }

  test("should be able to drop correct (node key) constraint by schema when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY")

    // THEN
    graph.getConstraintSchemaByName(constraintName2) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName) should equal((label, Seq(prop)))
    } should have message s"No constraint found with the name '$constraintName'."
  }

  test("should be able to drop correct (existence) constraint by schema when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$prop)")

    // THEN
    graph.getConstraintSchemaByName(constraintName) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName2) should equal((label, Seq(prop)))
    } should have message s"No constraint found with the name '$constraintName2'."
  }

  test("should be able to drop correct (node key) constraint by name when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)

    // WHEN
    executeSingle(s"DROP CONSTRAINT $constraintName")

    // THEN
    graph.getConstraintSchemaByName(constraintName2) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName)
    } should have message s"No constraint found with the name '$constraintName'."
  }

  test("should be able to drop correct (existence) constraint by name when overlapping") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)

    // WHEN
    executeSingle(s"DROP CONSTRAINT $constraintName2")

    // THEN
    graph.getConstraintSchemaByName(constraintName) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName2)
    } should have message s"No constraint found with the name '$constraintName2'."
  }

  test("should be able to drop correct (uniqueness) constraint by schema when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)
    graph.getNodeConstraint(label, Seq(prop))

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS UNIQUE")

    // THEN
    graph.getConstraintSchemaByName(constraintName2) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName) should equal((label, Seq(prop)))
    } should have message s"No constraint found with the name '$constraintName'."
  }

  test("should be able to drop correct (existence) constraint by schema when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)

    // WHEN
    executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$prop)")

    // THEN
    graph.getConstraintSchemaByName(constraintName) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName2) should equal((label, Seq(prop)))
    } should have message s"No constraint found with the name '$constraintName2'."
  }

  test("should be able to drop correct (uniqueness) constraint by name when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)

    // WHEN
    executeSingle(s"DROP CONSTRAINT $constraintName")

    // THEN
    graph.getConstraintSchemaByName(constraintName2) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName)
    } should have message s"No constraint found with the name '$constraintName'."
  }

  test("should be able to drop correct (existence) constraint by name when not overlapping") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)
    graph.createNodeExistenceConstraintWithName(constraintName2, label, prop)

    // WHEN
    executeSingle(s"DROP CONSTRAINT $constraintName2")

    // THEN
    graph.getConstraintSchemaByName(constraintName) should equal((label, Seq(prop)))
    the[IllegalArgumentException] thrownBy {
      graph.getConstraintSchemaByName(constraintName2)
    } should have message s"No constraint found with the name '$constraintName2'."
  }

  test("should fail when dropping constraint when only node index exists") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, label, prop)
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS UNIQUE")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$prop)")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON ()-[n:$label]-() ASSERT EXISTS (n.$prop)")
      // THEN
    } should have message s"Unable to drop constraint on -[:$label {$prop}]-: No such constraint -[:$label {$prop}]-."

    // Drop by name
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT $indexName")
      // THEN
    } should have message s"Unable to drop constraint `$indexName`: No such constraint $indexName."

    // Drop by name IF EXISTS
    // THEN no error
    val res = executeSingle(s"DROP CONSTRAINT $indexName IF EXISTS")
    assertStats(res, namedConstraintsRemoved = 0)
  }

  test("should fail when dropping constraint when only relationship property index exists") {
    // GIVEN
    graph.createRelationshipIndexWithName(indexName, label, prop)
    graph.awaitIndexesOnline()

    // Node key constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."

    // Uniqueness constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS UNIQUE")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."

    // Node property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON (n:$label) ASSERT EXISTS (n.$prop)")
      // THEN
    } should have message s"Unable to drop constraint on (:$label {$prop}): No such constraint (:$label {$prop})."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT ON ()-[n:$label]-() ASSERT EXISTS (n.$prop)")
      // THEN
    } should have message s"Unable to drop constraint on -[:$label {$prop}]-: No such constraint -[:$label {$prop}]-."

    // Drop by name
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"DROP CONSTRAINT $indexName")
      // THEN
    } should have message s"Unable to drop constraint `$indexName`: No such constraint $indexName."

    // Drop by name IF EXISTS
    // THEN no error
    val res = executeSingle(s"DROP CONSTRAINT $indexName IF EXISTS")
    assertStats(res, namedConstraintsRemoved = 0)
  }
}
