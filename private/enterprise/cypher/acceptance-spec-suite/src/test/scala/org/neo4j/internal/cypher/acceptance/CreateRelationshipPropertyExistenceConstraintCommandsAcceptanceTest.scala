/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.SyntaxException

//noinspection RedundantDefaultArgument
// Disable warnings for redundant default argument since its used for clarification of the `assertStats` when nothing should have happened
class CreateRelationshipPropertyExistenceConstraintCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating relationship property existence constraints */

  test("should create relationship property existence constraint (old syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

    // THEN

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("constraint_6c4e7adb")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("constraint_6c4e7adb")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should create relationship property existence constraint (new syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON ()-[r:HasPet]-() ASSERT r.since Is NOT NULL")

    // THEN

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("constraint_6c4e7adb")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("constraint_6c4e7adb")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should create named relationship property existence constraint (old syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

    // THEN

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("my_constraint")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("my_constraint")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should create named relationship property existence constraint (new syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")

    // THEN

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("my_constraint")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("my_constraint")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should create relationship property existence constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("constraint_6c4e7adb")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("constraint_6c4e7adb")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should create named relationship property existence constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("myConstraint")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("myConstraint")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should not create relationship property existence constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")
    val result2 = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)") // old syntax

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)
    assertStats(result2, existenceConstraintsAdded = 0)

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("existingConstraint")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("existingConstraint")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should not create named relationship property existence constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")
    val result2 = executeSingle("CREATE CONSTRAINT existingConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT r.age IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)
    assertStats(result2, existenceConstraintsAdded = 0)

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("existingConstraint")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("existingConstraint")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should create relationship property existence constraint on same schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraint("Type", "prop")

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on the same schema as existing named node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint1", "Type", "prop")

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint when existing node key constraint (diff name and same schema)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint1", "Type", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraint("Type", "prop")

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on the same schema as existing named uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint1", "Type", "prop")

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint when existing uniqueness constraint (diff name and same schema)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint1", "Type", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraint("Type", "prop")

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on the same schema as existing named node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint1", "Type", "prop")

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint when existing node property existence constraint (diff name and same schema)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint1", "Type", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should not create relationship property existence constraint when existing node key constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Type", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing node key constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Type", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing uniqueness constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Type", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing uniqueness constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Type", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing node property existence constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Type", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing node property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Type", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should create relationship property existence constraint on same schema as existing node index") {
    // GIVEN
    graph.createNodeIndex("Type", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndex("Type", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndex("Type", "prop")
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on same schema as existing named node index") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Type", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT my_constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on same schema as existing named relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Type", "prop")
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT my_constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should fail to create relationship property existence constraint with OR REPLACE") {
    val errorMessage = "Failed to create relationship property existence constraint: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create relationship property existence constraint with OPTIONS") {
    // WHEN
    val error = the[SyntaxException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON ()-[r:HasPet]-() ASSERT r.since IS NOT NULL OPTIONS {}")
    }
    // THEN
    error.getMessage should startWith ("Failed to create relationship property existence constraint: `OPTIONS` cannot be used together with this command.")
  }

  test("should fail to create multiple relationship property existence constraints with same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=1, name='constraint_3e723b4d', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type {prop}]- )'."
  }

  test("should fail to create multiple named relationship property existence constraints with same name and schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=1, name='constraint', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type {prop}]- )'."
  }

  test("should fail to create multiple named relationship property existence constraints with different name and same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint1 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "Constraint already exists: Constraint( id=1, name='constraint1', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type {prop}]- )"
  }

  test("should fail to create multiple named relationship property existence constraints with same name") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop1) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create relationship property existence constraint on same name and schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Type", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Type", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create relationship property existence constraint on same name and schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Type", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create relationship property existence constraint with same name as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Type", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create relationship property existence constraint on same name and schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Type", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Type", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Type", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Type", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create relationship property existence constraint with same name and schema as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Type", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create relationship property existence constraint with same name as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Type", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create relationship property existence constraint with same name as existing relationship property index with IF NOT EXISTS") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Type", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create relationship property existence constraint with same name and schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Type", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }
}
