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
class CreateNodePropertyExistenceConstraintCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating node property existence constraints */

  test("should create node property existence constraint (old syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_6ced8351")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_6ced8351")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create node property existence constraint (new syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS NOT NULL")

    // THEN

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_6ced8351")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_6ced8351")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named node property existence constraint (old syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT EXISTS (n.name)")

    // THEN

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("my_constraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named node property existence constraint (new syntax)") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT n.name IS NOT NULL")

    // THEN

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("my_constraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create node property existence constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_6ced8351")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_6ced8351")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named node property existence constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("myConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("myConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create node property existence constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT n.name IS NOT NULL")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("existingConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("existingConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create named node property existence constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT n.name IS NOT NULL")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL")
    val result2 = executeSingle("CREATE CONSTRAINT existingConstraint IF NOT EXISTS ON (n:Person) ASSERT n.age IS NOT NULL")
    val result3 = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT EXISTS(n.name)") // old syntax

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)
    assertStats(result2, existenceConstraintsAdded = 0)
    assertStats(result3, existenceConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("existingConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("existingConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create node property existence constraint on same schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraint("Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on the same schema as existing named node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint when existing node key constraint (diff name and same schema)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraint("Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on the same schema as existing named uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint when existing uniqueness constraint (diff name and same schema)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraint("Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on the same schema as existing named relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint when existing relationship property existence constraint (diff name and 'same' schema)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should not create node property existence constraint when existing node key constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create constraints when existing node key constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing uniqueness constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing uniqueness constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing relationship property existence constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing relationship property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should create node property existence constraint on same schema as existing node index") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on same schema as existing named node index") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on same schema as existing named relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should fail to create node property existence constraint with OR REPLACE") {
    val errorMessage = "Failed to create node property existence constraint: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint ON (n:Person) ASSERT n.name IS NOT NULL")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT ON (n:Person) ASSERT n.name IS NOT NULL")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT n.name IS NOT NULL")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create node property existence constraint with OPTIONS") {
    // WHEN
    val error = the[SyntaxException] thrownBy {
      executeSingle("CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT n.name IS NOT NULL OPTIONS {}")
    }
    // THEN
    error.getMessage should startWith ("Failed to create node property existence constraint: `OPTIONS` cannot be used together with this command.")
  }

  test("should fail to create multiple node property existence constraints with same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=1, name='constraint_f5a4058a', type='NODE PROPERTY EXISTENCE', schema=(:Label {prop}) )'."
  }

  test("should fail to create multiple named node property existence constraints with same name and schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=1, name='constraint', type='NODE PROPERTY EXISTENCE', schema=(:Label {prop}) )'."
  }

  test("should fail to create multiple named node property existence constraints with different name and same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "Constraint already exists: Constraint( id=1, name='constraint1', type='NODE PROPERTY EXISTENCE', schema=(:Label {prop}) )"
  }

  test("should fail to create multiple named node property existence constraints with same name") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop1) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node property existence constraint on same name and schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints with same name as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node property existence constraint on same name and schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node property existence constraint with same name as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node property existence constraint on same name and schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node property existence constraint with same name as existing relationship property existence constraint") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node property existence constraint with same name as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node property existence constraint with same name as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node property existence constraint with same name and schema as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node property existence constraint with same name as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node property existence constraint with same name as existing relationship property index with IF NOT EXISTS") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node property existence constraint with same name and schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }
}
