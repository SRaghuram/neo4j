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
    executeSingle(s"CREATE CONSTRAINT ON (n:$stableEntity) ASSERT EXISTS (n.$stableProp)")

    // THEN

    // get by schema
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_2da781f0")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_2da781f0")
    label should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create node property existence constraint (new syntax)") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT ON (n:$stableEntity) ASSERT n.$stableProp IS NOT NULL")

    // THEN

    // get by schema
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_2da781f0")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_2da781f0")
    label should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create named node property existence constraint (old syntax)") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT EXISTS (n.$prop)")

    // THEN

    // get by schema
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // get by name
    val (actualLabel, properties) = graph.getConstraintSchemaByName(constraintName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should create named node property existence constraint (new syntax)") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT n.$prop IS NOT NULL")

    // THEN

    // get by schema
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // get by name
    val (actualLabel, properties) = graph.getConstraintSchemaByName(constraintName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should create node property existence constraint if not existing") {
    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT IF NOT EXISTS ON (n:$stableEntity) ASSERT n.$stableProp IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint(stableEntity, Seq(stableProp)).getName should be("constraint_2da781f0")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_2da781f0")
    label should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create named node property existence constraint if not existing") {
    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT n.$prop IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // get by name
    val (actualLabel, properties) = graph.getConstraintSchemaByName(constraintName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should not create node property existence constraint if already existing") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT n.$prop IS NOT NULL")

    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT IF NOT EXISTS ON (n:$label) ASSERT n.$prop IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // get by name
    val (actualLabel, properties) = graph.getConstraintSchemaByName(constraintName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should not create named node property existence constraint if already existing") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT n.$prop IS NOT NULL")

    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON (n:$label) ASSERT n.$prop IS NOT NULL")
    val result2 = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT n.$prop2 IS NOT NULL")
    val result3 = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON (n:$label) ASSERT EXISTS(n.$prop)") // old syntax

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)
    assertStats(result2, existenceConstraintsAdded = 0)
    assertStats(result3, existenceConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint(label, Seq(prop)).getName should be(constraintName)

    // get by name
    val (actualLabel, properties) = graph.getConstraintSchemaByName(constraintName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should create node property existence constraint on same schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraint(label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on the same schema as existing named node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint when existing node key constraint (diff name and same schema)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraint(label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on the same schema as existing named uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint when existing uniqueness constraint (diff name and same schema)") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraint(label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on the same schema as existing named relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint when existing relationship property existence constraint (diff name and 'same' schema)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should not create node property existence constraint when existing node key constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create constraints when existing node key constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing uniqueness constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing uniqueness constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing relationship property existence constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create node property existence constraint when existing relationship property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName(constraintName, label, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should create node property existence constraint on same schema as existing node index") {
    // GIVEN
    graph.createNodeIndex(label, prop)
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndex(label, prop)
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT IF NOT EXISTS ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create node property existence constraint on same schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndex(label, prop)
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on same schema as existing named node index") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, label, prop)
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named node property existence constraint on same schema as existing named relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName(indexName, label, prop)
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should fail to create node property existence constraint with OR REPLACE") {
    val errorMessage = "Failed to create node property existence constraint: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT $constraintName ON (n:$label) ASSERT n.$prop IS NOT NULL")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT ON (n:$label) ASSERT n.$prop IS NOT NULL")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT n.$prop IS NOT NULL")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (n:$label) ASSERT n.$prop IS NOT NULL")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create node property existence constraint with OPTIONS") {
    // WHEN
    val error = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT n.$prop IS NOT NULL OPTIONS {}")
    }
    // THEN
    error.getMessage should startWith ("Failed to create node property existence constraint: `OPTIONS` cannot be used together with this command.")
  }

  test("should fail to create multiple node property existence constraints with same schema") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT ON (n:$stableEntity) ASSERT (n.$stableProp) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT ON (n:$stableEntity) ASSERT (n.$stableProp) IS NOT NULL")
      // THEN
    } should have message s"An equivalent constraint already exists, 'Constraint( id=1, name='constraint_2da781f0', type='NODE PROPERTY EXISTENCE', schema=(:$stableEntity {$stableProp}) )'."
  }

  test("should fail to create multiple named node property existence constraints with same name and schema") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")
      // THEN
    } should have message s"An equivalent constraint already exists, 'Constraint( id=1, name='$constraintName', type='NODE PROPERTY EXISTENCE', schema=(:$label {$prop}) )'."
  }

  test("should fail to create multiple named node property existence constraints with different name and same schema") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName2 ON (n:$label) ASSERT (n.$prop) IS NOT NULL")
      // THEN
    } should have message s"Constraint already exists: Constraint( id=1, name='$constraintName', type='NODE PROPERTY EXISTENCE', schema=(:$label {$prop}) )"
  }

  test("should fail to create multiple named node property existence constraints with same name") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create node property existence constraint on same name and schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create constraints with same name as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, label, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create node property existence constraint on same name and schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, label, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create node property existence constraint on same name and schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName(constraintName, label, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name as existing relationship property existence constraint") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName(constraintName, label, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName(constraintName, label, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndexWithName(constraintName, label, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name and schema as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName(constraintName, label, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName(constraintName, label, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name as existing relationship property index with IF NOT EXISTS") {
    // GIVEN
    graph.createRelationshipIndexWithName(constraintName, label, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON (n:$label) ASSERT (n.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create node property existence constraint with same name and schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName(constraintName, label, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON (n:$label) ASSERT (n.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }
}
