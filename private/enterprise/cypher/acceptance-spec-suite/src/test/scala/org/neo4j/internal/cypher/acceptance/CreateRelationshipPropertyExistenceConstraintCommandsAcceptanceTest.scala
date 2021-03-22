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
    executeSingle(s"CREATE CONSTRAINT ON ()-[r:$stableEntity]-() ASSERT EXISTS (r.$stableProp)")

    // THEN

    // get by schema
    graph.getRelationshipConstraint(stableEntity, stableProp).getName should be("constraint_782743d3")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("constraint_782743d3")
    relType should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create relationship property existence constraint (new syntax)") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT ON ()-[r:$stableEntity]-() ASSERT r.$stableProp Is NOT NULL")

    // THEN

    // get by schema
    graph.getRelationshipConstraint(stableEntity, stableProp).getName should be("constraint_782743d3")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("constraint_782743d3")
    relType should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create named relationship property existence constraint (old syntax)") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT EXISTS (r.$prop)")

    // THEN

    // get by schema
    graph.getRelationshipConstraint(relType, prop).getName should be(constraintName)

    // get by name
    val (actualRelType, properties) = graph.getConstraintSchemaByName(constraintName)
    actualRelType should be(relType)
    properties should be(Seq(prop))
  }

  test("should create named relationship property existence constraint (new syntax)") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")

    // THEN

    // get by schema
    graph.getRelationshipConstraint(relType, prop).getName should be(constraintName)

    // get by name
    val (actualRelType, properties) = graph.getConstraintSchemaByName(constraintName)
    actualRelType should be(relType)
    properties should be(Seq(prop))
  }

  test("should create relationship property existence constraint if not existing") {
    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:$stableEntity]-() ASSERT r.$stableProp IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getRelationshipConstraint(stableEntity, stableProp).getName should be("constraint_782743d3")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("constraint_782743d3")
    relType should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create named relationship property existence constraint if not existing") {
    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 1)

    // get by schema
    graph.getRelationshipConstraint(relType, prop).getName should be(constraintName)

    // get by name
    val (actualRelType, properties) = graph.getConstraintSchemaByName(constraintName)
    actualRelType should be(relType)
    properties should be(Seq(prop))
  }

  test("should not create relationship property existence constraint if already existing") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")

    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")
    val result2 = executeSingle(s"CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:$relType]-() ASSERT EXISTS (r.$prop)") // old syntax

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)
    assertStats(result2, existenceConstraintsAdded = 0)

    // get by schema
    graph.getRelationshipConstraint(relType, prop).getName should be(constraintName)

    // get by name
    val (actualRelType, properties) = graph.getConstraintSchemaByName(constraintName)
    actualRelType should be(relType)
    properties should be(Seq(prop))
  }

  test("should not create named relationship property existence constraint if already existing") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")

    // WHEN
    val result = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")
    val result2 = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT r.$prop2 IS NOT NULL")

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)
    assertStats(result2, existenceConstraintsAdded = 0)

    // get by schema
    graph.getRelationshipConstraint(relType, prop).getName should be(constraintName)

    // get by name
    val (actualRelType, properties) = graph.getConstraintSchemaByName(constraintName)
    actualRelType should be(relType)
    properties should be(Seq(prop))
  }

  test("should create relationship property existence constraint on same schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraint(relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on the same schema as existing named node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint when existing node key constraint (diff name and same schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraint(relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on the same schema as existing named uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint when existing uniqueness constraint (diff name and same schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraint(relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on the same schema as existing named node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint when existing node property existence constraint (diff name and same schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName2 IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should not create relationship property existence constraint when existing node key constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing node key constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, relType, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing uniqueness constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing uniqueness constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, relType, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing node property existence constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, relType, prop)

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should not create relationship property existence constraint when existing node property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, relType, prop)

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 0)
  }

  test("should create relationship property existence constraint on same schema as existing node index") {
    // GIVEN
    graph.createNodeIndex(relType, prop)
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndex(relType, prop)
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create relationship property existence constraint on same schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndex(relType, prop)
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on same schema as existing named node index") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, relType, prop)
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should create named relationship property existence constraint on same schema as existing named relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName(indexName, relType, prop)
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    // THEN
    assertStats(res, existenceConstraintsAdded = 1)
  }

  test("should fail to create relationship property existence constraint with OR REPLACE") {
    val errorMessage = "Failed to create relationship property existence constraint: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create relationship property existence constraint with OPTIONS") {
    // WHEN
    val error = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON ()-[r:$relType]-() ASSERT r.$prop IS NOT NULL OPTIONS {}")
    }
    // THEN
    error.getMessage should startWith ("Failed to create relationship property existence constraint: `OPTIONS` cannot be used together with this command.")
  }

  test("should fail to create multiple relationship property existence constraints with same schema") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT ON ()-[r:$stableEntity]-() ASSERT (r.$stableProp) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT ON ()-[r:$stableEntity]-() ASSERT (r.$stableProp) IS NOT NULL")
      // THEN
    } should have message s"An equivalent constraint already exists, 'Constraint( id=1, name='constraint_782743d3', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:$stableEntity {$stableProp}]- )'."
  }

  test("should fail to create multiple named relationship property existence constraints with same name and schema") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")
      // THEN
    } should have message s"An equivalent constraint already exists, 'Constraint( id=1, name='$constraintName', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:$relType {$prop}]- )'."
  }

  test("should fail to create multiple named relationship property existence constraints with different name and same schema") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName2 ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")
      // THEN
    } should have message s"Constraint already exists: Constraint( id=1, name='$constraintName', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:$relType {$prop}]- )"
  }

  test("should fail to create multiple named relationship property existence constraints with same name") {
    // GIVEN
    executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint on same name and schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, relType, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName(constraintName, relType, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint on same name and schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, relType, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName(constraintName, relType, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint on same name and schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, relType, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName(constraintName, relType, prop)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists a constraint called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName(constraintName, relType, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndexWithName(constraintName, relType, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name and schema as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName(constraintName, relType, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName(constraintName, relType, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name as existing relationship property index with IF NOT EXISTS") {
    // GIVEN
    graph.createRelationshipIndexWithName(constraintName, relType, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE CONSTRAINT $constraintName IF NOT EXISTS ON ()-[r:$relType]-() ASSERT (r.$prop2) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }

  test("should fail to create relationship property existence constraint with same name and schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName(constraintName, relType, prop)
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN (close as can get to same schema)
      executeSingle(s"CREATE CONSTRAINT $constraintName ON ()-[r:$relType]-() ASSERT (r.$prop) IS NOT NULL")
      // THEN
    } should have message s"There already exists an index called '$constraintName'."
  }
}
