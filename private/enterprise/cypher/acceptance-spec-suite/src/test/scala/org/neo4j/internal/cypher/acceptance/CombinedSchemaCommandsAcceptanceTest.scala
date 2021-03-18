/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

//noinspection RedundantDefaultArgument
// Disable warnings for redundant default argument since its used for clarification of the `assertStats` when nothing should have happened
class CombinedSchemaCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating and dropping combinations of indexes and constraints */

  test("should create unrelated indexes and constraints") {

    // WHEN
    executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop0)")
    executeSingle("CREATE INDEX index1 FOR (n:Label) ON (n.namedProp0)")
    executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop1)")
    executeSingle("CREATE INDEX index2 FOR ()-[r:Type]-() ON (r.namedProp1)")
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label) ASSERT (n.namedProp2) IS NODE KEY")
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop3) IS UNIQUE")
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.namedProp3) IS UNIQUE")
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop4) IS NOT NULL")
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT (n.namedProp4) IS NOT NULL")
    executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop5) IS NOT NULL")
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT (r.namedProp5) IS NOT NULL")
    graph.awaitIndexesOnline()

    // THEN
    withTx( tx => {
      val node_indexes = tx.schema().getIndexes(Label.label("Label")).asScala.toList.map(_.getName).toSet
      val rel_indexes = tx.schema().getIndexes(RelationshipType.withName("Type")).asScala.toList.map(_.getName).toSet
      val node_constraints = tx.schema().getConstraints(Label.label("Label")).asScala.toList.map(_.getName).toSet
      val rel_constraints = tx.schema().getConstraints(RelationshipType.withName("Type")).asScala.toList.map(_.getName).toSet

      node_indexes should equal(Set("index_ecdc263d", "index1", "constraint_4befd67f", "constraint1", "constraint_2b52dd68", "constraint2"))
      rel_indexes should equal(Set("index_11860c57", "index2"))
      node_constraints should equal(Set("constraint_4befd67f", "constraint1", "constraint_2b52dd68", "constraint2", "constraint_b753da28", "constraint3"))
      rel_constraints should equal(Set("constraint_612fc078", "constraint4"))
    } )
  }

  test("creating constraint on same schema as existing node index") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("creating constraint on same schema as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("creating constraint on same schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndex("Type", "prop")
    graph.awaitIndexesOnline()

    // THEN

    // Node key constraint (close as can get to same schema)
    val resNK = executeSingle("CREATE CONSTRAINT ON (n:Type) ASSERT (n.prop) IS NODE KEY")
    assertStats(resNK, nodekeyConstraintsAdded = 1)
    executeSingle("DROP CONSTRAINT `constraint_846711f3`") // needed to test the uniqueness constraint

    // Uniqueness constraint (close as can get to same schema)
    val resU = executeSingle("CREATE CONSTRAINT ON (n:Type) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 1)

    // Node property existence constraint (close as can get to same schema)
    val resN = executeSingle("CREATE CONSTRAINT ON (n:Type) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("creating named constraint on same schema as existing named node index") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE CONSTRAINT my_rel_constraint ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("creating named constraint on same schema as existing named relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Type", "prop")
    graph.awaitIndexesOnline()

    // THEN

    // Node key constraint (close as can get to same schema)
    val resNK = executeSingle("CREATE CONSTRAINT my_nk_constraint ON (n:Type) ASSERT (n.prop) IS NODE KEY")
    assertStats(resNK, nodekeyConstraintsAdded = 1)
    executeSingle("DROP CONSTRAINT my_nk_constraint") // needed to test the uniqueness constraint

    // Uniqueness constraint (close as can get to same schema)
    val resU = executeSingle("CREATE CONSTRAINT my_u_constraint ON (n:Type) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 1)

    // Node property existence constraint (close as can get to same schema)
    val resN = executeSingle("CREATE CONSTRAINT my_n_constraint ON (n:Type) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT my_r_constraint ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("should fail when creating constraint with same name as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail when creating constraint with same name as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Type) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Type) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Type) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail when creating constraint with same name and schema as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail when creating constraint with same name as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail when creating constraint with same name as existing relationship property index with IF NOT EXISTS") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Type) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Type) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Type) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail when creating constraint with same name and schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Type", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT (n.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("creating node index on same schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraint("Label", "prop1")
    graph.createUniqueConstraint("Label", "prop2")
    graph.createNodeExistenceConstraint("Label", "prop3")
    graph.createRelationshipExistenceConstraint("Label", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop1)")
      // THEN
    } should have message "There is a uniqueness constraint on (:Label {prop1}), so an index is already created that matches this."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop2)")
      // THEN
    } should have message "There is a uniqueness constraint on (:Label {prop2}), so an index is already created that matches this."

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("creating node index on same schema as existing constraint with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeKeyConstraint("Label", "prop1")
    graph.createUniqueConstraint("Label", "prop2")
    graph.createNodeExistenceConstraint("Label", "prop3")
    graph.createRelationshipExistenceConstraint("Label", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint
    // THEN no error, identical index already exists
    val resK = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.prop1)")
    assertStats(resK, indexesAdded = 0)

    // Uniqueness constraint
    // THEN no error, identical index already exists
    val resU = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.prop2)")
    assertStats(resU, indexesAdded = 0)

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("creating relationship property index on same schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraint("Type", "prop1")
    graph.createUniqueConstraint("Type", "prop2")
    graph.createNodeExistenceConstraint("Type", "prop3")
    graph.createRelationshipExistenceConstraint("Type", "prop4")
    graph.awaitIndexesOnline()

    // THEN

    // Node key constraint (close as can get to same schema)
    val resNK = executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop1)")
    assertStats(resNK, indexesAdded = 1)

    // Uniqueness constraint (close as can get to same schema)
    val resU = executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop2)")
    assertStats(resU, indexesAdded = 1)

    // Node property existence constraint (close as can get to same schema)
    val resN = executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("creating named node index on same schema as existing named constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("my_constraint1", "Label", "prop1")
    graph.createUniqueConstraintWithName("my_constraint2", "Label", "prop2")
    graph.createNodeExistenceConstraintWithName("my_constraint3", "Label", "prop3")
    graph.createRelationshipExistenceConstraintWithName("my_constraint4", "Label", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index1 FOR (n:Label) ON (n.prop1)")
      // THEN
    } should have message "There is a uniqueness constraint on (:Label {prop1}), so an index is already created that matches this."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index2 FOR (n:Label) ON (n.prop2)")
      // THEN
    } should have message "There is a uniqueness constraint on (:Label {prop2}), so an index is already created that matches this."

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE INDEX my_index3 FOR (n:Label) ON (n.prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE INDEX my_index4 FOR (n:Label) ON (n.prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("creating named relationship property index on same schema as existing named constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("my_constraint1", "Type", "prop1")
    graph.createUniqueConstraintWithName("my_constraint2", "Type", "prop2")
    graph.createNodeExistenceConstraintWithName("my_constraint3", "Type", "prop3")
    graph.createRelationshipExistenceConstraintWithName("my_constraint4", "Type", "prop4")
    graph.awaitIndexesOnline()

    // THEN

    // Node key constraint (close as can get to same schema)
    val resNK = executeSingle("CREATE INDEX my_index1 FOR ()-[r:Type]-() ON (r.prop1)")
    assertStats(resNK, indexesAdded = 1)

    // Uniqueness constraint (close as can get to same schema)
    val resU = executeSingle("CREATE INDEX my_index2 FOR ()-[r:Type]-() ON (r.prop2)")
    assertStats(resU, indexesAdded = 1)

    // Node property existence constraint (close as can get to same schema)
    val resN = executeSingle("CREATE INDEX my_index3 FOR ()-[r:Type]-() ON (r.prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE INDEX my_index4 FOR ()-[r:Type]-() ON (r.prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("should fail when creating node index with same name as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", "Label", "prop1")
    graph.createUniqueConstraintWithName("mine2", "Label", "prop2")
    graph.createNodeExistenceConstraintWithName("mine3", "Label", "prop3")
    graph.createRelationshipExistenceConstraintWithName("mine4", "Label", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine1 FOR (n:Label) ON (n.prop5)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine2 FOR (n:Label) ON (n.prop6)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine3 FOR (n:Label) ON (n.prop7)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine4 FOR (n:Label) ON (n.prop8)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating node index with same name and schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", "Label", "prop1")
    graph.createUniqueConstraintWithName("mine2", "Label", "prop2")
    graph.createNodeExistenceConstraintWithName("mine3", "Label", "prop3")
    graph.createRelationshipExistenceConstraintWithName("mine4", "Label", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine1 FOR (n:Label) ON (n.prop1)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine2 FOR (n:Label) ON (n.prop2)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine3 FOR (n:Label) ON (n.prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine4 FOR (n:Label) ON (n.prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating node index with same name and schema as existing constraint with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", "Label", "prop1")
    graph.createUniqueConstraintWithName("mine2", "Label", "prop2")
    graph.createNodeExistenceConstraintWithName("mine3", "Label", "prop3")
    graph.createRelationshipExistenceConstraintWithName("mine4", "Label", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint
    // THEN no error, index with same name already exists
    val resK = executeSingle("CREATE INDEX mine1 IF NOT EXISTS FOR (n:Label) ON (n.prop1)")
    assertStats(resK, indexesAdded = 0)

    // Uniqueness constraint
    // THEN no error, index with same name already exists
    val resU = executeSingle("CREATE INDEX mine2 IF NOT EXISTS FOR (n:Label) ON (n.prop2)")
    assertStats(resU, indexesAdded = 0)

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine3 IF NOT EXISTS FOR (n:Label) ON (n.prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine4 IF NOT EXISTS FOR (n:Label) ON (n.prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating relationship property index with same name as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", "Type", "prop1")
    graph.createUniqueConstraintWithName("mine2", "Type", "prop2")
    graph.createNodeExistenceConstraintWithName("mine3", "Type", "prop3")
    graph.createRelationshipExistenceConstraintWithName("mine4", "Type", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine1 FOR ()-[r:Type]-() ON (r.prop5)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine2 FOR ()-[r:Type]-() ON (r.prop6)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine3 FOR ()-[r:Type]-() ON (r.prop7)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine4 FOR ()-[r:Type]-() ON (r.prop8)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating relationship property index with same name and schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", "Type", "prop1")
    graph.createUniqueConstraintWithName("mine2", "Type", "prop2")
    graph.createNodeExistenceConstraintWithName("mine3", "Type", "prop3")
    graph.createRelationshipExistenceConstraintWithName("mine4", "Type", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine1 FOR ()-[r:Type]-() ON (r.prop1)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine2 FOR ()-[r:Type]-() ON (r.prop2)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine3 FOR ()-[r:Type]-() ON (r.prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine4 FOR ()-[r:Type]-() ON (r.prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating relationship property index with same name and schema as existing constraint with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", "Type", "prop1")
    graph.createUniqueConstraintWithName("mine2", "Type", "prop2")
    graph.createNodeExistenceConstraintWithName("mine3", "Type", "prop3")
    graph.createRelationshipExistenceConstraintWithName("mine4", "Type", "prop4")
    graph.awaitIndexesOnline()

    // Node key constraint (close as can get to same schema)
    // THEN no error, index with same name already exists
    val resK = executeSingle("CREATE INDEX mine1 IF NOT EXISTS FOR ()-[r:Type]-() ON (r.prop1)")
    assertStats(resK, indexesAdded = 0)

    // Uniqueness constraint (close as can get to same schema)
    // THEN no error, index with same name already exists
    val resU = executeSingle("CREATE INDEX mine2 IF NOT EXISTS FOR ()-[r:Type]-() ON (r.prop2)")
    assertStats(resU, indexesAdded = 0)

    // Node property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine3 IF NOT EXISTS FOR ()-[r:Type]-() ON (r.prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX mine4 IF NOT EXISTS FOR ()-[r:Type]-() ON (r.prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
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

  test("should not be able to create constraints when existing node index (same schema, different options)") {
    // When
    executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }

  test("should not be able to create node index when existing node key constraint (same schema, different options)") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message "There is a uniqueness constraint on (:Label {prop}), so an index is already created that matches this."
  }

  test("should not be able to create node index when existing unique property constraint (same schema, different options)") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message "There is a uniqueness constraint on (:Label {prop}), so an index is already created that matches this."
  }

  test("should be able to create constraints when existing relationship property index (close to same schema, different options)") {
    // When
    executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop) OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    val resNK = executeSingle("CREATE CONSTRAINT ON (n:Type) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'native-btree-1.0'}")
    assertStats(resNK, nodekeyConstraintsAdded = 1)
    executeSingle("DROP CONSTRAINT `constraint_846711f3`") // needed to test the uniqueness constraint

    // Then
    val resU = executeSingle("CREATE CONSTRAINT ON (n:Type) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'native-btree-1.0'}")
    assertStats(resU, uniqueConstraintsAdded = 1)
  }

  test("should be able to create relationship property index when existing node key constraint (close to same schema, different options)") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Type) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    val res = executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    assertStats(res, indexesAdded = 1)
  }

  test("should be able to create relationship property index when existing unique property constraint (close to same schema, different options)") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Type) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    val res = executeSingle("CREATE INDEX FOR ()-[r:Type]-() ON (r.prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    assertStats(res, indexesAdded = 1)
  }

  test("should not throw error on eventually consistent indexes") {
    //given
    executeSingle("""CREATE (c1:Country {id:'1', name:'CHL|USA|ESP|CHI'})""")
    executeSingle("""CREATE (c2:Country {id:'2', name:'MEX|CHI|CAN|USA'})""")
    executeSingle("""CALL db.index.fulltext.createNodeIndex("testindex",["Country"],["name"], { analyzer: "cypher", eventually_consistent: "true" })""")
    executeSingle("""CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh""")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, """MATCH (n:Country) WHERE n.name CONTAINS 'MEX' RETURN n.id""")

    //then
    result.toList should equal(List(Map("n.id" -> "2" )))
  }
}
