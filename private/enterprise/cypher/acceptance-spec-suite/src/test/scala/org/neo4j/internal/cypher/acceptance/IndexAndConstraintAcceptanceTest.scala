/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.exceptions.{CypherExecutionException, SyntaxException}
import org.neo4j.graphdb.{Label, RelationshipType}
import org.neo4j.internal.cypher.acceptance.comparisonsupport._

class IndexAndConstraintAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  // Create index

  test("should create index (old syntax)") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5c0607ad")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create index (new syntax)") {
    // WHEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5c0607ad")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create composite index (old syntax)") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name,age)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getIndex("Person", Seq("name", "age")).getName should be("index_c641c20c")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_c641c20c")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should create composite index (new syntax)") {
    // WHEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getIndex("Person", Seq("name", "age")).getName should be("index_c641c20c")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_c641c20c")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should create named index") {
    // WHEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("my_index")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("my_index")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named composite index") {
    // WHEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getIndex("Person", Seq("name", "age")).getName should be("my_index")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("my_index")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should fail to create multiple indexes with same schema (old syntax)") {
    // GIVEN
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX ON :Person(name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( 1, 'index_5c0607ad', GENERAL BTREE, :Person(name), native-btree-1.0 )'."
  }

  test("should fail to create multiple indexes with same schema (new syntax)") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( 1, 'index_5c0607ad', GENERAL BTREE, :Person(name), native-btree-1.0 )'."
  }

  test("should fail to create multiple indexes with same schema (mixed syntax)") {
    // GIVEN: old syntax
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN: new syntax
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( 1, 'index_5c0607ad', GENERAL BTREE, :Person(name), native-btree-1.0 )'."

    // GIVEN: new syntax
    executeSingle("CREATE INDEX ON :Person(age)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN: old syntax
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.age)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( 2, 'index_50166b1e', GENERAL BTREE, :Person(age), native-btree-1.0 )'."
  }

  test("should fail to create multiple named indexes with same name and schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( 1, 'my_index', GENERAL BTREE, :Person(name), native-btree-1.0 )'."
  }

  test("should fail to create multiple named indexes with different names but same schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX your_index FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "There already exists an index :Person(name)."
  }

  test("should fail to create multiple named indexes with same name") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.age)")
      // THEN
    } should have message "There already exists an index called 'my_index'."
  }

  // Drop index

  test("should drop index by schema") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    graph.getMaybeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop index by name") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    executeSingle("DROP INDEX `index_5c0607ad`")

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
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")
    executeSingle("DROP INDEX ON :Person(name)")
    graph.getMaybeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on :Person(name). There is no such index."
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
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  test("should get error when trying to drop non-existing index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on :Person(name). There is no such index."
  }

  test("should get error when trying to drop non-existing named index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  // Create constraint

  test("should create node key constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_9b73711d")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_9b73711d")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create composite node key constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name, n.age) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeConstraint("Person", Seq("name", "age")).getName should be("constraint_c11599ca")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_c11599ca")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should create unique property constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_e26b1a8b")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_e26b1a8b")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should fail to create composite unique property constraint") {
    val exception = the[SyntaxException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name, n.age) IS UNIQUE")
      // THEN
    }
    exception.getMessage should include("Only single property uniqueness constraints are supported")
  }

  test("should create node property existence constraint") {
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

  test("should create relationship property existence constraint") {
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

  test("should create named node key constraint") {
    // WHEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
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

    // get by schema
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

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("my_constraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("my_constraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should fail to create named composite unique property constraint") {
    val exception = the[SyntaxException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.name, n.age) IS UNIQUE")
      // THEN
    }
    exception.getMessage should include("Only single property uniqueness constraints are supported")
  }

  test("should create named node property existence constraint") {
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

  test("should create named relationship property existence constraint") {
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

  test("should fail to create multiple constraints with same schema") {
    // Node key constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    } should have message "An equivalent constraint already exists, 'Constraint( UNIQUE_EXISTS, :Label1(prop) )'."

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    } should have message "An equivalent constraint already exists, 'Constraint( UNIQUE, :Label2(prop) )'."

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label3) ASSERT EXISTS (n.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label3) ASSERT EXISTS (n.prop)")
    } should have message "An equivalent constraint already exists, 'Constraint( EXISTS, :Label3(prop) )'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    } should have message "An equivalent constraint already exists, 'Constraint( EXISTS, -[:Type(prop)]- )'."
  }

  test("should fail to create multiple named constraints with same name and schema") {
    // Node key constraint
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    } should have message "An equivalent constraint already exists, 'Constraint( UNIQUE_EXISTS, :Label1(prop) )'."

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    } should have message "An equivalent constraint already exists, 'Constraint( UNIQUE, :Label2(prop) )'."

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop)")
    } should have message "An equivalent constraint already exists, 'Constraint( EXISTS, :Label3(prop) )'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    } should have message "An equivalent constraint already exists, 'Constraint( EXISTS, -[:Type(prop)]- )'."
  }

  test("should fail to create multiple named constraints with different name and same schema") {
    // Node key constraint
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint5 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    } should have message "Constraint already exists: Constraint( UNIQUE_EXISTS, :Label1(prop) )"

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint6 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    } should have message "Constraint already exists: Constraint( UNIQUE, :Label2(prop) )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint7 ON (n:Label3) ASSERT EXISTS (n.prop)")
    } should have message "Constraint already exists: Constraint( EXISTS, :Label3(prop) )"

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint8 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    } should have message "Constraint already exists: Constraint( EXISTS, -[:Type(prop)]- )"
  }

  test("should fail to create multiple named constraints with same name") {
    // Node key constraint
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop1) IS NODE KEY")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop2) IS NODE KEY")
    } should have message "There already exists a constraint called 'constraint1'."

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop1) IS UNIQUE")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop2) IS UNIQUE")
    } should have message "There already exists a constraint called 'constraint2'."

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop1)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop2)")
    } should have message "There already exists a constraint called 'constraint3'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop1)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop2)")
    } should have message "There already exists a constraint called 'constraint4'."
  }

  test("creating constraints on same schema as existing node key constraint") {
    // Given
    graph.createNodeKeyConstraint("Label", "prop")

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "Constraint already exists: Constraint( UNIQUE_EXISTS, :Label(prop) )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("creating named constraints on the same schema as existing named node key constraint") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint1", "Label", "prop")

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "Constraint already exists: Constraint( UNIQUE_EXISTS, :Label(prop) )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT EXISTS (n.prop)")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("creating constraints on same name and schema as existing node key constraint") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "There already exists a constraint called 'constraint'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT EXISTS (n.prop)")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints with same name as existing node key constraint") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop1")

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
    } should have message "There already exists a constraint called 'constraint'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT EXISTS (n.prop3)")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop4)")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("creating constraints on same schema as existing uniqueness constraint") {
    // Given
    graph.createUniqueConstraint("Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "Constraint already exists: Constraint( UNIQUE, :Label(prop) )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("creating named constraints on the same schema as existing named uniqueness constraint") {
    // Given
    graph.createUniqueConstraintWithName("constraint1", "Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "Constraint already exists: Constraint( UNIQUE, :Label(prop) )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT EXISTS (n.prop)")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("creating constraints on same name and schema as existing uniqueness constraint") {
    // Given
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "There already exists a constraint called 'constraint'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT EXISTS (n.prop)")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints with same name as existing uniqueness constraint") {
    // Given
    graph.createUniqueConstraintWithName("constraint", "Label", "prop1")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    } should have message "There already exists a constraint called 'constraint'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT EXISTS (n.prop3)")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop4)")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("creating constraints on same schema as existing node property existence constraint") {
    // Given
    graph.createNodeExistenceConstraint("Label", "prop")

    // Node key constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    executeSingle("DROP CONSTRAINT `constraint_f6242497`") // needed to test the uniqueness constraint

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("creating named constraints on the same schema as existing named node property existence constraint") {
    // Given
    graph.createNodeExistenceConstraintWithName("constraint1", "Label", "prop")

    // Node key constraint
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    executeSingle("DROP CONSTRAINT constraint2") // needed to test the uniqueness constraint

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("creating constraints on same name and schema as existing node property existence constraint") {
    // Given
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "There already exists a constraint called 'constraint'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints with same name as existing node property existence constraint") {
    // Given
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop1")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    } should have message "There already exists a constraint called 'constraint'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop3) IS UNIQUE")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop4)")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("creating constraints on same schema as existing relationship property existence constraint") {
    // Given (close as can get to same schema)
    graph.createRelationshipExistenceConstraint("Label", "prop")

    // Node key constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    executeSingle("DROP CONSTRAINT `constraint_f6242497`") // needed to test the uniqueness constraint

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)")
  }

  test("creating named constraints on the same schema as existing named relationship property existence constraint") {
    // Given (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint1", "Label", "prop")

    // Node key constraint
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    executeSingle("DROP CONSTRAINT constraint2") // needed to test the uniqueness constraint

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON (n:Label) ASSERT EXISTS (n.prop)")
  }

  test("should fail to create constraints on same name and schema as existing relationship property existence constraint") {
    // Given (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "There already exists a constraint called 'constraint'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "There already exists a constraint called 'constraint'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT EXISTS (n.prop)")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints with same name as existing relationship property existence constraint") {
    // Given
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    } should have message "There already exists a constraint called 'constraint'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop3) IS UNIQUE")
    } should have message "There already exists a constraint called 'constraint'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT EXISTS (n.prop4)")
    } should have message "There already exists a constraint called 'constraint'."
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

  test("should drop node property existence constraint by name") {
    // GIVEN
    graph.createNodeExistenceConstraint("Person", "name")
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_6ced8351")

    // WHEN
    executeSingle("DROP CONSTRAINT `constraint_6ced8351`")

    // THEN
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)
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
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_9b73711d")
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.getMaybeNodeConstraint("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
      // THEN
    } should have message "Unable to drop constraint on :Person(name): No such constraint :Person(name)."
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
  }

  test("should get error when trying to drop non-existing constraint") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
      // THEN
    } should have message "Unable to drop constraint on :Person(name): No such constraint :Person(name)."
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

  // Combination

  test("should create unrelated indexes and constraints") {
    import scala.collection.JavaConverters._

    // WHEN
    executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop1)")
    executeSingle("CREATE INDEX index1 FOR (n:Label) ON (n.namedProp1)")
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label) ASSERT (n.namedProp2) IS NODE KEY")
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop3) IS UNIQUE")
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.namedProp3) IS UNIQUE")
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop4)")
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT EXISTS (n.namedProp4)")
    executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop5)")
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.namedProp5)")
    graph.awaitIndexesOnline()

    // THEN
    withTx( tx => {
      val indexes = tx.schema().getIndexes(Label.label("Label")).asScala.toList.map(_.getName).toSet
      val node_constraints = tx.schema().getConstraints(Label.label("Label")).asScala.toList.map(_.getName).toSet
      val rel_constraints = tx.schema().getConstraints(RelationshipType.withName("Type")).asScala.toList.map(_.getName).toSet

      indexes should equal(Set("index_8d3379fb", "index1", "constraint_4befd67f", "constraint1", "constraint_2b52dd68", "constraint2"))
      node_constraints should equal(Set("constraint_4befd67f", "constraint1", "constraint_2b52dd68", "constraint2", "constraint_b753da28", "constraint3"))
      rel_constraints should equal(Set("constraint_612fc078", "constraint4"))
    } )
  }

  test("creating constraint on same schema as existing index") {
    // GIVEN
    graph.createIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index :Label(prop). A constraint cannot be created until the index has been dropped."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index :Label(prop). A constraint cannot be created until the index has been dropped."

    // Node property existence constraint
    // THEN
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)")

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("creating named constraint on same schema as existing named index") {
    // GIVEN
    graph.createIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index :Label(prop). A constraint cannot be created until the index has been dropped."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index :Label(prop). A constraint cannot be created until the index has been dropped."

    // Node property existence constraint
    // THEN
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT EXISTS (n.prop)")

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    executeSingle("CREATE CONSTRAINT my_rel_constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
  }

  test("should fail when creating constraint with same name as existing index") {
    // GIVEN
    graph.createIndexWithName("mine", "Label", "prop")
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
      executeSingle("CREATE CONSTRAINT mine ON (n:Type) ASSERT EXISTS (n.prop)")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail when creating constraint with same name and schema as existing index") {
    // GIVEN
    graph.createIndexWithName("mine", "Label", "prop")
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
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT EXISTS (n.prop)")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("creating index on same schema as existing constraint") {
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
    } should have message "There is a uniqueness constraint on :Label(prop1), so an index is already created that matches this."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop2)")
      // THEN
    } should have message "There is a uniqueness constraint on :Label(prop2), so an index is already created that matches this."

    // Node property existence constraint
    // THEN
    executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop3)")

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop4)")
  }

  test("creating named index on same schema as existing named constraint") {
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
    } should have message "There is a uniqueness constraint on :Label(prop1), so an index is already created that matches this."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index2 FOR (n:Label) ON (n.prop2)")
      // THEN
    } should have message "There is a uniqueness constraint on :Label(prop2), so an index is already created that matches this."

    // Node property existence constraint
    // THEN
    executeSingle("CREATE INDEX my_index3 FOR (n:Label) ON (n.prop3)")

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    executeSingle("CREATE INDEX my_index4 FOR (n:Label) ON (n.prop4)")
  }

  test("should fail when creating index with same name as existing constraint") {
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

  test("should fail when creating index with same name and schema as existing constraint") {
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

  test("should fail when dropping constraint when only index exists") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
      // THEN
    } should have message "Unable to drop constraint on :Person(name): No such constraint :Person(name)."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
      // THEN
    } should have message "Unable to drop constraint on :Person(name): No such constraint :Person(name)."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")
      // THEN
    } should have message "Unable to drop constraint on :Person(name): No such constraint :Person(name)."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT ON ()-[n:Person]-() ASSERT EXISTS (n.name)")
      // THEN
    } should have message "Unable to drop constraint on -[:Person(name)]-: No such constraint -[:Person(name)]-."

    // Drop by name
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP CONSTRAINT my_index")
      // THEN
    } should have message "Unable to drop constraint `my_index`: No such constraint my_index."
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
    } should have message "Unable to drop index: Index belongs to constraint: :Label(prop1)"

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine1")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: `mine1`"

    // Uniqueness constraint (backed by index)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Label(prop2)")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: :Label(prop2)"

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine2")
      // THEN
    } should have message "Unable to drop index: Index belongs to constraint: `mine2`"

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Label(prop3)")
      // THEN
    } should have message "Unable to drop index on :Label(prop3). There is no such index."

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine3")
      // THEN
    } should have message "Unable to drop index called `mine3`. There is no such index."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Label(prop4)")
      // THEN
    } should have message "Unable to drop index on :Label(prop4). There is no such index."

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX mine4")
      // THEN
    } should have message "Unable to drop index called `mine4`. There is no such index."
  }
}
