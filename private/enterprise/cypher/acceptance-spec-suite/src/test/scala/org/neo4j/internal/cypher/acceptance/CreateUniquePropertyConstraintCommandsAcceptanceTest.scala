/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30

//noinspection RedundantDefaultArgument
// Disable warnings for redundant default argument since its used for clarification of the `assertStats` when nothing should have happened
class CreateUniquePropertyConstraintCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating unique property constraints */

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

  test("should create unique property constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, uniqueConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_e26b1a8b")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_e26b1a8b")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named unique property constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, uniqueConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("myConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("myConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create unique property constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, uniqueConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("existingConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("existingConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create named unique property constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT (n.name) IS UNIQUE")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS UNIQUE")
    val result2 = executeSingle("CREATE CONSTRAINT existingConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.age) IS UNIQUE")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, uniqueConstraintsAdded = 0)
    assertStats(result2, uniqueConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("existingConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("existingConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should be able to set index provider when creating unique property constraint") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexProvider : '$nativeLuceneProvider'}")
    graph.awaitIndexesOnline()

    // THEN: for the index backing the constraint
    val provider = graph.getIndexProvider("myConstraint")
    provider should be(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR)
  }

  test("should be able to set config values when creating unique property constraint") {
    // WHEN
    executeSingle(
      s"""CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexConfig: {
        | `$cartesianMin`: [-100.0, -100.0],
        | `$cartesianMax`: [100.0, 100.0],
        | `$cartesian3dMin`: [-100.0, -100.0, -100.0],
        | `$cartesian3dMax`: [100.0, 100.0, 100.0],
        | `$wgsMin`: [-60.0, -40.0],
        | `$wgsMax`: [60.0, 40.0],
        | `$wgs3dMin`: [-60.0, -40.0, -100.0],
        | `$wgs3dMax`: [60.0, 40.0, 100.0]
        |}}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN: for the index backing the constraint
    val configuration = graph.getIndexConfig("myConstraint")
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-100.0, -100.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(100.0, 100.0)
    configuration(SPATIAL_CARTESIAN_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-100.0, -100.0, -100.0)
    configuration(SPATIAL_CARTESIAN_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(100.0, 100.0, 100.0)
    configuration(SPATIAL_WGS84_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_WGS84_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0)
    configuration(SPATIAL_WGS84_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0, -100.0)
    configuration(SPATIAL_WGS84_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0, 100.0)
  }

  test("should be able to set both index provider and config when creating unique property constraint") {
    // WHEN
    executeSingle(
      s"""CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {
        | indexProvider : '$nativeProvider',
        | indexConfig: {`$cartesianMin`: [-60.0, -40.0], `$cartesianMax`: [60.0, 40.0]}
        |}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN: for the index backing the constraint
    val provider = graph.getIndexProvider("myConstraint")
    val configuration = graph.getIndexConfig("myConstraint")

    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0)
  }

  test("should get default values when creating unique property constraint with empty OPTIONS map") {
    // WHEN
    executeSingle("CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {}")
    graph.awaitIndexesOnline()

    // THEN: for the index backing the constraint
    val provider = graph.getIndexProvider("myConstraint")
    val configuration = graph.getIndexConfig("myConstraint")

    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-1000000.0, -1000000.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(1000000.0, 1000000.0)
    configuration(SPATIAL_CARTESIAN_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-1000000.0, -1000000.0, -1000000.0)
    configuration(SPATIAL_CARTESIAN_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(1000000.0, 1000000.0, 1000000.0)
    configuration(SPATIAL_WGS84_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-180.0, -90.0)
    configuration(SPATIAL_WGS84_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(180.0, 90.0)
    configuration(SPATIAL_WGS84_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-180.0, -90.0, -1000000.0)
    configuration(SPATIAL_WGS84_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(180.0, 90.0, 1000000.0)
  }

  test("should create unique property constraint on same schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraint("Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should create named unique property constraint on the same schema as existing named node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should create unique property constraint when existing node property existence constraint (diff name and same schema)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should create unique property constraint on same schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraint("Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should create named unique property constraint on the same schema as existing named relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should create unique property constraint when existing relationship property existence constraint (diff name and 'same' schema)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should not create unique property constraint when existing node key constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 0)
  }

  test("should not create unique property constraint when existing node key constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 0)
  }

  test("should not create unique property constraint when existing node property existence constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 0)
  }

  test("should not create unique property constraint when existing node property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 0)
  }

  test("should not create unique property constraint when existing relationship property existence constraint (same name and schema, IF NOT EXISTS)"){
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 0)
  }

  test("should not create unique property constraint when existing relationship property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 0)
  }

  test("should create unique property constraint on same schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should create named unique property constraint on same schema as existing named relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should create unique property constraint when existing relationship property index (close to same schema, different options)") {
    // GIVEN
    executeSingle("CREATE INDEX FOR ()-[r:Label]-() ON (r.prop) OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'native-btree-1.0'}")

    // THEN
    assertStats(res, uniqueConstraintsAdded = 1)
  }

  test("should fail to create composite unique property constraint") {
    val exception = the[SyntaxException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name, n.age) IS UNIQUE")
      // THEN
    }
    exception.getMessage should include("Only single property uniqueness constraints are supported")
  }

  test("should fail to create named composite unique property constraint") {
    val exception = the[SyntaxException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.name, n.age) IS UNIQUE")
      // THEN
    }
    exception.getMessage should include("Only single property uniqueness constraints are supported")
  }

  test("should fail to create unique property constraint with OR REPLACE") {
    val errorMessage = "Failed to create uniqueness constraint: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS UNIQUE")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS UNIQUE")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS UNIQUE")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create unique property constraint with invalid options") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {nonValidOption : 42}")
    }
    // THEN
    exception.getMessage should include("Failed to create uniqueness constraint: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create unique property constraint with invalid options (config map directly)") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {`$cartesianMax`: [100.0, 100.0]}")
    }
    // THEN
    exception.getMessage should include("Failed to create uniqueness constraint: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create unique property constraint with invalid provider: wrong provider type") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexProvider : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create uniqueness constraint with specified index provider '2'. Expected String value.")
  }

  test("should fail to create unique property constraint with invalid provider: misspelled provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexProvider : 'native-btree-1'}")
    }
    // THEN
    exception.getMessage should include("Could not create uniqueness constraint with specified index provider 'native-btree-1'.")
  }

  test("should fail to create unique property constraint with invalid provider: fulltext provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexProvider : '$fulltextProvider'}")
    }
    // THEN
    exception.getMessage should include(
      s"""Could not create uniqueness constraint with specified index provider '$fulltextProvider'.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create unique property constraint with invalid config: not a setting") {
    // WHEN
    val exception = the[IllegalArgumentException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexConfig: {`not.a.setting`: [4.0, 2.0]}}")
    }
    // THEN
    exception.getMessage should include("Invalid index config key 'not.a.setting', it was not recognized as an index setting.")
  }

  test("should fail to create unique property constraint with invalid config: not a config map") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexConfig : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create uniqueness constraint with specified index config '2'. Expected a map from String to Double[].")
  }

  test("should fail to create unique property constraint with invalid config: config value not a list") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexConfig : {`$cartesianMax`: 100.0}}")
    }
    // THEN
    exception.getMessage should include(s"Could not create uniqueness constraint with specified index config '{$cartesianMax: 100.0}'. Expected a map from String to Double[].")
  }

  test("should fail to create unique property constraint with invalid config: config value includes non-valid types") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexConfig : {`$cartesianMax`: [100.0,'hundred']}}")
    }
    // THEN
    exception.getMessage should include(
      s"Could not create uniqueness constraint with specified index config '{$cartesianMax: [100.0, hundred]}'. Expected a map from String to Double[].")
  }

  test("should fail to create unique property constraint with invalid config: fulltext config values") {
    // WHEN
    val exceptionBoolean = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexConfig : {`$eventuallyConsistent`: true}}")
    }
    // THEN
    exceptionBoolean.getMessage should include(
      s"""Could not create uniqueness constraint with specified index config '{$eventuallyConsistent: true}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

    // WHEN
    val exceptionList = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS UNIQUE OPTIONS {indexConfig : {`$analyzer`: [100.0], `$cartesianMax`: [100.0, 100.0]}}")
    }
    // THEN
    exceptionList.getMessage should include(
      s"""Could not create uniqueness constraint with specified index config '{$analyzer: [100.0], $cartesianMax: [100.0, 100.0]}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create multiple unique property constraints with same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=2, name='constraint_952591e6', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )'."
  }

  test("should fail to create multiple named unique property constraints with same name and schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=2, name='constraint', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )'."
  }

  test("should fail to create multiple named unique property constraints with different name and same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label) ASSERT (n.prop) IS UNIQUE")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create multiple named unique property constraints with same name") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop1) IS UNIQUE")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create unique property constraint on same schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraint("Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_f6242497', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create named unique property constraint on the same schema as existing named node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint1", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create unique property constraint on same name and schema as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints with same name as existing node key constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create unique property constraint when existing node key constraint (diff name and same schema)") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("constraint1", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create unique property constraint on same name and schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create unique property constraint with same name as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create unique property constraint on same name and schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create unique property constraint with same name as existing relationship property existence constraint") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create unique property constraint when existing node key constraint (same schema, different options)") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'native-btree-1.0'}")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_f6242497', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create unique property constraint on same schema as existing node index") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }

  test("should fail to create unique property constraint on same schema as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }

  test("should fail to create named unique property constraint on same schema as existing named node index") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }

  test("should fail to create unique property constraint with same name as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create unique property constraint with same name as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create unique property constraint with same name and schema as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create unique property constraint with same name as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create unique property constraint with same name as existing relationship property index with IF NOT EXISTS") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create unique property constraint with same name and schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create unique property constraints when existing node index (same schema, different options)") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'native-btree-1.0'}")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }
}
