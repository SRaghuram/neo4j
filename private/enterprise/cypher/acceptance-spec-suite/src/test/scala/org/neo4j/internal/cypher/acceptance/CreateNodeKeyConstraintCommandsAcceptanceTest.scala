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
class CreateNodeKeyConstraintCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating node key constraints */

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

  test("should create node key constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, nodekeyConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("constraint_9b73711d")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("constraint_9b73711d")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named node key constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, nodekeyConstraintsAdded = 1)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("myConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("myConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create node key constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, nodekeyConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("existingConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("existingConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create named node key constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT (n.name) IS NODE KEY")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS NODE KEY")
    val result2 = executeSingle("CREATE CONSTRAINT existingConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.age) IS NODE KEY")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, nodekeyConstraintsAdded = 0)
    assertStats(result2, nodekeyConstraintsAdded = 0)

    // get by schema
    graph.getNodeConstraint("Person", Seq("name")).getName should be("existingConstraint")

    // get by name
    val (label, properties) = graph.getConstraintSchemaByName("existingConstraint")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should be able to set index provider when creating node key constraint") {
    // WHEN
    executeSingle(s"CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexProvider : '$nativeProvider'}")
    graph.awaitIndexesOnline()

    // THEN: for the index backing the constraint
    val provider = graph.getIndexProvider("myConstraint")
    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
  }

  test("should be able to set config values when creating node key constraint") {
    // WHEN
    executeSingle(
      s"""CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexConfig: {
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

  test("should be able to set both index provider and config when creating node key constraint") {
    // WHEN
    executeSingle(
      s"""CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {
        | indexProvider : '$nativeLuceneProvider',
        | indexConfig: {`$cartesianMax`: [60.0, 40.0]}
        |}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN: for the index backing the constraint
    val provider = graph.getIndexProvider("myConstraint")
    val configuration = graph.getIndexConfig("myConstraint")

    provider should be(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-1000000.0, -1000000.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0)
  }

  test("should get default values when creating node key constraint with empty OPTIONS map") {
    // WHEN
    executeSingle("CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {}")
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

  test("should create node key constraint on same schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraint("Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should create named node key constraint on the same schema as existing named node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should create node key constraint when existing node property existence constraint (diff name and same schema)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should create node key constraint on same schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraint("Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should create named node key constraint on the same schema as existing named relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should create node key constraint when existing relationship property existence constraint (diff name and 'same' schema)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint1", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should not create node key constraint when existing uniqueness constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 0)
  }

  test("should not create node key constraint when existing uniqueness constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 0)
  }

  test("should not create node key constraint when existing node property existence constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 0)
  }

  test("should not create node key constraint when existing node property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 0)
  }

  test("should not create node key constraint when existing relationship property existence constraint (same name and schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 0)
  }

  test("should not create node key constraint when existing relationship property existence constraint (same name and diff schema, IF NOT EXISTS)") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 0)
  }

  test("should create node key constraint on same schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndex("Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should create node key named constraint on same schema as existing named relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    // WHEN (close as can get to same schema)
    val res = executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should be able to create node key constraint when existing relationship property index (close to same schema, different options)") {
    // GIVEN
    executeSingle("CREATE INDEX FOR ()-[r:Label]-() ON (r.prop) OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // WHEN
    val res = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'native-btree-1.0'}")

    // THEN
    assertStats(res, nodekeyConstraintsAdded = 1)
  }

  test("should fail to create node key constraint with OR REPLACE") {
    val errorMessage = "Failed to create node key constraint: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS NODE KEY")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS NODE KEY")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS NODE KEY")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create node key constraint with invalid options") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {nonValidOption : 42}")
    }
    // THEN
    exception.getMessage should include("Failed to create node key constraint: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create node key constraint with invalid options (config map directly)") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {`$cartesianMax`: [100.0, 100.0]}")
    }
    // THEN
    exception.getMessage should include("Failed to create node key constraint: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create node key constraint with invalid provider: wrong provider type") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexProvider : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create node key constraint with specified index provider '2'. Expected String value.")
  }

  test("should fail to create node key constraint with invalid provider: misspelled provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexProvider : 'native-btree-1'}")
    }
    // THEN
    exception.getMessage should include("Could not create node key constraint with specified index provider 'native-btree-1'.")
  }

  test("should fail to create node key constraint with invalid provider: fulltext provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexProvider : '$fulltextProvider'}")
    }
    // THEN
    exception.getMessage should include(
      s"""Could not create node key constraint with specified index provider '$fulltextProvider'.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create node key constraint with invalid config: not a setting") {
    // WHEN
    val exception = the[IllegalArgumentException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexConfig: {`not.a.setting`: [4.0, 2.0]}}")
    }
    // THEN
    exception.getMessage should include("Invalid index config key 'not.a.setting', it was not recognized as an index setting.")
  }

  test("should fail to create node key constraint with invalid config: not a config map") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexConfig : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create node key constraint with specified index config '2'. Expected a map from String to Double[].")
  }

  test("should fail to create node key constraint with invalid config: config value not a list") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexConfig : {`$cartesianMax`: 100.0}}")
    }
    // THEN
    exception.getMessage should include(s"Could not create node key constraint with specified index config '{$cartesianMax: 100.0}'. Expected a map from String to Double[].")
  }

  test("should fail to create node key constraint with invalid config: config value includes non-valid types") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexConfig : {`$cartesianMax`: [100.0,'hundred']}}")
    }
    // THEN
    exception.getMessage should include(
      s"Could not create node key constraint with specified index config '{$cartesianMax: [100.0, hundred]}'. Expected a map from String to Double[].")
  }

  test("should fail to create node key constraint with invalid config: fulltext config values") {
    // WHEN
    val exceptionBoolean = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexConfig : {`$eventuallyConsistent`: true}}")
    }
    // THEN
    exceptionBoolean.getMessage should include(
      s"""Could not create node key constraint with specified index config '{$eventuallyConsistent: true}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

    // WHEN
    val exceptionList = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE CONSTRAINT ON (n:Person) ASSERT (n.name) IS NODE KEY OPTIONS {indexConfig : {`$analyzer`: [100.0], `$cartesianMax`: [100.0, 100.0]}}")
    }
    // THEN
    exceptionList.getMessage should include(
      s"""Could not create node key constraint with specified index config '{$analyzer: [100.0], $cartesianMax: [100.0, 100.0]}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create multiple node key constraints with same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=2, name='constraint_f6242497', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )'."
  }

  test("should fail to create multiple named node key constraints with same name and schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "An equivalent constraint already exists, 'Constraint( id=2, name='constraint', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )'."
  }

  test("should fail to create multiple named node key constraints with different name and same schema") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label) ASSERT (n.prop) IS NODE KEY")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create multiple named node key constraints with same name") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop1) IS NODE KEY")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node key constraint on same schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraint("Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_952591e6', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create named node key constraint on the same schema as existing named uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint1", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create node key constraint on same name and schema as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node key constraint with same name as existing uniqueness constraint") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node key constraint when existing uniqueness constraint (diff name and same schema)") {
    // GIVEN
    graph.createUniqueConstraintWithName("constraint1", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create node key constraint on same name and schema as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints with same name as existing node property existence constraint") {
    // GIVEN
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create constraints on same name and schema as existing relationship property existence constraint") {
    // GIVEN (close as can get to same schema)
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node key constraint with same name as existing relationship property existence constraint") {
    // GIVEN
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should fail to create node key constraint when existing unique property constraint (same schema, different options)") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'native-btree-1.0'}")
      // THEN
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_952591e6', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should fail to create node key constraint on same schema as existing node index") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }

  test("should fail to create node key constraint on same schema as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndex("Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }

  test("should fail to create named node key constraint on same schema as existing named node index") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }

  test("should fail to create node key constraint with same name as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node key constraint with same name as existing node index with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node key constraint with same name and schema as existing node index") {
    // GIVEN
    graph.createNodeIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node key constraint with same name as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node key constraint with same name as existing relationship property index with IF NOT EXISTS") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop1")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node key constraint with same name and schema as existing relationship property index") {
    // GIVEN
    graph.createRelationshipIndexWithName("mine", "Label", "prop")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine ON (n:Label) ASSERT (n.prop) IS NODE KEY")
      // THEN
    } should have message "There already exists an index called 'mine'."
  }

  test("should fail to create node key constraint when existing node index (same schema, different options)") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Label) ON (n.prop) OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'native-btree-1.0'}")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."
  }
}
