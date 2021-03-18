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
class ConstraintCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating and dropping constraints */

  // Create constraint: Node key

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

  // Create constraint: Uniqueness

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

  // Create constraint: Node property existence

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

  // Create constraint: Relationship property existence

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

  // Create constraint: Multiple constraints

  test("should fail to create multiple constraints with same schema") {
    // Node key constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    } should have message "An equivalent constraint already exists, 'Constraint( id=2, name='constraint_a8ca1b14', type='NODE KEY', schema=(:Label1 {prop}), ownedIndex=1 )'."

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    } should have message "An equivalent constraint already exists, 'Constraint( id=4, name='constraint_380bd7de', type='UNIQUENESS', schema=(:Label2 {prop}), ownedIndex=3 )'."

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label3) ASSERT (n.prop) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label3) ASSERT (n.prop) IS NOT NULL")
    } should have message "An equivalent constraint already exists, 'Constraint( id=5, name='constraint_5f73eda7', type='NODE PROPERTY EXISTENCE', schema=(:Label3 {prop}) )'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    } should have message "An equivalent constraint already exists, 'Constraint( id=6, name='constraint_3e723b4d', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type {prop}]- )'."
  }

  test("should fail to create multiple named constraints with same name and schema") {
    // Node key constraint
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    } should have message "An equivalent constraint already exists, 'Constraint( id=2, name='constraint1', type='NODE KEY', schema=(:Label1 {prop}), ownedIndex=1 )'."

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    } should have message "An equivalent constraint already exists, 'Constraint( id=4, name='constraint2', type='UNIQUENESS', schema=(:Label2 {prop}), ownedIndex=3 )'."

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT (n.prop) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT (n.prop) IS NOT NULL")
    } should have message "An equivalent constraint already exists, 'Constraint( id=5, name='constraint3', type='NODE PROPERTY EXISTENCE', schema=(:Label3 {prop}) )'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    } should have message "An equivalent constraint already exists, 'Constraint( id=6, name='constraint4', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type {prop}]- )'."
  }

  test("should fail to create multiple named constraints with different name and same schema") {
    // Node key constraint
    executeSingle("CREATE CONSTRAINT constraint1 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint5 ON (n:Label1) ASSERT (n.prop) IS NODE KEY")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='NODE KEY', schema=(:Label1 {prop}), ownedIndex=1 )"

    // Uniqueness constraint
    executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint6 ON (n:Label2) ASSERT (n.prop) IS UNIQUE")
    } should have message "Constraint already exists: Constraint( id=4, name='constraint2', type='UNIQUENESS', schema=(:Label2 {prop}), ownedIndex=3 )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT (n.prop) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint7 ON (n:Label3) ASSERT (n.prop) IS NOT NULL")
    } should have message "Constraint already exists: Constraint( id=5, name='constraint3', type='NODE PROPERTY EXISTENCE', schema=(:Label3 {prop}) )"

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint8 ON ()-[r:Type]-() ASSERT (r.prop) IS NOT NULL")
    } should have message "Constraint already exists: Constraint( id=6, name='constraint4', type='RELATIONSHIP PROPERTY EXISTENCE', schema=-[:Type {prop}]- )"
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
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT (n.prop1) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT (n.prop2) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint3'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT (r.prop1) IS NOT NULL")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT (r.prop2) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint4'."
  }

  test("creating constraints on same schema as existing node key constraint") {
    // Given
    graph.createNodeKeyConstraint("Label", "prop")

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_f6242497', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
  }

  test("creating named constraints on the same schema as existing named node key constraint") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint1", "Label", "prop")

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop3) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT (r.prop4) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should not create constraints when existing node key constraint (same name and schema)") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 0)
  }

  test("should sometimes create constraints when existing node key constraint (diff name and same schema)") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint3 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("should not create constraints when existing node key constraint (same name and diff schema)") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop1")

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop3) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop4) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 0)
  }

  test("creating constraints on same schema as existing uniqueness constraint") {
    // Given
    graph.createUniqueConstraint("Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_952591e6', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
  }

  test("creating named constraints on the same schema as existing named uniqueness constraint") {
    // Given
    graph.createUniqueConstraintWithName("constraint1", "Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"

    // Node property existence constraint
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label) ASSERT (n.prop) IS NOT NULL")

    // Relationship property existence constraint (close as can get to same schema)
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop3) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT (r.prop4) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should not create constraints when existing uniqueness constraint (same name and schema)") {
    // Given
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 0)
  }

  test("should sometimes create constraints when existing uniqueness constraint (diff name and same schema)") {
    // Given
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint3 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("should not create constraints when existing uniqueness constraint (same name and diff schema)") {
    // Given
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop3) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop4) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 0)
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
    executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
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
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON ()-[r:Label]-() ASSERT (r.prop4) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should not create constraints when existing node property existence constraint (same name and schema)") {
    // Given
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 0)
  }

  test("should sometimes create constraints when existing node property existence constraint (diff name and same schema)") {
    // Given
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 1)
    executeSingle("DROP CONSTRAINT constraint2") // needed to test the uniqueness constraint

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint3 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("should not create constraints when existing node property existence constraint (same name and diff schema)") {
    // Given
    graph.createNodeExistenceConstraintWithName("constraint", "Label", "prop1")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop3) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT (r.prop4) IS NOT NULL")
    assertStats(resR, existenceConstraintsAdded = 0)
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
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NOT NULL")
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
    executeSingle("CREATE CONSTRAINT constraint4 ON (n:Label) ASSERT (n.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop) IS NOT NULL")
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
      executeSingle("CREATE CONSTRAINT constraint ON (n:Label) ASSERT (n.prop4) IS NOT NULL")
    } should have message "There already exists a constraint called 'constraint'."
  }

  test("should not create constraints when existing relationship property existence constraint (same name and schema)") {
    // Given
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 0)
  }

  test("should sometimes create constraints when existing relationship property existence constraint (diff name and 'same' schema)") {
    // Given
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint2 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 1)
    executeSingle("DROP CONSTRAINT constraint2") // needed to test the uniqueness constraint

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint3 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 1)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 1)
  }

  test("should not create constraints when existing relationship property existence constraint (same name and diff schema)") {
    // Given
    graph.createRelationshipExistenceConstraintWithName("constraint", "Label", "prop1")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop3) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop4) IS NOT NULL")
    assertStats(resN, existenceConstraintsAdded = 0)
  }

  test("should not be able to create unique property constraint when existing node key constraint (same schema, different options)") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_f6242497', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"
  }

  test("should not be able to create node key constraint when existing unique property constraint (same schema, different options)") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS NODE KEY OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_952591e6', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"
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
}
