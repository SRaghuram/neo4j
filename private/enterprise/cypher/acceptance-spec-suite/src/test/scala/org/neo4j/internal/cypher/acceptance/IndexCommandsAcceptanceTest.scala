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
class IndexCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating and dropping indexes */

  // Create node index

  test("should create node index (old syntax)") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5c0607ad")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create node index (new syntax)") {
    // WHEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5c0607ad")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create node index on similar schema as existing relationship property index") {
    // GIVEN
    executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // WHEN
    executeSingle("CREATE INDEX FOR (n:TYPE) ON (n.prop)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("index_e52237c6")
    graph.getNodeIndex("TYPE", Seq("prop")).getName should be("index_7250b57e")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_7250b57e")
    label should be("TYPE")
    properties should be(Seq("prop"))
  }

  test("should create composite node index (old syntax)") {
    // WHEN
    executeSingle("CREATE INDEX ON :Person(name,age)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex("Person", Seq("name", "age")).getName should be("index_c641c20c")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_c641c20c")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should create composite node index (new syntax)") {
    // WHEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex("Person", Seq("name", "age")).getName should be("index_c641c20c")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_c641c20c")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should create named node index") {
    // WHEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("my_index")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("my_index")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create named composite node index") {
    // WHEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex("Person", Seq("name", "age")).getName should be("my_index")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("my_index")
    label should be("Person")
    properties should be(Seq("name", "age"))
  }

  test("should not create an unnamed node index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")
  }

  test("should not create a named node index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")
  }

  test("should not create a named composite node index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex("Person", Seq("name", "age")).getName should be("index_c641c20c")
  }

  test("should create an unnamed node index if doesn't exist") {
    // WHEN
    val result = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5c0607ad")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create a named node index if doesn't exist") {
    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("myindex")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("myindex")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create a named node index if a different named node index exists with the same name") {
    // GIVEN
    executeSingle("CREATE INDEX myindex FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Badger) ON (n.mushroom)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex("Person", Seq("name")).getName should be("myindex")
    graph.getMaybeNodeIndex("Badger", Seq("mushroom")) should be(None)

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("myindex")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create a named node index if a different named relationship property index exists with the same name") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX my_index IF NOT EXISTS FOR (r:Label) ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("my_index")
    graph.getMaybeNodeIndex("Label", Seq("prop")) should be(None)

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("my_index")
    label should be("TYPE")
    properties should be(Seq("prop"))
  }

  test("should be able to set index provider when creating node index") {
    // WHEN
    executeSingle(s"CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexProvider : '$nativeProvider'}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
  }

  test("should be able to set config values when creating node index") {
    // WHEN
    executeSingle(
      s"""CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {
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

    // THEN
    val configuration = graph.getIndexConfig("myIndex")
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-100.0, -100.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(100.0, 100.0)
    configuration(SPATIAL_CARTESIAN_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-100.0, -100.0, -100.0)
    configuration(SPATIAL_CARTESIAN_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(100.0, 100.0, 100.0)
    configuration(SPATIAL_WGS84_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_WGS84_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0)
    configuration(SPATIAL_WGS84_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0, -100.0)
    configuration(SPATIAL_WGS84_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0, 100.0)
  }

  test("should be able to set both index provider and config when creating node index") {
    // WHEN
    executeSingle(
      s"""CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {
        | indexProvider : '$nativeLuceneProvider',
        | indexConfig: {`$cartesianMin`: [-60.0, -40.0]}
        |}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    val configuration = graph.getIndexConfig("myIndex")

    provider should be(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(1000000.0, 1000000.0)
  }

  test("should get default values when creating node index with empty OPTIONS map") {
    // WHEN
    executeSingle("CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    val configuration = graph.getIndexConfig("myIndex")

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

  test("should fail to create multiple node indexes with same schema (old syntax)") {
    // GIVEN
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX ON :Person(name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='index_5c0607ad', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple node indexes with same schema (new syntax)") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='index_5c0607ad', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple node indexes with same schema (mixed syntax)") {
    // GIVEN: old syntax
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN: new syntax
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='index_5c0607ad', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )'."

    // GIVEN: new syntax
    executeSingle("CREATE INDEX ON :Person(age)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN: old syntax
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.age)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=2, name='index_50166b1e', type='GENERAL BTREE', schema=(:Person {age}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named node indexes with same name and schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='my_index', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named node indexes with different names but same schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX your_index FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "There already exists an index (:Person {name})."
  }

  test("should fail to create multiple named node indexes with same name") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.age)")
      // THEN
    } should have message "There already exists an index called 'my_index'."
  }

  test("should fail to create named node index with same name as existing relationship property index") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR (r:Label) ON (r.prop)")
      // THEN
    } should have message "There already exists an index called 'my_index'."
  }

  test("should fail to create node index with OR REPLACE") {
    val errorMessage = "Failed to create index: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX myIndex FOR (n:Person) ON (n.name)")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX FOR (n:Person) ON (n.name)")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX myIndex IF NOT EXISTS FOR (n:Person) ON (n.name)")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create node index with invalid options") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption : 42}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create node index with invalid options (config map directly)") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {`$cartesianMax`: [100.0, 100.0]}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create node index with invalid provider: wrong provider type") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider '2'. Expected String value.")
  }

  test("should fail to create node index with invalid provider: misspelled provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 'native-btree-1'}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider 'native-btree-1'.")
  }

  test("should fail to create node index with invalid provider: fulltext provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : '$fulltextProvider'}")
    }
    // THEN
    exception.getMessage should include(
      s"""Could not create index with specified index provider '$fulltextProvider'.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create node index with invalid config: not a setting") {
    // WHEN
    val exception = the[IllegalArgumentException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {`not.a.setting`: [4.0, 2.0]}}")
    }
    // THEN
    exception.getMessage should include("Invalid index config key 'not.a.setting', it was not recognized as an index setting.")
  }

  test("should fail to create node index with invalid config: not a config map") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index config '2'. Expected a map from String to Double[].")
  }

  test("should fail to create node index with invalid config: config value not a list") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`$cartesianMax`: 100.0}}")
    }
    // THEN
    exception.getMessage should include(s"Could not create index with specified index config '{$cartesianMax: 100.0}'. Expected a map from String to Double[].")
  }

  test("should fail to create node index with invalid config: config value includes non-valid types") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`$cartesianMax`: [100.0,'hundred']}}")
    }
    // THEN
    exception.getMessage should include(
      s"Could not create index with specified index config '{$cartesianMax: [100.0, hundred]}'. Expected a map from String to Double[].")
  }

  test("should fail to create node index with invalid config: fulltext config values") {
    // WHEN
    val exceptionBoolean = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`$eventuallyConsistent`: true}}")
    }
    // THEN
    exceptionBoolean.getMessage should include(
      s"""Could not create index with specified index config '{$eventuallyConsistent: true}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

    // WHEN
    val exceptionList = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`$analyzer`: [100.0], `$cartesianMax`: [100.0, 100.0]}}")
    }
    // THEN
    exceptionList.getMessage should include(
      s"""Could not create index with specified index config '{$analyzer: [100.0], $cartesianMax: [100.0, 100.0]}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  // Create relationship index

  test("should create relationship property index") {
    // WHEN
    executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("index_e52237c6")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("index_e52237c6")
    relType should be("TYPE")
    properties should be(Seq("prop"))
  }

  test("should create relationship property index on similar schema as existing node index") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:TYPE) ON (n.prop)")
    graph.awaitIndexesOnline()

    // WHEN
    executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex("TYPE", Seq("prop")).getName should be("index_7250b57e")
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("index_e52237c6")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("index_e52237c6")
    relType should be("TYPE")
    properties should be(Seq("prop"))
  }

  test("should create composite relationship property index") {
    // WHEN
    executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop1, r.prop2)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop1", "prop2")).getName should be("index_4bf1946d")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("index_4bf1946d")
    relType should be("TYPE")
    properties should be(Seq("prop1", "prop2"))
  }

  test("should named create relationship property index") {
    // WHEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("my_index")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("my_index")
    relType should be("TYPE")
    properties should be(Seq("prop"))
  }

  test("should create named composite relationship property index") {
    // WHEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.prop1, r.prop2)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop1", "prop2")).getName should be("my_index")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("my_index")
    relType should be("TYPE")
    properties should be(Seq("prop1", "prop2"))
  }

  test("should not create unnamed relationship property index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("index_e52237c6")
  }

  test("should not create named composite relationship property index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop1, r.prop2)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX my_index IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop1, r.prop2)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop1", "prop2")).getName should be("index_4bf1946d")
  }

  test("should create unnamed relationship property index if doesn't exist") {
    // WHEN
    val result = executeSingle("CREATE INDEX IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("index_e52237c6")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("index_e52237c6")
    relType should be("TYPE")
    properties should be(Seq("prop"))
  }

  test("should create named relationship property index if doesn't exist") {
    // WHEN
    val result = executeSingle("CREATE INDEX my_index IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop")).getName should be("my_index")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("my_index")
    relType should be("TYPE")
    properties should be(Seq("prop"))
  }

  test("should not create a named relationship property index if a different named relationship property index exists with the same name") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.prop1, r.prop2)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX my_index IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex("TYPE", Seq("prop1", "prop2")).getName should be("my_index")
    graph.getMaybeRelIndex("TYPE", Seq("prop")) should be(None)

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("my_index")
    relType should be("TYPE")
    properties should be(Seq("prop1", "prop2"))
  }

  test("should not create a named relationship property index if a different named node index exists with the same name") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (r:Label) ON (r.prop)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX my_index IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex("Label", Seq("prop")).getName should be("my_index")
    graph.getMaybeRelIndex("TYPE", Seq("prop")) should be(None)

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("my_index")
    label should be("Label")
    properties should be(Seq("prop"))
  }

  test("should be able to set index provider when creating relationship property index") {
    // WHEN
    executeSingle(s"CREATE INDEX myIndex FOR ()-[r:TYPE]-() ON (r.name) OPTIONS {indexProvider : '$nativeProvider'}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
  }

  test("should be able to set config values when creating relationship property index") {
    // WHEN
    executeSingle(
      s"""CREATE INDEX myIndex FOR ()-[r:TYPE]-() ON (r.name) OPTIONS {indexConfig: {
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

    // THEN
    val configuration = graph.getIndexConfig("myIndex")
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-100.0, -100.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(100.0, 100.0)
    configuration(SPATIAL_CARTESIAN_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-100.0, -100.0, -100.0)
    configuration(SPATIAL_CARTESIAN_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(100.0, 100.0, 100.0)
    configuration(SPATIAL_WGS84_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_WGS84_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0)
    configuration(SPATIAL_WGS84_3D_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0, -100.0)
    configuration(SPATIAL_WGS84_3D_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0, 100.0)
  }

  test("should be able to set both index provider and config when creating relationship property index") {
    // WHEN
    executeSingle(
      s"""CREATE INDEX myIndex FOR ()-[r:TYPE]-() ON (r.name) OPTIONS {
         | indexProvider : '$nativeLuceneProvider',
         | indexConfig: {`$cartesianMin`: [-60.0, -40.0]}
         |}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    val configuration = graph.getIndexConfig("myIndex")

    provider should be(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(1000000.0, 1000000.0)
  }

  test("should get default values when creating relationship property index with empty OPTIONS map") {
    // WHEN
    executeSingle("CREATE INDEX myIndex FOR ()-[r:TYPE]-() ON (r.name) OPTIONS {}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    val configuration = graph.getIndexConfig("myIndex")

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

  test("should fail to create multiple relationship property indexes with same schema") {
    // GIVEN
    executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='index_bd6bd2b9', type='GENERAL BTREE', schema=-[:TYPE {name}]-, indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named relationship property indexes with same name and schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='my_index', type='GENERAL BTREE', schema=-[:TYPE {name}]-, indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named relationship property indexes with different names but same schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX your_index FOR ()-[r:TYPE]-() ON (r.name)")
      // THEN
    } should have message "There already exists an index -[:TYPE {name}]-."
  }

  test("should fail to create multiple named relationship property indexes with same name") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.age)")
      // THEN
    } should have message "There already exists an index called 'my_index'."
  }

  test("should fail to create named relationship property index with same name as existing node index") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (r:Label) ON (r.prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR ()-[r:TYPE]-() ON (r.prop)")
      // THEN
    } should have message "There already exists an index called 'my_index'."
  }

  test("should fail to create relationship property index with OR REPLACE") {
    val errorMessage = "Failed to create index: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX myIndex FOR ()-[r:TYPE]-() ON (r.prop)")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX FOR ()-[r:TYPE]-() ON (r.prop)")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX myIndex IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop)")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE INDEX IF NOT EXISTS FOR ()-[r:TYPE]-() ON (r.prop)")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create relationship property index with invalid options") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {nonValidOption : 42}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create relationship property index with invalid options (config map directly)") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {`$cartesianMax`: [100.0, 100.0]}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create relationship property index with invalid provider: wrong provider type") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider '2'. Expected String value.")
  }

  test("should fail to create relationship property index with invalid provider: misspelled provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : 'native-btree-1'}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider 'native-btree-1'.")
  }

  test("should fail to create relationship property index with invalid provider: fulltext provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexProvider : '$fulltextProvider'}")
    }
    // THEN
    exception.getMessage should include(
      s"""Could not create index with specified index provider '$fulltextProvider'.
         |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create relationship property index with invalid config: not a setting") {
    // WHEN
    val exception = the[IllegalArgumentException] thrownBy {
      executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexConfig: {`not.a.setting`: [4.0, 2.0]}}")
    }
    // THEN
    exception.getMessage should include("Invalid index config key 'not.a.setting', it was not recognized as an index setting.")
  }

  test("should fail to create relationship property index with invalid config: not a config map") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexConfig : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index config '2'. Expected a map from String to Double[].")
  }

  test("should fail to create relationship property index with invalid config: config value not a list") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexConfig : {`$cartesianMax`: 100.0}}")
    }
    // THEN
    exception.getMessage should include(s"Could not create index with specified index config '{$cartesianMax: 100.0}'. Expected a map from String to Double[].")
  }

  test("should fail to create relationship property index with invalid config: config value includes non-valid types") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexConfig : {`$cartesianMax`: [100.0,'hundred']}}")
    }
    // THEN
    exception.getMessage should include(
      s"Could not create index with specified index config '{$cartesianMax: [100.0, hundred]}'. Expected a map from String to Double[].")
  }

  test("should fail to create relationship property index with invalid config: fulltext config values") {
    // WHEN
    val exceptionBoolean = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexConfig : {`$eventuallyConsistent`: true}}")
    }
    // THEN
    exceptionBoolean.getMessage should include(
      s"""Could not create index with specified index config '{$eventuallyConsistent: true}', contains fulltext config options.
         |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

    // WHEN
    val exceptionList = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:TYPE]-() ON (r.prop) OPTIONS {indexConfig : {`$analyzer`: [100.0], `$cartesianMax`: [100.0, 100.0]}}")
    }
    // THEN
    exceptionList.getMessage should include(
      s"""Could not create index with specified index config '{$analyzer: [100.0], $cartesianMax: [100.0, 100.0]}', contains fulltext config options.
         |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  // Drop index

  test("should drop index by schema") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop node index by name") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    executeSingle("DROP INDEX `index_5c0607ad`")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop relationship property index by name") {
    // GIVEN
    graph.createRelationshipIndex("Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")

    // WHEN
    executeSingle("DROP INDEX `index_1d6349b4`")

    // THEN
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named index by schema") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX ON :Person(name)")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named node index by name") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX my_index")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
  }

  test("should drop named relationship property index by name") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("my_index")

    // WHEN
    executeSingle("DROP INDEX my_index")

    // THEN
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)
  }

  test("should drop node index by name if exists") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    val result = executeSingle("DROP INDEX `index_5c0607ad` IF EXISTS")

    // THEN
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)
    assertStats(result, indexesRemoved = 1)
  }

  test("should drop relationship property index by name if exists") {
    // GIVEN
    graph.createRelationshipIndex("Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")

    // WHEN
    val result = executeSingle("DROP INDEX `index_1d6349b4` IF EXISTS")

    // THEN
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)
    assertStats(result, indexesRemoved = 1)
  }

  test("should drop non-existent index by name if exists") {
    // WHEN
    val result = executeSingle("DROP INDEX `notexistint` IF EXISTS")

    // THEN
    assertStats(result, indexesRemoved = 0)
  }

  test("should get error when trying to drop the same index twice") {
    // GIVEN
    graph.createNodeIndex("Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("index_5c0607ad")
    executeSingle("DROP INDEX ON :Person(name)")
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on (:Person {name}). There is no such index."
  }

  test("should get error when trying to drop the same named node index twice") {
    // GIVEN
    graph.createNodeIndexWithName("my_index", "Person", "name")
    graph.getNodeIndex("Person", Seq("name")).getName should be("my_index")
    executeSingle("DROP INDEX my_index")
    graph.getMaybeNodeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  test("should get error when trying to drop the same named relationship property index twice") {
    // GIVEN
    graph.createRelationshipIndexWithName("my_index", "Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("my_index")
    executeSingle("DROP INDEX my_index")
    graph.getMaybeRelIndex("Person", Seq("name")) should be(None)

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
    } should have message "Unable to drop index on (:Person {name}). There is no such index."
  }

  test("should get error when trying to drop non-existing named index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  test("should fail to drop relationship property index by (node) schema") {
    // GIVEN
    graph.createRelationshipIndex("Person", "name")
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")

    // WHEN
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on (:Person {name}). There is no such index."

    // THEN
    graph.getRelIndex("Person", Seq("name")).getName should be("index_1d6349b4")
  }
}
