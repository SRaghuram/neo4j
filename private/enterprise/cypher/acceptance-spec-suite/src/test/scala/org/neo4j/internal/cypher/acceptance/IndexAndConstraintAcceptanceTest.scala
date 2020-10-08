/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

//noinspection RedundantDefaultArgument
// Disable warnings for redundant default argument since its used for clarification of the `assertStats` when nothing should have happened
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

  test("should not create an unnamed index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")
  }

  test("should not create a named index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")
  }

  test("should not create a named composite index if it already exists") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Person) ON (n.name, n.age)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getIndex("Person", Seq("name", "age")).getName should be("index_c641c20c")
  }

  test("should create an unnamed index if doesn't exist") {
    // WHEN
    val result = executeSingle("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5c0607ad")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should create an named index if doesn't exist") {
    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("myindex")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("myindex")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should not create an named index if a different named index exists with the same name") {
    // GIVEN
    executeSingle("CREATE INDEX myindex FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("CREATE INDEX myindex IF NOT EXISTS FOR (n:Badger) ON (n.mushroom)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getIndex("Person", Seq("name")).getName should be("myindex")
    graph.getMaybeIndex("Badger", Seq("mushroom")) should be(None)

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("myindex")
    label should be("Person")
    properties should be(Seq("name"))
  }

  test("should be able to set index provider when creating index") {
    // WHEN
    executeSingle("CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 'native-btree-1.0'}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
  }

  test("should be able to set config values when creating index") {
    // WHEN
    executeSingle(
      """CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {
        | `spatial.cartesian.min`: [-100.0, -100.0],
        | `spatial.cartesian.max`: [100.0, 100.0],
        | `spatial.cartesian-3d.min`: [-100.0, -100.0, -100.0],
        | `spatial.cartesian-3d.max`: [100.0, 100.0, 100.0],
        | `spatial.wgs-84.min`: [-60.0, -40.0],
        | `spatial.wgs-84.max`: [60.0, 40.0],
        | `spatial.wgs-84-3d.min`: [-60.0, -40.0, -100.0],
        | `spatial.wgs-84-3d.max`: [60.0, 40.0, 100.0]
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

  test("should be able to set both index provider and config when creating index") {
    // WHEN
    executeSingle(
      """CREATE INDEX myIndex FOR (n:Person) ON (n.name) OPTIONS {
        | indexProvider : 'lucene+native-3.0',
        | indexConfig: {`spatial.cartesian.min`: [-60.0, -40.0], `spatial.cartesian.max`: [60.0, 40.0]}
        |}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider("myIndex")
    val configuration = graph.getIndexConfig("myIndex")

    provider should be(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(60.0, 40.0)
  }

  test("should get default values when creating index with empty OPTIONS map when creating index") {
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

  test("should fail to create multiple indexes with same schema (old syntax)") {
    // GIVEN
    executeSingle("CREATE INDEX ON :Person(name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX ON :Person(name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='index_5c0607ad', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple indexes with same schema (new syntax)") {
    // GIVEN
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='index_5c0607ad', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple indexes with same schema (mixed syntax)") {
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

  test("should fail to create multiple named indexes with same name and schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "An equivalent index already exists, 'Index( id=1, name='my_index', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named indexes with different names but same schema") {
    // GIVEN
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.name)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE INDEX your_index FOR (n:Person) ON (n.name)")
      // THEN
    } should have message "There already exists an index (:Person {name})."
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

  test("should fail to create index with OR REPLACE") {
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

  test("should fail to create index with invalid options") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption : 42}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create index with invalid options (config map directly)") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {`spatial.cartesian.max`: [100.0, 100.0]}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create index with invalid provider: wrong provider type") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider '2'. Expected String value.")
  }

  test("should fail to create index with invalid provider: misspelled provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 'native-btree-1'}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider 'native-btree-1'.")
  }

  test("should fail to create index with invalid provider: fulltext provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 'fulltext-1.0'}")
    }
    // THEN
    exception.getMessage should include(
      """Could not create index with specified index provider 'fulltext-1.0'.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create index with invalid config: not a setting") {
    // WHEN
    val exception = the[IllegalArgumentException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig: {`not.a.setting`: [4.0, 2.0]}}")
    }
    // THEN
    exception.getMessage should include("Invalid index config key 'not.a.setting', it was not recognized as an index setting.")
  }

  test("should fail to create index with invalid config: not a config map") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index config '2'. Expected a map from String to Double[].")
  }

  test("should fail to create index with invalid config: config value not a list") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: 100.0}}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index config '{spatial.cartesian.max: 100.0}'. Expected a map from String to Double[].")
  }

  test("should fail to create index with invalid config: config value includes non-valid types") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,'hundred']}}")
    }
    // THEN
    exception.getMessage should include(
      "Could not create index with specified index config '{spatial.cartesian.max: [100.0, hundred]}'. Expected a map from String to Double[].")
  }

  test("should fail to create index with invalid config: fulltext config values") {
    // WHEN
    val exceptionBoolean = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`fulltext.eventually_consistent`: true}}")
    }
    // THEN
    exceptionBoolean.getMessage should include(
      """Could not create index with specified index config '{fulltext.eventually_consistent: true}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

    // WHEN
    val exceptionList = the[InvalidArgumentsException] thrownBy {
      executeSingle("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`fulltext.analyzer`: [100.0], `spatial.cartesian.max`: [100.0, 100.0]}}")
    }
    // THEN
    exceptionList.getMessage should include(
      """Could not create index with specified index config '{fulltext.analyzer: [100.0], spatial.cartesian.max: [100.0, 100.0]}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
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

  test("should drop index by name if exists") {
    // GIVEN
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")

    // WHEN
    val result = executeSingle("DROP INDEX `index_5c0607ad` IF EXISTS")

    // THEN
    graph.getMaybeIndex("Person", Seq("name")) should be(None)
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
    graph.createIndex("Person", "name")
    graph.getIndex("Person", Seq("name")).getName should be("index_5c0607ad")
    executeSingle("DROP INDEX ON :Person(name)")
    graph.getMaybeIndex("Person", Seq("name")) should be(None)

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX ON :Person(name)")
      // THEN
    } should have message "Unable to drop index on (:Person {name}). There is no such index."
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
    } should have message "Unable to drop index on (:Person {name}). There is no such index."
  }

  test("should get error when trying to drop non-existing named index") {
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("DROP INDEX my_index")
      // THEN
    } should have message "Unable to drop index called `my_index`. There is no such index."
  }

  // Create constraint

  // Node key

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

  // Uniqueness

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

  // Node property existence

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

  test("should create node property existence constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT EXISTS (n.name)")

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
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT EXISTS (n.name)")

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
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT EXISTS (n.name)")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT EXISTS (n.name)")

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
    executeSingle("CREATE CONSTRAINT existingConstraint ON (n:Person) ASSERT EXISTS (n.name)")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT EXISTS (n.name)")
    val result2 = executeSingle("CREATE CONSTRAINT existingConstraint IF NOT EXISTS ON (n:Person) ASSERT EXISTS (n.age)")

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)
    assertStats(result2, existenceConstraintsAdded = 0)

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
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint ON (n:Person) ASSERT exists(n.name)")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT ON (n:Person) ASSERT exists(n.name)")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint IF NOT EXISTS ON (n:Person) ASSERT exists(n.name)")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (n:Person) ASSERT exists(n.name)")
    }
    error4.getMessage should startWith (errorMessage)
  }

  // Relationship property existence

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

  test("should create relationship property existence constraint if not existing") {
    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

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
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

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
    executeSingle("CREATE CONSTRAINT existingConstraint ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

    // THEN
    assertStats(result, existenceConstraintsAdded = 0)

    // get by schema
    graph.getRelationshipConstraint("HasPet", "since").getName should be("existingConstraint")

    // get by name
    val (relType, properties) = graph.getConstraintSchemaByName("existingConstraint")
    relType should be("HasPet")
    properties should be(Seq("since"))
  }

  test("should not create named relationship property existence constraint if already existing") {
    // GIVEN
    executeSingle("CREATE CONSTRAINT existingConstraint ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")

    // WHEN
    val result = executeSingle("CREATE CONSTRAINT myConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")
    val result2 = executeSingle("CREATE CONSTRAINT existingConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.age)")

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
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT myConstraint IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:HasPet]-() ASSERT EXISTS (r.since)")
    }
    error4.getMessage should startWith (errorMessage)
  }

  // Multiple constraints

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
    executeSingle("CREATE CONSTRAINT ON (n:Label3) ASSERT EXISTS (n.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON (n:Label3) ASSERT EXISTS (n.prop)")
    } should have message "An equivalent constraint already exists, 'Constraint( id=5, name='constraint_5f73eda7', type='NODE PROPERTY EXISTENCE', schema=(:Label3 {prop}) )'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
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
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop)")
    } should have message "An equivalent constraint already exists, 'Constraint( id=5, name='constraint3', type='NODE PROPERTY EXISTENCE', schema=(:Label3 {prop}) )'."

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
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
    executeSingle("CREATE CONSTRAINT constraint3 ON (n:Label3) ASSERT EXISTS (n.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint7 ON (n:Label3) ASSERT EXISTS (n.prop)")
    } should have message "Constraint already exists: Constraint( id=5, name='constraint3', type='NODE PROPERTY EXISTENCE', schema=(:Label3 {prop}) )"

    // Relationship property existence constraint
    executeSingle("CREATE CONSTRAINT constraint4 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
    the[CypherExecutionException] thrownBy {
      executeSingle("CREATE CONSTRAINT constraint8 ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
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
    } should have message "Constraint already exists: Constraint( id=2, name='constraint_f6242497', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"

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
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='NODE KEY', schema=(:Label {prop}), ownedIndex=1 )"

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

  test("should not create constraints when existing node key constraint (same name and schema)") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop")

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop)")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
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
    val resN = executeSingle("CREATE CONSTRAINT constraint3 IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop)")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("should not create constraints when existing node key constraint (same name and diff schema)") {
    // Given
    graph.createNodeKeyConstraintWithName("constraint", "Label", "prop1")

    // Uniqueness constraint
    val resU = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS UNIQUE")
    assertStats(resU, uniqueConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop3)")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop4)")
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
    } should have message "Constraint already exists: Constraint( id=2, name='constraint1', type='UNIQUENESS', schema=(:Label {prop}), ownedIndex=1 )"

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

  test("should not create constraints when existing uniqueness constraint (same name and schema)") {
    // Given
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop)")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
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
    val resN = executeSingle("CREATE CONSTRAINT constraint3 IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop)")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("should not create constraints when existing uniqueness constraint (same name and diff schema)") {
    // Given
    graph.createUniqueConstraintWithName("constraint", "Label", "prop")

    // Node key constraint
    val resK = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT (n.prop2) IS NODE KEY")
    assertStats(resK, nodekeyConstraintsAdded = 0)

    // Node property existence constraint
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop3)")
    assertStats(resN, existenceConstraintsAdded = 0)

    // Relationship property existence constraint
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop4)")
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
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
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
    val resR = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
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
    val resR = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop4)")
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
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop)")
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
    val resN = executeSingle("CREATE CONSTRAINT constraint4 IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop)")
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
    val resN = executeSingle("CREATE CONSTRAINT constraint IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop4)")
    assertStats(resN, existenceConstraintsAdded = 0)
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

  // Combination

  test("should create unrelated indexes and constraints") {

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
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE CONSTRAINT ON (n:Label) ASSERT EXISTS (n.prop)")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE CONSTRAINT ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    assertStats(resR, existenceConstraintsAdded = 1)
  }

  test("creating constraint on same schema as existing index with IF NOT EXISTS") {
    // GIVEN
    graph.createIndex("Label", "prop")
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
    val resN = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT EXISTS (n.prop)")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    assertStats(resR, existenceConstraintsAdded = 1)
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
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT (n.prop) IS UNIQUE")
      // THEN
    } should have message "There already exists an index (:Label {prop}). A constraint cannot be created until the index has been dropped."

    // Node property existence constraint
    // THEN
    val resN = executeSingle("CREATE CONSTRAINT my_constraint ON (n:Label) ASSERT EXISTS (n.prop)")
    assertStats(resN, existenceConstraintsAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle("CREATE CONSTRAINT my_rel_constraint ON ()-[r:Label]-() ASSERT EXISTS (r.prop)")
    assertStats(resR, existenceConstraintsAdded = 1)
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

  test("should fail when creating constraint with same name as existing index with IF NOT EXISTS") {
    // GIVEN
    graph.createIndexWithName("mine", "Label", "prop")
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
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON (n:Type) ASSERT EXISTS (n.prop)")
      // THEN
    } should have message "There already exists an index called 'mine'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle("CREATE CONSTRAINT mine IF NOT EXISTS ON ()-[r:Type]-() ASSERT EXISTS (r.prop)")
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

  test("creating index on same schema as existing constraint with IF NOT EXISTS") {
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

  test("should fail when creating index with same name and schema as existing constraint with IF NOT EXISTS") {
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

  test("should fail when dropping constraint when only index exists") {
    // GIVEN
    graph.createIndexWithName("my_index", "Person", "name")
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
}
