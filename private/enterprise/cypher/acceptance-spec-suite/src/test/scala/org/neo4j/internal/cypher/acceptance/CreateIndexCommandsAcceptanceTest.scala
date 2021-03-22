/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

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
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

//noinspection RedundantDefaultArgument
// Disable warnings for redundant default argument since its used for clarification of the `assertStats` when nothing should have happened
class CreateIndexCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for creating indexes */

  private val indexName2 = "my_second_index"

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

  test("should create unrelated indexes and constraints") {
    // WHEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.prop0)")
    executeSingle(s"CREATE INDEX index1 FOR (n:$stableEntity) ON (n.namedProp0)")
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.prop1)")
    executeSingle(s"CREATE INDEX index2 FOR ()-[r:$stableEntity]-() ON (r.namedProp1)")
    executeSingle(s"CREATE CONSTRAINT ON (n:$stableEntity) ASSERT (n.prop2) IS NODE KEY")
    executeSingle(s"CREATE CONSTRAINT constraint1 ON (n:$stableEntity) ASSERT (n.namedProp2) IS NODE KEY")
    executeSingle(s"CREATE CONSTRAINT ON (n:$stableEntity) ASSERT (n.prop3) IS UNIQUE")
    executeSingle(s"CREATE CONSTRAINT constraint2 ON (n:$stableEntity) ASSERT (n.namedProp3) IS UNIQUE")
    executeSingle(s"CREATE CONSTRAINT ON (n:$stableEntity) ASSERT (n.prop4) IS NOT NULL")
    executeSingle(s"CREATE CONSTRAINT constraint3 ON (n:$stableEntity) ASSERT (n.namedProp4) IS NOT NULL")
    executeSingle(s"CREATE CONSTRAINT ON ()-[r:$stableEntity]-() ASSERT (r.prop5) IS NOT NULL")
    executeSingle(s"CREATE CONSTRAINT constraint4 ON ()-[r:$stableEntity]-() ASSERT (r.namedProp5) IS NOT NULL")
    graph.awaitIndexesOnline()

    // THEN
    withTx( tx => {
      val node_indexes = tx.schema().getIndexes(Label.label(stableEntity)).asScala.toList.map(_.getName).toSet
      val rel_indexes = tx.schema().getIndexes(RelationshipType.withName(stableEntity)).asScala.toList.map(_.getName).toSet
      val node_constraints = tx.schema().getConstraints(Label.label(stableEntity)).asScala.toList.map(_.getName).toSet
      val rel_constraints = tx.schema().getConstraints(RelationshipType.withName(stableEntity)).asScala.toList.map(_.getName).toSet

      node_indexes should equal(Set("index_c1759583", "index1", "constraint_47b850ee", "constraint1", "constraint_16dd2b20", "constraint2"))
      rel_indexes should equal(Set("index_378db546", "index2"))
      node_constraints should equal(Set("constraint_47b850ee", "constraint1", "constraint_16dd2b20", "constraint2", "constraint_cded3a61", "constraint3"))
      rel_constraints should equal(Set("constraint_8efa0d2", "constraint4"))
    } )
  }

  // Create node index

  test("should create node index (old syntax)") {
    // WHEN
    executeSingle(s"CREATE INDEX ON :$stableEntity($stableProp)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5ac9113e")
    label should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create node index (new syntax)") {
    // WHEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5ac9113e")
    label should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create node index on similar schema as existing relationship property index") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
    graph.awaitIndexesOnline()

    // WHEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex(stableEntity, Seq(stableProp)).getName should be("index_a84ff04f")
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")

    // get by name
    val (entity, properties) = graph.getIndexSchemaByName("index_5ac9113e")
    entity should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create composite node index (old syntax)") {
    // WHEN
    executeSingle(s"CREATE INDEX ON :$stableEntity($stableProp,$stableProp2)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp, stableProp2)).getName should be("index_e63107cf")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_e63107cf")
    label should be(stableEntity)
    properties should be(Seq(stableProp, stableProp2))
  }

  test("should create composite node index (new syntax)") {
    // WHEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp, n.$stableProp2)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp, stableProp2)).getName should be("index_e63107cf")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_e63107cf")
    label should be(stableEntity)
    properties should be(Seq(stableProp, stableProp2))
  }

  test("should create named node index") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex(label, Seq(prop)).getName should be(indexName)

    // get by name
    val (actualLabel, properties) = graph.getIndexSchemaByName(indexName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should create named composite node index") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop, n.$prop2)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex(label, Seq(prop, prop2)).getName should be(indexName)

    // get by name
    val (actualLabel, properties) = graph.getIndexSchemaByName(indexName)
    actualLabel should be(label)
    properties should be(Seq(prop, prop2))
  }

  test("should not create an unnamed node index if it already exists") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")
  }

  test("should not create a named node index if it already exists") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")
  }

  test("should not create a named composite node index if it already exists") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp, n.$stableProp2)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR (n:$stableEntity) ON (n.$stableProp, n.$stableProp2)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp, stableProp2)).getName should be("index_e63107cf")
  }

  test("should create an unnamed node index if doesn't exist") {
    // WHEN
    val result = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")

    // get by name
    val (label, properties) = graph.getIndexSchemaByName("index_5ac9113e")
    label should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create a named node index if doesn't exist") {
    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR (n:$label) ON (n.$prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getNodeIndex(label, Seq(prop)).getName should be(indexName)

    // get by name
    val (actualLabel, properties) = graph.getIndexSchemaByName(indexName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should not create a named node index if a different named node index exists with the same name") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR (n:$label) ON (n.$prop2)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex(label, Seq(prop)).getName should be(indexName)
    graph.getMaybeNodeIndex(label, Seq(prop2)) should be(None)

    // get by name
    val (actualLabel, properties) = graph.getIndexSchemaByName(indexName)
    actualLabel should be(label)
    properties should be(Seq(prop))
  }

  test("should not create a named node index if a different named relationship property index exists with the same name") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR (r:$label) ON (r.$prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex(relType, Seq(prop)).getName should be(indexName)
    graph.getMaybeNodeIndex(label, Seq(prop)) should be(None)

    // get by name
    val (entity, properties) = graph.getIndexSchemaByName(indexName)
    entity should be(relType)
    properties should be(Seq(prop))
  }

  test("should be able to set index provider when creating node index") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop) OPTIONS {indexProvider : '$nativeProvider'}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider(indexName)
    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
  }

  test("should be able to set config values when creating node index") {
    // WHEN
    executeSingle(
      s"""CREATE INDEX $indexName FOR (n:$label) ON (n.$prop) OPTIONS {indexConfig: {
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
    val configuration = graph.getIndexConfig(indexName)
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
      s"""CREATE INDEX $indexName FOR (n:$label) ON (n.$prop) OPTIONS {
        | indexProvider : '$nativeLuceneProvider',
        | indexConfig: {`$cartesianMin`: [-60.0, -40.0]}
        |}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider(indexName)
    val configuration = graph.getIndexConfig(indexName)

    provider should be(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(1000000.0, 1000000.0)
  }

  test("should get default values when creating node index with empty OPTIONS map") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop) OPTIONS {}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider(indexName)
    val configuration = graph.getIndexConfig(indexName)

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

  test("creating node index on same schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraint(label, prop)
    graph.createUniqueConstraint(label, prop2)
    graph.createNodeExistenceConstraint(label, prop3)
    graph.createRelationshipExistenceConstraint(label, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop)")
      // THEN
    } should have message s"There is a uniqueness constraint on (:$label {$prop}), so an index is already created that matches this."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop2)")
      // THEN
    } should have message s"There is a uniqueness constraint on (:$label {$prop2}), so an index is already created that matches this."

    // Node property existence constraint
    // THEN
    val resN = executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("creating node index on same schema as existing constraint with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeKeyConstraint(label, prop)
    graph.createUniqueConstraint(label, prop2)
    graph.createNodeExistenceConstraint(label, prop3)
    graph.createRelationshipExistenceConstraint(label, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint
    // THEN no error, identical index already exists
    val resK = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR (n:$label) ON (n.$prop)")
    assertStats(resK, indexesAdded = 0)

    // Uniqueness constraint
    // THEN no error, identical index already exists
    val resU = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR (n:$label) ON (n.$prop2)")
    assertStats(resU, indexesAdded = 0)

    // Node property existence constraint
    // THEN
    val resN = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR (n:$label) ON (n.$prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR (n:$label) ON (n.$prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("creating named node index on same schema as existing named constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("my_constraint1", label, prop)
    graph.createUniqueConstraintWithName("my_constraint2", label, prop2)
    graph.createNodeExistenceConstraintWithName("my_constraint3", label, prop3)
    graph.createRelationshipExistenceConstraintWithName("my_constraint4", label, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX my_index1 FOR (n:$label) ON (n.$prop)")
      // THEN
    } should have message s"There is a uniqueness constraint on (:$label {$prop}), so an index is already created that matches this."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX my_index2 FOR (n:$label) ON (n.$prop2)")
      // THEN
    } should have message s"There is a uniqueness constraint on (:$label {$prop2}), so an index is already created that matches this."

    // Node property existence constraint
    // THEN
    val resN = executeSingle(s"CREATE INDEX my_index3 FOR (n:$label) ON (n.$prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint (close as can get to same schema)
    // THEN
    val resR = executeSingle(s"CREATE INDEX my_index4 FOR (n:$label) ON (n.$prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("should fail to create multiple node indexes with same schema (old syntax)") {
    // GIVEN
    executeSingle(s"CREATE INDEX ON :$stableEntity($stableProp)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX ON :$stableEntity($stableProp)")
      // THEN
    } should have message s"An equivalent index already exists, 'Index( id=1, name='index_5ac9113e', type='GENERAL BTREE', schema=(:$stableEntity {$stableProp}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple node indexes with same schema (new syntax)") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
      // THEN
    } should have message s"An equivalent index already exists, 'Index( id=1, name='index_5ac9113e', type='GENERAL BTREE', schema=(:$stableEntity {$stableProp}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple node indexes with same schema (mixed syntax)") {
    // GIVEN: old syntax
    executeSingle(s"CREATE INDEX ON :$stableEntity($stableProp)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN: new syntax
      executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
      // THEN
    } should have message s"An equivalent index already exists, 'Index( id=1, name='index_5ac9113e', type='GENERAL BTREE', schema=(:$stableEntity {$stableProp}), indexProvider='native-btree-1.0' )'."

    // GIVEN: new syntax
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp2)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN: old syntax
      executeSingle(s"CREATE INDEX ON :$stableEntity($stableProp2)")
      // THEN
    } should have message s"An equivalent index already exists, 'Index( id=2, name='index_a90d25a', type='GENERAL BTREE', schema=(:$stableEntity {$stableProp2}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named node indexes with same name and schema") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop)")
      // THEN
    } should have message s"An equivalent index already exists, 'Index( id=1, name='$indexName', type='GENERAL BTREE', schema=(:$label {$prop}), indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named node indexes with different names but same schema") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName2 FOR (n:$label) ON (n.$prop)")
      // THEN
    } should have message s"There already exists an index (:$label {$prop})."
  }

  test("should fail to create multiple named node indexes with same name") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop2)")
      // THEN
    } should have message s"There already exists an index called '$indexName'."
  }

  test("should fail to create named node index with same name as existing relationship property index") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName FOR (r:$label) ON (r.$prop)")
      // THEN
    } should have message s"There already exists an index called '$indexName'."
  }

  test("should fail to create node index with OR REPLACE") {
    val errorMessage = "Failed to create index: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX $indexName FOR (n:$label) ON (n.$prop)")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX FOR (n:$label) ON (n.$prop)")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX $indexName IF NOT EXISTS FOR (n:$label) ON (n.$prop)")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR (n:$label) ON (n.$prop)")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create node index with invalid options") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {nonValidOption : 42}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create node index with invalid options (config map directly)") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {`$cartesianMax`: [100.0, 100.0]}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create node index with invalid provider: wrong provider type") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexProvider : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider '2'. Expected String value.")
  }

  test("should fail to create node index with invalid provider: misspelled provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexProvider : 'native-btree-1'}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider 'native-btree-1'.")
  }

  test("should fail to create node index with invalid provider: fulltext provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexProvider : '$fulltextProvider'}")
    }
    // THEN
    exception.getMessage should include(
      s"""Could not create index with specified index provider '$fulltextProvider'.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create node index with invalid config: not a setting") {
    // WHEN
    val exception = the[IllegalArgumentException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexConfig: {`not.a.setting`: [4.0, 2.0]}}")
    }
    // THEN
    exception.getMessage should include("Invalid index config key 'not.a.setting', it was not recognized as an index setting.")
  }

  test("should fail to create node index with invalid config: not a config map") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexConfig : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index config '2'. Expected a map from String to Double[].")
  }

  test("should fail to create node index with invalid config: config value not a list") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexConfig : {`$cartesianMax`: 100.0}}")
    }
    // THEN
    exception.getMessage should include(s"Could not create index with specified index config '{$cartesianMax: 100.0}'. Expected a map from String to Double[].")
  }

  test("should fail to create node index with invalid config: config value includes non-valid types") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexConfig : {`$cartesianMax`: [100.0,'hundred']}}")
    }
    // THEN
    exception.getMessage should include(
      s"Could not create index with specified index config '{$cartesianMax: [100.0, hundred]}'. Expected a map from String to Double[].")
  }

  test("should fail to create node index with invalid config: fulltext config values") {
    // WHEN
    val exceptionBoolean = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexConfig : {`$eventuallyConsistent`: true}}")
    }
    // THEN
    exceptionBoolean.getMessage should include(
      s"""Could not create index with specified index config '{$eventuallyConsistent: true}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

    // WHEN
    val exceptionList = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexConfig : {`$analyzer`: [100.0], `$cartesianMax`: [100.0, 100.0]}}")
    }
    // THEN
    exceptionList.getMessage should include(
      s"""Could not create index with specified index config '{$analyzer: [100.0], $cartesianMax: [100.0, 100.0]}', contains fulltext config options.
        |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail when creating node index with same name as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", label, prop)
    graph.createUniqueConstraintWithName("mine2", label, prop2)
    graph.createNodeExistenceConstraintWithName("mine3", label, prop3)
    graph.createRelationshipExistenceConstraintWithName("mine4", label, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine1 FOR (n:$label) ON (n.$prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine2 FOR (n:$label) ON (n.$prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine3 FOR (n:$label) ON (n.$prop2)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine4 FOR (n:$label) ON (n.$prop)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating node index with same name and schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", label, prop)
    graph.createUniqueConstraintWithName("mine2", label, prop2)
    graph.createNodeExistenceConstraintWithName("mine3", label, prop3)
    graph.createRelationshipExistenceConstraintWithName("mine4", label, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine1 FOR (n:$label) ON (n.$prop)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine2 FOR (n:$label) ON (n.$prop2)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine3 FOR (n:$label) ON (n.$prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine4 FOR (n:$label) ON (n.$prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating node index with same name and schema as existing constraint with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", label, prop)
    graph.createUniqueConstraintWithName("mine2", label, prop2)
    graph.createNodeExistenceConstraintWithName("mine3", label, prop3)
    graph.createRelationshipExistenceConstraintWithName("mine4", label, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint
    // THEN no error, index with same name already exists
    val resK = executeSingle(s"CREATE INDEX mine1 IF NOT EXISTS FOR (n:$label) ON (n.$prop)")
    assertStats(resK, indexesAdded = 0)

    // Uniqueness constraint
    // THEN no error, index with same name already exists
    val resU = executeSingle(s"CREATE INDEX mine2 IF NOT EXISTS FOR (n:$label) ON (n.$prop2)")
    assertStats(resU, indexesAdded = 0)

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine3 IF NOT EXISTS FOR (n:$label) ON (n.$prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine4 IF NOT EXISTS FOR (n:$label) ON (n.$prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating node index when existing node key constraint (same schema, different options)") {
    // When
    executeSingle(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS NODE KEY OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message s"There is a uniqueness constraint on (:$label {$prop}), so an index is already created that matches this."
  }

  test("should fail when creating node index when existing unique property constraint (same schema, different options)") {
    // When
    executeSingle(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$prop) IS UNIQUE OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    the[CypherExecutionException] thrownBy {
      executeSingle(s"CREATE INDEX FOR (n:$label) ON (n.$prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    } should have message s"There is a uniqueness constraint on (:$label {$prop}), so an index is already created that matches this."
  }

  // Create relationship index

  test("should create relationship property index") {
    // WHEN
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex(stableEntity, Seq(stableProp)).getName should be("index_a84ff04f")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("index_a84ff04f")
    relType should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create relationship property index on similar schema as existing node index") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR (n:$stableEntity) ON (n.$stableProp)")
    graph.awaitIndexesOnline()

    // WHEN
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getNodeIndex(stableEntity, Seq(stableProp)).getName should be("index_5ac9113e")
    graph.getRelIndex(stableEntity, Seq(stableProp)).getName should be("index_a84ff04f")

    // get by name
    val (entity, properties) = graph.getIndexSchemaByName("index_a84ff04f")
    entity should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create composite relationship property index") {
    // WHEN
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp, r.$stableProp2)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex(stableEntity, Seq(stableProp, stableProp2)).getName should be("index_5dd3dedc")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("index_5dd3dedc")
    relType should be(stableEntity)
    properties should be(Seq(stableProp, stableProp2))
  }

  test("should named create relationship property index") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex(relType, Seq(prop)).getName should be(indexName)

    // get by name
    val (actualRelType, properties) = graph.getIndexSchemaByName(indexName)
    actualRelType should be(relType)
    properties should be(Seq(prop))
  }

  test("should create named composite relationship property index") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop, r.$prop2)")
    graph.awaitIndexesOnline()

    // THEN

    // get by schema
    graph.getRelIndex(relType, Seq(prop, prop2)).getName should be(indexName)

    // get by name
    val (actualRelType, properties) = graph.getIndexSchemaByName(indexName)
    actualRelType should be(relType)
    properties should be(Seq(prop, prop2))
  }

  test("should not create unnamed relationship property index if it already exists") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex(stableEntity, Seq(stableProp)).getName should be("index_a84ff04f")
  }

  test("should not create named composite relationship property index if it already exists") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp, r.$stableProp2)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR ()-[r:$stableEntity]-() ON (r.$stableProp, r.$stableProp2)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex(stableEntity, Seq(stableProp, stableProp2)).getName should be("index_5dd3dedc")
  }

  test("should create unnamed relationship property index if doesn't exist") {
    // WHEN
    val result = executeSingle(s"CREATE INDEX IF NOT EXISTS FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getRelIndex(stableEntity, Seq(stableProp)).getName should be("index_a84ff04f")

    // get by name
    val (relType, properties) = graph.getIndexSchemaByName("index_a84ff04f")
    relType should be(stableEntity)
    properties should be(Seq(stableProp))
  }

  test("should create named relationship property index if doesn't exist") {
    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 1)

    // get by schema
    graph.getRelIndex(relType, Seq(prop)).getName should be(indexName)

    // get by name
    val (actualRelType, properties) = graph.getIndexSchemaByName(indexName)
    actualRelType should be(relType)
    properties should be(Seq(prop))
  }

  test("should not create a named relationship property index if a different named relationship property index exists with the same name") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop, r.$prop2)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getRelIndex(relType, Seq(prop, prop2)).getName should be(indexName)
    graph.getMaybeRelIndex(relType, Seq(prop)) should be(None)

    // get by name
    val (actualRelType, properties) = graph.getIndexSchemaByName(indexName)
    actualRelType should be(relType)
    properties should be(Seq(prop, prop2))
  }

  test("should not create a named relationship property index if a different named node index exists with the same name") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR (n:$label) ON (n.$prop)")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(s"CREATE INDEX $indexName IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    // THEN
    assertStats(result, indexesAdded = 0)

    // get by schema
    graph.getNodeIndex(label, Seq(prop)).getName should be(indexName)
    graph.getMaybeRelIndex(relType, Seq(prop)) should be(None)

    // get by name
    val (entity, properties) = graph.getIndexSchemaByName(indexName)
    entity should be(label)
    properties should be(Seq(prop))
  }

  test("should be able to set index provider when creating relationship property index") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexProvider : '$nativeProvider'}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider(indexName)
    provider should be(GenericNativeIndexProvider.DESCRIPTOR)
  }

  test("should be able to set config values when creating relationship property index") {
    // WHEN
    executeSingle(
      s"""CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexConfig: {
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
    val configuration = graph.getIndexConfig(indexName)
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
      s"""CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {
         | indexProvider : '$nativeLuceneProvider',
         | indexConfig: {`$cartesianMin`: [-60.0, -40.0]}
         |}""".stripMargin)
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider(indexName)
    val configuration = graph.getIndexConfig(indexName)

    provider should be(NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR)
    configuration(SPATIAL_CARTESIAN_MIN).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(-60.0, -40.0)
    configuration(SPATIAL_CARTESIAN_MAX).asInstanceOf[Array[Double]] should contain theSameElementsInOrderAs Array(1000000.0, 1000000.0)
  }

  test("should get default values when creating relationship property index with empty OPTIONS map") {
    // WHEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {}")
    graph.awaitIndexesOnline()

    // THEN
    val provider = graph.getIndexProvider(indexName)
    val configuration = graph.getIndexConfig(indexName)

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

  test("creating relationship property index on same schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraint(relType, prop)
    graph.createUniqueConstraint(relType, prop2)
    graph.createNodeExistenceConstraint(relType, prop3)
    graph.createRelationshipExistenceConstraint(relType, prop4)
    graph.awaitIndexesOnline()

    // THEN

    // Node key constraint (close as can get to same schema)
    val resNK = executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop)")
    assertStats(resNK, indexesAdded = 1)

    // Uniqueness constraint (close as can get to same schema)
    val resU = executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop2)")
    assertStats(resU, indexesAdded = 1)

    // Node property existence constraint (close as can get to same schema)
    val resN = executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("creating named relationship property index on same schema as existing named constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("my_constraint1", relType, prop)
    graph.createUniqueConstraintWithName("my_constraint2", relType, prop2)
    graph.createNodeExistenceConstraintWithName("my_constraint3", relType, prop3)
    graph.createRelationshipExistenceConstraintWithName("my_constraint4", relType, prop4)
    graph.awaitIndexesOnline()

    // THEN

    // Node key constraint (close as can get to same schema)
    val resNK = executeSingle(s"CREATE INDEX my_index1 FOR ()-[r:$relType]-() ON (r.$prop)")
    assertStats(resNK, indexesAdded = 1)

    // Uniqueness constraint (close as can get to same schema)
    val resU = executeSingle(s"CREATE INDEX my_index2 FOR ()-[r:$relType]-() ON (r.$prop2)")
    assertStats(resU, indexesAdded = 1)

    // Node property existence constraint (close as can get to same schema)
    val resN = executeSingle(s"CREATE INDEX my_index3 FOR ()-[r:$relType]-() ON (r.$prop3)")
    assertStats(resN, indexesAdded = 1)

    // Relationship property existence constraint
    val resR = executeSingle(s"CREATE INDEX my_index4 FOR ()-[r:$relType]-() ON (r.$prop4)")
    assertStats(resR, indexesAdded = 1)
  }

  test("should be able to create relationship property index when existing node key constraint (close to same schema, different options)") {
    // When
    executeSingle(s"CREATE CONSTRAINT ON (n:$relType) ASSERT (n.$prop) IS NODE KEY OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    val res = executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    assertStats(res, indexesAdded = 1)
  }

  test("should be able to create relationship property index when existing unique property constraint (close to same schema, different options)") {
    // When
    executeSingle(s"CREATE CONSTRAINT ON (n:$relType) ASSERT (n.$prop) IS UNIQUE OPTIONS {indexProvider: 'lucene+native-3.0'}")
    graph.awaitIndexesOnline()

    // Then
    val res = executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexProvider: 'native-btree-1.0'}")
    assertStats(res, indexesAdded = 1)
  }

  test("should fail to create multiple relationship property indexes with same schema") {
    // GIVEN
    executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX FOR ()-[r:$stableEntity]-() ON (r.$stableProp)")
      // THEN
    } should have message s"An equivalent index already exists, 'Index( id=1, name='index_a84ff04f', type='GENERAL BTREE', schema=-[:$stableEntity {$stableProp}]-, indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named relationship property indexes with same name and schema") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
      // THEN
    } should have message s"An equivalent index already exists, 'Index( id=1, name='$indexName', type='GENERAL BTREE', schema=-[:$relType {$prop}]-, indexProvider='native-btree-1.0' )'."
  }

  test("should fail to create multiple named relationship property indexes with different names but same schema") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName2 FOR ()-[r:$relType]-() ON (r.$prop)")
      // THEN
    } should have message s"There already exists an index -[:$relType {$prop}]-."
  }

  test("should fail to create multiple named relationship property indexes with same name") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop2)")
      // THEN
    } should have message s"There already exists an index called '$indexName'."
  }

  test("should fail to create named relationship property index with same name as existing node index") {
    // GIVEN
    executeSingle(s"CREATE INDEX $indexName FOR (r:$label) ON (r.$prop)")
    graph.awaitIndexesOnline()

    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
      // THEN
    } should have message s"There already exists an index called '$indexName'."
  }

  test("should fail to create relationship property index with OR REPLACE") {
    val errorMessage = "Failed to create index: `OR REPLACE` cannot be used together with this command."

    val error1 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX $indexName FOR ()-[r:$relType]-() ON (r.$prop)")
    }
    error1.getMessage should startWith (errorMessage)

    val error2 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX FOR ()-[r:$relType]-() ON (r.$prop)")
    }
    error2.getMessage should startWith (errorMessage)

    val error3 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX $indexName IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop)")
    }
    error3.getMessage should startWith (errorMessage)

    val error4 = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE OR REPLACE INDEX IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop)")
    }
    error4.getMessage should startWith (errorMessage)
  }

  test("should fail to create relationship property index with invalid options") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {nonValidOption : 42}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create relationship property index with invalid options (config map directly)") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {`$cartesianMax`: [100.0, 100.0]}")
    }
    // THEN
    exception.getMessage should include("Failed to create index: Invalid option provided, valid options are `indexProvider` and `indexConfig`.")
  }

  test("should fail to create relationship property index with invalid provider: wrong provider type") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexProvider : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider '2'. Expected String value.")
  }

  test("should fail to create relationship property index with invalid provider: misspelled provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexProvider : 'native-btree-1'}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index provider 'native-btree-1'.")
  }

  test("should fail to create relationship property index with invalid provider: fulltext provider") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexProvider : '$fulltextProvider'}")
    }
    // THEN
    exception.getMessage should include(
      s"""Could not create index with specified index provider '$fulltextProvider'.
         |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail to create relationship property index with invalid config: not a setting") {
    // WHEN
    val exception = the[IllegalArgumentException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexConfig: {`not.a.setting`: [4.0, 2.0]}}")
    }
    // THEN
    exception.getMessage should include("Invalid index config key 'not.a.setting', it was not recognized as an index setting.")
  }

  test("should fail to create relationship property index with invalid config: not a config map") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexConfig : 2}")
    }
    // THEN
    exception.getMessage should include("Could not create index with specified index config '2'. Expected a map from String to Double[].")
  }

  test("should fail to create relationship property index with invalid config: config value not a list") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexConfig : {`$cartesianMax`: 100.0}}")
    }
    // THEN
    exception.getMessage should include(s"Could not create index with specified index config '{$cartesianMax: 100.0}'. Expected a map from String to Double[].")
  }

  test("should fail to create relationship property index with invalid config: config value includes non-valid types") {
    // WHEN
    val exception = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexConfig : {`$cartesianMax`: [100.0,'hundred']}}")
    }
    // THEN
    exception.getMessage should include(
      s"Could not create index with specified index config '{$cartesianMax: [100.0, hundred]}'. Expected a map from String to Double[].")
  }

  test("should fail to create relationship property index with invalid config: fulltext config values") {
    // WHEN
    val exceptionBoolean = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexConfig : {`$eventuallyConsistent`: true}}")
    }
    // THEN
    exceptionBoolean.getMessage should include(
      s"""Could not create index with specified index config '{$eventuallyConsistent: true}', contains fulltext config options.
         |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)

    // WHEN
    val exceptionList = the[InvalidArgumentsException] thrownBy {
      executeSingle(s"CREATE INDEX FOR ()-[r:$relType]-() ON (r.$prop) OPTIONS {indexConfig : {`$analyzer`: [100.0], `$cartesianMax`: [100.0, 100.0]}}")
    }
    // THEN
    exceptionList.getMessage should include(
      s"""Could not create index with specified index config '{$analyzer: [100.0], $cartesianMax: [100.0, 100.0]}', contains fulltext config options.
         |To create fulltext index, please use 'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'.""".stripMargin)
  }

  test("should fail when creating relationship property index with same name as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", relType, prop)
    graph.createUniqueConstraintWithName("mine2", relType, prop2)
    graph.createNodeExistenceConstraintWithName("mine3", relType, prop3)
    graph.createRelationshipExistenceConstraintWithName("mine4", relType, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine1 FOR ()-[r:$relType]-() ON (r.$prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine2 FOR ()-[r:$relType]-() ON (r.$prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine3 FOR ()-[r:$relType]-() ON (r.$prop2)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine4 FOR ()-[r:$relType]-() ON (r.$prop)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating relationship property index with same name and schema as existing constraint") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", relType, prop)
    graph.createUniqueConstraintWithName("mine2", relType, prop2)
    graph.createNodeExistenceConstraintWithName("mine3", relType, prop3)
    graph.createRelationshipExistenceConstraintWithName("mine4", relType, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine1 FOR ()-[r:$relType]-() ON (r.$prop)")
      // THEN
    } should have message "There already exists a constraint called 'mine1'."

    // Uniqueness constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine2 FOR ()-[r:$relType]-() ON (r.$prop2)")
      // THEN
    } should have message "There already exists a constraint called 'mine2'."

    // Node property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine3 FOR ()-[r:$relType]-() ON (r.$prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine4 FOR ()-[r:$relType]-() ON (r.$prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }

  test("should fail when creating relationship property index with same name and schema as existing constraint with IF NOT EXISTS") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("mine1", relType, prop)
    graph.createUniqueConstraintWithName("mine2", relType, prop2)
    graph.createNodeExistenceConstraintWithName("mine3", relType, prop3)
    graph.createRelationshipExistenceConstraintWithName("mine4", relType, prop4)
    graph.awaitIndexesOnline()

    // Node key constraint (close as can get to same schema)
    // THEN no error, index with same name already exists
    val resK = executeSingle(s"CREATE INDEX mine1 IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop)")
    assertStats(resK, indexesAdded = 0)

    // Uniqueness constraint (close as can get to same schema)
    // THEN no error, index with same name already exists
    val resU = executeSingle(s"CREATE INDEX mine2 IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop2)")
    assertStats(resU, indexesAdded = 0)

    // Node property existence constraint (close as can get to same schema)
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine3 IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop3)")
      // THEN
    } should have message "There already exists a constraint called 'mine3'."

    // Relationship property existence constraint
    the[CypherExecutionException] thrownBy {
      // WHEN
      executeSingle(s"CREATE INDEX mine4 IF NOT EXISTS FOR ()-[r:$relType]-() ON (r.$prop4)")
      // THEN
    } should have message "There already exists a constraint called 'mine4'."
  }
}
