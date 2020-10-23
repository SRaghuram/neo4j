/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util
import java.util.concurrent.ThreadLocalRandom

import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.graphdb.schema.AnalyzerProvider
import org.neo4j.graphdb.schema.IndexSettingImpl
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_ANALYZER
import org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX
import org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.StandardNoStopWords
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.service.Services
import org.neo4j.values.storable.RandomValues

class ShowSchemaCommandsAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  private val defaultBtreeProvider = GenericNativeIndexProvider.DESCRIPTOR.name()
  private val fulltextProvider = FulltextIndexProviderFactory.DESCRIPTOR.name()

  private val defaultBtreeOptionsString: String =
    "{indexConfig: {" +
      s"`${SPATIAL_CARTESIAN_3D_MAX.getSettingName}`: [1000000.0, 1000000.0, 1000000.0]," +
      s"`${SPATIAL_CARTESIAN_3D_MIN.getSettingName}`: [-1000000.0, -1000000.0, -1000000.0]," +
      s"`${SPATIAL_CARTESIAN_MAX.getSettingName}`: [1000000.0, 1000000.0]," +
      s"`${SPATIAL_CARTESIAN_MIN.getSettingName}`: [-1000000.0, -1000000.0]," +
      s"`${SPATIAL_WGS84_3D_MAX.getSettingName}`: [180.0, 90.0, 1000000.0]," +
      s"`${SPATIAL_WGS84_3D_MIN.getSettingName}`: [-180.0, -90.0, -1000000.0]," +
      s"`${SPATIAL_WGS84_MAX.getSettingName}`: [180.0, 90.0]," +
      s"`${SPATIAL_WGS84_MIN.getSettingName}`: [-180.0, -90.0]}, " +
      s"indexProvider: '$defaultBtreeProvider'}"

  private val defaultBtreeOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      SPATIAL_CARTESIAN_3D_MAX.getSettingName -> Array(1000000.0, 1000000.0, 1000000.0),
      SPATIAL_CARTESIAN_3D_MIN.getSettingName -> Array(-1000000.0, -1000000.0, -1000000.0),
      SPATIAL_CARTESIAN_MAX.getSettingName -> Array(1000000.0, 1000000.0),
      SPATIAL_CARTESIAN_MIN.getSettingName -> Array(-1000000.0, -1000000.0),
      SPATIAL_WGS84_3D_MAX.getSettingName -> Array(180.0, 90.0, 1000000.0),
      SPATIAL_WGS84_3D_MIN.getSettingName -> Array(-180.0, -90.0, -1000000.0),
      SPATIAL_WGS84_MAX.getSettingName -> Array(180.0, 90.0),
      SPATIAL_WGS84_MIN.getSettingName -> Array(-180.0, -90.0)),
    "indexProvider" -> defaultBtreeProvider)

  private val defaultFulltextConfigString =
    s"{`${FULLTEXT_ANALYZER.getSettingName}`: '${StandardNoStopWords.ANALYZER_NAME}',`${FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName}`: 'false'}"

  private val defaultFulltextOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      FULLTEXT_ANALYZER.getSettingName -> StandardNoStopWords.ANALYZER_NAME,
      FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName -> false),
    "indexProvider" -> fulltextProvider)

  private val label = "Label"
  private val label2 = "Label2"
  private val labelWhitespace = "Label 1"
  private val labelWhitespace2 = "Label 2"
  private val labelBackticks = "`Label``3`"
  private val labelBackticks2 = "Label`4``"

  private val relType = "relType"
  private val relType2 = "relType2"
  private val relTypeWhitespace = "reltype 3"
  private val relTypeBackticks = "`rel`type`"

  private val prop = "prop"
  private val prop2 = "prop2"
  private val prop3 = "prop3"
  private val propWhitespace = "prop 1"
  private val propWhitespace2 = "prop 2"
  private val propBackticks = "`prop`4"
  private val propBackticks2 = "`prop5``"

  private val random: ThreadLocalRandom = ThreadLocalRandom.current()
  private val randomValues: RandomValues = RandomValues.create(random)

  // SHOW INDEXES tests

  test("show index should return empty result when there are no indexes") {
    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(empty)
  }

  test("should show index") {
    // GIVEN
    createDefaultBtreeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(List(defaultBtreeBriefOutput(1L)))
  }

  test("should show indexes in alphabetic order") {
    // GIVEN
    graph.createIndexWithName("poppy", label, propWhitespace)
    graph.createIndexWithName("benny", label2, prop, prop2)
    graph.createIndexWithName("albert", label2, prop2)
    graph.createIndexWithName("charlie", label, prop2)
    graph.createIndexWithName("xavier", label, prop)
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.columnAs("name").toList should equal(List("albert", "benny", "charlie", "poppy", "xavier"))
  }

  test("should show indexes backing constraints") {
    // GIVEN
    createDefaultConstraints()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW BTREE INDEXES")

    // THEN
    // Existence constraints are not backed by indexes so they should not be listed
    result.toList should be(List(defaultUniquenessBriefIndexOutput(1L), defaultNodeKeyBriefIndexOutput(3L)))
  }

  test("show indexes should show both btree and fulltext indexes") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(List(defaultFulltextNodeBriefOutput(2L), defaultFulltextRelBriefOutput(3L), defaultBtreeBriefOutput(1L)))
  }

  test("show all indexes should show both btree and fulltext indexes") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW ALL INDEXES")

    // THEN
    result.toList should be(List(defaultFulltextNodeBriefOutput(2L), defaultFulltextRelBriefOutput(3L), defaultBtreeBriefOutput(1L)))
  }

  test("show btree indexes should show only btree indexes") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW BTREE INDEXES")

    // THEN
    result.toList should be(List(defaultBtreeBriefOutput(1L)))
  }

  test("should show btree index with verbose output") {
    // GIVEN
    createDefaultBtreeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultBtreeVerboseOutput(1L)))
  }

  test("should show fulltext indexes with verbose output") {
    // GIVEN
    createDefaultFullTextNodeIndex()
    createDefaultFullTextRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultFulltextOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultFulltextNodeVerboseOutput(1L), defaultFulltextRelVerboseOutput(2L)))
  }

  test("should show indexes backing constraints with verbose output") {
    // GIVEN
    createDefaultUniquenessConstraint()
    createDefaultNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultUniquenessVerboseIndexOutput(1L), defaultNodeKeyVerboseIndexOutput(3L)))
  }

  test("should show correct options for btree index with random options") {
    // GIVEN
    val randomOptions = createBtreeIndexWithRandomOptions("btree", label, List(prop2))
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.size should be(1)
    assertCorrectOptionsMap(options.head, randomOptions)
  }

  test("should show correct options for fulltext node index with random options") {
    // GIVEN
    val randomConfig = createFulltextNodeIndexWithRandomOptions("fullNode", List(label2), List(prop2))
    val randomOptions = Map("indexConfig" -> randomConfig, "indexProvider" -> fulltextProvider)
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.size should be(1)
    assertCorrectOptionsMap(options.head, randomOptions)
  }

  test("should show correct options for fulltext rel index with random options") {
    // GIVEN
    val randomConfig = createFulltextRelIndexWithRandomOptions("fullNode", List(relType), List(prop))
    val randomOptions = Map("indexConfig" -> randomConfig, "indexProvider" -> fulltextProvider)
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.size should be(1)
    assertCorrectOptionsMap(options.head, randomOptions)
  }

  test("show indexes should show valid create index statements") {

    // Btree indexes
    createBtreeIndexWithRandomOptions("btree", label, List(prop))
    createBtreeIndexWithRandomOptions("btree composite", label, List(prop, prop2))
    createBtreeIndexWithRandomOptions("btree whitespace", labelWhitespace, List(propWhitespace))
    createBtreeIndexWithRandomOptions("btree backticks", labelBackticks, List(propBackticks))
    createBtreeIndexWithRandomOptions("``horrible `index`name```", label, List(prop2))

    // Fulltext node indexes
    createFulltextNodeIndexWithRandomOptions("full-text", List(label), List(prop))
    createFulltextNodeIndexWithRandomOptions("full-text whitespace", List(labelWhitespace), List(propWhitespace))
    createFulltextNodeIndexWithRandomOptions("full-text multi-label", List(label, label2), List(prop))
    createFulltextNodeIndexWithRandomOptions("full-text multi-prop", List(label), List(prop, prop2))
    createFulltextNodeIndexWithRandomOptions("full-text backticks", List(labelBackticks), List(propBackticks))
    createFulltextNodeIndexWithRandomOptions("advanced full-text backticks", List(labelBackticks, label), List(prop, propBackticks))
    createFulltextNodeIndexWithRandomOptions("``horrible `index`name``2`", List(label), List(prop2))

    // Fulltext rel indexes
    createFulltextRelIndexWithRandomOptions("relType full-text", List(relType), List(prop))
    createFulltextRelIndexWithRandomOptions("relType full-text whitespace", List(relTypeWhitespace), List(propWhitespace))
    createFulltextRelIndexWithRandomOptions("relType full-text multi-type", List(relType, relType2), List(prop))
    createFulltextRelIndexWithRandomOptions("relType full-text multi-prop", List(relType), List(prop, prop2))
    createFulltextRelIndexWithRandomOptions("relType full-text backticks", List(relTypeBackticks), List(propBackticks))
    createFulltextRelIndexWithRandomOptions("``horrible `index`name`3``", List(relType), List(prop2))

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateIndexesUsingCreateStatement("INDEX")
  }

  test("show indexes should show valid create constraint statements") {

    // Indexes backing uniqueness constraints
    createConstraint(UniqueConstraints, "unique property", label, prop)
    createConstraint(UniqueConstraints, "unique property whitespace", labelWhitespace, propWhitespace)
    createConstraint(UniqueConstraints, "unique backticks", labelBackticks, propBackticks)
    createConstraint(UniqueConstraints, "``horrible`name`", label2, prop)

    // Indexes backing node key constraints
    createConstraint(NodeKeyConstraints, "node key", label2, prop2)
    createConstraint(NodeKeyConstraints, "node key whitespace", labelWhitespace2, propWhitespace2)
    createConstraint(NodeKeyConstraints, "node key backticks", labelBackticks, propBackticks2)
    createConstraint(NodeKeyConstraints, "``horrible`name2`", label, prop2)

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateIndexesUsingCreateStatement("CONSTRAINT")
  }

  // SHOW CONSTRAINTS tests

  test("show constraints should return empty result when there are no constraints") {
    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS")

    // THEN
    result.toList should be(empty)
  }

  test("should show constraint") {
    // GIVEN
    createDefaultUniquenessConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L)))
  }

  test("should show constraints in alphabetic order") {
    // GIVEN
    graph.createNodeKeyConstraintWithName("poppy", label2, prop)
    graph.createNodeKeyConstraintWithName("benny", label2, prop, prop2)
    graph.createUniqueConstraintWithName("albert", label2, prop2)
    graph.createRelationshipExistenceConstraintWithName("charlie", label, prop2)
    graph.createNodeExistenceConstraintWithName("xavier", label, prop)

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS")

    // THEN
    result.columnAs("name").toList should equal(List("albert", "benny", "charlie", "poppy", "xavier"))
  }

  test("show constraints should show all types of constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L), defaultNodeKeyBriefConstraintOutput(4L),
      defaultNodeExistsBriefConstraintOutput(5L), defaultRelExistsBriefConstraintOutput(6L)))
  }

  test("show all constraints should show all types of constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW ALL CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L), defaultNodeKeyBriefConstraintOutput(4L),
      defaultNodeExistsBriefConstraintOutput(5L), defaultRelExistsBriefConstraintOutput(6L)))
  }

  test("show unique constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW UNIQUE CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L)))
  }

  test("show node key constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW NODE KEY CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultNodeKeyBriefConstraintOutput(4L)))
  }

  test("show node exists constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW NODE EXISTS CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultNodeExistsBriefConstraintOutput(5L)))
  }

  test("show relationship exists constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW RELATIONSHIP EXISTS CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultRelExistsBriefConstraintOutput(6L)))
  }

  test("show exists constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW EXISTS CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultNodeExistsBriefConstraintOutput(5L), defaultRelExistsBriefConstraintOutput(6L)))
  }

  test("should show unique constraint with verbose output") {
    // GIVEN
    createDefaultUniquenessConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW UNIQUE CONSTRAINTS VERBOSE OUTPUT")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultUniquenessVerboseConstraintOutput(2L)))
  }

  test("should show node key constraint with verbose output") {
    // GIVEN
    createDefaultNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW NODE KEY CONSTRAINTS VERBOSE OUTPUT")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultNodeKeyVerboseConstraintOutput(2L)))
  }

  test("should show node exists constraint with verbose output") {
    // GIVEN
    createDefaultNodeExistsConstraint()

    // WHEN
    val result = executeSingle("SHOW NODE EXISTS CONSTRAINTS VERBOSE OUTPUT")

    val options: List[Object] = result.columnAs("options").toList
    options should be(List(null))
    withoutColumns(result.toList, List("options")) should equal(List(defaultNodeExistsVerboseConstraintOutput(1L)))
  }

  test("should show rel exists constraint with verbose output") {
    // GIVEN
    createDefaultRelExistsConstraint()

    // WHEN
    val result = executeSingle("SHOW RELATIONSHIP EXISTS CONSTRAINTS VERBOSE OUTPUT")

    val options: List[Object] = result.columnAs("options").toList
    options should be(List(null))
    withoutColumns(result.toList, List("options")) should equal(List(defaultRelExistsVerboseConstraintOutput(1L)))
  }

  test("should show correct options for unique constraint with random options") {
    // GIVEN
    val randomOptions = createConstraint(UniqueConstraints, "unique", label2, prop)
    graph.awaitIndexesOnline()

    // THEN
    randomOptions.isDefined should be(true)

    // WHEN
    val indexResult = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val indexOptions: List[Object] = indexResult.columnAs("options").toList
    indexOptions.size should be(1)
    assertCorrectOptionsMap(indexOptions.head, randomOptions.get)

    // WHEN
    val constraintResult = executeSingle("SHOW CONSTRAINTS VERBOSE OUTPUT")

    // THEN
    val constraintOptions: List[Object] = constraintResult.columnAs("options").toList
    constraintOptions.size should be(1)
    assertCorrectOptionsMap(constraintOptions.head, randomOptions.get)
  }

  test("should show correct options for node key constraint with random options") {
    // GIVEN
    val randomOptions = createConstraint(NodeKeyConstraints, "nodeKey", label2, prop)
    graph.awaitIndexesOnline()

    // THEN
    randomOptions.isDefined should be(true)

    // WHEN
    val indexResult = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val indexOptions: List[Object] = indexResult.columnAs("options").toList
    indexOptions.size should be(1)
    assertCorrectOptionsMap(indexOptions.head, randomOptions.get)

    // WHEN
    val constraintResult = executeSingle("SHOW CONSTRAINTS VERBOSE OUTPUT")

    // THEN
    val constraintOptions: List[Object] = constraintResult.columnAs("options").toList
    constraintOptions.size should be(1)
    assertCorrectOptionsMap(constraintOptions.head, randomOptions.get)
  }

  test("show constraints should show valid create statements for unique constraints") {
    createConstraint(UniqueConstraints, "unique property", label, prop)
    createConstraint(UniqueConstraints, "unique property whitespace", labelWhitespace, propWhitespace)
    createConstraint(UniqueConstraints, "unique backticks", labelBackticks, propBackticks)
    createConstraint(UniqueConstraints, "``horrible`name`", label, prop2)

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  test("show constraints should show valid create statements for node key constraints") {
    createConstraint(NodeKeyConstraints, "node key", label, prop3)
    createConstraint(NodeKeyConstraints, "node key whitespace", labelWhitespace2, propWhitespace2)
    createConstraint(NodeKeyConstraints, "node key backticks", labelBackticks, propBackticks2)
    createConstraint(NodeKeyConstraints, "``horrible`name2", label2, prop)

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  test("show constraints should show valid create statements for node exists constraints") {
    createConstraint(NodeExistsConstraints, "node prop exists", label2, prop2)
    createConstraint(NodeExistsConstraints, "node prop exists whitespace", labelWhitespace, propWhitespace2)
    createConstraint(NodeExistsConstraints, "node prop exists backticks", labelBackticks2, propBackticks)
    createConstraint(NodeExistsConstraints, "horrible`name3``", label2, prop3)

    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  test("show constraints should show valid create statements for rel exists constraints") {
    createConstraint(RelExistsConstraints, "rel prop exists", relType, prop)
    createConstraint(RelExistsConstraints, "rel prop exists whitespace", relTypeWhitespace, propWhitespace)
    createConstraint(RelExistsConstraints, "rel prop exists backticks", relTypeBackticks, propBackticks)
    createConstraint(RelExistsConstraints, "horrible name`4````", relType, prop2)

    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  test("show constraints should show valid create statements for mixed constraints") {
    createConstraint(UniqueConstraints, "unique", label2, prop)
    createConstraint(NodeKeyConstraints, "node key", label, prop2)
    createConstraint(NodeExistsConstraints, "node exists", label, prop3)
    createConstraint(RelExistsConstraints, "rel exists", relType, prop3)

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  // general index help methods

  private def createDefaultIndexes(): Unit = {
    createDefaultBtreeIndex()
    createDefaultFullTextNodeIndex()
    createDefaultFullTextRelIndex()
    graph.awaitIndexesOnline()
  }

  private def indexOutputBrief(id: Long, name: String, uniqueness: String, indexType: String, entityType: String,
                               labelsOrTypes: List[String], properties: List[String], indexProvider: String): Map[String, Any] =
    Map("id" -> id,
      "name" -> name,
      "state" -> "ONLINE",
      "populationPercent" -> 100.0,
      "uniqueness" -> uniqueness,
      "type" -> indexType,
      "entityType" -> entityType,
      "labelsOrTypes" -> labelsOrTypes,
      "properties" -> properties,
      "indexProvider" -> indexProvider)

  // options cannot be handled by the normal CypherComparisonSupport assertions due to returning maps and arrays, so it is not included here
  private def indexOutputVerbose(createStatement: String): Map[String, Any] = Map("failureMessage" -> "", "createStatement" -> createStatement)

  // Btree index help methods

  private def createBtreeIndexWithRandomOptions(name: String, label: String, properties: List[String]): Map[String, Object] = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val escapedLabel = s"`${escapeBackticks(label)}`"
    val escapedProperties = properties.map(p => s"n.`${escapeBackticks(p)}`").mkString(",")
    val randomOptions = randomBtreeOptions()
    executeSingle(s"CREATE INDEX $escapedName FOR (n:$escapedLabel) ON ($escapedProperties) OPTIONS ${randomOptions._1}")
    randomOptions._2
  }

  private def createDefaultBtreeIndex(): Unit = graph.createIndexWithName("my_index", label, prop2, prop)

  private def defaultBtreeBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "my_index", "NONUNIQUE", "BTREE", "NODE", List(label), List(prop2, prop), "native-btree-1.0")

  private def defaultBtreeVerboseOutput(id: Long): Map[String, Any] = defaultBtreeBriefOutput(id) ++
    indexOutputVerbose(s"CREATE INDEX `my_index` FOR (n:`$label`) ON (n.`$prop2`, n.`$prop`) OPTIONS $defaultBtreeOptionsString")

  // Fulltext index help methods

  private def createFulltextNodeIndexWithRandomOptions(name: String, labelList: List[String], propertyList: List[String]): Map[String, Any] = {
    val labels: String = labelList.map(l => s"'$l'").mkString(",")
    val properties: String = propertyList.map(p => s"'$p'").mkString(",")
    val randomConfig = randomFulltextSetting()
    executeSingle(s"CALL db.index.fulltext.createNodeIndex('$name', [$labels], [$properties], ${randomConfig._1})")
    randomConfig._2
  }

  private def createDefaultFullTextNodeIndex(): Unit = executeSingle(s"CALL db.index.fulltext.createNodeIndex('fulltext_node', ['$label'], ['$prop'])")

  private def defaultFulltextNodeBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "fulltext_node", "NONUNIQUE", "FULLTEXT", "NODE", List(label), List(prop), fulltextProvider)

  private def defaultFulltextNodeVerboseOutput(id: Long): Map[String, Any] = defaultFulltextNodeBriefOutput(id) ++
    indexOutputVerbose(s"CALL db.index.fulltext.createNodeIndex('fulltext_node', ['$label'], ['$prop'], $defaultFulltextConfigString)")

  private def createFulltextRelIndexWithRandomOptions(name: String, labelList: List[String], propertyList: List[String]): Map[String, Any] = {
    val labels: String = labelList.map(l => s"'$l'").mkString(",")
    val properties: String = propertyList.map(p => s"'$p'").mkString(",")
    val randomConfig = randomFulltextSetting()
    executeSingle(s"CALL db.index.fulltext.createRelationshipIndex('$name', [$labels], [$properties], ${randomConfig._1})")
    randomConfig._2
  }

  private def createDefaultFullTextRelIndex(): Unit = executeSingle(s"CALL db.index.fulltext.createRelationshipIndex('fulltext_rel', ['$relType'], ['$prop'])")

  private def defaultFulltextRelBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "fulltext_rel", "NONUNIQUE", "FULLTEXT", "RELATIONSHIP", List(relType), List(prop), fulltextProvider)

  private def defaultFulltextRelVerboseOutput(id: Long): Map[String, Any] = defaultFulltextRelBriefOutput(id) ++
    indexOutputVerbose(s"CALL db.index.fulltext.createRelationshipIndex('fulltext_rel', ['$relType'], ['$prop'], $defaultFulltextConfigString)")

  // general constraint help methods

  private def createDefaultConstraints(): Unit = {
    createDefaultUniquenessConstraint()
    createDefaultNodeKeyConstraint()
    createDefaultNodeExistsConstraint()
    createDefaultRelExistsConstraint()
    graph.awaitIndexesOnline()
  }

  private def createConstraint(constraintType: ShowConstraintType, name: String, label: String, property: String): Option[Map[String, Any]] = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val escapedLabel = s"`${escapeBackticks(label)}`"
    val escapedProperty = s"`${escapeBackticks(property)}`"
    var randomOptionsMap: Option[Map[String, Any]] = None

    val query = constraintType match {
      case UniqueConstraints =>
        val randomOptions = randomBtreeOptions()
        randomOptionsMap = Some(randomOptions._2)
        s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT (n.$escapedProperty) IS UNIQUE OPTIONS ${randomOptions._1}"
      case NodeKeyConstraints =>
        val randomOptions = randomBtreeOptions()
        randomOptionsMap = Some(randomOptions._2)
        s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT (n.$escapedProperty) IS NODE KEY OPTIONS ${randomOptions._1}"
      case NodeExistsConstraints =>
        s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT exists(n.$escapedProperty)"
      case RelExistsConstraints =>
        s"CREATE CONSTRAINT $escapedName ON ()-[r:$escapedLabel]-() ASSERT exists(r.$escapedProperty)"
      case unexpectedType =>
        throw new IllegalArgumentException(s"Unexpected constraint type for constraint create command: ${unexpectedType.prettyPrint}")
    }
    executeSingle(query)
    randomOptionsMap
  }

  private def constraintOutputBrief(id: Long, name: String, constraintType: String, entityType: String,
                                    labelsOrTypes: List[String], properties: List[String], ownedIndexId: Option[Long]): Map[String, Any] =
    Map("id" -> id,
      "name" -> name,
      "type" -> constraintType,
      "entityType" -> entityType,
      "labelsOrTypes" -> labelsOrTypes,
      "properties" -> properties,
      "ownedIndexId" -> ownedIndexId.orNull
    )

  // options cannot be handled by the normal CypherComparisonSupport assertions due to returning maps and arrays, so it is not included here
  private def constraintOutputVerbose(createStatement: String): Map[String, Any] = Map("createStatement" -> createStatement)

  // unique constraint

  private def createDefaultUniquenessConstraint(): Unit = graph.createUniqueConstraintWithName("constraint1", label, prop)

  private def defaultUniquenessBriefIndexOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "constraint1", "UNIQUE", "BTREE", "NODE", List(label), List(prop), defaultBtreeProvider)

  private def defaultUniquenessVerboseIndexOutput(id: Long): Map[String, Any] = defaultUniquenessBriefIndexOutput(id) ++
    indexOutputVerbose(s"CREATE CONSTRAINT `constraint1` ON (n:`$label`) ASSERT (n.`$prop`) IS UNIQUE OPTIONS $defaultBtreeOptionsString")

  private def defaultUniquenessBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint1", "UNIQUENESS", "NODE", List(label), List(prop), Some(id - 1))

  private def defaultUniquenessVerboseConstraintOutput(id: Long): Map[String, Any] = defaultUniquenessBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint1` ON (n:`$label`) ASSERT (n.`$prop`) IS UNIQUE OPTIONS $defaultBtreeOptionsString")

  // node key constraint

  private def createDefaultNodeKeyConstraint(): Unit = graph.createNodeKeyConstraintWithName("constraint2", label2, prop2)

  private def defaultNodeKeyBriefIndexOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "constraint2", "UNIQUE", "BTREE", "NODE", List(label2), List(prop2), defaultBtreeProvider)

  private def defaultNodeKeyVerboseIndexOutput(id: Long): Map[String, Any] = defaultNodeKeyBriefIndexOutput(id) ++
    indexOutputVerbose(s"CREATE CONSTRAINT `constraint2` ON (n:`$label2`) ASSERT (n.`$prop2`) IS NODE KEY OPTIONS $defaultBtreeOptionsString")

  private def defaultNodeKeyBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint2", "NODE_KEY", "NODE", List(label2), List(prop2), Some(id - 1))

  private def defaultNodeKeyVerboseConstraintOutput(id: Long): Map[String, Any] = defaultNodeKeyBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint2` ON (n:`$label2`) ASSERT (n.`$prop2`) IS NODE KEY OPTIONS $defaultBtreeOptionsString")

  // node exists constraint

  private def createDefaultNodeExistsConstraint(): Unit = graph.createNodeExistenceConstraintWithName("constraint3", label, prop2)

  private def defaultNodeExistsBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint3", "NODE_PROPERTY_EXISTENCE", "NODE", List(label), List(prop2), None)

  private def defaultNodeExistsVerboseConstraintOutput(id: Long): Map[String, Any] = defaultNodeExistsBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint3` ON (n:`$label`) ASSERT exists(n.`$prop2`)")

  // rel exists constraint

  private def createDefaultRelExistsConstraint(): Unit = graph.createRelationshipExistenceConstraintWithName("constraint4", relType, prop)

  private def defaultRelExistsBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint4", "RELATIONSHIP_PROPERTY_EXISTENCE", "RELATIONSHIP", List(relType), List(prop), None)

  private def defaultRelExistsVerboseConstraintOutput(id: Long): Map[String, Any] = defaultRelExistsBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint4` ON ()-[r:`$relType`]-() ASSERT exists(r.`$prop`)")

  // Create statements help methods

  private def verifyCanDropAndRecreateIndexesUsingCreateStatement(schemaType: String): Unit = {
    // GIVEN
    val allIndexes = executeSingle("SHOW INDEXES VERBOSE OUTPUT")
    val createStatements = allIndexes.columnAs("createStatement").toList
    val names = allIndexes.columnAs("name").toList
    val options = allIndexes.columnAs("options").toList

    // WHEN
    dropAllFromNames(names, schemaType)

    // THEN
    executeSingle("SHOW INDEXES VERBOSE OUTPUT").toList should be(empty)

    // WHEN
    recreateAllFromCreateStatements(createStatements)
    graph.awaitIndexesOnline()

    // THEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // The ids will not be the same and options is not comparable using CCS
    val skipColumns = List("id", "options")
    withoutColumns(result.toList, skipColumns) should be(withoutColumns(allIndexes.toList, skipColumns))
    val recreatedOptions = result.columnAs("options").toList
    for (i <- recreatedOptions.indices) assertCorrectOptionsMap(recreatedOptions(i), options(i).asInstanceOf[Map[String, Any]])
  }

  private def verifyCanDropAndRecreateConstraintsUsingCreateStatement(): Unit = {
    // GIVEN
    val allConstraints = executeSingle("SHOW CONSTRAINTS VERBOSE OUTPUT")
    val createStatements = allConstraints.columnAs("createStatement").toList
    val names = allConstraints.columnAs("name").toList
    val options = allConstraints.columnAs("options").toList

    // WHEN
    dropAllFromNames(names, "CONSTRAINT")

    // THEN
    executeSingle("SHOW CONSTRAINTS VERBOSE OUTPUT").toList should be(empty)

    // WHEN
    recreateAllFromCreateStatements(createStatements)
    graph.awaitIndexesOnline()

    // THEN
    val result = executeSingle("SHOW CONSTRAINTS VERBOSE OUTPUT")

    // The ids will not be the same and options is not comparable using CCS
    val skipColumns = List("id", "ownedIndexId", "options")
    withoutColumns(result.toList, skipColumns) should be(withoutColumns(allConstraints.toList, skipColumns))
    val recreatedOptions = result.columnAs("options").toList
    for (i <- recreatedOptions.indices) {
      val correctOption: Option[Map[String, Any]] = Option(options(i))
      val recreatedOption: Option[Map[String, Any]] = Option(recreatedOptions(i))

      (correctOption, recreatedOption) match {
        case (Some(correct), Some(recreated)) =>
          assertCorrectOptionsMap(recreated, correct.asInstanceOf[Map[String, Any]])
        case (Some(correct), None) =>
          fail(s"Expected options to $correct but was null.")
        case (None, Some(recreated)) =>
          fail(s"Expected options to be null but was $recreated.")
        case (None, None) =>
          // Options was expected to be null and was null, this is success.
      }
    }
  }

  private def dropAllFromNames(names: List[String], schemaType: String): Unit = {
    names.foreach(name => executeSingle(s"DROP $schemaType `$name`"))
  }

  private def recreateAllFromCreateStatements(createStatements: List[String]): Unit = {
    createStatements.foreach(statement => executeSingle(statement))
  }

  // Options help methods

  private def assertCorrectOptionsMap(options: Object, correctOptions: Map[String, Any]): Unit = {
    options match {
      case optionsMap: Map[String, Any] =>
        optionsMap.keys should be(Set("indexConfig", "indexProvider"))
        optionsMap("indexProvider") should be(correctOptions("indexProvider"))
        optionsMap("indexConfig") match {
          case config: Map[String, Any] =>
            val correctConfig = correctOptions("indexConfig").asInstanceOf[Map[String, Any]]
            config.keys should be(correctConfig.keys)
            config.keys.foreach(key => config(key) should be(correctConfig(key)))
          case noMap => fail(s"Index config was on the wrong format, expected a Map, but got a ${noMap.getClass}")
        }
      case noMap => fail(s"Index options was on the wrong format, expected a Map, but got a ${noMap.getClass}")
    }
  }

  private def randomBtreeOptions(): (String, Map[String, Object]) = {
    val provider = randomBtreeProvider()
    val indexConfig = randomBtreeSetting()
    val optionsString = s"{indexConfig: {${indexConfig._1}}, indexProvider: '$provider'}"
    val optionsMap = Map("indexConfig" -> indexConfig._2, "indexProvider" -> provider)
    (optionsString, optionsMap)
  }

  private def randomBtreeProvider(): String = {
    val availableProviders = SchemaIndex.values().map(value => value.providerName())
    randomValues.among(availableProviders)
  }

  private def randomBtreeSetting(): (String, Map[String, Array[Double]]) = {
    val indexSettings: Array[IndexSettingImpl] = IndexSettingImpl.values
    val configMap = indexSettings.foldLeft(Map.empty[String, Array[Double]])((acc, setting) => {
      val settingName = setting.getSettingName
      if (settingName.startsWith("spatial")) {
        val settingValue = randomSpatialValue(setting)
        acc + (settingName -> settingValue)
      } else {
        acc
      }
    })

    val configString = configMap.map {
      case (key, value) => s"`$key`: [${value.mkString(", ")}]"
    }.mkString(", ")

    (configString, configMap)
  }

  private def randomSpatialValue(indexSetting: IndexSettingImpl): Array[Double] = {
    indexSetting match {
      case SPATIAL_CARTESIAN_MIN =>
        negative(randomValues.nextCartesianPoint.coordinate)
      case SPATIAL_CARTESIAN_MAX =>
        positive(randomValues.nextCartesianPoint.coordinate)
      case SPATIAL_CARTESIAN_3D_MIN =>
        negative(randomValues.nextCartesian3DPoint.coordinate)
      case SPATIAL_CARTESIAN_3D_MAX =>
        positive(randomValues.nextCartesian3DPoint().coordinate())
      case SPATIAL_WGS84_MIN =>
        negative(randomValues.nextGeographicPoint().coordinate())
      case SPATIAL_WGS84_MAX =>
        positive(randomValues.nextGeographicPoint.coordinate)
      case SPATIAL_WGS84_3D_MIN =>
        negative(randomValues.nextGeographic3DPoint.coordinate)
      case SPATIAL_WGS84_3D_MAX =>
        positive(randomValues.nextGeographic3DPoint.coordinate)
      case setting => fail("Unexpected spatial index setting: " + setting.getSettingName)
    }
  }

  private def positive(values: Array[Double]): Array[Double] = {
    values.map(i => Math.abs(i))
  }

  private def negative(values: Array[Double]): Array[Double] = {
    values.map(i => -Math.abs(i))
  }

  private def randomFulltextSetting(): (String, Map[String, Any]) = {
    val eventuallyConsistent = FULLTEXT_EVENTUALLY_CONSISTENT.getSettingName
    val analyzer = FULLTEXT_ANALYZER.getSettingName
    val randomBoolean = randomValues.nextBoolean()
    val randomAnalyzer = getRandomAnalyzer
    val settingString = s"{`$eventuallyConsistent`: '$randomBoolean', `$analyzer`: '$randomAnalyzer'}"
    val settingMap: Map[String, Any] = Map(eventuallyConsistent -> randomBoolean, analyzer -> randomAnalyzer)
    (settingString, settingMap)
  }

  private def getRandomAnalyzer: String = {
    val analyzers = new util.ArrayList[AnalyzerProvider](Services.loadAll(classOf[AnalyzerProvider]))
    randomValues.among(analyzers).getName
  }

  private def withoutColumns(result: List[Map[String, Any]], columns: List[String]): List[Map[String, Any]] = {
    result.map(innerMap => innerMap.filterNot(entry => columns.contains(entry._1)))
  }

  private def escapeBackticks(str: String): String = str.replaceAll("`", "``")
}
