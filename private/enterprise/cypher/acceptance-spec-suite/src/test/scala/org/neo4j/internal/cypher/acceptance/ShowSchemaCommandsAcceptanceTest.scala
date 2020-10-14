/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util
import java.util.concurrent.ThreadLocalRandom

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.graphdb.schema.AnalyzerProvider
import org.neo4j.graphdb.schema.IndexSettingImpl
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.service.Services
import org.neo4j.values.storable.RandomValues

class ShowSchemaCommandsAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  private val defaultBtreeOptionsString: String =
    "{indexConfig: {" +
      "`spatial.cartesian-3d.max`: [1000000.0, 1000000.0, 1000000.0]," +
      "`spatial.cartesian-3d.min`: [-1000000.0, -1000000.0, -1000000.0]," +
      "`spatial.cartesian.max`: [1000000.0, 1000000.0]," +
      "`spatial.cartesian.min`: [-1000000.0, -1000000.0]," +
      "`spatial.wgs-84-3d.max`: [180.0, 90.0, 1000000.0]," +
      "`spatial.wgs-84-3d.min`: [-180.0, -90.0, -1000000.0]," +
      "`spatial.wgs-84.max`: [180.0, 90.0]," +
      "`spatial.wgs-84.min`: [-180.0, -90.0]}, indexProvider: 'native-btree-1.0'}"

  private val defaultBtreeOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      "spatial.cartesian-3d.max" -> Array(1000000.0, 1000000.0, 1000000.0),
      "spatial.cartesian-3d.min" -> Array(-1000000.0, -1000000.0, -1000000.0),
      "spatial.cartesian.max" -> Array(1000000.0, 1000000.0),
      "spatial.cartesian.min" -> Array(-1000000.0, -1000000.0),
      "spatial.wgs-84-3d.max" -> Array(180.0, 90.0, 1000000.0),
      "spatial.wgs-84-3d.min" -> Array(-180.0, -90.0, -1000000.0),
      "spatial.wgs-84.max" -> Array(180.0, 90.0),
      "spatial.wgs-84.min" -> Array(-180.0, -90.0)),
    "indexProvider" -> "native-btree-1.0")

  private val defaultFulltextConfigString = "{`fulltext.analyzer`: 'standard-no-stop-words',`fulltext.eventually_consistent`: 'false'}"

  private val defaultFulltextOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      "fulltext.analyzer" -> "standard-no-stop-words",
      "fulltext.eventually_consistent" -> false),
    "indexProvider" -> "fulltext-1.0")

  private val label = "Label"
  private val label2 = "Label2"
  private val labelWhitespace = "Label 1"
  private val labelWhitespace2 = "Label 2"
  private val labelBackticks = "`Label``4`"

  private val relType = "relType"
  private val relType2 = "relType2"
  private val relTypeWhitespace = "reltype 3"
  private val reltypeBackticks = "`rel`type`"

  private val prop = "prop"
  private val prop2 = "prop2"
  private val propWhitespace = "prop 1"
  private val propWhitespace2 = "prop 2"
  private val propBackticks = "`prop`4`"
  private val propBackticks2 = "`prop5``"

  private val random: ThreadLocalRandom = ThreadLocalRandom.current()
  private val randomValues: RandomValues = RandomValues.create(random)

  // SHOW INDEXES test

  test("show index should return empty result when there are no indexes") {
    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(empty)
  }

  test("should show index") {
    // GIVEN
    createBtreeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(List(btreeBriefOutput(1L)))
  }

  test("should show indexes in alphabetic order") {
    // GIVEN
    graph.createIndexWithName("poppy", "A", "prop")
    graph.createIndexWithName("benny", "C", "foo")
    graph.createIndexWithName("albert", "B", "foo")
    graph.createIndexWithName("charlie", "A", "foo")
    graph.createIndexWithName("xavier", "A", "bar")
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.columnAs("name").toList should equal(List("`albert`", "`benny`", "`charlie`", "`poppy`", "`xavier`"))
  }

  test("should show indexes backing constraints") {
    // GIVEN
    createUniquenessConstraint()
    graph.createNodeExistenceConstraintWithName("constraint3", "Label2", "prop")
    graph.createRelationshipExistenceConstraintWithName("constraint4", "Type", "prop")
    createNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW BTREE INDEXES")

    // THEN
    // Existence constraints are not backed by indexes so they should not be listed
    result.toList should be(List(uniquenessBriefOutput(1L), nodeKeyBriefOutput(5L)))
  }

  test("show indexes should show both btree and fulltext indexes") {
    // GIVEN
    createBtreeIndex()
    createFullTextNodeIndex()
    createFullTextRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(List(fulltextNodeBriefOutput(2L), fulltextRelBriefOutput(3L), btreeBriefOutput(1L)))
  }

  test("show all indexes should show both btree and fulltext indexes") {
    // GIVEN
    createBtreeIndex()
    createFullTextNodeIndex()
    createFullTextRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW ALL INDEXES")

    // THEN
    result.toList should be(List(fulltextNodeBriefOutput(2L), fulltextRelBriefOutput(3L), btreeBriefOutput(1L)))
  }

  test("show btree indexes should show only btree indexes") {
    // GIVEN
    createBtreeIndex()
    createFullTextNodeIndex()
    createFullTextRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW BTREE INDEXES")

    // THEN
    result.toList should be(List(btreeBriefOutput(1L)))
  }

  test("should show btree index with verbose output") {
    // GIVEN
    createBtreeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(btreeVerboseOutput(1L)))
  }

  test("should show fulltext indexes with verbose output") {
    // GIVEN
    createFullTextNodeIndex()
    createFullTextRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultFulltextOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(fulltextNodeVerboseOutput(1L), fulltextRelVerboseOutput(2L)))
  }

  test("should show indexes backing constraints with verbose output") {
    // GIVEN
    createUniquenessConstraint()
    createNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(uniquenessVerboseOutput(1L), nodeKeyVerboseOutput(3L)))
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
    createFulltextRelIndexWithRandomOptions("relType full-text backticks", List(reltypeBackticks), List(propBackticks))
    createFulltextRelIndexWithRandomOptions("``horrible `index`name`3``", List(relType), List(prop2))

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateIndexesUsingCreateStatement("INDEX")
  }

  test("show indexes should show valid create constraint statements") {

    // Indexes backing uniqueness constraints
    createConstraintWithRandomOptions("UNIQUE", "unique property", label, prop)
    createConstraintWithRandomOptions("UNIQUE", "unique property whitespace", labelWhitespace, propWhitespace)
    createConstraintWithRandomOptions("UNIQUE", "unique backticks", labelBackticks, propBackticks)
    createConstraintWithRandomOptions("UNIQUE", "``horrible`name`", label2, prop)

    // Indexes backing node key constraints
    createConstraintWithRandomOptions("NODE KEY", "node key", label2, prop2)
    createConstraintWithRandomOptions("NODE KEY", "node key whitespace", labelWhitespace2, propWhitespace2)
    createConstraintWithRandomOptions("NODE KEY", "node key backticks", labelBackticks, propBackticks2)
    createConstraintWithRandomOptions("NODE KEY", "``horrible`name2`", label, prop2)

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateIndexesUsingCreateStatement("CONSTRAINT")
  }

  // Setup methods for indexes and constraints used in multiple tests

  private def createBtreeIndex(): Unit = graph.createIndexWithName("my_index", "Person", "name", "age")

  private def createFullTextNodeIndex(): Unit = executeSingle("CALL db.index.fulltext.createNodeIndex('fulltext_node', ['Label'], ['prop'])")

  private def createFullTextRelIndex(): Unit = executeSingle("CALL db.index.fulltext.createRelationshipIndex('fulltext_rel', ['Type'], ['prop'])")

  private def createUniquenessConstraint(): Unit = graph.createUniqueConstraintWithName("constraint1", "Label", "property")

  private def createNodeKeyConstraint(): Unit = graph.createNodeKeyConstraintWithName("constraint2", "Label2", "property2")

  // General methods

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

  private def withoutColumns(result: List[Map[String, Any]], columns: List[String]): List[Map[String, Any]] = {
    result.map(innerMap => innerMap.filterNot(entry => columns.contains(entry._1)))
  }

  private def escapeBackticks(str: String): String = str.replaceAll("`", "``")

  // Btree index help methods

  private def createBtreeIndexWithRandomOptions(name: String, label: String, properties: List[String]): Unit = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val escapedLabel = s"`${escapeBackticks(label)}`"
    val escapedProperties = properties.map(p => s"n.`${escapeBackticks(p)}`").mkString(",")
    executeSingle(s"CREATE INDEX $escapedName FOR (n:$escapedLabel) ON ($escapedProperties) OPTIONS ${randomBtreeOptions()}")
  }

  private def btreeBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "`my_index`", "NONUNIQUE", "BTREE", "NODE", List("Person"), List("age", "name"), "native-btree-1.0")

  private def btreeVerboseOutput(id: Long): Map[String, Any] = btreeBriefOutput(id) ++
    indexOutputVerbose(s"CREATE INDEX `my_index` FOR (n:`Person`) ON (n.`age`, n.`name`) OPTIONS $defaultBtreeOptionsString")

  // Btree index backing constraint help methods

  private def createConstraintWithRandomOptions(constraintType: String, name: String, label: String, property: String): Unit = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val escapedLabel = s"`${escapeBackticks(label)}`"
    val escapedProperty = s"`${escapeBackticks(property)}`"
    executeSingle(s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT (n.$escapedProperty) IS $constraintType OPTIONS ${randomBtreeOptions()}")
  }

  private def uniquenessBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "`constraint1`", "UNIQUE", "BTREE", "NODE", List("Label"), List("property"), "native-btree-1.0")

  private def uniquenessVerboseOutput(id: Long): Map[String, Any] = uniquenessBriefOutput(id) ++
    indexOutputVerbose(s"CREATE CONSTRAINT `constraint1` ON (n:`Label`) ASSERT (n.`property`) IS UNIQUE OPTIONS $defaultBtreeOptionsString")

  private def nodeKeyBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "`constraint2`", "UNIQUE", "BTREE", "NODE", List("Label2"), List("property2"), "native-btree-1.0")

  private def nodeKeyVerboseOutput(id: Long): Map[String, Any] = nodeKeyBriefOutput(id) ++
    indexOutputVerbose(s"CREATE CONSTRAINT `constraint2` ON (n:`Label2`) ASSERT (n.`property2`) IS NODE KEY OPTIONS $defaultBtreeOptionsString")

  // Fulltext index help methods

  private def createFulltextNodeIndexWithRandomOptions(name: String, labelList: List[String], propertyList: List[String]): Unit = {
    val labels: String = labelList.map(l => s"'$l'").mkString(",")
    val properties: String = propertyList.map(p => s"'$p'").mkString(",")
    executeSingle(s"CALL db.index.fulltext.createNodeIndex('$name', [$labels], [$properties], ${randomFulltextSetting()})")
  }

  private def fulltextNodeBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "`fulltext_node`", "NONUNIQUE", "FULLTEXT", "NODE", List("Label"), List("prop"), "fulltext-1.0")

  private def fulltextNodeVerboseOutput(id: Long): Map[String, Any] = fulltextNodeBriefOutput(id) ++
    indexOutputVerbose(s"CALL db.index.fulltext.createNodeIndex('fulltext_node', ['Label'], ['prop'], $defaultFulltextConfigString)")

  private def createFulltextRelIndexWithRandomOptions(name: String, labelList: List[String], propertyList: List[String]): Unit = {
    val labels: String = labelList.map(l => s"'$l'").mkString(",")
    val properties: String = propertyList.map(p => s"'$p'").mkString(",")
    executeSingle(s"CALL db.index.fulltext.createRelationshipIndex('$name', [$labels], [$properties], ${randomFulltextSetting()})")
  }

  private def fulltextRelBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "`fulltext_rel`", "NONUNIQUE", "FULLTEXT", "RELATIONSHIP", List("Type"), List("prop"), "fulltext-1.0")

  private def fulltextRelVerboseOutput(id: Long): Map[String, Any] = fulltextRelBriefOutput(id) ++
    indexOutputVerbose(s"CALL db.index.fulltext.createRelationshipIndex('fulltext_rel', ['Type'], ['prop'], $defaultFulltextConfigString)")

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

  private def dropAllFromNames(names: List[String], schemaType: String): Unit = {
    names.foreach(name => executeSingle(s"DROP $schemaType $name"))
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

  private def randomBtreeOptions(): String = {
    s"{indexConfig: {${randomBtreeSetting()}}, indexProvider: 'native-btree-1.0'}"
  }

  private def randomBtreeSetting(): String = {
    val indexSettings: Array[IndexSettingImpl] = IndexSettingImpl.values
    indexSettings.foldLeft(List.empty[String])((acc, setting) => {
      val settingName = setting.getSettingName
      if (settingName.startsWith("spatial")) {
        val stringValue = randomSpatialValue(setting).mkString(", ")
        s"`$settingName`: [$stringValue]" :: acc
      } else {
        acc
      }
    }).mkString(", ")
  }

  private def randomSpatialValue(indexSetting: IndexSettingImpl): Array[Double] = {
    indexSetting match {
      case IndexSettingImpl.SPATIAL_CARTESIAN_MIN =>
        negative(randomValues.nextCartesianPoint.coordinate)
      case IndexSettingImpl.SPATIAL_CARTESIAN_MAX =>
        positive(randomValues.nextCartesianPoint.coordinate)
      case IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN =>
        negative(randomValues.nextCartesian3DPoint.coordinate)
      case IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX =>
        positive(randomValues.nextCartesian3DPoint().coordinate())
      case IndexSettingImpl.SPATIAL_WGS84_MIN =>
        negative(randomValues.nextGeographicPoint().coordinate())
      case IndexSettingImpl.SPATIAL_WGS84_MAX =>
        positive(randomValues.nextGeographicPoint.coordinate)
      case IndexSettingImpl.SPATIAL_WGS84_3D_MIN =>
        negative(randomValues.nextGeographic3DPoint.coordinate)
      case IndexSettingImpl.SPATIAL_WGS84_3D_MAX =>
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

  private def randomFulltextSetting(): String = {
    val eventuallyConsistent = s"`${IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT}`"
    val analyzer = s"`${IndexSettingImpl.FULLTEXT_ANALYZER}`"
    s"{$eventuallyConsistent: '${randomValues.nextBoolean()}', $analyzer: '${randomAnalyzer()}'}"
  }

  private def randomAnalyzer(): String = {
    val analyzers = new util.ArrayList[AnalyzerProvider](Services.loadAll(classOf[AnalyzerProvider]))
    randomValues.among(analyzers).getName
  }
}
