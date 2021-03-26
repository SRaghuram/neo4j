/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.schema.AnalyzerProvider
import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.StandardNoStopWords
import org.neo4j.service.Services

import java.util

class ShowIndexCommandsAcceptanceTest extends ShowSchemaCommandsAcceptanceTestBase {
  /* Tests for listing indexes */

  private val defaultFulltextConfigString =
    s"{`$analyzer`: '${StandardNoStopWords.ANALYZER_NAME}',`$eventuallyConsistent`: 'false'}"

  private val defaultFulltextOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      analyzer -> StandardNoStopWords.ANALYZER_NAME,
      eventuallyConsistent -> false),
    "indexProvider" -> fulltextProvider)

  private val relType2 = "relType2"

  test("show index should return empty result when there are no indexes") {
    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(empty)
  }

  test("should show node index") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(List(defaultBtreeNodeBriefOutput(1L)))
  }

  test("should show relationship property index") {
    // GIVEN
    createDefaultBtreeRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES")

    // THEN
    result.toList should be(List(defaultBtreeRelBriefOutput(1L)))
  }

  test("should show node index with brief output (deprecated)") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES BRIEF")

    // THEN
    result.toList should be(List(defaultBtreeNodeBriefOutput(1L)))
  }

  test("should show relationship property index with brief output (deprecated)") {
    // GIVEN
    createDefaultBtreeRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES BRIEF")

    // THEN
    result.toList should be(List(defaultBtreeRelBriefOutput(1L)))
  }

  test("should show indexes in default alphabetic order") {
    // GIVEN
    graph.createNodeIndexWithName("poppy", label, propWhitespace)
    graph.createNodeIndexWithName("benny", label2, prop, prop2)
    graph.createRelationshipIndexWithName("albert", label2, prop2)
    graph.createNodeIndexWithName("charlie", label, prop2)
    graph.createRelationshipIndexWithName("xavier", label, prop)
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
    result.toList should be(List(defaultFulltextNodeBriefOutput(3L), defaultFulltextRelBriefOutput(4L), defaultBtreeNodeBriefOutput(1L), defaultBtreeRelBriefOutput(2L)))
  }

  test("show all indexes should show both btree and fulltext indexes") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW ALL INDEXES")

    // THEN
    result.toList should be(List(defaultFulltextNodeBriefOutput(3L), defaultFulltextRelBriefOutput(4L), defaultBtreeNodeBriefOutput(1L), defaultBtreeRelBriefOutput(2L)))
  }

  test("show btree indexes should show only btree indexes") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW BTREE INDEXES")

    // THEN
    result.toList should be(List(defaultBtreeNodeBriefOutput(1L), defaultBtreeRelBriefOutput(2L)))
  }

  test("show fulltext indexes should show only fulltext indexes") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW FULLTEXT INDEXES")

    // THEN
    result.toList should be(List(defaultFulltextNodeBriefOutput(3L), defaultFulltextRelBriefOutput(4L)))
  }

  test("should show btree node index with yield *") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD *")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultBtreeNodeVerboseOutput(1L)))
  }

  test("should show btree relationship property index with yield *") {
    // GIVEN
    createDefaultBtreeRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD *")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultBtreeRelVerboseOutput(1L)))
  }

  test("should show fulltext indexes with yield *") {
    // GIVEN
    createDefaultFullTextNodeIndex()
    createDefaultFullTextRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD *")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultFulltextOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultFulltextNodeVerboseOutput(1L), defaultFulltextRelVerboseOutput(2L)))
  }

  test("should show indexes backing constraints with yield *") {
    // GIVEN
    createDefaultUniquenessConstraint()
    createDefaultNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD *")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultUniquenessVerboseIndexOutput(1L), defaultNodeKeyVerboseIndexOutput(3L)))
  }

  test("should show all indexes with verbose output (deprecated)") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    createDefaultBtreeRelIndex()
    createDefaultFullTextNodeIndex()
    createDefaultFullTextRelIndex()
    createDefaultUniquenessConstraint()
    createDefaultNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES VERBOSE OUTPUT")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    assertCorrectOptionsMap(options(0), defaultBtreeOptionsMap) // constraint1
    assertCorrectOptionsMap(options(1), defaultBtreeOptionsMap) // constraint2
    assertCorrectOptionsMap(options(2), defaultFulltextOptionsMap) // fulltext_node
    assertCorrectOptionsMap(options(3), defaultFulltextOptionsMap) // fulltext_rel
    assertCorrectOptionsMap(options(4), defaultBtreeOptionsMap) // my_node_index
    assertCorrectOptionsMap(options(5), defaultBtreeOptionsMap) // my_rel_index

    withoutColumns(result.toList, List("options")) should equal(List(
      defaultUniquenessVerboseIndexOutput(5L), // constraint1
      defaultNodeKeyVerboseIndexOutput(7L), // constraint2
      defaultFulltextNodeVerboseOutput(3L), // fulltext_node
      defaultFulltextRelVerboseOutput(4L), // fulltext_rel
      defaultBtreeNodeVerboseOutput(1L), // my_node_index
      defaultBtreeRelVerboseOutput(2L), // my_rel_index
    ))
  }

  test("should show correct options for btree node index with random options") {
    // GIVEN
    val randomOptions = createBtreeNodeIndexWithRandomOptions("btree", label, List(prop2))
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD *")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.size should be(1)
    assertCorrectOptionsMap(options.head, randomOptions)
  }

  test("should show correct options for btree relationship property index with random options") {
    // GIVEN
    val randomOptions = createBtreeRelIndexWithRandomOptions("btree", label, List(prop2))
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD *")

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
    val result = executeSingle("SHOW INDEXES YIELD *")

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
    val result = executeSingle("SHOW INDEXES YIELD *")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    options.size should be(1)
    assertCorrectOptionsMap(options.head, randomOptions)
  }

  test("show indexes should show valid create index statements") {

    // Btree node indexes
    createBtreeNodeIndexWithRandomOptions("btree", label, List(prop))
    createBtreeNodeIndexWithRandomOptions("btree composite", label, List(prop, prop2))
    createBtreeNodeIndexWithRandomOptions("btree whitespace", labelWhitespace, List(propWhitespace))
    createBtreeNodeIndexWithRandomOptions("btree backticks", labelBackticks, List(propBackticks))
    createBtreeNodeIndexWithRandomOptions("``horrible `index`name```", label, List(prop2))

    // Btree relationship property indexes
    createBtreeRelIndexWithRandomOptions("relType btree", relType, List(prop))
    createBtreeRelIndexWithRandomOptions("relType btree composite", relType, List(prop, prop2))
    createBtreeRelIndexWithRandomOptions("relType btree whitespace", relTypeWhitespace, List(propWhitespace))
    createBtreeRelIndexWithRandomOptions("relType btree backticks", relTypeBackticks, List(propBackticks))
    createBtreeRelIndexWithRandomOptions("``horrible `index`name``1`", relType, List(prop2))

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

  // Filtering

  test("should show index with yield") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD name, type")

    // THEN
    result.toList should be(List(defaultBtreeNodeVerboseOutput(1L).filterKeys(k => Seq("name", "type").contains(k))))
  }

  test("should show index with yield, where and return and brief columns") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD name, type WHERE type = 'BTREE' RETURN name, type")

    // THEN
    result.toList should be(List(defaultBtreeNodeVerboseOutput(1L).filterKeys(k => Seq("name", "type").contains(k)), defaultBtreeRelVerboseOutput(2L).filterKeys(k => Seq("name", "type").contains(k))))
  }

  test("should show index with yield, where and return and verbose columns") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD name, type, createStatement WHERE type = 'BTREE' RETURN name, createStatement")

    // THEN
    result.toList should be(List(defaultBtreeNodeVerboseOutput(1L).filterKeys(k => Seq("name", "createStatement").contains(k)), defaultBtreeRelVerboseOutput(2L).filterKeys(k => Seq("name", "createStatement").contains(k))))
  }

  test("should show index with yield, where and return for relationship property index") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW BTREE INDEXES YIELD name, entityType WHERE entityType = 'RELATIONSHIP' RETURN name")

    // THEN
    result.toList should be(List(defaultBtreeRelVerboseOutput(2L).filterKeys(k => Seq("name").contains(k))))
  }

  test("should show index with yield, where and return for fulltext index") {
    // GIVEN
    createDefaultIndexes()

    // WHEN
    val result = executeSingle("SHOW FULLTEXT INDEXES YIELD name, entityType WHERE entityType = 'RELATIONSHIP' RETURN name")

    // THEN
    result.toList should be(List(defaultFulltextRelVerboseOutput(2L).filterKeys(k => Seq("name").contains(k))))
  }

  test("should show index with full yield") {
    createDefaultBtreeNodeIndex()
    createDefaultConstraints()
    graph.awaitIndexesOnline()

    // WHEN
    val result = execute("SHOW INDEXES YIELD name AS nm ORDER BY nm SKIP 1 LIMIT 10 WHERE nm starts with 'c' RETURN *")

    // THEN
    result.toList should be(List(Map("nm" -> "constraint2")))
  }

  test("should show index with yield, return and aggregations") {
    // GIVEN
    createDefaultConstraints()
    createDefaultFullTextNodeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD name, type RETURN collect(name) as names, type order by size(names)")

    // THEN
    result.toList should be(List(
      Map("names" -> List("fulltext_node"), "type" -> "FULLTEXT"),
      Map("names" -> List("constraint1", "constraint2"), "type" -> "BTREE"),
    ))
  }

  test("should show index with yield, where, return and aliasing") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES YIELD name as options, options as name where size(options) > 0 RETURN options as name")

    // THEN
    result.toList should be(List(defaultBtreeNodeBriefOutput(1L).filterKeys(_ == "name")))
  }

  test("should show index with where on type") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    createDefaultFullTextNodeIndex()
    createDefaultFullTextRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES WHERE type = 'BTREE'")

    // THEN
    result.toList should be(List(defaultBtreeNodeBriefOutput(1L)))
  }

  test("should show index with where on entity") {
    // GIVEN
    createDefaultBtreeNodeIndex()
    createDefaultBtreeRelIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES WHERE entityType = 'RELATIONSHIP'")

    // THEN
    result.toList should be(List(defaultBtreeRelBriefOutput(2L)))
  }

  test("should show indexes with where in default alphabetic order") {
    // GIVEN
    graph.createNodeIndexWithName("poppy", label, propWhitespace)
    graph.createNodeIndexWithName("benny", label2, prop, prop2)
    graph.createNodeIndexWithName("albert", label2, prop2)
    graph.createNodeIndexWithName("charlie", label, prop2)
    graph.createNodeIndexWithName("xavier", label, prop)
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW INDEXES WHERE NOT name CONTAINS 'x'")

    // THEN
    result.columnAs("name").toList should equal(List("albert", "benny", "charlie", "poppy"))
  }

  test("should show index with where and exists sub-clause") {
    // GIVEN
    createLabeledNode(Map(prop -> "foo", prop2 -> "bar", prop3 -> "BTREE"), label)
    createDefaultBtreeNodeIndex()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(
      s"""SHOW INDEXES WHERE EXISTS {
        | MATCH (n: $label {$prop3: type})
        |}""".stripMargin)

    // THEN
    result.toList should be(List(defaultBtreeNodeBriefOutput(1L)))
  }

  test("should show index with multiple order by") {
    // GIVEN
    createDefaultIndexes()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(
      s"""SHOW INDEXES YIELD * ORDER BY type DESC RETURN name, type ORDER BY name ASC""".stripMargin)

    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").containingArgument("name ASC")
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").containingArgument("type DESC")

    // THEN
    result.toList should be(List(
      Map("name" -> "fulltext_node", "type" -> "FULLTEXT"),
      Map("name" -> "fulltext_rel", "type" -> "FULLTEXT"),
      Map("name" -> "my_node_index", "type" -> "BTREE"),
      Map("name" -> "my_rel_index", "type" -> "BTREE")
    ))

  }

  test("should fail to show index with yield, return with aggregation and illegal order by") {
    // GIVEN
    graph.awaitIndexesOnline()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("SHOW INDEXES YIELD name, type RETURN collect(name) as names order by type")
    }

    // THEN
    exception.getMessage should startWith("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: type")
  }

  test("should fail to show index with where on verbose column") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("SHOW INDEXES WHERE isEmpty(options)")
    }

    // THEN
    exception.getMessage should startWith("Variable `options` not defined")
  }

  test("should fail to show index with where on non-existing columns") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("SHOW INDEXES WHERE foo = 'UNIQUE'")
    }

    // THEN
    exception.getMessage should include("Variable `foo` not defined")
  }

  // Planner tests

  test("show index plan BTREE and VERBOSE (deprecated)") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW BTREE INDEXES VERBOSE")

    // THEN
    result.executionPlanString() should include ("btreeIndexes, allColumns")
  }

  test("show index plan BTREE and YIELD *") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW BTREE INDEXES YIELD *")

    // THEN
    result.executionPlanString() should include ("btreeIndexes, allColumns")
  }

  test("show index plan ALL INDEXES and BRIEF (deprecated)") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW ALL INDEXES BRIEF")

    // THEN
    result.executionPlanString() should include ("allIndexes, defaultColumns")
  }

  test("show index plan SHOW INDEXES") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW INDEXES")

    // THEN
    result.executionPlanString() should include ("allIndexes, defaultColumns")
  }

  test("show index plan SHOW FULLTEXT INDEXES") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW FULLTEXT INDEXES")

    // THEN
    result.executionPlanString() should include ("fulltextIndexes, defaultColumns")
  }

  test("show index plan SHOW INDEXES with WHERE") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW INDEXES WHERE name STARTS WITH 'my'")

    // THEN
    result.executionPlanString() should include ("allIndexes, defaultColumns")
    result.executionPlanString() should include ("name STARTS WITH \"my\"")
  }

  // General index help methods

  private def createDefaultIndexes(): Unit = {
    createDefaultBtreeNodeIndex()
    createDefaultBtreeRelIndex()
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

  private def escapeVariablesAndGenerateRandomOptions(name: String, entity: String, properties: List[String]) = {
    val escapedName = s"`${escapeBackticks(name)}`"
    val escapedEntity = s"`${escapeBackticks(entity)}`"
    val escapedProperties = properties.map(p => s"e.`${escapeBackticks(p)}`").mkString(",")
    val randomOptions = randomBtreeOptions()
    (escapedName, escapedEntity, escapedProperties, randomOptions)
  }

  private def createBtreeNodeIndexWithRandomOptions(name: String, label: String, properties: List[String]): Map[String, Object] = {
    val (escapedName, escapedLabel, escapedProperties, randomOptions) = escapeVariablesAndGenerateRandomOptions(name, label, properties)
    executeSingle(s"CREATE INDEX $escapedName FOR (e:$escapedLabel) ON ($escapedProperties) OPTIONS ${randomOptions._1}")
    randomOptions._2
  }

  private def createDefaultBtreeNodeIndex(): Unit = graph.createNodeIndexWithName("my_node_index", label, prop2, prop)

  private def defaultBtreeNodeBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "my_node_index", "NONUNIQUE", "BTREE", "NODE", List(label), List(prop2, prop), "native-btree-1.0")

  private def defaultBtreeNodeVerboseOutput(id: Long): Map[String, Any] = defaultBtreeNodeBriefOutput(id) ++
    indexOutputVerbose(s"CREATE INDEX `my_node_index` FOR (n:`$label`) ON (n.`$prop2`, n.`$prop`) OPTIONS $defaultBtreeOptionsString")

  private def createBtreeRelIndexWithRandomOptions(name: String, relType: String, properties: List[String]): Map[String, Object] = {
    val (escapedName, escapedType, escapedProperties, randomOptions) = escapeVariablesAndGenerateRandomOptions(name, relType, properties)
    executeSingle(s"CREATE INDEX $escapedName FOR ()-[e:$escapedType]-() ON ($escapedProperties) OPTIONS ${randomOptions._1}")
    randomOptions._2
  }

  private def createDefaultBtreeRelIndex(): Unit = graph.createRelationshipIndexWithName("my_rel_index", relType, prop2)

  private def defaultBtreeRelBriefOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "my_rel_index", "NONUNIQUE", "BTREE", "RELATIONSHIP", List(relType), List(prop2), "native-btree-1.0")

  private def defaultBtreeRelVerboseOutput(id: Long): Map[String, Any] = defaultBtreeRelBriefOutput(id) ++
    indexOutputVerbose(s"CREATE INDEX `my_rel_index` FOR ()-[r:`$relType`]-() ON (r.`$prop2`) OPTIONS $defaultBtreeOptionsString")

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

  // Constraints help methods

  private def defaultUniquenessBriefIndexOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "constraint1", "UNIQUE", "BTREE", "NODE", List(label), List(prop), nativeProvider)

  private def defaultUniquenessVerboseIndexOutput(id: Long): Map[String, Any] = defaultUniquenessBriefIndexOutput(id) ++
    indexOutputVerbose(s"CREATE CONSTRAINT `constraint1` ON (n:`$label`) ASSERT (n.`$prop`) IS UNIQUE OPTIONS $defaultBtreeOptionsString")

  private def defaultNodeKeyBriefIndexOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "constraint2", "UNIQUE", "BTREE", "NODE", List(label2), List(prop2), nativeProvider)

  private def defaultNodeKeyVerboseIndexOutput(id: Long): Map[String, Any] = defaultNodeKeyBriefIndexOutput(id) ++
    indexOutputVerbose(s"CREATE CONSTRAINT `constraint2` ON (n:`$label2`) ASSERT (n.`$prop2`) IS NODE KEY OPTIONS $defaultBtreeOptionsString")

  // Create statements and options help methods

  private def verifyCanDropAndRecreateIndexesUsingCreateStatement(schemaType: String): Unit = {
    // GIVEN
    val allIndexes = executeSingle("SHOW INDEXES YIELD *")
    val createStatements = allIndexes.columnAs("createStatement").toList
    val names = allIndexes.columnAs("name").toList
    val options = allIndexes.columnAs("options").toList

    // WHEN
    dropAllFromNames(names, schemaType)

    // THEN
    executeSingle("SHOW INDEXES YIELD *").toList should be(empty)

    // WHEN
    recreateAllFromCreateStatements(createStatements)
    graph.awaitIndexesOnline()

    // THEN
    val result = executeSingle("SHOW INDEXES YIELD *")

    // The ids will not be the same and options is not comparable using CCS
    val skipColumns = List("id", "options")
    withoutColumns(result.toList, skipColumns) should be(withoutColumns(allIndexes.toList, skipColumns))
    val recreatedOptions = result.columnAs("options").toList
    for (i <- recreatedOptions.indices) assertCorrectOptionsMap(recreatedOptions(i), options(i).asInstanceOf[Map[String, Any]])
  }

  private def randomFulltextSetting(): (String, Map[String, Any]) = {
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
}
