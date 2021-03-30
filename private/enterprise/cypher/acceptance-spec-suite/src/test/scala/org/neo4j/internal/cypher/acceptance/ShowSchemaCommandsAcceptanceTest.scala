/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util
import java.util.concurrent.ThreadLocalRandom

import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.schema.AnalyzerProvider
import org.neo4j.graphdb.schema.IndexSettingImpl
import org.neo4j.kernel.api.impl.fulltext.analyzer.providers.StandardNoStopWords
import org.neo4j.service.Services
import org.neo4j.values.storable.RandomValues

class ShowSchemaCommandsAcceptanceTest extends SchemaCommandsAcceptanceTestBase {
  /* Tests for listing indexes and constraints */

  private val defaultBtreeOptionsString: String =
    "{indexConfig: {" +
      s"`$cartesian3dMax`: [1000000.0, 1000000.0, 1000000.0]," +
      s"`$cartesian3dMin`: [-1000000.0, -1000000.0, -1000000.0]," +
      s"`$cartesianMax`: [1000000.0, 1000000.0]," +
      s"`$cartesianMin`: [-1000000.0, -1000000.0]," +
      s"`$wgs3dMax`: [180.0, 90.0, 1000000.0]," +
      s"`$wgs3dMin`: [-180.0, -90.0, -1000000.0]," +
      s"`$wgsMax`: [180.0, 90.0]," +
      s"`$wgsMin`: [-180.0, -90.0]}, " +
      s"indexProvider: '$nativeProvider'}"

  private val defaultBtreeOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      cartesian3dMax -> Array(1000000.0, 1000000.0, 1000000.0),
      cartesian3dMin -> Array(-1000000.0, -1000000.0, -1000000.0),
      cartesianMax -> Array(1000000.0, 1000000.0),
      cartesianMin -> Array(-1000000.0, -1000000.0),
      wgs3dMax -> Array(180.0, 90.0, 1000000.0),
      wgs3dMin -> Array(-180.0, -90.0, -1000000.0),
      wgsMax -> Array(180.0, 90.0),
      wgsMin -> Array(-180.0, -90.0)),
    "indexProvider" -> nativeProvider)

  private val defaultFulltextConfigString =
    s"{`$analyzer`: '${StandardNoStopWords.ANALYZER_NAME}',`$eventuallyConsistent`: 'false'}"

  private val defaultFulltextOptionsMap: Map[String, Object] = Map(
    "indexConfig" -> Map(
      analyzer -> StandardNoStopWords.ANALYZER_NAME,
      eventuallyConsistent -> false),
    "indexProvider" -> fulltextProvider)

  private val label2 = "Label2"
  private val labelWhitespace = "Label 1"
  private val labelWhitespace2 = "Label 2"
  private val labelBackticks = "`Label``3`"
  private val labelBackticks2 = "Label`4``"

  private val relType2 = "relType2"
  private val relTypeWhitespace = "reltype 3"
  private val relTypeBackticks = "`rel`type`"

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

  test("should show indexes in alphabetic order") {
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

  test("should show indexes with where in alphabetic order") {
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

  test("show index plan SHOW INDEXES with WHERE") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW INDEXES WHERE name STARTS WITH 'my'")

    // THEN
    result.executionPlanString() should include ("allIndexes, defaultColumns")
    result.executionPlanString() should include ("name STARTS WITH \"my\"")
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

  test("show node existence constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW NODE EXIST CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultNodeExistsBriefConstraintOutput(5L)))
  }

  test("show relationship existence constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW RELATIONSHIP EXISTENCE CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultRelExistsBriefConstraintOutput(6L)))
  }

  test("show existence constraints should filter constraints") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW PROPERTY EXISTENCE CONSTRAINTS")

    // THEN
    result.toList should be(List(defaultNodeExistsBriefConstraintOutput(5L), defaultRelExistsBriefConstraintOutput(6L)))
  }

  test("show exists constraints should filter constraints (deprecated syntax)") {
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
    val result = executeSingle("SHOW NODE EXISTENCE CONSTRAINTS VERBOSE OUTPUT")

    val options: List[Object] = result.columnAs("options").toList
    options should be(List(null))
    withoutColumns(result.toList, List("options")) should equal(List(defaultNodeExistsVerboseConstraintOutput(1L)))
  }

  test("should show rel exists constraint with verbose output") {
    // GIVEN
    createDefaultRelExistsConstraint()

    // WHEN
    val result = executeSingle("SHOW RELATIONSHIP PROPERTY EXIST CONSTRAINTS VERBOSE OUTPUT")

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
    val indexResult = executeSingle("SHOW INDEXES YIELD *")

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
    val indexResult = executeSingle("SHOW INDEXES YIELD *")

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
    createConstraint(NodeExistsConstraints(), "node prop exists", label2, prop2)
    createConstraint(NodeExistsConstraints(), "node prop exists whitespace", labelWhitespace, propWhitespace2)
    createConstraint(NodeExistsConstraints(), "node prop exists backticks", labelBackticks2, propBackticks)
    createConstraint(NodeExistsConstraints(), "horrible`name3``", label2, prop3)

    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  test("show constraints should show valid create statements for rel exists constraints") {
    createConstraint(RelExistsConstraints(), "rel prop exists", relType, prop)
    createConstraint(RelExistsConstraints(), "rel prop exists whitespace", relTypeWhitespace, propWhitespace)
    createConstraint(RelExistsConstraints(), "rel prop exists backticks", relTypeBackticks, propBackticks)
    createConstraint(RelExistsConstraints(), "horrible name`4````", relType, prop2)

    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  test("show constraints should show valid create statements for mixed constraints") {
    createConstraint(UniqueConstraints, "unique", label2, prop)
    createConstraint(NodeKeyConstraints, "node key", label, prop2)
    createConstraint(NodeExistsConstraints(), "node exists", label, prop3)
    createConstraint(RelExistsConstraints(), "rel exists", relType, prop3)

    graph.awaitIndexesOnline()
    verifyCanDropAndRecreateConstraintsUsingCreateStatement()
  }

  // general index help methods

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
      case _: NodeExistsConstraints =>
        s"CREATE CONSTRAINT $escapedName ON (n:$escapedLabel) ASSERT (n.$escapedProperty) IS NOT NULL"
      case _: RelExistsConstraints =>
        s"CREATE CONSTRAINT $escapedName ON ()-[r:$escapedLabel]-() ASSERT (r.$escapedProperty) IS NOT NULL"
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
    indexOutputBrief(id, "constraint1", "UNIQUE", "BTREE", "NODE", List(label), List(prop), nativeProvider)

  private def defaultUniquenessVerboseIndexOutput(id: Long): Map[String, Any] = defaultUniquenessBriefIndexOutput(id) ++
    indexOutputVerbose(s"CREATE CONSTRAINT `constraint1` ON (n:`$label`) ASSERT (n.`$prop`) IS UNIQUE OPTIONS $defaultBtreeOptionsString")

  private def defaultUniquenessBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint1", "UNIQUENESS", "NODE", List(label), List(prop), Some(id - 1))

  private def defaultUniquenessVerboseConstraintOutput(id: Long): Map[String, Any] = defaultUniquenessBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint1` ON (n:`$label`) ASSERT (n.`$prop`) IS UNIQUE OPTIONS $defaultBtreeOptionsString")

  // node key constraint

  private def createDefaultNodeKeyConstraint(): Unit = graph.createNodeKeyConstraintWithName("constraint2", label2, prop2)

  private def defaultNodeKeyBriefIndexOutput(id: Long): Map[String, Any] =
    indexOutputBrief(id, "constraint2", "UNIQUE", "BTREE", "NODE", List(label2), List(prop2), nativeProvider)

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
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint3` ON (n:`$label`) ASSERT (n.`$prop2`) IS NOT NULL")

  // rel exists constraint

  private def createDefaultRelExistsConstraint(): Unit = graph.createRelationshipExistenceConstraintWithName("constraint4", relType, prop)

  private def defaultRelExistsBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint4", "RELATIONSHIP_PROPERTY_EXISTENCE", "RELATIONSHIP", List(relType), List(prop), None)

  private def defaultRelExistsVerboseConstraintOutput(id: Long): Map[String, Any] = defaultRelExistsBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint4` ON ()-[r:`$relType`]-() ASSERT (r.`$prop`) IS NOT NULL")

  // Create statements help methods

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

  private def withoutColumns(result: List[Map[String, Any]], columns: List[String]): List[Map[String, Any]] = {
    result.map(innerMap => innerMap.filterNot(entry => columns.contains(entry._1)))
  }

  private def escapeBackticks(str: String): String = str.replaceAll("`", "``")
}
