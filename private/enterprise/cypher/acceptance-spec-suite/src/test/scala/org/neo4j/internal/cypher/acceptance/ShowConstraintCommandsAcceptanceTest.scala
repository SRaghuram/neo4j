/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.exceptions.SyntaxException

class ShowConstraintCommandsAcceptanceTest extends ShowSchemaCommandsAcceptanceTestBase {
  /* Tests for listing constraints */

  private val labelBackticks2 = "Label`4``"

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

  test("should show constraint with brief output (deprecated)") {
    // GIVEN
    createDefaultUniquenessConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS BRIEF")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L)))
  }

  test("should show constraints in default alphabetic order") {
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

  test("should show unique constraint with yield *") {
    // GIVEN
    createDefaultUniquenessConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW UNIQUE CONSTRAINTS YIELD *")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultUniquenessVerboseConstraintOutput(2L)))
  }

  test("should show node key constraint with yield *") {
    // GIVEN
    createDefaultNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW NODE KEY CONSTRAINTS YIELD *")

    val options: List[Object] = result.columnAs("options").toList
    options.foreach(option => assertCorrectOptionsMap(option, defaultBtreeOptionsMap))
    withoutColumns(result.toList, List("options")) should equal(List(defaultNodeKeyVerboseConstraintOutput(2L)))
  }

  test("should show node exists constraint with yield *") {
    // GIVEN
    createDefaultNodeExistsConstraint()

    // WHEN
    val result = executeSingle("SHOW NODE EXIST CONSTRAINTS YIELD *")

    val options: List[Object] = result.columnAs("options").toList
    options should be(List(null))
    withoutColumns(result.toList, List("options")) should equal(List(defaultNodeExistsVerboseConstraintOutput(1L)))
  }

  test("should show rel exists constraint with yield *") {
    // GIVEN
    createDefaultRelExistsConstraint()

    // WHEN
    val result = executeSingle("SHOW RELATIONSHIP EXIST CONSTRAINTS YIELD *")

    val options: List[Object] = result.columnAs("options").toList
    options should be(List(null))
    withoutColumns(result.toList, List("options")) should equal(List(defaultRelExistsVerboseConstraintOutput(1L)))
  }

  test("should show all constraints with verbose output (deprecated)") {
    // GIVEN
    createDefaultUniquenessConstraint()
    createDefaultNodeKeyConstraint()
    createDefaultNodeExistsConstraint()
    createDefaultRelExistsConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS VERBOSE")

    // THEN
    val options: List[Object] = result.columnAs("options").toList
    assertCorrectOptionsMap(options(0), defaultBtreeOptionsMap) // constraint1
    assertCorrectOptionsMap(options(1), defaultBtreeOptionsMap) // constraint2
    options(2) should be(null) // constraint3
    options(3) should be(null) // constraint4

    withoutColumns(result.toList, List("options")) should equal(List(
      defaultUniquenessVerboseConstraintOutput(2L), // constraint1
      defaultNodeKeyVerboseConstraintOutput(4L), // constraint2
      defaultNodeExistsVerboseConstraintOutput(5L), // constraint3
      defaultRelExistsVerboseConstraintOutput(6L), // constraint4
    ))
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
    val constraintResult = executeSingle("SHOW CONSTRAINTS YIELD *")

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
    val constraintResult = executeSingle("SHOW CONSTRAINTS YIELD *")

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

  // Filtering

  test("should show constraints with yield") {
    // GIVEN
    createDefaultUniquenessConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS YIELD name, type")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L).filterKeys(k => Seq("name", "type").contains(k))))
  }

  test("should show constraints with yield, where and return and brief columns") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS YIELD name, type WHERE type = 'NODE_KEY' RETURN name, type")

    // THEN
    result.toList should be(List(defaultNodeKeyVerboseConstraintOutput(4L).filterKeys(k => Seq("name", "type").contains(k))))
  }

  test("should show constraints with yield, where and return and verbose columns") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS YIELD name, type, createStatement WHERE type = 'UNIQUENESS' RETURN name, createStatement")

    // THEN
    result.toList should be(List(defaultUniquenessVerboseConstraintOutput(4L).filterKeys(k => Seq("name", "createStatement").contains(k))))
  }

  test("should show constraints with full yield") {
    createConstraint(UniqueConstraints, "my_constraint", label, prop3) // to filter something in the WHERE
    createDefaultConstraints()

    // WHEN
    val result = execute("SHOW CONSTRAINTS YIELD name AS nm ORDER BY nm SKIP 1 LIMIT 2 WHERE nm starts with 'c' RETURN *")

    // THEN
    result.toList should be(List(Map("nm" -> "constraint2"), Map("nm" -> "constraint3")))
  }

  test("should show constraints with yield, return and aggregations") {
    // GIVEN
    createConstraint(NodeExistsConstraints(), "my_constraint", label, prop3) // to get more than 1 constraint of one type
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW EXISTENCE CONSTRAINTS YIELD name, type RETURN collect(name) as names, type order by size(names)")

    // THEN
    result.toList should be(List(
      Map("names" -> List("constraint4"), "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE"),
      Map("names" -> List("constraint3", "my_constraint"), "type" -> "NODE_PROPERTY_EXISTENCE"),
    ))
  }

  test("should show constraints with yield, where, return and aliasing") {
    // GIVEN
    createDefaultUniquenessConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS YIELD name as options, options as name where size(options) > 0 RETURN options as name")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L).filterKeys(_ == "name")))
  }

  test("should show constraints with where") {
    // GIVEN
    createDefaultConstraints()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS WHERE type = 'UNIQUENESS'")

    // THEN
    result.toList should be(List(defaultUniquenessBriefConstraintOutput(2L)))
  }

  test("should show constraints with where in default alphabetic order") {
    // GIVEN
    graph.createUniqueConstraintWithName("poppy", label, propWhitespace)
    graph.createNodeKeyConstraintWithName("benny", label2, prop, prop2)
    graph.createNodeExistenceConstraintWithName("albert", label2, prop2)
    graph.createRelationshipExistenceConstraintWithName("charlie", label, prop2)
    graph.createUniqueConstraintWithName("xavier", label, prop)
    graph.createUniqueConstraintWithName("danny", label, prop3)
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle("SHOW CONSTRAINTS WHERE NOT name CONTAINS 'x'")

    // THEN
    result.columnAs("name").toList should equal(List("albert", "benny", "charlie", "danny", "poppy"))
  }

  test("should show constraints with where and exists sub-clause") {
    // GIVEN
    createLabeledNode(Map(prop -> "foo", prop2 -> "bar", prop3 -> "NODE_KEY"), label)
    createDefaultNodeKeyConstraint()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(
      s"""SHOW CONSTRAINTS WHERE EXISTS {
         | MATCH (n: $label {$prop3: type})
         |}""".stripMargin)

    // THEN
    result.toList should be(List(defaultNodeKeyBriefConstraintOutput(2L)))
  }

  test("should show constraints with multiple order by") {
    // GIVEN
    createDefaultConstraints()
    graph.awaitIndexesOnline()

    // WHEN
    val result = executeSingle(
      s"""SHOW CONSTRAINTS YIELD * ORDER BY type DESC RETURN name, type ORDER BY name ASC""".stripMargin)

    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").containingArgument("name ASC")
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").containingArgument("type DESC")

    // THEN
    result.toList should be(List(
      Map("name" -> "constraint1", "type" -> "UNIQUENESS"),
      Map("name" -> "constraint2", "type" -> "NODE_KEY"),
      Map("name" -> "constraint3", "type" -> "NODE_PROPERTY_EXISTENCE"),
      Map("name" -> "constraint4", "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE")
    ))
  }

  test("should fail to show constraints with yield, return with aggregation and illegal order by") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("SHOW CONSTRAINTS YIELD name, type RETURN collect(name) as names order by type")
    }

    // THEN
    exception.getMessage should startWith("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: type")
  }

  test("should fail to show constraints with where on verbose column") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("SHOW CONSTRAINTS WHERE isEmpty(options)")
    }

    // THEN
    exception.getMessage should startWith("Variable `options` not defined")
  }

  test("should fail to show constraints with where on non-existing columns") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      executeSingle("SHOW CONSTRAINTS WHERE foo = 'UNIQUENESS'")
    }

    // THEN
    exception.getMessage should include("Variable `foo` not defined")
  }

  // Planner tests

  test("show constraints plan SHOW CONSTRAINTS") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("allConstraints, defaultColumns")
  }

  test("show constraints plan SHOW ALL CONSTRAINTS") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW ALL CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("allConstraints, defaultColumns")
  }

  test("show constraints plan SHOW NODE KEY CONSTRAINTS") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW NODE KEY CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("nodeKeyConstraints, defaultColumns")
  }

  test("show constraints plan SHOW UNIQUE CONSTRAINTS") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW UNIQUE CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("uniquenessConstraints, defaultColumns")
  }

  test("show constraints plan SHOW PROPERTY EXIST CONSTRAINTS") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW PROPERTY EXIST CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("existenceConstraints, defaultColumns")
  }

  test("show constraints plan SHOW NODE EXISTENCE CONSTRAINTS") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW NODE EXISTENCE CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("nodeExistenceConstraints, defaultColumns")
  }

  test("show constraints plan SHOW REL EXIST CONSTRAINTS") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW REL EXIST CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("relationshipExistenceConstraints, defaultColumns")
  }

  test("show constraints plan YIELD *") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW CONSTRAINTS YIELD *")

    // THEN
    result.executionPlanString() should include ("allConstraints, allColumns")
  }

  test("show constraints plan with WHERE") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW CONSTRAINTS WHERE name STARTS WITH 'my'")

    // THEN
    result.executionPlanString() should include ("allConstraints, defaultColumns")
    result.executionPlanString() should include ("name STARTS WITH \"my\"")
  }

  test("show constraints plan BRIEF (deprecated)") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW CONSTRAINTS BRIEF")

    // THEN
    result.executionPlanString() should include ("allConstraints, defaultColumns")
  }

  test("show constraints plan VERBOSE (deprecated)") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW CONSTRAINTS VERBOSE")

    // THEN
    result.executionPlanString() should include ("allConstraints, allColumns")
  }

  test("show constraints plan SHOW EXISTS CONSTRAINTS (deprecated)") {
    // WHEN
    val result = executeSingle("EXPLAIN SHOW EXISTS CONSTRAINTS")

    // THEN
    result.executionPlanString() should include ("existenceConstraints, defaultColumns")
  }

  // Help methods

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

  private def defaultUniquenessBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint1", "UNIQUENESS", "NODE", List(label), List(prop), Some(id - 1))

  private def defaultUniquenessVerboseConstraintOutput(id: Long): Map[String, Any] = defaultUniquenessBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint1` ON (n:`$label`) ASSERT (n.`$prop`) IS UNIQUE OPTIONS $defaultBtreeOptionsString")

  private def defaultNodeKeyBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint2", "NODE_KEY", "NODE", List(label2), List(prop2), Some(id - 1))

  private def defaultNodeKeyVerboseConstraintOutput(id: Long): Map[String, Any] = defaultNodeKeyBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint2` ON (n:`$label2`) ASSERT (n.`$prop2`) IS NODE KEY OPTIONS $defaultBtreeOptionsString")

  private def defaultNodeExistsBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint3", "NODE_PROPERTY_EXISTENCE", "NODE", List(label), List(prop2), None)

  private def defaultNodeExistsVerboseConstraintOutput(id: Long): Map[String, Any] = defaultNodeExistsBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint3` ON (n:`$label`) ASSERT (n.`$prop2`) IS NOT NULL")

  private def defaultRelExistsBriefConstraintOutput(id: Long): Map[String, Any] =
    constraintOutputBrief(id, "constraint4", "RELATIONSHIP_PROPERTY_EXISTENCE", "RELATIONSHIP", List(relType), List(prop), None)

  private def defaultRelExistsVerboseConstraintOutput(id: Long): Map[String, Any] = defaultRelExistsBriefConstraintOutput(id) ++
    constraintOutputVerbose(s"CREATE CONSTRAINT `constraint4` ON ()-[r:`$relType`]-() ASSERT (r.`$prop`) IS NOT NULL")

  private def verifyCanDropAndRecreateConstraintsUsingCreateStatement(): Unit = {
    // GIVEN
    val allConstraints = executeSingle("SHOW CONSTRAINTS YIELD *")
    val createStatements = allConstraints.columnAs("createStatement").toList
    val names = allConstraints.columnAs("name").toList
    val options = allConstraints.columnAs("options").toList

    // WHEN
    dropAllFromNames(names, "CONSTRAINT")

    // THEN
    executeSingle("SHOW CONSTRAINTS YIELD *").toList should be(empty)

    // WHEN
    recreateAllFromCreateStatements(createStatements)
    graph.awaitIndexesOnline()

    // THEN
    val result = executeSingle("SHOW CONSTRAINTS YIELD *")

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
}
