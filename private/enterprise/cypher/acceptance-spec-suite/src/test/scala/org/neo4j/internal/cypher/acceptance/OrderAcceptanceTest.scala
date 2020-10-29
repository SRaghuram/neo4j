/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

import scala.collection.mutable

class OrderAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport with AstConstructionTestSupport {

  val nodeList: mutable.MutableList[Node] = mutable.MutableList()

  override def beforeEach(): Unit = {
    super.beforeEach()
    nodeList += createLabeledNode(Map("age" -> 10, "name" -> "A", "foo" -> 6, "gender" -> "female"), "A")
    nodeList += createLabeledNode(Map("age" -> 9, "name" -> "B", "foo" -> 5, "gender" -> "male", "born" -> 1990), "A")
    nodeList += createLabeledNode(Map("age" -> 12, "name" -> "C", "foo" -> 4, "gender" -> "male"), "A")
    nodeList += createLabeledNode(Map("age" -> 16, "name" -> "D", "foo" -> 3, "gender" -> "female"), "A")
    nodeList += createLabeledNode(Map("age" -> 14, "name" -> "E", "foo" -> 2, "gender" -> "female", "born" -> 1960), "A")
    nodeList += createLabeledNode(Map("age" -> 4, "name" -> "F", "foo" -> 1, "gender" -> "male"), "A")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    nodeList.clear()
  }

  test("should sort first unaliased and then aliased columns in the right order") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a:A) WITH a, EXISTS(a.born) AS bday ORDER BY a.name, bday RETURN a.name, bday")
    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc(prop("a", "name")).asc(varFor("bday")))

    result.toList should equal(List(
      Map("a.name" -> "A", "bday" -> false),
      Map("a.name" -> "B", "bday" -> true),
      Map("a.name" -> "C", "bday" -> false),
      Map("a.name" -> "D", "bday" -> false),
      Map("a.name" -> "E", "bday" -> true),
      Map("a.name" -> "F", "bday" -> false)
    ))
  }

  test("should sort first aliased and then unaliased columns in the right order") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a:A) WITH a, EXISTS(a.born) AS bday ORDER BY bday, a.name RETURN a.name, bday")
    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc(varFor("bday")).asc(prop("a", "name")))

    result.toList should equal(List(
      Map("a.name" -> "A", "bday" -> false),
      Map("a.name" -> "C", "bday" -> false),
      Map("a.name" -> "D", "bday" -> false),
      Map("a.name" -> "F", "bday" -> false),
      Map("a.name" -> "B", "bday" -> true),
      Map("a.name" -> "E", "bday" -> true)
    ))
  }

  test("should use partial sort after full sort") {
    val query = "MATCH (a:A) WITH a, a.gender AS gender ORDER BY gender ASC RETURN gender, a.name ORDER BY gender ASC, a.name ASC"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.executionPlanDescription() should includeSomewhere
      .aPlan("PartialSort")
      .withOrder(ProvidedOrder.asc(varFor("gender")).asc(varFor("a.name")).fromLeft)
      .containingArgument("gender ASC, a.name ASC")

    result.toList should equal(List(
      Map("gender" -> "female", "a.name" -> "A"),
      Map("gender" -> "female", "a.name" -> "D"),
      Map("gender" -> "female", "a.name" -> "E"),
      Map("gender" -> "male", "a.name" -> "B"),
      Map("gender" -> "male", "a.name" -> "C"),
      Map("gender" -> "male", "a.name" -> "F")
    ))
  }

  test("should use Top for SKIP and LIMIT") {
    val query = "MATCH (a:A) RETURN a.name ORDER BY a.name ASC SKIP 3 LIMIT 3"
    val result = executeWith(Configs.All, query)
    result.executionPlanDescription() should includeSomewhere
      .aPlan("Skip")
      .containingArgument("$autoint_0")
      .onTopOf(aPlan("Top")
        .withOrder(ProvidedOrder.asc(varFor("a.name")).fromLeft)
        .containingArgument("`a.name` ASC LIMIT 3 + $autoint_0")
      )

    result.toList should equal(List(
      Map("a.name" -> "D"),
      Map("a.name" -> "E"),
      Map("a.name" -> "F")
    ))
  }

  test("should use partial top after full sort") {
    val query = "MATCH (a:A) WITH a, a.gender AS gender ORDER BY gender ASC RETURN gender, a.name ORDER BY gender ASC, a.name ASC LIMIT 3"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.executionPlanDescription() should includeSomewhere
      .aPlan("PartialTop")
      .withOrder(ProvidedOrder.asc(varFor("gender")).asc(varFor("a.name")).fromLeft)
      .containingArgument("gender ASC, a.name ASC")

    result.toList should equal(List(
      Map("a.name" -> "A", "gender" -> "female"),
      Map("a.name" -> "D", "gender" -> "female"),
      Map("a.name" -> "E", "gender" -> "female")
    ))
  }

  test("should use partial top1 after full sort") {
    val query = "MATCH (a:A) WITH a, a.gender AS gender ORDER BY gender ASC RETURN gender, a.name ORDER BY gender ASC, a.name ASC LIMIT 1"
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.executionPlanDescription() should includeSomewhere
      .aPlan("PartialTop")
      .withOrder(ProvidedOrder.asc(varFor("gender")).asc(varFor("a.name")).fromLeft)
      .containingArgument("gender ASC, a.name ASC")

    result.toList should equal(List(
      Map("a.name" -> "A", "gender" -> "female")
    ))
  }

  test("ORDER BY previously unprojected column in WITH") {
    val result = executeWith(Configs.All, "MATCH (a:A) WITH a ORDER BY a.age RETURN a.name")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("`a.name`"-> "a.name"))
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc(prop("a", "age")))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentForProjection(Map("`a.age`" -> "a.age"))
        ))

    result.columnAs[String]("a.name").toList should equal(List("F", "B", "A", "C", "E", "D"))
  }

  test("ORDER BY previously unprojected column in WITH and return that column") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a:A) WITH a ORDER BY a.age RETURN a.name, a.age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("`a.name`" -> "a.name", "`a.age`" -> "cache[a.age]"))
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc(prop("a", "age")))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentForProjection(Map("`a.age`" -> "cache[a.age]"))
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "a.age" -> 4),
      Map("a.name" -> "B", "a.age" -> 9),
      Map("a.name" -> "A", "a.age" -> 10),
      Map("a.name" -> "C", "a.age" -> 12),
      Map("a.name" -> "E", "a.age" -> 14),
      Map("a.name" -> "D", "a.age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in WITH and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a, a.age AS age
      ORDER BY age
      RETURN a.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("`a.name`" -> "a.name"))
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc(varFor("age")))
        .containingArgument("age ASC")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentForProjection(Map("age" -> "a.age"))
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "age" -> 4),
      Map("a.name" -> "B", "age" -> 9),
      Map("a.name" -> "A", "age" -> 10),
      Map("a.name" -> "C", "age" -> 12),
      Map("a.name" -> "E", "age" -> 14),
      Map("a.name" -> "D", "age" -> 16)
    ))
  }

  test("ORDER BY renamed column old name in WITH and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a, a.name AS name
      WITH name AS b, a.age AS age
      ORDER BY name
      RETURN b, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("age" -> "a.age"))
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc(varFor("b")))
        .containingArgument("b ASC")
        .onTopOf(aPlan("Projection")
          .containingArgumentForProjection(Map("b" -> "name"))
        ))

    result.toList should equal(List(
      Map("b" -> "A", "age" -> 10),
      Map("b" -> "B", "age" -> 9),
      Map("b" -> "C", "age" -> 12),
      Map("b" -> "D", "age" -> 16),
      Map("b" -> "E", "age" -> 14),
      Map("b" -> "F", "age" -> 4)
    ))
  }

  test("ORDER BY renamed column new name in WITH and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a, a.name AS name
      WITH name AS b, a.age AS age
      ORDER BY b
      RETURN b, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("age" -> "a.age"))
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc(varFor("b")))
        .containingArgument("b ASC")
        .onTopOf(aPlan("Projection")
          .containingArgumentForProjection(Map("b" -> "name"))
        ))

    result.toList should equal(List(
      Map("b" -> "A", "age" -> 10),
      Map("b" -> "B", "age" -> 9),
      Map("b" -> "C", "age" -> 12),
      Map("b" -> "D", "age" -> 16),
      Map("b" -> "E", "age" -> 14),
      Map("b" -> "F", "age" -> 4)
    ))
  }

  test("ORDER BY renamed column expression with old name in WITH and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a AS b, a.age AS age
      ORDER BY a.foo, a.age + 5
      RETURN b.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingArgumentRegex("b\\.foo AS `b\\.foo`, age \\+ \\$autoint_\\d+ AS `age \\+ \\$autoint_\\d+`".r)
        .onTopOf(aPlan("Projection")
          .containingArgumentForProjection(Map("b" -> "a", "age" -> "a.age"))
        )
      )

    result.toList should equal(List(
      Map("b.name" -> "F", "age" -> 4),
      Map("b.name" -> "E", "age" -> 14),
      Map("b.name" -> "D", "age" -> 16),
      Map("b.name" -> "C", "age" -> 12),
      Map("b.name" -> "B", "age" -> 9),
      Map("b.name" -> "A", "age" -> 10)
    ))
  }

  test("ORDER BY renamed column expression with new name in WITH and project and return that column") {
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
      MATCH (a:A)
      WITH a AS b, a.age AS age
      ORDER BY b.foo, b.age + 5
      RETURN b.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("age" -> "cache[a.age]"))
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingArgumentRegex("b\\.foo AS `b\\.foo`, cache\\[b\\.age\\] \\+ \\$autoint_\\d+ AS `b\\.age \\+ \\$autoint_\\d+`".r)
          .onTopOf(aPlan("Projection")
            .containingArgumentForProjection(Map("b" -> "a"))
          )
        ))

    result.toList should equal(List(
      Map("b.name" -> "F", "age" -> 4),
      Map("b.name" -> "E", "age" -> 14),
      Map("b.name" -> "D", "age" -> 16),
      Map("b.name" -> "C", "age" -> 12),
      Map("b.name" -> "B", "age" -> 9),
      Map("b.name" -> "A", "age" -> 10)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN") {
    val result = executeWith(Configs.All, "MATCH (a:A) RETURN a.name ORDER BY a.age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("`a.name`" -> "a.name"))
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc(prop("a", "age")))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentForProjection(Map("`a.age`" -> "a.age"))
        ))

    result.toList should equal(List(
      Map("a.name" -> "F"),
      Map("a.name" -> "B"),
      Map("a.name" -> "A"),
      Map("a.name" -> "C"),
      Map("a.name" -> "E"),
      Map("a.name" -> "D")
    ))
  }

  test("ORDER BY previously unprojected column in RETURN and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN a.name, a.age
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("`a.name`" -> "a.name"))
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentForProjection(Map("`a.age`" -> "a.age"))
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "a.age" -> 4),
      Map("a.name" -> "B", "a.age" -> 9),
      Map("a.name" -> "A", "a.age" -> 10),
      Map("a.name" -> "C", "a.age" -> 12),
      Map("a.name" -> "E", "a.age" -> 14),
      Map("a.name" -> "D", "a.age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN a.name, a.age AS age
      ORDER BY age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("`a.name`" -> "a.name"))
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentForProjection(Map("age" -> "a.age"))
        ))

    result.toList should equal(List(
      Map("a.name" -> "F", "age" -> 4),
      Map("a.name" -> "B", "age" -> 9),
      Map("a.name" -> "A", "age" -> 10),
      Map("a.name" -> "C", "age" -> 12),
      Map("a.name" -> "E", "age" -> 14),
      Map("a.name" -> "D", "age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN *") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN *
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingVariables("a")
        .containingArgumentForProjection(Map("`a.age`" -> "a.age"))
      )

    result.toList should equal(List(
      Map("a" -> nodeList(5)),
      Map("a" -> nodeList(1)),
      Map("a" -> nodeList(0)),
      Map("a" -> nodeList(2)),
      Map("a" -> nodeList(4)),
      Map("a" -> nodeList(3))
    ))
  }

  test("ORDER BY previously unprojected column in RETURN * and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN *, a.age
      ORDER BY a.age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingVariables("a")
        .containingArgumentForProjection(Map("`a.age`" -> "a.age"))
      )

    result.toList should equal(List(
      Map("a" -> nodeList(5), "a.age" -> 4),
      Map("a" -> nodeList(1), "a.age" -> 9),
      Map("a" -> nodeList(0), "a.age" -> 10),
      Map("a" -> nodeList(2), "a.age" -> 12),
      Map("a" -> nodeList(4), "a.age" -> 14),
      Map("a" -> nodeList(3), "a.age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column in RETURN * and project and return that column") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      RETURN *, a.age AS age
      ORDER BY age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .onTopOf(aPlan("Projection")
        .containingVariables("a")
        .containingArgumentForProjection(Map("age" -> "a.age"))
      )

    result.toList should equal(List(
      Map("a" -> nodeList(5), "age" -> 4),
      Map("a" -> nodeList(1), "age" -> 9),
      Map("a" -> nodeList(0), "age" -> 10),
      Map("a" -> nodeList(2), "age" -> 12),
      Map("a" -> nodeList(4), "age" -> 14),
      Map("a" -> nodeList(3), "age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected column with expression in WITH") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a
      ORDER BY a.age + 4
      RETURN a.name
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgumentForProjection(Map("`a.name`" -> "a.name"))
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentRegex("a.age \\+ \\$autoint_\\d+ AS `a.age \\+ \\$autoint_\\d+`".r)
        ))

    result.toList should equal(List(
      Map("a.name" -> "F"),
      Map("a.name" -> "B"),
      Map("a.name" -> "A"),
      Map("a.name" -> "C"),
      Map("a.name" -> "E"),
      Map("a.name" -> "D")
    ))
  }

  test("ORDER BY previously unprojected DISTINCT column in WITH and project and return it") {
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH DISTINCT a.age AS age
      ORDER BY age
      RETURN age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc(varFor("age")))
      .containingArgument("age ASC")
      .onTopOf(aPlan("Distinct")
        .containingVariables("age")
        .containingArgument("a.age AS age")
      )

    result.toList should equal(List(
      Map("age" -> 4),
      Map("age" -> 9),
      Map("age" -> 10),
      Map("age" -> 12),
      Map("age" -> 14),
      Map("age" -> 16)
    ))
  }

  test("ORDER BY column that isn't referenced in WITH DISTINCT") {
    val result = executeWith(Configs.All, "MATCH (a:A) WITH DISTINCT a.name AS name, a ORDER BY a.age RETURN name")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc(prop("a", "age")))
      .onTopOf(aPlan("Projection")
        .containingArgumentForProjection(Map("`a.age`" -> "a.age"))
        .onTopOf(aPlan("Distinct")
          .containingVariables("a", "name")
          .containingArgument("a.name AS name, a")
        )
      )

    result.toList should equal(List(
      Map("name" -> "F"),
      Map("name" -> "B"),
      Map("name" -> "A"),
      Map("name" -> "C"),
      Map("name" -> "E"),
      Map("name" -> "D")
    ))
  }

  test("ORDER BY previously unprojected AGGREGATING column in WITH and project and return it") {
    // sum is not supported in compiled
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
      MATCH (a:A)
      WITH a.name AS name, sum(a.age) AS age
      ORDER BY age
      RETURN name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc(varFor("age")))
      .containingArgument("age ASC")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("a.name AS name, sum(a.age) AS age") // the details column
      )

    result.toList should equal(List(
      Map("name" -> "F", "age" -> 4),
      Map("name" -> "B", "age" -> 9),
      Map("name" -> "A", "age" -> 10),
      Map("name" -> "C", "age" -> 12),
      Map("name" -> "E", "age" -> 14),
      Map("name" -> "D", "age" -> 16)
    ))
  }

  test("ORDER BY previously unprojected GROUPING column in WITH and project and return it") {
    // sum is not supported in compiled
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """
      MATCH (a:A)
      WITH a.name AS name, sum(a.age) AS age
      ORDER BY name
      RETURN name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc(varFor("name")))
      .containingArgument("name ASC")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("a.name AS name, sum(a.age) AS age") // the details column
      )

    result.toList should equal(List(
      Map("name" -> "A", "age" -> 10),
      Map("name" -> "B", "age" -> 9),
      Map("name" -> "C", "age" -> 12),
      Map("name" -> "D", "age" -> 16),
      Map("name" -> "E", "age" -> 14),
      Map("name" -> "F", "age" -> 4)
    ))
  }

  test("ORDER BY column that isn't referenced in WITH GROUP BY") {
    // sum is not supported in compiled
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a:A) WITH a.name AS name, a, sum(a.age) AS age ORDER BY a.foo RETURN name, age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc(prop("a", "foo")))
      .onTopOf(aPlan("Projection")
        .containingArgumentForProjection(Map("`a.foo`" -> "a.foo"))
        .onTopOf(aPlan("EagerAggregation")
          .containingVariables("age", "name") // the introduced variables
          .containingArgument("a.name AS name, a, sum(a.age) AS age") // the details column
        ))

    result.toList should equal(List(
      Map("name" -> "F", "age" -> 4),
      Map("name" -> "E", "age" -> 14),
      Map("name" -> "D", "age" -> 16),
      Map("name" -> "C", "age" -> 12),
      Map("name" -> "B", "age" -> 9),
      Map("name" -> "A", "age" -> 10)
    ))
  }

  test("Should fail when accessing undefined variable after WITH ORDER BY") {
    failWithError(Configs.All, "MATCH (a) WITH a.name AS n ORDER BY a.foo RETURN a.x",
      errorType = Seq("SyntaxException"))
  }

  test("Should be able to reuse unprojected node variable after WITH ORDER BY") {

    val query = "MATCH (a) WITH a.name AS n ORDER BY a.foo MATCH (a) RETURN a.age ORDER BY a.age"
    val result = executeWith(Configs.All, query)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection").containingArgumentForProjection(Map("n" -> "a.name"))
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection").containingArgumentForProjection(Map("`a.foo`" -> "a.foo")))
      )

    result.toSet should equal(Set(
      Map("a.age" -> 10),
      Map("a.age" -> 9),
      Map("a.age" -> 12),
      Map("a.age" -> 16),
      Map("a.age" -> 14),
      Map("a.age" -> 4)
    ))
  }

  test("Should be able to reuse unprojected node and rel variables after WITH ORDER BY") {

    graph.withTx( tx =>
      tx.execute("MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}) CREATE (a)-[:Rel]->(b)-[:Rel]->(c)").close()
    )

    val query =
      """
        |MATCH (n)-[r]->(m)
        |WITH n.name as name ORDER BY n.foo
        |MATCH (n)-[r]->(m)
        |RETURN n.age ORDER BY n.age
      """.stripMargin

    val result = executeWith(Configs.All, query)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort").withOrder(ProvidedOrder.asc(varFor("n.age"))).onTopOf(
      includeSomewhere.aPlan("Projection").containingArgumentForProjection(Map("name" -> "n.name"))
        .onTopOf(aPlan("Sort").withOrder(ProvidedOrder.asc(prop("n", "foo")))
          .onTopOf(aPlan("Projection").containingArgumentForProjection(Map("`n.foo`" -> "n.foo")))
        )
    )

    // First MATCH gives two rows
    result.toList should equal(List(
      Map("n.age" -> 9),
      Map("n.age" -> 9),
      Map("n.age" -> 10),
      Map("n.age" -> 10)
    ))
  }

  test("Order by property that has been renamed several times") {
    val query =
      """
        |MATCH (z:A) WHERE z.foo > 0
        |WITH z AS y, 1 AS foo, z.foo AS temp
        |WITH y AS x, 1 AS bar
        |WITH x.foo AS xfoo, 1 AS foo
        |WITH xfoo AS zfoo
        |RETURN zfoo ORDER BY zfoo DESC
      """.stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort").withOrder(ProvidedOrder.desc(varFor("zfoo")))
    result.toList should be(List(
      Map("zfoo" -> 6),
      Map("zfoo" -> 5),
      Map("zfoo" -> 4),
      Map("zfoo" -> 3),
      Map("zfoo" -> 2),
      Map("zfoo" -> 1)
    ))
  }

  test("Order by property that has been renamed several times (with index)") {
    val query =
      """
        |MATCH (z:A) WHERE z.foo > 0
        |WITH z AS y, 1 AS foo, z.foo AS temp
        |WITH y AS x, 1 AS bar
        |WITH x.foo AS xfoo, 1 AS foo
        |WITH xfoo AS zfoo
        |RETURN zfoo ORDER BY zfoo DESC
      """.stripMargin

    graph.createIndex("A", "foo")
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("NodeIndexSeekByRange")
          .containingArgumentForCachedProperty("z", "foo"))
    result.toList should be(List(
      Map("zfoo" -> 6),
      Map("zfoo" -> 5),
      Map("zfoo" -> 4),
      Map("zfoo" -> 3),
      Map("zfoo" -> 2),
      Map("zfoo" -> 1)
    ))
  }

  test("Order by should be correct when updating property") {
    makeJoeAndFriends()
    val query =
      """MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.start = true
        |SET u.name = 'joe'
        |RETURN u.name, u.foo
        |ORDER BY u.name ASC, u.foo DESC""".stripMargin
    val result = executeWith(Configs.InterpretedAndSlotted, query)

    result.toList should be(List(
      Map("u.name" -> "joe", "u.foo" -> 1),
      Map("u.name" -> "joe", "u.foo" -> 1),
      Map("u.name" -> "joe", "u.foo" -> 1),
      Map("u.name" -> "joe", "u.foo" -> 1),
      Map("u.name" -> "joe", "u.foo" -> 0),
      Map("u.name" -> "joe", "u.foo" -> 0),
      Map("u.name" -> "joe", "u.foo" -> 0),
      Map("u.name" -> "joe", "u.foo" -> 0)))

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort").withOrder(ProvidedOrder.asc(varFor("u.name")).desc(varFor("u.foo")))
      .onTopOf(includeSomewhere.aPlan("SetProperty"))
  }

  test("Should plan sort before first expand and partial sort in the end") {
    makeJoeAndFriends()
    val query =
      """
        |MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book)
        |WHERE u.start = true
        |WITH u, b
        |ORDER BY u.name
        |RETURN u.name, b.title
        |ORDER BY u.name, b.title
      """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("PartialSort").onTopOf(
      includeSomewhere.aPlan("Expand(All)").onTopOf(                                          // 8 rows
        includeSomewhere.aPlan("Expand(All)").onTopOf(                                        // 8 rows
          includeSomewhere.aPlan("Sort").withOrder(ProvidedOrder.asc(prop("u", "name"))).onTopOf(
            includeSomewhere.aPlan("Filter").onTopOf(                                              // 2 rows
              aPlan("NodeByLabelScan")                                                             // 12 rows
            )
          )
        )
      )
    )

    result.toList should be(List(
      Map("u.name" -> "Joe", "b.title" -> "Book_0"),
      Map("u.name" -> "Joe", "b.title" -> "Book_1"),
      Map("u.name" -> "Joe", "b.title" -> "Book_2"),
      Map("u.name" -> "Joe", "b.title" -> "Book_3"),
      Map("u.name" -> "Joseph", "b.title" -> "Book_2"),
      Map("u.name" -> "Joseph", "b.title" -> "Book_3"),
      Map("u.name" -> "Joseph", "b.title" -> "Book_4"),
      Map("u.name" -> "Joseph", "b.title" -> "Book_5")
    ))
  }

  test("should plan sort with property on non-variable") {
    val query =
      """
        |WITH [{name: 'a'},{name: 'b'},{name: 'c'}] AS ns
        |RETURN ns[0] AS n
        |ORDER BY n.name
      """.stripMargin
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").withOrder(ProvidedOrder.asc(prop("n", "name")))
    result.toList should be(List(Map("n" -> Map("name" -> "a"))))
  }

  test("ascending node index seek of multiple values") {
   //given
    executeSingle("CREATE CONSTRAINT ON (n:Frame) ASSERT n.tic IS UNIQUE")
    executeSingle("UNWIND range(1, 500) AS i CREATE (:Frame {tic:i})")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (f:Frame) WHERE f.tic IN range(3, 45)
        |WITH f ORDER BY f.tic ASC
        |RETURN f.tic""".stripMargin)

    //then
    result.toList should be((3 to 45).map(i => Map("f.tic" -> i)))
  }

  test("descending node index seek of multiple values") {
    //given
    executeSingle("CREATE CONSTRAINT ON (n:Frame) ASSERT n.tic IS UNIQUE")
    executeSingle("UNWIND range(1, 500) AS i CREATE (:Frame {tic:i})")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (f:Frame) WHERE f.tic IN range(3, 45)
        |WITH f ORDER BY f.tic DESC
        |RETURN f.tic""".stripMargin)

    //then
    result.toList should be((45 to 3 by -1).map(i => Map("f.tic" -> i)))
  }

  test("ascending node index seek on composite index, sort on first value") {
    //given
    executeSingle("CREATE INDEX FOR (n:Frame) ON (n.tic1, n.tic2)")
    executeSingle("UNWIND range(1, 500) AS i CREATE (:Frame {tic1:i, tic2: 500 - i})")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (f:Frame) WHERE f.tic1 IN range(3,45) AND f.tic2 >= 4
        |WITH f ORDER BY f.tic1 ASC
        |RETURN f.tic1""".stripMargin)

    //then
    result.toList should be((3 to 45).map(i => Map("f.tic1" -> i)))
  }

  test("ascending node index seek on composite index, sort on second value") {
    //given
    executeSingle("CREATE INDEX FOR (n:Frame) ON (n.tic1, n.tic2)")
    executeSingle("UNWIND range(1, 500) AS i CREATE (:Frame {tic1:i, tic2: 500 - i})")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (f:Frame) WHERE f.tic1 IN range(3,45) AND f.tic2 >= 4
        |WITH f ORDER BY f.tic2 ASC
        |RETURN f.tic1""".stripMargin)

    //then
    result.toList should be((45 to 3 by -1).map(i => Map("f.tic1" -> i)))
  }

  test("descending node index seek on composite index, sort on first value") {
    //given
    executeSingle("CREATE INDEX FOR (n:Frame) ON (n.tic1, n.tic2)")
    executeSingle("UNWIND range(1, 500) AS i CREATE (:Frame {tic1:i, tic2: 500 - i})")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (f:Frame) WHERE f.tic1 IN range(3,45) AND f.tic2 >= 4
        |WITH f ORDER BY f.tic1 DESC
        |RETURN f.tic1""".stripMargin)

    //then
    result.toList should be((45 to 3 by -1).map(i => Map("f.tic1" -> i)))
  }

  test("descending node index seek on composite index, sort on second value") {
    //given
    executeSingle("CREATE INDEX FOR (n:Frame) ON (n.tic1, n.tic2)")
    executeSingle("UNWIND range(1, 500) AS i CREATE (:Frame {tic1:i, tic2: 500 - i})")

    //when
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (f:Frame) WHERE f.tic1 IN range(3,45) AND f.tic2 >= 4
        |WITH f ORDER BY f.tic2 DESC
        |RETURN f.tic1""".stripMargin)

    //then
    result.toList should be((3 to 45).map(i => Map("f.tic1" -> i)))
  }

  private def makeJoeAndFriends(friendCount:Int = 10): Unit = {
    val joe = createLabeledNode(Map("name" -> s"Joe", "foo" -> 0, "start" -> true), "Person")
    val joseph = createLabeledNode(Map("name" -> s"Joseph", "foo" -> 1, "start" -> true), "Person")

    val friends = Range(0, friendCount).map(f => createLabeledNode(Map("name" -> s"Jane_$f"), "Person"))
    friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
    for (i <- 0 until friendCount) {
      val book = createLabeledNode(Map("title" -> s"Book_$i"), "Book")
      relate(friends(i), book, "READ")
    }

    val relCount = if (friendCount < 6) friendCount - 2 else 4 // friends(relCount+2) has to exist
    for (i <- 0 until relCount) {
      relate(joe, friends(i), "FRIEND")
      relate(joseph, friends(i+2), "FRIEND")
    }
  }
}
