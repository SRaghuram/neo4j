/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.ir.v4_0.ProvidedOrder
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

import scala.collection.{Map, mutable}

class OrderAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  val nodeList: mutable.MutableList[Node] = mutable.MutableList()

  override def beforeEach(): Unit = {
    super.beforeEach()
    nodeList += createLabeledNode(Map("age" -> 10, "name" -> "A", "foo" -> 6), "A")
    nodeList += createLabeledNode(Map("age" -> 9, "name" -> "B", "foo" -> 5, "born" -> 1990), "A")
    nodeList += createLabeledNode(Map("age" -> 12, "name" -> "C", "foo" -> 4), "A")
    nodeList += createLabeledNode(Map("age" -> 16, "name" -> "D", "foo" -> 3), "A")
    nodeList += createLabeledNode(Map("age" -> 14, "name" -> "E", "foo" -> 2, "born" -> 1960), "A")
    nodeList += createLabeledNode(Map("age" -> 4, "name" -> "F", "foo" -> 1), "A")
  }

  test("should sort first unaliased and then aliased columns in the right order") {
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (a:A) WITH a, EXISTS(a.born) AS bday ORDER BY a.name, bday RETURN a.name, bday")
    result.executionPlanDescription() should includeSomewhere
        .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("a.name").asc("bday"))

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
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "MATCH (a:A) WITH a, EXISTS(a.born) AS bday ORDER BY bday, a.name RETURN a.name, bday")
    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("bday").asc("a.name"))

    result.toList should equal(List(
      Map("a.name" -> "A", "bday" -> false),
      Map("a.name" -> "C", "bday" -> false),
      Map("a.name" -> "D", "bday" -> false),
      Map("a.name" -> "F", "bday" -> false),
      Map("a.name" -> "B", "bday" -> true),
      Map("a.name" -> "E", "bday" -> true)
    ))
  }

  test("ORDER BY previously unprojected column in WITH") {
    val result = executeWith(Configs.All, "MATCH (a:A) WITH a ORDER BY a.age RETURN a.name")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("a.age"))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{a.age : a.age}")
        ))

    result.columnAs[String]("a.name").toList should equal(List("F", "B", "A", "C", "E", "D"))
  }

  test("ORDER BY previously unprojected column in WITH and return that column") {
    val result = executeWith(Configs.All, "MATCH (a:A) WITH a ORDER BY a.age RETURN a.name, a.age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{a.name : a.name, a.age : a.age}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("a.age"))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{a.age : a.age}")
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
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("age"))
        .containingArgument("age")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{age : a.age}")
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
      .containingArgument("{age : a.age}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("b"))
        .containingArgument("b")
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : name}")
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
      .containingArgument("{age : a.age}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("b"))
        .containingArgument("b")
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : name}")
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
        .containingArgumentRegex("\\{b\\.foo : b\\.foo, age \\+ \\{  AUTOINT\\d+\\} : age \\+ \\$`  AUTOINT\\d+`\\}".r)
        .onTopOf(aPlan("Projection")
          .containingArgument("{b : a, age : a.age}")
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
    val result = executeWith(Configs.All,
      """
      MATCH (a:A)
      WITH a AS b, a.age AS age
      ORDER BY b.foo, b.age + 5
      RETURN b.name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection")
      .containingArgument("{age : a.age}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingArgumentRegex("\\{b\\.foo : b\\.foo, b\\.age \\+ \\{  AUTOINT\\d+\\} : b\\.age \\+ \\$`  AUTOINT\\d+`\\}".r)
          .onTopOf(aPlan("Projection")
            .containingArgument("{b : a}")
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
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .withOrder(ProvidedOrder.asc("a.age"))
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{a.age : a.age}")
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
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{a.age : a.age}")
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
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgument("{age : a.age}")
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
        .containingArgument("{a.age : a.age}")
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
        .containingArgument("{a.age : a.age}")
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
        .containingArgument("{age : a.age}")
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
      .containingArgument("{a.name : a.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection")
          .containingVariables("a")
          .containingArgumentRegex("\\{a.age \\+ \\{  AUTOINT\\d+\\} : a.age \\+ \\$`  AUTOINT\\d+`\\}".r)
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
    val result = executeWith(Configs.All - Configs.Morsel,
      """
      MATCH (a:A)
      WITH DISTINCT a.age AS age
      ORDER BY age
      RETURN age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("age"))
      .containingArgument("age")
      .onTopOf(aPlan("Distinct")
        .containingVariables("age")
        .containingArgument("age")
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
    val result = executeWith(Configs.All - Configs.Morsel, "MATCH (a:A) WITH DISTINCT a.name AS name, a ORDER BY a.age RETURN name")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("a.age"))
      .onTopOf(aPlan("Projection")
        .containingArgument("{a.age : a.age}")
        .onTopOf(aPlan("Distinct")
          .containingVariables("name", "a")
          .containingArgument("name, a")
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
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
      MATCH (a:A)
      WITH a.name AS name, sum(a.age) AS age
      ORDER BY age
      RETURN name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("age"))
      .containingArgument("age")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("name") // the group column
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
    val result = executeWith(Configs.InterpretedAndSlotted,
      """
      MATCH (a:A)
      WITH a.name AS name, sum(a.age) AS age
      ORDER BY name
      RETURN name, age
      """)

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("name"))
      .containingArgument("name")
      .onTopOf(aPlan("EagerAggregation")
        .containingVariables("age", "name") // the introduced variables
        .containingArgument("name") // the group column
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
    val result = executeWith(Configs.InterpretedAndSlotted, "MATCH (a:A) WITH a.name AS name, a, sum(a.age) AS age ORDER BY a.foo RETURN name, age")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort")
      .withOrder(ProvidedOrder.asc("a.foo"))
      .onTopOf(aPlan("Projection")
        .containingArgument("{a.foo : a.foo}")
        .onTopOf(aPlan("EagerAggregation")
          .containingVariables("age", "name") // the introduced variables
          .containingArgument("name, a") // the group columns
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

    // a.prop is written as `a`.prop in the plan due to the reuse of the variable
    result.executionPlanDescription() should includeSomewhere
      .aPlan("Projection").containingArgument("{n : `a`.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection").containingArgument("{  a@7.foo : `a`.foo}"))
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

    graph.execute("MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}) CREATE (a)-[:Rel]->(b)-[:Rel]->(c)")

    val query =
      """
        |MATCH (n)-[r]->(m)
        |WITH n.name as name ORDER BY n.foo
        |MATCH (n)-[r]->(m)
        |RETURN n.age ORDER BY n.age
      """.stripMargin

    val result = executeWith(Configs.All, query)

    // n.prop is written as `n`.prop in the plan due to the reuse of the variable
    result.executionPlanDescription() should (includeSomewhere
      .aPlan("Projection").containingArgument("{name : `n`.name}")
      .onTopOf(aPlan("Sort")
        .onTopOf(aPlan("Projection").containingArgument("{  n@7.foo : `n`.foo}"))
      ) and includeSomewhere.nTimes(2, aPlan("Sort")))

    // First MATCH gives two rows
    result.toList should equal(List(
      Map("n.age" -> 9),
      Map("n.age" -> 9),
      Map("n.age" -> 10),
      Map("n.age" -> 10)
    ))
  }
  test("should plan sort in optimal position within idp") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u.name, b.title ORDER BY u.name"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Expand(All)").onTopOf(includeSomewhere.aPlan("Sort").withRows(2))
  }

  test("should plan sort in optimal position within idp - a") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' WITH b, u AS v WITH b, v.name as name RETURN name, b.title ORDER BY name"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").withRows(200)
  }

  test("should plan sort in optimal position within idp - b") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u.name, b.title ORDER BY b.title"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Projection").onTopOf(aPlan("Sort").withRows(200))
  }

  test("should plan sort in optimal position within idp - c") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u.name, b.title ORDER BY u"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Expand(All)").onTopOf(includeSomewhere.aPlan("Sort").withRowsBetween(2, 20))
  }

  test("should plan sort in optimal position within idp - d") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u.name, b.title ORDER BY b"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Projection").onTopOf(aPlan("Sort").withRows(200))
  }

  test("should plan sort in optimal position within idp - 1") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(p => createLabeledNode(Map("name" -> s"Jane_$p"), "Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u.name + p.name, b.title ORDER BY u.name + p.name"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Expand(All)").onTopOf(includeSomewhere.aPlan("Sort").withRows(20))
  }

  test("should plan sort in optimal position within idp - 2") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u.name AS name, b.title ORDER BY name"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Expand(All)").onTopOf(includeSomewhere.aPlan("Sort").withRowsBetween(2, 20))
  }

  test("should plan sort in optimal position within idp - 3") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u.name AS name, b.title ORDER BY u.name"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Expand(All)").onTopOf(includeSomewhere.aPlan("Sort").withRowsBetween(2, 20))
  }

  test("should plan sort in optimal position within idp - 4") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(_ => createLabeledNode("Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u AS v, b.title ORDER BY v.name"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Expand(All)").onTopOf(includeSomewhere.aPlan("Sort").withRowsBetween(2, 20))
  }

  test("should plan sort in optimal position within idp - 5") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(f => createLabeledNode(Map("name" -> s"Jane_$f"), "Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u, b.title ORDER BY u.name + p.name"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Expand(All)").onTopOf(includeSomewhere.aPlan("Sort").withRowsBetween(2, 20))
  }

  test("should plan sort in optimal position within idp - 6") {
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i"), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 10).map(f => createLabeledNode(Map("name" -> s"Jane_$f"), "Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 10).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' RETURN u, b.title ORDER BY u.name + b.title"
    val result = executeSingle(query)
    result.executionPlanDescription() should includeSomewhere.aPlan("Sort").withRows(200).onTopOf(aPlan("Projection"))
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

    val result = executeWith(Configs.InterpretedAndSlotted, query)
    result.executionPlanDescription() should includeSomewhere
      .aPlan("Sort").withOrder(ProvidedOrder.desc("zfoo"))
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
    val result = executeWith(Configs.InterpretedAndSlotted, query)
    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("NodeIndexSeekByRange")
          .containingVariables("cached[z.foo]"))
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
    val joes = Range(0, 2).map(i => createLabeledNode(Map("name" -> s"Joe_$i", "foo" -> i), "Person"))
    joes.foreach { n =>
      val friends = Range(0, 2).map(f => createLabeledNode(Map("name" -> s"Jane_$f"), "Person"))
      friends.reduce { (previous, friend) => relate(previous, friend, "FRIEND"); friend }
      friends.foreach { friend =>
        relate(n, friend, "FRIEND")
        Range(0, 1).foreach { b =>
          val book = createLabeledNode(Map("title" -> s"Book_${friend.getId}_$b"), "Book")
          relate(friend, book, "READ")
        }
      }
    }
    val query = "PROFILE MATCH (u:Person)-[f:FRIEND]->(p:Person)-[r:READ]->(b:Book) WHERE u.name STARTS WITH 'Joe' SET u.name = 'joe' RETURN u.name, u.foo ORDER BY u.name ASC, u.foo DESC"
    val result = executeSingle(query)
    //println(result.executionPlanDescription())
    result.toList should be(List(Map("u.name" -> "joe", "u.foo" -> 1), Map("u.name" -> "joe", "u.foo" -> 1), Map("u.name" -> "joe", "u.foo" -> 0), Map("u.name" -> "joe", "u.foo" -> 0)))
  }
}
