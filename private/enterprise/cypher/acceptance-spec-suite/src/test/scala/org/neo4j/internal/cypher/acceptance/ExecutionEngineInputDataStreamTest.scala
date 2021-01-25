/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.runtime.InputDataStreamTestSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.mapAsJavaMapConverter

class ExecutionEngineInputDataStreamTest
  extends ExecutionEngineFunSuite
    with CypherComparisonSupport
    with AstConstructionTestSupport
    with InputDataStreamTestSupport
    with FullyParsedQueryTestSupport {

  test("pass through integers") {
    // WITH input[0] AS x, input[1] AS y, input[2] AS z
    // RETURN x AS r, y AS s, z AS t
    val q = query(
      input(varFor("x"), varFor("y"), varFor("z")),
      return_(varFor("x").as("r"), varFor("y").as("s"), varFor("z").as("t"))
    )

    execute(
      prepare(q),
      noParams,
      iteratorInput(Iterator(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9)))
    ).toComparableResult
      .shouldEqual(List(
        Map("r" -> 1, "s" -> 2, "t" -> 3),
        Map("r" -> 4, "s" -> 5, "t" -> 6),
        Map("r" -> 7, "s" -> 8, "t" -> 9),
      ))
  }

  test("compute on integers") {
    // WITH input[0] AS x
    // UNWIND [10,20] AS y
    // RETURN x + y AS r
    val q = query(
      input(varFor("x")),
      unwind(listOf(literal(10), literal(20)), varFor("y")),
      return_(add(varFor("x"), varFor("y")).as("r"))
    )

    execute(
      prepare(q),
      noParams,
      iteratorInput(Iterator(Array(1), Array(2), Array(3), Array(4)))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("r" -> 11),
        Map("r" -> 21),
        Map("r" -> 12),
        Map("r" -> 22),
        Map("r" -> 13),
        Map("r" -> 23),
        Map("r" -> 14),
        Map("r" -> 24),
      ))
  }

  test("pass through strings") {
    // WITH input[0] AS x
    // RETURN x AS r
    val q = query(
      input(varFor("x")),
      return_(varFor("x").as("r"))
    )

    execute(
      prepare(q),
      noParams,
      iteratorInput(Iterator(Array("1"), Array("2"), Array("3"), Array("4")))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("r" -> "1"),
        Map("r" -> "2"),
        Map("r" -> "3"),
        Map("r" -> "4"),
      ))
  }

  test("operations on streamed maps") {
    // WITH input[0] AS x
    // RETURN x.p AS r
    val q = query(
      input(varFor("x")),
      return_(prop(varFor("x"), "p").as("r"))
    )

    execute(
      prepare(q),
      noParams,
      iteratorInput(Iterator(
        Array(map("p" -> 1)),
        Array(map("p" -> 2)),
        Array(map("p" -> "a")),
        Array(map("p" -> "b"))))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("r" -> 1),
        Map("r" -> 2),
        Map("r" -> "a"),
        Map("r" -> "b"),
      ))
  }

  test("operations on streamed nodes (in Materialized Entities mode)") {
    // WITH input[0] AS x
    // RETURN id(x) AS id,
    //        x.p AS r,
    //        x.l[1] AS s,
    //        'A' IN labels(x) AS t
    //        exists((x)--(x)) AS u
    val q = query(
      input(varFor("x")),
      return_(
        function("id", varFor("x")).as("id"),
        prop(varFor("x"), "p").as("r"),
        index(prop(varFor("x"), "l"), 1).as("s"),
        hasLabels(varFor("x"), "A").as("t"),
        exists(patternExpression(varFor("x"), varFor("x"))).as("u")
      )
    )

    execute(
      prepare(q, materializedEntities),
      noParams,
      iteratorInputRaw(Iterator(
        Array(node(1, Seq("A"), map("p" -> "a", "l" -> arr(1, 2, 3)))),
        Array(node(2, Seq("B"), map("p" -> "b", "l" -> arr()))),
        Array(node(3, Seq("A"), map("p" -> "c"))),
      ))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("id" -> 1, "r" -> "a", "s" -> 2,    "t" -> true,  "u" -> false),
        Map("id" -> 2, "r" -> "b", "s" -> null, "t" -> false, "u" -> false),
        Map("id" -> 3, "r" -> "c", "s" -> null, "t" -> true,  "u" -> false),
      ))
  }

  test("operations on streamed relationships (in Materialized Entities mode)") {
    // WITH input[0] AS x
    // RETURN id(x) AS id,
    //        x.p AS r,
    //        x.l[1] AS s
    val q = query(
      input(varFor("x")),
      return_(
        function("id", varFor("x")).as("id"),
        prop(varFor("x"), "p").as("r"),
        index(prop(varFor("x"), "l"), 1).as("s"),
      )
    )

    execute(
      prepare(q, materializedEntities),
      noParams,
      iteratorInputRaw(Iterator(
        Array(relationship(10, node(1, Seq("A"), map()), node(4, Seq("B"), map()), "A", map("p" -> "a", "l" -> arr(1, 2, 3)))),
        Array(relationship(20, node(2, Seq("A"), map()), node(5, Seq("B"), map()), "B", map("p" -> "b", "l" -> arr()))),
        Array(relationship(30, node(3, Seq("A"), map()), node(6, Seq("B"), map()), "A", map("p" -> "c"))),
      ))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("id" -> 10, "r" -> "a", "s" -> 2),
        Map("id" -> 20, "r" -> "b", "s" -> null),
        Map("id" -> 30, "r" -> "c", "s" -> null),
      ))
  }

  private def map(kvs: (String, Any)*): util.Map[String, Any] =
    Map(kvs: _*).asJava

  private def arr(vs: Any*): Array[Any] =
    Array(vs: _*)

  private def node(id: Long, labels: Seq[String], props: util.Map[String, Any]) =
    VirtualValues.nodeValue(id, Values.stringArray(labels: _*), ValueUtils.asMapValue(props))

  private def relationship(id: Long, start: NodeValue, end: NodeValue, label: String, props: util.Map[String, Any]) =
    VirtualValues.relationshipValue(id, start, end, Values.stringValue(label), ValueUtils.asMapValue(props))

  private val materializedEntities = QueryOptions.default.copy(
    queryOptions = CypherQueryOptions.default.copy(
      runtime = CypherRuntimeOption.slotted,
      expressionEngine = CypherExpressionEngineOption.interpreted,
    ),
    materializedEntitiesMode = true,
  )
}
