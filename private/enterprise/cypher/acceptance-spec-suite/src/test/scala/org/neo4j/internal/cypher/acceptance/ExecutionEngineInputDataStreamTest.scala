/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.cypher.{CypherExpressionEngineOption, CypherRuntimeOption, ExecutionEngineFunSuite}
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.runtime.InputDataStreamTestSupport
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticFeature.{Cypher9Comparability, MultipleDatabases}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticState
import org.neo4j.cypher.internal.v4_0.ast.{AstConstructionTestSupport, Statement}
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{GeneratingNamer, IfNoParameter}
import org.neo4j.cypher.internal.{FullyParsedQuery, QueryOptions}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{MapValue, NodeValue, VirtualValues}

import scala.collection.JavaConverters._

class ExecutionEngineInputDataStreamTest
  extends ExecutionEngineFunSuite
    with CypherComparisonSupport
    with AstConstructionTestSupport
    with InputDataStreamTestSupport {

  test("pass through integers") {
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
    val q = query(
      input(varFor("x")),
      return_(prop(varFor("x"), "p").as("r"))
    )

    execute(
      prepare(q),
      noParams,
      iteratorInput(Iterator(Array(map("p" -> 1)), Array(map("p" -> 2)), Array(map("p" -> "a")), Array(map("p" -> "b"))))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("r" -> 1),
        Map("r" -> 2),
        Map("r" -> "a"),
        Map("r" -> "b"),
      ))
  }

  test("operations on streamed nodes (in NoDbAccess mode)") {
    val q = query(
      input(varFor("x")),
      return_(
        function("id", varFor("x")).as("id"),
        prop(varFor("x"), "p").as("r"),
        index(prop(varFor("x"), "l"), 1).as("s"),
        hasLabels(varFor("x"), "A").as("t")
      )
    )

    execute(
      prepare(q, noDbAccess),
      noParams,
      iteratorInputRaw(Iterator(
        Array(node(1, Seq("A"), map("p" -> "a", "l" -> arr(1, 2, 3)))),
        Array(node(2, Seq("B"), map("p" -> "b", "l" -> arr()))),
        Array(node(3, Seq("A"), map("p" -> "c"))),
      ))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("id" -> 1, "r" -> "a", "s" -> 2, "t" -> true),
        Map("id" -> 2, "r" -> "b", "s" -> null, "t" -> false),
        Map("id" -> 3, "r" -> "c", "s" -> null, "t" -> true),
      ))
  }

  test("operations on streamed relationships (in NoDbAccess mode)") {
    val q = query(
      input(varFor("x")),
      return_(
        function("id", varFor("x")).as("id"),
        prop(varFor("x"), "p").as("r"),
        index(prop(varFor("x"), "l"), 1).as("s"),
      )
    )

    execute(
      prepare(q, noDbAccess),
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
    VirtualValues.relationshipValue(id, start, end, ValueUtils.asTextValue(label), ValueUtils.asMapValue(props))

  private def noParams: Map[String, Any] = Map.empty

  private val semanticAnalysis =
    SemanticAnalysis(warn = true, Cypher9Comparability, MultipleDatabases).adds(BaseContains[SemanticState]) andThen
      AstRewriting(RewriterStepSequencer.newPlain, IfNoParameter, innerVariableNamer = new GeneratingNamer())

  private val noDbAccess = QueryOptions.default.copy(
    runtime = CypherRuntimeOption.slotted,
    expressionEngine = CypherExpressionEngineOption.interpreted,
    noDatabaseAccess = true,
  )

  private def prepare(query: Statement, options: QueryOptions = QueryOptions.default) =
    FullyParsedQuery(
      state = semanticAnalysis.transform(
        InitialState("", None, PlannerNameFor(options.planner.name))
          .withStatement(query),
        ContextHelper.create()
      ),
      options = options
    )
}
