/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
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
import org.neo4j.values.virtual.{MapValue, VirtualValues}

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

  test("select from map stream") {
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

  test("select from node stream") {
    val q = query(
      input(varFor("x")),
      return_(prop(varFor("x"), "p").as("r"))
    )

    execute(
      prepare(q),
      noParams,
      iteratorInput(Iterator(
        Array(node(1, Seq("A"), map("p" -> 1))),
        Array(node(2, Seq("B"), map("p" -> 1))),
        Array(node(3, Seq("A"), map("p" -> 1))),
      ))
    )
      .toComparableResult
      .shouldEqual(List(
        Map("r" -> 1),
        Map("r" -> 2),
        Map("r" -> "a"),
        Map("r" -> "b"),
      ))
  }

  private def map(kvs: (String, _)*) =
    ValueUtils.asMapValue(Map(kvs: _*).asJava)

  private def node(id: Long, labels: Seq[String], props: MapValue) =
    VirtualValues.nodeValue(id, Values.stringArray(labels: _*), props)

  private def noParams: Map[String, Any] = Map.empty

  private val semanticAnalysis =
    SemanticAnalysis(warn = true, Cypher9Comparability, MultipleDatabases).adds(BaseContains[SemanticState]) andThen
      AstRewriting(RewriterStepSequencer.newPlain, IfNoParameter, innerVariableNamer = new GeneratingNamer())

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
