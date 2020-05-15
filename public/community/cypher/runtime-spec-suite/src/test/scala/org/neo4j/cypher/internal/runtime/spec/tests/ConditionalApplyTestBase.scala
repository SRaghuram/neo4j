/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

abstract class ConditionalApplyTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("conditional apply should not run rhs if lhs is empty") {
    // given
    val lhsRows = inputValues()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.filter("1/0 > 0")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // should not throw 1/0 exception
    runtimeResult should beColumns("x").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs, where condition(lhs) always is true") {
    // given
    val nodes = given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.expandInto("(y)--(x)")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // because graph contains no relationships, the expand will return no rows
    runtimeResult should beColumns("x").withNoRows()
  }

  test("conditional apply on nonempty lhs and empty rhs") {
    // given
    given {
      nodeGraph(19, "RHS")
      nodeGraph(sizeHint)
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.filter("false")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    // will only return lhs where condition(lhs) is false
    runtimeResult should beColumns("x").withSingleRow(null)
  }

  test("conditional apply on nonempty lhs and nonempty rhs") {
    // given
    given {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .input(variables = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = (Seq.fill(sizeHint)(Array[Any]("42")) :+ Array[Any](null)) ++ Seq.fill(sizeHint)(Array[Any]("43"))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("conditional apply on non-nullable node") {
    val nodeCount = sizeHint
    val (nodes, _) = given {
      circleGraph(nodeCount, "L")
    }

    //when
    val limit = 2
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.limit(limit)
      .|.expand("(y)--(z)")
      .|.nodeByLabelScan("y", "L", IndexOrderNone, "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)


    //then
    runtimeResult should beColumns("x").withRows(nodes.flatMap(n => Seq.fill(limit)(Array[Any](n))))
  }

  test("conditional apply on the RHS of an apply") {
    // given
    given {
      nodeGraph(sizeHint)
      nodeGraph(sizeHint, "RHS")
    }
    val lhsRows = inputValues(Array("42"), Array(null), Array("43"))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.conditionalApply("x")
      .|.|.nodeByLabelScan("y", "RHS", IndexOrderNone, "x")
      .|.filter("x = '42' OR x IS NULL")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()


    val runtimeResult = execute(logicalQuery, runtime, lhsRows)

    // then
    val expected = Seq.fill(sizeHint)(Array[Any]("42")) :+ Array[Any](null)
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("conditional apply with limit on rhs") {
    val limit = 10

    val unfilteredNodes = given {
      val size = 100
      val nodes = nodeGraph(size)
      randomlyConnect(nodes, Connectivity(1, limit, "REL"))
      nodes
    }

    val nodes = select(unfilteredNodes, selectivity = 0.5, duplicateProbability = 0.5, nullProbability = 0.3)
    val input = batchedInputValues(sizeHint / 8, nodes.map(n => Array[Any](n)): _*).stream()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.limit(limit)
      .|.expandInto("(y)--(x)")
      .|.allNodeScan("y", "x")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedRowCountsFromRhs = for {
      x <- nodes
      if x != null
      subquery = for {
        y <- unfilteredNodes
        rel <- y.getRelationships.asScala if rel.getOtherNode(y) == x
      } yield Array(x)
    } yield math.min(subquery.size, limit)

    val expectedRowCount = expectedRowCountsFromRhs.sum + nodes.count(_ == null)
    runtimeResult should beColumns("x").withRows(rowCount(expectedRowCount))
  }

  test("should support limit on top of conditional apply") {
    // given
    val nodesPerLabel = 50
    val (nodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }
    val input = inputColumns(100000, 3, i => nodes(i % nodes.size)).stream()

    val limit = nodesPerLabel * nodesPerLabel - 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(limit)
      .conditionalApply("x")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .input(nodes = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, input)

    runtimeResult should beColumns("x").withRows(rowCount(limit))
    input.hasMore shouldBe true
  }

  test("should support reduce -> limit on the RHS of conditional apply") {
    // given
    val nodesPerLabel = 100
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .conditionalApply("x")
      .|.limit(10)
      .|.sort(Seq(Ascending("y")))
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = for{
      x <- aNodes
      _ <- 1 to 10
    } yield Array[Any](x)

    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should aggregation on top of conditional apply with expand and limit and aggregation on rhs of apply") {
    // given
    val nodesPerLabel = 10
    val (aNodes, _) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val limit = nodesPerLabel / 2

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("counts")
      .aggregation(Seq.empty, Seq("count(x) AS counts"))
      .conditionalApply("x")
      .|.aggregation(Seq.empty, Seq("count(*) AS c"))
      .|.limit(limit)
      .|.expand("(x)-[:R]->(y)")
      .|.argument("x")
      .nodeByLabelScan("x","A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("counts").withSingleRow(nodesPerLabel)
  }

  test("should aggregate with no grouping on top of conditional apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("xs")
      .aggregation(Seq.empty, Seq("count(x) AS xs"))
      .conditionalApply("x")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x","A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("xs").withSingleRow(aNodes.size * bNodes.size)
  }

  test("should aggregate on top of conditional apply with expand on RHS") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xs")
      .aggregation(Seq("x AS x"), Seq("count(x) AS xs"))
      .conditionalApply("x")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x","A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map(x => Array[Any](x, bNodes.size))
    runtimeResult should beColumns("x", "xs").withRows(expected)
  }

  test("should aggregate on top of conditional apply with expand on RHS with nulls") {
    // given
    val nodesPerLabel = 10
    val (aNodes, bNodes) = given { bipartiteGraph(nodesPerLabel, "A", "B", "R",
      { case i if i % 2 == 0 => Map("prop" -> i)})}

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "xs")
      .aggregation(Seq("x AS x"), Seq("count(x) AS xs"))
      .conditionalApply("prop")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .projection("x AS x", "x.prop AS prop")
      .nodeByLabelScan("x","A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = aNodes.map {
      case x if x.hasProperty("prop") => Array[Any](x, bNodes.size)
      case x  => Array[Any](x, 1)
    }
    runtimeResult should beColumns("x", "xs").withRows(expected)
  }

}
