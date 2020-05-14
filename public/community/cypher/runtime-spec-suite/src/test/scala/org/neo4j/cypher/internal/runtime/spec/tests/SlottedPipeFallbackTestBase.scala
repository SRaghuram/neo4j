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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.exceptions.HintException

abstract class SlottedPipeFallbackTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should expand into and provide variables for relationship - outgoing") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "r")
      .expandInto("(x)-[r]->(y)")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("x", "y", "r").withRows(expected)
  }

  test("should use fallback correctly if rows are filtered out by fallback pipe") {
    // given
    val rels = given {
      val (_, rels) = circleGraph(sizeHint)
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "n", "m")
      .projectEndpoints("(n)-[r]->(m)", startInScope = true, endInScope = false)
      .input(nodes = Seq("n"), relationships = Seq("r"))
      .build()

    val input = for {
      r <- rels
      n <- Seq(r.getStartNode, r.getEndNode)
    } yield Array[Any](n, r)

    val runtimeResult = execute(logicalQuery, runtime, inputValues(input: _*))

    // then
    val expected = rels.map { r => Array(r, r.getStartNode, r.getEndNode) }
    runtimeResult should beColumns("r", "n", "m").withRows(expected)
  }

  test("should get exception with error plan") {
    given { nodeGraph(10) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .errorPlan(new HintException("hello"))
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    a[HintException] should be thrownBy {
      consume(runtimeResult)
    }
  }

  test("should do multiple middle correctly and profile") {
    // given
    val n = sizeHint
    val relTuples = (for(i <- 0 until n) yield {
      Seq(
        (i, (i + 1) % n, "NEXT")
      )
    }).reduce(_ ++ _)

    val (nodes, rels) = given {
      val nodes = nodeGraph(n, "Honey")
      val rels = connect(nodes, relTuples)
      (nodes, rels)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "n", "m", "r") // 0
      .projectEndpoints("(a)-[r]->(b)", startInScope = false, endInScope = false) // 1
      .projectEndpoints("(n)-[r]->(m)", startInScope = false, endInScope = false) // 2
      .input(relationships = Seq("r")) // 3
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(rels.map(Array[Any](_)): _*))

    // then
    val expected = relTuples.zip(rels).map {
      case ((f, t, _), rel) => Array(nodes(f), nodes(t), nodes(f), nodes(t), rel)
    }

    runtimeResult should beColumns("a", "b", "n", "m", "r").withRows(expected)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    // ROWS
    queryProfile.operatorProfile(0).rows() shouldBe relTuples.size // produce result
    queryProfile.operatorProfile(1).rows() shouldBe relTuples.size // project endpoints
    queryProfile.operatorProfile(2).rows() shouldBe relTuples.size // project endpoints
    queryProfile.operatorProfile(3).rows() shouldBe relTuples.size // input

    // TIME
    queryProfile.operatorProfile(0).time() should be > 0L // produce result
    queryProfile.operatorProfile(1).time() shouldBe OperatorProfile.NO_DATA // project endpoints
    queryProfile.operatorProfile(2).time() shouldBe OperatorProfile.NO_DATA // project endpoints
    queryProfile.operatorProfile(3).time() should be > 0L // input
  }

  test("should profile head with multiple middle correctly") {
    // given
    val (nodes, rels) = given {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "n", "m", "r", "x", "y") // 0
      .projectEndpoints("(a)-[r]->(b)", startInScope = false, endInScope = false) // 1
      .projectEndpoints("(n)-[r]->(m)", startInScope = false, endInScope = false) // 2
      .pruningVarExpand("(x)-[*1..1]->(y)") // 3
      .input(nodes = Seq("x"), relationships = Seq("r")) // 4
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues((0 until sizeHint).map(i => Array[Any](nodes(i), rels(i))): _*))
    consume(runtimeResult)

    val queryProfile = runtimeResult.runtimeResult.queryProfile()

    // ROWS
    queryProfile.operatorProfile(0).rows() shouldBe sizeHint // produce result
    queryProfile.operatorProfile(1).rows() shouldBe sizeHint // project endpoints
    queryProfile.operatorProfile(2).rows() shouldBe sizeHint // project endpoints
    queryProfile.operatorProfile(3).rows() shouldBe sizeHint // var expand
    queryProfile.operatorProfile(4).rows() shouldBe sizeHint // input

    // TIME
    queryProfile.operatorProfile(0).time() should be > 0L // produce result
    queryProfile.operatorProfile(1).time() shouldBe OperatorProfile.NO_DATA // project endpoints
    queryProfile.operatorProfile(2).time() shouldBe OperatorProfile.NO_DATA // project endpoints
    queryProfile.operatorProfile(3).time() shouldBe OperatorProfile.NO_DATA // var expand
    queryProfile.operatorProfile(4).time() should be > 0L // input

    // DB HITS
    queryProfile.operatorProfile(0).dbHits() shouldBe 0L // produce result
    queryProfile.operatorProfile(1).dbHits() shouldBe 0L // project endpoints
    queryProfile.operatorProfile(2).dbHits() shouldBe 0L // project endpoints
    queryProfile.operatorProfile(3).dbHits() shouldBe 2 * sizeHint // var expand
    queryProfile.operatorProfile(4).dbHits() shouldBe 0L // input
  }
}
