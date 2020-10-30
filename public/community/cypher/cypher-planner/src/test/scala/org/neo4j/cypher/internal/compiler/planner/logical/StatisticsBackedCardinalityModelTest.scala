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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.CardinalityModelIntegrationTest
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.AssumeIndependenceQueryGraphCardinalityModel.MIN_INBOUND_CARDINALITY
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class StatisticsBackedCardinalityModelTest extends CypherFunSuite with CardinalityModelIntegrationTest {

  val allNodes = 733.0
  val personCount = 324.0
  val relCount = 50.0
  val rel2Count = 78.0

  test("query containing a WITH and LIMIT on low/fractional cardinality") {
    val i = .1
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()",
      Math.max(MIN_INBOUND_CARDINALITY.amount, Math.min(i, 10.0)) * relCount / i)
  }

  test("query containing a WITH and LIMIT on high cardinality") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()",
      Math.min(i, 10.0) * relCount / i)
  }

  test("query containing a WITH and LIMIT on parameterized cardinality") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a LIMIT $limit MATCH (a)-[:REL]->()",
      Math.min(i, DEFAULT_LIMIT_CARDINALITY) * relCount / i)
  }

  test("query containing a WITH and aggregation vol. 2") {
    val patternNodeCrossProduct = allNodes * allNodes
    val labelSelectivity = personCount / allNodes
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = rel2Count / maxRelCount
    val firstQG = patternNodeCrossProduct * labelSelectivity * relSelectivity
    val aggregation = Math.sqrt(firstQG)

    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personCount)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .setRelationshipCardinality("()-[:REL2]->()", rel2Count)
      .setRelationshipCardinality("(:Person)-[:REL2]->()", rel2Count)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person)-[:REL2]->(b) WITH a, count(*) as c MATCH (a)-[:REL]->()",
      aggregation * relCount / personCount)
  }

  test("query containing both SKIP and LIMIT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (n:Person) WITH n SKIP 5 LIMIT 10",
      Math.min(i, 10.0))
  }

  test("query containing LIMIT by expression") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (n:Person) WITH n LIMIT toInteger(1+1)",
      Math.min(i, 2.0))
  }

  test("query containing both SKIP and LIMIT with large skip, so skip + limit exceeds total row count boundary") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, s"MATCH (n:Person) WITH n SKIP ${(personCount - 5).toInt} LIMIT 10",
      Math.min(i, 5.0))
  }

  test("query containing SKIP by expression") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, s"MATCH (n:Person) WITH n SKIP toInteger($personCount - 2)",
      Math.min(i, 2.0))
  }

  test("should reduce cardinality for a WHERE after a WITH") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a LIMIT 10 WHERE a.age = 20",
        Math.min(i, 10.0) * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("should reduce cardinality for a WHERE after a WITH, unknown LIMIT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a LIMIT $limit WHERE a.age = 20",
        Math.min(i, DEFAULT_LIMIT_CARDINALITY) * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("should reduce cardinality for a WHERE after a WITH, with ORDER BY") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a ORDER BY a.name WHERE a.age = 20",
        i * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("should reduce cardinality for a WHERE after a WITH, with DISTINCT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH DISTINCT a WHERE a.age = 20",
        i * DEFAULT_DISTINCT_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY)
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION without grouping") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH count(a) AS count WHERE count > 20",
        Math.max(DEFAULT_RANGE_SELECTIVITY, MIN_INBOUND_CARDINALITY.amount))
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION with grouping") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH count(a) AS count, a.name AS name WHERE count > 20",
        Math.sqrt(i) * DEFAULT_RANGE_SELECTIVITY)
  }

  private val signature = ProcedureSignature(
    QualifiedName(Seq("my", "proc"), "foo"),
    IndexedSeq(FieldSignature("int", CTInteger)),
    Some(IndexedSeq(FieldSignature("x", CTNode))),
    None,
    ProcedureReadOnlyAccess(Array.empty),
    id = 0)

  test("standalone procedure call should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "CALL my.proc.foo(42) YIELD x",
      DEFAULT_MULTIPLIER)
  }

  test("procedure call with no input should not have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Foo", 0)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) CALL my.proc.foo(42) YIELD x",
      1)
  }

  test("procedure call with large input should multiply cardinality") {
    val inputSize = 1000000
    val config = plannerBuilder()
      .setAllNodesCardinality(inputSize)
      .setLabelCardinality("Foo", inputSize)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) CALL my.proc.foo(42) YIELD x",
      DEFAULT_MULTIPLIER * inputSize)
  }

  test("standalone LOAD CSV should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "LOAD CSV FROM 'foo' AS csv",
      DEFAULT_MULTIPLIER)
  }

  test("LOAD CSV with no input should not have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Foo", 0)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) LOAD CSV FROM 'foo' AS csv",
      1)
  }

  test("LOAD CSV with large input should multiply cardinality") {
    val inputSize = 1000000
    val config = plannerBuilder()
      .setAllNodesCardinality(inputSize)
      .setLabelCardinality("Foo", inputSize)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) LOAD CSV FROM 'foo' AS csv",
      DEFAULT_MULTIPLIER * inputSize)
  }

  test("UNWIND with no information should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND $foo AS i",
      DEFAULT_MULTIPLIER)
  }

  test("UNWIND with empty list literal should have min inbound cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND [] AS i",
      MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with non-empty list literal should have list size cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND [1, 2, 3, 4, 5] AS i",
      5)
  }

  test("UNWIND with single element range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, 0) AS i",
      1)
  }

  test("UNWIND with empty range 1") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, -1) AS i",
      MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with empty range 2") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(10, 0, 1) AS i",
      MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with empty range 3") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, 10, -1) AS i",
      MIN_INBOUND_CARDINALITY.amount)
  }

  test("UNWIND with non-empty range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 10) AS i",
      10)
  }

  test("UNWIND with non-empty DESC range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(10, 1, -1) AS i",
      10)
  }

  test("UNWIND with non-empty range with aligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 9, 2) AS i",
      5)
  }

  test("UNWIND with non-empty DESC range with aligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(9, 1, -2) AS i",
      5)
  }

  test("UNWIND with non-empty range with unaligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 9, 3) AS i",
      3)
  }

  test("UNWIND with non-empty DESC range with unaligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(9, 1, -3) AS i",
      3)
  }

  test("empty graph") {
    val config = plannerBuilder()
      .setAllNodesCardinality(0)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a) WHERE a.prop = 10", 0)
  }

  test("honours bound arguments") {
    val relCount = 1000.0
    val fooCount = 100.0
    val barCount = 400.0
    val inboundCardinality = 13
    val nodeCount = fooCount + barCount
    val config = plannerBuilder()
      .setAllNodesCardinality(nodeCount)
      .setLabelCardinality("FOO", fooCount)
      .setLabelCardinality("BAR", barCount)
      .setAllRelationshipsCardinality(relCount)
      .setRelationshipCardinality("(:FOO)-[:TYPE]->()", relCount)
      .setRelationshipCardinality("()-[:TYPE]->(:BAR)", relCount)
      .setRelationshipCardinality("(:FOO)-[:TYPE]->(:BAR)", relCount)
      .build()

    queryShouldHaveCardinality(config, s"MATCH (a:FOO) WITH a LIMIT 1 UNWIND range(1, $inboundCardinality) AS i MATCH (a:FOO)-[:TYPE]->(b:BAR)",
      relCount / nodeCount * inboundCardinality)
  }

  test("input cardinality <1.0 => 1.0 * scan cardinality") {
    val nodes = 500
    val config = plannerBuilder()
      .setAllNodesCardinality(nodes)
      .setLabelCardinality("Foo", 1)
      .addIndex("Foo", Seq("bar"), 1.0, 0.5)
      .build()

    queryShouldHaveCardinality(config, s"MATCH (f:Foo) WHERE f.bar = 1 WITH f, 1 AS horizon MATCH (a)",
      nodes)
  }

  test("input cardinality >1.0 => input cardinality * scan cardinality") {
    val inboundCardinality = 10
    val nodes = 500
    val config = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("Foo", inboundCardinality)
      .addIndex("Foo", Seq("bar"), 1.0, 1.0)
      .build()

    queryShouldHaveCardinality(config, s"MATCH (f:Foo) WHERE f.bar = 1 WITH f, 1 AS horizon MATCH (a)",
      inboundCardinality * nodes)
  }
}
