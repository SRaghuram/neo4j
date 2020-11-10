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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class LimitPropagationPlanningIntegrationTest
  extends CypherFunSuite
  with LogicalPlanningIntegrationTestSupport
  with AstConstructionTestSupport {

  private def statisticsForLimitPropagationTests(plannerBuilder: StatisticsBackedLogicalPlanningConfigurationBuilder) =
    plannerBuilder
      .setAllNodesCardinality(3333)
      .setLabelCardinality("A", 111)
      .setLabelCardinality("B", 7)
      .setLabelCardinality("C", 2222)
      .setRelationshipCardinality("(:A)-[:REL_AB]->()", 123)
      .setRelationshipCardinality("(:A)-[:REL_AB]->(:B)", 123)
      .setRelationshipCardinality("()-[:REL_AB]->(:B)", 555)
      .setRelationshipCardinality("(:C)-[:REL_CB]->()", 4444)
      .setRelationshipCardinality("(:C)-[:REL_CB]->(:B)", 4444)
      .setRelationshipCardinality("()-[:REL_CB]->(:B)", 10000)
      .addIndex("A", Seq("id"), 0.5, 1.0 / 111.0, providesOrder = IndexOrderCapability.ASC)
      .addIndex("C", Seq("id"), 0.5, 1.0 / 2222.0, providesOrder = IndexOrderCapability.ASC)
      .build()

  private def assertExpectedPlanForQueryGivenStatistics(queryString: String,
                                                        buildStats: StatisticsBackedLogicalPlanningConfigurationBuilder => StatisticsBackedLogicalPlanningConfiguration)
                                                       (buildExpectedPlan: LogicalPlanBuilder => LogicalPlan): Assertion = {
    val cfg = buildStats(plannerBuilder())
    val plan = cfg.plan(queryString)
    plan shouldEqual buildExpectedPlan(cfg.planBuilder())
  }

  test("should plan lazy index seek instead of sort when under limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |RETURN a, c ORDER BY c.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder => planBuilder
      .produceResults("a", "c")
      .limit(10)
      .nodeHashJoin("b")
      .|.expandAll("(c)-[cb:REL_CB]->(b)")
      .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending)
      .filterExpression(hasLabels("b", "B"))
      .expandAll("(a)-[ab:REL_AB]->(b)")
      .nodeIndexOperator("a:A(id = 123)", indexOrder = IndexOrderAscending)
      .build()
    }
  }

  test("should plan lazy index scan instead of sort when under limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE EXISTS(c.id)
         |RETURN a, c ORDER BY c.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder => planBuilder
      .produceResults("a", "c")
      .limit(10)
      .nodeHashJoin("b")
      .|.expandAll("(c)-[cb:REL_CB]->(b)")
      .|.nodeIndexOperator("c:C(id)", indexOrder = IndexOrderAscending)
      .filterExpression(hasLabels("b", "B"))
      .expandAll("(a)-[ab:REL_AB]->(b)")
      .nodeIndexOperator("a:A(id = 123)", indexOrder = IndexOrderAscending)
      .build()
    }
  }

  test("should not (yet) plan lazy index seek instead of sort when limit is in a different query part") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH a, c ORDER BY c.id
         |RETURN a, c LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder => planBuilder
      .produceResults("a", "c")
      .top(Seq(Ascending("c.id")), 10)
      .projection("cache[c.id] AS `c.id`")
      .filterExpression(
        hasLabels("c", "C"),
        startsWith(
          cachedNodeProp("c", "id"),
          literalString("")))
      .expandAll("(b)<-[cb:REL_CB]-(c)")
      .filterExpression(hasLabels("b", "B"))
      .expandAll("(a)-[ab:REL_AB]->(b)")
      .nodeIndexOperator("a:A(id = 123)", indexOrder = IndexOrderAscending)
      .build()
    }
  }

  test("should plan lazy index seek instead of sort when under limit and aggregation in the next query part") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH a, c ORDER BY c.id LIMIT 10
         |RETURN count(*) AS count
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder => planBuilder
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(*) AS count"))
      .limit(10)
      .nodeHashJoin("b")
      .|.expandAll("(c)-[cb:REL_CB]->(b)")
      .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending)
      .filterExpression(hasLabels("b", "B"))
      .expandAll("(a)-[ab:REL_AB]->(b)")
      .nodeIndexOperator("a:A(id = 123)", indexOrder = IndexOrderAscending)
      .build()
    }
  }

  test("should plan lazy index seek instead of sort when under limit and small skip") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |RETURN a, c ORDER BY c.id
         |SKIP 7 LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder => planBuilder
      .produceResults("a", "c")
      .skip(7)
      .limit(add(literalInt(10), literalInt(7)))
      .nodeHashJoin("b")
      .|.expandAll("(c)-[cb:REL_CB]->(b)")
      .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending)
      .filterExpression(hasLabels("b", "B"))
      .expandAll("(a)-[ab:REL_AB]->(b)")
      .nodeIndexOperator("a:A(id = 123)", indexOrder = IndexOrderAscending)
      .build()
    }
  }

  test("should not plan lazy index seek instead of sort when under limit and large skip") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |RETURN a, c ORDER BY c.id
         |SKIP 100000 LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder => planBuilder
      .produceResults("a", "c")
      .skip(100000)
      .top(Seq(Ascending("c.id")), add(literalInt(10), literalInt(100000)))
      .projection("cache[c.id] AS `c.id`")
      .filterExpression(
        hasLabels("c", "C"),
        startsWith(
          cachedNodeProp("c", "id"),
          literalString("")))
      .expandAll("(b)<-[cb:REL_CB]-(c)")
      .filterExpression(hasLabels("b", "B"))
      .expandAll("(a)-[ab:REL_AB]->(b)")
      .nodeIndexOperator("a:A(id = 123)", indexOrder = IndexOrderAscending)
      .build()
    }
  }
}
