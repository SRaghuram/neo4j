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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical

import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.RegularPlannerQuery
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.{ASC, BOTH, DESC}
import org.neo4j.cypher.internal.v4_0.expressions.{LabelToken, SemanticDirection}
import org.neo4j.cypher.internal.logical.plans.{Limit => LimitPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class IndexWithProvidedOrderPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  case class TestOrder(indexOrder: IndexOrder, cypherToken: String, indexOrderCapability: IndexOrderCapability, sortOrder: String => ColumnOrder)
  val ASCENDING = TestOrder(IndexOrderAscending, "ASC", ASC, Ascending)
  val DESCENDING = TestOrder(IndexOrderDescending, "DESC", DESC, Descending)
  val DESCENDING_BOTH = TestOrder(IndexOrderDescending, "DESC", BOTH, Descending)

  for (TestOrder(plannedOrder, cypherToken, orderCapability, sortOrder) <- List(ASCENDING, DESCENDING, DESCENDING_BOTH)) {

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan sort if index does not provide order") {
      val plan = new given {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Sort(
          Projection(
            IndexSeek(
              "n:Awesome(prop > 'foo')", indexOrder =
                IndexOrderNone),
            Map("n.prop" -> prop("n", "prop"))),
          Seq(sortOrder("n.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan partial sort if index does partially provide order") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken, n.foo ASC"

      plan._2 should equal(
        PartialSort(
          Projection(
            Projection(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
              Map("n.prop" -> prop("n", "prop"))),
            Map("n.foo" -> prop("n", "foo"))),
          Seq(sortOrder("n.prop")), Seq(Ascending("n.foo")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan partial sort if index does partially provide order and the second column is more complicated") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken, n.foo + 1 ASC"

      plan._2 should equal(
          PartialSort(
            Projection(
              Projection(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
                Map("n.prop" -> prop("n", "prop"))),
              Map("n.foo + 1" -> add(prop("n", "foo"), literalInt(1)))),
            Seq(sortOrder("n.prop")), Seq(Ascending("n.foo + 1")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan multiple partial sorts") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' WITH n, n.prop AS p, n.foo AS f ORDER BY p $cypherToken, f ASC RETURN p ORDER BY p $cypherToken, f ASC, n.bar ASC"


      plan._2 should equal(
        PartialSort(
          Projection(
            PartialSort(
              Projection(
                IndexSeek(
                  "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
                Map("f" -> prop("n", "foo"), "p" -> prop("n", "prop"))),
              Seq(sortOrder("p")), Seq(Ascending("f"))),
            Map("n.bar" -> prop("n", "bar"))),
          Seq(sortOrder("p"), Ascending("f")), Seq(Ascending("n.bar")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property renamed in an earlier WITH") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor
        s"""MATCH (n:Awesome) WHERE n.prop > 'foo'
           |WITH n AS nnn
           |MATCH (m)-[r]->(nnn)
           |RETURN nnn.prop ORDER BY nnn.prop $cypherToken""".stripMargin

      plan._2 should equal(
        Projection(
          Expand(
            Projection(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
              Map("nnn" -> varFor("n"))),
            "nnn", SemanticDirection.INCOMING, Seq.empty, "m", "r"),
          Map("nnn.prop" -> prop("nnn", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property renamed in same return") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor
        s"""MATCH (n:Awesome) WHERE n.prop > 'foo'
           |RETURN n AS m ORDER BY m.prop $cypherToken""".stripMargin

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
          Map("m" -> varFor("n")))
      )
    }

    test(s"$cypherToken-$orderCapability: Cannot order by index when ordering is on same property name, but different node") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (m:Awesome), (n:Awesome) WHERE n.prop > 'foo' RETURN m.prop ORDER BY m.prop $cypherToken"

      val expectedIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        Sort(
          Projection(
            CartesianProduct(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = expectedIndexOrder),
              NodeByLabelScan("m", labelName("Awesome"), Set.empty)),
            Map("m.prop" -> prop("m", "prop"))),
          Seq(sortOrder("m.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (starts with scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "n:Awesome(prop STARTS WITH 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    // This is supported because internally all kernel indexes which support ordering will just scan and filter to serve contains
    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (contains scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop CONTAINS 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    // This is supported because internally all kernel indexes which support ordering will just scan and filter to serve ends with
    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (ends with scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop ENDS WITH 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE EXISTS(n.prop) RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "n:Awesome(prop)", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an Apply") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        indexOn("B", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop > 'foo' AND a.prop = b.prop RETURN a.prop ORDER BY a.prop $cypherToken"

      val expectedBIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        Projection(
          Apply(
            IndexSeek(
              "a:A(prop > 'foo')", indexOrder = plannedOrder),
            IndexSeek("b:B(prop = ???)",
              indexOrder = expectedBIndexOrder,
              paramExpr = Some(prop("a", "prop")),
              labelId = 1,
              argumentIds = Set("a"))
          ),
          Map("a.prop" -> prop("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed properties in a plan with an Apply needs Partial Sort if RHS order required") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        indexOn("B", "prop").providesOrder(orderCapability)
        // This query is very fragile in the sense that the slightest modification will result in a stupid plan
      } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop STARTS WITH 'foo' AND b.prop > a.prop RETURN a.prop, b.prop ORDER BY a.prop $cypherToken, b.prop $cypherToken"

      val expectedBIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        PartialSort(
          Projection(
            Apply(
              IndexSeek("a:A(prop STARTS WITH 'foo')", indexOrder = plannedOrder),
              IndexSeek("b:B(prop > ???)",
                indexOrder = expectedBIndexOrder,
                paramExpr = Some(prop("a", "prop")),
                labelId = 1,
                argumentIds = Set("a"))
            ),
            Map("a.prop" -> prop("a", "prop"), "b.prop" -> prop("b", "prop"))),
          Seq(sortOrder("a.prop")), Seq(sortOrder("b.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an renaming Projection") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (a:A) WHERE a.prop > 'foo' WITH a.prop AS theProp, 1 AS x RETURN theProp ORDER BY theProp $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "a:A(prop > 'foo')", indexOrder = plannedOrder),
          Map("theProp" -> prop("a", "prop"), "x" -> literalInt(1)))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an aggregation and an expand") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN a.prop, count(b) ORDER BY a.prop $cypherToken"

      plan._2 should equal(
        Aggregation(
          Expand(
            IndexSeek(
              "a:A(prop > 'foo')", indexOrder = plannedOrder),
            "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
          Map("a.prop" -> prop("a", "prop")), Map("count(b)" -> count(varFor("b"))))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with partial provided order and with an expand") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken, b.prop"

      plan._2 should equal(
        PartialSort(
          Projection(
            Projection(
              Expand(
                IndexSeek(
                  "a:A(prop > 'foo')", indexOrder = plannedOrder),
                "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
              Map("a.prop" -> prop("a", "prop"))),
            Map("b.prop" -> prop("b", "prop"))),
          Seq(sortOrder("a.prop")), Seq(Ascending("b.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with partial provided order and with two expand - should plan partial sort in the middle") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("c") => 2000.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 50.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b", "c") => 500.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 1000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b)-[q]->(c) WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken, b.prop"

      plan._2 should equal(
        Selection(Seq(not(equals(varFor("q"), varFor("r")))),
          Expand(
            PartialSort(
              Projection(
                Projection(
                  Expand(
                    IndexSeek(
                      "a:A(prop > 'foo')", indexOrder = plannedOrder),
                    "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
                  Map("a.prop" -> prop("a", "prop"))),
                Map("b.prop" -> prop("b", "prop"))),
              Seq(sortOrder("a.prop")), Seq(Ascending("b.prop"))),
            "b", SemanticDirection.OUTGOING, Seq.empty, "c", "q"))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a distinct") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN DISTINCT a.prop ORDER BY a.prop $cypherToken"

      plan._2 should equal(
        Distinct(
          Expand(
            IndexSeek(
              "a:A(prop > 'foo')", indexOrder = plannedOrder),
            "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
          Map("a.prop" -> prop("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an outer join") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at b
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 20.0
        }
      } getLogicalPlanFor s"MATCH (b) OPTIONAL MATCH (a:A)-[r]->(b) USING JOIN ON b WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken"

      plan._2 should equal(
        Projection(
          LeftOuterHashJoin(Set("b"),
            AllNodesScan("b", Set.empty),
            Expand(
              IndexSeek(
                "a:A(prop > 'foo')", indexOrder = plannedOrder),
              "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")),
          Map("a.prop" -> prop("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a tail apply") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor
        s"""MATCH (a:A) WHERE a.prop > 'foo' WITH a SKIP 0
           |MATCH (b)
           |RETURN a.prop, b ORDER BY a.prop $cypherToken""".stripMargin

      plan._2 should equal(
        Projection(
          Apply(
            SkipPlan(
              IndexSeek(
                "a:A(prop > 'foo')", indexOrder = plannedOrder),
              literalInt(0)),
            AllNodesScan("b", Set("a"))),
          Map("a.prop" -> prop("a", "prop")))
      )
    }

    // Given that we could get provided order for this query (by either a type constraint
    // or kernel support for ordering in point index), it should be possible to skip the sorting
    // for this case. Right now this only works on integration test level and not in production.
    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (scan) in case of existence constraint") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
        existenceOrNodeKeyConstraintOn("Awesome", Set("prop"))
      } getLogicalPlanFor s"MATCH (n:Awesome) RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "n:Awesome(prop)", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }
  }

  // Min and Max

  // Tests (ASC, min), (DESC, max), (BOTH, min), (BOTH, max) -> interesting and provided order are the same
  val ASCENDING_BOTH = TestOrder(IndexOrderAscending, "ASC", BOTH, Ascending)
  for ((TestOrder(plannedOrder, cypherToken, orderCapability, _), functionName) <- List((ASCENDING, "min"), (DESCENDING, "max"), (ASCENDING_BOTH, "min"), (DESCENDING_BOTH, "max"))) {

    test(s"$orderCapability-$functionName: should use provided index order with range") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop)"

      plan._2 should equal(
        Optional(
          LimitPlan(
            Projection(
              IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
              Map(s"$functionName(n.prop)" -> cachedNodeProperty("n", "prop"))
            ),
            literalInt(1),
            DoNotIncludeTies
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order with ORDER BY") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) ORDER BY $functionName(n.prop) $cypherToken"

      plan._2 should equal(
        Optional(
          LimitPlan(
            Projection(
              IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
              Map(s"$functionName(n.prop)" -> cachedNodeProperty("n", "prop"))
            ),
            literalInt(1),
            DoNotIncludeTies
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order followed by sort for ORDER BY with reverse order") {
      val (inverseOrder, inverseSortOrder) = cypherToken match {
        case "ASC" => ("DESC", Descending)
        case "DESC" => ("ASC", Ascending)
      }

      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) ORDER BY $functionName(n.prop) $inverseOrder"

      plan._2 should equal(
        Sort(
          Optional(
            LimitPlan(
              Projection(
                IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
                Map(s"$functionName(n.prop)" -> cachedNodeProperty("n", "prop"))
              ),
              literalInt(1),
              DoNotIncludeTies
            )),
          Seq(inverseSortOrder(s"$functionName(n.prop)"))
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order with additional Limit") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) LIMIT 2"

      plan._2 should equal(
        LimitPlan(
          Optional(
            LimitPlan(
              Projection(
                IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
                Map(s"$functionName(n.prop)" -> cachedNodeProperty("n", "prop"))
              ),
              literalInt(1),
              DoNotIncludeTies
            )),
          literalInt(2),
          DoNotIncludeTies
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order for multiple QueryGraphs") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor
        s"""MATCH (n:Awesome)
           |WHERE n.prop > 0
           |WITH $functionName(n.prop) AS agg
           |RETURN agg
           |ORDER BY agg $cypherToken""".stripMargin

      plan._2 should equal(
        Optional(
          LimitPlan(
            Projection(
              IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
              Map("agg" -> cachedNodeProperty("n", "prop"))
            ),
            literalInt(1),
            DoNotIncludeTies
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: cannot use provided index order for multiple aggregations") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop), count(n.prop)"

      val expectedIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        Aggregation(
          IndexSeek("n:Awesome(prop > 0)", indexOrder = expectedIndexOrder, getValue = GetValue),
          Map.empty,
          Map(s"$functionName(n.prop)" -> function(functionName, cachedNodeProperty("n", "prop")),
            "count(n.prop)" -> count(cachedNodeProperty("n", "prop")))
        )
      )
    }

    test(s"should plan aggregation with exists and index for $functionName when there is no $orderCapability") {
      val plan = new given {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE exists(n.prop) RETURN $functionName(n.prop)"

      plan._2 should equal(
        Aggregation(
          NodeIndexScan(
            "n",
            LabelToken("Awesome", LabelId(0)),
            indexedProperty("prop", 0, GetValue),
            Set.empty,
            IndexOrderNone),
          Map.empty,
          Map(s"$functionName(n.prop)" -> function(functionName, cachedNodeProperty("n", "prop")))
        )
      )
    }
  }

  // Tests (ASC, max), (DESC, min) -> interesting and provided order differs
  for ((TestOrder(plannedOrder, _, orderCapability, _), functionName) <- List((ASCENDING, "max"), (DESCENDING, "min"))) {

    test(s"$orderCapability-$functionName: cannot use provided index order with range") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop)"

      plan._2 should equal(
        Aggregation(
          IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
          Map.empty,
          Map(s"$functionName(n.prop)" -> function(functionName, cachedNodeProperty("n", "prop")))
        )
      )
    }
  }
}
