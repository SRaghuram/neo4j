/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanConstructionTestSupport
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.Assertion

class UnnestApplyTest extends CypherFunSuite with LogicalPlanRewritingTestSupport with LogicalPlanConstructionTestSupport with AstConstructionTestSupport {

  private val po_n: ProvidedOrder = ProvidedOrder.asc(varFor("n"))

  test("should unnest apply with a single Argument on the lhs") {
    // If we tracked distinctness, we would get input plans where the RHS provided order
    // would be already propagated to the top of the Apply.
    // But with that missing, the top of the Apply will currently not have any ProvidedOrder

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .apply().withCardinality(20)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes(new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
    )
  }

  test("should unnest apply with a single Argument on the rhs") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20).withProvidedOrder(po_n)
      .apply().withCardinality(20).withProvidedOrder(po_n)
      .|.argument().withCardinality(1)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(20).withProvidedOrder(po_n)
      .nodeByLabelScan("m", "M", "n").withCardinality(20).withProvidedOrder(po_n)
    )
  }

  test("should not cross OPTIONAL boundaries") {
    val input = new LogicalPlanBuilder()
      .produceResults("x", "n")
      .apply()
      .|.optional("n")
      .|.filter("n.prop = 42")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("apply on apply should be extracted nicely") {
    /*
                            Apply1
                         LHS   Apply2       =>  Expand
                             Arg1  Expand        LHS
                                      Arg2
     */


    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m", "o").withCardinality(300).withProvidedOrder(po_n)
      .apply().withCardinality(300).withProvidedOrder(po_n)
      .|.apply().withCardinality(3)
      .|.|.expand("(n)-->(m)").withCardinality(6)
      .|.|.argument("n").withCardinality(1)
      .|.filter("n.prop = 42").withCardinality(0.5)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m", "o").withCardinality(300).withProvidedOrder(po_n)
        .expand("(n)-->(m)").withCardinality(300).withProvidedOrder(po_n)
        .filter("n.prop = 42").withCardinality(50).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("apply on apply should be extracted nicely 2") {
    /*  Moves filtering on a LHS of inner Apply to the LHS of the outer

                            Apply1                      Apply1
                         LHS1     Apply2       =>  Filter    Apply2
                              Filter   RHS        LHS1     LHS2   RHS
                           LHS2
     */

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(40000).withProvidedOrder(po_n)
      .apply().withCardinality(40000).withProvidedOrder(po_n)
      .|.apply().withCardinality(400)
      .|.|.nodeByLabelScan("o", "O", "n", "m").withCardinality(40)
      .|.filter("n.prop > 10").withCardinality(10)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(40000).withProvidedOrder(po_n)
      .apply().withCardinality(40000).withProvidedOrder(po_n)
      .|.apply().withCardinality(800)
      .|.|.nodeByLabelScan("o", "O", "n", "m").withCardinality(40)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20)
      .filter("n.prop > 10").withCardinality(50).withProvidedOrder(po_n)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("apply on apply should be left unchanged") {
    /*  Does not moves filtering on a LHS of inner Apply to the LHS of the outer if Selection has a dependency on a
    variable introduced in the source operator

                            Apply1
                         LHS1     Apply2       =>  remains unchanged
                              Filter   RHS
                           LHS2
     */

    val input = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(40000)
      .apply().withCardinality(40000)
      .|.apply().withCardinality(400)
      .|.|.nodeByLabelScan("o", "O", "n", "m").withCardinality(40)
      .|.filter("m.prop > 10").withCardinality(10)
      .|.nodeByLabelScan("m", "M", "n").withCardinality(20)
      .allNodeScan("n").withCardinality(100)
      .build()

    rewrite(input) should equal(input)
  }

  test("apply on apply on optional should be OK") {
    /*
                            Apply1                Apply
                         LHS   Apply2       =>  LHS Optional
                             Arg1  Optional             Expand
                                     Expand                Arg
                                       Arg2
     */

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .apply().withCardinality(1000).withProvidedOrder(po_n)
      .|.apply().withCardinality(10)
      .|.|.optional("n").withCardinality(10)
      .|.|.expand("(n)-->(m)").withCardinality(15)
      .|.|.argument("n").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .apply().withCardinality(1000).withProvidedOrder(po_n)
      .|.optional("n").withCardinality(10)
      .|.expand("(n)-->(m)").withCardinality(15)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("AntiConditionalApply on apply on optional should be OK") {
    /*
                            ACA                  ACA
                         LHS   Apply       =>  LHS Optional
                             Arg1  Optional             Expand
                                     Expand                Arg
                                       Arg2
     */

    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .antiConditionalApply("n").withCardinality(1000).withProvidedOrder(po_n)
      .|.apply().withCardinality(10)
      .|.|.optional("n").withCardinality(10)
      .|.|.expand("(n)-->(m)").withCardinality(15)
      .|.|.argument("n").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(1000).withProvidedOrder(po_n)
      .antiConditionalApply("n").withCardinality(1000).withProvidedOrder(po_n)
      .|.optional("n").withCardinality(10)
      .|.expand("(n)-->(m)").withCardinality(15)
      .|.argument("n").withCardinality(1)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("π (Arg) Ax R => π (R)") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(100)
      .apply().withCardinality(100)
      .|.allNodeScan("n", "x").withCardinality(100).withProvidedOrder(po_n)
      .projection("5 AS x").withCardinality(1)
      .argument().withCardinality(1)

    inputBuilder shouldRewriteToPlanWithAttributes(new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(100)
      .projection("5 as x").withCardinality(100).withProvidedOrder(po_n)
      .allNodeScan("n").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("π (Arg) Ax R => π (R): keeps arguments if nested under another apply") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(10000)
      .apply().withCardinality(10000)
      .|.apply().withCardinality(100)
      .|.|.allNodeScan("n", "x", "m").withCardinality(100).withProvidedOrder(po_n)
      .|.projection("5 AS x").withCardinality(1)
      .|.argument("m").withCardinality(1)
      .allNodeScan("m").withCardinality(100)

    inputBuilder shouldRewriteToPlanWithAttributes(new LogicalPlanBuilder()
      .produceResults("x", "n").withCardinality(10000)
      .projection("5 as x").withCardinality(10000)
      .apply().withCardinality(10000)
      .|.allNodeScan("n", "m").withCardinality(100).withProvidedOrder(po_n)
      .allNodeScan("m").withCardinality(100)
    )
  }

  test("π (Arg) Ax R => π (R): if R uses projected value") {
    val input = new LogicalPlanBuilder()
      .produceResults("x", "n")
      .apply()
      .|.nodeIndexOperator("n:Label(prop=???)", paramExpr = Some(varFor("x")), argumentIds = Set("x"))
      .projection("5 AS x")
      .argument()
      .build()

    rewrite(input) should equal(input)
  }

  test("should unnest Expand and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.expand("(n)-->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
    new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .expand("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest two Expands and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m", "o").withCardinality(400).withProvidedOrder(po_n)
      .apply().withCardinality(400).withProvidedOrder(po_n)
      .|.expand("(m)-->(o)").withCardinality(4)
      .|.expand("(n)-->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m", "o").withCardinality(400).withProvidedOrder(po_n)
        .expand("(m)-->(o)").withCardinality(400).withProvidedOrder(po_n)
        .expand("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest Expand but keep Apply and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.expandInto("(n)-->(m)").withCardinality(2)
      .|.allNodeScan("m").withCardinality(1000)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
        .expandInto("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
        .apply().withCardinality(100000).withProvidedOrder(po_n)
        .|.allNodeScan("m").withCardinality(1000)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest Selection and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(50).withProvidedOrder(po_n)
      .apply().withCardinality(50).withProvidedOrder(po_n)
      .|.filter("n.prop > 0").withCardinality(0.5)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n").withCardinality(50).withProvidedOrder(po_n)
        .filter("n.prop > 0").withCardinality(50).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest Projection and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(50).withProvidedOrder(po_n)
      .apply().withCardinality(50).withProvidedOrder(po_n)
      .|.projection("n AS m").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(50).withProvidedOrder(po_n)
        .projection("n AS m").withCardinality(100).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest VarExpand and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.expand("(n)-[*]->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
        .expand("(n)-[*]->(m)").withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest OptionalExpand and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
      .apply().withCardinality(200).withProvidedOrder(po_n)
      .|.optionalExpandAll("(n)-->(m)").withCardinality(2)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(200).withProvidedOrder(po_n)
        .optionalExpandAll("(n)-->(m)").withCardinality(200).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should not unnest OptionalExpand on top of non-Argument") {
    val input = new LogicalPlanBuilder()
      .produceResults("n", "m")
      .apply().withCardinality(400)
      .|.optionalExpandAll("(n)-->(o)")
      .|.optionalExpandAll("(n)-->(m)")
      .|.argument("n")
      .nodeByLabelScan("n", "N")
      .build()

    rewrite(input) should equal(input)
  }

  test("should unnest Create and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(100).withProvidedOrder(po_n)
      .apply().withCardinality(100).withProvidedOrder(po_n)
      .|.create(createNode("m")).withCardinality(1)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(100).withProvidedOrder(po_n)
        .create(createNode("m")).withCardinality(100).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest ForeachApply and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n").withCardinality(100)
      .apply().withCardinality(100)
      .|.foreachApply("n", "[1, 2, 3]").withCardinality(1)
      .|.|.setProperty("n", "prop", "5").withCardinality(1)
      .|.|.argument("n").withCardinality(1)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n").withCardinality(100)
        .foreachApply("n", "[1, 2, 3]").withCardinality(100) // Writing plans do not keep provided order
        .|.setProperty("n", "prop", "5").withCardinality(1)
        .|.argument("n").withCardinality(1)
        .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
    )
  }

  test("should unnest LeftOuterHashJoin and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(2500)
      .apply().withCardinality(2500)
      .|.leftOuterHashJoin("n").withCardinality(25).withProvidedOrder(po_n)
      .|.|.expand("(m)--(n)").withCardinality(50).withProvidedOrder(po_n)
      .|.|.nodeByLabelScan("m", "M").withCardinality(10).withProvidedOrder(po_n)
      .|.argument("n").withCardinality(1)
      .nodeByLabelScan("n", "N").withCardinality(100)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(2500)
        .leftOuterHashJoin("n").withCardinality(2500).withProvidedOrder(po_n)
        .|.expand("(m)--(n)").withCardinality(50).withProvidedOrder(po_n)
        .|.nodeByLabelScan("m", "M").withCardinality(10).withProvidedOrder(po_n)
        .nodeByLabelScan("n", "N").withCardinality(100)
    )
  }

  test("should unnest RightOuterHashJoin and multiply cardinality") {
    val inputBuilder = new LogicalPlanBuilder()
      .produceResults("n", "m").withCardinality(2500)
      .apply().withCardinality(2500)
      .|.rightOuterHashJoin("n").withCardinality(25).withProvidedOrder(po_n)
      .|.|.argument("n").withCardinality(1).withProvidedOrder(po_n)
      .|.expand("(m)--(n)").withCardinality(50)
      .|.nodeByLabelScan("m", "M").withCardinality(10)
      .nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)

    inputBuilder shouldRewriteToPlanWithAttributes(
      new LogicalPlanBuilder()
        .produceResults("n", "m").withCardinality(2500)
        .rightOuterHashJoin("n").withCardinality(2500).withProvidedOrder(po_n) // Assuming the label-scan order was propagated to the removed Argument
        .|.nodeByLabelScan("n", "N").withCardinality(100).withProvidedOrder(po_n)
        .expand("(m)--(n)").withCardinality(50)
        .nodeByLabelScan("m", "M").withCardinality(10)
    )
  }

  implicit private class AssertableInputBuilder(inputBuilder: LogicalPlanBuilder) {
    def shouldRewriteToPlanWithAttributes(expectedBuilder: LogicalPlanBuilder): Assertion = {
      val resultPlan = rewrite(inputBuilder.build(), inputBuilder.cardinalities, inputBuilder.providedOrders,inputBuilder.idGen)
      (resultPlan, inputBuilder.cardinalities) should haveSameCardinalitiesAs((expectedBuilder.build(), expectedBuilder.cardinalities))
      (resultPlan, inputBuilder.providedOrders) should haveSameProvidedOrdersAs((expectedBuilder.build(), expectedBuilder.providedOrders))
    }
  }

  private def rewrite(p: LogicalPlan, cardinalities: Cardinalities, providedOrders: ProvidedOrders, idGen: IdGen): LogicalPlan = {
    val unnest = unnestApply(
      new StubSolveds,
      cardinalities,
      providedOrders,
      Attributes(idGen)
    )
    p.endoRewrite(unnest)
  }

  private def stubCardinalities(): StubCardinalities = new StubCardinalities {
    override def defaultValue: Cardinality = Cardinality.SINGLE
  }

  private def rewrite(p: LogicalPlan): LogicalPlan = rewrite(p, stubCardinalities(), new StubProvidedOrders, idGen)
}
