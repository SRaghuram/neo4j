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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.PushBatchedExecution
import org.neo4j.cypher.internal.compiler.VolcanoModelExecution
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.BIG_CHUNK_SIZE
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.DEFAULT_COST_PER_ROW
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel.PROPERTY_ACCESS_DB_HITS
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.ir.LazyMode
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CardinalityCostModelTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private def costFor(plan: LogicalPlan,
                      input: QueryGraphSolverInput,
                      semanticTable: SemanticTable,
                      cardinalities: Cardinalities,
                      providedOrders: ProvidedOrders,
                      executionModel: ExecutionModel = VolcanoModelExecution): Cost = {
    CardinalityCostModel(executionModel).costFor(plan, input, semanticTable, cardinalities, providedOrders)
  }

  test("expand should only be counted once") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filterExpression(hasLabels("a", "Awesome")).withCardinality(10)
      .expand("(a)-[r1]->(b)").withCardinality(100)
      .filterExpression(hasLabels("a", "Awesome")).withCardinality(10)
      .expand("(a)-[r1]->(b)").withCardinality(100)
      .argument("a").withCardinality(10)
      .build()

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should equal(Cost(231))
  }

  test("should introduce increase cost when estimating an eager operator and laziness is preferred") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .nodeHashJoin("a").withCardinality(10)
      .|.nodeByLabelScan("a", "A").withCardinality(10)
      .expand("(b)-[r]->(a)").withCardinality(15)
      .nodeByLabelScan("b", "B").withCardinality(5)
      .build()

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    val whatever = QueryGraphSolverInput.empty

    costFor(plan, whatever, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should be < costFor(plan, pleaseLazy, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
  }

  test("non-lazy plan should be penalized when estimating cost wrt a lazy one when laziness is preferred") {
    // MATCH (a1: A)-[r1]->(b)<-[r2]-(a2: A) RETURN b
    val lazyBuilder = new LogicalPlanBuilder(wholePlan = false)
    val lazyPlan = lazyBuilder
      .projection("b AS b").withCardinality(250)
      .filterExpression(hasLabels("a2", "A")).withCardinality(250)
      .expand("(b)<-[r2]-(a2)").withCardinality(250)
      .expand("(a1)-[r1]->(b)").withCardinality(50)
      .nodeByLabelScan("a1", "A").withCardinality(10)
      .build()

    val eagerBuilder = new LogicalPlanBuilder(wholePlan = false)
    val eagerPlan = eagerBuilder
      .projection("b AS b").withCardinality(250)
      .nodeHashJoin("b").withCardinality(250)
      .|.expand("(a2)-[r2]->(b)").withCardinality(50)
      .|.nodeByLabelScan("a2", "A").withCardinality(10)
      .expand("(a1)-[r1]->(b)").withCardinality(50)
      .nodeByLabelScan("a1", "A").withCardinality(10)
      .build()

    val whatever = QueryGraphSolverInput.empty
    costFor(lazyPlan, whatever, lazyBuilder.getSemanticTable, lazyBuilder.cardinalities, lazyBuilder.providedOrders) should be > costFor(eagerPlan, whatever, eagerBuilder.getSemanticTable, eagerBuilder.cardinalities, eagerBuilder.providedOrders)

    val pleaseLazy = QueryGraphSolverInput.empty.withPreferredStrictness(LazyMode)
    costFor(lazyPlan, pleaseLazy, lazyBuilder.getSemanticTable, lazyBuilder.cardinalities, lazyBuilder.providedOrders) should be < costFor(eagerPlan, pleaseLazy, eagerBuilder.getSemanticTable, eagerBuilder.cardinalities, eagerBuilder.providedOrders)
  }

  test("multiple property expressions are counted for in cost") {
    val cardinality = 10.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filter("a.prop1 = 42", "a.prop1 = 43", "a.prop1 = 44").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality).newVar("a", CTNode)
      .build()

    val numberOfPredicates = 3
    val costForSelection = cardinality * numberOfPredicates * PROPERTY_ACCESS_DB_HITS
    val costForArgument = cardinality * DEFAULT_COST_PER_ROW.cost
    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should equal(Cost(costForSelection + costForArgument))
  }

  test("deeply nested property access does not increase cost") {
    val cardinality = 10.0

    val shallowPlanBuilder = new LogicalPlanBuilder(wholePlan = false)
    val shallowPlan = shallowPlanBuilder
      .filter("a.prop1 = 42").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality).newVar("a", CTNode)
      .build()
    val deepPlanBuilder = new LogicalPlanBuilder(wholePlan = false)
    val deepPlan = deepPlanBuilder
      .filter("a.foo.bar.baz.blob.boing.peng.brrt = 2").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality).newVar("a", CTNode)
      .build()

    costFor(shallowPlan, QueryGraphSolverInput.empty, shallowPlanBuilder.getSemanticTable, shallowPlanBuilder.cardinalities, shallowPlanBuilder.providedOrders) should
      equal(costFor(deepPlan, QueryGraphSolverInput.empty, deepPlanBuilder.getSemanticTable, deepPlanBuilder.cardinalities, deepPlanBuilder.providedOrders))
  }

  test("lazy plans should be cheaper when limit selectivity is < 1.0") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .filter("n:Label").withCardinality(47)
      .allNodeScan("n").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivity(Selectivity.of(0.5).get)

    costFor(plan, withLimit, builder.getSemanticTable, builder.cardinalities, new StubProvidedOrders) should be < costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
  }

  test("hash join should be cheaper when limit selectivity is < 1.0") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .nodeHashJoin("b").withCardinality(56)
      .|.expandAll("(c)-[:REL]->(b)").withCardinality(78)
      .|.nodeByLabelScan("c", "C").withCardinality(321)
      .filter("b:B").withCardinality(77)
      .expandAll("(a)-[:REL]->(b)").withCardinality(42)
      .nodeByLabelScan("a", "A").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivity(Selectivity.of(0.5).get)

    costFor(plan, withLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should be < costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders)
  }

  test("eager plans should cost the same regardless of limit selectivity") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .aggregation(Seq("n.prop AS x"), Seq("max(n.val) AS y"))
      .allNodeScan("n").withCardinality(123)
      .build()

    val withoutLimit = QueryGraphSolverInput.empty
    val withLimit = QueryGraphSolverInput.empty.withLimitSelectivity(Selectivity.of(0.5).get)

    costFor(plan, withLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders) should equal(costFor(plan, withoutLimit, builder.getSemanticTable, builder.cardinalities, builder.providedOrders))
  }

  test("cartesian product with 1 row from the left is equally expensive in both execution models") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(100)
      .|.argument("b").withCardinality(100)
      .argument("a").withCardinality(1)
      .build()

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, VolcanoModelExecution) should equal(
      costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, PushBatchedExecution)
    )
  }

  test("cartesian product with many row from the left is cheaper in PushBatchedExecution, big chunk size") {
    val cardinality = 1500.0
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(cardinality * cardinality)
      .|.argument("b").withCardinality(cardinality)
      .argument("a").withCardinality(cardinality)
      .build()

    val argCost = Cardinality(cardinality) * DEFAULT_COST_PER_ROW

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, VolcanoModelExecution) should equal(argCost + argCost * cardinality)
    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, PushBatchedExecution) should equal(argCost + argCost * Math.ceil(cardinality / BIG_CHUNK_SIZE))
  }

  test("cartesian product with many row from the left but with provided order is equally expensive in both execution models") {
    val builder = new LogicalPlanBuilder(wholePlan = false)
    val plan = builder
      .cartesianProduct().withCardinality(10000).withProvidedOrder(ProvidedOrder.asc(varFor("a")))
      .|.argument("b").withCardinality(100)
      .argument("a").withCardinality(100).withProvidedOrder(ProvidedOrder.asc(varFor("a")))
      .build()

    costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, VolcanoModelExecution) should equal(
      costFor(plan, QueryGraphSolverInput.empty, builder.getSemanticTable, builder.cardinalities, builder.providedOrders, PushBatchedExecution)
    )
  }
}
