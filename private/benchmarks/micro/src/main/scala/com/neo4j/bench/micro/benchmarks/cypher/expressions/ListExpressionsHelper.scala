/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.expressions

import com.neo4j.bench.micro.benchmarks.cypher.TestSetup
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

trait ListExpressionsHelper {
  val ROWS: Int = 10000
  val VALUES: ListValue = VirtualValues.list((1 to ROWS).map(Values.intValue).toArray: _*)

  def listExpressionPlan(planContext: PlanContext,
                         listParameter: Parameter,
                         listExpression: Expression): TestSetup = {
    val listType = symbols.CTList(symbols.CTAny)
    val unwindListParameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), unwindVariable.name, unwindListParameter)(IdGen)
    val projection = plans.Projection(leaf, Map("result" -> listExpression))(IdGen)
    val resultColumns = List("result")
    val produceResult = plans.ProduceResult(projection, columns = resultColumns)(IdGen)
    TestSetup(produceResult, SemanticTable(), resultColumns)
  }

  def getParams(size: Int): MapValue = {
    // shuffle list to make predicate result predictable for branch predictor
    val shuffledList: Seq[Int] = scala.util.Random.shuffle((1 to size).toList)
    val list = VirtualValues.list(shuffledList.map(Values.intValue).toArray: _*)
    VirtualValues.map(
      Array("x", "list"),
      Array(list, VALUES))
  }
}
