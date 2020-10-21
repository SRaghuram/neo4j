/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astFunctionInvocation
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.Plans.cypherTypeFor
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.util.symbols

object AggregationHelper {
  def setupAggregation(propertyType: String, aggregatingFunction: String, additionalAggregationParameters: Expression*): TestSetup = {
    val listElementType = cypherTypeFor(propertyType)
    val listType = symbols.CTList(listElementType)
    val parameter = astParameter("list", listType)
    val unwindVariable = astVariable("value")
    val leaf = plans.UnwindCollection(plans.Argument()(IdGen), "value", parameter)(IdGen)
    val groupingExpressions = Map[String, Expression]()
    val aggregationParameters = astVariable("value") +: additionalAggregationParameters
    val aggregationExpressions = Map("result" -> astFunctionInvocation(aggregatingFunction, aggregationParameters:_*))
    val aggregation = plans.Aggregation(leaf, groupingExpressions, aggregationExpressions)(IdGen)
    val resultColumns = List("result")
    val produceResults = plans.ProduceResult(aggregation, resultColumns)(IdGen)

    val table = SemanticTable(types = ASTAnnotationMap.empty.updated(unwindVariable, ExpressionTypeInfo(listElementType.invariant, None)))

    TestSetup(produceResults, table, resultColumns)
  }
}
