/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.codegen.api.CodeGeneration.ByteCodeGeneration
import org.neo4j.codegen.api.CodeGeneration.CodeSaver
import org.neo4j.codegen.api.CodeGeneration.SourceCodeGeneration
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.NestedPlanArgumentConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.compiled.expressions.CachingExpressionCompilerCache
import org.neo4j.cypher.internal.runtime.compiled.expressions.CachingExpressionCompilerTracer
import org.neo4j.cypher.internal.runtime.compiled.expressions.CompiledExpressionContext
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.logging.BufferingLog

class CompiledExpressionConverterTest extends CypherFunSuite with AstConstructionTestSupport {

  val compiledExpressionsContext = CompiledExpressionContext(
    new CachingExpressionCompilerCache(TestExecutorCaffeineCacheFactory),
    CachingExpressionCompilerTracer.NONE)

  test("should log unexpected errors") {
    // Given
    val physicalPlan = PhysicalPlan(null,
      0,
      new SlotConfigurations,
      new ArgumentSizes,
      new ApplyPlans,
      new NestedPlanArgumentConfigurations,
      new AvailableExpressionVariables,
      ParameterMapping.empty)


    val logByteCode = new BufferingLog
    val converterByteCode = new CompiledExpressionConverter(logByteCode,
      physicalPlan,
      TokenContext.EMPTY,
      readOnly = false,
      codeGenerationMode = ByteCodeGeneration(new CodeSaver(false, false)),
      neverFail = true,
      compiledExpressionsContext = compiledExpressionsContext)
    val logSourceCode = new BufferingLog
    val converterSourceCode = new CompiledExpressionConverter(logSourceCode,
      physicalPlan,
      TokenContext.EMPTY,
      readOnly = false,
      codeGenerationMode = SourceCodeGeneration(new CodeSaver(false, false)),
      neverFail = true,
      compiledExpressionsContext = compiledExpressionsContext)

    // When
    //There is a limit of 65535 on the length of a String literal, so by exceeding that limit
    //we trigger a compilation error
    val e = add(literalString("*" * (65535 + 1)), literalString("*"))

    // Then
    converterByteCode.toCommandExpression(Id.INVALID_ID, e, mock[ExpressionConverters]) should equal(None)
    logByteCode.toString should include(s"Failed to compile expression: $e")
    converterSourceCode.toCommandExpression(Id.INVALID_ID, e, mock[ExpressionConverters]) should equal(None)
    logSourceCode.toString should include(s"Failed to compile expression: $e")
  }
}
