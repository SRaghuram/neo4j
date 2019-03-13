/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.{ApplyPlans, ArgumentSizes, NestedPlanArgumentConfigurations, SlotConfigurations}
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.AvailableExpressionVariables
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.logging.BufferingLog

class CompiledExpressionConverterTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should log unexpected errors") {
    // Given
    val log = new BufferingLog
    val physicalPlan = PhysicalPlan(null,
                                    0,
                                    new SlotConfigurations,
                                    new ArgumentSizes,
                                    new ApplyPlans,
                                    new NestedPlanArgumentConfigurations,
                                    new AvailableExpressionVariables,
                                    Map.empty)

    val converter = new CompiledExpressionConverter(log, physicalPlan, TokenContext.EMPTY, neverFail = true)

    // When
    //There is a limit of 65535 on the length of a String literal, so by exceeding that limit
    //we trigger a compilation error
    val e = add(literalString("*" * (65535 + 1)), literalString("*"))

    // Then
    converter.toCommandExpression(Id.INVALID_ID, e, mock[ExpressionConverters]) should equal(None)
    log.toString should startWith(s"Failed to compile expression: $e")
  }
}
