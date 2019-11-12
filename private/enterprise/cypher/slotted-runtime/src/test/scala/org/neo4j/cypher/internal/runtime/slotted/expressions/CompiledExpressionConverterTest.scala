/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.PhysicalPlanningAttributes.{ArgumentSizes, NestedPlanArgumentConfigurations, SlotConfigurations}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.logging.BufferingLog
import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions.{Add, StringLiteral}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class CompiledExpressionConverterTest extends CypherFunSuite with AstConstructionTestSupport {

  test("should log unexpected errors") {
    // Given
    val log = new BufferingLog
    val converter = new CompiledExpressionConverter(log, PhysicalPlan(new SlotConfigurations, new ArgumentSizes, new NestedPlanArgumentConfigurations), TokenContext.EMPTY)

    // When
    //There is a limit of 65535 on the length of a String literal, so by exceeding that limit
    //we trigger a compilation error
    val e = Add(StringLiteral("*" * (65535 + 1))(pos), StringLiteral("*")(pos))(pos)

    // Then
    converter.toCommandExpression(Id.INVALID_ID, e, mock[ExpressionConverters]) should equal(None)
    log.toString should startWith(s"Failed to compile expression: $e")
  }
}
