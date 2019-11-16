/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTNode}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class PipelinedExecutionContextTest extends CypherFunSuite {

  test("should ignore null refs on estimateHeapUsage") {
    // GIVEN
    val morselSize = 3
    val slots =
      SlotConfiguration.empty
        .newLong("n", nullable = false, CTNode)
        .newReference("x", nullable = true, CTAny)

    // WHEN
    val morsel = Morsel.create(slots, morselSize)
    val ctx = new MorselExecutionContext(morsel, slots, morselSize)

    // THEN
    val expectedSizeOfLongs = morselSize * 8L
    val expectedSizeOfRefs = 0L
    ctx.estimatedHeapUsage should be(expectedSizeOfLongs + expectedSizeOfRefs)
  }
}
