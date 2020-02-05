/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MorselExecutionContextTest extends CypherFunSuite {

  test("should ignore null refs on estimateHeapUsage") {
    // GIVEN
    val morselSize = 3
    val slots =
      SlotConfiguration.empty
        .newLong("n", nullable = false, CTNode)
        .newReference("x", nullable = true, CTAny)

    // WHEN
    val ctx = MorselFactory.allocate(slots, morselSize)

    // THEN
    val expectedSizeOfLongs = morselSize * 8L
    val expectedSizeOfRefs = 0L
    ctx.estimatedHeapUsage should be(expectedSizeOfLongs + expectedSizeOfRefs)
  }

  test("should compute estimateHeapUsage on a view") {
    // GIVEN
    val morselSize = 3
    val slots =
      SlotConfiguration.empty
        .newLong("n", nullable = false, CTNode)
        .newReference("x", nullable = true, CTAny)

    // WHEN
    val morsel = Morsel.create(slots, morselSize)
    val ctx = new MorselExecutionContext(morsel, slots, morselSize, 0, 0, morselSize).view(1, morselSize )

    // THEN
    val expectedSizeOfLongs = morselSize * 8L
    val expectedSizeOfRefs = 0L
    ctx.estimatedHeapUsage should be(expectedSizeOfLongs + expectedSizeOfRefs)
  }
}
