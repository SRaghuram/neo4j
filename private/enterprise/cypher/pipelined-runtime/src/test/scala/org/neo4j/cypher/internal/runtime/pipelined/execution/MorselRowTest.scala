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

class MorselRowTest extends CypherFunSuite {

  test("should compute estimateHeapUsage on a view") {
    // GIVEN
    val morselSize = 5
    val slots =
      SlotConfiguration.empty
        .newLong("n", nullable = false, CTNode)
        .newReference("x", nullable = true, CTAny)

    // WHEN
    val ctx1 = MorselFactory.allocate(slots, morselSize).view(0, 1)
    val ctx2 = MorselFactory.allocate(slots, morselSize).view(1, morselSize)
    val ctx3 = MorselFactory.allocate(slots, morselSize).view(0, 3)
    val ctx4 = MorselFactory.allocate(slots, morselSize).view(3, morselSize)
    val morsel = MorselFactory.allocate(slots, morselSize)

    // THEN
    ctx1.estimatedHeapUsage should be > 0L
    ctx2.estimatedHeapUsage should be > 0L
    ctx3.estimatedHeapUsage should be > 0L
    ctx4.estimatedHeapUsage should be > 0L
    ctx1.estimatedHeapUsage + ctx2.estimatedHeapUsage shouldBe ctx3.estimatedHeapUsage + ctx4.estimatedHeapUsage
  }
}
