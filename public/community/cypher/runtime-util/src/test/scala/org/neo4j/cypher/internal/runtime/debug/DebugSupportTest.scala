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
package org.neo4j.cypher.internal.runtime.debug

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class DebugSupportTest extends CypherFunSuite {

  test("I think you forgot to disable DebugSupport after debugging...") {
    DebugSupport.PHYSICAL_PLANNING.enabled shouldBe false

    DebugSupport.TIMELINE.enabled shouldBe false

    DebugSupport.WORKERS.enabled shouldBe false
    DebugSupport.QUERIES.enabled shouldBe false
    DebugSupport.TRACKER.enabled shouldBe false
    DebugSupport.LOCKS.enabled shouldBe false
    DebugSupport.ERROR_HANDLING.enabled shouldBe false
    DebugSupport.CURSORS.enabled shouldBe false
    DebugSupport.BUFFERS.enabled shouldBe false
    DebugSupport.SCHEDULING.enabled shouldBe false
    DebugSupport.ASM.enabled shouldBe false

    DebugSupport.DEBUG_PIPELINES shouldBe false

    DebugSupport.DEBUG_GENERATED_SOURCE_CODE shouldBe false
  }
}
