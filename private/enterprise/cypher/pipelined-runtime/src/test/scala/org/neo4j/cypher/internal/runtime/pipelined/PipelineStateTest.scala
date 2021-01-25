/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.runtime.pipelined.PipelineState.MIN_MORSEL_SIZE
import org.neo4j.cypher.internal.runtime.pipelined.PipelineState.computeMorselSize
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PipelineStateTest extends CypherFunSuite {

  test("should pick remaining rows as morsel size when in valid range") {
    //given
    val remainingRows = MIN_MORSEL_SIZE + 1
    val morselSize = 2 * MIN_MORSEL_SIZE

    //then
    computeMorselSize(remainingRows, morselSize) shouldBe remainingRows
  }

  test("should never use a morsel size below threshold") {
    //given
    val remainingRows = MIN_MORSEL_SIZE - 1
    val configuredMorselSize = 2 * MIN_MORSEL_SIZE

    //then
    computeMorselSize(remainingRows, configuredMorselSize) shouldBe MIN_MORSEL_SIZE
  }

  test("should never use a morsel size bigger than configured value") {
    //given
    val remainingRows = 3 * MIN_MORSEL_SIZE
    val configuredMorselSize = 2 * MIN_MORSEL_SIZE

    //then
    computeMorselSize(remainingRows, configuredMorselSize) shouldBe configuredMorselSize
  }

  test("should use a smaller morsel size if explicitly configured") {
    //given
    val remainingRows = Long.MaxValue
    val configuredMorselSize = MIN_MORSEL_SIZE - 1

    //then
    computeMorselSize(remainingRows, configuredMorselSize) shouldBe configuredMorselSize
  }
}
