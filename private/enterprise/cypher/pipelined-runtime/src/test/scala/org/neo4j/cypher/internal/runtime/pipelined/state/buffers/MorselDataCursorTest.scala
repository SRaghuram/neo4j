/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MorselDataCursorTest extends CypherFunSuite with MorselTestHelper {
  test("MorselData Cursor should iterate multiple morsels") {
    iterationTest(Seq(Seq(2L), Seq(1L, 2L, 3L), Seq(10L)))
  }

  test("MorselData Cursor should iterate multiple morsels beginning with empty morsel") {
    iterationTest(Seq(Seq(), Seq(), Seq(2L)))
  }

  test("MorselData Cursor should iterate empty morsels") {
    iterationTest(Seq(Seq(), Seq(), Seq()))
  }

  test("MorselData Cursor should support initialising on first row") {
    val cursor = createMorselData(Seq(Seq(), Seq(42))).readCursor(true)
    cursor.hasNext shouldBe false
    cursor.onValidRow() shouldBe true
    cursor.getLongAt(0) shouldBe 42
    cursor.next() shouldBe false
  }

  test("MorselData Cursor should support initialising on first row with empty data") {
    val cursor = createMorselData(Seq()).readCursor(true)
    cursor.hasNext shouldBe false
    cursor.onValidRow() shouldBe false
    cursor.next() shouldBe false
  }

  test("MorselData Cursor should handle morsel data without morsels") {
    val cursor = createMorselDataWithMorsels(IndexedSeq()).readCursor()
    cursor.hasNext shouldBe false
    cursor.onValidRow() shouldBe false
    cursor.next() shouldBe false
  }

  private def iterationTest(data: Seq[Seq[Long]]): Unit = {
    val morselData = createMorselData(data)
    val expectedData = data.flatten.toIndexedSeq
    val cursor = morselData.readCursor()
    cursor.hasNext shouldBe (if (expectedData.isEmpty) false else true)
    cursor.onValidRow() shouldBe false
    var i = 0
    while (cursor.hasNext) {
      cursor.next() shouldBe true
      cursor.onValidRow() shouldBe true
      cursor.getLongAt(0) shouldBe expectedData(i)
      i += 1
    }
    i shouldBe expectedData.size
    cursor.next() shouldBe false
    cursor.onValidRow() shouldBe false
    cursor.hasNext shouldBe false
  }

  private def createMorselDataWithMorsels(morsels: IndexedSeq[Morsel]) = MorselData(morsels, null, null, null)

  private def createMorselData(data: Seq[Seq[Long]]) = {
    val morsels = data.map(data => longMorsel(longsPerRow = 1)(data:_*))
    createMorselDataWithMorsels(morsels.toIndexedSeq)
  }
}
