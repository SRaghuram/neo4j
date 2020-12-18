/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselTestHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

class MorselIndexedSeqCursorTest extends CypherFunSuite with MorselTestHelper {
  test("MorselIndexedSeq Cursor should iterate single morsel") {
    iterationTest(Seq(Seq(1L, 2L, 3L)))
  }

  test("MorselIndexedSeq Cursor should iterate multiple morsels") {
    iterationTest(Seq(Seq(1L), Seq(2L, 3L, 4L), Seq(5L)))
  }

  test("MorselIndexedSeq Cursor should iterate multiple morsels beginning with empty morsel") {
    iterationTest(Seq(Seq(), Seq(), Seq(1L)))
  }

  test("MorselIndexedSeq Cursor should iterate multiple morsels ending with empty morsel") {
    iterationTest(Seq(Seq(1L), Seq(2L, 3L), Seq(), Seq(4L), Seq(), Seq()))
  }

  test("MorselIndexedSeq Cursor should iterate empty morsels") {
    iterationTest(Seq(Seq(), Seq(), Seq()))
  }

  test("MorselIndexedSeq Cursor should iterate randomized") {
    Random.setSeed(1337L)
    (0 until 10).foreach { _ =>
      val n = (Math.abs(Random.nextInt()) % 10) + 2
      var v = 0L
      val data: Seq[Seq[Long]] = (0 until n).map(_ => (0 until (Math.abs(Random.nextInt()) % 4)).map { _ =>
        v += 1L
        v
      })
      iterationTest(data)
    }
  }

  test("MorselIndexedSeq Cursor should support initialising on first row") {
    val cursor = createMorselIndexedSeqCursor(Seq(Seq(42, 43))).readCursor(true)
    cursor.hasNext shouldBe true
    cursor.onValidRow() shouldBe true
    cursor.getLongAt(0) shouldBe 42
    cursor.next() shouldBe true
    cursor.next() shouldBe false
  }

  test("MorselIndexedSeq Cursor should support initialising on first row with empty morsel") {
    val cursor = createMorselIndexedSeqCursor(Seq(Seq(), Seq(42))).readCursor(true)
    cursor.hasNext shouldBe false
    cursor.onValidRow() shouldBe true
    cursor.getLongAt(0) shouldBe 42
    cursor.next() shouldBe false
  }

  test("MorselIndexedSeq Cursor should support initialising on first row with multiple empty morsels") {
    val cursor = createMorselIndexedSeqCursor(Seq(Seq(), Seq(), Seq(42))).readCursor(true)
    cursor.hasNext shouldBe false
    cursor.onValidRow() shouldBe true
    cursor.getLongAt(0) shouldBe 42
    cursor.next() shouldBe false
  }

  test("MorselIndexedSeq Cursor should support initialising on first row with empty data") {
    val cursor = createMorselIndexedSeqCursor(Seq()).readCursor(true)
    cursor.hasNext shouldBe false
    cursor.onValidRow() shouldBe false
    cursor.next() shouldBe false
  }

  test("MorselIndexedSeq Cursor should handle morsel data without morsels") {
    val cursor = Morsels(IndexedSeq()).readCursor()
    cursor.hasNext shouldBe false
    cursor.onValidRow() shouldBe false
    cursor.next() shouldBe false
  }

  private def iterationTest(data: Seq[Seq[Long]]): Unit = {
    val morselIndexedSeq = createMorselIndexedSeqCursor(data)
    val expectedData = data.flatten.toIndexedSeq

    // Start
    val cursor = morselIndexedSeq.readCursor()
    val positions: Array[Any] = new Array(expectedData.size)

    cursor.hasNext shouldBe (if (expectedData.isEmpty) false else true)
    cursor.onValidRow() shouldBe false

    // Test start position
    val startPosition = cursor.position
    cursor.setPosition(startPosition)
    cursor.onValidRow() shouldBe false

    var i = 0
    while (cursor.hasNext) {
      cursor.next() shouldBe true
      cursor.onValidRow() shouldBe true
      cursor.getLongAt(0) shouldBe expectedData(i)

      // Test position
      val position = cursor.position
      cursor.setPosition(position)
      cursor.onValidRow() shouldBe true
      cursor.getLongAt(0) shouldBe expectedData(i)
      positions(i) = position // Save for later

      i += 1
    }
    i shouldBe expectedData.size
    cursor.next() shouldBe false
    cursor.onValidRow() shouldBe false
    cursor.hasNext shouldBe false

    // Test end position
    val endPosition = cursor.position
    cursor.setPosition(endPosition)
    cursor.next() shouldBe false
    cursor.onValidRow() shouldBe false
    cursor.hasNext shouldBe false

    // Restart iteration
    cursor.setPosition(startPosition)
    cursor.hasNext shouldBe (if (expectedData.isEmpty) false else true)
    cursor.onValidRow() shouldBe false

    i = 0
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

    // Test all saved positions
    i = 0
    while (i < expectedData.size) {
      cursor.setPosition(positions(i).asInstanceOf[cursor.Position])
      cursor.getLongAt(0) shouldBe expectedData(i)
      i += 1
    }
  }

  private def createMorselIndexedSeqCursor(data: Seq[Seq[Long]]) = {
    val morsels = data.map(data => longMorsel(longsPerRow = 1)(data:_*))
    Morsels(morsels.toIndexedSeq)
  }

  case class Morsels(morsels: IndexedSeq[Morsel]) extends MorselIndexedSeq
}

