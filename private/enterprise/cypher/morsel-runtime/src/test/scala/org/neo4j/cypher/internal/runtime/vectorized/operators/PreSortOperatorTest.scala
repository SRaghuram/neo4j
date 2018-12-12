/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.{LongSlot, RefSlot}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.parallel.{WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized.{EmptyQueryState, Morsel, MorselExecutionContext, QueryResources}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.intValue

class PreSortOperatorTest extends CypherFunSuite {

  private val resources = new QueryResources(mock[CursorFactory])

  private val workId: WorkIdentity = WorkIdentityImpl(42, "Work Identity Description")

  test("sort a morsel with a single long column") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val sortOperator = new PreSortOperator(workId, columnOrdering)

    val longs = Array[Long](9, 8, 7, 6, 5, 4, 3, 2, 1)
    val data = new Morsel(longs, Array[AnyValue]())

    sortOperator.operate(MorselExecutionContext(data, numberOfLongs, numberOfReferences, longs.length), null, null, resources)

    data.longs should equal(Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9))
  }

  test("sort a morsel with a one long slot and one ref slot, order by ref") {
    val numberOfLongs = 1
    val numberOfReferences = 1
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = RefSlot(0, nullable = false, CTNumber)
    val columnOrdering = Seq(Ascending(slot2))
    val sortOperator = new PreSortOperator(workId, columnOrdering)

    val longs = Array[Long](
      6, 5, 4,
      9, 8, 7,
      3, 2, 1)
    val refs = Array[AnyValue](
      intValue(6), intValue(5), intValue(4),
      intValue(9), intValue(8), intValue(7),
      intValue(3), intValue(2), intValue(1))
    val data = new Morsel(longs, refs)

    sortOperator.operate(MorselExecutionContext(data, numberOfLongs, numberOfReferences, longs.length), null, null, resources)

    data.longs should equal(Array[Long](
      1, 2, 3,
      4, 5, 6,
      7, 8, 9))
    data.refs should equal(Array[AnyValue](
      intValue(1), intValue(2), intValue(3),
      intValue(4), intValue(5), intValue(6),
      intValue(7), intValue(8), intValue(9))
    )

  }

  test("sort a morsel with a two long columns by one") {
    val numberOfLongs = 2
    val numberOfReferences = 0
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = LongSlot(1, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1))
    val sortOperator = new PreSortOperator(workId, columnOrdering)

    val longs = Array[Long](
      9, 0,
      8, 1,
      7, 2,
      6, 3,
      5, 4,
      4, 5,
      3, 6,
      2, 7,
      1, 8)
    val rows = longs.length / 2 // Since we have two columns per row
    val data = new Morsel(longs, Array[AnyValue]())

    sortOperator.operate(MorselExecutionContext(data, numberOfLongs, numberOfReferences, rows), null, null, resources)

    data.longs should equal(Array[Long](
      1, 8,
      2, 7,
      3, 6,
      4, 5,
      5, 4,
      6, 3,
      7, 2,
      8, 1,
      9, 0)
    )
  }

  test("sort a morsel with no valid data") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val sortOperator = new PreSortOperator(workId, columnOrdering)

    val longs = new Array[Long](10)
    val data = new Morsel(longs, Array[AnyValue]())

    sortOperator.operate(MorselExecutionContext(data, numberOfLongs, numberOfReferences, 0), null, null, resources)

    data.longs should equal(Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
  }

  test("sort a morsel with empty array") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val sortOperator = new PreSortOperator(workId, columnOrdering)

    val data = new Morsel(Array.empty, Array[AnyValue]())

    sortOperator.operate(MorselExecutionContext(data, numberOfLongs, numberOfReferences, 0), null, null, resources)

    data.longs shouldBe empty
  }

  test("top on a morsel with a single long column") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val topOperator = new PreSortOperator(workId, columnOrdering, Some(Literal(3)))

    val longs = Array[Long](9, 8, 7, 6, 5, 4, 3, 2, 1)
    val data = new Morsel(longs, Array[AnyValue]())

    val outputRow = MorselExecutionContext(data, numberOfLongs, numberOfReferences, longs.length)
    topOperator.operate(outputRow, null, EmptyQueryState(), resources)

    data.longs.take(3) should equal(Array[Long](1, 2, 3))
    outputRow.getValidRows shouldBe 3
  }

  test("top with n > morselSize on a morsel with a single long column") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val topOperator = new PreSortOperator(workId, columnOrdering, Some(Literal(20)))

    val longs = Array[Long](9, 8, 7, 6, 5, 4, 3, 2, 1)
    val data = new Morsel(longs, Array[AnyValue]())

    val outputRow = MorselExecutionContext(data, numberOfLongs, numberOfReferences, longs.length)
    topOperator.operate(outputRow, null, EmptyQueryState(), resources)

    data.longs should equal(Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9))
    outputRow.getValidRows shouldBe longs.length
  }

  test("top on a morsel with a one long slot and one ref slot, order by ref") {
    val numberOfLongs = 1
    val numberOfReferences = 1
    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = RefSlot(0, nullable = false, CTNumber)
    val columnOrdering = Seq(Ascending(slot2))
    val topOperator = new PreSortOperator(workId, columnOrdering, Some(Literal(3)))

    val longs = Array[Long](
      6, 5, 4,
      9, 8, 7,
      3, 2, 1)
    val refs = Array[AnyValue](
      intValue(6), intValue(5), intValue(4),
      intValue(9), intValue(8), intValue(7),
      intValue(3), intValue(2), intValue(1))
    val data = new Morsel(longs, refs)

    val outputRow = MorselExecutionContext(data, numberOfLongs, numberOfReferences, longs.length)
    topOperator.operate(outputRow, null, EmptyQueryState(), resources)

    data.longs.take(3) should equal(Array[Long](
      1, 2, 3))
    data.refs.take(3) should equal(Array[AnyValue](
      intValue(1), intValue(2), intValue(3))
    )
    outputRow.getValidRows shouldBe 3
  }

  test("top on a morsel with no valid data") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val topOperator = new PreSortOperator(workId, columnOrdering, Some(Literal(3)))

    val longs = new Array[Long](10)
    val data = new Morsel(longs, Array[AnyValue]())

    val outputRow = MorselExecutionContext(data, numberOfLongs, numberOfReferences, 0)
    topOperator.operate(outputRow, null, EmptyQueryState(), resources)

    data.longs should equal(Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    outputRow.getValidRows shouldBe 0
  }

  test("top on a morsel with empty array") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))
    val topOperator = new PreSortOperator(workId, columnOrdering, Some(Literal(3)))

    val data = new Morsel(Array.empty, Array[AnyValue]())

    val outputRow = MorselExecutionContext(data, numberOfLongs, numberOfReferences, 0)
    topOperator.operate(outputRow, null, EmptyQueryState(), resources)

    data.longs shouldBe empty
    outputRow.getValidRows shouldBe 0
  }
}
