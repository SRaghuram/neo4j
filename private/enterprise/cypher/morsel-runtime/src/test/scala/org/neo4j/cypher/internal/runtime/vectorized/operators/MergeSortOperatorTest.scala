/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.LongSlot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.parallel.{WorkIdentity, WorkIdentityImpl}
import org.neo4j.cypher.internal.runtime.slotted.pipes.Ascending
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.v4_0.util.symbols.CTNode
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.values.AnyValue

class MergeSortOperatorTest extends CypherFunSuite {

  private val resources = new QueryResources(mock[CursorFactory])

  private val workId: WorkIdentity = WorkIdentityImpl(42, "Work Identity Description")

  test("sort a single morsel") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue]())
    val out = new Morsel(new Array[Long](longs.length), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering)
    val task = operator.init(null, null, Array(MorselExecutionContext(in, numberOfLongs, numberOfReferences, longs.length)), resources)
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, longs.length), null, null, resources)
    task.canContinue should be(false)
    out.longs should equal(longs)
  }

  test("top on a single morsel") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue]())
    val out = new Morsel(new Array[Long](longs.length), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering, Some(Literal(3)))
    val inputRow = MorselExecutionContext(in, numberOfLongs, numberOfReferences, longs.length)
    val task = operator.init(null, EmptyQueryState(), Array(inputRow), resources)
    val outputRow = MorselExecutionContext(out, numberOfLongs, numberOfReferences, longs.length)
    task.operate(outputRow, null, EmptyQueryState(), resources)
    task.canContinue should be(false)
    out.longs.take(3) should equal(Array[Long](1, 2, 3))
    outputRow.getValidRows shouldBe 3
  }

  test("sort two morsels") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val long1 = Array[Long](1, 2, 3, 4, 6, 7, 8, 9)
    val long2 = Array[Long](5, 7, 9, 14, 86, 92)
    val in1 = new Morsel(long1, Array[AnyValue]())
    val in2 = new Morsel(long2, Array[AnyValue]())
    val out = new Morsel(new Array[Long](5), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering)

    val task = operator.init(null, null, Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences, long1.length),
                                                                  MorselExecutionContext(in2, numberOfLongs, numberOfReferences, long2.length)), resources)
    val outputRow1 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, 5)
    task.operate(outputRow1, null, null, resources)
    task.canContinue should be(true)
    out.longs should equal(Array(1, 2, 3, 4, 5))
    outputRow1.getValidRows shouldBe 5

    val outputRow2 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, 5)
    task.operate(outputRow2, null, null, resources)
    task.canContinue should be(true)
    out.longs should equal(Array(6, 7, 7, 8, 9))
    outputRow2.getValidRows shouldBe 5

    val outputRow3 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, 5)
    task.operate(outputRow3, null, null, resources)
    task.canContinue should be(false)
    out.longs.take(4) should equal(Array(9, 14, 86,92))
    outputRow3.getValidRows shouldBe 4
  }

  test("sort two morsels with additional column") {
    val numberOfLongs = 2
    val numberOfReferences = 0

    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = LongSlot(1, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1))

    val long1 = Array[Long](
      1, 101,
      3, 103,
      5, 105,
      7, 107,
      9, 109
    )
    val long2 = Array[Long](
      2, 102,
      4, 104,
      6, 106,
      8, 108,
      10, 110
    )
    val length1 = long1.length / numberOfLongs
    val length2 = long2.length / numberOfLongs
    val in1 = new Morsel(long1, Array[AnyValue]())
    val in2 = new Morsel(long2, Array[AnyValue]())

    val outputRowsPerMorsel = 4
    val out = new Morsel(new Array[Long](numberOfLongs * outputRowsPerMorsel), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering)

    val task = operator.init(null, null,
      Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences, length1),
            MorselExecutionContext(in2, numberOfLongs, numberOfReferences, length2)), resources)
    val outputRow1 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, outputRowsPerMorsel)
    task.operate(outputRow1, null, null, resources)
    task.canContinue should be(true)
    out.longs should equal(Array(1, 101, 2, 102, 3, 103, 4, 104))
    outputRow1.getValidRows shouldBe outputRowsPerMorsel

    val outputRow2 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, outputRowsPerMorsel)
    task.operate(outputRow2, null, null, resources)
    task.canContinue should be(true)
    out.longs should equal(Array(5, 105, 6, 106, 7, 107, 8, 108))
    outputRow2.getValidRows shouldBe outputRowsPerMorsel

    val outputRow3 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, outputRowsPerMorsel)
    task.operate(outputRow3, null, null, resources)
    task.canContinue should be(false)
    out.longs.take(4) should equal(Array(9, 109, 10, 110))
    outputRow3.getValidRows shouldBe 2
  }

  test("sort two morsels by two columns") {
    val numberOfLongs = 2
    val numberOfReferences = 0

    val slot1 = LongSlot(0, nullable = false, CTNode)
    val slot2 = LongSlot(1, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot1), Ascending(slot2))

    val long1 = Array[Long](
      1, 101,
      1, 102,
      2, 202,
      5, 501,
      7, 701
    )
    val long2 = Array[Long](
      1, 103,
      2, 201,
      3, 301,
      5, 502,
      5, 503
    )
    val length1 = long1.length / numberOfLongs
    val length2 = long2.length / numberOfLongs
    val in1 = new Morsel(long1, Array[AnyValue]())
    val in2 = new Morsel(long2, Array[AnyValue]())

    val outputRowsPerMorsel = 4
    val out = new Morsel(new Array[Long](numberOfLongs * outputRowsPerMorsel), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering)

    val task = operator.init(null, null,
      Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences, length1),
            MorselExecutionContext(in2, numberOfLongs, numberOfReferences, length2)), resources)
    val outputRow1 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, outputRowsPerMorsel)
    task.operate(outputRow1, null, null, resources)
    task.canContinue should be(true)
    out.longs should equal(Array(1, 101, 1, 102, 1, 103, 2, 201))
    outputRow1.getValidRows shouldBe outputRowsPerMorsel

    val outputRow2 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, outputRowsPerMorsel)
    task.operate(outputRow2, null, null, resources)
    task.canContinue should be(true)
    out.longs should equal(Array(2, 202, 3, 301, 5, 501, 5, 502))
    outputRow2.getValidRows shouldBe outputRowsPerMorsel

    val outputRow3 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, outputRowsPerMorsel)
    task.operate(outputRow3, null, null, resources)
    task.canContinue should be(false)
    out.longs.take(4) should equal(Array(5, 503, 7, 701))
    outputRow3.getValidRows shouldBe 2
  }

  test("top on two morsels") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val long1 = Array[Long](1, 2, 3, 4, 6, 7, 8, 9)
    val long2 = Array[Long](5, 7, 9, 14, 86, 92)
    val length1 = long1.length
    val length2 = long2.length
    val in1 = new Morsel(long1, Array[AnyValue]())
    val in2 = new Morsel(long2, Array[AnyValue]())
    val out = new Morsel(new Array[Long](5), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering, Some(Literal(9)))

    val task = operator.init(null, EmptyQueryState(),
      Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences, length1),
            MorselExecutionContext(in2, numberOfLongs, numberOfReferences, length2)), resources)
    val outputRow = MorselExecutionContext(out, numberOfLongs, numberOfReferences, 5)
    task.operate(outputRow, null, EmptyQueryState(), resources)
    task.canContinue should be(true)
    out.longs should equal(Array(1, 2, 3, 4, 5))
    outputRow.getValidRows shouldBe 5

    val outputRow2 = MorselExecutionContext(out, numberOfLongs, numberOfReferences, 5)
    task.operate(outputRow2, null, EmptyQueryState(), resources)
    task.canContinue should be(false)
    out.longs.take(4) should equal(Array(6, 7, 7, 8))
    outputRow2.getValidRows shouldBe 4
  }

  test("sort two morsels with one empty array") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val long1 = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val long2 = Array.empty[Long]
    val in1 = new Morsel(long1, Array[AnyValue]())
    val in2 = new Morsel(long2, Array[AnyValue]())
    val out = new Morsel(new Array[Long](9), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering)
    val task = operator.init(null, null,
      Array(MorselExecutionContext(in1, numberOfLongs, numberOfReferences, long1.length),
            MorselExecutionContext(in2, numberOfLongs, numberOfReferences, long2.length)), resources)
    task.operate(MorselExecutionContext(out, numberOfLongs, numberOfReferences, 9), null, null, resources)
    task.canContinue should be(false)
    out.longs should equal(long1)
  }

  test("sort with too many output slots") {
    val numberOfLongs = 1
    val numberOfReferences = 0
    val slot = LongSlot(0, nullable = false, CTNode)
    val columnOrdering = Seq(Ascending(slot))

    val longs = Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
    val in = new Morsel(longs, Array[AnyValue]())
    val out = new Morsel(new Array[Long](longs.length + 5), Array[AnyValue]())

    val operator = new MergeSortOperator(workId, columnOrdering)
    val task = operator.init(null, null, Array(MorselExecutionContext(in, numberOfLongs, numberOfReferences, longs.length)), resources)
    val outputRow = MorselExecutionContext(out, numberOfLongs, numberOfReferences, longs.length + 5)
    task.operate(outputRow, null, null, resources)
    task.canContinue should be(false)
    out.longs should equal(Array[Long](1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0))
    outputRow.getValidRows should equal(longs.length)
  }

}
