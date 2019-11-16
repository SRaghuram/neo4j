/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.pipelined.execution._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

abstract class MorselUnitTest extends CypherFunSuite {
  protected val resources: QueryResources = mock[QueryResources](RETURNS_DEEP_STUBS)

  private def answer[T](a: InvocationOnMock => T): Answer[T] = {
    invocationOnMock: InvocationOnMock => a(invocationOnMock)
  }

  class Input {
    private val _longs = Array.newBuilder[Long]
    private val _refs = Array.newBuilder[AnyValue]
    private var _longSlots: Option[Int] = None
    private var _refSlots: Option[Int] = None
    private var _rows: Int = 0

    def longs: Array[Long] = _longs.result()
    def refs: Array[AnyValue] = _refs.result()
    def longSlots: Int = _longSlots.getOrElse(0)
    def refSlots: Int = _refSlots.getOrElse(0)
    def rows: Int = _rows

    private def setLongSlots(longSlots: Int): Unit = {
      _longSlots = _longSlots match {
        case None => Some(longSlots)
        case Some(`longSlots`) => Some(longSlots)
        case Some(otherNumber) => throw new IllegalArgumentException(s"Number of longs does not match: $longSlots vs $otherNumber")
      }
    }
    private def setRefSlots(refSlots: Int): Unit = {
      _refSlots = _refSlots match {
        case None => Some(refSlots)
        case Some(`refSlots`) => Some(refSlots)
        case Some(otherNumber) => throw new IllegalArgumentException(s"Number of refs does not match: $refSlots vs $otherNumber")
      }
    }

    def withNoRows(longSlots: Int, refSlots: Int): this.type = {
      setLongSlots(longSlots)
      setRefSlots(refSlots)
      this
    }

    def addRow(): this.type = {
      _rows += 1
      setLongSlots(0)
      setRefSlots(0)
      this
    }

    def addRow(refs:Refs): this.type = {
      _rows += 1
      setLongSlots(0)
      setRefSlots(refs.refs.size)
      _refs ++= refs.refs
      this
    }

    def addRow(longs:Longs): this.type = {
      _rows += 1
      setLongSlots(longs.longs.size)
      setRefSlots(0)
      _longs ++= longs.longs
      this
    }

    def addRow(longs:Longs, refs:Refs): this.type = {
      _rows += 1
      setLongSlots(longs.longs.size)
      setRefSlots(refs.refs.size)
      _longs ++= longs.longs
      _refs ++= refs.refs
      this
    }

    def build() : MorselExecutionContext = {
      val morsel = new Morsel(longs, refs)
      val slots = SlotConfiguration(Map.empty, longSlots, refSlots)
      val context = MorselExecutionContext(morsel, slots, rows)
      context
    }
  }

  class FilteringInput extends Input {
    override def build(): FilteringPipelinedExecutionContext = {
      val morsel = new Morsel(longs, refs)
      val slots = SlotConfiguration(Map.empty, longSlots, refSlots)
      val context = MorselExecutionContext(morsel, slots, rows)
      val filteringContext = FilteringPipelinedExecutionContext(context)
      filteringContext
    }
  }

  class Given {
    protected var context: QueryContext = _
    protected var state: QueryState = _

    def withContext(context: QueryContext): this.type = {
      this.context = context
      this
    }

    def withQueryState(state: QueryState): this.type = {
      this.state = state
      this
    }

    def withOperator(operator: StatelessOperator): StatelessOperatorGiven = {
      val res = new StatelessOperatorGiven(operator)
      res.context = this.context
      res.state = this.state
      res
    }
  }

  trait HasOneInput {
    self: Given =>
    protected val input = new Input()

    def withNoInputRow(longSlots: Int, refSlots: Int): this.type = {
      input.withNoRows(longSlots, refSlots)
      this
    }

    def addInputRow(): this.type = {
      input.addRow()
      this
    }

    def addInputRow(refs:Refs): this.type = {
      input.addRow(refs)
      this
    }

    def addInputRow(longs:Longs): this.type = {
      input.addRow(longs)
      this
    }

    def addInputRow(longs:Longs, refs:Refs): this.type = {
      input.addRow(longs, refs)
      this
    }
  }

  def alwaysTruePredicate: Long => Boolean = _ => true
  def alwaysFalsePredicate: Long => Boolean = _ => false
  def moduloPredicate(n: Long): Long => Boolean = _ % n == 0
  def ltPredicate(n: Long): Long => Boolean = _ < n
  def gtePredicate(n: Long): Long => Boolean = _ >= n
  def eqPredicate(n: Long): Long => Boolean = _ == n

  def buildSequentialInput(numberOfRows: Int): FilteringPipelinedExecutionContext = {
    var rb = new FilteringInput()
    (0 until numberOfRows).foreach { i =>
      rb = rb.addRow(Longs(i, i*2), Refs(Values.stringValue(i.toString), Values.stringValue((i*2).toString)))
    }
    rb.build()
  }

  def validateRows(row: FilteringPipelinedExecutionContext, numberOfRows: Int, predicate: Long => Boolean): Unit = {
    val rawRow = row.shallowCopy()

    val expectedValidRows = (0 until numberOfRows).foldLeft(0)((count, i) => if (predicate(i)) count + 1 else count)

    row.getCurrentRow shouldEqual 0
    row.numberOfRows shouldEqual numberOfRows
    rawRow.getCurrentRow shouldEqual 0
    rawRow.numberOfRows shouldEqual numberOfRows

    row.getValidRows shouldEqual expectedValidRows
    rawRow.getValidRows shouldEqual expectedValidRows

    row.resetToFirstRow()
    (0 until numberOfRows).foreach { i =>
      rawRow.isValidRawRow shouldBe true
      if (predicate(i)) {
        rawRow.isCancelled(i) shouldBe false
        row.isValidRow shouldBe true
        validateRowDataContent(row, i)
        validateRowDataContent(rawRow, i)

        val hasNextRow = row.hasNextRow
        row.moveToNextRow()
        row.isValidRow shouldEqual hasNextRow
      } else {
        rawRow.isCancelled(i) shouldBe true
        rawRow.isValidRow shouldBe false
      }
      rawRow.moveToNextRawRow()
    }
    row.isValidRow shouldEqual false
    rawRow.isValidRow shouldEqual false
    rawRow.isValidRawRow shouldEqual false
  }

  def validateRowDataContent(row: MorselExecutionContext, i: Int): Unit = {
    row.getLongAt(0) shouldEqual i
    row.getLongAt(1) shouldEqual i*2
    row.getRefAt(0) shouldEqual Values.stringValue(i.toString)
    row.getRefAt(1) shouldEqual Values.stringValue((i*2).toString)
  }

  class StatelessOperatorGiven(operator: StatelessOperator) extends Given with HasOneInput {

    def whenOperate(): ThenOutput = {
      val row = input.build
      operator.operate(row, context, state, resources)
      new ThenOutput(row, input.longSlots, input.refSlots)
    }
  }

  class ThenOutput(outputRow: MorselExecutionContext, longSlots: Int, refSlots: Int) {
    private var rowCount = 0
    private val currentRow = outputRow.shallowCopy()
    currentRow.resetToFirstRow()

    private def assertLongs(longs: Longs): Unit = {
      if (longs.longs.size != longSlots) {
        throw new IllegalArgumentException(s"Unexpected number of longs in assertion: ${longs.longs.size}. Expected: $longSlots.")
      }
      var i = 0
      while (i < longSlots) {
        currentRow.getLongAt(i) shouldEqual longs.longs(i)
        i += 1
      }
    }

    private def assertRefs(refs: Refs): Unit = {
      if (refs.refs.size != refSlots) {
        throw new IllegalArgumentException(s"Unexpected number of refs in assertion: ${refs.refs.size}. Expected: $refSlots.")
      }
      var i = 0
      while (i < refSlots) {
        currentRow.getRefAt(i) shouldEqual refs.refs(i)
        i += 1
      }
    }

    def shouldReturnRow(longs: Longs): this.type = {
      rowCount += 1
      assertLongs(longs)
      assertRefs(Refs())
      currentRow.moveToNextRow()
      this
    }

    def shouldReturnRow(refs: Refs): this.type = {
      rowCount += 1
      assertLongs(Longs())
      assertRefs(refs)
      this
    }

    def shouldReturnRow(longs: Longs, refs: Refs): this.type = {
      rowCount += 1
      assertLongs(longs)
      assertRefs(refs)
      this
    }

    def shouldBeDone(): Unit = {
      outputRow.getValidRows should be(rowCount)
    }
  }

  class ThenContinuableOutput(task: ContinuableOperatorTask,
                              outputRow: MorselExecutionContext,
                              longSlots: Int,
                              refSlots: Int) extends ThenOutput(outputRow, longSlots, refSlots) {

    override def shouldBeDone(): Unit = {
      super.shouldBeDone()
      task.canContinue should be(false)
    }

    def shouldContinue(): Unit = {
      super.shouldBeDone()
      task.canContinue should be(true)
    }
  }

  implicit class CountSettingInt(i: Int) {
    def longs: Counts => Counts = _.copy(longSlots = i)

    def refs: Counts => Counts = _.copy(refSlots = i)

    def rows: Counts => Counts = _.copy(rows = i)
  }

  case class Counts(longSlots: Int, refSlots: Int, rows: Int)
  case class Longs(longs: Long*)
  case class Refs(refs: AnyValue*)
}
