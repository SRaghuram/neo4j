/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.morsel.execution.{Morsel, MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue

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

    def build : (Morsel, MorselExecutionContext) = {
      val morsel = new Morsel(longs, refs)
      val context = MorselExecutionContext(morsel, longSlots, refSlots, rows)
      (morsel, context)
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

  class StatelessOperatorGiven(operator: StatelessOperator) extends Given with HasOneInput {

    def whenOperate(): ThenOutput = {
      val (morsel, row) = input.build
      operator.operate(row, context, state, resources)
      new ThenOutput(morsel, row, input.longSlots, input.refSlots)
    }
  }

  class ThenOutput(outputMorsel: Morsel, outputRow: MorselExecutionContext, longSlots: Int, refSlots: Int) {
    private var longPointer = 0
    private var refPointer = 0
    private var rowCount = 0

    private def assertLongs(longs: Longs): Unit = {
      if (longs.longs.size != longSlots) {
        throw new IllegalArgumentException(s"Unexpected number of longs in assertion: ${longs.longs.size}. Expected: $longSlots.")
      }
      outputMorsel.longs.slice(longPointer, longPointer + longs.longs.size) should equal(longs.longs)
      longPointer += longs.longs.size
    }

    private def assertRefs(refs: Refs): Unit = {
      if (refs.refs.size != refSlots) {
        throw new IllegalArgumentException(s"Unexpected number of refs in assertion: ${refs.refs.size}. Expected: $refSlots.")
      }
      outputMorsel.refs.slice(refPointer, refPointer + refs.refs.size) should equal(refs.refs)
      refPointer += refs.refs.size
    }

    def shouldReturnRow(longs: Longs): this.type = {
      rowCount += 1
      assertLongs(longs)
      assertRefs(Refs())
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
                              outputMorsel: Morsel,
                              outputRow: MorselExecutionContext,
                              longSlots: Int,
                              refSlots: Int) extends ThenOutput(outputMorsel, outputRow, longSlots, refSlots) {

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
