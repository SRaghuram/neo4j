/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentity
import org.neo4j.cypher.internal.runtime.parallel.WorkIdentityImpl
import org.neo4j.cypher.internal.runtime.vectorized.ContinuableOperatorTask
import org.neo4j.cypher.internal.runtime.vectorized.EagerReduceOperator
import org.neo4j.cypher.internal.runtime.vectorized.EmptyQueryState
import org.neo4j.cypher.internal.runtime.vectorized.Morsel
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.vectorized.QueryResources
import org.neo4j.cypher.internal.runtime.vectorized.QueryState
import org.neo4j.cypher.internal.runtime.vectorized.StatelessOperator
import org.neo4j.cypher.internal.runtime.vectorized.StreamingOperator
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.values.AnyValue

abstract class MorselUnitTest extends CypherFunSuite {
  protected val resources = mock[QueryResources](RETURNS_DEEP_STUBS)

  protected val workId: WorkIdentity = WorkIdentityImpl(42, "Work Identity Description")

  def nodeCursor(longs: Long*): NodeCursor = {
    val cursor = mock[NodeCursor]
    val bools = longs.map(_ => true) :+ false
    when(cursor.next()).thenReturn(bools.head, bools.tail: _*)
    when(cursor.nodeReference()).thenReturn(longs.head, longs.tail: _*)
    cursor
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

    def noRows(longSlots: Int, refSlots: Int): this.type = {
      setLongSlots(longSlots)
      setRefSlots(refSlots)
      this
    }

    def row(): this.type = {
      _rows += 1
      setLongSlots(0)
      setRefSlots(0)
      this
    }

    def row(refs:Refs): this.type = {
      _rows += 1
      setLongSlots(0)
      setRefSlots(refs.refs.size)
      _refs ++= refs.refs
      this
    }

    def row(longs:Longs): this.type = {
      _rows += 1
      setLongSlots(longs.longs.size)
      setRefSlots(0)
      _longs ++= longs.longs
      this
    }

    def row(longs:Longs, refs:Refs): this.type = {
      _rows += 1
      setLongSlots(longs.longs.size)
      setRefSlots(refs.refs.size)
      _longs ++= longs.longs
      _refs ++= refs.refs
      this
    }
  }

  class Given {
    protected var context: QueryContext = _
    protected var state: QueryState = _

    def context(context: QueryContext): this.type = {
      this.context = context
      this
    }

    def state(state: QueryState): this.type = {
      this.state = state
      this
    }

    def operator(operator: StreamingOperator): StreamingOperatorGiven = {
      val res = new StreamingOperatorGiven(operator)
      res.context = this.context
      res.state = this.state
      res
    }

    def operator(operator: StatelessOperator): StatelessOperatorGiven = {
      val res = new StatelessOperatorGiven(operator)
      res.context = this.context
      res.state = this.state
      res
    }

    def operator(operator: EagerReduceOperator): EagerReduceOperatorGiven = {
      val res = new EagerReduceOperatorGiven(operator)
      res.context = this.context
      res.state = this.state
      res
    }
  }

  trait OneInputGiven extends Given {
    protected val input = new Input()

    def noInputRow(longSlots: Int, refSlots: Int): this.type = {
      input.noRows(longSlots, refSlots)
      this
    }

    def inputRow(): this.type = {
      input.row()
      this
    }

    def inputRow(refs:Refs): this.type = {
      input.row(refs)
      this
    }

    def inputRow(longs:Longs): this.type = {
      input.row(longs)
      this
    }

    def inputRow(longs:Longs, refs:Refs): this.type = {
      input.row(longs, refs)
      this
    }
  }

  trait OutputGiven extends Given {
    protected var output = Counts(0, 0, 0)

    def output(setters: (Counts => Counts)*): this.type = {
      output = setters.foldLeft(output)((o, setter) => setter(o))
      this
    }
  }

  class StreamingOperatorGiven(operator: StreamingOperator) extends OneInputGiven with OutputGiven {

    def whenInit(rowNum: Int = 0): ThenTasks = {
      val morsel = new Morsel(input.longs, input.refs)
      val row = MorselExecutionContext(morsel, input.longSlots, input.refSlots, input.rows)
      (0 until rowNum).foreach(_ => row.moveToNextRow())
      // TODO operator.init will return Seq in the near future (when we merge the parallel scans PR)
      // Thus, we prepare here already be allowing to assert on multiple returned tasks
      val tasks = IndexedSeq(operator.init(context, state, row, resources))
      new ThenTasks(tasks, context, output)
    }
  }

  class StatelessOperatorGiven(operator: StatelessOperator) extends OneInputGiven {

    def whenOperate(): ThenOutput = {
      val morsel = new Morsel(input.longs, input.refs)
      val row = MorselExecutionContext(morsel, input.longSlots, input.refSlots, input.rows)
      operator.operate(row, context, state, resources)
      new ThenOutput(morsel, row, input.longSlots, input.refSlots)
    }
  }

  class EagerReduceOperatorGiven(operator: EagerReduceOperator) extends OutputGiven {
    private var inputs = Array.newBuilder[Input]

    def addInput(input: Input): this.type = {
      inputs += input
      this
    }

    def whenInit(): WhenContinuableOperatorTask = {
      val rows = inputs.result().map { input =>
        val morsel = new Morsel(input.longs, input.refs)
        MorselExecutionContext(morsel, input.longSlots, input.refSlots, input.rows)
      }
      val task = operator.init(context, state, rows, resources)
      new WhenContinuableOperatorTask(task, context, output)
    }
  }

  class ThenTasks(tasks: IndexedSeq[ContinuableOperatorTask], context: QueryContext, output: Counts) {
    def shouldReturnNTasks(n: Int): IndexedSeq[WhenContinuableOperatorTask] = {
      tasks.size should be(n)
      tasks.map(new WhenContinuableOperatorTask(_, context, output))
    }
  }

  class WhenContinuableOperatorTask(task: ContinuableOperatorTask, context: QueryContext, output: Counts) {
    def whenOperate: ThenContinuableOutput = {
      val outputMorsel = new Morsel(new Array[Long](output.longSlots * output.rows), new Array[AnyValue](output.refSlots * output.rows))
      val outputRow = MorselExecutionContext(outputMorsel, output.longSlots, output.refSlots, output.rows)
      task.operate(outputRow, context, EmptyQueryState(), resources)
      new ThenContinuableOutput(task, outputMorsel, outputRow, output.longSlots, output.refSlots)
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
