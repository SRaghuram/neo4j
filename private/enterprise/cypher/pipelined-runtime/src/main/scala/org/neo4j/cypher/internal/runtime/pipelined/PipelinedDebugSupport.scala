/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.operators.AntiOperatorTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithAccumulators
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithMorsel
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithMorselData
import org.neo4j.cypher.internal.runtime.pipelined.operators.InputLoopTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.InputLoopWithMorselDataTask
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.EndOfNonEmptyStream
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.MorselData
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.NotTheEnd
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

object PipelinedDebugSupport {

  private val MORSEL_INDENT = "  "

  def prettyStartTask(startTask: ContinuableOperatorTask, workIdentity: WorkIdentity): Seq[String] = {
    startTask match {
      case withMorsel: ContinuableOperatorTaskWithMorsel =>
        prettyMorselWithHeader("INPUT:", withMorsel.inputMorsel, currentRow(withMorsel)) :+
        prettyWorkIdentity(workIdentity)
      case withMorselData: ContinuableOperatorTaskWithMorselData =>
        prettyMorselDataWithHeader("INPUT DATA:", withMorselData.morselData, -1) :+
        prettyWorkIdentity(workIdentity)
      case task: InputLoopWithMorselDataTask =>
        Array("INPUT:") ++
        prettyStreamedData(task.morselData) :+
        prettyWorkIdentity(workIdentity)
      case task: AntiOperatorTask =>
        Array("INPUT:") ++
          task.morselDatas.flatMap(prettyStreamedData) :+
          prettyWorkIdentity(workIdentity)

      case withAccumulator: ContinuableOperatorTaskWithAccumulators[_, _] =>
        Array("INPUT:") ++
          withAccumulator.accumulators.map(acc => acc.toString) :+
          prettyWorkIdentity(withAccumulator.workIdentity)
    }
  }

  def prettyPostStartTask(startTask: ContinuableOperatorTask): Seq[String] = {
    startTask match {
      case withMorsel: ContinuableOperatorTaskWithMorsel =>
        prettyMorselWithHeader(
          "INPUT POST (canContinue: "+startTask.canContinue + "):",
          withMorsel.inputMorsel, currentRow(withMorsel))
      case _: ContinuableOperatorTask => Array(
        s"INPUT POST (canContinue: ${startTask.canContinue})"
      )
    }
  }

  def prettyWork(morsel: Morsel, workIdentity: WorkIdentity): Seq[String] = {
    prettyMorselWithHeader("OUTPUT:", morsel) ++
      Array(
        prettyWorkIdentity(workIdentity),
        ""
      )
  }

  def prettyWorkIdentity(workIdentity: WorkIdentity): String =
    DebugSupport.Bold + workIdentity.toString + DebugSupport.Reset

  def prettyMorselWithHeader(header: String, morsel: Morsel, currentRow: Int = -1): Seq[String] = {
    (
      Array(header) ++
        morsel.prettyString(currentRow)
      ).map(row => MORSEL_INDENT+row)
  }

  def prettyMorselDataWithHeader(header: String, morselData: MorselData, currentRow: Int = -1): Seq[String] = {
    (
      Array(header) ++
        morselData.morsels.zipWithIndex
          .flatMap {
            case (morsel, index) =>
              Seq(s"- Morsel $index") ++ morsel.prettyString(currentRow)
          }
      ).map(row => MORSEL_INDENT+row)
  }

  def prettyWorkDone: Seq[String] = {
    Seq("------------------------------------------------------------------------------------")
  }

  private def currentRow(withMorsel: ContinuableOperatorTaskWithMorsel): Int =
    withMorsel match {
      case x: InputLoopTask => x.inputCursor.row
      case _ => -1
    }

  private def prettyStreamedData(streamedData: MorselData): Seq[String] = {
    Array(s"MorselData with argument row: ${streamedData.viewOfArgumentRow}",
      s"with downstream arg ids ${streamedData.argumentRowIdsForReducers.toSeq}") ++
      streamedData.morsels.flatMap(morsel => prettyMorselWithHeader("", morsel)) ++
      (streamedData.argumentStream match {
        case EndOfEmptyStream => Array("EndOfEmptyStream")
        case EndOfNonEmptyStream => Array("EndOfNonEmptyStream")
        case NotTheEnd => Array("NotTheEnd")
      })
  }
}
