/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselCypherRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTask
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.operators.ContinuableOperatorTaskWithMorsel
import org.neo4j.cypher.internal.runtime.pipelined.operators.OptionalOperatorTask
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
        prettyMorselWithHeader("INPUT:", withMorsel.inputMorsel) ++
          Array(
            workIdentity.toString
          )
      case task:OptionalOperatorTask =>
        Array("INPUT:") ++
          prettyStreamedData(task.morselData) ++
          Array(
            workIdentity.toString
          )
      case withAccumulator: ContinuableOperatorTaskWithAccumulator[_, _] =>
        Seq("INPUT:", withAccumulator.accumulator.toString, withAccumulator.workIdentity.toString)
    }
  }

  def prettyPostStartTask(startTask: ContinuableOperatorTask): Seq[String] = {
    startTask match {
      case withMorsel: ContinuableOperatorTaskWithMorsel =>
        prettyMorselWithHeader(
          "INPUT POST (canContinue: "+startTask.canContinue + "):",
          withMorsel.inputMorsel)
      case _:ContinuableOperatorTask => Array(
        s"INPUT POST (canContinue: ${startTask.canContinue})"
      )
    }
  }

  def prettyWork(morsel: MorselCypherRow, workIdentity: WorkIdentity): Seq[String] = {
    prettyMorselWithHeader("OUTPUT:", morsel) ++
      Array(
        workIdentity.toString,
        ""
      )
  }

  def prettyMorselWithHeader(header: String, morsel: MorselCypherRow): Seq[String] = {
    (
      Array(header) ++
        morsel.prettyString
      ).map(row => MORSEL_INDENT+row)
  }

  def prettyWorkDone: Seq[String] = {
    Seq("------------------------------------------------------------------------------------")
  }

  private def prettyStreamedData(streamedData: MorselData): Seq[String] = {
    Array(s"MorselData with downstream arg ids ${streamedData.argumentRowIdsForReducers.toSeq}") ++
      streamedData.morsels.flatMap(morsel => prettyMorselWithHeader("", morsel)) ++
      (streamedData.argumentStream match {
        case EndOfEmptyStream(argRow) => prettyMorselWithHeader("EndOfEmptyStream", argRow)
        case EndOfNonEmptyStream => Array("EndOfNonEmptyStream")
        case NotTheEnd => Array("NotTheEnd")
      })
  }
}
