/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.runtime.morsel.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.morsel.operators._
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity

object MorselDebugSupport {

  private val MORSEL_INDENT = "  "

  def prettyStartTask(startTask: ContinuableOperatorTask, workIdentity: WorkIdentity): Seq[String] = {
    startTask match {
      case withMorsel: ContinuableOperatorTaskWithMorsel =>
        prettyMorselWithHeader("INPUT:", withMorsel.inputMorsel) ++
          Array(
            workIdentity.toString
          )
    }
  }

  def prettyPostStartTask(startTask: ContinuableOperatorTask): Seq[String] = {
    startTask match {
      case withMorsel: ContinuableOperatorTaskWithMorsel =>
        prettyMorselWithHeader(
          "INPUT POST (canContinue: "+startTask.canContinue + "):",
          withMorsel.inputMorsel)
    }
  }

  def prettyWork(morsel: MorselExecutionContext, workIdentity: WorkIdentity): Seq[String] = {
    prettyMorselWithHeader("OUTPUT:", morsel) ++
      Array(
        "",
        workIdentity.toString
      )
  }

  private def prettyMorselWithHeader(header: String, morsel: MorselExecutionContext): Seq[String] = {
    (
      Array(header) ++
      morsel.prettyString
    ).map(row => MORSEL_INDENT+row)
  }
}
