/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.bitbucket.inkytonik.kiama.output.PrettyPrinter._
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compiler.v4_0.phases.LogicalPlanState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.{CypherException, InternalException}

trait DebugPrettyPrinter {
  val PRINT_QUERY_TEXT = true
  val PRINT_LOGICAL_PLAN = true
  val PRINT_REWRITTEN_LOGICAL_PLAN = true
  val PRINT_PIPELINE_INFO = true
  val PRINT_FAILURE_STACK_TRACE = true

  protected def printPlanInfo(lpState: LogicalPlanState) = {
    println(s"\n========================================================================")
    if (PRINT_QUERY_TEXT)
      println(s"\u001b[32m[QUERY]\n\n${lpState.queryText}") // Green
    if (PRINT_LOGICAL_PLAN) {
      println(s"\n\u001b[35m[LOGICAL PLAN]\n") // Magenta
      prettyPrintLogicalPlan(lpState.logicalPlan)
    }
    println("\u001b[30m")
  }

  protected def printRewrittenPlanInfo(logicalPlan: LogicalPlan) = {
    if (PRINT_REWRITTEN_LOGICAL_PLAN) {
      println(s"\n\u001b[35m[REWRITTEN LOGICAL PLAN]\n") // Magenta
      prettyPrintLogicalPlan(logicalPlan)
    }
    println("\u001b[30m")
  }

  protected def printPipe(slotConfigurations: SlotConfigurations, pipe: Pipe) = {
    if (PRINT_PIPELINE_INFO) {
      println(s"\n\u001b[36m[SLOT CONFIGURATIONS]\n") // Cyan
      prettyPrintPipelines(slotConfigurations)
      println(s"\n\u001b[34m[PIPE INFO]\n") // Blue
      prettyPrintPipe(pipe)
    }
    println("\u001b[30m")
  }

  protected def printFailureStackTrace(e: CypherException) = {
    if (PRINT_FAILURE_STACK_TRACE) {
      println("------------------------------------------------")
      println("<<< Slotted failed because:\u001b[31m") // Red
      e.printStackTrace(System.out)
      println("\u001b[30m>>>")
      println("------------------------------------------------")
    }
  }

  private def prettyPrintLogicalPlan(plan: LogicalPlan): Unit = {
    val planAnsiPre = "\u001b[1m\u001b[35m" // Bold on + magenta
    val planAnsiPost = "\u001b[21m\u001b[35m" // Restore to bold off + magenta
    def prettyPlanName(plan: LogicalPlan) = s"$planAnsiPre${plan.productPrefix}$planAnsiPost"
    def prettyId(id: Id) = s"\u001b[4m\u001b[35m${id}\u001b[24m\u001b[35m" // Underlined + magenta

    def show(v: Any): Doc =
      link(v.asInstanceOf[AnyRef],
        v match {
          case id: Id =>
            text(prettyId(id))

          case plan: LogicalPlan =>
            (plan.lhs, plan.rhs) match {
              case (None, None) =>
                val elements = plan.productIterator.toList
                list(plan.id :: elements, prettyPlanName(plan), show)

              case (Some(lhs), None) =>
                val otherElements: List[Any] = plan.productIterator.toList.filter {
                  case e: AnyRef => e ne lhs
                  case _ => true
                }
                list(plan.id :: otherElements, prettyPlanName(plan), show) <>
                  line <> show(lhs)

              case (Some(lhs), Some(rhs)) =>
                val otherElements: List[Any] = plan.productIterator.toList.filter {
                  case e: AnyRef => (e ne lhs) && (e ne rhs)
                  case _ => true
                }
                val lhsDoc = "[LHS]" <> line <> nest(show(lhs), 2)
                val rhsDoc = s"[RHS of ${plan.getClass.getSimpleName} (${plan.id})]" <> line <> nest(show(rhs), 2)
                list(plan.id :: otherElements, prettyPlanName(plan), show) <>
                  line <> nest(lhsDoc, 2) <>
                  line <> nest(rhsDoc, 2)

              case _ =>
                throw new InternalException("Invalid logical plan structure")
            }

          case _ =>
            any(v)
        }
      )

    val prettyDoc = pretty(show(plan), w = 120)
    println(prettyDoc.layout)
  }

  protected def prettyPrintPipelines(pipelines: SlotConfigurations): Unit = {
    val transformedPipelines = pipelines.iterator.foldLeft(Seq.empty[Any]) {
      case (acc, (k: Id, v)) => acc :+ (k.x -> v)
    }.sortBy { case (k: Int, _) => k }
    val prettyDoc = pretty(any(transformedPipelines), w = 120)
    println(prettyDoc.layout)
  }

  protected def prettyPrintPipe(pipe: Pipe): Unit = {
    val prettyDoc = pretty(any(pipe), w = 120)
    println(prettyDoc.layout)
  }
}
