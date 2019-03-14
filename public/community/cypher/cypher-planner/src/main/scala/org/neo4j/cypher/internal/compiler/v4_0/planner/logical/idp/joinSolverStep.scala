/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.idp.joinSolverStep._
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.{LogicalPlanningContext, LogicalPlanningSupport}
import org.neo4j.cypher.internal.ir.{InterestingOrder, PatternRelationship, QueryGraph}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v4_0.logical.plans.LogicalPlan

object joinSolverStep {
  val VERBOSE = false
}

case class joinSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, InterestingOrder, LogicalPlan, LogicalPlanningContext] {

  import LogicalPlanningSupport._

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan, InterestingOrder], context: LogicalPlanningContext): Iterator[LogicalPlan] = {

    if (VERBOSE) {
      println(s"\n>>>> start solving ${show(goal, goalSymbols(goal, registry))}")
    }

    /**
      *  Normally, it is not desirable to join on the argument(s).
      *  The exception is when all bits that occurs in goal and the IDP table are compacted ones
      *  (= not registered), because then it will not be possible to find an expand solution anymore.
      */
    def registered: Int => Boolean = nbr => registry.lookup(nbr).isDefined
    val removeArguments = goal.exists(registered) || table.plans.exists(p => p._1._1.exists(registered))
    val argumentsToRemove  = if (removeArguments) qg.argumentIds else Set.empty[String]

    val goalSize = goal.size
    val planProducer = context.logicalPlanProducer
    val builder = Vector.newBuilder[LogicalPlan]

    for {
      leftSize <- 1.until(goalSize)
      leftGoal <- goal.subsets(leftSize)
      rightSize <- 1.until(goalSize)
      rightGoal <- goal.subsets(rightSize) if (leftGoal != rightGoal) && ((leftGoal | rightGoal) == goal)
      (_, lhs) <- table(leftGoal)
      (_, rhs) <- table(rightGoal)
    } {
        val overlappingNodes = computeOverlappingNodes(lhs, rhs, context.planningAttributes.solveds, argumentsToRemove)
        if (overlappingNodes.nonEmpty) {
          val overlappingSymbols = computeOverlappingSymbols(lhs, rhs, argumentsToRemove)
          if (overlappingSymbols == overlappingNodes) {
            if (VERBOSE) {
              println(s"${show(leftGoal, nodes(lhs, context.planningAttributes.solveds))} overlap ${show(rightGoal, nodes(rhs, context.planningAttributes.solveds))} on ${showNames(overlappingNodes)}")
            }
            // This loop is designed to find both LHS and RHS plans, so no need to generate them swapped here
            val matchingHints = qg.joinHints.filter(_.coveredBy(overlappingNodes))
            builder += planProducer.planNodeHashJoin(overlappingNodes, lhs, rhs, matchingHints, context)
          }
        }
      }

    builder.result().iterator
  }

  private def computeOverlappingNodes(lhs: LogicalPlan, rhs: LogicalPlan, solveds: Solveds, argumentsToRemove: Set[String]): Set[String] = {
    val leftNodes = nodes(lhs, solveds)
    val rightNodes = nodes(rhs, solveds)
    (leftNodes intersect rightNodes) -- argumentsToRemove
  }

  private def computeOverlappingSymbols(lhs: LogicalPlan, rhs: LogicalPlan, argumentsToRemove: Set[String]): Set[String] = {
    val leftSymbols = lhs.availableSymbols
    val rightSymbols = rhs.availableSymbols
    (leftSymbols intersect rightSymbols) -- argumentsToRemove
  }

  private def nodes(plan: LogicalPlan, solveds: Solveds) =
    solveds.get(plan.id).queryGraph.patternNodes

  private def show(goal: Goal, symbols: Set[String]) =
    s"${showIds(goal.toSet)}: ${showNames(symbols)}"

  private def goalSymbols(goal: Goal, registry: IdRegistry[PatternRelationship]) =
    registry.explode(goal).flatMap(_.coveredIds)

  private def showIds(ids: Set[Int]) =
    ids.toIndexedSeq.sorted.mkString("{", ", ", "}")

  private def showNames(ids: Set[String]) =
    ids.toIndexedSeq.sorted.mkString("[", ", ", "]")
}
