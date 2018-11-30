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

import org.neo4j.cypher.internal.compiler.v4_0.helpers.LazyIterable
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.{ProjectingSelector, Selector}

import scala.collection.immutable.BitSet

trait IDPSolverMonitor {
  def startIteration(iteration: Int)
  def endIteration(iteration: Int, depth: Int, tableSize: Int)
  def foundPlanAfter(iterations: Int)
}

trait ExtraRequirement[Requirement, Result] {
  def none: Requirement
  def is(requirement: Requirement): Boolean
  def forResult(result: Result): Requirement
}

/**
 * Based on the main loop of the IDP1 algorithm described in the paper
 *
 *   "Iterative Dynamic Programming: A New Class of Query Optimization Algorithms"
 *
 * written by Donald Kossmann and Konrad Stocker
 */
class IDPSolver[Solvable, Requirement, Result, Context](generator: IDPSolverStep[Solvable, Requirement, Result, Context], // generates candidates at each step
                                                        projectingSelector: ProjectingSelector[Result], // pick best from a set of candidates
                                                        registryFactory: () => IdRegistry[Solvable] = () => IdRegistry[Solvable], // maps from Set[S] to BitSet
                                                        tableFactory: (IdRegistry[Solvable], Seed[Solvable, Requirement, Result]) => IDPTable[Result, Requirement] = (registry: IdRegistry[Solvable], seed: Seed[Solvable, Requirement, Result]) => IDPTable(registry, seed),
                                                        maxTableSize: Int, // limits computation effort, reducing result quality
                                                        iterationDurationLimit: Long, // limits computation effort, reducing result quality
                                                        extraRequirement: ExtraRequirement[Requirement, Result],
                                                        monitor: IDPSolverMonitor) {

  def apply(seed: Seed[Solvable, Requirement, Result], initialToDo: Set[Solvable], context: Context): Iterator[((Set[Solvable], Requirement), Result)] = {
    val registry = registryFactory()
    val table = tableFactory(registry, seed)
    var toDo = registry.registerAll(initialToDo)

    // utility functions
    val goalSelector: Selector[((Goal, Requirement), Result)] = projectingSelector.apply[((Goal, Requirement), Result)](_._2, _)

    def generateBestCandidates(maxBlockSize: Int): Int = {
      var largestFinishedIteration = 0
      var blockSize = 1
      var keepGoing = true
      val start = System.currentTimeMillis()

      while (keepGoing && blockSize <= maxBlockSize) {
        var foundNoCandidate = true
        blockSize += 1
        val goals = toDo.subsets(blockSize)
        while (keepGoing && goals.hasNext) {
          val goal = goals.next()
          if (table(goal).isEmpty) {
            // TODO: Make sure the generator is NOT lazy and repetitive
            val candidates = LazyIterable(generator(registry, goal, table, context))
            val (baseCandidates, extraCandidates) = candidates.partition(candidate => extraRequirement.forResult(candidate) == extraRequirement.none)
            projectingSelector(baseCandidates).foreach { candidate =>
              foundNoCandidate = false
              table.put(goal, extraRequirement.none, candidate)
            }
            projectingSelector(extraCandidates).foreach { candidate =>
              foundNoCandidate = false
              table.put(goal, extraRequirement.forResult(candidate), candidate)
            }
            keepGoing = blockSize == 2 ||
              (table.size <= maxTableSize && (System.currentTimeMillis() - start) < iterationDurationLimit)
          }
        }
        largestFinishedIteration = if (foundNoCandidate || goals.hasNext) largestFinishedIteration else blockSize
      }
      largestFinishedIteration
    }

    def findBestCandidateInBlock(blockSize: Int): ((Goal, Requirement), Result) = {
      val blockCandidates: Iterable[((Goal, Requirement), Result)] = LazyIterable(table.plansOfSize(blockSize)).toIndexedSeq
      val bestInBlock: Option[((Goal, Requirement), Result)] = goalSelector(blockCandidates)
      bestInBlock.getOrElse {
        throw new IllegalStateException(
          s"""Found no solution for block with size $blockSize,
              |$blockCandidates were the selected candidates from the table $table""".stripMargin)
      }
    }

    def compactBlock(original: Goal): Unit = {
      val newId = registry.compact(original)
      table(original).foreach {
        case (ro, plan) =>
          table.put(BitSet.empty + newId, ro, plan)
      }
      toDo = toDo -- original + newId
      table.removeAllTracesOf(original)
    }

    // actual algorithm

    var iterations = 0

    while (toDo.size > 1) {
      iterations += 1
      monitor.startIteration(iterations)
      val largestFinished = generateBestCandidates(toDo.size)
      val (bestGoal, bestInBlock) = findBestCandidateInBlock(largestFinished)
      monitor.endIteration(iterations, largestFinished, table.size)
      compactBlock(bestGoal._1)
    }
    monitor.foundPlanAfter(iterations)

    table.plans.collect { case ((k, o), v) if extraRequirement.is(o) => (registry.explode(k), o) -> v }
  }
}
