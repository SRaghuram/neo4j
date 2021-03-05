/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ir.QgWithInfo
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.UnnestingRewriter
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.fixedPoint

import scala.annotation.tailrec

object Eagerness {

  /**
   * Determines whether there is a conflict between the so-far planned LogicalPlan
   * and the remaining parts of the PlannerQuery. This function assumes that the
   * argument PlannerQuery is the very head of the PlannerQuery chain.
   */
  def readWriteConflictInHead(plan: LogicalPlan, plannerQuery: SinglePlannerQuery): Boolean = {
    // The first leaf node is always reading through a stable iterator.
    // We will only consider this analysis for all other node iterators.
    val leaves = plan.leaves.collect {
      case n: NodeLogicalLeafPlan => n.idName
      case r: RelationshipLogicalLeafPlan => r.idName
    }

    if (leaves.isEmpty)
      false // the query did not start with a read, possibly CREATE () ...
    else {
      // Start recursion by checking the given plannerQuery against itself
      val headQgWithInfo = QgWithInfo(plannerQuery.queryGraph, unstableLeaves = leaves.tail.toSet, stableIdentifier = leaves.headOption)
      headConflicts(plannerQuery, plannerQuery, headQgWithInfo)
    }
  }

  @tailrec
  private def headConflicts(head: SinglePlannerQuery, tail: SinglePlannerQuery, headQgWithInfo: QgWithInfo): Boolean = {
    val mergeReadWrite = head == tail && head.queryGraph.containsMergeRecursive
    val conflict = if (tail.queryGraph.readOnly || mergeReadWrite) false
    else {
      //if we have unsafe rels we need to check relation overlap and delete
      //overlap immediately
      (hasUnsafeRelationships(head.queryGraph) &&
        (tail.queryGraph.createRelationshipOverlap(headQgWithInfo) ||
          tail.queryGraph.deleteOverlap(headQgWithInfo) ||
          tail.queryGraph.setPropertyOverlap(headQgWithInfo))
        ) ||
        tail.queryGraph.nodeOverlap(headQgWithInfo) ||
        tail.queryGraph.removeLabelOverlap(headQgWithInfo) ||
        tail.queryGraph.setLabelOverlap(headQgWithInfo) ||
        tail.queryGraph.createRelationshipOverlap(headQgWithInfo) ||
        tail.queryGraph.setPropertyOverlap(headQgWithInfo) ||
        tail.queryGraph.deleteOverlap(headQgWithInfo) ||
        tail.queryGraph.foreachOverlap(headQgWithInfo)
    }
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      headConflicts(head, tail.tail.get, headQgWithInfo)
  }

  def headReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflictInHead(inputPlan, query))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def tailReadWriteEagerizeNonRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || readWriteConflict(query, query))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  // NOTE: This does not check conflict within the query itself (like tailReadWriteEagerizeNonRecursive)
  def tailReadWriteEagerizeRecursive(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    if (alwaysEager || (query.tail.isDefined && readWriteConflictInTail(query, query.tail.get)))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def headWriteReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon, context.semanticTable)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInHead(query, query.tail.get, context))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def tailWriteReadEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    val conflictInHorizon = query.queryGraph.overlapsHorizon(query.horizon, context.semanticTable)
    if (alwaysEager || conflictInHorizon || query.tail.isDefined && writeReadConflictInTail(query, query.tail.get, context))
      context.logicalPlanProducer.planEager(inputPlan, context)
    else
      inputPlan
  }

  def horizonReadWriteEagerize(inputPlan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val alwaysEager = context.config.updateStrategy.alwaysEager
    inputPlan match {
      case ProcedureCall(left, call) if call.signature.eager =>
        context.logicalPlanProducer.planProcedureCall(context.logicalPlanProducer.planEager(left, context), call, context)
      case _ if alwaysEager || (query.tail.nonEmpty && horizonReadWriteConflict(query, query.tail.get, context)) =>
        context.logicalPlanProducer.planEager(inputPlan, context)
      case _ =>
        inputPlan
    }
  }

  /**
   * Determines whether there is a conflict between the two PlannerQuery objects.
   * This function assumes that none of the argument PlannerQuery objects is
   * the head of the PlannerQuery chain.
   */
  @tailrec
  def readWriteConflictInTail(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
    val conflict = readWriteConflict(head, tail)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      readWriteConflictInTail(head, tail.tail.get)
  }

  /**
   * @return a QgWithInfo, where there is no stable identifier. Moreover all variables are assumed to be leaves.
   */
  private def qgWithNoStableIdentifierAndOnlyLeaves(qg: QueryGraph): QgWithInfo =
    QgWithInfo(qg, qg.allCoveredIds, None)

  def readWriteConflict(readQuery: SinglePlannerQuery, writeQuery: SinglePlannerQuery): Boolean = {
    val mergeReadWrite = readQuery == writeQuery && readQuery.queryGraph.containsMergeRecursive
    val conflict =
      if (writeQuery.queryGraph.readOnly || mergeReadWrite)
        false
      else
        writeQuery.queryGraph overlaps qgWithNoStableIdentifierAndOnlyLeaves(readQuery.queryGraph)
    conflict
  }

  @tailrec
  def writeReadConflictInTail(head: SinglePlannerQuery, tail: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    val tailQgWithInfo = qgWithNoStableIdentifierAndOnlyLeaves(tail.queryGraph)
    val conflict =
      if (tail.queryGraph.writeOnly) false
      else (head.queryGraph overlaps tailQgWithInfo) ||
        head.queryGraph.overlapsHorizon(tail.horizon, context.semanticTable) ||
        deleteReadOverlap(head.queryGraph, tail.queryGraph, context)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      writeReadConflictInTail(head, tail.tail.get, context)
  }

  @tailrec
  def horizonReadWriteConflict(head: SinglePlannerQuery, tail: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    val conflict = tail.queryGraph.overlapsHorizon(head.horizon, context.semanticTable)
    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      horizonReadWriteConflict(head, tail.tail.get, context)
  }

  private def deleteReadOverlap(from: QueryGraph, to: QueryGraph, context: LogicalPlanningContext): Boolean = {
    val deleted = from.identifiersToDelete
    deletedRelationshipsOverlap(deleted, to, context) || deletedNodesOverlap(deleted, to, context)
  }

  private def deletedRelationshipsOverlap(deleted: Set[String], to: QueryGraph, context: LogicalPlanningContext): Boolean = {
    val relsToRead = to.allPatternRelationshipsRead
    val relsDeleted = deleted.filter(id => context.semanticTable.isRelationship(id))
    relsToRead.nonEmpty && relsDeleted.nonEmpty
  }

  private def deletedNodesOverlap(deleted: Set[String], to: QueryGraph, context: LogicalPlanningContext): Boolean = {
    val nodesToRead = to.allPatternNodesRead
    val nodesDeleted = deleted.filter(id => context.semanticTable.isNode(id))
    nodesToRead.nonEmpty && nodesDeleted.nonEmpty
  }

  def writeReadConflictInHead(head: SinglePlannerQuery, tail: SinglePlannerQuery, context: LogicalPlanningContext): Boolean = {
    // If the first planner query is write only, we can use a different overlaps method (writeOnlyHeadOverlaps)
    // that makes us less eager
    if (head.queryGraph.writeOnly)
      writeReadConflictInHeadRecursive(head, tail)
    else
      writeReadConflictInTail(head, tail, context)
  }

  @tailrec
  def writeReadConflictInHeadRecursive(head: SinglePlannerQuery, tail: SinglePlannerQuery): Boolean = {
    // TODO:H Refactor: This is same as writeReadConflictInTail, but with different overlaps method. Pass as a parameter
    val conflict =
      if (tail.queryGraph.writeOnly) false
      else
      // NOTE: Here we do not check writeOnlyHeadOverlapsHorizon, because we do not know of any case where a
      // write-only head could cause problems with reads in future horizons
        head.queryGraph writeOnlyHeadOverlaps qgWithNoStableIdentifierAndOnlyLeaves(tail.queryGraph)

    if (conflict)
      true
    else if (tail.tail.isEmpty)
      false
    else
      writeReadConflictInHeadRecursive(head, tail.tail.get)
  }

  case class unnestEager(override val solveds: Solveds,
                         override val cardinalities: Cardinalities,
                         override val providedOrders: ProvidedOrders,
                         override val attributes: Attributes[LogicalPlan]) extends Rewriter with UnnestingRewriter {

    /*
    Based on unnestApply (which references a paper)

    This rewriter does _not_ adhere to the contract of moving from a valid
    plan to a valid plan, but it is crucial to get eager plans placed correctly.

    Glossary:
      Ax : Apply
      L,R: Arbitrary operator, named Left and Right
      E : Eager
      Up : UpdatingPlan
     */

    private val instance: Rewriter = fixedPoint(bottomUp(Rewriter.lift {

      // L Ax (E R) => E Ax (L R)
      case apply@Apply(lhs, eager: Eager) =>
        unnestRightUnary(apply, lhs, eager)

     // L Ax (Up R) => Up Ax (L R)
      case apply@Apply(lhs, updatingPlan: UpdatingPlan) =>
        unnestRightUnary(apply, lhs, updatingPlan)
    }))

    override def apply(input: AnyRef): AnyRef = instance.apply(input)
  }
}
