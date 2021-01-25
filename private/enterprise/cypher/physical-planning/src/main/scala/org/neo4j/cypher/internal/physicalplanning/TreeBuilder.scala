/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.exceptions.InternalException

import scala.collection.mutable

/**
 * Traverses the logical plan tree structure and builds up the corresponding output structure.
 * The traversal order is a kind of depth-first combined in/post-order (left first), in that
 * we visit (1) the left subtree, (2) the root, (3) the right subtree, and then (4) revisit the root.
 *
 * In addition to traversing the tree, this class keeps track of the right hand side
 * nesting (`ARGUMENT`s) of all plans. This is done by stacking the generic `ARGUMENT` for
 * every binary operator (returned at (2)), and passing the top `ARGUMENT` into all plan callbacks.
 *
 * We model the whole plan tree as being encompassed by an implicit outer apply which is executed
 * with a single row. This lets us avoid specializing reducing operators that are not on the rhs of an apply.
 * This single argument is constructed using the [[TreeBuilder#initialArgument]] callback.
 *
 * The argument handling can be used to flatten the output structure, e.g. an Apply plan `a` can be turned into:
 *
 *   a              a
 *  / \    ===>    /
 * b   c          c
 *               /
 *              b
 *
 * Given a logical plan such as:
 *
 *           a
 *        /    \
 *       b      c
 *      / \    / \
 *     d   e  f   g
 *     |
 *     h
 *
 * The virtual method callbacks will be called in the following sequence:
 *
 * H      = onLeaf(h, initialArgument)
 * D      = onOneChildPlan(d, H, initialArgument)
 * argB   = onTwoChildPlanComingFromLeft(b, D, initialArgument)
 * E      = onLeaf(e, argB)
 * B      = onTwoChildPlanComingFromRight(b, D, E, argB)
 * argA   = onTwoChildPlanComingFromLeft(a, B, initialArgument)
 * F      = onLeaf(f, argA)
 * argC   = onTwoChildPlanComingFromLeft(c, F, argA)
 * G      = onLeaf(g, argC)
 * C      = onTwoChildPlanComingFromRight(c, F, G, argC)
 * A      = onTwoChildPlanComingFromRight(a, B, C, argA)
 *
 * @tparam T type of build output
 * @tparam ARGUMENT type of argument
 */
trait TreeBuilder[T, ARGUMENT] {

  protected def initialArgument(leftLeaf: LogicalPlan): ARGUMENT
  protected def onLeaf(plan: LogicalPlan, argument: ARGUMENT): T
  protected def onOneChildPlan(plan: LogicalPlan, source: T, argument: ARGUMENT): T
  protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan, lhs: T, argument: ARGUMENT): ARGUMENT
  protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: T, rhs: T, argument: ARGUMENT): T
  protected def validatePlan(plan: LogicalPlan)

  def build(plan: LogicalPlan): T = {

    val planStack = new mutable.Stack[LogicalPlan]()
    val outputStack = new mutable.Stack[T]()
    val argumentStack = new mutable.Stack[ARGUMENT]()
    var comingFrom = plan

    /**
     * Eagerly populate the stack using all the lhs children.
     */
    def populate(plan: LogicalPlan): Unit = {
      var current = plan
      while (!current.isLeaf) {
        validatePlan(current)
        planStack.push(current)
        current = current.lhs.get
      }
      comingFrom = current
      planStack.push(current)
    }

    populate(plan)
    argumentStack.push(initialArgument(planStack.top))

    while (planStack.nonEmpty) {
      val current = planStack.pop()
      val argument = argumentStack.top

      (current.lhs, current.rhs) match {
        case (None, None) =>
          val output = onLeaf(current, argument)
          outputStack.push(output)

        case (Some(_), None) =>
          val source = outputStack.pop()
          val output = onOneChildPlan(current, source, argument)
          outputStack.push(output)

        case (Some(left), Some(right)) if right eq left =>
          throw new InternalException(s"Tried to map bad logical plan. LHS and RHS must never be the same: op: $current\nfull plan: $plan")

        case (Some(left), Some(right)) if comingFrom eq left =>
          val leftOutput = outputStack.top
          val newArgument = onTwoChildPlanComingFromLeft(current, leftOutput, argument)
          argumentStack.push(newArgument)
          planStack.push(current)
          populate(right)

        case (Some(_), Some(right)) if comingFrom eq right =>
          val rightOutput = outputStack.pop()
          val leftOutput = outputStack.pop()
          val output = onTwoChildPlanComingFromRight(current, leftOutput, rightOutput, argument)
          argumentStack.pop()
          outputStack.push(output)
      }

      comingFrom = current
    }

    val result = outputStack.pop()
    require(outputStack.isEmpty, "Should have emptied the stack of output by now!")

    result
  }
}
