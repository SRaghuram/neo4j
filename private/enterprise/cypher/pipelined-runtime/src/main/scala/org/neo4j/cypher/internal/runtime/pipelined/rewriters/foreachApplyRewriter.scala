/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.rewriters

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.IsEmpty
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.FreshIdNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * In the general case it rewrites ForeachApply from
 *
 * {{{
 *    ForeachApply(i IN LIST)
 *       /  \
 *      L    R
 *}}}
 * to
 *
 * {{{
 *  SelectOrSemiApply(l IS NULL OR isEmpty(l))
 *       /                  \
 * Projection(LIST AS l)   Apply
 *    /                     / \
 *   L        Unwind(i IN l)   R
 *                       /
 *                     ARG
 *}}}
 *
 * A very common case is that R here is just a singly branched update plan such as
 * CreateNode or SetProperties. In this case we specialize and put the Unwind directly on R.
 * For example:
 *
 * {{{
 *    ForeachApply(i IN LIST)
 *       /  \
 *      L   Create
 *            \
 *            Argument
 *
 *}}}
 *
 * is rewritten to
 *
 *{{{
 *  SelectOrSemiApply(l IS NULL OR isEmpty(l))
 *       /                  \
 * Projection(LIST AS l)   Create
 *    /                      \
 *   L                     Unwind(i IN l)
 *                             \
 *                             Argument
 *}}}
 */
case class foreachApplyRewriter(cardinalities: Cardinalities,
                                effectiveCardinalities: EffectiveCardinalities,
                                providedOrders: ProvidedOrders,
                                idGen: IdGen,
                                stopper: AnyRef => Boolean) extends Rewriter {

  /**
   *  Finds the left-most leaf and adds an Unwind on top of the leaf
   */
  private def addUnwindToLeftMostLeaf(leftMostLeaf: LogicalPlan, variableName: String, listExpression: Expression) = bottomUp(Rewriter.lift {
    case leaf: LogicalLeafPlan if leaf eq leftMostLeaf =>
      newPlan(leaf, UnwindCollection(_, variableName, listExpression)(idGen))
  })

  private def newPlan(source: LogicalPlan, creator: LogicalPlan => LogicalPlan): LogicalPlan = {
    val plan = creator(source)
    cardinalities.copy(source.id, plan.id)
    effectiveCardinalities.copy(source.id, plan.id)
    providedOrders.copy(source.id, plan.id)
    plan
  }

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case o@ForeachApply(lhs: LogicalPlan, rhs: LogicalPlan, variableName, listExpression) =>
      val listProjected = Variable(s"${FreshIdNameGenerator.name(listExpression.position)}_$variableName")(listExpression.position)
      val newLhs = newPlan(lhs, Projection(_, Map(listProjected.name -> listExpression))(idGen))
      val newRhs = rhs.leaves match {
        case Seq(a: Argument) => //special case for single branch plans
          rhs.endoRewrite(addUnwindToLeftMostLeaf(a, variableName, listProjected))
        case _ => //general case
          val unwind = UnwindCollection(Argument()(idGen), variableName, listProjected)(idGen)
          cardinalities.copy(rhs.id, unwind.id)
          effectiveCardinalities.copy(rhs.id, unwind.id)
          providedOrders.copy(rhs.id, unwind.id)
          val apply = newPlan(unwind, Apply(_, rhs)(idGen))
          apply
      }

      SelectOrSemiApply(newLhs, newRhs, Ors(Seq(IsNull(listProjected)(InputPosition.NONE), IsEmpty.asInvocation(listProjected)(InputPosition.NONE)))(InputPosition.NONE))(SameId(o.id))
  }, stopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}