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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.{CandidateGenerator, LogicalPlanningContext}
import org.neo4j.cypher.internal.ir.{InterestingOrder, QueryGraph}
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.Exists
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.v4_0.util.{FreshIdNameGenerator, UnNamedNameGenerator}

case object selectPatternPredicates extends CandidateGenerator[LogicalPlan] {

  def apply(lhs: LogicalPlan, queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    for (
      pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
      if applicable(lhs, queryGraph, pattern, context.planningAttributes.solveds))
      yield {
        pattern match {
          case e:ExistsSubClause =>
            val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
            context.logicalPlanProducer.planSemiApply(lhs, innerPlan, e, context)
          case p@Not(e: ExistsSubClause) =>
            val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
            context.logicalPlanProducer.planAntiSemiApply(lhs, innerPlan, p, context)
          case p@Exists(patternExpression: PatternExpression) =>
            val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
            context.logicalPlanProducer.planSemiApply(lhs, rhs, p, context)
          case p@Not(Exists(patternExpression: PatternExpression)) =>
            val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
            context.logicalPlanProducer.planAntiSemiApply(lhs, rhs, p, context)
          case Ors(exprs) =>
            val (patternExpressions, expressions) = exprs.partition {
              case Exists(_: PatternExpression) => true
              case Not(Exists(_: PatternExpression)) => true
              case _ => false
            }
            val (plan, solvedPredicates) = planPredicates(lhs, patternExpressions, expressions, None, interestingOrder, context)
            context.logicalPlanProducer.solvePredicate(plan, onePredicate(solvedPredicates), context)
        }
      }
  }

  private def planInnerOfSubquery(lhs: LogicalPlan, context: LogicalPlanningContext, interestingOrder: InterestingOrder, e: ExistsSubClause): LogicalPlan = {
    // Creating a query graph by combining all extracted query graphs created by each entry of the patternElements
    val emptyTuple = (Map.empty[PatternElement, Variable], QueryGraph.empty)
    val(namedMap, qg) = e.patternElements.foldLeft(emptyTuple) { (acc, patternElement) =>
      patternElement match {
      case elem: RelationshipChain =>
        val patternExpr = PatternExpression(RelationshipsPattern(elem)(elem.position))
        val (namedExpr, namedMap) = PatternExpressionPatternElementNamer.apply(patternExpr)
        val qg = extractQG(lhs, namedExpr, context)

        (acc._1 ++ namedMap, acc._2 ++ qg)

      case elem: NodePattern =>
        val patternExpr = NodePatternExpression(List(elem))(elem.position)
        val (namedExpr, namedMap) = PatternExpressionPatternElementNamer.apply(patternExpr)
        val qg = extractQG(lhs, namedExpr, context)

        (acc._1 ++ namedMap, acc._2 ++ qg)
      }
    }

    // Adding the predicates to new query graph
    val new_qg = e.optionalWhereExpression.foldLeft(qg) {
      case (acc, p) => acc.addPredicates(e.outerScope.map(id => id.name), p)
    }

    val innerContext = createPlannerContext(context, namedMap)
    innerContext.strategy.plan(new_qg, interestingOrder, innerContext)
  }

  private def extractQG(source: LogicalPlan, namedExpr: NodePatternExpression, context: LogicalPlanningContext): QueryGraph = {
    import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._

    val qgArguments = getQueryGraphArguments(source, namedExpr)
    asQueryGraph(namedExpr, context.innerVariableNamer).withArgumentIds(qgArguments)
  }

  private def extractQG(source: LogicalPlan, namedExpr: PatternExpression, context: LogicalPlanningContext): QueryGraph = {
    import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._

    val qgArguments = getQueryGraphArguments(source, namedExpr)
    asQueryGraph(namedExpr, context.innerVariableNamer).withArgumentIds(qgArguments)
  }

  private def getQueryGraphArguments(source: LogicalPlan, namedExpr: Expression) = {
    val dependencies = namedExpr.
      dependencies.
      map(_.name).
      filter(id => UnNamedNameGenerator.isNamed(id))

    source.availableSymbols intersect dependencies
  }

  private def createPlannerContext(context: LogicalPlanningContext, namedMap: Map[PatternElement, Variable]): LogicalPlanningContext = {
    val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
    val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
    context.forExpressionPlanning(namedNodes, namedRels)
  }

  private def planPredicates(lhs: LogicalPlan,
                             patternExpressions: Set[Expression],
                             expressions: Set[Expression],
                             letExpression: Option[Expression],
                             interestingOrder: InterestingOrder,
                             context: LogicalPlanningContext): (LogicalPlan, Set[Expression]) = {
    patternExpressions.toList match {
      case (p@Exists(patternExpression: PatternExpression)) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val plan = context.logicalPlanProducer.planSelectOrSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet), interestingOrder, context)
        (plan, expressions + p)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val plan = context.logicalPlanProducer.planSelectOrAntiSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet), interestingOrder, context)
        (plan, expressions + p)

      case (p@Exists(patternExpression: PatternExpression)) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val (newLhs, newLetExpr) = createLetSemiApply(lhs, rhs, patternExpression, expressions, letExpression, interestingOrder, context)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), interestingOrder, context)
        (plan, solvedPredicates ++ Set(p) ++ expressions)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val (newLhs, newLetExpr) = createLetAntiSemiApply(lhs, rhs, patternExpression, p, expressions, letExpression, interestingOrder, context)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), interestingOrder, context)
        (plan, solvedPredicates ++ Set(p) ++ expressions)

      case _ =>
        throw new IllegalArgumentException("There should be at least one pattern expression")
    }
  }

  private def createLetSemiApply(lhs: LogicalPlan,
                                 rhs: LogicalPlan,
                                 patternExpression: PatternExpression,
                                 expressions: Set[Expression],
                                 letExpression: Option[Expression],
                                 interestingOrder: InterestingOrder,
                                 context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(patternExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), interestingOrder, context), ident)
  }

  private def createLetAntiSemiApply(lhs: LogicalPlan,
                                     rhs: LogicalPlan,
                                     patternExpression: PatternExpression,
                                     predicate: Expression,
                                     expressions: Set[Expression],
                                     letExpression: Option[Expression],
                                     interestingOrder: InterestingOrder,
                                     context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(patternExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetAntiSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrAntiSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), interestingOrder, context), ident)
  }

  private def rhsPlan(lhs: LogicalPlan, pattern: PatternExpression, interestingOrder: InterestingOrder, ctx: LogicalPlanningContext) = {
    val context = ctx.withUpdatedCardinalityInformation(lhs)
    val (plan, _) = context.strategy.planPatternExpression(lhs.availableSymbols, pattern, interestingOrder, context)
    plan
  }

  private def onePredicate(expressions: Set[Expression]): Expression = if (expressions.size == 1)
    expressions.head
  else
    Ors(expressions)(expressions.head.position)

  private def applicable(outerPlan: LogicalPlan, qg: QueryGraph, expression: Expression, solveds: Solveds) = {
    val symbolsAvailable = qg.argumentIds.subsetOf(outerPlan.availableSymbols)
    val isSolved = solveds.get(outerPlan.id).asSinglePlannerQuery.exists(_.queryGraph.selections.contains(expression))
    symbolsAvailable && !isSolved
  }


  private def freshId(patternExpression: PatternExpression) = {
    val name = FreshIdNameGenerator.name(patternExpression.position)
    (name, Variable(name)(patternExpression.position))
  }
}
