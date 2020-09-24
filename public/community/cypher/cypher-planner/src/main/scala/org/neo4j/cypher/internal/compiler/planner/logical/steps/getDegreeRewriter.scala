/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

case object getDegreeRewriter extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val instance = bottomUp(rewriter,
    // Do not traverse into NestedPlanExpressions as they have been optimized already by an earlier call to plan
    _.isInstanceOf[NestedPlanExpression])

  private def rewriter = Rewriter.lift {

    //GetDegree > limit
    case e@GreaterThan(GetDegree(node, typ, dir), limit) =>
      HasDegreeGreaterThan(node, typ, dir, limit)(e.position)
    //limit > GetDegree
    case e@GreaterThan(limit, GetDegree(node, typ, dir)) =>
      HasDegreeLessThan(node, typ, dir, limit)(e.position)
    //GetDegree >= limit
    case e@GreaterThanOrEqual(GetDegree(node, typ, dir), limit) =>
      HasDegreeGreaterThanOrEqual(node, typ, dir, limit)(e.position)
    //limit >= GetDegree
    case e@GreaterThanOrEqual(limit, GetDegree(node, typ, dir)) =>
      HasDegreeLessThanOrEqual(node, typ, dir, limit)(e.position)
    //GetDegree < limit
    case e@LessThan(GetDegree(node, typ, dir), limit) =>
      HasDegreeLessThan(node, typ, dir, limit)(e.position)
    //limit < GreaterThan
    case e@LessThan(limit, GetDegree(node, typ, dir)) =>
      HasDegreeGreaterThan(node, typ, dir, limit)(e.position)
    //GetDegree <= limit
    case e@LessThanOrEqual(GetDegree(node, typ, dir), limit) =>
      HasDegreeLessThanOrEqual(node, typ, dir, limit)(e.position)
    //limit <= GreaterThan
    case e@LessThanOrEqual(limit, GetDegree(node, typ, dir)) =>
      HasDegreeGreaterThanOrEqual(node, typ, dir, limit)(e.position)
    //GetDegree = limit
    case e@Equals(GetDegree(node, typ, dir), value) =>
      HasDegree(node, typ, dir, value)(e.position)
    //limit = GreaterThan
    case e@Equals(value, GetDegree(node, typ, dir)) =>
      HasDegree(node, typ, dir, value)(e.position)

    // LENGTH( (a)-[]->() )
    case FunctionOfPattern(func, pe, node, rel, types, dir, otherNode) if func.function == functions.Length || func.function == functions.Size =>
      val peDeps = pe.dependencies
      if (peDeps.contains(node) && !peDeps.contains(rel) && !peDeps.contains(otherNode)) {
        calculateUsingGetDegree(func, node, types, dir)
      } else if (!peDeps.contains(node) && !peDeps.contains(rel) && peDeps.contains(otherNode)) {
        calculateUsingGetDegree(func, otherNode, types, dir.reversed)
      } else {
        func
      }

    // EXISTS( (a)-[]->() ) rewritten to GetDegree( (a)-[]->() ) > 0
    case FunctionOfPattern(func, pe, node, rel, types, dir, otherNode) if func.function == functions.Exists =>
      val peDeps = pe.dependencies
      if (peDeps.contains(node) && !peDeps.contains(rel) && !peDeps.contains(otherNode)) {
        calculateUsingHasDegreeGreaterThan(func, node, types, dir)
      } else if (!peDeps.contains(node) && !peDeps.contains(rel) && peDeps.contains(otherNode)) {
        calculateUsingHasDegreeGreaterThan(func, otherNode, types, dir.reversed)
      } else {
        func
      }
  }

  private def calculateUsingGetDegree(expr: Expression, node: LogicalVariable, types: Seq[RelTypeName], dir: SemanticDirection): Expression = {
    types
      .map(typ => GetDegree(node.copyId, Some(typ), dir)(typ.position))
      .reduceOption[Expression](Add(_, _)(expr.position))
      .getOrElse(GetDegree(node, None, dir)(expr.position))
  }

  private def calculateUsingHasDegreeGreaterThan(expr: Expression, node: LogicalVariable, types: Seq[RelTypeName], dir: SemanticDirection): Expression = {
    types
      .map(typ => HasDegreeGreaterThan(node.copyId, Some(typ), dir, SignedDecimalIntegerLiteral("0")(expr.position))(typ.position))
      .reduceOption[Expression](Or(_, _)(expr.position))
      .getOrElse(HasDegreeGreaterThan(node, None, dir, SignedDecimalIntegerLiteral("0")(expr.position))(expr.position))
  }
}

object FunctionOfPattern {
  def unapply(arg: Any): Option[(FunctionInvocation, PatternExpression, LogicalVariable, LogicalVariable, Seq[RelTypeName], SemanticDirection, LogicalVariable)] = arg match {
    //(a)-[]->()
    case func@FunctionInvocation(_, _, _, IndexedSeq(pe@PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(node), List(), None, _), RelationshipPattern(Some(rel), types, None, None, dir, _, _), NodePattern(Some(otherNode), List(), None, _))))))
    => Some((func, pe, node, rel, types, dir, otherNode))

    case _ => None
  }
}






