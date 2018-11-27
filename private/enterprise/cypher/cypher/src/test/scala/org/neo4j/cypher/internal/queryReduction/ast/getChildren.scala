/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.queryReduction.ast

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.{BinaryOperatorExpression, _}
import org.neo4j.cypher.internal.v4_0.util._

object getChildren {

  private def ofOption[I](maybe: Option[I]): Seq[I] = {
    maybe match {
      case None => Seq()
      case Some(thing) => Seq(thing)
    }
  }

  def apply(node: ASTNode): Seq[ASTNode] = {
    node match {
      case Match(_, pattern, hints, maybeWhere) =>
        Seq(pattern) ++ hints ++ ofOption(maybeWhere)

      case Query(maybeHint, queryPart) =>
        ofOption(maybeHint) ++ Seq(queryPart)

      case SingleQuery(clauses) =>
        clauses

      case Pattern(pParts) =>
        pParts

      case EveryPath(elem) =>
        Seq(elem)

      case NodePattern(maybeVar, labels, maybeProps, maybeBaseNode) =>
        ofOption(maybeVar) ++ labels ++ ofOption(maybeProps) ++ ofOption(maybeBaseNode)

      case Variable(_) =>
        Seq()

      case Return(_, returnItems, maybeOrderBy, maybeSkip, maybeLimit, _) =>
        Seq(returnItems) ++ ofOption(maybeOrderBy) ++
          ofOption(maybeSkip) ++ ofOption(maybeLimit)

      case ReturnItems(_, items) =>
        items

      case UnaliasedReturnItem(exp, _) =>
        Seq(exp)

      case AliasedReturnItem(exp, variable) =>
        Seq(exp, variable)

      case Where(exp) =>
        Seq(exp)

      case _:Literal =>
        Seq()

      case Parameter(_, _) =>
        Seq()

      case Property(map, propertyKey) =>
        Seq(map, propertyKey)

      case Create(pattern) =>
        Seq(pattern)

      case HasLabels(expression, labels) =>
        Seq(expression) ++ labels

      case _:SymbolicName =>
        Seq()

      case RelationshipChain(element, relationship, rightNode) =>
        Seq(element, relationship, rightNode)

      case RelationshipPattern(variable, types, length, properties, _, _, maybeBaseRel) =>
        ofOption(variable) ++  types ++ ofOption(length.flatten) ++ ofOption(properties) ++ ofOption(maybeBaseRel)

      case FunctionInvocation(namespace, functionName, _, args) =>
        Seq(namespace, functionName) ++ args

      case Namespace(_) =>
        Seq()

      case With(distinct, returnItems, orderBy, skip, limit, where) =>
        Seq(returnItems) ++
        ofOption(orderBy) ++ ofOption(skip) ++ ofOption(limit) ++ ofOption(where)

      case MapExpression(items) =>
        items.flatMap { case (pkn, exp) => Seq(pkn, exp) }

      case FilterExpression(scope, expression) =>
        Seq(scope, expression)

      case i:IterablePredicateExpression =>
        Seq(i.scope, i.expression)

      case FilterScope(variable, innerPredicate) =>
        Seq(variable) ++ ofOption(innerPredicate)

      case ListLiteral(expressions) =>
        expressions

      case OrderBy(sortItems) =>
        sortItems

      case b:BinaryOperatorExpression =>
        Seq(b.lhs, b.rhs)

      case l:LeftUnaryOperatorExpression =>
        Seq(l.rhs)

      case r:RightUnaryOperatorExpression =>
        Seq(r.lhs)

      case m:MultiOperatorExpression =>
        m.exprs.toSeq

      case s:SortItem =>
        Seq(s.expression)

      case a:ASTSlicingPhrase =>
        Seq(a.expression)

      case u:Union =>
        Seq(u.part, u.query)

      case CaseExpression(expression, alternatives, default) =>
        ofOption(expression) ++ alternatives.flatMap { case (a, b) => Seq(a, b) } ++ ofOption(default)

      case ContainerIndex(expr, idx) =>
        Seq(expr, idx)

      case ReduceExpression(scope, init, list) =>
        Seq(scope, init, list)

      case ReduceScope(accumulator, variable, expression) =>
        Seq(accumulator, variable, expression)

      case Foreach(variable, expression, updates) =>
        Seq(variable, expression) ++ updates

      case SetClause(items) =>
        items

      case SetPropertyItem(property, expression) =>
        Seq(property, expression)

      case PatternComprehension(namedPath, pattern, predicate, projection) =>
        ofOption(namedPath) ++ Seq(pattern) ++ ofOption(predicate) ++ Seq(projection)

      case RelationshipsPattern(element) =>
        Seq(element)
    }
  }

}
