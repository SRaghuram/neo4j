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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, Literal, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherBoolean
import org.neo4j.values.storable._
import org.neo4j.values.{AnyValue, Equality}

abstract sealed class ComparablePredicate(val left: Expression, val right: Expression) extends Predicate {

  def comparator: (AnyValue, AnyValue) => Value

  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val l = left(m, state)
    val r = right(m, state)
    comparator(l, r) match {
      case Values.TRUE => Some(true)
      case Values.FALSE => Some(false)
      case Values.NO_VALUE => None
    }
  }

  def sign: String

  override def toString: String = left.toString() + " " + sign + " " + right.toString()

  def containsIsNull = false

  def arguments = Seq(left, right)

  def symbolTableDependencies: Set[String] = left.symbolTableDependencies ++ right.symbolTableDependencies

  def other(e: Expression): Expression = if (e != left) {
    assert(e == right, "This expression is neither LHS nor RHS")
    left
  } else {
    right
  }
}

case class Equals(a: Expression, b: Expression) extends Predicate {

  def other(x: Expression): Option[Expression] = {
    if (x == a) Some(b)
    else if (x == b) Some(a)
    else None
  }

  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val l = a(m, state)
    val r = b(m, state)

    l.ternaryEquals(r) match {
      case Equality.UNDEFINED => None
      case Equality.FALSE => Some(false)
      case Equality.TRUE => Some(true)
    }
  }

  override def toString = s"$a == $b"

  def containsIsNull: Boolean = (a, b) match {
    case (Variable(_), Literal(null)) => true
    case _ => false
  }

  def rewrite(f: Expression => Expression) = f(Equals(a.rewrite(f), b.rewrite(f)))

  def arguments = Seq(a, b)

  def symbolTableDependencies: Set[String] = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class LessThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.lessThan

  def sign: String = "<"

  def rewrite(f: Expression => Expression) = f(LessThan(a.rewrite(f), b.rewrite(f)))
}

case class GreaterThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.greaterThan

  def sign: String = ">"

  def rewrite(f: Expression => Expression) = f(GreaterThan(a.rewrite(f), b.rewrite(f)))
}

case class LessThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.lessThanOrEqual

  def sign: String = "<="

  def rewrite(f: Expression => Expression) = f(LessThanOrEqual(a.rewrite(f), b.rewrite(f)))
}

case class GreaterThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.greaterThanOrEqual

  def sign: String = ">="

  def rewrite(f: Expression => Expression) = f(GreaterThanOrEqual(a.rewrite(f), b.rewrite(f)))
}
