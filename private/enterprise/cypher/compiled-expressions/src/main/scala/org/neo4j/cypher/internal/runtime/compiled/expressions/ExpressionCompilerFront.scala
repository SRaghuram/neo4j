/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.util.attribution.Id

/**
  * Compiles AST expressions into [[IntermediateExpression]]s.
  */
trait ExpressionCompilerFront {

  /**
   * Compiles the given grouping keys to an instance of [[IntermediateExpression]]
   *
   * @param orderedGroupings the groupings to compile, already sorted in correct grouping key order
   * @param id               the id of the operator calling this
   * @return an instance of [[IntermediateGroupingExpression]] corresponding to the provided groupings
   */
  def compileGroupingKey(orderedGroupings: Seq[Expression], id: Id): Option[IntermediateExpression]

  /**
   * Compiles the given expression to an [[IntermediateExpression]]
   *
   * @param expression the expression to compile
   * @param id         the id of the operator calling this
   * @return an [[IntermediateExpression]] corresponding to the provided expression.
   */
  def compileExpression(expression: Expression, id: Id): Option[IntermediateExpression]

  /**
   * Compiles the given projections to an [[IntermediateExpression]]
   *
   * @param projections the projections to compile
   * @param id          the id of the operator calling this
   * @return an [[IntermediateExpression]] corresponding to the provided projections.
   */
  def compileProjection(projections: Map[String, Expression], id: Id): Option[IntermediateExpression]

  /**
   * Compiles the given ordered groupings to an [[IntermediateGroupingExpression]]
   *
   * @param orderedGroupings the projections to compile
   * @param keyName          name of the grouping key variable
   * @param id               the id of the operator calling this
   * @return an [[IntermediateGroupingExpression]] corresponding to the provided ordered grouping.
   */
  def compileGroupingExpression(orderedGroupings: Seq[(Slot, IntermediateExpression)], keyName: String, id: Id): IntermediateGroupingExpression
}
