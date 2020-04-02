/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.util.InputPosition

object QueryRenderer {

  private object clauseExtension extends Prettifier.ClausePrettifier {
    override def asString(ctx: Prettifier.QueryPrettifier): PartialFunction[Clause, String] = {
      case rc: ResolvedCall => ctx.asString(Ast.unresolvedCall(rc))
    }
  }

  private object exprExtension extends ExpressionStringifier.Extension {
    override def apply(ctx: ExpressionStringifier)(expression: Expression): String = expression match {
      case rf: ResolvedFunctionInvocation => ctx.apply(Ast.unresolvedFunction(rf))
    }
  }

  private val expressionsPretty = ExpressionStringifier(extension = exprExtension, alwaysParens = false, alwaysBacktick = false, preferSingleQuotes = false)
  private val expressionsStrict = ExpressionStringifier(extension = exprExtension, alwaysParens = true, alwaysBacktick = true, preferSingleQuotes = false)
  private val renderer = Prettifier(expressionsStrict, extension = clauseExtension)

  private val pos = InputPosition.NONE

  private val NL = System.lineSeparator()

  def render(clauses: Seq[Clause]): String =
    render(Query(None, SingleQuery(clauses)(pos))(pos))

  def render(statement: Statement): String =
    renderer.asString(statement)

  def render(statement: Statement, options: QueryOptions): String = {
    val cypher = options.render.map(_ + NL).getOrElse("")
    val query = renderer.asString(statement)
    cypher + query
  }

  def pretty(expression: Expression): String =
    expressionsPretty.apply(expression)
}
