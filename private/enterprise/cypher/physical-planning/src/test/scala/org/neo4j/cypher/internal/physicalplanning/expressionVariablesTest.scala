/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v4_0.VarPatternLength
import org.neo4j.cypher.internal.physicalplanning.ast.ExpressionVariable
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, _}
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.parser.Expressions
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, topDown}
import org.parboiled.scala.{ReportingParseRunner, Rule1}

//noinspection NameBooleanParameters
class expressionVariablesTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  val exprParser = new ExpressionParser

  private val semanticTable = SemanticTable()

  test("should noop for regular variable") {
    // given
    val plan = Selection(List(varFor("x")), Argument())

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    newPlan should be(plan)
  }

  test("should replace expression variable") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | x + 1]")
    val plan = projectPlan(expr)

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    newPlan should be(projectPlan(withExpressionVariables(expr, ExpressionVariable(0, "x"))))
  }

  test("should replace independent expression variables") {
    // given
    val exprX = exprParser.parse("[ x IN [1,2,3] | x + 1]")
    val exprY = exprParser.parse("[ y IN [1,2,3] | y + 1]")
    val exprZ = exprParser.parse("[ z IN [1,2,3] | z + 1]")
    val plan = projectPlan(exprX, exprY, exprZ)

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    newPlan should be(projectPlan(
      withExpressionVariables(exprX, ExpressionVariable(0, "x")),
      withExpressionVariables(exprY, ExpressionVariable(0, "y")),
      withExpressionVariables(exprZ, ExpressionVariable(0, "z"))
    ))
  }

  test("should replace independent expression variables II") {
    // given
    val expr = exprParser.parse("[ x IN [ y IN [1,2,3] | y + 1] | x + 2]")
    val plan = projectPlan(expr)

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "x"),
                                                          ExpressionVariable(0, "y"))))
  }

  test("should replace nested expression variables") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | [ y IN [1,2,3] | y + x ] ]")
    val plan = projectPlan(expr)

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "x"),
                                                          ExpressionVariable(1, "y"))))
  }

  test("should replace independent nested expression variables") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | [ y IN [1,2,3] | y + x ] ++ [ z IN [1,2,3] | z + x ] ]")
    val plan = projectPlan(expr)

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "x"),
                                                          ExpressionVariable(1, "y"),
                                                          ExpressionVariable(1, "z"))))
  }

  test("should replace both reduce expression variables") {
    // given
    val expr = exprParser.parse("reduce(acc = 0, x IN [1,2,3] | acc + x ]")
    val plan = projectPlan(expr)

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "acc"),
                                                          ExpressionVariable(1, "x"))))
  }

  test("should replace var-length expression variables") {
    // given
    val nodePred = exprParser.parse("reduce(acc = true, z IN tempNode.prop | acc && z )")
    val edgePred = exprParser.parse("tempEdge = true")
    val plan = varLengthPlan(varFor("tempNode"), varFor("tempEdge"), nodePred, edgePred)

    // when
    val (newPlan, _) = expressionVariables.replace(plan, semanticTable)

    // then
    val tempNode = ExpressionVariable(0, "tempNode")
    val tempEdge = ExpressionVariable(1, "tempEdge")
    newPlan should be(varLengthPlan(tempNode,
                                    tempEdge,
                                    withExpressionVariables(nodePred,
                                                            tempNode,
                                                            ExpressionVariable(2, "acc"),
                                                            ExpressionVariable(3, "z")),
                                    withExpressionVariables(edgePred,
                                                            tempEdge)
                      ))
  }

  private def projectPlan(exprs: Expression*): LogicalPlan = {
    val projections = (for (i <- exprs.indices) yield s"x$i" -> exprs(i)).toMap
    Projection(Argument(), projections)
  }

  private def varLengthPlan(tempNode: LogicalVariable, tempEdge: LogicalVariable, nodePred: Expression, edgePred: Expression): LogicalPlan = {
    VarExpand(Argument(),
              "a",
              SemanticDirection.OUTGOING,
              SemanticDirection.OUTGOING,
              Seq.empty,
              "b",
              "r",
              VarPatternLength(2, Some(10)),
              ExpandAll,
              tempNode,
              tempEdge,
              nodePred,
              edgePred,
              Seq.empty)
  }

  private def withExpressionVariables(expression: Expression, exprVars: ExpressionVariable*): Expression = {
    expression.endoRewrite(topDown( Rewriter.lift {
      case x: LogicalVariable =>
        exprVars.find(_.name == x.name) match {
          case Some(exprVar) => exprVar
          case None => x
        }
    }))
  }
}

class ExpressionParser extends Expressions {
  private val parser: Rule1[Expression] = Expression

  def parse(text: String): Expression = {
    val res = ReportingParseRunner(parser).run(text)
    res.result match {
      case Some(e) => e
      case None => throw new IllegalArgumentException(s"Could not parse expression: ${res.parseErrors}")
    }
  }
}
