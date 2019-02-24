/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.ast.ExpressionVariable
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{LogicalVariable, ScopeExpression}
import org.neo4j.cypher.internal.v4_0.logical.plans.{LogicalPlan, VarExpand}
import org.neo4j.cypher.internal.v4_0.util.attribution.Attribute
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, topDown}

import scala.collection.mutable

object expressionVariables {

  class ExpressionSlots() extends Attribute[Int]

  def replace(lp: LogicalPlan, semanticTable: SemanticTable): (LogicalPlan, SemanticTable) = {

    val globalMapping = mutable.Map[String, Int]()

    lp.treeFold(0) {
      case x: ScopeExpression =>
        prevNumExpressionVariables => {
          var slot = prevNumExpressionVariables
          for (variable <- x.introducedVariables) {
            globalMapping += variable.name -> slot
            slot += 1
          }
          (slot, Some(_ => prevNumExpressionVariables))
        }

      case x: VarExpand =>
        prevNumExpressionVariables => {
          globalMapping += x.tempNode.name -> prevNumExpressionVariables
          globalMapping += x.tempEdge.name -> (prevNumExpressionVariables+1)
          (prevNumExpressionVariables+2, Some(_ => prevNumExpressionVariables))
        }
    }


    val rewriter =
      topDown( Rewriter.lift {
        case x: LogicalVariable if globalMapping.contains(x.name) =>
          ExpressionVariable(globalMapping(x.name), x.name)
      })

    (lp.endoRewrite(rewriter), semanticTable)
  }
}
