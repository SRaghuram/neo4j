/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.v3_3.logical.plans

import org.neo4j.cypher.internal.frontend.v3_3.symbols.{CypherType, _}
import org.neo4j.cypher.internal.ir.v3_3.{CardinalityEstimation, PlannerQuery}

// Argument is used inside of an Apply to feed the row from the LHS of the Apply to the leaf of the RHS
case class Argument(argumentIds: Set[String])(val solved: PlannerQuery with CardinalityEstimation)
                    (val typeInfo: Map[String, CypherType] = argumentIds.map( id => id -> CTNode).toMap)
  extends LogicalLeafPlan {

  val availableSymbols = argumentIds

  override def updateSolved(newSolved: PlannerQuery with CardinalityEstimation) =
    copy(argumentIds)(newSolved)(typeInfo)

  override def copyPlan(): LogicalPlan = this.copy(argumentIds)(solved)(typeInfo).asInstanceOf[this.type]

  override def dup(children: Seq[AnyRef]) = children.size match {
    case 1 =>
      copy(children.head.asInstanceOf[Set[String]])(solved)(typeInfo).asInstanceOf[this.type]
  }
}
