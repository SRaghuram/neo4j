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

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case class relationshipTypeScanLeafPlanner(skipIDs: Set[String]) extends LeafPlanner {

  def apply(queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    def shouldIgnore(pattern: PatternRelationship) =
      queryGraph.argumentIds.contains(pattern.name) ||
      skipIDs.contains(pattern.name)

    queryGraph.patternRelationships.flatMap {

      //(a)-[:R]->(b)
      case p@PatternRelationship(name, (startNode, endNode), OUTGOING, Seq(typ), SimplePatternLength) if !shouldIgnore(p) =>
        Some(context.logicalPlanProducer.planDirectedRelationshipByTypeScan(name, startNode, typ, endNode, p, queryGraph.argumentIds, context))

      //(a)<-[:R]-(b)
      case p@PatternRelationship(name, (startNode, endNode), INCOMING, Seq(typ), SimplePatternLength) if !shouldIgnore(p) =>
        Some(context.logicalPlanProducer.planDirectedRelationshipByTypeScan(name, endNode, typ, startNode, p, queryGraph.argumentIds, context))

      //(a)-[:R]-(b)
      case p@PatternRelationship(name, (startNode, endNode), BOTH, Seq(typ), SimplePatternLength) if !shouldIgnore(p) =>
        Some(context.logicalPlanProducer.planUndirectedRelationshipByTypeScan(name, startNode, typ, endNode, p, queryGraph.argumentIds, context))

      case _ => None
    }.toIndexedSeq
  }

}
