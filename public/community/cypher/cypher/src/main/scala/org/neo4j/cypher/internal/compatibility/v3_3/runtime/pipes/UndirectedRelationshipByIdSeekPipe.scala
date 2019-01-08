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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters._

case class UndirectedRelationshipByIdSeekPipe(ident: String, relIdExpr: SeekArgs, toNode: String, fromNode: String)
                                             (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends Pipe {

  relIdExpr.registerOwningPipe(this)

  protected override def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val ctx = state.initialContext.getOrElse(ExecutionContext.empty)
    val relIds = VirtualValues.dropNoValues(relIdExpr.expressions(ctx, state))
    new UndirectedRelationshipIdSeekIterator(ident, fromNode, toNode, ctx, state.query.relationshipOps, relIds.iterator.asScala)
  }

}
