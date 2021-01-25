/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.CollectAllFunction
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.memory.MemoryTracker

case class CollectAll(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  override def createAggregationFunction(memoryTracker: MemoryTracker): AggregationFunction = {
    memoryTracker.allocateHeap(CollectAllFunction.SHALLOW_SIZE)
    new CollectAllFunction(anInner, memoryTracker)
  }

  override val expectedInnerType: CypherType = CTAny

  override def rewrite(f: Expression => Expression): Expression = f(CollectAll(anInner.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(anInner)
}
