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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId

/**
 * For each nodeId in 'nodeIds', fetch the corresponding node. Produce one row with the contents of argument and
 * the node (assigned to 'idName').
 */
case class NodeByIdSeek(idName: String, nodeIds: SeekableArgs, argumentIds: Set[String])(implicit idGen: IdGen) extends NodeLogicalLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds + idName

  override def usedVariables: Set[String] = nodeIds.expr.dependencies.map(_.name)

  override def withoutArgumentIds(argsToExclude: Set[String]): NodeByIdSeek = copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))
}
