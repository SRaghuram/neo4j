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

import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId

/**
 * This operator does a full scan of an index, producing two rows per entry.
 *
 * Given each found `relationship`, the rows will have the following structure:
 *
 *  - `{idName: relationship, leftNode: relationship.startNode, relationship.endNode}`
 *  - `{idName: relationship, leftNode: relationship.endNode, relationship.startNode}`
 */
case class UndirectedRelationshipIndexScan(idName: String,
                                           leftNode: String,
                                           rightNode: String,
                                           typeToken: RelationshipTypeToken,
                                           properties: Seq[IndexedProperty],
                                           argumentIds: Set[String],
                                           indexOrder: IndexOrder)
                                          (implicit idGen: IdGen)
  extends RelationshipIndexLeafPlan(idGen) {

  override val availableSymbols: Set[String] = argumentIds ++ Set(idName, leftNode, rightNode)

  override def usedVariables: Set[String] = Set.empty

  override def withoutArgumentIds(argsToExclude: Set[String]): UndirectedRelationshipIndexScan = copy(argumentIds = argumentIds -- argsToExclude)(SameId(this.id))

  override def copyWithoutGettingValues: UndirectedRelationshipIndexScan =
    UndirectedRelationshipIndexScan(idName, leftNode, rightNode, typeToken, properties.map{ p => IndexedProperty(p.propertyKeyToken, DoNotGetValue) }, argumentIds, indexOrder)(SameId(this.id))

  override def withMappedProperties(f: IndexedProperty => IndexedProperty): UndirectedRelationshipIndexScan =
    UndirectedRelationshipIndexScan(idName, leftNode, rightNode, typeToken, properties.map(f), argumentIds, indexOrder)(SameId(this.id))
}
