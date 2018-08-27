/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_4

import org.neo4j.cypher.internal.ir.v3_5.{PlannerQuery, QueryGraph, QueryHorizon, RequiredOrder}
import org.neo4j.cypher.internal.ir.{v3_4 => irV3_4, v3_5 => irv3_5}

class PlannerQueryWrapper(pq: irV3_4.PlannerQuery) extends irv3_5.PlannerQuery {
  override val queryGraph = null
  override val requiredOrder = null
  override val horizon = null
  override val tail = null
  override def dependencies = ???
  override protected def copy(queryGraph: QueryGraph, requiredOrder: RequiredOrder, horizon: QueryHorizon, tail: Option[PlannerQuery]) = ???
  override lazy val readOnly = pq.readOnly
}
