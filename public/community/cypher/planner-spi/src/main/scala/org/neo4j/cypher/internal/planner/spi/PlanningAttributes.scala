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
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen

object PlanningAttributes {
  class Solveds extends Attribute[LogicalPlan, PlannerQueryPart]
  class Cardinalities extends Attribute[LogicalPlan, Cardinality]
  class ProvidedOrders extends Attribute[LogicalPlan, ProvidedOrder] {
    def hasProvidedOrder(id: Id): Boolean = if (isDefinedAt(id)) !get(id).isEmpty else false
  }
}

case class PlanningAttributes(solveds: Solveds, cardinalities: Cardinalities, providedOrders: ProvidedOrders) {
  def asAttributes(idGen: IdGen): Attributes[LogicalPlan] = Attributes[LogicalPlan](idGen, solveds, cardinalities, providedOrders)

  def copy() : PlanningAttributes =
    PlanningAttributes(solveds.copyTo(new Solveds()), cardinalities.copyTo(new Cardinalities()), providedOrders.copyTo(new ProvidedOrders()))

  def hasEqualSizeAttributes: Boolean = {
    providedOrders.size == cardinalities.size && providedOrders.size == solveds.size
  }
}
