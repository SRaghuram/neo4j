/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher

sealed abstract class CypherInterpretedPipesFallbackOption(mode: String) extends CypherOption(mode)

case object CypherInterpretedPipesFallbackOption extends CypherOptionCompanion[CypherInterpretedPipesFallbackOption] {
  case object disabled extends CypherInterpretedPipesFallbackOption("disabled")
  case object whitelistedPlansOnly extends CypherInterpretedPipesFallbackOption("whitelisted_plans_only")
  case object allPossiblePlans extends CypherInterpretedPipesFallbackOption("all_possible_plans")

  val all: Set[CypherInterpretedPipesFallbackOption] = Set(disabled, whitelistedPlansOnly, allPossiblePlans)
  override val default: CypherInterpretedPipesFallbackOption = whitelistedPlansOnly
}
