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
package org.neo4j.cypher.internal

sealed abstract class RuntimeName {
  def name: String
  def toTextOutput: String = name
}

case object InterpretedRuntimeName extends RuntimeName {
  override val name = "INTERPRETED"
}

case object SlottedRuntimeName extends RuntimeName {
  override val name = "SLOTTED"
}

case object MorselRuntimeName extends RuntimeName {
  override val name = "MORSEL"
}

case object CompiledRuntimeName extends RuntimeName {
  override val name = "COMPILED"
}

case object ProcedureRuntimeName extends RuntimeName {
  override val name = "PROCEDURE"
}

case object SystemCommandRuntimeName extends RuntimeName {
  override val name = "SYSTEM"
}

object RuntimeName {

  def apply(name: String): RuntimeName = name.toUpperCase match {
    case InterpretedRuntimeName.name => InterpretedRuntimeName
    case SlottedRuntimeName.name => SlottedRuntimeName
    case MorselRuntimeName.name => MorselRuntimeName
    case CompiledRuntimeName.name => CompiledRuntimeName
    case ProcedureRuntimeName.name => ProcedureRuntimeName
    case SystemCommandRuntimeName.name => SystemCommandRuntimeName

    case n => throw new IllegalArgumentException(s"$n is not a valid runtime")
  }
}
