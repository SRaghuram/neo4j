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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.values.storable.{NumberValue, Values}

case class Pow(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def calc(a: NumberValue, b: NumberValue) = Values.doubleValue(math.pow(a.doubleValue(), b.doubleValue()))

  def rewrite(f: (Expression) => Expression) = f(Pow(a.rewrite(f), b.rewrite(f)))

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies

  def throwTypeError(aType: String, bType: String): Nothing = {
    throw new CypherTypeException("Cannot raise `" + aType + "` to the power of `" + bType + "`")

  }
}
