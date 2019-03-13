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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.v4_0.util.ParameterNotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.MapValue

object createParameterArray extends ((MapValue, Map[String,Int]) => Array[AnyValue]) {

  override def apply(params: MapValue,
                     parameterMapping: Map[String, Int]): Array[AnyValue] = {
    val parameterArray = new Array[AnyValue](parameterMapping.size)
    parameterMapping.foreach {
      case (key, slot) =>
        val value = params.get(key)
        if ((value eq NO_VALUE) && !params.containsKey(key)) throw new ParameterNotFoundException("Expected a parameter named " + key)
        parameterArray(slot) = value
    }
    parameterArray
  }
}
