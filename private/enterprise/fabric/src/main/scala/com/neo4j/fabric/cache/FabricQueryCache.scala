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
package com.neo4j.fabric.cache

import com.neo4j.fabric.planning.FabricQuery
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.values.virtual.MapValue

class FabricQueryCache {

  type Query = String
  type ParamTypes = Map[String, Class[_]]
  type Key = (Query, ParamTypes)

  private val cache = new LFUCache[Key, FabricQuery](100)

  private var hits: Long = 0
  private var misses: Long = 0

  def computeIfAbsent(query: String, params: MapValue, compute: (String, MapValue) => FabricQuery): FabricQuery = {
    val paramTypes = QueryCache.extractParameterTypeMap(params)
    val key = (query, paramTypes)
    cache.get(key) match {
      case None =>
        val result = compute(query, params)
        cache.put(key, result)
        misses += 1
        result

      case Some(result) =>
        hits += 1
        result
    }
  }

  def getHits: Long = hits

  def getMisses: Long = misses
}
