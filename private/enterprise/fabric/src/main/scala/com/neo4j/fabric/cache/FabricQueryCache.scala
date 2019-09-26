/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
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
