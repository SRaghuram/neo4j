/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.cache

import com.neo4j.fabric.planning.FabricPlan
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.values.virtual.MapValue

class FabricQueryCache(size: Int) {

  type Query = String
  type Params = MapValue
  type ParamTypes = Map[String, Class[_]]
  type DefaultContextName = String

  type Key = (Query, ParamTypes, DefaultContextName)
  type Value = FabricPlan

  private val cache = new LFUCache[Key, Value](size)

  private var hits: Long = 0
  private var misses: Long = 0

  def computeIfAbsent(query: Query, params: Params, defaultContextName: DefaultContextName, compute: () => FabricPlan): FabricPlan = {
    val paramTypes = QueryCache.extractParameterTypeMap(params)
    val key = (query, paramTypes, defaultContextName)
    cache.get(key) match {
      case None =>
        val result = compute()
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
