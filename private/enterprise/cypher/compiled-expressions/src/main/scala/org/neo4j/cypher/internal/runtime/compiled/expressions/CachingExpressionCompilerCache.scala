/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache

class CachingExpressionCompilerCache(cacheFactory: CaffeineCacheFactory) {
  val SIZE = 1000
  val expressionsCache = new LFUCache[IntermediateExpression, CompiledExpression](cacheFactory, SIZE)
  val projectionsCache = new LFUCache[IntermediateExpression, CompiledProjection](cacheFactory, SIZE)
  val groupingsCache = new LFUCache[IntermediateGroupingExpression, CompiledGroupingExpression](cacheFactory, SIZE)
}
