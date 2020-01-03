/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.Duration

import org.hamcrest.Matchers
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.{ExecutionEngine, StringCacheMonitor}
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.helpers.collection.Pair
import org.neo4j.kernel.impl.query.QuerySubscriber.DO_NOTHING_SUBSCRIBER
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

import scala.collection.Map

class CypherCompilerStringCacheMonitoringAcceptanceTest extends ExecutionEngineFunSuite {

  case class CacheCounts(hits: Int = 0, misses: Int = 0, flushes: Int = 0, evicted: Int = 0) {
    override def toString = s"hits = $hits, misses = $misses, flushes = $flushes, evicted = $evicted"
  }

  class CacheCounter(var counts: CacheCounts = CacheCounts()) extends StringCacheMonitor {
    override def cacheMiss(key: Pair[String, ParameterTypeMap]) {
      counts = counts.copy(misses = counts.misses + 1)
    }

    override def cacheHit(key: Pair[String, ParameterTypeMap]) {
      counts = counts.copy(hits = counts.hits + 1)
    }

    override def cacheFlushDetected(sizeBeforeFlush: Long) {
      counts = counts.copy(flushes = counts.flushes + 1)
    }

    override def cacheDiscard(key: Pair[String, ParameterTypeMap], key2: String, secondsSinceReplan: Int) {
      counts = counts.copy(evicted = counts.evicted + 1)
    }
  }

  override def databaseConfig(): Map[Setting[_],Object] = Map(GraphDatabaseSettings.cypher_min_replan_interval -> Duration.ZERO)

  test("should monitor cache miss") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(misses = 1, flushes = 1))
  }

  test("should monitor cache misses and hits") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1))
  }

  test("should monitor cache flushes") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList
    execute("create constraint on (n:Person) assert n.id is unique").toList
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(misses = 3, flushes = 2))
  }

  test("should monitor cache evictions") {
    // given
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)
    val query = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    execute(query).toList

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    execute(query).toList

    // then
    counter.counts should equal(CacheCounts(misses = 2, flushes = 1, evicted = 1))
  }

  override lazy val logProvider: AssertableLogProvider = new AssertableLogProvider()

  test("should log on cache evictions") {
    // given
    val engine = createEngine(graph)
    val counter = new CacheCounter()
    kernelMonitors.addMonitorListener(counter)
    val query = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    graph.withTx{ tx =>
      val result1 = engine.execute(query,
        EMPTY_MAP,
        graph.transactionalContext(tx, query = query -> Map.empty),
        profile = false,
        prePopulate = false,
        DO_NOTHING_SUBSCRIBER)
      result1.consumeAll()
    }

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    graph.withTx { tx =>
      val result2 = engine.execute(query,
        EMPTY_MAP,
        graph.transactionalContext(tx, query = query -> Map.empty),
        profile = false,
        prePopulate = false,
        DO_NOTHING_SUBSCRIBER)
      result2.consumeAll()
    }

    logProvider.assertAtLeastOnce(

      AssertableLogProvider
        .inLog(classOf[ExecutionEngine])
        .info(
          Matchers.allOf[String](
            Matchers.containsString("Discarded stale query from the query cache"),
            Matchers.containsString(query)
          )
        )
      )
  }
}

