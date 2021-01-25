/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.Duration

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.CacheCounts
import org.neo4j.cypher.ExecutionEngineCacheCounter
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.query.QuerySubscriber.DO_NOTHING_SUBSCRIBER
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.AssertableLogProvider.Level
import org.neo4j.logging.LogAssertions.assertThat
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

class CypherCompilerExecutionEngineQueryCacheMonitoringAcceptanceTest extends ExecutionEngineFunSuite {

  override def databaseConfig(): Map[Setting[_],Object] = super.databaseConfig() ++
    Map(GraphDatabaseSettings.cypher_min_replan_interval -> Duration.ZERO,
      GraphDatabaseInternalSettings.cypher_expression_recompilation_limit -> Integer.valueOf(2)
    )

  test("should monitor cache miss") {
    // given
    val counter = new ExecutionEngineCacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(misses = 1, flushes = 1, compilations = 1))
  }

  test("should monitor cache misses and hits") {
    // given
    val counter = new ExecutionEngineCacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(hits = 1, misses = 1, flushes = 1, compilations = 1))
  }

  test("should monitor compilations with expression code generation") {
    // given
    val counter = new ExecutionEngineCacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList
    execute("return 42").toList
    execute("return 42").toList
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(hits = 3, misses = 1, flushes = 1, compilations = 1, compilationsWithExpressionCodeGen = 1))
  }

  test("should monitor cache flushes") {
    // given
    val counter = new ExecutionEngineCacheCounter()
    kernelMonitors.addMonitorListener(counter)

    // when
    execute("return 42").toList
    execute("create constraint on (n:Person) assert n.id is unique").toList
    execute("return 42").toList

    // then
    counter.counts should equal(CacheCounts(misses = 3, flushes = 2, compilations = 3))
  }

  test("should monitor cache evictions") {
    // given
    val counter = new ExecutionEngineCacheCounter()
    kernelMonitors.addMonitorListener(counter)
    val query = "match (n:Person:Dog) return n"

    createLabeledNode("Dog")
    (0 until 50).foreach { _ => createLabeledNode("Person") }
    execute(query).toList

    // when
    (0 until 1000).foreach { _ => createLabeledNode("Dog") }
    execute(query).toList

    // then
    counter.counts should equal(CacheCounts(misses = 2, flushes = 1, evicted = 1, compilations = 2))
  }

  override lazy val logProvider: AssertableLogProvider = new AssertableLogProvider()

  test("should log on cache evictions") {
    // given
    val engine = createEngine(graph)
    val counter = new ExecutionEngineCacheCounter()
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

    assertThat(logProvider).forClass(classOf[ExecutionEngine]).forLevel(Level.INFO)
      .containsMessages("Discarded stale query from the query cache", query)
  }
}

