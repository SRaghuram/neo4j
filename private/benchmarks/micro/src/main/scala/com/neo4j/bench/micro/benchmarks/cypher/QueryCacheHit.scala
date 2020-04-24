/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.nio.file.Paths
import java.time.Clock

import com.neo4j.bench.common.profiling.ParameterizedProfiler
import com.neo4j.bench.common.profiling.ProfilerType
import com.neo4j.bench.common.util.ErrorReporter
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark
import com.neo4j.bench.micro.benchmarks.cypher.QueryCacheHitThreadState.CacheValue
import com.neo4j.bench.micro.benchmarks.cypher.QueryCacheHitThreadState.DataPoint
import org.neo4j.cypher.internal.CacheLookup
import org.neo4j.cypher.internal.CacheTracer
import org.neo4j.cypher.internal.CacheabilityInfo
import org.neo4j.cypher.internal.FineToReuse
import org.neo4j.cypher.internal.PlanStalenessCaller
import org.neo4j.cypher.internal.QueryCache
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.internal.helpers.collection.Pair
import org.neo4j.logging.NullLog
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.runner.options.TimeValue

@BenchmarkEnabled(true)
class QueryCacheHit extends BaseRegularBenchmark {

  override def description = "Query cache hit"

  override def benchmarkGroup = "Cypher"

  override def isThreadSafe = false

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def queryCacheHit(threadState: QueryCacheHitThreadState): CacheLookup[CacheValue] = {
    threadState.nextCacheHit()
  }
}

@State(Scope.Thread)
class QueryCacheHitThreadState {
  var queryCache: QueryCache[String, Pair[String, ParameterTypeMap], CacheValue] = _
  var index: Int = 0

  @Setup
  def setUp(benchmarkState: QueryCacheHit): Unit = {
    queryCache =
      new QueryCache(
        QueryCacheHitThreadState.dataPoints.length,
        new PlanStalenessCaller(
          Clock.systemDefaultZone(),
          StatsDivergenceCalculator.divergenceNoDecayCalculator(0.5, 1009),
          () => 42L,
          (_, _) => FineToReuse,
          NullLog.getInstance()
        ),
        QueryCacheHitThreadState.NO_TRACER
      )

    QueryCacheHitThreadState.dataPoints.map(lookupDataPoint)
  }

  def nextCacheHit(): CacheLookup[CacheValue] = {
    index += 1
    if (index >= QueryCacheHitThreadState.dataPoints.length) {
      index = 0
    }
    lookupDataPoint(QueryCacheHitThreadState.dataPoints(index))
  }

  private def lookupDataPoint(dataPoint: DataPoint): CacheLookup[CacheValue] = {
    val parameterTypeMap = QueryCache.extractParameterTypeMap(dataPoint.params)
    queryCache.computeIfAbsentOrStale(
      Pair.of(dataPoint.query, parameterTypeMap),
      null,
      () => CacheValue(dataPoint.query.toLowerCase),
      i => None
    )
  }

  @TearDown
  def tearDown(): Unit = {
    queryCache = null
  }
}

object QueryCacheHitThreadState {
  val NO_TRACER: CacheTracer[Pair[String, ParameterTypeMap]] = new CacheTracer[Pair[String, ParameterTypeMap]] {
    override def queryCacheHit(queryKey: Pair[String, ParameterTypeMap],
                               metaData: String): Unit = {}
    override def queryCacheMiss(queryKey: Pair[String, ParameterTypeMap], metaData: String): Unit = {}
    override def queryCacheRecompile(queryKey: Pair[String, ParameterTypeMap], metaData: String): Unit = {}
    override def queryCacheStale(queryKey: Pair[String, ParameterTypeMap], secondsSincePlan: Int, metaData: String, maybeReason: Option[String]): Unit = {}
    override def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {}
  }

  case class DataPoint(query: String, params: MapValue)
  val dataPoints: Array[DataPoint] = {
    val randomValues = RandomValues.create(new java.util.Random(12345))
    (0 until 1000).map(i => {
      val query = randomValues.nextTextValue().stringValue()
      val paramKeys = randomValues.nextStringArrayRaw(0, 10, 1, 14)
      val params = VirtualValues.map(paramKeys, paramKeys.map(key => randomValues.nextNumberValue()))
      DataPoint(query, params)
    }).toArray
  }

  case class CacheValue(query: String) extends CacheabilityInfo {
    override def shouldBeCached: Boolean = true
    override def notifications: Set[InternalNotification] = Set.empty
  }

  def main(args: Array[String]): Unit = {
    val storesDir = Paths.get( "benchmark_stores" );
    val profilerRecordingsOutputDir = Paths.get( "profiler_recordings" );
    val forkCount = 1;
    val profilers = ParameterizedProfiler.defaultProfilers();
    val errorPolicy = ErrorReporter.ErrorPolicy.FAIL;
    val jvmFile = null;
    Main.run( classOf[QueryCacheHit],
      forkCount,
      2,
      TimeValue.seconds(3),
      profilers,
      storesDir,
      profilerRecordingsOutputDir,
      errorPolicy,
      jvmFile );
  }
}
