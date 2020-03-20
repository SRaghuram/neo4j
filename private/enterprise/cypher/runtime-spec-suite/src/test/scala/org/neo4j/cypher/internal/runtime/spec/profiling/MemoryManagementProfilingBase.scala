/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.profiling

import java.lang.Boolean.TRUE
import java.nio.file.Files
import java.nio.file.Path

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings
import com.neo4j.kernel.impl.query.HeapDumper
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.EnterpriseRuntimeContextManager
import org.neo4j.cypher.internal.PipelinedRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.RuntimeEnvironment
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.DEFAULT_HEAP_DUMP_INTERVAL
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.DEFAULT_INPUT_LIMIT
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.HEAP_DUMP_ENABLED
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.HEAP_DUMP_PATH
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.LOG_HEAP_DUMP_ACTIVITY
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.OVERWRITE_EXISTING_HEAP_DUMPS
import org.neo4j.cypher.internal.runtime.spec.tests.InputStreams
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.kernel.api.Kernel
import org.neo4j.scheduler.JobScheduler

import scala.util.Random

object MemoryManagementProfilingBase {
  // The configured max memory per transaction in Bytes
  val maxMemory: Long = Long.MaxValue

  val DEFAULT_MORSEL_SIZE_BIG: Int = GraphDatabaseSettings.cypher_pipelined_batch_size_big.defaultValue()
  val DEFAULT_MORSEL_SIZE_SMALL: Int = GraphDatabaseSettings.cypher_pipelined_batch_size_small.defaultValue()

  def WITH_FUSING(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseSettings.cypher_operator_engine -> GraphDatabaseSettings.CypherOperatorEngine.COMPILED)

  // Global heap dump settings
  val HEAP_DUMP_ENABLED: Boolean = true
  val HEAP_DUMP_PATH: String = "/home/henym/debug/memory/dumps/runtime"
  val DEFAULT_INPUT_LIMIT: Long = 1000000L
  val DEFAULT_HEAP_DUMP_INTERVAL: Long = 250000L
  val OVERWRITE_EXISTING_HEAP_DUMPS: Boolean = false
  val LOG_HEAP_DUMP_ACTIVITY: Boolean = true

  // Edition
  private val profilingEdition = new Edition[EnterpriseRuntimeContext](
    () => new TestEnterpriseDatabaseManagementServiceBuilder(),
    (runtimeConfig, resolver, lifeSupport, logProvider) => {
      val kernel = resolver.resolveDependency(classOf[Kernel])
      val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
      val workerManager = resolver.resolveDependency(classOf[WorkerManagement])

      val runtimeEnvironment = RuntimeEnvironment.of(runtimeConfig, jobScheduler, kernel.cursors(), lifeSupport, workerManager)

      EnterpriseRuntimeContextManager(
        GeneratedQueryStructure,
        logProvider.getLog("test"),
        runtimeConfig,
        runtimeEnvironment
      )
    },
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(-1),
    GraphDatabaseSettings.cypher_operator_engine -> GraphDatabaseSettings.CypherOperatorEngine.COMPILED,
    MetricsSettings.metricsEnabled -> java.lang.Boolean.FALSE
  )

//  def WITH_MORSEL_SIZE(morselSize: Int, edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
//    edition.copyWith(GraphDatabaseSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(morselSize),
//                     GraphDatabaseSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(morselSize))

  val ENTERPRISE_PROFILING: Edition[EnterpriseRuntimeContext] = profilingEdition
//  val MORSEL_SIZE_BIG: Edition[EnterpriseRuntimeContext] = WITH_MORSEL_SIZE(DEFAULT_MORSEL_SIZE_BIG, profilingEdition)
//  val MORSEL_SIZE_SMALL: Edition[EnterpriseRuntimeContext] = WITH_MORSEL_SIZE(DEFAULT_MORSEL_SIZE_SMALL, profilingEdition)
}

trait ProfilingInputStreams[CONTEXT <: RuntimeContext] extends InputStreams[CONTEXT] {
  self: RuntimeTestSuite[CONTEXT] =>

  /**
   * Finite iterator that creates periodic heap dumps at the given input row interval.
   *
   * @param data an array of at least one element of input row data that will be cycled through as input.
   *             E.g. limit=10, data=Array(Array(1), Array(2), Array(3)) will produce input rows containing
   *                  [1, 2, 3, 1, 2, 3, 1, 2, 3, 1]
   * @param limit the iterator will be exhausted after the given amount of rows
   * @param heapDumpInterval a heap dump will be created every time this number of input rows has been produced,
   *                         always including a final heap dump at the last row
   * @param heapDumpFileNamePrefix a full path and file name prefix for the heap dump files.
   *                               The number of input rows produced when the heap dump is taken, and an
   *                               appropriate file extension will be appended to form the full file path.
   */
  protected def finiteCyclicInputWithPeriodicHeapDump(data: Array[Array[Any]],
                                                      limit: Long,
                                                      heapDumpInterval: Long,
                                                      heapDumpFileNamePrefix: String): InputDataStream = {
    iteratorInput(iterateWithPeriodicHeapDump(data, Some(limit), heapDumpInterval, heapDumpFileNamePrefix))
  }

  /**
   * Create a heap-dumping iterator.
   *
   * @param data      Items from this will be returned in every call to `next` in a cyclic fashion. It is required to have at least one element
   * @param limit     if defined, the iterator will be exhausted after the given amount of rows
   * @param heapDumpInterval The number of rows after which a periodic heap dump will be created.
   * @param heapDumpFileNamePrefix The full path and file name prefix of the heap dump file.
   *                               The row count and file extension will be appended to form the complete file name.
   * @param heapDumpLiveObjectsOnly Only include objects that are in the live set of the heap.
   * @param rowSize   the size of a row in the operator under test. This value determines when to fail the test if the query is not killed soon enough.
   */
  protected def iterateWithPeriodicHeapDump(data: Array[Array[Any]],
                                            limit: Option[Long],
                                            heapDumpInterval: Long,
                                            heapDumpFileNamePrefix: String,
                                            heapDumpLiveObjectsOnly: Boolean = true,
                                            rowSize: Option[Long] = None): Iterator[Array[Any]] = new Iterator[Array[Any]] {
    private val killThreshold: Long = if (rowSize.isDefined) killAfterNRows(rowSize.get) else Long.MaxValue
    private var i = 0L
    private var j = 0L
    override def hasNext: Boolean = limit.fold(true)(i < _)

    override def next(): Array[Any] = {
      i += 1
      j += 1
      if (limit.isEmpty && i > killThreshold) {
        fail("The query was not killed even though it consumed too much memory.")
      }
      if (HEAP_DUMP_ENABLED && (j >= heapDumpInterval || (limit.isDefined && i == limit.get))) {
        val fileName = s"$heapDumpFileNamePrefix-$i.hprof"
        val path = Path.of(fileName)
        val alreadyExists = Files.exists(path)
        if (alreadyExists && OVERWRITE_EXISTING_HEAP_DUMPS) {
          if (LOG_HEAP_DUMP_ACTIVITY) println(s"""Overwriting existing heap dump "$fileName"""")
          Files.delete(path)
          HeapDumper.createHeapDump(fileName, heapDumpLiveObjectsOnly)
        } else if (alreadyExists) {
          if (LOG_HEAP_DUMP_ACTIVITY) println(s"""Skipping already existing heap dump "$fileName"""")
        } else {
          if (LOG_HEAP_DUMP_ACTIVITY) println(s"""Creating new heap dump "$fileName"""")
          HeapDumper.createHeapDump(fileName, heapDumpLiveObjectsOnly)
        }
        j = 0
      }
      data(((i - 1) % data.length).toInt)
    }
  }
}

abstract class MemoryManagementProfilingBase[CONTEXT <: EnterpriseRuntimeContext](
                                                                                   edition: Edition[CONTEXT],
                                                                                   runtime: CypherRuntime[CONTEXT],
                                                                                   morselSize: Int = MemoryManagementProfilingBase.DEFAULT_MORSEL_SIZE_BIG
                                                                                 )
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    GraphDatabaseSettings.track_query_allocation -> TRUE,
    GraphDatabaseSettings.query_max_memory -> Long.box(MemoryManagementProfilingBase.maxMemory),
    GraphDatabaseSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(morselSize),
    GraphDatabaseSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(morselSize)), runtime
  ) with ProfilingInputStreams[CONTEXT] with TimeLimitedCypherTest {

  private val runtimeName = if (runtime.isInstanceOf[PipelinedRuntime]) s"${runtime.name}_$morselSize" else runtime.name

  test("measure grouping aggregation 1") {
    val testName = "agg_grp1"
    val heapDumpFileNamePrefix = s"$HEAP_DUMP_PATH/${testName}_${runtimeName}"

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val data: Array[Array[Any]] = (1L to 10000L).map { i => Array[Any](i) }.toArray

    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profile(logicalQuery, runtime, input)
    consume(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile)
  }

  test("measure sort 1") {
    val testName = "sort1"
    val heapDumpFileNamePrefix = s"$HEAP_DUMP_PATH/${testName}_${runtimeName}"

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    // when
    val random = new Random(seed = 1337)
    val data: Array[Array[Any]] = (1L to 10000L).map { i => Array[Any](random.nextInt(10000)) }.toArray

    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profile(logicalQuery, runtime, input)
    consume(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile)
  }

}
