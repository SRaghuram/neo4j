/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.profiling

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.lang.Boolean.TRUE
import java.nio.file.Files
import java.nio.file.Path

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.EnterpriseRuntimeContextManager
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.PipelinedRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.RuntimeEnvironment
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.CUSTOM_MEMORY_TRACKING_CONTROLLER
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MemoryTrackingController.MemoryTrackerDecorator
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.runtime.spec.COMMUNITY
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.DEFAULT_HEAP_DUMP_INTERVAL
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.DEFAULT_INPUT_LIMIT
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.HEAP_DUMP_ENABLED
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.HEAP_DUMP_PATH
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.LOG_HEAP_DUMP_ACTIVITY
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.LOG_HEAP_DUMP_STACK_TRACE
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.OVERWRITE_EXISTING_HEAP_DUMPS
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.doHeapDump
import org.neo4j.cypher.internal.runtime.spec.tests.InputStreams
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.kernel.api.Kernel
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapDumper
import org.neo4j.memory.HeapDumpingMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

import scala.util.Random

object MemoryManagementProfilingBase {
  // The configured max memory per transaction in Bytes (zero means unlimited)
  val maxMemory: Long = 0L

  val DEFAULT_MORSEL_SIZE_BIG: Int = GraphDatabaseSettings.cypher_pipelined_batch_size_big.defaultValue()
  val DEFAULT_MORSEL_SIZE_SMALL: Int = GraphDatabaseSettings.cypher_pipelined_batch_size_small.defaultValue()

  def WITH_FUSING(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseSettings.cypher_operator_engine -> GraphDatabaseSettings.CypherOperatorEngine.COMPILED)

  // Global heap dump settings
  val HEAP_DUMP_ENABLED: Boolean = true
  val HEAP_DUMP_PATH: String = "target/heapdumps"
  val DEFAULT_INPUT_LIMIT: Long = 1000000L
  val DEFAULT_HEAP_DUMP_INTERVAL: Long = 500000L
  val OVERWRITE_EXISTING_HEAP_DUMPS: Boolean = false
  val LOG_HEAP_DUMP_ACTIVITY: Boolean = true
  val LOG_HEAP_DUMP_STACK_TRACE: Boolean = true

  private val heapDumper = new HeapDumper

  if (HEAP_DUMP_ENABLED) {
    if (Files.notExists(Path.of(HEAP_DUMP_PATH))) {
      Files.createDirectory(Path.of(HEAP_DUMP_PATH))
    }
  }

  def doHeapDump(fileName: String, heapDumpLiveObjectsOnly: Boolean = true): Unit = {
    val path = Path.of(fileName)
    val alreadyExists = Files.exists(path)
    if (alreadyExists && OVERWRITE_EXISTING_HEAP_DUMPS) {
      if (LOG_HEAP_DUMP_ACTIVITY) println(s"""Overwriting existing heap dump "$fileName"""")
      Files.delete(path)
      heapDumper.createHeapDump(fileName, heapDumpLiveObjectsOnly)
    } else if (alreadyExists) {
      if (LOG_HEAP_DUMP_ACTIVITY) println(s"""Skipping already existing heap dump "$fileName"""")
    } else {
      if (LOG_HEAP_DUMP_ACTIVITY) println(s"""Creating new heap dump "$fileName"""")
      heapDumper.createHeapDump(fileName, heapDumpLiveObjectsOnly)
    }
  }

  // Custom memory tracking. NOTE: It is not safe to run profiling test cases using this concurrently!
  private final val dynamicMemoryTrackerDecorator: MemoryTrackerDecorator = (m: MemoryTracker) => {
    if (memoryTrackerDecorator != null) {
      memoryTrackerDecorator.apply(m)
    } else {
      m
    }
  }
  private var memoryTrackerDecorator: MemoryTrackerDecorator = _
  private val memoryTrackingController = CUSTOM_MEMORY_TRACKING_CONTROLLER(dynamicMemoryTrackerDecorator)

  def setMemoryTrackingDecorator(decorator: MemoryTrackerDecorator): Unit = memoryTrackerDecorator = decorator
  def resetMemoryTrackingDecorator(): Unit = memoryTrackerDecorator = null

  // Edition
  private val enterpriseProfilingEdition = new Edition[EnterpriseRuntimeContext](
    () => new TestEnterpriseDatabaseManagementServiceBuilder(),
    (runtimeConfig, resolver, lifeSupport, logProvider) => {
      val kernel = resolver.resolveDependency(classOf[Kernel])
      val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
      val workerManager = resolver.resolveDependency(classOf[WorkerManagement])
      val augmentedRuntimeConfig = runtimeConfig.copy(memoryTrackingController = memoryTrackingController)

      val runtimeEnvironment = RuntimeEnvironment.of(augmentedRuntimeConfig, jobScheduler, kernel.cursors(), lifeSupport, workerManager, EmptyMemoryTracker.INSTANCE)

      EnterpriseRuntimeContextManager(
        GeneratedQueryStructure,
        logProvider.getLog("test"),
        augmentedRuntimeConfig,
        runtimeEnvironment
      )
    },
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(-1),
    GraphDatabaseSettings.cypher_operator_engine -> GraphDatabaseSettings.CypherOperatorEngine.COMPILED,
    MetricsSettings.metricsEnabled -> java.lang.Boolean.FALSE
  )

  val COMMUNITY_PROFILING: Edition[CommunityRuntimeContext] = COMMUNITY.EDITION
  val ENTERPRISE_PROFILING: Edition[EnterpriseRuntimeContext] = enterpriseProfilingEdition
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
                                                      heapDumpFileNamePrefix: String,
                                                      heapDumpLiveObjectsOnly: Boolean = true): InputDataStream with IteratorProgress = {
    iteratorInput(iterateWithPeriodicHeapDump(data, Some(limit), heapDumpInterval, heapDumpFileNamePrefix, heapDumpLiveObjectsOnly))
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
                                            rowSize: Option[Long] = None): Iterator[Array[Any]] = new Iterator[Array[Any]] with IteratorProgress {
    private val killThreshold: Long = if (rowSize.isDefined) killAfterNRows(rowSize.get) else Long.MaxValue
    var nextCallsTotal = 0L
    private var nextCallsSinceLastHeapDump = 0L
    override def hasNext: Boolean = limit.fold(true)(nextCallsTotal < _)

    override def nextCalls: Long = nextCallsTotal

    override def next(): Array[Any] = {
      nextCallsTotal += 1
      nextCallsSinceLastHeapDump += 1
      if (limit.isEmpty && nextCallsTotal > killThreshold) {
        fail("The query was not killed even though it consumed too much memory.")
      }
      if (HEAP_DUMP_ENABLED && (nextCallsSinceLastHeapDump >= heapDumpInterval || (limit.isDefined && nextCallsTotal == limit.get))) {
        val fileName = s"$heapDumpFileNamePrefix-$nextCallsTotal.hprof"
        doHeapDump(fileName, heapDumpLiveObjectsOnly)
        nextCallsSinceLastHeapDump = 0
      }
      val index = ((nextCallsTotal - 1) % data.length).toInt
      val dataElement = data(index)
      if (limit.isDefined && ((nextCallsTotal - 1) >= (limit.get - data.length))) {
        data(index) = null // Clear the reference to the data in the last run over it to avoid retaining heap in the test case
      }
      dataElement
    }
  }
}

abstract class MemoryManagementProfilingBase[CONTEXT <: RuntimeContext](
                                                                        edition: Edition[CONTEXT],
                                                                        runtime: CypherRuntime[CONTEXT],
                                                                        morselSize: Int = MemoryManagementProfilingBase.DEFAULT_MORSEL_SIZE_BIG,
                                                                        runtimeSuffix: String = ""
                                                                       )
  extends RuntimeTestSuite[CONTEXT](edition.copyWith(
    GraphDatabaseSettings.track_query_allocation -> TRUE,
    GraphDatabaseSettings.memory_transaction_max_size -> Long.box(MemoryManagementProfilingBase.maxMemory),
    GraphDatabaseSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(morselSize),
    GraphDatabaseSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(morselSize)), runtime
  ) with ProfilingInputStreams[CONTEXT] with TimeLimitedCypherTest {

  private val runtimeName = (if (runtime.isInstanceOf[PipelinedRuntime]) s"${runtime.name}_$morselSize" else runtime.name) +
    (if (runtimeSuffix.nonEmpty) s"_$runtimeSuffix" else "")

  def heapDumpFileNamePrefixForTestName(testName: String) = s"$HEAP_DUMP_PATH/${testName}_$runtimeName"

  if (HEAP_DUMP_ENABLED) {
    if (Files.notExists(Path.of(HEAP_DUMP_PATH))) {
      Files.createDirectory(Path.of(HEAP_DUMP_PATH))
    }
  }

  test("measure grouping aggregation 1") {
    val testName = "agg_grp1"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq("x AS x"), Seq("collect(x) AS c"))
      .input(variables = Seq("x"))
      .build()

    // when
    val data: Array[Array[Any]] = (1L to 10000L).map(Array[Any](_)).toArray
    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure sort 1 column") {
    val testName = "sort1"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x"))
      .build()

    // when
    val random = new Random(seed = 1337)
    val data: Array[Array[Any]] = (1L to 10000L).map { _ => Array[Any](random.nextInt(10000)) }.toArray

    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure sort 1 column with payload") {
    val testName = "sort1-pay"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .input(variables = Seq("x", "payload"))
      .build()

    // when
    val n = DEFAULT_INPUT_LIMIT.toInt
    val random = new Random(seed = 1337)
    var payload: Array[ListValue] = (1 to n).map { _ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)}.toArray
    val data: Array[Array[Any]] = (0 until n).map { i => Array[Any](random.nextInt(10000), payload(i)) }.toArray

    payload = null // Clear unnecessary reference

    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure distinct 1 pct") {
    val testName = "distinct1pct"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    // when
    val random = new Random(seed = 1337)
    var data = (1L to 10000L).map(Array[Any](_))
    val shuffledData = random.shuffle(data).toArray

    data = null // Clear unnecessary reference

    val input = finiteCyclicInputWithPeriodicHeapDump(shuffledData, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure distinct 100 pct") {
    val testName = "distinct100pct"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .distinct("x AS x")
      .input(variables = Seq("x"))
      .build()

    // when
    val random = new Random(seed = 1337)
    var data = (1L to DEFAULT_INPUT_LIMIT).map(Array[Any](_))
    val shuffledData = random.shuffle(data).toArray

    data = null // Clear unnecessary reference

    val input = finiteCyclicInputWithPeriodicHeapDump(shuffledData, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure hash join 1-1 - single node") {
    val testName = "nodehashjoin1-1"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    var nodes = given { nodeGraph(DEFAULT_INPUT_LIMIT.toInt) }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"))
      .build()

    // when
    val random = new Random(seed = 1337)
    var data = nodes.map(Array[Any](_))
    val shuffledData = random.shuffle(data).toArray

    // Make sure to clear out all unnecessary references to get a clean dominator tree in the heap dump
    // (The elements of shuffledData will be cleared as they are streamed by finiteCyclicInputWithPeriodicHeapDump)
    nodes = null
    data = null

    val input = finiteCyclicInputWithPeriodicHeapDump(shuffledData, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure hash join 1-1 - multiple nodes") {
    val testName = "nodehashjoin-multi1-1"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val n = DEFAULT_INPUT_LIMIT.toInt
    var paths = given { chainGraphs(n, "R") }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x", "y")
      .|.expandAll("(x)-[r:R]->(y)")
      .|.allNodeScan("x")
      .input(nodes = Seq("x", "y"))
      .build()

    // when
    val random = new Random(seed = 1337)
    var data = (0 until n).map { i => Array[Any](paths(i).startNode, paths(i).endNode()) }
    val shuffledData = random.shuffle(data).toArray

    // Make sure to clear out all unnecessary references to get a clean dominator tree in the heap dump
    // (The elements of shuffledData will be cleared as they are streamed by finiteCyclicInputWithPeriodicHeapDump)
    paths = null
    data = null

    val input = finiteCyclicInputWithPeriodicHeapDump(shuffledData, n, n/2, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure hash join 1-1 - single node with payload") {
    val testName = "nodehashjoin1-1-pay"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)
    val random = new Random(seed = 1337)

    // given
    val n = DEFAULT_INPUT_LIMIT.toInt
    var payload = (1L to n).map { _ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)}.toArray
    var nodes = given { nodeGraph(n) }.toArray[Any]
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "payload")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"), variables = Seq("payload"))
      .build()

    // when
    var data = (0 until n).map { i => Array[Any](nodes(i), payload(i)) }
    val shuffledData = random.shuffle(data).toArray

    // Make sure to clear out all unnecessary references to get a clean dominator tree in the heap dump
    // (The elements of shuffledData will be cleared as they are streamed by finiteCyclicInputWithPeriodicHeapDump)
    nodes = null
    payload = null
    data = null

    val input = finiteCyclicInputWithPeriodicHeapDump(shuffledData, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure hash join 1-1 - single node with payload under limit") { // Force use of FilteringMorsel by adding a (otherwise useless) limit
    val testName = "nodehashjoin1-1-pay2"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)
    val random = new Random(seed = 1337)

    // given
    val n = DEFAULT_INPUT_LIMIT.toInt
    var payload = (1L to n).map { _ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)}.toArray
    var nodes = given { nodeGraph(n) }.toArray[Any]
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "payload")
      .limit(n)
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .input(nodes = Seq("x"), variables = Seq("payload"))
      .build()

    // when
    var data = (0 until n).map { i => Array[Any](nodes(i), payload(i)) }
    val shuffledData = random.shuffle(data).toArray

    // Make sure to clear out all unnecessary references to get a clean dominator tree in the heap dump
    // (The elements of shuffledData will be cleared as they are streamed by finiteCyclicInputWithPeriodicHeapDump)
    nodes = null
    payload = null
    data = null

    val input = finiteCyclicInputWithPeriodicHeapDump(shuffledData, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure pruning-var-expand 1") {
    val testName = "prunvarexp-starc1"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    val (center, _) = nestedStarGraphCenterOnly(6, 10, "C", "A")

    // We need to restart the transaction after constructing the graph or we will track all the memory usage from it
    runtimeTestSupport.restartTx()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .pruningVarExpand("(x)<-[*2..6]-(y)")
      .input(nodes=Seq("x"))
      .build()

    // Input the same center twice to make sure we do not overestimate heap usage, since every input row restarts with a new state.
    // (i.e. the result should not increase with identical input rows)
    val inputArray = Array(Array[Any](center), Array[Any](center))

    runPeakMemoryUsageProfiling(logicalQuery, inputArray, heapDumpFileNamePrefix)
  }

  test("measure partial top - ordered column has one value") {
    val testName = "partial-top-one"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), DEFAULT_INPUT_LIMIT)
      .input(variables = Seq("x", "y"))
      .build()

    val inputRows = for (i <- 0L until DEFAULT_INPUT_LIMIT) yield Array[Any](1, DEFAULT_INPUT_LIMIT - i)
    runPeakMemoryUsageProfiling(logicalQuery, inputRows.toArray, heapDumpFileNamePrefix)
  }

  test("measure partial top - ordered column has distinct values") {
    val testName = "partial-top-distinct"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), DEFAULT_INPUT_LIMIT)
      .input(variables = Seq("x", "y"))
      .build()

    val inputRows = for (i <- 0L until DEFAULT_INPUT_LIMIT) yield Array[Any](i, i)
    runPeakMemoryUsageProfiling(logicalQuery, inputRows.toArray, heapDumpFileNamePrefix)
  }

  test("measure partial sort - ordered column has one value") {
    val testName = "partial-sort-one"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val inputRows = for (i <- 0L until DEFAULT_INPUT_LIMIT) yield Array[Any](1, DEFAULT_INPUT_LIMIT - i)
    runPeakMemoryUsageProfiling(logicalQuery, inputRows.toArray, heapDumpFileNamePrefix)
  }

  // In this case no chunk spans multiple morsels, so we overestimate peak memory usage by 100% - the size of one morsel.
  test("measure partial sort - ordered column has distinct values in spans of three") {
    val testName = "partial-sort-distinct"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
      .input(variables = Seq("x", "y"))
      .build()

    val inputRows = for (i <- 0L until DEFAULT_INPUT_LIMIT) yield Array[Any](i, i)
    runPeakMemoryUsageProfiling(logicalQuery, inputRows.toArray, heapDumpFileNamePrefix)
  }

  /**
   * Convenience method when you have an Array of input data
   */
  protected def runPeakMemoryUsageProfiling(logicalQuery: LogicalQuery, inputArray: Array[Array[Any]], heapDumpFileNamePrefix: String): Unit = {
    val input1 = finiteInput(inputArray.length, Some(i => inputArray(i.toInt - 1)))
    val input2 = finiteInput(inputArray.length, Some(i => {
      val index = i.toInt - 1
      val record = inputArray(index)
      inputArray(index) = null // Clear the reference to the data in the last run over it to avoid retaining heap in the test case
      record
    }))
    runPeakMemoryUsageProfiling(logicalQuery, input1, input2, heapDumpFileNamePrefix)
  }

  /**
   * Run query once to determine estimated peak usage, then run again with the same data (we trust the user here ;)
   * and heap dump when that estimated peak usage is reached.
   *
   * NOTE: inputStream1 and inputStream2 should have identical elements
   *
   * @param logicalQuery The query to execute
   * @param inputStream1 Input data stream for the first run, to determine max allocated memory
   * @param inputStream2 Input data stream for the second run, to create a heap when the target is reached
   */
  protected def runPeakMemoryUsageProfiling(logicalQuery: LogicalQuery, inputStream1: InputDataStream, inputStream2: InputDataStream,
                                            heapDumpFileNamePrefix: String): Unit = {
    val heapDumpFileName = heapDumpFileNamePrefix + "-peak.hprof"

    // Run query once to determine estimated peak usage
    val result1 = profileNonRecording(logicalQuery, runtime, inputStream1)
    consumeNonRecording(result1)

    val queryProfile = result1.runtimeResult.queryProfile()
    val estimatedPeakHeapUsage = result1.runtimeResult.queryProfile().maxAllocatedMemory()

    // Then run again and dump when that estimated peak usage is reached. Make sure to restart transaction to get a cleared transaction memory tracker.
    runtimeTestSupport.restartTx()

    // Inject a custom memory tracker
    var lastAllocation: Long = 0L
    var stackTrace: Option[String] = None

    val memoryTrackerDecorator: MemoryTrackerDecorator = (transactionMemoryTracker: MemoryTracker) => {
      val heapDumpingMemoryTracker = new HeapDumpingMemoryTracker(transactionMemoryTracker)

      heapDumpingMemoryTracker.setHeapDumpAtHighWaterMark(estimatedPeakHeapUsage, heapDumpFileName, OVERWRITE_EXISTING_HEAP_DUMPS, true,
                                                          (_: HeapDumpingMemoryTracker) => {
        lastAllocation = heapDumpingMemoryTracker.lastAllocatedBytes()
        if (LOG_HEAP_DUMP_STACK_TRACE) {
          val stackTraceByteArrayOS = new ByteArrayOutputStream()
          new Exception("Stack trace").printStackTrace(new PrintStream(stackTraceByteArrayOS))
          stackTrace = Some(stackTraceByteArrayOS.toString)
        }
      })
      heapDumpingMemoryTracker
    }

    try {
      MemoryManagementProfilingBase.setMemoryTrackingDecorator(memoryTrackerDecorator)

      // Do the second run. If everything is detereministic this should trigger the heap dump
      val result2 = profileNonRecording(logicalQuery, runtime, inputStream2)
      consumeNonRecording(result2)

      printQueryProfile(heapDumpFileNamePrefix + "-peak.profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY, lastAllocation, stackTrace)
    } finally {
      MemoryManagementProfilingBase.resetMemoryTrackingDecorator()
    }
  }
}

/**
 * Profiling for runtimes with full language support
 */
trait FullSupportMemoryManagementProfilingBase [CONTEXT <: RuntimeContext] {
  self: MemoryManagementProfilingBase[CONTEXT] =>

  test("measure eager - single node with payload") {
    val testName = "eager-1-pay"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val n = DEFAULT_INPUT_LIMIT.toInt
    var payload = (1L to n).map { _ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)}.toArray
    var nodes = given { nodeGraph(n) }.toArray[Any]
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "payload")
      .eager()
      .input(nodes = Seq("x"), variables = Seq("payload"))
      .build()

    // when
    val data = (0 until n).map { i => Array[Any](nodes(i), payload(i)) }.toArray

    // Make sure to clear out all unnecessary references to get a clean dominator tree in the heap dump
    // (The elements of shuffledData will be cleared as they are streamed by finiteCyclicInputWithPeriodicHeapDump)
    nodes = null
    payload = null

    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure peak of two sequential eagers - single node with payload") {
    val testName = "eager-2-pay"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val n = DEFAULT_INPUT_LIMIT.toInt
    var payload = (1L to n).map { _ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)}.toArray
    var nodes = given { nodeGraph(n) }.toArray[Any]

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "payload")
      .eager()
      .eager()
      .input(nodes = Seq("x"), variables = Seq("payload"))
      .build()

    // when
    val data = (0 until n).map { i => Array[Any](nodes(i), payload(i)) }.toArray

    // Make sure to clear out all unnecessary references to get a clean dominator tree in the heap dump
    // (The elements of shuffledData will be cleared as they are streamed by finiteCyclicInputWithPeriodicHeapDump)
    nodes = null
    payload = null

    runPeakMemoryUsageProfiling(logicalQuery, data, heapDumpFileNamePrefix)
  }

  test("measure aggregation percentileCont without grouping") {
    val testName = "agg_percentile_cont"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .aggregation(Seq.empty, Seq("percentileCont(x,0.50) AS y"))
      .input(variables = Seq("x"))
      .build()

    val n = DEFAULT_INPUT_LIMIT.toInt
    val random = new Random(seed = 1337)
    val data: Array[Array[Any]] = (0 until n).map { i => Array[Any](random.nextInt(10000)) }.toArray

    // when
    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure aggregation percentileCont with grouping") {
    val testName = "agg_percentile_cont_grouping"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z")
      .aggregation(Seq("y AS y"), Seq("percentileCont(x,0.20) AS z"))
      .input(variables = Seq("x", "y"))
      .build()

    val n = DEFAULT_INPUT_LIMIT.toInt
    val random = new Random(seed = 1337)
    val data: Array[Array[Any]] = (0 until n).map { i => Array[Any](random.nextInt(10000), Math.round(i/30)) }.toArray

    // when
    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure aggregation percentileDisc without grouping") {
    val testName = "agg_percentile_disc"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .aggregation(Seq.empty, Seq("percentileDisc(x,0.20) AS y"))
      .input(variables = Seq("x"))
      .build()


    val n = DEFAULT_INPUT_LIMIT.toInt
    val random = new Random(seed = 1337)
    val data: Array[Array[Any]] = (0 until n).map { i => Array[Any](random.nextInt(10000)) }.toArray

    // when
    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }

  test("measure aggregation percentileDisc with grouping") {
    val testName = "agg_percentile_disc_grouping"
    val heapDumpFileNamePrefix = heapDumpFileNamePrefixForTestName(testName)

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y", "z")
      .aggregation(Seq("y AS y"), Seq("percentileDisc(x,0.20) AS z"))
      .input(variables = Seq("x", "y"))
      .build()


    val n = DEFAULT_INPUT_LIMIT.toInt
    val random = new Random(seed = 1337)
    val data: Array[Array[Any]] = (0 until n).map { i => Array[Any](random.nextInt(10000), Math.round(i/30)) }.toArray

    // when
    val input = finiteCyclicInputWithPeriodicHeapDump(data, DEFAULT_INPUT_LIMIT, DEFAULT_HEAP_DUMP_INTERVAL, heapDumpFileNamePrefix)

    // then
    val result = profileNonRecording(logicalQuery, runtime, input)
    consumeNonRecording(result)

    val queryProfile = result.runtimeResult.queryProfile()
    printQueryProfile(heapDumpFileNamePrefix + ".profile", queryProfile, LOG_HEAP_DUMP_ACTIVITY)
  }
}
