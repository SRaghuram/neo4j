/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.profiling

import java.lang.Boolean.TRUE

import com.neo4j.configuration.MetricsSettings
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.CommunityRuntimeContextManager
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.EnterpriseRuntimeContextManager
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.RuntimeEnvironment
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.runtime.CUSTOM_MEMORY_TRACKING_CONTROLLER
import org.neo4j.cypher.internal.runtime.MemoryTrackingController.MemoryTrackerDecorator
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryMeasurementTestBase.DEFAULT_INPUT_SIZE
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.api.Kernel
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.seqAsJavaListConverter

object MemoryMeasurementTestBase {
  // The configured max memory per transaction in Bytes (zero means unlimited)
  val maxMemory: Long = 0L

  val DEFAULT_MORSEL_SIZE_BIG: Int = GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big.defaultValue()
  val DEFAULT_MORSEL_SIZE_SMALL: Int = GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small.defaultValue()

  // Input size
  val DEFAULT_INPUT_SIZE: Long = 100000L

  // Default tolerances
  val DEFAULT_ABSOLUTE_TOLERANCE: (Int, ByteUnit) = (50, ByteUnit.KibiByte)
  val DEFAULT_FRACTION_TOLERANCE: Double = 0.15

  // Enables printing debug info like heap dump file paths etc.
  val DEBUG_PRINT = false

  // Enables printing a csv summary in afterAll
  val SUMMARY_PRINT = false

  // Delete heap dump files when done with them
  val DELETE_HEAP_DUMPS = true

  // Custom memory tracking. NOTE: It is not safe to run profiling test cases using this concurrently!
  private var memoryTrackerDecorator: MemoryTrackerDecorator = _
  private val memoryTrackingController = CUSTOM_MEMORY_TRACKING_CONTROLLER { inner =>
    if (memoryTrackerDecorator != null) {
      memoryTrackerDecorator.apply(inner)
    } else {
      inner
    }
  }

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
        logProvider.getLog("test"),
        augmentedRuntimeConfig,
        runtimeEnvironment
      )
    },
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseInternalSettings.cypher_worker_count -> Integer.valueOf(-1),
    GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED,
    MetricsSettings.metrics_enabled -> java.lang.Boolean.FALSE
  )

  private val communityProfilingEdition = new Edition[CommunityRuntimeContext](
    () => new TestDatabaseManagementServiceBuilder(),
    (runtimeConfig, _, _, logProvider) => {
      val augmentedRuntimeConfig = runtimeConfig.copy(memoryTrackingController = memoryTrackingController)

      CommunityRuntimeContextManager(
        logProvider.getLog("test"),
        augmentedRuntimeConfig
      )
    },
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseInternalSettings.cypher_worker_count -> Integer.valueOf(-1),
    GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED,
    MetricsSettings.metrics_enabled -> java.lang.Boolean.FALSE
  )

  val COMMUNITY_PROFILING: Edition[CommunityRuntimeContext] = communityProfilingEdition
  val ENTERPRISE_PROFILING: Edition[EnterpriseRuntimeContext] = enterpriseProfilingEdition
}

abstract class MemoryMeasurementTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  morselSize: Int = MemoryMeasurementTestBase.DEFAULT_MORSEL_SIZE_BIG,
) extends RuntimeTestSuite[CONTEXT](edition.copyWith(
  GraphDatabaseSettings.track_query_allocation -> TRUE,
  GraphDatabaseSettings.memory_transaction_max_size -> Long.box(MemoryMeasurementTestBase.maxMemory),
  GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(morselSize),
  GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(morselSize)),
  runtime
) with InputStreamHelpers[CONTEXT]
  with MemoryMeasurementTestHelpers[CONTEXT]
  with TimeLimitedCypherTest {

  override def setMemoryTrackingDecorator(decorator: MemoryTrackerDecorator): Unit = MemoryMeasurementTestBase.setMemoryTrackingDecorator(decorator)
  override def resetMemoryTrackingDecorator(): Unit = MemoryMeasurementTestBase.resetMemoryTrackingDecorator()

  override def debugPrint: Boolean = MemoryMeasurementTestBase.DEBUG_PRINT
  override def summaryPrint: Boolean = MemoryMeasurementTestBase.SUMMARY_PRINT
  override def deleteHeapDumps: Boolean = MemoryMeasurementTestBase.DELETE_HEAP_DUMPS

  val DEFAULT_TOLERANCE: Tolerance = EitherTolerance(
    AbsoluteErrorTolerance(MemoryMeasurementTestBase.DEFAULT_ABSOLUTE_TOLERANCE._1, MemoryMeasurementTestBase.DEFAULT_ABSOLUTE_TOLERANCE._2),
    ErrorFractionTolerance(MemoryMeasurementTestBase.DEFAULT_FRACTION_TOLERANCE)
  )

  // ============ AGGREGATION ============

  private def measureAggregation(grouped: Boolean, aggregator: String,
                                 measuringStrategy: MeasuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
                                 tolerance: Tolerance): Unit =
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("c")
        .aggregation(Seq("x AS x").filter(_ => grouped), Seq(aggregator + " AS c"))
        .input(variables = Seq("x"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](i)),
      measuringStrategy = measuringStrategy,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = tolerance,
    )

  test("measure aggregation: count(...)") {
    measureAggregation(grouped = false, "count(x)", tolerance = AbsoluteErrorTolerance(100, ByteUnit.KibiByte))
  }

  test("measure aggregation: collect(...)") {
    measureAggregation(grouped = false, "collect(x)", tolerance = ErrorFractionTolerance(0.5))
  }

  test("measure grouping aggregation: count(...)") {
    measureAggregation(grouped = true, "count(x)", tolerance = DEFAULT_TOLERANCE)
  }

  test("measure grouping aggregation: collect(...)") {
    measureAggregation(grouped = true, "collect(x)", tolerance = ErrorFractionTolerance(0.5))
  }

  test("measure aggregation: count(DISTINCT ...)") {
    measureAggregation(grouped = false, "count(DISTINCT x)", tolerance = ErrorFractionTolerance(0.5))
  }

  test("measure aggregation: collect(DISTINCT ...)") {
    measureAggregation(grouped = false, "collect(DISTINCT x)", tolerance = ErrorFractionTolerance(1.0))
  }

  test("measure grouping aggregation: count(DISTINCT ...)") {
    measureAggregation(grouped = true, "count(DISTINCT x)", tolerance = DEFAULT_TOLERANCE)
  }

  test("measure grouping aggregation: collect(DISTINCT ...)") {
    measureAggregation(grouped = true, "collect(DISTINCT x)", tolerance = ErrorFractionTolerance(0.5))
  }

  // ===== ORDERED AGGREGATION ========

  test("measure ordered aggregation: count(...) - ordered key has 10 distinct values") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("c")
        .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(x) AS c"), Seq("x"))
        .projection("42 AS y")
        .input(variables = Seq("x"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt) { i =>
        val x = i / (DEFAULT_INPUT_SIZE/10)
        Array[Any](x)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMark,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = AbsoluteErrorTolerance(100, ByteUnit.KibiByte),
    )
  }

  test("measure ordered aggregation: count(...) - ordered key is fixed") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("c")
        .orderedAggregation(Seq("x AS x", "y AS y"), Seq("count(x) AS c"), Seq("y"))
        .projection("42 AS y")
        .input(variables = Seq("x"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMark,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure sort 1 column") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .sort(Seq(Ascending("x")))
        .input(variables = Seq("x"))
        .build(),
      input = withRandom(random => finiteInput(DEFAULT_INPUT_SIZE.toInt, Some(_ => Array[Any](random.nextInt(10000))))),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.5),
    )
  }

  test("measure sort 1 column with payload") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .sort(Seq(Ascending("x")))
        .input(variables = Seq("x", "payload"))
        .build(),
      input = withRandom(random => finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(_ => {
        val x = random.nextInt(10000)
        val payload = VirtualValues.list((1 to 8).map(Values.longValue(_)): _*)
        Array[Any](x, payload)
      })),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.5),
    )
  }

  test("measure distinct 1 pct") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .distinct("x AS x")
        .input(variables = Seq("x"))
        .build(),
      input = withRandom(random => finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(_ =>
        Array[Any](random.nextInt(DEFAULT_INPUT_SIZE.toInt / 100))
      )),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = AbsoluteErrorTolerance(100, ByteUnit.KibiByte),
    )
  }

  test("measure distinct 100 pct") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .distinct("x AS x")
        .input(variables = Seq("x"))
        .build(),
      input = withRandom(random => finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(_ =>
        Array[Any](random.nextInt(DEFAULT_INPUT_SIZE.toInt))
      )),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(1.0),
    )
  }

  // ============ JOIN ============

  test("measure hash join 1-1 - single node") {

    // given
    val nodes = given {
      nodeGraph(DEFAULT_INPUT_SIZE.toInt)
    }

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .nodeHashJoin("x")
        .|.allNodeScan("x")
        .input(nodes = Seq("x"))
        .build(),
      input = withRandom { random =>
        val data = nodes.map(Array[Any](_))
        val shuffled = random.shuffle(data).toArray
        finiteSeqInput(shuffled)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure hash join 1-1 - multiple nodes") {
    val n = DEFAULT_INPUT_SIZE.toInt
    val paths = given { chainGraphs(n, "R") }

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .nodeHashJoin("x", "y")
        .|.expandAll("(x)-[r:R]->(y)")
        .|.allNodeScan("x")
        .input(nodes = Seq("x", "y"))
        .build(),
      input = withRandom { random =>
        val data = (0 until n).map { i => Array[Any](paths(i).startNode, paths(i).endNode()) }
        val shuffled = random.shuffle(data).toArray
        finiteSeqInput(shuffled)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure hash join 1-1 - single node with payload") {
    val n = DEFAULT_INPUT_SIZE.toInt
    val nodes = given { nodeGraph(n) }.toArray[Any]
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "payload")
        .nodeHashJoin("x")
        .|.allNodeScan("x")
        .input(nodes = Seq("x"), variables = Seq("payload"))
        .build(),
      input = withShuffle(n) { shuffle =>
        finiteGeneratedInput(n){ i =>
          val node = nodes(shuffle(i))
          val payload = VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)
          Array[Any](node, payload)
        }
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure hash join 1-1 - single node with payload under limit") { // Force use of FilteringMorsel by adding a (otherwise useless) limit
    val n = DEFAULT_INPUT_SIZE.toInt
    val nodes = given { nodeGraph(n) }.toArray[Any]

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "payload")
        .limit(n)
        .nodeHashJoin("x")
        .|.allNodeScan("x")
        .input(nodes = Seq("x"), variables = Seq("payload"))
        .build(),
      input = withShuffle(n) { shuffle =>
        finiteGeneratedInput(n) { i =>
          val node = nodes(shuffle(i))
          val payload = VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)
          Array[Any](node, payload)
        }
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure value hash join") {
    val nodes = given { nodePropertyGraph(DEFAULT_INPUT_SIZE.toInt, { case i: Int => Map("prop" -> i)}) }

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("xp", "yp")
        .projection("x.prop AS xp", "y.prop AS yp")
        .valueHashJoin("x.prop = y.prop")
        .|.allNodeScan("y")
        .input(nodes = Seq("x"))
        .build(),
      input = withRandom { random =>
        val data = nodes.map(Array[Any](_))
        val shuffled = random.shuffle(data).toArray
        finiteSeqInput(shuffled)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  // ============ EXPAND ============

  // This test seems to suffer from bad measurement noise/estimation error in PipelinedBigMorselMemoryMeasurementTest
  ignore("measure pruning-var-expand 1") {
    val (center, _) = nestedStarGraphCenterOnly(6, 10, "C", "A")

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .pruningVarExpand("(x)<-[*2..6]-(y)")
        .input(nodes = Seq("x"))
        .build(),
      input = finiteConstantInput(2)(Array[Any](center)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(1)),
      tolerance = ErrorFractionTolerance(50000.0),
    )
  }

  private def measureExpandInto(multiplier: Int, logicalQuery: LogicalQuery, tolerance: Tolerance): Unit = {

    val n = 100
    val nodes = given {
      val (aNodes,bNodes) = bipartiteGraph(n, "A", "B", "OUT")
      val r2 = RelationshipType.withName("IN")
      for {a <- aNodes
           b <- bNodes
           _ <- 0 until multiplier} {
        b.createRelationshipTo(a, r2)
      }
      aNodes
    }

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = logicalQuery,
      input = finiteSeqColumnInput(nodes),
      measuringStrategy = HeapDumpAtEstimateHighWaterMark,
      baselineStrategy = Some(HeapDumpAtInputOffset(nodes.size / 2)),
      tolerance = tolerance,
    )
  }

  // This test seems to suffer from bad measurement noise/estimation error in PipelinedBigMorselNoFusingMemoryMeasurementTest
  ignore("measure expand-into small") {
    val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "b")
      .expandInto("(a)<-[r:IN]-(b)")
      .expand("(a)-[:OUT]->(b)")
      .input(nodes = Seq("a"))
      .build()

    measureExpandInto(1, logicalQuery, tolerance = ErrorFractionTolerance(0.5))
  }

  // This test takes over 5 min on TC
  ignore("measure expand-into big") {
    val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "b")
      .expandInto("(a)<-[r:IN]-(b)")
      .expand("(a)-[:OUT]->(b)")
      .input(nodes = Seq("a"))
      .build()

    measureExpandInto(50, logicalQuery, tolerance = DEFAULT_TOLERANCE)
  }

  // This test seems to suffer from bad measurement noise/estimation error in PipelinedBigMorselNoFusingMemoryMeasurementTest
  ignore("measure optional expand-into small") {
    val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "b")
      .optionalExpandInto("(a)<-[r:IN]-(b)")
      .expand("(a)-[:OUT]->(b)")
      .input(nodes = Seq("a"))
      .build()

    measureExpandInto(1, logicalQuery, tolerance = ErrorFractionTolerance(0.5))
  }

  // This test takes over 5 min on TC
  ignore("measure optional expand-into big") {
    val logicalQuery: LogicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "b")
      .optionalExpandInto("(a)<-[r:IN]-(b)")
      .expand("(a)-[:OUT]->(b)")
      .input(nodes = Seq("a"))
      .build()

    measureExpandInto(100, logicalQuery, tolerance = DEFAULT_TOLERANCE)
  }

  // ============ SHORTEST PATH ============

  private def measureShortestPath(start: Node, end: Node, all: Boolean,
                                  tolerance: Tolerance): Unit =
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("path")
        .shortestPath("(x)-[r*]->(y)", Some("path"), all)
        .input(Seq("x", "y"))
        .build(),
      input = finiteConstantInput(2)(Array[Any](start, end)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(1)),
      tolerance = tolerance,
    )

  // This test seems to suffer from bad measurement noise/estimation error in PipelinedBigMorselMemoryMeasurementTest
  ignore("measure all shortest paths peak - many shared relationships") {
    val chainCount = 6
    val chainDepth = 6
    val (start, end) = given {
      linkedChainGraph(chainCount, chainDepth)
    }

    measureShortestPath(start, end, all = true, tolerance = DEFAULT_TOLERANCE)
  }

  // This test seems to suffer from bad measurement noise/estimation error in InterpretedMemoryMeasurementTest
  ignore("measure all shortest paths peak - no shared relationships") {
    val chainCount = 100000
    val chainDepth = 2
    val (start, end) = given {
      linkedChainGraphNoCrossLinking(chainCount, chainDepth)
    }

    measureShortestPath(start, end, all = true, tolerance = DEFAULT_TOLERANCE)
  }

  test("measure single shortest paths peak - many shared relationships") {
    val chainCount = 6
    val chainDepth = 6
    val (start, end) = given {
      linkedChainGraph(chainCount, chainDepth)
    }

    measureShortestPath(start, end, all = false, tolerance = DEFAULT_TOLERANCE)
  }

  // This test seems to suffer from bad measurement noise/estimation error in PipelinedSmallMorselMemoryMeasurementTest
  ignore("measure single shortest paths peak - no shared relationships") {
    val chainCount = 100000
    val chainDepth = 2
    val (start, end) = given {
      linkedChainGraphNoCrossLinking(chainCount, chainDepth)
    }

    measureShortestPath(start, end, all = false, tolerance = ErrorFractionTolerance(100000.0))
  }

  // ============ PARTIAL TOP ============

  test("measure partial top - ordered column has one value") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), DEFAULT_INPUT_SIZE)
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](1, DEFAULT_INPUT_SIZE - i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.5),
    )
  }

  // This test seems to suffer from bad measurement noise/estimation error in PipelinedBigMorselMemoryMeasurementTest
  ignore("measure partial top - ordered column has distinct values") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), DEFAULT_INPUT_SIZE)
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](i, i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure partial top - worst case") {
    val n = DEFAULT_INPUT_SIZE / 10
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "y")
        .partialTop(Seq(Ascending("x")), Seq(Ascending("y")), n / 2)
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(n.toInt) { i =>
        // First half of the rows has no y column. This half will get evicted.
        // Second half of the rows has a large y value. This half will get retained.
        val y = if (i < n / 2) null else Seq.fill(1000)(0).asJava
        Array[Any](1, y)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(n/ 2)),
      tolerance = ErrorFractionTolerance(0.5),
    )
  }

  // ============ PARTIAL SORT ============

  test("measure partial sort - ordered column has one value") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](1, DEFAULT_INPUT_SIZE - i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.5),
    )
  }

  // In this case no chunk spans multiple morsels, so we overestimate peak memory usage by 100% - the size of one morsel.
  // This test seems to suffer from bad measurement noise/estimation error in PipelinedBigMorselMemoryMeasurementTest
  ignore("measure partial sort - ordered column has distinct values in spans of three") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .partialSort(Seq(Ascending("x")), Seq(Ascending("y")))
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](i, i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  // ============ TOP ============

  test("measure top - all distinct") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .top(Seq(Ascending("x")), DEFAULT_INPUT_SIZE)
        .input(variables = Seq("x"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.5),
    )
  }

  test("measure top - worst case") {
    val n = DEFAULT_INPUT_SIZE / 10L

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "y")
        .top(Seq(Descending("x")), n / 2)
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(n.toInt) { i =>
        // First half of the rows has no y column. This half will get evicted.
        // Second half of the rows has a large y value. This half will get retained.
        val y = if (i < n / 2) null else Seq.fill(1000)(0).asJava
        Array[Any](i, y)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(n / 2)),
      tolerance = ErrorFractionTolerance(2.0),
    )
  }

  // ============ ORDERED DISTINCT ============

  test("measure ordered distinct - ordered column has one value") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .orderedDistinct(Seq("x"), "x AS x", "y AS y")
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](1, DEFAULT_INPUT_SIZE - i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.5),
    )
  }

  test("measure ordered distinct - ordered column has distinct values") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .orderedDistinct(Seq("x"), "x AS x", "y AS y")
        .input(variables = Seq("x", "y"))
        .build(),
      input = finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](i, i)),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure apply with RHS limit") {
    val nodes = given {
      nodeGraph(DEFAULT_INPUT_SIZE.toInt)
    }

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("l")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("l")
        .apply()
        .|.limit(1)
        .|.nonFuseable()
        .|.unwind("[1,2] as i")
        .|.argument("l")
        .input(Seq("l"))
        .build(),
      input = withRandom { random =>
        val data = nodes.map(Array[Any](_))
        val shuffled = random.shuffle(data).toArray
        finiteSeqInput(shuffled)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }
}

/**
 * Profiling for runtimes with full language support
 */
trait FullSupportMemoryMeasurementTestBase [CONTEXT <: RuntimeContext] {
  self: MemoryMeasurementTestBase[CONTEXT] =>

  test("measure eager - single node with payload") {
    // given
    val n = DEFAULT_INPUT_SIZE.toInt
    val payload = (1L to n).map(_ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)).toArray
    val nodes = given { nodeGraph(n) }.toArray[Any]

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x"), variables = Seq("payload")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "payload")
        .eager()
        .input(nodes = Seq("x"), variables = Seq("payload"))
        .build(),
      input = {
        val data = (0 until n).map(i => Array[Any](nodes(i), payload(i))).toArray
        finiteSeqInput(data)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(5.0),
    )
  }

  test("measure peak of two sequential eagers - single node with payload") {
    // given
    val n = DEFAULT_INPUT_SIZE.toInt
    val payload = (1L to n).map(_ => VirtualValues.list((1 to 8).map(Values.longValue(_)).toArray: _*)).toArray
    val nodes = given { nodeGraph(n) }.toArray[Any]

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x"), variables = Seq("payload")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x", "payload")
        .eager()
        .eager()
        .input(nodes = Seq("x"), variables = Seq("payload"))
        .build(),
      input = {
        val data = (0 until n).map(i => Array[Any](nodes(i), payload(i))).toArray
        finiteSeqInput(data)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(5.0),
    )
  }

  test("measure aggregation percentileCont without grouping") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .aggregation(Seq.empty, Seq("percentileCont(x,0.50) AS y"))
        .input(variables = Seq("x"))
        .build(),
      input = withRandom(random => finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(_ => Array[Any](random.nextInt(10000)))),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure aggregation percentileCont with grouping") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y", "z")
        .aggregation(Seq("y AS y"), Seq("percentileCont(x,0.20) AS z"))
        .input(variables = Seq("x", "y"))
        .build(),
      input = withRandom(random => finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](random.nextInt(10000), Math.round(i /30)))),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.3),
    )
  }

  test("measure aggregation percentileDisc without grouping") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y")
        .aggregation(Seq.empty, Seq("percentileDisc(x,0.20) AS y"))
        .input(variables = Seq("x"))
        .build(),
      input = withRandom(random => finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(_ => Array[Any](random.nextInt(10000)))),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure aggregation percentileDisc with grouping") {
    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(variables = Seq("x", "y")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("y", "z")
        .aggregation(Seq("y AS y"), Seq("percentileDisc(x,0.20) AS z"))
        .input(variables = Seq("x", "y"))
        .build(),
      input = withRandom(random => finiteGeneratedInput(DEFAULT_INPUT_SIZE.toInt)(i => Array[Any](random.nextInt(10000), Math.round(i / 30)))),
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = ErrorFractionTolerance(0.3),
    )
  }

  test("measure left outer hash join") {
    val nodes = given { nodeGraph(DEFAULT_INPUT_SIZE.toInt) }

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .leftOuterHashJoin("x")
        .|.allNodeScan("x")
        .input(nodes = Seq("x"))
        .build(),
      input = withRandom { random =>
        val data = nodes.map(Array[Any](_))
        val shuffledData = random.shuffle(data).toArray
        finiteSeqInput(shuffledData)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }

  test("measure right outer hash join") {
    val nodes = given { nodeGraph(DEFAULT_INPUT_SIZE.toInt) }

    validateMaxAllocatedMemoryEstimate(
      baselineQuery = passThoughQuery(nodes = Seq("x")),
      measuredQuery = new LogicalQueryBuilder(this)
        .produceResults("x")
        .rightOuterHashJoin("x")
        .|.allNodeScan("x")
        .input(nodes = Seq("x"))
        .build(),
      input = withRandom { random =>
        val data = nodes.map(Array[Any](_))
        val shuffledData = random.shuffle(data).toArray
        finiteSeqInput(shuffledData)
      },
      measuringStrategy = HeapDumpAtEstimateHighWaterMarkInputOffset,
      baselineStrategy = Some(HeapDumpAtInputOffset(DEFAULT_INPUT_SIZE / 2)),
      tolerance = DEFAULT_TOLERANCE,
    )
  }
}
