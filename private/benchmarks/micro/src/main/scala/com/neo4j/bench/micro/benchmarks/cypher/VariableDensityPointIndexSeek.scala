/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.CRS
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.LabelKeyDefinition
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLabelToken
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astRangeBetweenPointsQueryExpression
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.PointGenerator.grid
import com.neo4j.bench.micro.data.PointGenerator.random
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.ValueGeneratorFun
import com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.graphdb.spatial.Point
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_bottom_threshold
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_extra_levels
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.space_filling_curve_top_threshold
import org.neo4j.kernel.impl.util.ValueUtils
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

@BenchmarkEnabled(true)
class VariableDensityPointIndexSeek extends AbstractSpatialBenchmark {

  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME))
  @Param(Array[String]())
  var runtime: String = _

  /*
   * --- Maximum Levels ---
   *
   * > deepest level to go to, while doing 2D->1D mapping
   * > fewer levels:
   *   - more false positives --> more post filtering
   *   + fewer range queries against index
   * > more levels:
   *   - 2D->1D mapping takes longer (object allocations?)
   *   - more range queries to index (index querying more costly)
   *   + less false positives --> less post filtering
   */
  @ParamValues(
    allowed = Array("60"),
    base = Array("60"))
  @Param(Array[String]())
  var maxBits: Int = _

  /*
   * ============================================================================================================
   *                        Heuristic Configuration
   * ============================================================================================================
   */

  /*
   * --- General Description of Heuristic ---
   *  > parameterizes the computation of ranges to send to the spatial index
   *  > if tile overlap (with search space) is 0% stop going deeper at that tile
   *  > if tile overlap (with search space) is 100% stop going deeper at that tile
   */

  /*
   *  --- Extra Levels (applies to whole space, not per tile) ---
   *  > when [value] == 0, stop going deeper as soon as [search area] <= [tile size] at current depth
   *  > if [value] > 0, go [value] levels deeper than the level at which [search area] <= [tile size]
   */
  @ParamValues(
    allowed = Array("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
    base = Array("5"))
  @Param(Array[String]())
  var extraLevels: Int = _

  /*
   *  --- Top Threshold (per tile) ---
   *  > overrides the default "100% overlap" per-tile cutoff
   *  > top & bottom thresholds define the gradient of a linear function: [f]
   *  > top threshold decreases linearly with depth, reaches bottom threshold when [search area] <= [tile size]
   *  > the smaller the search space the greater the depth at which [f] reaches its minimum (bottom threshold)
   *  > at a given depth [f] returns the current threshold [c]
   *  > when [search space] overlaps a tile by >= [c], stop going deeper at that tile
   *
   *  * [f] has negative gradient, rather than flat line, because at greater depths the cost of stopping is lower
   *  * there are fewer false positives (tile is smaller in absolute terms)
   */
  @ParamValues(
    allowed = Array("0.5", "0.6", "0.7", "0.8", "0.9", "0.99"),
    base = Array("0.8"))
  @Param(Array[String]())
  var thresholdTop: Double = _

  /*
   *  --- Top Delta (per tile) ---
   *  > amount the threshold should be reduced by at the lowest level
   *  > [top threshold] - [delta] == [bottom threshold]
   */
  @ParamValues(
    allowed = Array("0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"),
    base = Array("0.3"))
  @Param(Array[String]())
  var thresholdDelta: Double = _

  /*
   * ============================================================================================================
   *                        Benchmark Configuration Parameters
   * ============================================================================================================
   */

  /*
   * --- Area of Query Search Space ---
   *  > specifies the percentage of index extent that the query looks in, i.e., the size of the query search space
   *  > area(query search space) == area(index extent) * ratio
   */
  @ParamValues(
    allowed = Array("0.1", "0.01", "0.001"),
    base = Array("0.1", "0.001"))
  @Param(Array[String]())
  var ratio: Double = _

  /*
   * --- Coordinate Reference System ---
   *  > PointPlacement.OutsidePlacement is not valid with CRS.Wgs84
   *  > only QueryLocation.CenterLocation is valid with CRS.Wgs84
   */
  @ParamValues(
    // TODO add wgs84
    //    allowed = Array("cartesian", "wgs84"),
    allowed = Array("cartesian"),
    base = Array("cartesian"))
  @Param(Array[String]())
  var crs: String = _

  override def description: String = "Variable Density Point Index Seek:\n" +
    " * simple grid data distribution\n" +
    " * fixed point density\n" +
    " * random reads"

  override def crsSetting: CRS = CRS.from(crs)

  override def dataExtentsRatio: Double = ratio

  override def queryExtentsRatio: Double = ratio

  override protected def getConfig: DataGeneratorConfig = {
    val bottomThreshold = calculateBottomThreshold(thresholdTop, thresholdDelta)
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(
        new PropertyDefinition(
          KEY,
          grid(dataExtentMinX, dataExtentMaxX, dataExtentMinY, dataExtentMaxY, NODE_COUNT, crsSetting)))
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .withNeo4jConfig(
        Neo4jConfigBuilder
          .empty()
          .withSetting(space_filling_curve_extra_levels, extraLevels.toString)
          .withSetting(space_filling_curve_top_threshold, thresholdTop.toString)
          .withSetting(space_filling_curve_bottom_threshold, bottomThreshold.toString)
          .build())
      .build()
  }

  override def setup(planContext: PlanContext): TestSetup = {
    val node = astVariable("node")
    val point = astParameter("point", symbols.CTPoint)
    val distance = computeQueryDistance(crsSetting.crs(),
      dataExtentX,
      dataExtentY,
      ratio)
    val distanceLiteral = astLiteralFor(distance, DBL)
    val seekExpression = astRangeBetweenPointsQueryExpression(point, distanceLiteral)
    val indexSeek = plans.NodeIndexSeek(
      node.name,
      astLabelToken(LABEL, planContext),
      Seq(IndexedProperty(astPropertyKeyToken(KEY, planContext), DoNotGetValue)),
      seekExpression,
      Set.empty,
      IndexOrderNone)(IdGen)
    val resultColumns = List(node.name)
    val produceResults = ProduceResult(indexSeek, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: VariableDensityPointIndexSeekThreadState, rngState: RNGState, bh: Blackhole): Long = {
    val point = threadState.points.next(rngState.rng)
    val params = ValueUtils.asMapValue(mutable.Map[String, AnyRef]("point" -> point).asJava)
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(threadState.expectedRowCountMin, threadState.expectedRowCountMax, subscriber)
  }
}

@State(Scope.Thread)
class VariableDensityPointIndexSeekThreadState {
  private val TOLERATED_ROW_COUNT_ERROR = 0.1
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var points: ValueGeneratorFun[Point] = _
  var expectedRowCountMin: Int = _
  var expectedRowCountMax: Int = _

  @Setup
  def setUp(benchmark: VariableDensityPointIndexSeek): Unit = {
    executablePlan = benchmark.buildPlan(from(benchmark.runtime))
    points = random(
      benchmark.searchExtentMinX,
      benchmark.searchExtentMaxX,
      benchmark.searchExtentMinY,
      benchmark.searchExtentMaxY,
      benchmark.crsSetting).create()
    calculateExpectedRowCounts(benchmark)
    tx = benchmark.beginInternalTransaction()
  }

  private def calculateExpectedRowCounts(benchmark: VariableDensityPointIndexSeek): Unit = {
    val expectedRowCount = Math.round(
      benchmark.NODE_COUNT * benchmark.ratio * benchmark.ratio)
    // for very small search spaces tolerance of 10% is meaningless, never set tolerance lower than 10 rows
    val tolerance = Math.max(expectedRowCount * TOLERATED_ROW_COUNT_ERROR, 10)
    // never set min below 0
    expectedRowCountMin = Math.max(0, Math.round(expectedRowCount - tolerance)).toInt
    expectedRowCountMax = Math.round(expectedRowCount + tolerance).toInt
  }

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
