/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.client.model.Neo4jConfig
import com.neo4j.bench.micro.config.{BenchmarkEnabled, ParamValues}
import com.neo4j.bench.micro.data.Plans._
import com.neo4j.bench.micro.data.PointGenerator.{grid, random}
import com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL
import com.neo4j.bench.micro.data._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.graphdb.spatial.Point
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings.{space_filling_curve_bottom_threshold, space_filling_curve_extra_levels, space_filling_curve_max_bits, space_filling_curve_top_threshold}
import org.neo4j.kernel.impl.util.ValueUtils
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.collection.mutable

@BenchmarkEnabled(true)
class FixedDensityPointIndexSeek extends AbstractSpatialBenchmark {

  @ParamValues(
    allowed = Array(Interpreted.NAME, EnterpriseInterpreted.NAME),
    base = Array(Interpreted.NAME, EnterpriseInterpreted.NAME))
  @Param(Array[String]())
  var FixedDensityPointIndexSeek_runtime: String = _

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
    allowed = Array("10", "30", "60"),
    base = Array("60"))
  @Param(Array[String]())
  var FixedDensityPointIndexSeek_maxBits: Int = _

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
  var FixedDensityPointIndexSeek_extraLevels: Int = _

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
  var FixedDensityPointIndexSeek_thresholdTop: Double = _

  /*
   *  --- Top Delta (per tile) ---
   *  > amount the threshold should be reduced by at the lowest level
   *  > [top threshold] - [delta] == [bottom threshold]
   */
  @ParamValues(
    allowed = Array("0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"),
    base = Array("0.3"))
  @Param(Array[String]())
  var FixedDensityPointIndexSeek_thresholdDelta: Double = _

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
  var FixedDensityPointIndexSeek_ratio: Double = _

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
  var FixedDensityPointIndexSeek_crs: String = _

  override def description: String = "Fixed Density Point Index Seek:\n" +
    " * simple grid data distribution\n" +
    " * fixed point density\n" +
    " * random reads"

  override def crs: CRS = CRS.from(FixedDensityPointIndexSeek_crs)

  override def dataExtentsRatio: Double = 1.0

  override def queryExtentsRatio: Double = FixedDensityPointIndexSeek_ratio

  override protected def getConfig: DataGeneratorConfig = {
    val bottomThreshold = calculateBottomThreshold(FixedDensityPointIndexSeek_thresholdTop, FixedDensityPointIndexSeek_thresholdDelta)
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(
        new PropertyDefinition(
          KEY,
          grid(dataExtentMinX, dataExtentMaxX, dataExtentMinY, dataExtentMaxY, NODE_COUNT, crs)))
      .withSchemaIndexes(new LabelKeyDefinition(LABEL, KEY))
      .isReusableStore(true)
      .withNeo4jConfig(
        Neo4jConfig
        .empty()
        .withSetting(space_filling_curve_max_bits, FixedDensityPointIndexSeek_maxBits.toString)
        .withSetting(space_filling_curve_extra_levels, FixedDensityPointIndexSeek_extraLevels.toString)
        .withSetting(space_filling_curve_top_threshold, FixedDensityPointIndexSeek_thresholdTop.toString)
        .withSetting(space_filling_curve_bottom_threshold, bottomThreshold.toString))
      .build()
  }

  override def getLogicalPlanAndSemanticTable(planContext: PlanContext): (plans.LogicalPlan, SemanticTable, List[String]) = {
    val node = astVariable("node")
    val point = astParameter("point", symbols.CTPoint)
    val distance = computeQueryDistance(crs.crs(),
      dataExtentX,
      dataExtentY,
      FixedDensityPointIndexSeek_ratio)
    val distanceLiteral = astLiteralFor(distance, DBL)
    val seekExpression = astRangeBetweenPointsQueryExpression(point, distanceLiteral)
    val indexSeek = plans.NodeIndexSeek(
      node.name,
      astLabelToken(LABEL, planContext),
      Seq(astPropertyKeyToken(KEY, planContext)),
      seekExpression,
      Set.empty)(IdGen)
    val resultColumns = List(node.name)
    val produceResults = ProduceResult(indexSeek, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    (produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: FixedDensityPointIndexSeekThreadState, rngState: RNGState, bh: Blackhole): Long = {
    import scala.collection.JavaConverters._
    val point = threadState.points.next(rngState.rng)
    val params = ValueUtils.asMapValue(mutable.Map[String, AnyRef]("point" -> point).asJava)
    val visitor = new CountVisitor(bh)
    threadState.executionResult(params, tx = threadState.tx).accept(visitor)
    assertExpectedRowCount(threadState.expectedRowCountMin, threadState.expectedRowCountMax, visitor)
  }
}

@State(Scope.Thread)
class FixedDensityPointIndexSeekThreadState {
  private val TOLERATED_ROW_COUNT_ERROR = 0.1
  var tx: InternalTransaction = _
  var executionResult: InternalExecutionResultBuilder = _
  var points: ValueGeneratorFun[Point] = _
  var expectedRowCountMin: Int = _
  var expectedRowCountMax: Int = _

  @Setup
  def setUp(benchmark: FixedDensityPointIndexSeek): Unit = {
    executionResult = benchmark.buildPlan(from(benchmark.FixedDensityPointIndexSeek_runtime))
    points = random(
      benchmark.searchExtentMinX,
      benchmark.searchExtentMaxX,
      benchmark.searchExtentMinY,
      benchmark.searchExtentMaxY,
      benchmark.crs).create()
    calculateExpectedRowCounts(benchmark)
    tx = benchmark.beginInternalTransaction()
  }

  private def calculateExpectedRowCounts(benchmark: FixedDensityPointIndexSeek): Unit = {
    val expectedRowCount = Math.round(
      benchmark.NODE_COUNT * benchmark.FixedDensityPointIndexSeek_ratio * benchmark.FixedDensityPointIndexSeek_ratio)
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
