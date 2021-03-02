/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.data.CRS
import com.neo4j.bench.data.DataGeneratorConfig
import com.neo4j.bench.data.DataGeneratorConfigBuilder
import com.neo4j.bench.data.LabelKeyDefinition
import com.neo4j.bench.data.PointGenerator.ClusterGridDefinition
import com.neo4j.bench.data.PointGenerator.circleGrid
import com.neo4j.bench.data.PropertyDefinition
import com.neo4j.bench.data.ValueGeneratorFun
import com.neo4j.bench.data.ValueGeneratorUtil.DBL
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.benchmarks.Kaboom
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.Pos
import com.neo4j.bench.micro.data.Plans.astLabelToken
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astProperty
import com.neo4j.bench.micro.data.Plans.astPropertyKeyToken
import com.neo4j.bench.micro.data.Plans.astRangeBetweenPointsQueryExpression
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.Plans.distanceFunction
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Selection
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
import scala.collection.immutable.Seq
import scala.collection.mutable

@BenchmarkEnabled(true)
class CircleDistancePointIndexSeek extends AbstractSpatialBenchmark {
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

  /*
  * --- Logical Plan ---
  *  > specifies whether or not the logical plan contains a post Filter operator
  */
  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("false"))
  @Param(Array[String]())
  var filtered: Boolean = _

  override def description: String = "Retrieves all reachable points from center of a circle:\n" +
    " * data distribution is a grid of circles\n" +
    " * circle size is parameterized\n" +
    " * number of points in each circle is constant\n" +
    " * runs with and without post filter distance function\n" +
    " * reads always start from the center of a random circle"

  override def crsSetting: CRS = CRS.from(crs)

  override def dataExtentsRatio: Double = ratio

  override def queryExtentsRatio: Double = ratio

  // allows for radius variation due to floating point errors in floating math operations (e.g., sin & cosine)
  val ROUNDING_ERROR: Double = 1.00001

  // create 100 clusters of exactly same size as query extent, nodes per cluster will be: 1,000,000 / 100 == 10,000
  val clusterCountX: Long = 5
  val clusterCountY: Long = 5
  lazy val clustersDefinition: ClusterGridDefinition = ClusterGridDefinition.from(
    searchExtentX,
    searchExtentY,
    clusterCountX,
    clusterCountY,
    dataExtentMinX,
    dataExtentMaxX,
    dataExtentMinY,
    dataExtentMaxY,
    NODE_COUNT,
    crsSetting)

  override protected def getConfig: DataGeneratorConfig = {
    val bottomThreshold = calculateBottomThreshold(thresholdTop, thresholdDelta)
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withLabels(LABEL)
      .withNodeProperties(new PropertyDefinition(KEY, circleGrid(clustersDefinition)))
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

    // circle diameter. allows for variations due to floating point errors
    val distance = ROUNDING_ERROR * Math.max(clustersDefinition.clusterSizeX(), clustersDefinition.clusterSizeY()) / 2

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

    val produceResults = if (filtered) {
      val filter = Selection(
        Seq(LessThanOrEqual(distanceFunction(astProperty(node, KEY), point), distanceLiteral)(Pos)),
        indexSeek)(IdGen)
      ProduceResult(filter, resultColumns)(IdGen)
    }
    else
      ProduceResult(indexSeek, resultColumns)(IdGen)

    val table = SemanticTable().addNode(node)

    TestSetup(produceResults, table, resultColumns)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def executePlan(threadState: CircleDistancePointIndexSeekThreadState, rngState: RNGState, bh: Blackhole): Long = {
    val point = threadState.randomPoint(rngState)
    val params = ValueUtils.asMapValue(mutable.Map[String, AnyRef]("point" -> point).asJava)
    val subscriber = new CountSubscriber(bh)
    val result = threadState.executablePlan.execute(params, tx = threadState.tx, subscriber = subscriber)
    result.consumeAll()
    assertExpectedRowCount(threadState.expectedRowCount, subscriber)
  }
}

@State(Scope.Thread)
class CircleDistancePointIndexSeekThreadState {
  var tx: InternalTransaction = _
  var executablePlan: ExecutablePlan = _
  var points: List[Point] = _
  var expectedRowCount: Int = _

  @Setup
  def setUp(benchmark: CircleDistancePointIndexSeek): Unit = {
    executablePlan = benchmark.buildPlan(from(benchmark.runtime))

    // collection of points to use as query params. contains the middle of every cluster.
    val pointsFun: ValueGeneratorFun[Point] = benchmark.clustersDefinition.clusterCenters()
    val clusterCount = benchmark.clusterCountX * benchmark.clusterCountY
    points = (0 until clusterCount.toInt).map(_ => pointsFun.next(null)).toList
    if (points.length != points.distinct.length) {
      throw new Kaboom("Point parameters list should not contain duplicates")
    }

    expectedRowCount = benchmark.clustersDefinition.pointsPerCluster().toInt
    tx = benchmark.beginInternalTransaction()
  }

  def randomPoint(rngState: RNGState): Point = points(rngState.rng.nextInt(points.length))

  @TearDown
  def tearDown(): Unit = {
    tx.close()
  }
}
