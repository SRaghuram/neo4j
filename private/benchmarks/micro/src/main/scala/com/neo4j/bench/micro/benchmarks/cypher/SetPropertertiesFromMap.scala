/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.util.SplittableRandom

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import com.neo4j.bench.micro.benchmarks.TxBatchWithSecurity
import com.neo4j.bench.micro.benchmarks.cypher.CypherRuntime.from
import com.neo4j.bench.micro.data.ConstantGenerator
import com.neo4j.bench.micro.data.DataGeneratorConfig
import com.neo4j.bench.micro.data.DataGeneratorConfigBuilder
import com.neo4j.bench.micro.data.NumberGenerator
import com.neo4j.bench.micro.data.Plans.IdGen
import com.neo4j.bench.micro.data.Plans.astLiteralFor
import com.neo4j.bench.micro.data.Plans.astMap
import com.neo4j.bench.micro.data.Plans.astParameter
import com.neo4j.bench.micro.data.Plans.astVariable
import com.neo4j.bench.micro.data.PropertyDefinition
import com.neo4j.bench.micro.data.TypeParamValues.LNG
import com.neo4j.bench.micro.data.ValueGeneratorFactory
import com.neo4j.bench.micro.data.ValueGeneratorFun
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.MapValue
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
class SetPropertiesFromMap extends AbstractCypherBenchmark {
  @ParamValues(
    allowed = Array(Interpreted.NAME, Slotted.NAME, Pipelined.NAME, Parallel.NAME),
    base = Array(Slotted.NAME)
  )
  @Param(Array[String]())
  var runtime: String = _

  @ParamValues(
    allowed = Array("1", "10", "100", "1000", "10000"),
    base = Array("100")
  )
  @Param(Array[String]())
  var txSize: Int = _

  @ParamValues(
    allowed = Array("1", "2", "4", "16"),
    base = Array("4"))
  @Param(Array[String]())
  var propertyCountSet: Int = _

  @ParamValues(
    allowed = Array("0", "4", "16"),
    base = Array("4"))
  @Param(Array[String]())
  var propertyCountRemove: Int = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true")
  )
  @Param(Array[String]())
  var removeOtherProps: Boolean = _

  @ParamValues(
    allowed = Array("true", "false"),
    base = Array("true")
  )
  @Param(Array[String]())
  var auth: Boolean = _

  @ParamValues(
    allowed = Array("full", "white", "black"),
    base = Array("full", "white", "black")
  )
  @Param(Array[String]())
  var user: String = _

  override def description = "Set properties from map"

  val NODE_COUNT = 1000000

  private lazy val originalProperties: Array[PropertyDefinition] =
    Array.range(0, propertyCountSet + propertyCountRemove)
      .map(i => new PropertyDefinition(s"prop_$i", ConstantGenerator.constant(LNG, 1L).asInstanceOf[ValueGeneratorFactory[_]]))
  private lazy val propertySetKeys = Seq.range(0, propertyCountSet).map(i => s"prop_$i")

  override protected def getConfig: DataGeneratorConfig =
    new DataGeneratorConfigBuilder()
      .withNodeCount(NODE_COUNT)
      .withNodeProperties(originalProperties: _*)
      .isReusableStore(false)
      .withNeo4jConfig(Neo4jConfigBuilder.empty()
        .withSetting(GraphDatabaseSettings.auth_enabled, auth.toString).build())
      .build()

  override def setup(planContext: PlanContext): TestSetup = {
    val node = "node"
    val idParam = astParameter("id", symbols.CTNumber)
    val idSeek = plans.NodeByIdSeek(node, SingleSeekableArg(idParam), Set.empty)(IdGen)
    val propMap = astMap(propertySetKeys.map((_, astLiteralFor(2L, LNG))):_*)
    val setProperties = plans.SetPropertiesFromMap(idSeek, astVariable(node), propMap, removeOtherProps)(IdGen)
    val empty = plans.EmptyResult(setProperties)(IdGen)
    val produceResult = ProduceResult(empty, Seq.empty)(IdGen)
    val table = SemanticTable().addNode(astVariable(node))

    TestSetup(produceResult, table, List.empty)
  }

  var subscriber: CountSubscriber = _

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  def executePlan(threadState: SetPropertiesFromMapThreadState, bh: Blackhole, rngState: RNGState): Unit = {
    if ( null == subscriber ) subscriber = new CountSubscriber(bh)
    threadState.advance()
    val result = threadState.executablePlan.execute(params = threadState.param(rngState.rng), tx = threadState.transaction(), subscriber = subscriber)
    result.consumeAll()
    bh.consume(subscriber.count)
  }

  def database(): GraphDatabaseService = db()
}

@State(Scope.Thread)
class SetPropertiesFromMapThreadState {
  var txBatch: TxBatchWithSecurity = _
  var executablePlan: ExecutablePlan = _
  var benchmarkState: SetPropertiesFromMap = _
  var ids: ValueGeneratorFun[java.lang.Long] = _

  @Setup
  def setUp(benchmarkState: SetPropertiesFromMap): Unit = {
    ids = NumberGenerator.randLong(0, benchmarkState.NODE_COUNT).create()
    txBatch = new TxBatchWithSecurity(benchmarkState.database(), benchmarkState.txSize, benchmarkState.users(benchmarkState.user))
    executablePlan = benchmarkState.buildPlan(from(benchmarkState.runtime))
  }

  def advance(): Unit = {
    txBatch.advance()
  }

  def param(rng: SplittableRandom): MapValue = {
    val nodeId = ids.next(rng)
    ValueUtils.asMapValue(mutable.Map[String, AnyRef](
      "id" -> Long.box(nodeId)
    ).asJava)
  }

  def transaction(): InternalTransaction = txBatch.transaction()

  @TearDown
  def tearDown(): Unit = {
    txBatch.close()
  }
}

object SetPropertiesFromMap {
  def main(args: Array[String]): Unit = {
    Main.run(classOf[SetPropertiesFromMap])
  }
}
