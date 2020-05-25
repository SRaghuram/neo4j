/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.util.Random

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.SlottedRuntime
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.runtime.spec.LogicalPlanFuzzTesting.beSameResultAs
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.util.DefaultValueMapper
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.RandomValues
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Signaler
import org.scalatest.concurrent.ThreadSignaler
import org.scalatest.concurrent.TimeLimits
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.time.Seconds
import org.scalatest.time.Span

import scala.util.Failure
import scala.util.Success
import scala.util.Try

// This is still an early prototype, and there are a number of unsolved problems
// * How to generate the graph
// * How to  generate valid plans (or at x % of valid plans) where the invalid ones could be a result of other erros, not system errors
// Also it only generates very few plans right now.

// Class name not ending in `Test` to make sure `mvn test` won't run it
class LogicalPlanFuzzTesting extends CypherFunSuite with BeforeAndAfterAll with TimeLimits {

  private val managementService = ENTERPRISE.DEFAULT.newGraphManagementService()
  private val graphDb = managementService.database(DEFAULT_DATABASE_NAME)
  private val logProvider: AssertableLogProvider = new AssertableLogProvider()
  private val runtimeTestSupport = new RuntimeTestSupport[EnterpriseRuntimeContext](graphDb, ENTERPRISE.WITH_FUSING(ENTERPRISE.DEFAULT), false, logProvider) with GraphCreation[EnterpriseRuntimeContext] {
    override protected def runtimeTestSupport: RuntimeTestSupport[EnterpriseRuntimeContext] = this
  }

  private val runtimes = Seq(InterpretedRuntime, SlottedRuntime, PIPELINED/*, PARALLEL*/)

  private val (tx, txContext) = {
    // Create the data (all executors use the same database instance)
    runtimeTestSupport.start()
    runtimeTestSupport.startTx()
    LogicalPlanFuzzTesting.createData(runtimeTestSupport)
    runtimeTestSupport.tx.commit()
    runtimeTestSupport.stopTx()

    runtimeTestSupport.startTx()
    (runtimeTestSupport.tx, runtimeTestSupport.txContext)
  }

  override protected def afterAll(): Unit = {
    txContext.close()
    tx.close()
    runtimeTestSupport.stop()
    managementService.shutdown()
  }

  private val initialSeed = Seed.random() // use `Seed.fromBase64(...).get` to reproduce test failures
  private val maxCost = Cost(sys.env.getOrElse("LOGICAL_PLAN_FUZZ_MAX_COST", "1000000").toInt)
  private val iterationCount = sys.env.getOrElse("LOGICAL_PLAN_FUZZ_ITERATIONS", "100").toInt
  private val maxIterationTimeSpan = Span(
    sys.env.getOrElse("LOGICAL_PLAN_FUZZ_MAX_ITERATION_TIME_SECONDS", "60").toInt,
    Seconds)

  private val generator = LogicalQueryGenerator.logicalQuery(txContext, maxCost)

  Range(0, iterationCount).foldLeft(initialSeed) {
    (seed: Seed, iter: Int) =>
      test(s"[${iter + 1}/$iterationCount] seed = ${seed.toBase64}") {
        runTest(seed, generator)
      }
      Seed.random()
  }

  private def runTest(seed: Seed, generator: Gen[WithState[LogicalQuery]]): Unit = {
    val WithState(logicalQuery, state) = generator.pureApply(Gen.Parameters.default, seed)
    val plan = logicalQuery.logicalPlan
    val randVals = RandomValues.create(new Random(seed.long._1))
    val parameters = state.parameters.map(_ -> randVals.nextValue().asObject()).toMap

    val cost = CardinalityCostModel(logicalQuery.logicalPlan, QueryGraphSolverInput.empty, logicalQuery.cardinalities)

    val clues = Seq(
      s"plan = $plan",
      s"parameters = $parameters",
      s"cost = $cost",
      s"seed = $seed.get"
    )

    withClue(clues.mkString("", "\n", "\n")) {
      runtimeTestSupport.startTx()

      try {
        val results = {
          // If the reference runtime does not finish in time, let's just ignore this case.
          withCluesCancelAfter(maxIterationTimeSpan, s"runtime = ${runtimes.head.name}") {
            Try(runtimeTestSupport.executeAndConsumeTransactionally(logicalQuery, runtimes.head, parameters))
          }
        } +: runtimes.tail.map {
          runtime =>
            // If other runtimes do not finish in time, but the reference runtime did, let's fail.
            Try(resultFailingIfHangs(logicalQuery, runtime, parameters))
        }

        results.zip(runtimes).foreach {
          case (Failure(_: CantCompileQueryException), _) => // OK
          case (Failure(_: CypherTypeException | _: ParameterWrongTypeException), _) => // Ignore these for now, need to generate parameters of appropriate types first
          case (Failure(e), runtime) =>
            unexpectedFailure(runtime.name, plan, e)
          case (Success(_), _) => // checking `Failure`s first
        }

        val referenceResult = results.head

        results.zip(runtimes).tail.foreach {
          case (Success(result), runtime) if referenceResult.isSuccess =>
            compareResults(runtime.name, plan, result, referenceResult.get)
          case (Success(_), runtime) if referenceResult.isFailure =>
            unexpectedSuccess(runtimes.head.name, runtime.name, plan, referenceResult.failed.get)
          case (Failure(_), _) => // already checked
        }
      } finally {
        runtimeTestSupport.stopTx()
      }
    }
  }

  private def unexpectedSuccess(failedIn: String, succeededIn: String, plan: LogicalPlan, t: Throwable): Unit = {
    val testCase = testCaseString(plan,
      s"a[${t.getClass.getSimpleName}] should be thrownBy {",
      """consume(runtimeResult)
        |}
        |""".stripMargin
    )
    withClue(testCase) {
      fail(s"Failed in $failedIn, but succeeded in $succeededIn", t)
    }
  }


  private def unexpectedFailure(failedIn: String, plan: LogicalPlan, t: Throwable): Unit = {
    val testCase = testCaseString(plan,"",
      """consume(runtimeResult)
        |
        |// should succeed
        |""".stripMargin
    )
    withClue(testCase) {
      fail(s"Error in $failedIn", t)
    }
  }

  private def resultFailingIfHangs(logicalQuery: LogicalQuery, runtime: CypherRuntime[EnterpriseRuntimeContext], parameters: Map[String, AnyRef]): IndexedSeq[Array[AnyValue]] = {
    val testCase = testCaseString(logicalQuery.logicalPlan, "",
      """consume(runtimeResult)
        |
        |// should complete
        |""".stripMargin
    )
    withCluesFailAfter(maxIterationTimeSpan, s"runtime = $runtime", testCase) {
      runtimeTestSupport.executeAndConsumeTransactionally(logicalQuery, runtime, parameters)
    }
  }

  private def compareResults(runtime: String, plan: LogicalPlan, result: IndexedSeq[Array[AnyValue]], expectedResult: IndexedSeq[Array[AnyValue]]): Unit = {
    val columns = plan.asInstanceOf[ProduceResult].columns.toIndexedSeq
    // This can get very large, so we split in multiple methods to not exceed JVM method size limit.
    val expectedRowMethods = expectedResult.map(row => row.map(_.map(new DefaultValueMapper(tx))).map {
      // TODO revisit this valueMapper when we have non-node/non-rel columns in ProduceResults
      case n:Node => s"tx.getNodeById(${n.getId})"
      case r:Relationship => s"tx.getRelationshipById(${r.getId})"
      case null => "null"
      case x => x.toString
    }).zipWithIndex.map {
      case (row, index) => s"def row_$index = ${row.mkString("Array(", ", ", ")")}"
    }.mkString("\n")

    val testCase = testCaseString(plan, "",
      s"""val expected = ${expectedResult.indices.map(i => s"row_$i")}
        |runtimeResult should beColumns(${columns.mkString("\"", "\", \"", "\"")}).withRows(expected)
        |""".stripMargin,
      expectedRowMethods
    )
    // Comparing results can take very long. We should cancel if that is the case.
    withCluesCancelAfter(maxIterationTimeSpan, s"Comparing $runtime against ${runtimes.head.name}, result.size = ${result.size}", testCase) {
      result should beSameResultAs(expectedResult, columns)
    }
  }

  private def testCaseString(plan: LogicalPlan, preExecute: String, postExecute: String, otherCode: String = ""): String = {
    s"""// To reproduce, copy this test case
       |test("test") {
       |
       |  // Change this when data creation is randomized!
       |  given {
       |    LogicalPlanFuzzTesting.createData(this)
       |  }
       |
       |  // when
       |  val logicalQuery = new LogicalQueryBuilder(this)
       |  ${LogicalPlanToPlanBuilderString(plan)}
       |
       |  // then
       |  $preExecute
       |  val runtimeResult = execute(logicalQuery, runtime)
       |  $postExecute
       |}
       |$otherCode
       |""".stripMargin
  }


  private def withCluesFailAfter[T](span: Span, clues: Any*)(f: => T): T = {
    implicit val signaler: Signaler = ThreadSignaler
    withClue(clues.mkString("", "\n", "\n")) {
      failAfter(span) {
        f
      }
    }
  }

  private def withCluesCancelAfter[T](span: Span, clues: Any*)(f: => T): T = {
    implicit val signaler: Signaler = ThreadSignaler
    withClue(clues.mkString("", "\n", "\n")) {
      cancelAfter(span) {
        f
      }
    }
  }
}

object LogicalPlanFuzzTesting {
  def createData[CONTEXT <: RuntimeContext](graphCreation: GraphCreation[CONTEXT]): Unit = {
    graphCreation.bidirectionalBipartiteGraph(10, "A", "B", "AB", "BA")
    graphCreation.nodeGraph(10, "A")
    graphCreation.nodeGraph(12, "B")
    graphCreation.nodeGraph(5, "C")
  }

  case class beSameResultAs(expected: IndexedSeq[Array[AnyValue]], columns: IndexedSeq[String]) extends Matcher[IndexedSeq[Array[AnyValue]]] {
    private val rowsMatcher = EqualInAnyOrder(expected)

    override def apply(actual: IndexedSeq[Array[AnyValue]]): MatchResult = rowsMatcher.matches(columns, actual) match {
      case RowsMatch => MatchResult(matches = true, "", "")
      case RowsDontMatch(msg) => MatchResult(matches = false, msg, "")
    }
  }
}