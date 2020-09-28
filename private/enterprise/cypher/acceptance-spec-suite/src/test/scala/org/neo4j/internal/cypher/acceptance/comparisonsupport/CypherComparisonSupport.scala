/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import java.lang.Boolean.TRUE

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import cypher.features.Phase
import org.apache.commons.lang3.exception.ExceptionUtils
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.ExecutionEngineHelper.asJavaMapDeep
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.Eagerly
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherTestSupport
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Will run a query across configurations, making sure they all agree on the results and/or errors.
 *
 * For every query tested using `executeWith`, the query will be run against all configurations. Every configuration
 * is expected to either succeed or fail. When new features are added that enable queries in new configurations,
 * acceptance tests will start failing because now a configuration is succeeding that was not successful before.
 *
 * This is expected and useful - it let's us know how a change impacts how many acceptance tests now start
 * succeeding where they weren't earlier.
 */
trait CypherComparisonSupport extends AbstractCypherComparisonSupport {
  self: ExecutionEngineFunSuite =>

  override def eengineExecute(tx: InternalTransaction, query: String,
                              params: Map[String, Any]): Result = {
    executeOfficial(tx, query, params.toSeq:_*)
  }

  override def eengineExecute(query: String,
                              params: MapValue,
                              context: TransactionalContext,
                              subscriber: QuerySubscriber): QueryExecution = {
    eengine.execute(query, params, context, profile = false, prePopulate = false, subscriber)
  }

  override def makeRewinadable(in: Result): RewindableExecutionResult = RewindableExecutionResult(in)

  override def rollback[T](f: InternalTransaction => T): T = graph.rollback(f)

  override def inTx[T](f: InternalTransaction => T): T = graph.inTx(f)

  override def transactionalContext(tx: InternalTransaction, query: (String, Map[String, Any])): TransactionalContext = graph.transactionalContext(tx, query = query)

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++
    Map(GraphDatabaseSettings.cypher_hints_error -> TRUE,
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(4),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(4),
      GraphDatabaseInternalSettings.cypher_worker_count -> Integer.valueOf(if (Configs.runOnlySafeScenarios) -1 else 0)
    )
}

trait AbstractCypherComparisonSupport extends CypherFunSuite with CypherTestSupport with EnterpriseGraphDatabaseTestSupport {

  // abstract, can be defined through CypherComparisonSupport
  def eengineExecute(tx: InternalTransaction, query: String, params: Map[String, Any]): Result

  def eengineExecute(query: String,
                     params: MapValue,
                     context: TransactionalContext,
                     subscriber: QuerySubscriber): QueryExecution

  def makeRewinadable(in:Result): RewindableExecutionResult

  def rollback[T](f: InternalTransaction => T): T

  def inTx[T](f:InternalTransaction => T): T

  def transactionalContext(tx: InternalTransaction, query: (String, Map[String, Any])): TransactionalContext

  def kernelMonitors: Monitors

  // Concrete stuff

  /**
   * Get rid of Arrays and java.util.Map to make it easier to compare results by equality.
   */
  implicit class RichInternalExecutionResults(res: RewindableExecutionResult) {
    def toComparableResultWithOptions(replaceNaNs: Boolean): Seq[Map[String, Any]] = res.toList.toComparableSeq(replaceNaNs)

    def toComparableResult: Seq[Map[String, Any]] = res.toList.toComparableSeq(replaceNaNs = false)
  }

  implicit class RichMapSeq(res: Seq[Map[String, Any]]) {

    object NanReplacement

    def toComparableSeq(replaceNaNs: Boolean): Seq[Map[String, Any]] = {
      def convert(v: Any): Any = v match {
        case a: Array[_] => a.toList.map(convert)
        case m: Map[_, _] =>
          Eagerly.immutableMapValues(m, convert)
        case m: java.util.Map[_, _] =>
          Eagerly.immutableMapValues(m.asScala, convert)
        case l: java.util.List[_] => l.asScala.map(convert)
        case d: java.lang.Double if replaceNaNs && java.lang.Double.isNaN(d) => NanReplacement
        case m => m
      }

      res.map((map: Map[String, Any]) => map.map {
        case (k, v) => k -> convert(v)
      })
    }
  }

  override protected def initTest() {
    super.initTest()
    kernelMonitors.addMonitorListener(NewPlannerMonitor)
  }

  protected def failWithError(expectedSpecificFailureFrom: TestConfiguration,
                              query: String,
                              message: Seq[String] = Seq.empty,
                              errorType: Seq[String] = Seq.empty,
                              params: Map[String, Any] = Map.empty): Unit =
    validateError(expectedSpecificFailureFrom, innerExecuteTransactionally(_, params), query, message, errorType)

  protected def failWithErrorOnTx(expectedSpecificFailureFrom: TestConfiguration,
                              transaction: Transaction,
                              query: String,
                              message: String,
                              params: Map[String, Any] = Map.empty): Unit =
    validateError(expectedSpecificFailureFrom, transaction.execute(_, params.mapValues(_.asInstanceOf[Object]).asJava), query, Seq(message), Seq.empty)

  private def validateError[R](expectedSpecificFailureFrom: TestConfiguration,
                               executeQuery: String => R,
                               query: String,
                               message: Seq[String] = Seq.empty,
                               errorType: Seq[String] = Seq.empty): Unit = {
    val explicitlyRequestedExperimentalScenarios = expectedSpecificFailureFrom.scenarios intersect Configs.Experimental.scenarios
    val scenariosToExecute = Configs.All.scenarios ++ Configs.Experimental.scenarios
    for (thisScenario <- scenariosToExecute) {
      val expectedToFailWithSpecificMessage = expectedSpecificFailureFrom.containsScenario(thisScenario)
      val silentUnexpectedSuccess = shouldSilenceUnexpectedSuccess(thisScenario, explicitlyRequestedExperimentalScenarios)
      val tryResult = Try(executeQuery(s"CYPHER ${thisScenario.preparserOptions} $query"))
    tryResult match {
      case Success(_) =>
        if (expectedToFailWithSpecificMessage) {
          fail("Unexpectedly Succeeded in " + thisScenario.name)
        }
      // It was not expected to fail with the specified error message, do nothing
      case Failure(e: Throwable) =>
        val actualErrorType = e.toString
        if (expectedToFailWithSpecificMessage) {
          if (!correctError(actualErrorType, errorType)) {
            fail("Correctly failed in " + thisScenario.name + " but instead of one the given error types, the error was '" + actualErrorType + "'", e)
          }
          if (!correctError(e.getMessage, message)) {
            fail("Correctly failed in " + thisScenario.name + " but instead of one of the given messages, the error message was '" + e.getMessage + "'", e)
          }
        } else {
          if (correctError(e.getMessage, message) && correctError(actualErrorType, errorType)) {
            if (!silentUnexpectedSuccess) {
              fail("Unexpectedly (but correctly!) failed in " + thisScenario.name + " with the correct error. Did you forget to add this config?", e)
            }
          }
        }
    }
    }
  }

  /**
   * Execute query and dump the result into a string.
   */
  protected def dumpToString(query: String,
                             params: Map[String, Any] = Map.empty): String = {
    inTx({ tx =>
      val result = Try(eengineExecute(tx, query, params).resultAsString())

      if (!result.isSuccess) fail(s"Failed to execute ´$query´")

      result.get
    })
  }

  /**
   * Execute a query with different pre-parser options and
   * assert which configurations should success and which ones should fail.
   *
   * @param expectSucceed the scenarios that are expected to support this query
   * @param query the query
   * @param expectedDifferentResults Scenarios which are expected to work, but return different results. This is often the case for bugfixes that have not been
   *                                 backported to older versions.
   * @param planComparisonStrategy a strategy how to compare the execution plans. Disabled by default.
   * @param resultAssertionInTx if `Some(...)` this will execute the given assertions inside of the transaction. That can be helpful is the assertions rely on
   *                            being executed in the same transaction as the query.
   * @param executeBefore This will be executed before each scenario.
   * @param executeExpectedFailures if you set this to `false`, queries that expect an error won't even be run.
   * @param params query parameters
   */
  protected def executeWith(expectSucceed: TestConfiguration,
                            query: String,
                            expectedDifferentResults: TestConfiguration = Configs.Empty,
                            planComparisonStrategy: PlanComparisonStrategy = DoNotComparePlans,
                            resultAssertionInTx: Option[RewindableExecutionResult => Unit] = None,
                            executeBefore: InternalTransaction => Unit = _ => {},
                            executeExpectedFailures: Boolean = true,
                            params: Map[String, Any] = Map.empty,
                            printPlanDescription: Boolean = false): RewindableExecutionResult = {
    if (printPlanDescription) {
      val result = executeSingle(s"EXPLAIN $query", params)
      println(result.executionPlanDescription())
    }
    if (expectSucceed.scenarios.nonEmpty) {
      val compareResults = expectSucceed - expectedDifferentResults
      val baseScenario = extractBaseScenario(expectSucceed, compareResults)
      val explicitlyRequestedExperimentalScenarios = expectSucceed.scenarios intersect Configs.Experimental.scenarios

      val positiveResults = ((Configs.All.scenarios ++ Configs.Experimental.scenarios) - baseScenario).flatMap {
        thisScenario =>
          executeScenario(thisScenario,
                          query,
                          expectSucceed.containsScenario(thisScenario),
                          executeBefore,
                          params,
                          resultAssertionInTx,
                          executeExpectedFailures,
                          silentUnexpectedSuccess = shouldSilenceUnexpectedSuccess(thisScenario, explicitlyRequestedExperimentalScenarios))
      }

      //Must be run last and have no rollback to be able to do certain result assertions
      val baseOption = executeScenario(baseScenario,
                                       query,
                                       expectedToSucceed = true,
                                       executeBefore,
                                       params,
                                       resultAssertionInTx = None,
                                       executeExpectedFailures = false,
                                       shouldRollback = false)

      // Assumption: baseOption.get is safe because the baseScenario is expected to succeed
      val baseResult = baseOption.get._2
      //must also check planComparisonStrategy on baseScenario
      planComparisonStrategy.compare(expectSucceed, baseScenario, baseResult)

      positiveResults.foreach {
        case (scenario, result) =>
          planComparisonStrategy.compare(expectSucceed, scenario, result)

          if (compareResults.containsScenario(scenario)) {
            assertResultsSame(result, baseResult, query, s"${scenario.name} returned different results than ${baseScenario.name}")
          } else {
            assertResultsNotSame(result, baseResult, query, s"Unexpectedly (but correctly!)\n${scenario.name} returned same results as ${baseScenario.name}")
          }
      }
      baseResult
    } else {
      /**
       * If we are ending up here we don't expect any config to succeed i.e. Configs.Empty was used.
       * Currently this only happens when we use a[xxxException] should be thrownBy...
       * Consider to not allow this, but always use failWithError instead.
       * For now, don't support plan comparisons and only run som default config without a transaction to get a result.
       */
      if (planComparisonStrategy != DoNotComparePlans) {
        fail("At least one scenario must be expected to succeed to be able to compare plans")
      }

      val baseScenario = TestScenario(Planners.Cost, Runtimes.Interpreted)
      inTx(tx => executeBefore(tx))
      val baseResult = innerExecuteTransactionally(s"CYPHER ${baseScenario.preparserOptions} $query", params)
      baseResult
    }
  }

  @deprecated("Rewrite to use executeWith instead")
  protected def assertResultsSameDeprecated(result1: RewindableExecutionResult, result2: RewindableExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit =
    assertResultsSame(result1, result2, queryText, errorMsg, replaceNaNs)

  /**
   * Execute a single CYPHER query (without multiple different pre-parser options). Obtain a RewindableExecutionResult.
   */
  protected def executeSingle(queryText: String, params: Map[String, Any] = Map.empty): RewindableExecutionResult =
    innerExecuteTransactionally(queryText, params)

  private def correctError(actualError: String, possibleErrors: Seq[String]): Boolean = {
    possibleErrors == Seq.empty || (actualError != null && possibleErrors.exists(s => actualError.replaceAll("\\r", "").contains(s.replaceAll("\\r", ""))))
  }

  private def shouldSilenceUnexpectedSuccess(scenario: TestScenario, explicitlyRequestedExperimentalScenarios: Set[TestScenario]): Boolean = {
    Configs.Experimental.containsScenario(scenario) && !explicitlyRequestedExperimentalScenarios.contains(scenario)
  }

  private def extractBaseScenario(expectSucceed: TestConfiguration, compareResults: TestConfiguration): TestScenario = {
    val scenariosToChooseFrom = if (compareResults.scenarios.isEmpty) expectSucceed else compareResults

    if (scenariosToChooseFrom.scenarios.isEmpty) {
      fail("At least one scenario must be expected to succeed, to be comparable with plan and result")
    }
    val preferredScenario = TestScenario(Planners.Cost, Runtimes.Interpreted)
    if (scenariosToChooseFrom.containsScenario(preferredScenario)) {
      preferredScenario
    } else {
      scenariosToChooseFrom.scenarios.head
    }
  }

  private def executeScenario(scenario: TestScenario,
                              query: String,
                              expectedToSucceed: Boolean,
                              executeBefore: InternalTransaction => Unit,
                              params: Map[String, Any],
                              resultAssertionInTx: Option[RewindableExecutionResult => Unit],
                              executeExpectedFailures: Boolean,
                              silentUnexpectedSuccess: Boolean = false,
                              shouldRollback: Boolean = true): Option[(TestScenario, RewindableExecutionResult)] = {

    def execute(tx: InternalTransaction) = {
      executeBefore(tx)
      val queryWithPreparserOptions = s"CYPHER ${scenario.preparserOptions} $query"
      val tryRes =
        if (expectedToSucceed || executeExpectedFailures) {
          Try(innerExecute(tx, queryWithPreparserOptions, params))
        } else {
          Failure(NotExecutedException)
        }
      if (expectedToSucceed && resultAssertionInTx.isDefined) {
        tryRes match {
          case Success(thisResult) =>
            withClue(s"result in transaction for ${scenario.name}\n") {
              resultAssertionInTx.get.apply(thisResult)
            }
          case Failure(_) =>
          // No need to do anything: will be handled by match below
        }
      }
      if (expectedToSucceed) {
        tryRes match {
          case Success(thisResult) =>
            scenario.checkResultForSuccess(query, thisResult, silentUnexpectedSuccess)
            Some(scenario -> thisResult)
          case Failure(e) =>
            fail(s"Expected to succeed in ${scenario.name} but got exception\nRoot cause: ${ExceptionUtils.getRootCauseMessage(e)}\n", e)
        }
      } else {
        val maybePhase = tryRes match {
          case Success(_) => None
          case Failure(NotExecutedException) => None
          case Failure(_) =>
            // Re-run the query with EXPLAIN to determine if the failure was at compile-time or runtime
            Some(Try(innerExecuteTransactionally(s"EXPLAIN $queryWithPreparserOptions", params)) match {
              case Failure(_) => Phase.compile
              case Success(_) => Phase.runtime
            })
        }
        scenario.checkResultForFailure(query, tryRes, maybePhase, silentUnexpectedSuccess)
        None
      }
    }
    if (shouldRollback) rollback(tx => execute(tx)) else inTx(tx => execute(tx))
  }

  private def assertResultsSame(result1: RewindableExecutionResult, result2: RewindableExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit = {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsInOrderAs result2.toComparableResultWithOptions(replaceNaNs)
      } else {
        result1.toComparableResultWithOptions(replaceNaNs) should contain theSameElementsAs result2.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }

  private def assertResultsNotSame(result1: RewindableExecutionResult, result2: RewindableExecutionResult, queryText: String, errorMsg: String, replaceNaNs: Boolean = false): Unit = {
    withClue(errorMsg) {
      if (queryText.toLowerCase contains "order by") {
        result1.toComparableResultWithOptions(replaceNaNs) shouldNot contain theSameElementsInOrderAs result2.toComparableResultWithOptions(replaceNaNs)
      } else {
        result1.toComparableResultWithOptions(replaceNaNs) shouldNot contain theSameElementsAs result2.toComparableResultWithOptions(replaceNaNs)
      }
    }
  }

  private def innerExecuteTransactionally(queryText: String, params: Map[String, Any]): RewindableExecutionResult = {
    graph.withTx { tx =>
      innerExecute(tx, queryText, params)
    }
  }

  private def innerExecute(tx: InternalTransaction, queryText: String, params: Map[String, Any]) = {
    val subscriber = new RecordingQuerySubscriber
    val context = transactionalContext(tx, queryText -> params)
    val queryContext = new TransactionBoundQueryContext(TransactionalContextWrapper(context), new ResourceManager())(mock[IndexSearchMonitor])
    val innerResult = eengineExecute(queryText, ValueUtils.asParameterMapValue(asJavaMapDeep(params)), context, subscriber)
    RewindableExecutionResult(innerResult, queryContext, subscriber)
  }
}

object NewPlannerMonitor {

  sealed trait NewPlannerMonitorCall {
    def stackTrace: String
  }

  final case class UnableToHandleQuery(stackTrace: String) extends NewPlannerMonitorCall

  final case class NewQuerySeen(stackTrace: String) extends NewPlannerMonitorCall

}

object NewRuntimeMonitor {

  sealed trait NewRuntimeMonitorCall {
    def stackTrace: String
  }

  final case class UnableToCompileQuery(stackTrace: String) extends NewRuntimeMonitorCall

  final case class NewPlanSeen(stackTrace: String) extends NewRuntimeMonitorCall

}

object NotExecutedException extends Exception
