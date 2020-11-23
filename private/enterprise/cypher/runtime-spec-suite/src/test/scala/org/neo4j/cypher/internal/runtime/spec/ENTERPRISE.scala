/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.lang.Boolean.TRUE
import java.util.concurrent.ThreadLocalRandom

import com.neo4j.configuration.MetricsSettings
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.RuntimeEnvironment
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.kernel.api.Kernel
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.scheduler.JobScheduler

//noinspection TypeAnnotation
object ENTERPRISE {
  //randomize morsel size evenly in [1, 5]
  lazy val MORSEL_SIZE = {
    ThreadLocalRandom.current().nextInt(1, 6)
  }
  val MORSEL_SIZE_FOR_SCHEDULING_TESTS = 2

  private def edition(customSchedulerTracer: Option[SchedulerTracer]) = new Edition[EnterpriseRuntimeContext](
    () => new TestEnterpriseDatabaseManagementServiceBuilder(),
    (runtimeConfig, resolver, lifeSupport, logProvider) => {
      val kernel = resolver.resolveDependency(classOf[Kernel])
      val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
      val workerManager = resolver.resolveDependency(classOf[WorkerManagement])

      val tracer = customSchedulerTracer.getOrElse(
        new ComposingSchedulerTracer(RuntimeEnvironment.createTracer(runtimeConfig, jobScheduler, lifeSupport), new ParallelismTracer))

      val runtimeEnvironment = RuntimeEnvironment.
        of(runtimeConfig, jobScheduler, kernel.cursors(), lifeSupport, workerManager, EmptyMemoryTracker.INSTANCE)
          .withSchedulerTracer(tracer)

      TracingRuntimeContextManager(
        logProvider.getLog("test"),
        runtimeConfig,
        runtimeEnvironment,
        kernel.cursors())
    },
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseInternalSettings.cypher_worker_count -> Integer.valueOf(-1),
    GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(customSchedulerTracer.fold(MORSEL_SIZE)(_ => MORSEL_SIZE_FOR_SCHEDULING_TESTS)),
    GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(customSchedulerTracer.fold(MORSEL_SIZE)(_ => MORSEL_SIZE_FOR_SCHEDULING_TESTS)),
    MetricsSettings.metrics_enabled -> java.lang.Boolean.FALSE
  )

  def WITH_FUSING(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.COMPILED)

  def WITH_NO_FUSING(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.INTERPRETED)

  def WITH_WORKERS(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseInternalSettings.cypher_worker_count -> Integer.valueOf(0))

  def WITH_MORSEL_SIZE(size: Int): Edition[EnterpriseRuntimeContext] =
    DEFAULT.copyWith(
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(size),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(size)
    )

  val DEFAULT = edition(None)

  def WITH_TRACER(tracer: SchedulerTracer): Edition[EnterpriseRuntimeContext] = edition(Some(tracer))

  val HAS_EVIDENCE_OF_PARALLELISM: ContextCondition[EnterpriseRuntimeContext] =
    ContextCondition[EnterpriseRuntimeContext](
      context => {
        val composingTracer = context.runtimeEnvironment.tracer.asInstanceOf[ComposingSchedulerTracer]
        composingTracer.inners.collectFirst {
          case x: ParallelismTracer => x.hasEvidenceOfParallelism
        }.get
      },
      "Evidence of parallelism could not be found"
    )
}
