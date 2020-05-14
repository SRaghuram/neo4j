/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.lang.Boolean.TRUE
import java.util.concurrent.ThreadLocalRandom

import com.neo4j.kernel.impl.enterprise.configuration.MetricsSettings
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.RuntimeEnvironment
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManagement
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.kernel.api.Kernel
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.scheduler.JobScheduler

//noinspection TypeAnnotation
object ENTERPRISE {
  //randomize morsel size evenly in [1, 5]
  lazy val MORSEL_SIZE = {
    ThreadLocalRandom.current().nextInt(1, 6)
  }

  private val edition = new Edition[EnterpriseRuntimeContext](
    () => new TestEnterpriseDatabaseManagementServiceBuilder(),
    (runtimeConfig, resolver, lifeSupport, logProvider) => {
      val kernel = resolver.resolveDependency(classOf[Kernel])
      val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
      val workerManager = resolver.resolveDependency(classOf[WorkerManagement])

      val runtimeEnvironment = RuntimeEnvironment.of(runtimeConfig, jobScheduler, kernel.cursors(), lifeSupport, workerManager, EmptyMemoryTracker.INSTANCE)

      TracingRuntimeContextManager(
        GeneratedQueryStructure,
        logProvider.getLog("test"),
        runtimeConfig,
        runtimeEnvironment,
        kernel.cursors(),
        () => new ComposingSchedulerTracer(RuntimeEnvironment.createTracer(runtimeConfig, jobScheduler, lifeSupport),
          new ParallelismTracer))
    },
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(-1),
    GraphDatabaseSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(MORSEL_SIZE),
    GraphDatabaseSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(MORSEL_SIZE),
    MetricsSettings.metricsEnabled -> java.lang.Boolean.FALSE
  )

  def WITH_FUSING(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseSettings.cypher_operator_engine -> GraphDatabaseSettings.CypherOperatorEngine.COMPILED)

  def WITH_NO_FUSING(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseSettings.cypher_operator_engine -> GraphDatabaseSettings.CypherOperatorEngine.INTERPRETED)

  def WITH_WORKERS(edition: Edition[EnterpriseRuntimeContext]): Edition[EnterpriseRuntimeContext] =
    edition.copyWith(GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(0))

  def WITH_MORSEL_SIZE(size: Int) =
    edition.copyWith(
      GraphDatabaseSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(size),
      GraphDatabaseSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(size)
    )

  val DEFAULT = edition

  val HAS_EVIDENCE_OF_PARALLELISM: ContextCondition[EnterpriseRuntimeContext] =
    ContextCondition[EnterpriseRuntimeContext](
      context =>
        if (System.getenv().containsKey("RUN_EXPERIMENTAL")) {
          val composingTracer = context.runtimeEnvironment.tracer.asInstanceOf[ComposingSchedulerTracer]
          composingTracer.inners.collectFirst {
            case x: ParallelismTracer => x.hasEvidenceOfParallelism
          }.get
        } else true,
      "Evidence of parallelism could not be found"
    )
}
