/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.lang.Boolean.{FALSE, TRUE}

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.CypherMorselRuntimeScheduler
import org.neo4j.cypher.internal.runtime.morsel.WorkerManagement
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, RuntimeEnvironment}
import org.neo4j.internal.kernel.api.Kernel
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler

//noinspection TypeAnnotation
object ENTERPRISE {
  private val edition = new Edition[EnterpriseRuntimeContext](
    () => new TestEnterpriseDatabaseManagementServiceBuilder(),
    (runtimeConfig, resolver, lifeSupport) => {
      val kernel = resolver.resolveDependency(classOf[Kernel])
      val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
      val txBridge = resolver.resolveDependency(classOf[ThreadToStatementContextBridge])
      val workerManager = resolver.resolveDependency(classOf[WorkerManagement])

      TracingRuntimeContextManager(
        GeneratedQueryStructure,
        NullLog.getInstance(),
        runtimeConfig,
        RuntimeEnvironment.createQueryExecutor(runtimeConfig, kernel.cursors(), txBridge, lifeSupport, workerManager),
        kernel.cursors(),
        () => new ComposingSchedulerTracer(RuntimeEnvironment.createTracer(runtimeConfig, jobScheduler, lifeSupport),
                                           new ParallelismTracer))
    },
    GraphDatabaseSettings.cypher_hints_error -> TRUE,
    GraphDatabaseSettings.cypher_morsel_size_small -> Integer.valueOf(4),
    GraphDatabaseSettings.cypher_morsel_size_big -> Integer.valueOf(4))

  val SINGLE_THREADED = edition.copyWith(GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(1),
                                         GraphDatabaseSettings.cypher_morsel_runtime_scheduler -> CypherMorselRuntimeScheduler.SINGLE_THREADED)

  val SINGLE_THREADED_NO_FUSING = edition.copyWith(GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(1),
                                                   GraphDatabaseSettings.cypher_morsel_runtime_scheduler -> CypherMorselRuntimeScheduler.SINGLE_THREADED,
                                                   GraphDatabaseSettings.cypher_morsel_fuse_operators -> FALSE)

  val PARALLEL = edition.copyWith(GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(0),
                                  GraphDatabaseSettings.cypher_morsel_runtime_scheduler -> CypherMorselRuntimeScheduler.LOCK_FREE)

  val PARALLEL_NO_FUSING = edition.copyWith(GraphDatabaseSettings.cypher_worker_count -> Integer.valueOf(0),
                                            GraphDatabaseSettings.cypher_morsel_runtime_scheduler -> CypherMorselRuntimeScheduler.LOCK_FREE,
                                            GraphDatabaseSettings.cypher_morsel_fuse_operators -> FALSE)

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
