/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import com.neo4j.test.TestCommercialGraphDatabaseFactory
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, RuntimeEnvironment}
import org.neo4j.internal.kernel.api.Kernel
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler

object ENTERPRISE {
  private val edition = new Edition[EnterpriseRuntimeContext](
    new TestCommercialGraphDatabaseFactory(),
    (runtimeConfig,resolver) => {
      val kernel = resolver.resolveDependency(classOf[Kernel])
      val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
      val txBridge = resolver.resolveDependency(classOf[ThreadToStatementContextBridge])

      TracingRuntimeContextCreator(
        GeneratedQueryStructure,
        NullLog.getInstance(),
        runtimeConfig,
        RuntimeEnvironment.createDispatcher(runtimeConfig, jobScheduler, kernel.cursors(), txBridge),
        RuntimeEnvironment.createQueryExecutor(runtimeConfig, jobScheduler, kernel.cursors(), txBridge),
        kernel.cursors(),
        // TODO not done here. tracer needs to be configurable from outside
        () => RuntimeEnvironment.createTracer(runtimeConfig, jobScheduler))
//        () => new ParallelismTracer)
    },
    GraphDatabaseSettings.cypher_hints_error -> "true",
    GraphDatabaseSettings.cypher_morsel_size -> "4")

  val SINGLE_THREADED = edition.copyWith(GraphDatabaseSettings.cypher_worker_count -> "1")

  val PARALLEL = edition.copyWith(GraphDatabaseSettings.cypher_worker_count -> "0")

  val HasEvidenceOfParallelism: ContextCondition[EnterpriseRuntimeContext] =
    ContextCondition[EnterpriseRuntimeContext](
      context =>
        if (System.getenv().containsKey("RUN_EXPERIMENTAL"))
          context.runtimeEnvironment.tracer.asInstanceOf[ParallelismTracer].hasEvidenceOfParallelism
        else true,
      "Evidence of parallelism could not be found"
    )
}
