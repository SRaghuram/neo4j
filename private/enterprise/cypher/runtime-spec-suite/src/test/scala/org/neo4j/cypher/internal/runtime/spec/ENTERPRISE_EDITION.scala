/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import com.neo4j.test.TestCommercialGraphDatabaseFactory
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.{CypherConfiguration, CypherRuntimeConfiguration, EnterpriseRuntimeContext, RuntimeEnvironment}
import org.neo4j.internal.kernel.api.Kernel
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler

object ENTERPRISE_SINGLE_THREAD extends EnterpriseEdition(RuntimeConfig.DEFAULT.copy(workers = 1))

object ENTERPRISE_PARALLEL extends EnterpriseEdition(RuntimeConfig.DEFAULT.copy(workers = 0)) {
  val HasEvidenceOfParallelism: ContextCondition[EnterpriseRuntimeContext] =
    ContextCondition[EnterpriseRuntimeContext](
      context =>
        if (System.getenv().containsKey("RUN_EXPERIMENTAL"))
          context.runtimeEnvironment.tracer.asInstanceOf[ParallelismTracer].hasEvidenceOfParallelism
        else true,
      "Evidence of parallelism could not be found"
    )
}

object RuntimeConfig {
  val DEFAULT: CypherRuntimeConfiguration = CypherConfiguration.fromConfig(Config.defaults()).toCypherRuntimeConfiguration
}

class EnterpriseEdition(runtimeConfig: CypherRuntimeConfiguration) extends Edition[EnterpriseRuntimeContext](
  new TestCommercialGraphDatabaseFactory()
) {
  override def runtimeContextCreator(resolver: DependencyResolver): TracingRuntimeContextCreator = {
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
      () => new ParallelismTracer)
  }
}
