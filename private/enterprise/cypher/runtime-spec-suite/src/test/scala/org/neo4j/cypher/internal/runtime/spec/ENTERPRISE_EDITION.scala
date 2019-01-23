/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import com.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.spi.codegen.GeneratedQueryStructure
import org.neo4j.cypher.internal.{CypherConfiguration, EnterpriseRuntimeContext, EnterpriseRuntimeContextCreator, RuntimeEnvironment}
import org.neo4j.internal.kernel.api.Kernel
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.logging.NullLog
import org.neo4j.scheduler.JobScheduler

object ENTERPRISE_EDITION extends Edition[EnterpriseRuntimeContext](
  new TestEnterpriseGraphDatabaseFactory()
) {
  override def runtimeContextCreator(resolver: DependencyResolver): EnterpriseRuntimeContextCreator = {
    val runtimeConfig = CypherConfiguration.fromConfig(Config.defaults()).toCypherRuntimeConfiguration

    val runtimeEnvironment = {
      val jobScheduler = resolver.resolveDependency(classOf[JobScheduler])
      val kernel = resolver.resolveDependency(classOf[Kernel])
      val txBridge = resolver.resolveDependency(classOf[ThreadToStatementContextBridge])
      RuntimeEnvironment(runtimeConfig, jobScheduler, kernel.cursors(), txBridge)
    }

    EnterpriseRuntimeContextCreator(
      GeneratedQueryStructure,
      NullLog.getInstance(),
      runtimeConfig,
      runtimeEnvironment)
  }
}
