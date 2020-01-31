/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.time.Clock

import org.neo4j.cypher.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.CypherOperatorEngineOption
import org.neo4j.cypher.internal.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.RuntimeContextManager
import org.neo4j.cypher.internal.RuntimeEnvironment
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.pipelined.tracing.SchedulerTracer
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.logging.Log

case class TracingRuntimeContextManager(codeStructure: CodeStructure[GeneratedQuery],
                                        log: Log,
                                        config: CypherRuntimeConfiguration,
                                        runtimeEnvironment: RuntimeEnvironment,
                                        cursors: CursorFactory,
                                        newTracer: () => SchedulerTracer)
  extends RuntimeContextManager[EnterpriseRuntimeContext] {

  override def create(tokenContext: TokenContext,
                      schemaRead: SchemaRead,
                      clock: Clock,
                      debugOptions: Set[String],
                      compileExpressions: Boolean,
                      materializedEntitiesMode: Boolean,
                      operatorEngine: CypherOperatorEngineOption,
                      interpretedPipesFallback: CypherInterpretedPipesFallbackOption): EnterpriseRuntimeContext = {

    EnterpriseRuntimeContext(tokenContext,
                             schemaRead,
                             codeStructure,
                             log,
                             clock,
                             debugOptions,
                             config,
                             runtimeEnvironment,
                             compileExpressions,
                             materializedEntitiesMode,
                             operatorEngine,
                             interpretedPipesFallback)
  }

  override def assertAllReleased(): Unit = {
    runtimeEnvironment.getQueryExecutor(parallelExecution = true).assertAllReleased()
  }
}
