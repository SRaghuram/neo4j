/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec

import java.time.Clock

import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.morsel.execution.QueryExecutor
import org.neo4j.cypher.internal.runtime.morsel.tracing.SchedulerTracer
import org.neo4j.cypher.internal.{CypherRuntimeConfiguration, EnterpriseRuntimeContext, RuntimeContextManager, RuntimeEnvironment}
import org.neo4j.internal.kernel.api.{CursorFactory, SchemaRead}
import org.neo4j.logging.Log

case class TracingRuntimeContextManager(codeStructure: CodeStructure[GeneratedQuery],
                                        log: Log,
                                        config: CypherRuntimeConfiguration,
                                        queryExecutor: QueryExecutor,
                                        cursors: CursorFactory,
                                        newTracer: () => SchedulerTracer)
  extends RuntimeContextManager[EnterpriseRuntimeContext] {

  override def create(tokenContext: TokenContext,
                      schemaRead: SchemaRead,
                      clock: Clock,
                      debugOptions: Set[String],
                      compileExpressions: Boolean): EnterpriseRuntimeContext = {

    EnterpriseRuntimeContext(tokenContext,
                             schemaRead,
                             codeStructure,
                             log,
                             clock,
                             debugOptions,
                             config,
                             new RuntimeEnvironment(config, queryExecutor, newTracer(), cursors),
                             compileExpressions)
  }

  override def assertAllReleased(): Unit = {
    queryExecutor.assertAllReleased()
  }
}
