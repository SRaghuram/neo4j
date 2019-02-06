/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher

import java.time.Clock

import org.neo4j.cypher.internal.util.v3_4.{CypherException, InputPosition}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.frontend.v3_4.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors, devNullLogger}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.runtime.vectorized.dispatcher.SingleThreadedExecutor
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen
import org.neo4j.cypher.internal.v3_4.executionplan.GeneratedQuery
import org.scalatest.mock.MockitoSugar

object ContextHelper extends MockitoSugar {
  def create(exceptionCreator: (String, InputPosition) => CypherException = (_, _) => null,
             tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING,
             notificationLogger: InternalNotificationLogger = devNullLogger,
             planContext: PlanContext,
             monitors: Monitors = mock[Monitors],
             metrics: Metrics = mock[Metrics],
             queryGraphSolver: QueryGraphSolver = mock[QueryGraphSolver],
             config: CypherCompilerConfiguration = mock[CypherCompilerConfiguration],
             updateStrategy: UpdateStrategy = mock[UpdateStrategy],
             debugOptions: Set[String] = Set.empty,
             clock: Clock = Clock.systemUTC(),
             logicalPlanIdGen: IdGen,
             codeStructure: CodeStructure[GeneratedQuery] = mock[CodeStructure[GeneratedQuery]]): EnterpriseRuntimeContext ={
    val dispatcher = new SingleThreadedExecutor(morselSize = 100000)
    new EnterpriseRuntimeContext(exceptionCreator, tracer, notificationLogger, planContext,
      monitors, metrics, config, queryGraphSolver, updateStrategy, debugOptions, clock, logicalPlanIdGen,
      codeStructure, dispatcher)
  }
}
