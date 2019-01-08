/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.compiled_runtime.v3_2.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{PlanFingerprint, PlanFingerprintReference}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.RuntimeTypeConverter
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_2.{CypherCompilerConfiguration, NotImplementedPlanContext, UpdateStrategy}
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.v3_2.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors, devNullLogger}
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition, InternalException}
import org.scalatest.mock.MockitoSugar

object CompiledRuntimeContextHelper extends MockitoSugar {
    def create(exceptionCreator: (String, InputPosition) => CypherException = (_, _) => new InternalException("apa"),
               tracer: CompilationPhaseTracer = NO_TRACING,
               notificationLogger: InternalNotificationLogger = devNullLogger,
               planContext: PlanContext = new NotImplementedPlanContext,
               typeConverter: RuntimeTypeConverter = mock[RuntimeTypeConverter],
               createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference = _ => mock[PlanFingerprintReference],
               monitors: Monitors = mock[Monitors],
               metrics: Metrics = mock[Metrics],
               queryGraphSolver: QueryGraphSolver = mock[QueryGraphSolver],
               config: CypherCompilerConfiguration = mock[CypherCompilerConfiguration],
               updateStrategy: UpdateStrategy = mock[UpdateStrategy],
               debugOptions: Set[String] = Set.empty,
               clock: Clock = Clock.systemUTC(),
               codeStructure: CodeStructure[GeneratedQuery] = mock[CodeStructure[GeneratedQuery]]): CompiledRuntimeContext = {
      new CompiledRuntimeContext(exceptionCreator, tracer, notificationLogger, planContext, typeConverter, createFingerprintReference,
        monitors, metrics, queryGraphSolver, config, updateStrategy, debugOptions, clock, codeStructure)
    }

}
