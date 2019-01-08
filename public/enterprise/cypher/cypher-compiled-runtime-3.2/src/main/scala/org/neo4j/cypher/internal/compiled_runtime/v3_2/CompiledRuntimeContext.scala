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
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilerContext
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.{Metrics, QueryGraphSolver}
import org.neo4j.cypher.internal.compiler.v3_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_2.{CypherCompilerConfiguration, UpdateStrategy}
import org.neo4j.cypher.internal.frontend.v3_2.phases.{CompilationPhaseTracer, InternalNotificationLogger, Monitors}
import org.neo4j.cypher.internal.frontend.v3_2.{CypherException, InputPosition}

class CompiledRuntimeContext(override val exceptionCreator: (String, InputPosition) => CypherException,
                             override val tracer: CompilationPhaseTracer,
                             override val notificationLogger: InternalNotificationLogger,
                             override val planContext: PlanContext,
                             override val typeConverter: RuntimeTypeConverter,
                             override val createFingerprintReference: Option[PlanFingerprint] => PlanFingerprintReference,
                             override val monitors: Monitors,
                             override val metrics: Metrics,
                             override val queryGraphSolver: QueryGraphSolver,
                             override val config: CypherCompilerConfiguration,
                             override val updateStrategy: UpdateStrategy,
                             override val debugOptions: Set[String],
                             override val clock: Clock,
                             val codeStructure: CodeStructure[GeneratedQuery])
  extends CompilerContext(exceptionCreator, tracer,
    notificationLogger, planContext, typeConverter, createFingerprintReference, monitors, metrics, queryGraphSolver,
    config, updateStrategy, debugOptions, clock)
