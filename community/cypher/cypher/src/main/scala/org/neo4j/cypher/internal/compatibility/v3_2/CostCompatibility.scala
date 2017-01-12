/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_2

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{ByteCodeMode, SourceCodeMode}
import org.neo4j.cypher.internal.spi.v3_2.codegen.GeneratedQueryStructure
import org.neo4j.cypher.{CypherCodeGenMode, CypherPlanner, CypherRuntime, CypherUpdateStrategy}
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

// TODO: Merge with the trait?
case class CostCompatibility(config: CypherCompilerConfiguration,
                             clock: Clock,
                             kernelMonitors: KernelMonitors,
                             kernelAPI: KernelAPI,
                             log: Log,
                             planner: CypherPlanner,
                             runtime: CypherRuntime,
                             codeGenMode: CypherCodeGenMode,
                             updateStrategy: CypherUpdateStrategy) extends Compatibility {

  protected val compiler = {
    val maybePlannerName = planner match {
      case CypherPlanner.default => None
      case CypherPlanner.cost | CypherPlanner.idp => Some(IDPPlannerName)
      case CypherPlanner.dp => Some(DPPlannerName)
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${planner.name}")
    }

    val maybeRuntimeName = runtime match {
      case CypherRuntime.default => None
      case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
      case CypherRuntime.compiled => Some(CompiledRuntimeName)
    }

    val maybeCodeGenMode = codeGenMode match {
      case CypherCodeGenMode.default => None
      case CypherCodeGenMode.byteCode => Some(ByteCodeMode)
      case CypherCodeGenMode.sourceCode => Some(SourceCodeMode)
    }

    val maybeUpdateStrategy = updateStrategy match {
      case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
      case _ => None
    }

    val logger = new StringInfoLogger(log)
    val monitors = WrappedMonitors(kernelMonitors)
    CypherCompilerFactory.costBasedCompiler(config, clock, GeneratedQueryStructure, monitors, logger, rewriterSequencer,
      maybePlannerName, maybeRuntimeName, maybeCodeGenMode, maybeUpdateStrategy, typeConversions)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
