/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compatibility.CypherRuntime
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => ExecutionPlanv3_5}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{CompiledRuntimeName, RuntimeName}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenConfiguration, CodeGenerator}
import org.neo4j.cypher.internal.runtime.compiled.{CompiledPlan, projectIndexProperties}
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.util.InternalNotification

object CompiledRuntime extends CypherRuntime[EnterpriseRuntimeContext] {

  @throws[CantCompileQueryException]
  override def compileToExecutable(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlanv3_5 = {
    val (newPlan, newSemanticTable) = projectIndexProperties(state.logicalPlan, state.semanticTable())

    val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
    val compiled: CompiledPlan = codeGen.generate(
      newPlan,
      context.tokenContext,
      newSemanticTable,
      state.plannerName,
      context.readOnly,
      state.planningAttributes.cardinalities,
      state.planningAttributes.providedOrders)
    new CompiledExecutionPlan(compiled)
  }

  /**
    * Execution plan for compiled runtime. Beware: will be cached.
    */
  class CompiledExecutionPlan(val compiled: CompiledPlan) extends ExecutionPlanv3_5 {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue): RuntimeResult = {

      val executionMode = if (doProfile) ProfileMode else NormalMode
      val tracer =
        if (doProfile) Some(new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider))
        else None

      compiled.executionResultBuilder(queryContext, executionMode, tracer, params)
    }

    override val runtimeName: RuntimeName = CompiledRuntimeName

    override def metadata: Seq[Argument] = compiled.executionResultBuilder.metadata

    override def notifications: Set[InternalNotification] = Set.empty
  }
}
