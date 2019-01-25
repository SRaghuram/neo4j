/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.plan_description.Argument
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.compiled.codegen.{CodeGenConfiguration, CodeGenerator}
import org.neo4j.cypher.internal.runtime.compiled.{CompiledPlan, projectIndexProperties}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.virtual.MapValue

object CompiledRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def name: String = "compiled"

  @throws[CantCompileQueryException]
  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext): ExecutionPlan = {
    val (newPlan, newSemanticTable) = projectIndexProperties(query.logicalPlan, query.semanticTable)

    val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
    val compiled: CompiledPlan = codeGen.generate(newPlan,
                                                  context.tokenContext,
                                                  newSemanticTable,
                                                  query.readOnly,
                                                  query.cardinalities)
    new CompiledExecutionPlan(compiled)
  }

  /**
    * Execution plan for compiled runtime. Beware: will be cached.
    */
  class CompiledExecutionPlan(val compiled: CompiledPlan) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     doProfile: Boolean,
                     params: MapValue,
                     prePopulateResults: Boolean,
                     input: InputDataStream): RuntimeResult = {

      val executionMode = if (doProfile) ProfileMode else NormalMode
      val tracer =
        if (doProfile) Some(new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider))
        else None

      compiled.executionResultBuilder(queryContext, executionMode, tracer, params, prePopulateResults)
    }

    override val runtimeName: RuntimeName = CompiledRuntimeName

    override def metadata: Seq[Argument] = compiled.executionResultBuilder.metadata

    override def notifications: Set[InternalNotification] = Set.empty
  }
}
