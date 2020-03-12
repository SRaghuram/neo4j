/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.profiling.ProfilingTracer
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.CompiledPlan
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenConfiguration
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenerator
import org.neo4j.cypher.internal.runtime.compiled.removeCachedProperties
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

object CompiledRuntime extends CypherRuntime[EnterpriseRuntimeContext] {
  override def name: String = "legacy_compiled"

  @throws[CantCompileQueryException]
  override def compileToExecutable(query: LogicalQuery, context: EnterpriseRuntimeContext): ExecutionPlan = {
    val (newPlan, newSemanticTable) = removeCachedProperties(query.logicalPlan, query.semanticTable)

    val codeGen = new CodeGenerator(context.codeStructure, context.clock, CodeGenConfiguration(context.debugOptions))
    val compiled: CompiledPlan = codeGen.generate(newPlan,
      context.tokenContext,
      newSemanticTable,
      query.readOnly,
      query.cardinalities,
      query.resultColumns)
    new CompiledExecutionPlan(compiled)
  }

  /**
   * Execution plan for compiled runtime. Beware: will be cached.
   */
  class CompiledExecutionPlan(val compiled: CompiledPlan) extends ExecutionPlan {

    override def run(queryContext: QueryContext,
                     executionMode: ExecutionMode,
                     params: MapValue,
                     prePopulateResults: Boolean,
                     input: InputDataStream,
                     subscriber: QuerySubscriber): RuntimeResult = {
      val doProfile = executionMode == ProfileMode
      val tracer =
        if (doProfile) Some(new ProfilingTracer(queryContext.transactionalContext.kernelStatisticProvider))
        else None

      compiled.executionResultBuilder(queryContext, executionMode, tracer, params, prePopulateResults, subscriber)
    }

    override val runtimeName: RuntimeName = CompiledRuntimeName

    override def metadata: Seq[Argument] = compiled.executionResultBuilder.metadata

    override def notifications: Set[InternalNotification] = Set.empty
  }
}
