/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen

import java.time.Clock

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.executionplan.GeneratedQuery
import org.neo4j.cypher.internal.executionplan.GeneratedQueryExecution
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.profiling.ProfilingTracer
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled
import org.neo4j.cypher.internal.runtime.compiled.CompiledExecutionResult
import org.neo4j.cypher.internal.runtime.compiled.CompiledPlan
import org.neo4j.cypher.internal.runtime.compiled.RunnablePlan
import org.neo4j.cypher.internal.runtime.compiled.codegen.CodeGenerator.generateCode
import org.neo4j.cypher.internal.runtime.compiled.codegen.LogicalPlanConverter.asCodeGenPlan
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructure
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.CodeStructureResult
import org.neo4j.cypher.internal.util.Eagerly
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

class CodeGenerator(val structure: CodeStructure[GeneratedQuery],
                    clock: Clock,
                    conf: CodeGenConfiguration = CodeGenConfiguration()) {


  /**
   * @param originalReturnColumns the original column names, as written in the query and without Namespacing.
   */
  def generate(plan: LogicalPlan,
               tokenContext: TokenContext,
               semanticTable: SemanticTable,
               readOnly: Boolean,
               cardinalities: Cardinalities,
               originalReturnColumns: Seq[String]
              ): CompiledPlan = {
    plan match {
      case res: ProduceResult =>
        val query: CodeStructureResult[GeneratedQuery] = try {
          generateQuery(plan, semanticTable, res.columns, conf, cardinalities)
        } catch {
          case e: CantCompileQueryException => throw e
          case e: Exception => throw new CantCompileQueryException(e.getMessage, e)
        }

        val builder = new RunnablePlan {
          def apply(queryContext: QueryContext,
                    execMode: ExecutionMode,
                    tracer: Option[ProfilingTracer],
                    params: MapValue,
                    prePopulateResults: Boolean,
                    subscriber: QuerySubscriber): RuntimeResult = {

            val execution: GeneratedQueryExecution = query.query.execute(queryContext,
              tracer.getOrElse(QueryProfiler.NONE), params)
            new CompiledExecutionResult(queryContext, execution, tracer.getOrElse(QueryProfile.NONE), prePopulateResults, subscriber, originalReturnColumns.toArray)
          }

          def metadata: Seq[Argument] = query.code
        }

        compiled.CompiledPlan(updating = false, res.columns, builder)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  /**
   * @param columns the column names, which may have been changes by the Namespacer
   */
  private def generateQuery(plan: LogicalPlan, semantics: SemanticTable,
                            columns: Seq[String], conf: CodeGenConfiguration, cardinalities: Cardinalities): CodeStructureResult[GeneratedQuery] = {
    val lookup = columns.indices.map(i => columns(i) -> i).toMap
    implicit val context = new CodeGenContext(semantics, lookup)
    val (_, instructions) = asCodeGenPlan(plan).produce(context, cardinalities)
    generateCode(structure)(instructions, context.operatorIds.toMap, columns, conf)
  }

  private def javaValue(value: Any): Object = value match {
    case null => null
    case iter: Seq[_] => iter.map(javaValue).asJava
    case iter: scala.collection.Map[_, _] => Eagerly.immutableMapValues(iter, javaValue).asJava
    case x: Any => x.asInstanceOf[AnyRef]
  }
}

object CodeGenerator {
  type SourceSink = Option[(String, String) => Unit]

  def generateCode[T](structure: CodeStructure[T])(instructions: Seq[Instruction],
                                                   operatorIds: Map[String, Id],
                                                   columns: Seq[String],
                                                   conf: CodeGenConfiguration)(implicit context: CodeGenContext): CodeStructureResult[T] = {
    structure.generateQuery(Namer.newClassName(), columns, operatorIds, conf) { accept =>
      instructions.foreach(insn => insn.init(accept))
      instructions.foreach(insn => insn.body(accept))
    }
  }
}
