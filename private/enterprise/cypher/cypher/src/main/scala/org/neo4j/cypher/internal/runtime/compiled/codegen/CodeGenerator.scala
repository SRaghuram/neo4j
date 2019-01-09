/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen

import java.time.Clock
import java.util

import org.neo4j.cypher.internal.codegen.QueryExecutionTracer
import org.neo4j.cypher.internal.codegen.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.CompiledRuntimeName
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.executionplan.Provider
import org.neo4j.cypher.internal.compiler.v4_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.executionplan.{GeneratedQuery, GeneratedQueryExecution}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import org.neo4j.cypher.internal.planner.v4_0.spi.TokenContext
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir._
import org.neo4j.cypher.internal.runtime.compiled.codegen.spi.{CodeStructure, CodeStructureResult}
import org.neo4j.cypher.internal.runtime.compiled.{CompiledExecutionResult, CompiledPlan, RunnablePlan}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{Runtime, RuntimeImpl}
import org.neo4j.cypher.internal.runtime.planDescription.{Argument, InternalPlanDescription, LogicalPlan2PlanDescription}
import org.neo4j.cypher.internal.runtime.{ExecutionMode, QueryContext, compiled}
import org.neo4j.cypher.internal.v4_0.logical.plans.{LogicalPlan, ProduceResult}
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.util.Eagerly
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

class CodeGenerator(val structure: CodeStructure[GeneratedQuery],
                    clock: Clock,
                    conf: CodeGenConfiguration = CodeGenConfiguration()) {

  import CodeGenerator.generateCode

  type PlanDescriptionProvider =
          InternalPlanDescription => (Provider[InternalPlanDescription], Option[QueryExecutionTracer])

  def generate(plan: LogicalPlan,
               tokenContext: TokenContext,
               semanticTable: SemanticTable,
               readOnly: Boolean,
               cardinalities: Cardinalities
              ): CompiledPlan = {
    plan match {
      case res: ProduceResult =>
        val query: CodeStructureResult[GeneratedQuery] = try {
          generateQuery(plan, semanticTable, res.columns, conf, cardinalities)
        } catch {
          case e: CantCompileQueryException => throw e
          case e: Exception => throw new CantCompileQueryException(cause = e)
        }

        val builder = new RunnablePlan {
          def apply(queryContext: QueryContext,
                    execMode: ExecutionMode,
                    tracer: Option[ProfilingTracer],
                    params: MapValue,
                    prePopulateResults: Boolean): RuntimeResult = {

            val execution: GeneratedQueryExecution = query.query.execute(queryContext,
                                                                         tracer.getOrElse(QueryExecutionTracer.NONE), params)
            new CompiledExecutionResult(queryContext, execution, tracer.getOrElse(QueryProfile.NONE), prePopulateResults)
          }

          def metadata: Seq[Argument] = query.code
        }

        compiled.CompiledPlan(updating = false, res.columns, builder)

      case _ => throw new CantCompileQueryException("Can only compile plans with ProduceResult on top")
    }
  }

  private def generateQuery(plan: LogicalPlan, semantics: SemanticTable,
                            columns: Seq[String], conf: CodeGenConfiguration, cardinalities: Cardinalities): CodeStructureResult[GeneratedQuery] = {
    import LogicalPlanConverter._
    val lookup = columns.indices.map(i => columns(i) -> i).toMap
    implicit val context = new CodeGenContext(semantics, lookup)
    val (_, instructions) = asCodeGenPlan(plan).produce(context, cardinalities)
    generateCode(structure)(instructions, context.operatorIds.toMap, columns, conf)
  }

  private def asJavaHashMap(params: scala.collection.Map[String, Any]) = {
    val jMap = new util.HashMap[String, Object]()
    params.foreach {
      case (key, value) => jMap.put(key, javaValue(value))
    }
    jMap
  }

  import scala.collection.JavaConverters._
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
