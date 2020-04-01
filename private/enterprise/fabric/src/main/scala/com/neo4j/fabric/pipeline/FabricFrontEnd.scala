/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.pipeline

import com.neo4j.fabric.planning.FabricPlan
import com.neo4j.fabric.planning.QueryRenderer
import com.neo4j.fabric.util.Errors
import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.CypherUpdateStrategy
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.NotificationWrapping
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ExpressionsInViewInvocations
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleGraphs
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseGraphSelector
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.phases.Compatibility3_5
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_0
import org.neo4j.cypher.internal.compiler.phases.Compatibility4_1
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.Condition
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.RecordingNotificationLogger
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.planning.WrappedMonitors
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.rewriting.rewriters.GeneratingNamer
import org.neo4j.cypher.internal.rewriting.rewriters.Never
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.graphdb.Notification
import org.neo4j.monitoring


case class FabricFrontEnd(
  cypherConfig: CypherConfiguration,
  kernelMonitors: monitoring.Monitors,
  signatures: ProcedureSignatureResolver,
) {

  val compilationTracer = new TimingCompilationTracer(
    kernelMonitors.newMonitor(classOf[TimingCompilationTracer.EventListener]))

  object preParsing {

    private val preParser = new PreParser(
      cypherConfig.version,
      cypherConfig.planner,
      cypherConfig.runtime,
      cypherConfig.expressionEngineOption,
      cypherConfig.operatorEngine,
      cypherConfig.interpretedPipesFallback,
      cypherConfig.queryCacheSize,
    )

    private def assertNotPeriodicCommit(options: QueryOptions): Unit =
      if (options.isPeriodicCommit) Errors.notSupported("Periodic commit")

    private def assertOptionsNotSet(options: QueryOptions): Unit = {
      def check[T](name: String, a: T, b: T): Unit =
        if (a != b) Errors.notSupported(s"Query option '$name'")

      check("version", options.version, cypherConfig.version)
      check("planner", options.planner, cypherConfig.planner)
      check("runtime", options.runtime, cypherConfig.runtime)
      check("updateStrategy", options.updateStrategy, CypherUpdateStrategy.default)
      check("expressionEngine", options.expressionEngine, cypherConfig.expressionEngineOption)
      check("operatorEngine", options.operatorEngine, cypherConfig.operatorEngine)
      check("interpretedPipesFallback", options.interpretedPipesFallback, cypherConfig.interpretedPipesFallback)
    }

    private def assertValidExecutionType(options: QueryOptions): Unit =
      executionType(options)

    def executionType(options: QueryOptions): FabricPlan.ExecutionType = options.executionMode match {
      case CypherExecutionMode.normal  => FabricPlan.Execute
      case CypherExecutionMode.explain => FabricPlan.Explain
      case CypherExecutionMode.profile => Errors.notSupported("Query option: 'PROFILE'")
    }

    def preParse(queryString: String): PreParsedQuery = {
      val query = preParser.preParseQuery(queryString)
      assertValidExecutionType(query.options)
      assertNotPeriodicCommit(query.options)
      assertOptionsNotSet(query.options)
      query
    }
  }

  case class Pipeline(
    query: PreParsedQuery,
  ) {

    private val queryString = query.statement

    def traceStart(): CompilationTracer.QueryCompilationEvent =
      compilationTracer.compileQuery(query.description)

    private val context: BaseContext = new BaseContext {
      val monitors: Monitors = WrappedMonitors(kernelMonitors)
      val tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
      val notificationLogger: InternalNotificationLogger = new RecordingNotificationLogger(Some(query.options.offset))
      val cypherExceptionFactory: CypherExceptionFactory = Neo4jCypherExceptionFactory(queryString, None)

      val errorHandler: Seq[SemanticErrorDef] => Unit = (errors: Seq[SemanticErrorDef]) =>
        errors.foreach(e => throw cypherExceptionFactory.syntaxException(e.msg, e.position))
    }

    private val compatibilityMode =
      query.options.version match {
        case CypherVersion.v3_5 => Compatibility3_5
        case CypherVersion.v4_0 => Compatibility4_0
        case CypherVersion.v4_1 => Compatibility4_1
      }

    private val semanticFeatures =
      CompilationPhases.defaultSemanticFeatures ++ Seq(
        MultipleGraphs,
        UseGraphSelector,
        ExpressionsInViewInvocations
      )

    private val parsingConfig = CompilationPhases.ParsingConfig(
      sequencer = RewriterStepSequencer.newPlain,
      innerVariableNamer = new GeneratingNamer,
      compatibilityMode = compatibilityMode,
      literalExtraction = Never,
      parameterTypeMapping = Map.empty,
      semanticFeatures = semanticFeatures,
    )

    object parseAndPrepare extends TransformerChain(
      CompilationPhases.parsing(parsingConfig),
      CompilationPhases.prepareForFabric(signatures, parsingConfig)
    ) {
      def process(): BaseState =
        transformer.transform(InitialState(queryString, None, null), context)
    }

    object checkAndFinalize extends TransformerChain(
      CompilationPhases.parsingFinal(parsingConfig)
    ) {
      def process(query: Statement): BaseState = {
        val localQueryString = QueryRenderer.render(query)
        transformer.transform(InitialState(localQueryString, None, CostBasedPlannerName.default).withStatement(query), context)
      }
    }

    def notifications: Seq[Notification] =
      context.notificationLogger.notifications
        .toSeq.map(NotificationWrapping.asKernelNotification(Some(query.options.offset)))
  }
}

abstract class TransformerChain(parts: Transformer[BaseContext, BaseState, BaseState]*) {
  val transformer: Transformer[BaseContext, BaseState, BaseState] = parts.reduce(_ andThen _)
}


case class FabricPreparatoryRewriting(
  signatures: ProcedureSignatureResolver
) extends Phase[BaseContext, BaseState, BaseState] {
  override val phase =
    CompilationPhaseTracer.CompilationPhase.AST_REWRITE

  override val description =
    "rewrite the AST into a shape that the fabric planner can act on"

  override def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(from.statement().endoRewrite(chain(
      // we need all return columns for data flow analysis between query segments
      expandStar(from.semantics()),
      TryResolveProcedures(signatures)
    )))

  private def chain[T](funcs: (T => T)*): T => T =
    funcs.reduceLeft(_ andThen _)

  override def postConditions: Set[Condition] = Set()
}
