/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.pipeline

import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.planner.spi.{CostBasedPlannerName, ProcedureSignatureResolver}
import org.neo4j.cypher.internal.planning.WrappedMonitors
import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticFeature._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticErrorDef, SemanticState}
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.rewriting.rewriters.{GeneratingNamer, Never, expandStar}
import org.neo4j.cypher.internal.rewriting.{Deprecations, RewriterStepSequencer}
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.monitoring.{Monitors => KernelMonitors}


object Pipeline {

  case class Instance(
    kernelMonitors: KernelMonitors,
    queryText: String,
    signatures: ProcedureSignatureResolver
  ) {
    private val monitors = WrappedMonitors(kernelMonitors)
    private val exceptionFactory = Neo4jCypherExceptionFactory(queryText, None)
    private val context: BaseContext = new BlankBaseContext(exceptionFactory, monitors)

    val parseAndPrepare = ParsingPipeline(Seq(
      parse,
      deprecations,
      prepare,
      semantics,
      fabricPrepare(signatures),
      semantics
    ), context)

    val checkAndFinalize = AnalysisPipeline(Seq(
      prepare,
      semantics,
      namespace,
      rewrite
    ), context)
  }

  class BlankBaseContext(
    val cypherExceptionFactory: CypherExceptionFactory,
    val monitors: Monitors,
    val tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING,
    val notificationLogger: InternalNotificationLogger = devNullLogger
  ) extends BaseContext {

    override val errorHandler: Seq[SemanticErrorDef] => Unit =
      (errors: Seq[SemanticErrorDef]) =>
        errors.foreach(e => throw cypherExceptionFactory.syntaxException(e.msg, e.position))
  }

  private val features = Seq(
    Cypher9Comparability,
    MultipleDatabases,
    MultipleGraphs,
    UseGraphSelector,
    CorrelatedSubQueries,
    ExpressionsInViewInvocations
  )

  private val parse: Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement])

  private val deprecations =
    SyntaxDeprecationWarnings(Deprecations.V2)

  private val semantics =
    SemanticAnalysis(warn = true, features: _*).adds(BaseContains[SemanticState])

  private val prepare =
    PreparatoryRewriting(Deprecations.V2)

  private def fabricPrepare(signatures: ProcedureSignatureResolver) =
    FabricPreparatoryRewriting(signatures)

  private val namespace =
    Namespacer

  private val rewrite =
    AstRewriting(
      sequencer = RewriterStepSequencer.newPlain,
      literalExtraction = Never,
      innerVariableNamer = new GeneratingNamer)
}

trait Pipeline {
  val parts: Seq[Transformer[BaseContext, BaseState, BaseState]]
  val context: BaseContext

  val transformer: Transformer[BaseContext, BaseState, BaseState] = parts.reduce(_ andThen _)
}

case class ParsingPipeline(
  parts: Seq[Transformer[BaseContext, BaseState, BaseState]],
  context: BaseContext
) extends Pipeline {
  def process(query: String): BaseState = {
    transformer.transform(InitialState(query, None, null), context)
  }
}

case class AnalysisPipeline(
  parts: Seq[Transformer[BaseContext, BaseState, BaseState]],
  context: BaseContext
) extends Pipeline {
  def process(query: Statement): BaseState = {
    transformer.transform(InitialState("", None, CostBasedPlannerName.default).withStatement(query), context)
  }
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
