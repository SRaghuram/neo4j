/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.pipeline

import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.planner.spi.CostBasedPlannerName
import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticFeature.{Cypher9Comparability, MultipleDatabases, MultipleGraphs}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticErrorDef, SemanticState}
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{GeneratingNamer, Never, expandStar}
import org.neo4j.cypher.internal.v4_0.rewriting.{AstRewritingMonitor, Deprecations, RewriterStepSequencer}
import org.neo4j.cypher.internal.v4_0.util.CypherExceptionFactory

import scala.reflect.ClassTag


object Pipeline {

  trait BlankBaseContext extends BaseContext {
    override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING

    override def notificationLogger: InternalNotificationLogger = devNullLogger

    override def monitors: Monitors = new Monitors {
      override def newMonitor[T <: AnyRef : ClassTag](tags: String*): T =
        new AstRewritingMonitor {
          override def abortedRewriting(obj: AnyRef): Unit = ()

          override def abortedRewritingDueToLargeDNF(obj: AnyRef): Unit = ()
        }.asInstanceOf[T]

      override def addMonitorListener[T](monitor: T, tags: String*): Unit = ()
    }
  }

  val exceptionFactory = Neo4jCypherExceptionFactory("", None)
  case class DefaultContext(cypherExceptionFactory: CypherExceptionFactory)

  private val defaultContext: BaseContext =
    new BlankBaseContext() {
      override def cypherExceptionFactory: CypherExceptionFactory = exceptionFactory

      override def errorHandler: Seq[SemanticErrorDef] => Unit =
        (errors: Seq[SemanticErrorDef]) => errors.foreach(e => throw cypherExceptionFactory.syntaxException(e.msg, e.position))
    }

  private val features = Seq(
    Cypher9Comparability,
    MultipleDatabases,
    MultipleGraphs
  )

  private val parse: Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement])

  private val deprecations =
    SyntaxDeprecationWarnings(Deprecations.V2)

  private val semantics =
    SemanticAnalysis(warn = true, features: _*).adds(BaseContains[SemanticState])

  private val prepare =
    PreparatoryRewriting(Deprecations.V2)

  private val fabricPrepare =
    FabricPreparatoryRewriting()

  private val rewrite =
    AstRewriting(
      sequencer = RewriterStepSequencer.newPlain,
      literalExtraction = Never,
      innerVariableNamer = new GeneratingNamer)

  val parseAndPrepare = ParsingPipeline(Seq(
    parse,
    deprecations,
    prepare,
    semantics,
    fabricPrepare,
    semantics
  ), defaultContext)

  val checkAndFinalize = AnalysisPipeline(Seq(
    prepare,
    semantics,
    rewrite
  ), defaultContext)
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

case class FabricPreparatoryRewriting() extends Phase[BaseContext, BaseState, BaseState] {
  override val phase =
    CompilationPhaseTracer.CompilationPhase.AST_REWRITE

  override val description =
    "rewrite the AST into a shape that the fabric planner can act on"

  override def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(from.statement().endoRewrite(
      // we need all return columns for data flow analysis between query segments
      expandStar(from.semantics())
    ))

  override def postConditions: Set[Condition] = Set()
}
