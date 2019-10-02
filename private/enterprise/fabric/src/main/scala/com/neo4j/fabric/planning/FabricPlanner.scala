/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.cache.FabricQueryCache
import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.eval.Catalog
import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.planning.FabricQuery._
import com.neo4j.fabric.planning.Fragment.{Chain, Leaf, Union}
import com.neo4j.fabric.util.Errors
import com.neo4j.fabric.util.Rewritten._
import org.neo4j.cypher.internal.logical.plans.{ResolvedCall, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticState, SemanticTable}
import org.neo4j.cypher.internal.v4_0.ast.{ProcedureResult, SingleQuery, Statement, UnresolvedCall}
import org.neo4j.cypher.internal.v4_0.expressions.{FunctionInvocation, FunctionName, Namespace, ProcedureName}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseState, Condition}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CypherType}
import org.neo4j.cypher.internal.v4_0.{ast, expressions => exp}
import org.neo4j.cypher.internal.{CypherConfiguration, CypherPreParser, FullyParsedQuery, PreParser, QueryOptions}
import org.neo4j.cypher.{CypherExpressionEngineOption, CypherRuntimeOption}
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

object FabricPlanner {
  private var printPlans: Boolean = false
  private var printBasePlans: Boolean = false
  private var printFragments: Boolean = false
  def setPrintPlans(enabled: Boolean): Unit = printPlans = enabled
  def setPrintBasePlans(enabled: Boolean): Unit = printBasePlans = enabled
  def setPrintFragments(enabled: Boolean): Unit = printFragments = enabled
}

case class FabricPlanner(
  config: FabricConfig,
  cypherConfig: CypherConfiguration,
  monitors: Monitors,
  signatures: ProcedureSignatureResolver
) {

  private val preParser = new PreParser(
    cypherConfig.version,
    cypherConfig.planner,
    cypherConfig.runtime,
    cypherConfig.expressionEngineOption,
    cypherConfig.operatorEngine,
    cypherConfig.interpretedPipesFallback,
    cypherConfig.queryCacheSize,
  )
  private val catalog = Catalog.fromConfig(config)
  private[planning] val queryCache = new FabricQueryCache()

  def plan(
    query: String,
    parameters: MapValue
  ): FabricQuery = {

    val result = queryCache.computeIfAbsent(
      query, parameters,
      (q, p) => init(q, p).fabricQuery
    )

    if (FabricPlanner.printPlans) {
      FabricQuery.pretty.pprint(result)
    }

    result
  }

  def init(
    query: String,
    parameters: MapValue
  ): PlannerContext = {
    val preParsed = preParser.preParseQuery(query)
    val pipeline = Pipeline.Instance(monitors, query, signatures)
    val state = pipeline.parseAndPrepare.process(preParsed.statement)
    PlannerContext(
      original = state.statement(),
      parameters = parameters,
      semantic = state.semantics(),
      catalog = catalog,
      pipeline = pipeline
    )
  }

  case class PlannerContext(
    original: Statement,
    parameters: MapValue,
    semantic: SemanticState,
    catalog: Catalog,
    pipeline: Pipeline.Instance
  ) {

    def fragment: Fragment = original match {
      case ast.Query(hint, part) => fragment(part, Seq.empty, Seq.empty, Option.empty)
      case d: ast.CatalogDDL     => Errors.unimplemented("Support for DDL", d)
      case c: ast.Command        => Errors.unimplemented("Support for commands", c)
    }

    private def fragment(
      part: ast.QueryPart,
      incoming: Seq[String],
      local: Seq[String],
      use: Option[ast.UseGraph]
    ): Fragment = {

      case class State(
        incoming: Seq[String],
        locals: Seq[String],
        imports: Seq[String],
        use: Option[ast.UseGraph],
        fragments: Seq[Fragment] = Seq.empty
      ) {

        def columns: Columns = Columns(
          incoming = incoming,
          local = locals,
          imports = imports,
          output = Seq.empty,
        )

        def append(frag: Fragment): State =
          copy(
            incoming = frag.columns.output,
            locals = frag.columns.output,
            imports = Seq.empty,
            fragments = fragments :+ frag,
          )
      }

      part match {
        case sq: ast.SingleQuery =>
          val imports = sq.importColumns
          val parts = partitioned(sq.clauses)
          val start = State(incoming, local, imports, leadingUse(sq).orElse(use))
          val state = parts.foldLeft(start) {
            case (current, part) => part match {

              case Right(clauses) =>
                val leaf = Leaf(
                  use = current.use,
                  clauses = clauses.filterNot(_.isInstanceOf[ast.UseGraph]),
                  columns = current.columns.copy(
                    output = produced(clauses.last),
                  )
                )
                current.append(Fragment.Direct(leaf, leaf.columns))

              case Left(sub) =>
                val frag = fragment(
                  part = sub.part,
                  incoming = current.incoming,
                  local = Seq.empty,
                  use = current.use
                )
                current.append(Fragment.Apply(
                  frag,
                  current.columns.copy(
                    imports = Seq.empty,
                    output = Columns.combine(current.locals, frag.columns.output),
                  )
                ))
            }
          }

          state.fragments match {
            case Seq(single) => single
            case many        => Chain(
              fragments = many,
              columns = Columns(
                incoming = incoming,
                local = local,
                imports = imports,
                output = many.last.columns.output,
              )
            )
          }

        case uq: ast.Union =>
          val lhs = fragment(uq.part, incoming, local, use)
          val rhs = fragment(uq.query, incoming, local, use)
          val distinct = uq match {
            case _: ast.UnionAll      => false
            case _: ast.UnionDistinct => true
          }
          Union(distinct, lhs, rhs, rhs.columns)
      }
    }

    private def leadingUse(sq: ast.SingleQuery): Option[ast.UseGraph] = {
      val clauses = sq.clausesExceptImportWith
      val (use, rest) = clauses.headOption match {
        case Some(u: ast.UseGraph) => (Some(u), clauses.tail)
        case _                      => (None, clauses)
      }

      Errors.invalidOnError(
        rest.filter(_.isInstanceOf[ast.UseGraph])
          .map(Errors.semantic("USE can only appear at the beginning of a (sub-)query", _))
      )

      use
    }

    private def produced(clause: ast.Clause): Seq[String] = clause match {
      case r: ast.Return => r.returnColumns.map(_.name)
      case c             =>
        val scope = semantic.scope(c)
        scope.get.symbolNames.toSeq
    }

    /**
     * Returns a sequence where each element is either a subquery clause
     * or a segment of clauses with no subqueries
     */
    private def partitioned(clauses: Seq[ast.Clause]) =
      partition(clauses) {
        case s: ast.SubQuery => Left(s)
        case c               => Right(c)
      }

    /**
     * Partitions the elements of a sequence depending on a predicate.
     * The predicate returns either Left[H] or Right[M] for each element
     * Running lengths of Right[M]'s gets aggregated into sub-sequences
     * while Left[H]'s are left singular
     */
    private def partition[E, H, M](es: Seq[E])(pred: E => Either[H, M]): Seq[Either[H, Seq[M]]] = {
      es.map(pred).foldLeft(Seq[Either[H, Seq[M]]]()) {
        case (seq, Left(hit))   => seq :+ Left(hit)
        case (seq, Right(miss)) => seq.lastOption match {
          case None            => seq :+ Right(Seq(miss))
          case Some(Left(_))   => seq :+ Right(Seq(miss))
          case Some(Right(ms)) => seq.init :+ Right(ms :+ miss)
        }
      }
    }

    def fabricQuery: FabricQuery = {
      val frag = fragment
      if (FabricPlanner.printFragments) {
        Fragment.pretty.pprint(frag)
      }
      fabricQuery(frag)
    }

    private def fabricQuery(fragment: Fragment): FabricQuery = fragment match {

      case chain: Fragment.Chain =>
        FabricQuery.ChainedQuery(
          queries = chain.fragments.map(fabricQuery),
          columns = chain.columns,
        )

      case union: Fragment.Union =>
        FabricQuery.UnionQuery(
          lhs = fabricQuery(union.lhs),
          rhs = fabricQuery(union.rhs),
          distinct = union.distinct,
          columns = union.columns,
        )

      case direct: Fragment.Direct =>
        FabricQuery.Direct(
          fabricQuery(direct.fragment),
          direct.columns
        )

      case apply: Fragment.Apply =>
        FabricQuery.Apply(
          fabricQuery(apply.fragment),
          apply.columns
        )

      case leaf: Fragment.Leaf =>
        val pos = leaf.clauses.head.position
        val query = ast.Query(None, ast.SingleQuery(leaf.clauses)(pos))(pos)
        val base = leaf.use match {

          case Some(use) =>
            FabricQuery.RemoteQuery(
              use = use,
              query = query,
              columns = leaf.columns
            )

          case None =>
            FabricQuery.LocalQuery(
              query = FullyParsedQuery(
                state = PartialState(query),
                // Other runtimes can fail when we don't have READ access
                // even for "calculator queries"
                // TODO: Switch to default runtime selection
                options = QueryOptions.default.copy(
                  runtime = CypherRuntimeOption.slotted,
                  expressionEngine = CypherExpressionEngineOption.interpreted,
                  materializedEntitiesMode = true
                )
              ),
              columns = leaf.columns
            )
        }

        if (FabricPlanner.printBasePlans) {
          FabricQuery.pretty.pprint(base)
        }

        base
          .rewritten
          .bottomUp {
            // Insert InputDataStream in front of local queries
            case lq: LocalQuery if lq.input.nonEmpty =>
              lq.rewritten.bottomUp {
                case sq: ast.SingleQuery =>
                  sq.withInputDataStream(lq.input)
              }
          }
          .rewritten
          .bottomUp {
            // Add parameter bindings for shard query imports
            case sq: RemoteQuery if sq.parameters.nonEmpty =>
              sq.rewritten.bottomUp {
                case q: ast.SingleQuery =>
                  q.withParamBindings(sq.parameters)
              }
          }
          .rewritten
          .bottomUp {
            // Make all producing queries return
            case lq: LeafQuery if lq.columns.output.nonEmpty =>
              lq.rewritten.bottomUp {
                case q: ast.SingleQuery =>
                  q.clauses.last match {
                    case _: ast.Return => q
                    case _             => q.withReturnAliased(lq.columns.output)
                  }
              }
          }
          .rewritten
          .bottomUp {
            // Un-resolve procedures for rendering
            case rc: ResolvedCall =>
              val pos = rc.position
              val name = rc.signature.name
              UnresolvedCall(
                procedureNamespace = Namespace(name.namespace.toList)(pos),
                procedureName = ProcedureName(name.name)(pos),
                declaredArguments = if (rc.declaredArguments) Some(rc.callArguments) else None,
                declaredResult = if (rc.declaredResults) Some(ProcedureResult(rc.callResults)(pos)) else None,
              )(pos)
            // Un-resolve functions for rendering
            case rf: ResolvedFunctionInvocation =>
              val pos = rf.position
              val name = rf.qualifiedName
              FunctionInvocation(
                namespace = Namespace(name.namespace.toList)(pos),
                functionName = FunctionName(name.name)(pos),
                distinct = false,
                args = rf.arguments.toIndexedSeq,
              )(pos)
          }
          .rewritten
          .bottomUp {
            // Prepare for planning (run rest of pipeline for local queries)
            case PartialState(stmt) =>
              pipeline.checkAndFinalize.process(stmt)
          }
    }

    private implicit class SingleQueryRewrites(sq: ast.SingleQuery) {
      private val pos = InputPosition.NONE

      def append(clause: ast.Clause): ast.SingleQuery =
        sq.copy(sq.clauses :+ clause)(sq.position)

      def prepend(clause: ast.Clause): ast.SingleQuery =
        sq.copy(clause +: sq.clauses)(sq.position)

      def withReturnNone: ast.SingleQuery =
        append(ast.Return(ast.ReturnItems(includeExisting = false, Seq())(pos))(pos))

      def withReturnAliased(names: Seq[String]): ast.SingleQuery =
        append(ast.Return(ast.ReturnItems(includeExisting = false,
          names.map(n => ast.AliasedReturnItem(exp.Variable(n)(pos), exp.Variable(n)(pos))(pos))
        )(pos))(pos))

      def withParamBindings(bindings: Map[String, String]): SingleQuery = {
        val items = for {
          (varName, parName) <- bindings.toSeq
        } yield ast.AliasedReturnItem(exp.Parameter(parName, CTAny)(pos), exp.Variable(varName)(pos))(pos)
        prepend(
          ast.With(ast.ReturnItems(false, items)(pos))(pos)
        )
      }

      def withInputDataStream(names: Seq[String]): SingleQuery =
        prepend(ast.InputDataStream(
          names.map(name => exp.Variable(name)(pos))
        )(pos))
    }

    private case class PartialState(stmt: ast.Statement) extends BaseState {
      override def maybeStatement: Option[ast.Statement] = Some(stmt)

      override def queryText: String = fail("queryText")

      override def startPosition: Option[InputPosition] = fail("startPosition")

      override def initialFields: Map[String, CypherType] = fail("initialFields")

      override def maybeSemantics: Option[SemanticState] = fail("maybeSemantics")

      override def maybeExtractedParams: Option[Map[String, Any]] = fail("maybeExtractedParams")

      override def maybeSemanticTable: Option[SemanticTable] = fail("maybeSemanticTable")

      override def maybeReturnColumns: Option[Seq[String]] = fail("maybeReturnColumns")

      override def accumulatedConditions: Set[Condition] = fail("accumulatedConditions")

      override def plannerName: PlannerName = fail("plannerName")

      override def withStatement(s: ast.Statement): BaseState = fail("withStatement")

      override def withSemanticTable(s: SemanticTable): BaseState = fail("withSemanticTable")

      override def withSemanticState(s: SemanticState): BaseState = fail("withSemanticState")

      override def withReturnColumns(cols: Seq[String]): BaseState = fail("withReturnColumns")

      override def withParams(p: Map[String, Any]): BaseState = fail("withParams")
    }

  }

  /** Extracted from PreParser */
  def isPeriodicCommit(query: String): Boolean = {
    val preParsedStatement = CypherPreParser(query)
    PreParser.periodicCommitHintRegex.findFirstIn(preParsedStatement.statement.toUpperCase).nonEmpty
  }

}
