/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.cache.FabricQueryCache
import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.eval.Catalog
import com.neo4j.fabric.executor.FabricException
import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.planning.FabricPlan.DebugOptions
import com.neo4j.fabric.planning.FabricQuery._
import com.neo4j.fabric.planning.Fragment.{Chain, Leaf, Union}
import com.neo4j.fabric.util.Errors
import com.neo4j.fabric.util.Rewritten._
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.logical.plans.{ResolvedCall, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticState, SemanticTable}
import org.neo4j.cypher.internal.v4_0.ast.{ProcedureResult, SingleQuery, Statement, UnresolvedCall}
import org.neo4j.cypher.internal.v4_0.expressions.{FunctionInvocation, FunctionName, Namespace, ProcedureName}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseState, Condition}
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.{CTAny, CypherType}
import org.neo4j.cypher.internal.v4_0.{ast, expressions => exp}
import org.neo4j.cypher.{CypherExecutionMode, CypherExpressionEngineOption, CypherRuntimeOption, CypherUpdateStrategy}
import org.neo4j.kernel.api.exceptions.Status.Statement.SemanticError
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

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
  private[planning] val queryCache = new FabricQueryCache(cypherConfig.queryCacheSize)

  def plan(
    query: String,
    parameters: MapValue
  ): FabricPlan =
    queryCache.computeIfAbsent(query, parameters, (q, p) => init(q, p).plan)

  def init(query: String, parameters: MapValue): PlannerContext = {
    val pipeline = Pipeline.Instance(monitors, query, signatures)

    val preParsed = preParse(query)
    val state = pipeline.parseAndPrepare.process(preParsed.statement)
    PlannerContext(
      query = query,
      original = state.statement(),
      parameters = parameters,
      semantic = state.semantics(),
      options = preParsed.options,
      catalog = catalog,
      pipeline = pipeline,
    )
  }

  private def preParse(query: String): PreParsedQuery = {
    val preParsed = preParser.preParseQuery(query)
    assertNotPeriodicCommit(preParsed)
    assertOptionsNotSet(preParsed.options)
    preParsed
  }

  private def assertNotPeriodicCommit(preParsedStatement: PreParsedQuery): Unit = {
    if (preParsedStatement.options.isPeriodicCommit) {
      throw new FabricException(SemanticError, "Periodic commit is not supported in Fabric")
    }
  }

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

  case class PlannerContext(
    query: String,
    original: Statement,
    parameters: MapValue,
    semantic: SemanticState,
    options: QueryOptions,
    catalog: Catalog,
    pipeline: Pipeline.Instance,
  ) {

    def fragment: Fragment = original match {
      case ast.Query(hint, part) => fragment(part, Seq.empty, Seq.empty, Option.empty)
      case ddl: ast.CatalogDDL => Errors.ddlNotSupported(ddl)
      case cmd: ast.Command => Errors.notSupported("Commands")
    }

    case class State(
      incoming: Seq[String],
      locals: Seq[String],
      imports: Seq[String],
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

    private def fragment(
      part: ast.QueryPart,
      incoming: Seq[String],
      local: Seq[String],
      use: Option[ast.UseGraph]
    ): Fragment = {

      part match {
        case sq: ast.SingleQuery =>
          val imports = sq.importColumns
          val localUse = leadingUse(sq).orElse(use)
          val parts = partitioned(sq.clauses)
          val initial = State(incoming, local, imports)

          val result = parts.foldLeft(initial) {
            case (current, part) => part match {

              case Right(clauses) =>
                val leaf = Leaf(
                  use = localUse,
                  clauses = clauses.filterNot(_.isInstanceOf[ast.UseGraph]),
                  columns = current.columns.copy(
                    output = produced(clauses.last),
                  )
                )
                current.append(Fragment.Direct(leaf, leaf.columns))

              case Left(sub) if localUse.isDefined =>
                Errors.syntax("Nested subqueries in remote query-parts is not supported", query, sub.position)

              case Left(sub) =>
                val frag = fragment(
                  part = sub.part,
                  incoming = current.incoming,
                  local = Seq.empty,
                  use = localUse
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

          result.fragments match {
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
        case _                     => (None, clauses)
      }

      rest.filter(_.isInstanceOf[ast.UseGraph])
        .map(clause => Errors.syntax("USE can only appear at the beginning of a (sub-)query", query, clause.position))

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

    def plan: FabricPlan = {
      val fQuery = fabricQuery

      FabricPlan(
        query = fQuery,
        queryType = QueryType.of(fQuery),
        executionType = options.executionMode match {
          case CypherExecutionMode.normal  => FabricPlan.Execute
          case CypherExecutionMode.explain => FabricPlan.Explain
          case CypherExecutionMode.profile => Errors.notSupported("Query option: 'PROFILE'")
        },
        debugOptions = DebugOptions.from(options.debugOptions)
      )
    }

    def fabricQuery: FabricQuery =
      fabricQuery(fragment)

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
        val queryType = QueryType.of(query)
        val base = leaf.use match {

          case Some(use) =>
            FabricQuery.RemoteQuery(
              use = use,
              query = query,
              columns = leaf.columns,
              queryType = queryType,
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
              columns = leaf.columns,
              queryType = queryType,
            )
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

}
