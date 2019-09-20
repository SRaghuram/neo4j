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
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticState, SemanticTable}
import org.neo4j.cypher.internal.v4_0.ast.{PeriodicCommitHint, SingleQuery, Statement}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseState, Condition}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CypherType}
import org.neo4j.cypher.internal.v4_0.{ast, expressions => exp}
import org.neo4j.cypher.internal.{CypherPreParser, FullyParsedQuery, PreParser, QueryOptions}
import org.neo4j.cypher.{CypherExpressionEngineOption, CypherRuntimeOption}
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue


case class FabricPlanner(config: FabricConfig, monitors: Monitors) {

  private val catalog = Catalog.fromConfig(config)
  private val renderer = Prettifier(ExpressionStringifier())
  private[planning] val queryCache = new FabricQueryCache()

  def plan(
    query: String,
    parameters: MapValue
  ): FabricQuery = {

    queryCache.computeIfAbsent(
      query, parameters,
      (q, p) => init(q, p).fabricQuery
    )
  }

  def init(
    query: String,
    parameters: MapValue
  ): PlannerContext = {

    val pipeline = Pipeline.Instance(monitors, query)
    val state = pipeline.parseAndPrepare.process(query)

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
      case ast.Query(hint, part) => fragment(part, Seq.empty, Option.empty)
      case d: ast.CatalogDDL     => Errors.unimplemented("Support for DDL", d)
      case c: ast.Command        => Errors.unimplemented("Support for commands", c)
    }

    private def fragment(qp: ast.QueryPart, input: Seq[String], from: Option[ast.FromGraph]): Fragment = {

      case class State(
        columns: Seq[String],
        local: Seq[String],
        from: Option[ast.FromGraph],
        fragments: Seq[Fragment] = Seq.empty
      ) {

        def append(f: Fragment): State = copy(
          columns = Columns.combine(columns, f.produced),
          local = Columns.combine(local, f.produced),
          fragments = fragments :+ f
        )

        def append(l: Leaf): State = copy(
          columns = Columns.combine(columns, l.produced),
          local = Columns.combine(local, l.produced),
          from = l.from,
          fragments = fragments :+ l
        )
      }

      qp match {
        case sq: ast.SingleQuery =>
          val imports = sq.importColumns
          val parts = partitioned(sq.clauses)
          val start = State(input, Seq(), leadingFrom(sq).orElse(from))
          val state = parts.foldLeft(start) {
            case (curr, part) => part match {

              case Left(sub) =>
                curr.append(fragment(
                  qp = sub.part,
                  input = curr.columns,
                  from = curr.from
                ))

              case Right(clauses) =>
                curr.append(Leaf(
                  from = curr.from,
                  clauses = clauses.filterNot(_.isInstanceOf[ast.FromGraph]),
                  columns = Columns(
                    input = curr.columns,
                    local = curr.local,
                    imports = imports,
                    passThrough = input,
                    produced = produced(clauses.last)
                  )
                ))
            }
          }

          state.fragments match {
            case Seq(single) => single
            case many        => Chain(many)
          }

        case uq: ast.Union =>
          val lhs = fragment(uq.part, input, from)
          val rhs = fragment(uq.query, input, from)
          val distinct = uq match {
            case _: ast.UnionAll      => false
            case _: ast.UnionDistinct => true
          }
          Union(distinct, lhs, rhs, rhs.produced)
      }
    }

    private def leadingFrom(sq: ast.SingleQuery): Option[ast.FromGraph] = {
      val clauses = sq.clausesExceptImportWith
      val (from, rest) = clauses.headOption match {
        case Some(f: ast.FromGraph) => (Some(f), clauses.tail)
        case _                      => (None, clauses)
      }

      Errors.invalidOnError(
        rest.filter(_.isInstanceOf[ast.FromGraph])
          .map(Errors.semantic("FROM can only appear at the beginning of a (sub-)query", _))
      )

      from
    }


    private def produced(clause: ast.Clause): Seq[String] = clause match {
      case r: ast.Return => r.returnColumns.map(_.name)
      case c             => semantic.scope(c).get.symbolNames.toSeq
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

    def fabricQuery: FabricQuery =
      fabricQuery(fragment)

    private def fabricQuery(fragment: Fragment): FabricQuery = fragment match {

      case chain: Chain => ChainedQuery(
        queries = chain.fragments.map(fabricQuery)
      )

      case union: Union => UnionQuery(
        lhs = fabricQuery(union.lhs),
        rhs = fabricQuery(union.rhs),
        distinct = union.distinct
      )

      case leaf: Leaf =>
        val pos = leaf.clauses.head.position

        val query = ast.Query(None, ast.SingleQuery(leaf.clauses)(pos))(pos)

        val base = leaf.from match {

          case Some(from) =>
            ShardQuery(
              from = from,
              query = query,
              columns = leaf.columns
            )

          case None =>
            LocalQuery(
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

        base
          .rewritten
          .bottomUp {
            // Insert InputDataStream in front of local queries
            case lq: LocalQuery if lq.columns.input.nonEmpty =>
              val pos = lq.query.state.statement().position
              val vars = lq.columns.input.map(name => exp.Variable(name)(pos))
              val is = ast.InputDataStream(vars)(pos)
              lq.rewritten.bottomUp {
                case sq: ast.SingleQuery => sq.copy(is +: sq.clauses)(sq.position)
              }
          }
          .rewritten
          .bottomUp {
            // Add parameter bindings for shard query imports
            case sq: ShardQuery if sq.parameters.nonEmpty =>
              sq.rewritten.bottomUp {
                case q: ast.SingleQuery =>
                  q.withParamBindings(sq.parameters)
              }
          }
          .rewritten
          .bottomUp {
            // Make local intermediates pass input columns through by pre-pending them
            case lq: LocalQuery if lq.columns.passThrough.nonEmpty =>
              lq
                .rewritten
                .bottomUp {
                  // Add empty return, if there is none
                  case q: ast.SingleQuery =>
                    q.clauses.last match {
                      case _: ast.Return => q
                      case _             => q.withReturnNone
                    }
                }
                .rewritten
                .bottomUp {
                  // Prepend variables to all horizons
                  case ri @ ast.ReturnItems(false, _) =>
                    val pos = ri.position
                    val existing = ri.items.map(_.name)
                    val toAdd = lq.columns.passThrough
                      .filterNot(existing.contains)
                      .map(n => ast.AliasedReturnItem(exp.Variable(n)(pos)))
                    ri.copy(items = toAdd ++ ri.items)(pos)
                }
          }
          .rewritten
          .bottomUp {
            // Make all producing queries return
            case lq: LeafQuery if lq.produced.nonEmpty =>
              lq.rewritten.bottomUp {
                case q: ast.SingleQuery =>
                  q.clauses.last match {
                    case _: ast.Return => q
                    case _             => q.withReturnAliased(lq.produced)
                  }
              }
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
