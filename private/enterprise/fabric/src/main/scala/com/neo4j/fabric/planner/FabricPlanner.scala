/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planner

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.planner.FabricQuery._
import com.neo4j.fabric.utils.Rewritten._
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticState, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.Variable
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseState, Condition}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.internal.{CypherPreParser, FullyParsedQuery, PreParser, QueryOptions}
import org.neo4j.cypher.{CypherExpressionEngineOption, CypherRuntimeOption}
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue


case class FabricPlanner(config: FabricConfig, monitors: Monitors) {

  private val catalog = Catalog.fromConfig(config)
  private val renderer = Prettifier(ExpressionStringifier())

  private[planner] val queryCache = new FabricQueryCache()

  def plan(
    query: String,
    parameters: MapValue
  ): FabricQuery = {

    queryCache.computeIfAbsent(
      query, parameters,
      (q, p) => init(q, p).plan
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

    def plan: FabricQuery = {

      fabricQuery
        .rewritten
        .bottomUp {
          // Insert InputDataStream in front of local queries
          case lq: LocalQuery if lq.input.nonEmpty =>
            val pos = lq.query.state.statement().position
            val vars = lq.input.map(name => Variable(name)(pos))
            val is = InputDataStream(vars)(pos)
            lq.rewritten.bottomUp {
              case sq: SingleQuery => sq.copy(is +: sq.clauses)(sq.position)
            }
        }
        .rewritten
        .bottomUp {
          // Make local subqueries pass input columns through by prepending them
          case lq @ LocalQuery(_, input, _, true) =>
            lq.rewritten.bottomUp {
              case ri @ ReturnItems(false, _) =>
                val pos = ri.position
                val existing = ri.items.map(_.name)
                val passThrough = input
                  .filterNot(existing.contains)
                  .map(n => AliasedReturnItem(Variable(n)(pos)))
                ri.copy(items = passThrough ++ ri.items)(pos)
            }

          // Make local intermediates return
          case lq @ LocalQuery(_, _, output, false) if output.nonEmpty =>
            lq.rewritten.bottomUp {
              case sq @ SingleQuery(clauses) =>
                clauses.last match {
                  case r: Return => sq
                  case _         =>
                    val pos = InputPosition.NONE
                    val items = output.map(n => AliasedReturnItem(Variable(n)(pos)))
                    val ret = Return(ReturnItems(false, items)(pos))(pos)
                    SingleQuery(clauses :+ ret)(sq.position)
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

    private abstract class Segment(val subquery: Boolean) {

      def clauses: Seq[Clause]

      def produced: Seq[String]

      def output(input: Seq[String]): Seq[String]

      // break apart subqueries and intermediate clauses into segments
      def segments: Seq[Segment] =
        clauses
          .foldLeft(Seq[Segment]()) {
            case (segs, sub: SubQuery)        =>
              sub.part match {
                case singleQuery: SingleQuery => Sub(singleQuery.clauses) +: segs
                // TODO fix this
                case u => Errors.unimplemented("Union queries in subqueries", u.productPrefix)
              }
            case (Seq(Seg(cs), rest @ _*), c) =>
              Seg(cs :+ c) +: rest
            case (segs, c)                    =>
              Seg(Seq(c)) +: segs
          }
          .reverse
    }

    private case class Sub(clauses: Seq[Clause]) extends Segment(true) {
      override def segments: Seq[Segment] =
        super.segments match {
          // Return self if not split to preserve full output
          case Seq(one) => Seq(this)
          case segs     => segs
        }

      def produced: Seq[String] =
        clauses.last.returnColumns.map(variable => variable.name)

      def output(input: Seq[String]): Seq[String] =
        input.filterNot(produced.contains) ++ produced
    }

    private case class Seg(clauses: Seq[Clause]) extends Segment(false) {
      def produced: Seq[String] =
        semantic.scope(clauses.last).get.symbolNames.toSeq

      def output(input: Seq[String]): Seq[String] =
        produced
    }

    def fabricQuery: FabricQuery = original match {
      case Query(hint, part) => fabricQuery(part)
      case d: CatalogDDL     => Errors.unimplemented("Support for DDL", d)
      case c: Command        => Errors.unimplemented("Support for commands", c)
    }

    private def fabricQuery(part: QueryPart): FabricQuery = part match {
      case SingleQuery(clauses)    => fabricQuery(Seg(clauses), input = Seq())
      case UnionDistinct(lhs, rhs) => UnionQuery(fabricQuery(lhs), fabricQuery(rhs), distinct = true)
      case UnionAll(lhs, rhs)      => UnionQuery(fabricQuery(lhs), fabricQuery(rhs), distinct = false)
    }

    private def fabricQuery(segment: Segment, input: Seq[String]): FabricQuery = {

      segment.segments match {
        case Seq() =>
          Errors.error("Could not split query into executable segments")

        case Seq(single) if single.clauses == segment.clauses =>
          // No splitting happened - this is a leaf
          leafQuery(single, input)

        case segs =>
          val head = fabricQuery(segs.head, input)
          val tail = segs.tail.foldLeft(head) {
            case (lhs, seg) =>
              ChainedQuery(
                lhs = lhs,
                rhs = fabricQuery(seg, lhs.output))
          }
          tail
      }
    }

    private def leafQuery(segment: Segment, input: Seq[String]): LeafQuery = {

      val fromErrors = segment.clauses.tail
        .filter(_.isInstanceOf[FromGraph])
        .map(Errors.semantic("FROM can only appear at the beginning of a (sub-)query", _))

      Errors.invalidOnError(fromErrors)

      val clauses = segment.clauses
        .filterNot(_.isInstanceOf[FromGraph])

      val pos = clauses.head.position

      val query = Query(None, SingleQuery(clauses)(pos))(pos)

      segment.clauses.head match {
        case f: FromGraph =>
          ShardQuery(
            from = f,
            query = query,
            input = input,
            output = segment.output(input),
            hasOutput = segment.produced.nonEmpty,
            queryString = renderer.asString(query),
            subquery = segment.subquery
          )

        case _ =>
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
            input = input,
            output = segment.output(input),
            subquery = segment.subquery
          )

      }
    }

    private case class PartialState(stmt: Statement) extends BaseState {
      override def maybeStatement: Option[Statement] = Some(stmt)

      override def queryText: String = fail("queryText")

      override def startPosition: Option[InputPosition] = fail("startPosition")

      override def initialFields: Map[String, CypherType] = fail("initialFields")

      override def maybeSemantics: Option[SemanticState] = fail("maybeSemantics")

      override def maybeExtractedParams: Option[Map[String, Any]] = fail("maybeExtractedParams")

      override def maybeSemanticTable: Option[SemanticTable] = fail("maybeSemanticTable")

      override def accumulatedConditions: Set[Condition] = fail("accumulatedConditions")

      override def plannerName: PlannerName = fail("plannerName")

      override def withStatement(s: Statement): BaseState = fail("withStatement")

      override def withSemanticTable(s: SemanticTable): BaseState = fail("withSemanticTable")

      override def withSemanticState(s: SemanticState): BaseState = fail("withSemanticState")

      override def withParams(p: Map[String, Any]): BaseState = fail("withParams")

      override def maybeReturnColumns: Option[Seq[String]] = fail("maybeReturnColumns")

      override def withReturnColumns(cols: Seq[String]): BaseState = fail("withReturnColumns")
    }

  }

  /** Extracted from PreParser */
  def isPeriodicCommit(query: String): Boolean = {
    val preParsedStatement = CypherPreParser(query)
    PreParser.periodicCommitHintRegex.findFirstIn(preParsedStatement.statement.toUpperCase).nonEmpty
  }

}
