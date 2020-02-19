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
import com.neo4j.fabric.planning.FabricQuery.LeafQuery
import com.neo4j.fabric.planning.FabricQuery.LocalQuery
import com.neo4j.fabric.planning.FabricQuery.RemoteQuery
import com.neo4j.fabric.util.Errors
import com.neo4j.fabric.util.Rewritten.RewritingOps
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Condition
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal
import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.CypherExpressionEngineOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.CypherUpdateStrategy
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.QueryOptions
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
  ): FabricPlan = queryCache.computeIfAbsent(query, parameters, (q, p) => prepare(q, p).plan)

  private def prepare(query: String, parameters: MapValue): PlannerContext = {
    val pipeline = Pipeline.Instance(monitors, query, signatures)

    val preParsed = preParse(query)
    val state = pipeline.parseAndPrepare.process(preParsed.statement)
    PlannerContext(
      queryString = query,
      queryStatement = state.statement(),
      parameters = parameters,
      semantics = state.semantics(),
      options = preParsed.options,
      catalog = catalog,
      pipeline = pipeline,
      obfuscationMetadata = state.obfuscationMetadata()
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

  private case class PlannerContext(
    queryString: String,
    queryStatement: Statement,
    parameters: MapValue,
    semantics: SemanticState,
    options: QueryOptions,
    catalog: Catalog,
    pipeline: Pipeline.Instance,
    obfuscationMetadata: ObfuscationMetadata
  ) {

    private val fragmenter = new FabricFragmenter(queryString, queryStatement, semantics)

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
        debugOptions = DebugOptions.from(options.debugOptions),
        obfuscationMetadata = obfuscationMetadata
      )
    }

    private def fabricQuery: FabricQuery =
      fabricQuery(fragmenter.fragment)

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
          names.map(n => ast.AliasedReturnItem(internal.expressions.Variable(n)(pos), internal.expressions.Variable(n)(pos))(pos))
        )(pos))(pos))

      def withParamBindings(bindings: Map[String, String]): SingleQuery = {
        val items = for {
          (varName, parName) <- bindings.toSeq
        } yield ast.AliasedReturnItem(internal.expressions.Parameter(parName, CTAny)(pos), internal.expressions.Variable(varName)(pos))(pos)
        prepend(
          ast.With(ast.ReturnItems(false, items)(pos))(pos)
        )
      }

      def withInputDataStream(names: Seq[String]): SingleQuery =
        prepend(ast.InputDataStream(
          names.map(name => internal.expressions.Variable(name)(pos))
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

      override def maybeObfuscationMetadata: Option[ObfuscationMetadata] = fail("maybeObfuscation")

      override def accumulatedConditions: Set[Condition] = fail("accumulatedConditions")

      override def plannerName: PlannerName = fail("plannerName")

      override def withStatement(s: ast.Statement): BaseState = fail("withStatement")

      override def withSemanticTable(s: SemanticTable): BaseState = fail("withSemanticTable")

      override def withSemanticState(s: SemanticState): BaseState = fail("withSemanticState")

      override def withReturnColumns(cols: Seq[String]): BaseState = fail("withReturnColumns")

      override def withParams(p: Map[String, Any]): BaseState = fail("withParams")

      override def withObfuscationMetadata(o: ObfuscationMetadata): BaseState = fail("withObfuscation")
    }

  }

}
