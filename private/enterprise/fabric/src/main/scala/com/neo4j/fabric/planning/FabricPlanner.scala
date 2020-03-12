/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.cache.FabricQueryCache
import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.eval.UseEvaluation
import com.neo4j.fabric.executor.FabricException
import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.planning.FabricPlan.DebugOptions
import com.neo4j.fabric.planning.FabricQuery.LocalQuery
import com.neo4j.fabric.planning.FabricQuery.RemoteQuery
import com.neo4j.fabric.util.Errors
import com.neo4j.fabric.util.Rewritten.RewritingOps
import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.CypherExpressionEngineOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.CypherUpdateStrategy
import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.InputDataStream
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryPart
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SubQuery
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTAny
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
  private[planning] val queryCache = new FabricQueryCache(cypherConfig.queryCacheSize)

  def instance(queryString: String, queryParams: MapValue, defaultGraphName: String): PlannerInstance =
    PlannerInstance(queryString, queryParams, defaultGraphName)

  case class PlannerInstance(
    queryString: String,
    queryParams: MapValue,
    defaultGraphName: String,
  ) {
    private val pipeline = Pipeline.Instance(monitors, queryString, signatures)

    lazy val plan: FabricPlan =
      queryCache.computeIfAbsent(queryString, queryParams, defaultGraphName, () => computePlan())

    private def computePlan(): FabricPlan = {

      val preParsed = preParse(queryString)
      val prepared = pipeline.parseAndPrepare.process(preParsed.statement)

      val fragmenter = new FabricFragmenter(defaultGraphName, queryString, prepared.statement(), prepared.semantics())
      val fragments = fragmenter.fragment

      FabricPlan(
        query = fragments,
        queryType = QueryType.global(fragments),
        executionType = preParsed.options.executionMode match {
          case CypherExecutionMode.normal  => FabricPlan.Execute
          case CypherExecutionMode.explain => FabricPlan.Explain
          case CypherExecutionMode.profile => Errors.notSupported("Query option: 'PROFILE'")
        },
        debugOptions = DebugOptions.from(preParsed.options.debugOptions),
        obfuscationMetadata = prepared.obfuscationMetadata()
      )
    }

    def asLocal(leaf: Fragment.Leaf): LocalQuery = {
      val pos = leaf.clauses.head.position
      val clauses = Seq(
        Ast.inputDataStream(leaf.input.outputColumns, pos).toSeq,
        Ast.paramBindings(leaf.importColumns, pos).toSeq,
        leaf.clauses,
        Ast.aliasedReturn(leaf.clauses.last, leaf.outputColumns, pos).toSeq,
      ).flatten
      val rewrites: Query => Query = Ast.chain(
        Ast.withoutGraphSelection,
        Ast.unresolveCallables
      )
      val query = rewrites(Query(None, SingleQuery(clauses)(pos))(pos))
      val state = pipeline.checkAndFinalize.process(query)

      LocalQuery(
        query = FullyParsedQuery(
          state = state,
          options = QueryOptions.default.copy(
            runtime = CypherRuntimeOption.slotted,
            expressionEngine = CypherExpressionEngineOption.interpreted,
            materializedEntitiesMode = true,
          )
        ),
        queryType = QueryType.local(leaf),
      )
    }

    def asRemote(leaf: Fragment.Leaf): RemoteQuery = {
      val pos = InputPosition.NONE
      val part = stitch(leaf)
      val rewrites: Query => Query = Ast.unresolveCallables
      val query = rewrites(Query(None, part)(pos))

      RemoteQuery(
        query = QueryRenderer.render(query),
        queryType = QueryType.local(leaf),
      )
    }

    def offendingGraphSelections(fragment: Fragment, singleGraphMode: Boolean): Seq[GraphSelection] = {
      if (singleGraphMode) {
        val all = allGraphSelections(fragment).distinct
        val (static, nonStatic) = all.partition(UseEvaluation.isStatic)
        if (nonStatic.nonEmpty) {
          nonStatic
        } else {
          static.filterNot(_ == all.head)
        }
      } else {
        Seq()
      }
    }

    private def allGraphSelections(fragment: Fragment): Seq[GraphSelection] =
      fragment match {
        case Fragment.Init(use, _, _)              => Seq(use.graphSelection)
        case f: Fragment.Leaf                      => allGraphSelections(f.input)
        case f: Fragment.Apply                     => allGraphSelections(f.input) ++ allGraphSelections(f.inner)
        case f: Fragment.Union                     => allGraphSelections(f.lhs) ++ allGraphSelections(f.rhs)
      }

    private def stitch(leaf: Fragment.Leaf): QueryPart =
      stitch(leaf, nested = false)

    private def stitch(fragment: Fragment, nested: Boolean): QueryPart = {
      val pos = InputPosition.NONE

      fragment match {
        case chain: Fragment.Chain =>
          SingleQuery(stitchChain(chain, nested))(pos)

        case union: Fragment.Union =>
          val part = stitch(union.lhs, nested = true)
          val single = SingleQuery(stitchChain(union.rhs, nested = true))(pos)
          if (union.distinct) {
            UnionDistinct(part, single)(pos)
          } else {
            UnionAll(part, single)(pos)
          }
      }
    }

    private def stitchChain(fragment: Fragment.Chain, nested: Boolean): Seq[Clause] = {
      val pos = InputPosition.NONE

      fragment match {
        case _: Fragment.Init if nested =>
          Seq()

        case init: Fragment.Init =>
          Seq(Ast.paramBindings(init.importColumns, pos)).flatten

        case leaf: Fragment.Leaf =>
          val before = stitchChain(leaf.input, nested)
          val clauses = if (nested) leaf.clauses else Ast.withoutGraphSelection(leaf.clauses)
          before ++ clauses

        case apply: Fragment.Apply =>
          val before = stitchChain(apply.input, nested)
          val inner = stitch(apply.inner, nested = true)
          before :+ SubQuery(inner)(pos)
      }
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
  }

  private object Ast {

    private def conditionally[T](cond: Boolean, prod: => T) =
      if (cond) Some(prod) else None

    private def variable(name: String, pos: InputPosition) =
      internal.expressions.Variable(name)(pos)

    def paramBindings(columns: Seq[String], pos: InputPosition): Option[With] =
      conditionally(
        columns.nonEmpty,
        With(ReturnItems(
          includeExisting = false,
          items = for {
            varName <- columns
            parName = Columns.paramName(varName)
          } yield AliasedReturnItem(
            expression = Parameter(parName, CTAny)(pos),
            variable = variable(varName, pos),
          )(pos)
        )(pos))(pos)
      )

    def inputDataStream(names: Seq[String], pos: InputPosition): Option[InputDataStream] =
      conditionally(
        names.nonEmpty,
        InputDataStream(
          variables = for {
            name <- names
          } yield variable(name, pos)
        )(pos)
      )

    def aliasedReturn(lastClause: Clause, names: Seq[String], pos: InputPosition): Option[Return] =
      lastClause match {
        case _: Return => None
        case _         => Some(aliasedReturn(names, pos))
      }

    def aliasedReturn(names: Seq[String], pos: InputPosition): Return =
      Return(ReturnItems(
        includeExisting = false,
        items = for {
          name <- names
        } yield AliasedReturnItem(
          expression = variable(name, pos),
          variable = variable(name, pos),
        )(pos)
      )(pos))(pos)

    def unresolveCallables[T <: AnyRef](tree: T): T = {
      tree.rewritten.bottomUp {
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
    }

    def withoutGraphSelection(clauses: Seq[Clause]): Seq[Clause] =
      clauses.filter {
        case _: GraphSelection => false
        case _                 => true
      }

    def withoutGraphSelection(query: Query): Query = {
      query.rewritten.bottomUp {
        case sq: SingleQuery =>
          SingleQuery(clauses = withoutGraphSelection(sq.clauses))(sq.position)
      }
    }

    def chain[T <: AnyRef](rewrites: (T => T)*): T => T =
      rewrites.foldLeft(identity[T] _)(_ andThen _)

  }
}
