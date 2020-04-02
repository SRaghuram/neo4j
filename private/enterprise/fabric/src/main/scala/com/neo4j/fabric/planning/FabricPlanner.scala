/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.cache.FabricQueryCache
import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.eval.UseEvaluation
import com.neo4j.fabric.pipeline.FabricFrontEnd
import com.neo4j.fabric.planning.FabricPlan.DebugOptions
import com.neo4j.fabric.planning.FabricQuery.LocalQuery
import com.neo4j.fabric.planning.FabricQuery.RemoteQuery
import org.neo4j.cypher.CypherExpressionEngineOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryPart
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SubQuery
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.Notification
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

case class FabricPlanner(
  config: FabricConfig,
  cypherConfig: CypherConfiguration,
  monitors: Monitors,
  signatures: ProcedureSignatureResolver
) {

  private[planning] val queryCache = new FabricQueryCache(cypherConfig.queryCacheSize)

  private val frontend = FabricFrontEnd(cypherConfig, monitors, signatures)

  private def fabricContextName: Option[String] = for {
    database <- Option(config.getDatabase)
    name <- Option(database.getName)
  } yield name.name()

  def instance(queryString: String, queryParams: MapValue, defaultGraphName: String): PlannerInstance = {
    val query = frontend.preParsing.preParse(queryString)
    PlannerInstance(query, queryParams, defaultGraphName, fabricContextName)
  }

  case class PlannerInstance(
    query: PreParsedQuery,
    queryParams: MapValue,
    defaultContextName: String,
    fabricContextName: Option[String],
  ) {

    private val pipeline = frontend.Pipeline(query, queryParams)

    lazy val plan: FabricPlan = {
      val plan = queryCache.computeIfAbsent(
        query.cacheKey, queryParams, defaultContextName,
        () => computePlan()
      )
      plan.copy(
        executionType = frontend.preParsing.executionType(query.options))
    }

    private def computePlan(): FabricPlan = trace {
      val prepared = pipeline.parseAndPrepare.process()

      val fragmenter = new FabricFragmenter(defaultContextName, query.statement, prepared.statement(), prepared.semantics())
      val fragments = fragmenter.fragment

      val fabricContext = inFabricContext(fragments)

      val stitching = FabricStitcher(query.statement, fabricContext, fabricContextName)
      val stitchedFragments = stitching.convert(fragments)

      FabricPlan(
        query = stitchedFragments,
        queryType = QueryType.recursive(stitchedFragments),
        executionType = FabricPlan.Execute,
        queryString = query.statement,
        debugOptions = DebugOptions.from(query.options.debugOptions),
        obfuscationMetadata = prepared.obfuscationMetadata(),
        inFabricContext = fabricContext,
      )
    }

    private def trace(compute: => FabricPlan): FabricPlan = {
      val event = pipeline.traceStart()
      try compute
      finally event.close()
    }

    def notifications: Seq[Notification] =
      pipeline.notifications

    def asLocal(fragment: Fragment.Exec): LocalQuery = LocalQuery(
      query = FullyParsedQuery(
        state = pipeline.checkAndFinalize.process(fragment.query),
        options = QueryOptions.default.copy(
          runtime = CypherRuntimeOption.slotted,
          expressionEngine = CypherExpressionEngineOption.interpreted,
          materializedEntitiesMode = true,
        )
      ),
      queryType = fragment.queryType
    )

    def asRemote(fragment: Fragment.Exec): RemoteQuery = RemoteQuery(
      query = QueryRenderer.render(fragment.query),
      queryType = fragment.queryType,
    )

    private def inFabricContext(fragment: Fragment): Boolean = {
      def inFabricDefaultContext =
        fabricContextName.contains(defaultContextName)

      def isFabricFragment(fragment: Fragment): Boolean =
        fragment match {
          case chain: Fragment.Chain =>
            UseEvaluation.evaluateStatic(chain.use.graphSelection)
              .exists(cn => cn.parts == fabricContextName.toList)

          case union: Fragment.Union =>
            isFabricFragment(union.lhs) && isFabricFragment(union.rhs)
        }

      inFabricDefaultContext || isFabricFragment(fragment)
    }

    private[planning] def withForceFabricContext(force: Boolean) =
      if (force) this.copy(fabricContextName = Some(defaultContextName))
      else this.copy(fabricContextName = None)
  }
}
