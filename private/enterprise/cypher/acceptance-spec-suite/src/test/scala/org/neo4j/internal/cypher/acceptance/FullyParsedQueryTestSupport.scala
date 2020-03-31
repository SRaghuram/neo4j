/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.Cypher9Comparability
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleDatabases
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.phases.AstRewriting
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.rewriting.rewriters.GeneratingNamer
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter

trait FullyParsedQueryTestSupport {

  def noParams: Map[String, Any] = Map.empty

  private val parsing = CompilationPhases.parsing(ParsingConfig(
    RewriterStepSequencer.newPlain,
    new GeneratingNamer()
  ))

  def parse(qs: String, options: QueryOptions = QueryOptions.default) =
    FullyParsedQuery(
      state = parsing.transform(
        InitialState(qs, None, PlannerNameFor(options.planner.name)),
        ContextHelper.create()
      ),
      options = options
    )

  private val semanticAnalysis =
    SemanticAnalysis(warn = true, Cypher9Comparability, MultipleDatabases).adds(BaseContains[SemanticState]) andThen
      AstRewriting(RewriterStepSequencer.newPlain, IfNoParameter, innerVariableNamer = new GeneratingNamer())

  def prepare(query: Statement, options: QueryOptions = QueryOptions.default) =
    FullyParsedQuery(
      state = semanticAnalysis.transform(
        InitialState("", None, PlannerNameFor(options.planner.name)).withStatement(query),
        ContextHelper.create()
      ),
      options = options
    )
}
