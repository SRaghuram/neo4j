/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.rewriters.recordScopes

case class SemanticAnalysis(warn: Boolean, features: SemanticFeature*)
  extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val startState = {
      if (from.initialFields.nonEmpty)
        SemanticState.withStartingVariables(from.initialFields.toSeq: _*)
      else
        SemanticState.clean
    }.withFeatures(features: _*)

    val SemanticCheckResult(state, errors) = SemanticChecker.check(from.statement(), startState)
    if (warn) state.notifications.foreach(context.notificationLogger.log)

    context.errorHandler(errors)

    val table = from.maybeSemanticTable match {
      case Some(existingTable) =>
      // We might already have a SemanticTable from a previous run, and that might already have tokens.
      // We don't want to lose these
        existingTable.copy(types = state.typeTable, recordedScopes = state.recordedScopes.mapValues(_.scope))
      case None => SemanticTable(types = state.typeTable, recordedScopes = state.recordedScopes.mapValues(_.scope))
    }

    val rewrittenStatement = if (errors.isEmpty) {
      // Some expressions record some semantic information in themselves.
      // This is done by the recordScopes rewriter.
      // We need to apply it after each pass of SemanticAnalysis.
      from.statement().endoRewrite(recordScopes(state))
    } else {
      // If we have errors we should rather avoid running recordScopes, since the state might be incomplete.
      from.statement()
    }
    from
      .withStatement(rewrittenStatement)
      .withSemanticState(state)
      .withSemanticTable(table)
  }

  override def phase: CompilationPhaseTracer.CompilationPhase = SEMANTIC_CHECK

  override def description = "do variable binding, typing, type checking and other semantic checks"

  override def postConditions = Set(BaseContains[SemanticState], StatementCondition(containsNoNodesOfType[UnaliasedReturnItem]))
}
