/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.frontend.phases

import org.neo4j.cypher.internal.v4_0.ast.semantics.Scope
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.ast.semantics.SymbolUse
import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.ProcedureOutput
import org.neo4j.cypher.internal.v4_0.expressions.Variable
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.v4_0.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.v4_0.util.Ref
import org.neo4j.cypher.internal.v4_0.util.Rewriter
import org.neo4j.cypher.internal.v4_0.util.bottomUp
import org.neo4j.cypher.internal.v4_0.util.inSequence

object Namespacer extends Phase[BaseContext, BaseState, BaseState] {
  type VariableRenamings = Map[Ref[Variable], Variable]

  override def phase: CompilationPhase = AST_REWRITE

  override def description: String = "rename variables so they are all unique"

  override def process(from: BaseState, ignored: BaseContext): BaseState = {
    val withProjectedUnions = from.statement().endoRewrite(projectUnions)

    val ambiguousNames = shadowedNames(from.semantics().scopeTree)
    val variableDefinitions: Map[SymbolUse, SymbolUse] = from.semantics().scopeTree.allVariableDefinitions
    val renamings = variableRenamings(withProjectedUnions, variableDefinitions, ambiguousNames)

    val rewriter = renamingRewriter(renamings)
    val newStatement = withProjectedUnions.endoRewrite(rewriter)
    val table = SemanticTable(types = from.semantics().typeTable, recordedScopes = from.semantics().recordedScopes)

    val newSemanticTable = table.replaceExpressions(rewriter)
    from.withStatement(newStatement).withSemanticTable(newSemanticTable)
  }

  override def postConditions: Set[Condition] = Set(
    StatementCondition(containsNoNodesOfType[UnionAll]),
    StatementCondition(containsNoNodesOfType[UnionDistinct]))

  private def shadowedNames(scopeTree: Scope): Set[String] = {
    val definitions = scopeTree.allSymbolDefinitions

    definitions.collect {
      case (name, symbolDefinitions) if symbolDefinitions.size > 1 => name
    }.toSet
  }

  private def variableRenamings(statement: Statement, variableDefinitions: Map[SymbolUse, SymbolUse],
                                ambiguousNames: Set[String]): VariableRenamings =
    statement.treeFold(Map.empty[Ref[Variable], Variable]) {
      case i: Variable if ambiguousNames(i.name) =>
        val symbolDefinition = variableDefinitions(SymbolUse(i))
        val newVariable = i.renameId(s"  ${symbolDefinition.nameWithPosition}")
        val renaming = Ref(i) -> newVariable
        acc => (acc + renaming, Some(identity))
    }

  private def projectUnions: Rewriter =
    bottomUp(Rewriter.lift {
      case u: UnionAll => ProjectingUnionAll(u.part, u.query, u.unionMappings)(u.position)
      case u: UnionDistinct => ProjectingUnionDistinct(u.part, u.query, u.unionMappings)(u.position)
    })

  private def renamingRewriter(renamings: VariableRenamings): Rewriter = inSequence(
    bottomUp(Rewriter.lift {
      case item@ProcedureResultItem(None, v: Variable) if renamings.contains(Ref(v)) =>
        item.copy(output = Some(ProcedureOutput(v.name)(v.position)))(item.position)
    }),
    bottomUp(Rewriter.lift {
      case v: Variable =>
        renamings.get(Ref(v)) match {
          case Some(newVariable) => newVariable
          case None              => v
        }
    }))

}
