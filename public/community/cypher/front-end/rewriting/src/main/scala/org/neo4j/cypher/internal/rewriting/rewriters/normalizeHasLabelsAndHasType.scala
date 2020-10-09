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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.rewriting.RewritingStep
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.topDown

case object HasLabelsOrTypesReplacedIfPossible extends StepSequencer.Condition

case class normalizeHasLabelsAndHasType(semanticState: SemanticState) extends RewritingStep {

  // TODO this should be captured differently. This has an invalidated condition `ProjectionClausesHaveSemanticInfo`,
  // which is a pre-condition of expandStar. It can invalidate this condition by rewriting things inside WITH/RETURN.
  // But to do that we need a step that introduces that condition which would be SemanticAnalysis.
  override def preConditions: Set[StepSequencer.Condition] = Set(containsNoReturnAll)

  override def postConditions: Set[StepSequencer.Condition] = Set(HasLabelsOrTypesReplacedIfPossible)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def rewrite(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case p@HasLabelsOrTypes(e, labels) if semanticState.expressionType(e).actual == CTNode.invariant =>
      HasLabels(e, labels.map(l => LabelName(l.name)(l.position)))(p.position)
    case p@HasLabelsOrTypes(e, labels) if semanticState.expressionType(e).actual == CTRelationship.invariant =>
      HasTypes(e, labels.map(l => RelTypeName(l.name)(l.position)))(p.position)
  })
}
