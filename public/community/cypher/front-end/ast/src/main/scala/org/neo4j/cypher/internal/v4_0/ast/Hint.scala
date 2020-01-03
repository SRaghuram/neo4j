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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticAnalysisTooling, SemanticCheckable, _}
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, Parameter, PropertyKeyName, Variable, _}
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, InputPosition, NonEmptyList}

sealed trait Hint extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {
  def variables: NonEmptyList[Variable]
}

trait NodeHint {
  self: Hint =>
}

trait RelationshipHint {
  self: Hint =>
}

object Hint {
  implicit val byVariable: Ordering[Hint] =
    Ordering.by { hint: Hint => hint.variables.head }(Variable.byName)
}
// allowed on match

sealed trait UsingHint extends Hint

// allowed on start item

sealed trait UsingIndexHintSpec {
  def fulfilledByScan: Boolean
}
case object SeekOnly extends UsingIndexHintSpec {
  override def fulfilledByScan: Boolean = false
}
case object SeekOrScan extends UsingIndexHintSpec {
  override def fulfilledByScan: Boolean = true
}

case class UsingIndexHint(
                           variable: Variable,
                           label: LabelName,
                           properties: Seq[PropertyKeyName],
                           spec: UsingIndexHintSpec = SeekOrScan
                         )(val position: InputPosition) extends UsingHint with NodeHint {
  def variables = NonEmptyList(variable)
  def semanticCheck = ensureDefined(variable) chain expectType(CTNode.covariant, variable)

  override def toString: String = s"USING INDEX ${if(spec == SeekOnly) "SEEK " else ""}${variable.name}:${label.name}(${properties.map(_.name).mkString(", ")})"
}

case class UsingScanHint(variable: Variable, label: LabelName)(val position: InputPosition) extends UsingHint with NodeHint {
  def variables = NonEmptyList(variable)
  def semanticCheck = ensureDefined(variable) chain expectType(CTNode.covariant, variable)

  override def toString: String = s"USING SCAN ${variable.name}:${label.name}"
}

object UsingJoinHint {
  import NonEmptyList._

  def apply(elts: Seq[Variable])(pos: InputPosition): UsingJoinHint =
    UsingJoinHint(elts.toNonEmptyListOption.getOrElse(throw new IllegalStateException("Expected non-empty sequence of variables")))(pos)
}

case class UsingJoinHint(variables: NonEmptyList[Variable])(val position: InputPosition) extends UsingHint with NodeHint {
  def semanticCheck =
    variables.map { variable => ensureDefined(variable) chain expectType(CTNode.covariant, variable) }.reduceLeft(_ chain _)

  override def toString: String = s"USING JOIN ON ${variables.map(_.name).toIndexedSeq.mkString(", ")}"
}

// start items

sealed trait StartItem extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {
  def variable: Variable
  def name = variable.name
}

sealed trait NodeStartItem extends StartItem {
  def semanticCheck = declareVariable(variable, CTNode)
}

case class NodeByParameter(variable: Variable, parameter: Param)(val position: InputPosition) extends NodeStartItem
case class AllNodes(variable: Variable)(val position: InputPosition) extends NodeStartItem

sealed trait RelationshipStartItem extends StartItem {
  def semanticCheck = declareVariable(variable, CTRelationship)
}

case class RelationshipByIds(variable: Variable, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends RelationshipStartItem
case class RelationshipByParameter(variable: Variable, parameter: Param)(val position: InputPosition) extends RelationshipStartItem
case class AllRelationships(variable: Variable)(val position: InputPosition) extends RelationshipStartItem

// no longer supported non-hint legacy start items

case class NodeByIds(variable: Variable, ids: Seq[UnsignedIntegerLiteral])(val position: InputPosition) extends NodeStartItem

