/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v4_0.planner

import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.ir.v4_0._
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.v4_0.spi.{GraphStatistics, IndexOrderCapability}
import org.neo4j.cypher.internal.v4_0.logical.plans.{LogicalPlan, ProcedureSignature}
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions.{Expression, HasLabels}
import org.opencypher.v9_0.util.{Cardinality, Cost, LabelId}

class StubbedLogicalPlanningConfiguration(val parent: LogicalPlanningConfiguration)
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  self =>

  var knownLabels: Set[String] = Set.empty
  var cardinality: PartialFunction[PlannerQuery, Cardinality] = PartialFunction.empty
  var cost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = PartialFunction.empty
  var labelCardinality: Map[String, Cardinality] = Map.empty
  var statistics: GraphStatistics = null
  var qg: QueryGraph = null
  var expressionEvaluator: ExpressionEvaluator = new ExpressionEvaluator {
    override def evaluateExpression(expr: Expression): Option[Any] = ???

    override def isDeterministic(expr: Expression): Boolean = ???

    override def hasParameters(expr: Expression): Boolean = ???
  }

  var indexes: Set[(String, Seq[String])] = Set.empty
  var uniqueIndexes: Set[(String, Seq[String])] = Set.empty
  // A subset of indexes and uniqueIndexes
  var indexesWithValues: Set[(String, Seq[String])] = Set.empty

  var indexesWithOrdering: Map[(String, Seq[String]), IndexOrderCapability] = Map.empty

  var constraints: Set[(String, String)] = Set.empty

  var procedureSignatures: Set[ProcedureSignature] = Set.empty

  lazy val labelsById: Map[Int, String] = (indexes ++ uniqueIndexes).map(_._1).zipWithIndex.map(_.swap).toMap

  case class IndexModifier(label: String, properties: Seq[String]) {
    def providesValues(): IndexModifier = {
      indexesWithValues = indexesWithValues + (label -> properties)
      this
    }
    def providesOrder(order: IndexOrderCapability): IndexModifier = {
      indexesWithOrdering = indexesWithOrdering + ((label, properties) -> order)
      this
    }
  }

  def indexOn(label: String, properties: String*): IndexModifier = {
    indexes = indexes + (label -> properties)
    IndexModifier(label, properties)
  }

  def uniqueIndexOn(label: String, properties: String*): IndexModifier = {
    uniqueIndexes = uniqueIndexes + (label -> properties)
    IndexModifier(label, properties)
  }

  def existenceOrNodeKeyConstraintOn(label: String, property: String): Unit = {
    constraints = constraints + (label -> property)
  }

  def procedure(signature: ProcedureSignature): Unit = {
    procedureSignatures += signature
  }

  override def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities), Cost] = cost.orElse(parent.costModel())

  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, evaluator: ExpressionEvaluator): CardinalityModel = {
    new CardinalityModel {
      override def apply(pq: PlannerQuery, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
        val labelIdCardinality: Map[LabelId, Cardinality] = labelCardinality.map {
          case (name: String, cardinality: Cardinality) =>
            semanticTable.resolvedLabelNames(name) -> cardinality
        }
        val labelScanCardinality: PartialFunction[PlannerQuery, Cardinality] = {
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes.size == 1 &&
            computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).isDefined =>
            computeOptionCardinality(queryGraph, semanticTable, labelIdCardinality).get
        }

        val r: PartialFunction[PlannerQuery, Cardinality] = labelScanCardinality.orElse(cardinality)
        if (r.isDefinedAt(pq)) r.apply(pq) else parent.cardinalityModel(queryGraphCardinalityModel, evaluator)(pq, input, semanticTable)
      }
    }
  }

  private def computeOptionCardinality(queryGraph: QueryGraph, semanticTable: SemanticTable,
                                       labelIdCardinality: Map[LabelId, Cardinality]) = {
    val labelMap: Map[String, Set[HasLabels]] = queryGraph.selections.labelPredicates
    val labels = queryGraph.patternNodes.flatMap(labelMap.get).flatten.flatMap(_.labels)
    val results = labels.collect {
      case label if semanticTable.id(label).isDefined &&
                    labelIdCardinality.contains(semanticTable.id(label).get) =>
        labelIdCardinality(semanticTable.id(label).get)
    }
    results.headOption
  }

  override def graphStatistics: GraphStatistics =
    Option(statistics).getOrElse(parent.graphStatistics)

}
