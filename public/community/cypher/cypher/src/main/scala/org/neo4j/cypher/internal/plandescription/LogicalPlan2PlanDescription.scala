/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.plandescription.Arguments._
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import org.neo4j.cypher.internal.v4_0.expressions.functions.Point
import org.neo4j.cypher.internal.v4_0.expressions.{FunctionInvocation, FunctionName, LabelToken, MapExpression, Namespace, PropertyKeyToken, Expression => ASTExpression}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.util.InternalException

object LogicalPlan2PlanDescription {

  def apply(input: LogicalPlan,
            plannerName: PlannerName,
            cypherVersion: CypherVersion,
            readOnly: Boolean,
            cardinalities: Cardinalities,
            providedOrders: ProvidedOrders): InternalPlanDescription = {
    new LogicalPlan2PlanDescription(readOnly, cardinalities, providedOrders).create(input)
      .addArgument(Version("CYPHER " + cypherVersion.name))
      .addArgument(RuntimeVersion("4.0"))
      .addArgument(Planner(plannerName.toTextOutput))
      .addArgument(PlannerImpl(plannerName.name))
      .addArgument(PlannerVersion("4.0"))
  }
}

case class LogicalPlan2PlanDescription(readOnly: Boolean, cardinalities: Cardinalities, providedOrders: ProvidedOrders)
  extends LogicalPlans.Mapper[InternalPlanDescription] {

  def create(plan: LogicalPlan): InternalPlanDescription =
    LogicalPlans.map(plan, this)

  override def onLeaf(plan: LogicalPlan): InternalPlanDescription = {
    assert(plan.isLeaf)

    val id = plan.id
    val variables = plan.availableSymbols ++ plan.availableCachedNodeProperties.values.map(_.asCanonicalStringVal)

    val result: InternalPlanDescription = plan match {
      case _: AllNodesScan =>
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq.empty, variables)

      case NodeByLabelScan(_, label, _) =>
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(LabelName(label.name)), variables)

      case NodeByIdSeek(_, _, _) =>
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(), variables)

      case NodeIndexSeek(_, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getDescriptions(label, properties.map(_.propertyKeyToken), valueExpr, unique = false, readOnly)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(indexDesc), variables)

      case NodeUniqueIndexSeek(_, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getDescriptions(label, properties.map(_.propertyKeyToken), valueExpr, unique = true, readOnly)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(indexDesc), variables)

      case ProduceResult(_, _) =>
        PlanDescriptionImpl(id, "ProduceResults", NoChildren, Seq(), variables)

      case _: plans.Argument if variables.nonEmpty =>
        PlanDescriptionImpl(id, "Argument", NoChildren, Seq.empty, variables)

      case _: plans.Argument =>
        ArgumentPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(_, relIds, _, _, _) =>
        val entityByIdRhs = EntityByIdRhs(relIds)
        PlanDescriptionImpl(id, "DirectedRelationshipByIdSeek", NoChildren, Seq(entityByIdRhs), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", NoChildren, Seq.empty, variables)

      case NodeCountFromCountStore(variable, labelNames, _) =>
        val arguments = Seq(CountNodesExpression(variable, labelNames.map(l => l.map(_.name))))
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren, arguments, variables)

      case NodeIndexContainsScan(_, label, property, valueExpr, _, _) =>
        val arguments = Seq(Index(label.name, Seq(property.propertyKeyToken.name)), Expression(valueExpr))
        PlanDescriptionImpl(id, "NodeIndexContainsScan", NoChildren, arguments, variables)

      case NodeIndexEndsWithScan(_, label, property, valueExpr, _, _) =>
        val arguments = Seq(Index(label.name, Seq(property.propertyKeyToken.name)), Expression(valueExpr))
        PlanDescriptionImpl(id, "NodeIndexEndsWithScan", NoChildren, arguments, variables)

      case NodeIndexScan(_, label, properties, _, _) =>
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren, Seq(Index(label.name, properties.map(_.propertyKeyToken.name))), variables)

      case ProcedureCall(_, call) =>
        val signature = Signature(call.qualifiedName, call.callArguments, call.callResultTypes)
        PlanDescriptionImpl(id, "ProcedureCall", NoChildren, Seq(signature), variables)

      case StandAloneProcedureCall(signature, _, resultSymbols, _) =>
        val signatureDesc = Signature(signature.name, Seq.empty, resultSymbols)
        PlanDescriptionImpl(id, "ProcedureCall", NoChildren, Seq(signatureDesc), resultSymbols.map(_._1).toSet)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        val exp = CountRelationshipsExpression(ident, startLabel.map(_.name), typeNames.map(_.name),
                                               endLabel.map(_.name))
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(exp), variables)

      case _: UndirectedRelationshipByIdSeek =>
        PlanDescriptionImpl(id, "UndirectedRelationshipByIdSeek", NoChildren, Seq.empty, variables)

      case _: CreateIndex =>
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq.empty, variables)

      case _: DropIndex =>
        PlanDescriptionImpl(id, "DropIndex", NoChildren, Seq.empty, variables)

      case _: CreateUniquePropertyConstraint =>
        PlanDescriptionImpl(id, "CreateUniquePropertyConstraint", NoChildren, Seq.empty, variables)

      case _: CreateNodeKeyConstraint =>
        PlanDescriptionImpl(id, "CreateNodeKeyConstraint", NoChildren, Seq.empty, variables)

      case _: CreateNodePropertyExistenceConstraint =>
        PlanDescriptionImpl(id, "CreateNodePropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case _: CreateRelationshipPropertyExistenceConstraint =>
        PlanDescriptionImpl(id, "CreateRelationshipPropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case _: DropUniquePropertyConstraint =>
        PlanDescriptionImpl(id, "DropUniquePropertyConstraint", NoChildren, Seq.empty, variables)

      case _: DropNodeKeyConstraint =>
        PlanDescriptionImpl(id, "DropNodeKeyConstraint", NoChildren, Seq.empty, variables)

      case _: DropNodePropertyExistenceConstraint =>
        PlanDescriptionImpl(id, "DropNodePropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case _: DropRelationshipPropertyExistenceConstraint =>
        PlanDescriptionImpl(id, "DropRelationshipPropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case ShowDatabase(name) =>
        val dbName = Database(name)
        PlanDescriptionImpl(id, "ShowDatabase", NoChildren, Seq(dbName), variables)

      case ShowDatabases() =>
        PlanDescriptionImpl(id, "ShowDatabases", NoChildren, Seq.empty, variables)

      case CreateDatabase(name) =>
        val dbName = Database(name)
        PlanDescriptionImpl(id, "CreateDatabase", NoChildren, Seq(dbName), variables)

      case DropDatabase(name) =>
        val dbName = Database(name)
        PlanDescriptionImpl(id, "DropDatabase", NoChildren, Seq(dbName), variables)

      case StartDatabase(name) =>
        val dbName = Database(name)
        PlanDescriptionImpl(id, "StartDatabase", NoChildren, Seq(dbName), variables)

      case StopDatabase(name) =>
        val dbName = Database(name)
        PlanDescriptionImpl(id, "StopDatabase", NoChildren, Seq(dbName), variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addPlanningAttributes(result, plan)
  }

  override def onOneChildPlan(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    assert(plan.lhs.nonEmpty)
    assert(plan.rhs.isEmpty)

    val id = plan.id
    val variables = plan.availableSymbols ++ plan.availableCachedNodeProperties.values.map(_.asCanonicalStringVal)
    val children = if (source.isInstanceOf[ArgumentPlanDescription]) NoChildren else SingleChild(source)

    val result: InternalPlanDescription = plan match {
      case Aggregation(_, groupingExpressions, aggregationExpressions) if aggregationExpressions.isEmpty =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case Distinct(_, groupingExpressions) =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case OrderedDistinct(_, groupingExpressions, _) =>
        PlanDescriptionImpl(id, "OrderedDistinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case Aggregation(_, groupingExpressions, _) =>
        PlanDescriptionImpl(id, "EagerAggregation", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)),
                            variables)

      case _: Create =>
        PlanDescriptionImpl(id, "Create", children, Seq.empty, variables)

      case _: DeleteExpression | _: DeleteNode | _: DeletePath | _: DeleteRelationship =>
        PlanDescriptionImpl(id, "Delete", children, Seq.empty, variables)

      case _: DetachDeleteExpression | _: DetachDeleteNode | _: DetachDeletePath =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq.empty, variables)

      case _: Eager =>
        PlanDescriptionImpl(id, "Eager", children, Seq.empty, variables)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables)

      case _: DropResult =>
        PlanDescriptionImpl(id, "DropResult", children, Seq.empty, variables)

      case NodeCountFromCountStore(idName, labelName, _) =>
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren,
                            Seq(CountNodesExpression(idName, labelName.map(l => l.map(_.name)))), variables)

      case RelationshipCountFromCountStore(idName, start, types, end, _) =>
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren,
                            Seq(
                              CountRelationshipsExpression(idName, start.map(_.name), types.map(_.name), end.map(_.name))),
                            variables)

      case NodeUniqueIndexSeek(_, label, properties, _, _, _) =>
        PlanDescriptionImpl(id = plan.id, "NodeUniqueIndexSeek", NoChildren,
                            Seq(Index(label.name, properties.map(_.propertyKeyToken.name))), variables)

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables)

      case Expand(_, fromName, dir, typeNames, toName, relName, mode) =>
        val expression = ExpandExpression(fromName, relName, typeNames.map(_.name), toName, dir, 1, Some(1))
        val modeText = mode match {
          case ExpandAll => "Expand(All)"
          case ExpandInto => "Expand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(expression), variables)

      case Limit(_, count, DoNotIncludeTies) =>
        PlanDescriptionImpl(id, name = "Limit", children, Seq(Expression(count)), variables)

      case LockNodes(_, nodesToLock) =>
        PlanDescriptionImpl(id, name = "LockNodes", children, Seq(KeyNames(nodesToLock.toSeq)), variables)

      case OptionalExpand(_, fromName, dir, typeNames, toName, relName, mode, predicates) =>
        val expressions = predicates.map(Expression.apply).toSeq :+
          ExpandExpression(fromName, relName, typeNames.map(_.name), toName, dir, 1, Some(1))
        val modeText = mode match {
          case ExpandAll => "OptionalExpand(All)"
          case ExpandInto => "OptionalExpand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, expressions, variables)

      case ProduceResult(_, _) =>
        PlanDescriptionImpl(id, "ProduceResults", children, Seq(), variables)

      case Projection(_, expr) =>
        val expressions = Expressions(expr)
        PlanDescriptionImpl(id, "Projection", children, Seq(expressions), variables)

      case Selection(predicate, _) =>
        PlanDescriptionImpl(id, "Filter", children, predicate.exprs.map(Expression).toSeq, variables)

      case Skip(_, count) =>
        PlanDescriptionImpl(id, name = "Skip", children, Seq(Expression(count)), variables)

      case FindShortestPaths(_, _, predicates, _, _) =>
        val args = predicates.zipWithIndex.map { case (p, idx) => s"p$idx" -> p }
        PlanDescriptionImpl(id, "ShortestPath", children, Seq(Expressions(args.toMap)), variables)

      case Limit(_, count, _) =>
        PlanDescriptionImpl(id, "Limit", children, Seq(Expression(count)), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", children, Seq.empty, variables)

      case _: MergeCreateNode =>
        PlanDescriptionImpl(id, "MergeCreateNode", children, Seq.empty, variables)

      case _: MergeCreateRelationship =>
        PlanDescriptionImpl(id, "MergeCreateRelationship", children, Seq.empty, variables)

      case _: Optional =>
        PlanDescriptionImpl(id, "Optional", children, Seq.empty, variables)

      case ProcedureCall(_, call) =>
        val signature = Signature(call.qualifiedName, call.callArguments, call.callResultTypes)
        PlanDescriptionImpl(id, "ProcedureCall", children, Seq(signature), variables)

      case ProjectEndpoints(_, relName, start, _, end, _, _, directed, _) =>
        val name = if (directed) "ProjectEndpoints" else "ProjectEndpoints(BOTH)"
        PlanDescriptionImpl(id, name, children, Seq(KeyNames(Seq(relName, start, end))), variables)

      case PruningVarExpand(_, fromName, dir, types, toName, min, max, maybeNodePredicate, maybeRelationshipPredicate) =>
        val expandSpec = ExpandExpression(fromName, "", types.map(_.name), toName, dir, minLength = min,
                                          maxLength = Some(max))
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate)
        PlanDescriptionImpl(id, s"VarLengthExpand(Pruning)", children, Seq(expandSpec) ++ predicatesDescription, variables)

      case _: RemoveLabels =>
        PlanDescriptionImpl(id, "RemoveLabels", children, Seq.empty, variables)

      case _: SetLabels =>
        PlanDescriptionImpl(id, "SetLabels", children, Seq.empty, variables)

      case _: SetNodePropertiesFromMap =>
        PlanDescriptionImpl(id, "SetNodePropertyFromMap", children, Seq.empty, variables)

      case _: SetProperty |
           _: SetNodeProperty |
           _: SetRelationshipProperty =>
        PlanDescriptionImpl(id, "SetProperty", children, Seq.empty, variables)

      case _: SetRelationshipPropertiesFromMap =>
        PlanDescriptionImpl(id, "SetRelationshipPropertyFromMap", children, Seq.empty, variables)

      case Sort(_, orderBy) =>
        PlanDescriptionImpl(id, "Sort", children, Seq(KeyNames(orderBy.map(_.id))), variables)

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix) =>
        PlanDescriptionImpl(id, "PartialSort", children, Seq(KeyNames(alreadySortedPrefix.map(_.id)), KeyNames(stillToSortSuffix.map(_.id))), variables)

      case Top(_, orderBy, limit) =>
        PlanDescriptionImpl(id, "Top", children, Seq(KeyNames(orderBy.map(_.id)), Expression(limit)), variables)

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit) =>
        PlanDescriptionImpl(id, "PartialTop", children, Seq(KeyNames(alreadySortedPrefix.map(_.id)), KeyNames(stillToSortSuffix.map(_.id)), Expression(limit)), variables)

      case UnwindCollection(_, _, expression) =>
        PlanDescriptionImpl(id, "Unwind", children, Seq(Expression(expression)), variables)

      case VarExpand(_, fromName, dir, _, types, toName, relName, length, mode, maybeNodePredicate, maybeRelationshipPredicate) =>
        val expandDescription = ExpandExpression(fromName, relName, types.map(_.name), toName, dir,
                                                 minLength = length.min, maxLength = length.max)
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate)
        val modeDescr = mode match {
          case ExpandAll => "All"
          case ExpandInto => "Into"
        }
        PlanDescriptionImpl(id, s"VarLengthExpand($modeDescr)", children,
                            Seq(expandDescription) ++ predicatesDescription, variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addPlanningAttributes(result, plan)
  }

  override def onTwoChildPlan(plan: LogicalPlan, lhs: InternalPlanDescription,
                              rhs: InternalPlanDescription): InternalPlanDescription = {
    assert(plan.lhs.nonEmpty)
    assert(plan.rhs.nonEmpty)

    val id = plan.id
    val variables = plan.availableSymbols ++ plan.availableCachedNodeProperties.values.map(_.asCanonicalStringVal)
    val children = TwoChildren(lhs, rhs)

    val result: InternalPlanDescription = plan match {
      case _: AntiConditionalApply =>
        PlanDescriptionImpl(id, "AntiConditionalApply", children, Seq.empty, variables)

      case _: AntiSemiApply =>
        PlanDescriptionImpl(id, "AntiSemiApply", children, Seq.empty, variables)

      case _: ConditionalApply =>
        PlanDescriptionImpl(id, "ConditionalApply", children, Seq.empty, variables)

      case _: Apply =>
        PlanDescriptionImpl(id, "Apply", children, Seq.empty, variables)

      case _: AssertSameNode =>
        PlanDescriptionImpl(id, "AssertSameNode", children, Seq.empty, variables)

      case CartesianProduct(_, _) =>
        PlanDescriptionImpl(id, "CartesianProduct", children, Seq.empty, variables)

      case NodeHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeHashJoin", children, Seq(KeyNames(nodes.toIndexedSeq)), variables)

      case _: ForeachApply =>
        PlanDescriptionImpl(id, "Foreach", children, Seq.empty, variables)

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case row: plans.Argument =>
        ArgumentPlanDescription(id = plan.id, Seq.empty, row.argumentIds)

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case _: LetSemiApply =>
        PlanDescriptionImpl(id, "LetSemiApply", children, Seq.empty, variables)

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(id, "LetAntiSemiApply", children, Seq.empty, variables)

      case LeftOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeLeftOuterHashJoin", children, Seq(KeyNames(nodes.toSeq)), variables)

      case RightOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeRightOuterHashJoin", children, Seq(KeyNames(nodes.toSeq)), variables)

      case RollUpApply(_, _, collectionName, _, _) =>
        PlanDescriptionImpl(id, "RollUpApply", children, Seq(KeyNames(Seq(collectionName))), variables)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables)

      case TriadicSelection(_, _, _, source, seen, target) =>
        PlanDescriptionImpl(id, "TriadicSelection", children, Seq(KeyNames(Seq(source, seen, target))), variables)

      case _: Union =>
        PlanDescriptionImpl(id, "Union", children, Seq.empty, variables)

      case ValueHashJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueHashJoin",
          children = children,
          arguments = Seq(Expression(predicate)),
          variables
        )

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addPlanningAttributes(result, plan)
  }

  private def addPlanningAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    val withEstRows = if (cardinalities.isDefinedAt(plan.id))
      description.addArgument(EstimatedRows(cardinalities.get(plan.id).amount))
    else
      description

    if (providedOrders.isDefinedAt(plan.id) && !providedOrders(plan.id).isEmpty)
      withEstRows.addArgument(Order(providedOrders(plan.id)))
    else
      withEstRows
  }

  private def buildPredicatesDescription(maybeNodePredicate: Option[VariablePredicate],
                                         maybeRelationshipPredicate: Option[VariablePredicate]): Option[Expressions] = {
    val predicatesMap =
      (maybeNodePredicate.map(variablePredicate => "node" -> variablePredicate.predicate) ++
        maybeRelationshipPredicate.map(variablePredicate => "relationship" -> variablePredicate.predicate)).toMap

    if (predicatesMap.isEmpty)
      None
    else
      Some(Expressions(predicatesMap))
  }

  private def getDescriptions(label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[ASTExpression],
                              unique: Boolean,
                              readOnly: Boolean): (String, Argument) = {

    def findName(exactOnly: Boolean =  true) =
      if (unique && !readOnly && exactOnly) "NodeUniqueIndexSeek(Locking)"
      else if (unique) "NodeUniqueIndexSeek"
      else "NodeIndexSeek"

    val (name, indexDesc) = valueExpr match {
      case e: RangeQueryExpression[_] =>
        assert(propertyKeys.size == 1)
        val propertyKey = propertyKeys.head.name
        val name = if (unique) "NodeUniqueIndexSeekByRange" else "NodeIndexSeekByRange"
        e.expression match {
          case PrefixSeekRangeWrapper(range) =>
            (name, PrefixIndex(label.name, propertyKey, range.prefix))
          case InequalitySeekRangeWrapper(RangeLessThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey,
              bounds.map(bound => s"<${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq))
          case InequalitySeekRangeWrapper(RangeGreaterThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey,
              bounds.map(bound => s">${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq))
          case InequalitySeekRangeWrapper(RangeBetween(greaterThanBounds, lessThanBounds)) =>
            val greaterThanBoundsText = greaterThanBounds.bounds.map(bound =>
              s">${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq
            val lessThanBoundsText = lessThanBounds.bounds.map(bound =>
              s"<${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq
            (name, InequalityIndex(label.name, propertyKey, greaterThanBoundsText ++ lessThanBoundsText))
          case PointDistanceSeekRangeWrapper(PointDistanceRange(point, distance, inclusive)) =>
            val funcName = Point.name
            val poi = point match {
              case FunctionInvocation(Namespace(List()), FunctionName(`funcName`), _, Seq(MapExpression(args))) =>
                s"point(${args.map(_._1.name).mkString(",")})"
              case _ => point.toString
            }
            (name, PointDistanceIndex(label.name, propertyKey, poi, distance.toString, inclusive))
          case _ => throw new InternalException("This should never happen. Missing a case?")
        }
      case e: CompositeQueryExpression[_] =>
        val predicates = e.inner.map {
          case _: ExistenceQueryExpression[Expression] => "exists"
          case _: RangeQueryExpression[Expression] => "range"
          case _: CompositeQueryExpression[Expression] =>
            throw new InternalException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")
          case _ => "equality"
        }
        (s"${findName(e.exactOnly)}(${predicates.mkString(",")})", Index(label.name, propertyKeys.map(_.name)))
      case _ => (findName(), Index(label.name, propertyKeys.map(_.name)))
    }

    (name, indexDesc)
  }
}
