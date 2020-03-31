/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.physicalplanning
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlan
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeCollectExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NestedPipeExistsExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.slotted
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters.orderGroupingKeyExpressions
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.Projector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.multiIncomingRelationshipProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.multiIncomingRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.multiOutgoingRelationshipProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.multiOutgoingRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.multiUndirectedRelationshipProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.multiUndirectedRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.nilProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.singleIncomingRelationshipProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.singleNodeProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.singleOutgoingRelationshipProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.singleRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath.singleUndirectedRelationshipProjector
import org.neo4j.cypher.internal.runtime.slotted.pipes.EmptyGroupingExpression
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlotExpression
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedGroupingExpression
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedGroupingExpression1
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedGroupingExpression2
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedGroupingExpression3
import org.neo4j.cypher.internal.util.attribution.Id

object SlottedExpressionConverters {
  // This is a shared method to provide consistent ordering of grouping keys for all slot-based runtimes
  def orderGroupingKeyExpressions(groupings: Iterable[(String, Expression)], orderToLeverage: Seq[Expression])
                                 (slots: SlotConfiguration): Seq[(String, Expression, Boolean)] = {
    // SlotConfiguration is a separate parameter list so it can be applied at a later stage
    groupings.toSeq.map(a => (a._1, a._2, orderToLeverage.contains(a._2)))
      // Sort grouping key (1) by expressions with provided order first, followed by unordered expressions
      //          and then (2) by slot offset
      .sortBy(b => (!b._3, slots(b._1).offset))
  }
}

case class SlottedExpressionConverters(physicalPlan: PhysicalPlan, maybeOwningPipe: Option[Pipe] = None) extends ExpressionConverter {

  override def toCommandProjection(id: Id, projections: Map[String, Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    val slots = physicalPlan.slotConfigurations(id)
    val projected = for {(k, v) <- projections if !slots(k).isLongSlot } yield slots(k).offset -> self.toCommandExpression(id, v)
    Some(SlottedCommandProjection(projected))
  }

  override def toGroupingExpression(id: Id,
                                    projections: Map[String, Expression],
                                    orderToLeverage: Seq[Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = {
    val slots = physicalPlan.slotConfigurations(id)
    val orderedGroupings = orderGroupingKeyExpressions(projections, orderToLeverage)(slots)
      .map(e => (slots(e._1), self.toCommandExpression(id, e._2), e._3))

    orderedGroupings.toList match {
      case Nil => Some(EmptyGroupingExpression)
      case (slot, e, ordered)::Nil => Some(SlottedGroupingExpression1(SlotExpression(slot, e, ordered)))
      case (s1, e1, o1)::(s2, e2, o2)::Nil => Some(SlottedGroupingExpression2(SlotExpression(s1, e1, o1), SlotExpression(s2, e2, o2)))
      case (s1, e1, o1)::(s2, e2, o2)::(s3, e3, o3)::Nil => Some(SlottedGroupingExpression3(SlotExpression(s1, e1, o1), SlotExpression(s2, e2, o2), SlotExpression(s3, e3, o3)))
      case _ => Some(SlottedGroupingExpression(orderedGroupings.map(t => SlotExpression(t._1, t._2, t._3)).toArray))
    }
  }

  override def toCommandExpression(id: Id, expression: expressions.Expression, self: ExpressionConverters): Option[commands.expressions.Expression] =
    expression match {
      case physicalplanning.ast.NodeFromSlot(offset, _) =>
        Some(slotted.expressions.NodeFromSlot(offset))
      case physicalplanning.ast.RelationshipFromSlot(offset, _) =>
        Some(slotted.expressions.RelationshipFromSlot(offset))
      case physicalplanning.ast.ReferenceFromSlot(offset, _) =>
        Some(slotted.expressions.ReferenceFromSlot(offset))
      case physicalplanning.ast.NodeProperty(offset, token, _) =>
        Some(slotted.expressions.NodeProperty(offset, token))
      case physicalplanning.ast.SlottedCachedPropertyWithPropertyToken(_, _, offset, offsetIsForLongSlot, token, cachedPropertyOffset, NODE_TYPE, _) =>
        Some(slotted.expressions.SlottedCachedNodeProperty(offset, offsetIsForLongSlot, token, cachedPropertyOffset))
      case physicalplanning.ast.SlottedCachedPropertyWithPropertyToken(_, _, offset, offsetIsForLongSlot, token, cachedPropertyOffset, RELATIONSHIP_TYPE, _) =>
        Some(slotted.expressions.SlottedCachedRelationshipProperty(offset, offsetIsForLongSlot, token, cachedPropertyOffset))
      case physicalplanning.ast.RelationshipProperty(offset, token, _) =>
        Some(slotted.expressions.RelationshipProperty(offset, token))
      case physicalplanning.ast.IdFromSlot(offset) =>
        Some(slotted.expressions.IdFromSlot(offset))
      case physicalplanning.ast.LabelsFromSlot(offset) =>
        Some(slotted.expressions.LabelsFromSlot(offset))
      case e: physicalplanning.ast.HasLabelsFromSlot =>
        Some(hasLabelsFromSlot(id, e, self))
      case physicalplanning.ast.RelationshipTypeFromSlot(offset) =>
        Some(slotted.expressions.RelationshipTypeFromSlot(offset))
      case physicalplanning.ast.NodePropertyLate(offset, propKey, _) =>
        Some(slotted.expressions.NodePropertyLate(offset, propKey))
      case physicalplanning.ast.SlottedCachedPropertyWithoutPropertyToken(_, _, offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset, NODE_TYPE, _) =>
        Some(slotted.expressions.SlottedCachedNodePropertyLate(offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset))
      case physicalplanning.ast.SlottedCachedPropertyWithoutPropertyToken(_, _, offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset, RELATIONSHIP_TYPE, _) =>
        Some(slotted.expressions.SlottedCachedRelationshipPropertyLate(offset, offsetIsForLongSlot, propertyKey, cachedPropertyOffset))
      case physicalplanning.ast.RelationshipPropertyLate(offset, propKey, _) =>
        Some(slotted.expressions.RelationshipPropertyLate(offset, propKey))
      case physicalplanning.ast.PrimitiveEquals(a, b) =>
        val lhs = self.toCommandExpression(id, a)
        val rhs = self.toCommandExpression(id, b)
        Some(slotted.expressions.PrimitiveEquals(lhs, rhs))
      case physicalplanning.ast.GetDegreePrimitive(offset, typ, direction) =>
        Some(slotted.expressions.GetDegreePrimitive(offset, typ, direction))
      case physicalplanning.ast.NodePropertyExists(offset, token, _) =>
        Some(slotted.expressions.NodePropertyExists(offset, token))
      case physicalplanning.ast.NodePropertyExistsLate(offset, token, _) =>
        Some(slotted.expressions.NodePropertyExistsLate(offset, token))
      case physicalplanning.ast.RelationshipPropertyExists(offset, token, _) =>
        Some(slotted.expressions.RelationshipPropertyExists(offset, token))
      case physicalplanning.ast.RelationshipPropertyExistsLate(offset, token, _) =>
        Some(slotted.expressions.RelationshipPropertyExistsLate(offset, token))
      case physicalplanning.ast.NullCheck(offset, inner) =>
        val a = self.toCommandExpression(id, inner)
        Some(slotted.expressions.NullCheck(offset, a))
      case physicalplanning.ast.NullCheckVariable(offset, inner) =>
        val a = self.toCommandExpression(id, inner)
        Some(slotted.expressions.NullCheck(offset, a))
      case physicalplanning.ast.NullCheckProperty(offset, inner) =>
        val a = self.toCommandExpression(id, inner)
        Some(slotted.expressions.NullCheck(offset, a))
      case physicalplanning.ast.NullCheckReferenceProperty(offset, inner) =>
        val a = self.toCommandExpression(id, inner)
        Some(slotted.expressions.NullCheckReference(offset, a))
      case e: expressions.PathExpression =>
        Some(toCommandProjectedPath(id, e, self))
      case physicalplanning.ast.IsPrimitiveNull(offset) =>
        Some(slotted.expressions.IsPrimitiveNull(offset))
      case e: ExpressionVariable =>
        Some(commands.expressions.ExpressionVariable(e.offset, e.name))
      case e: NestedPipeCollectExpression =>
        Some(slotted.expressions.NestedPipeCollectSlottedExpression(
          e.pipe,
          self.toCommandExpression(id, e.projection),
          physicalPlan.nestedPlanArgumentConfigurations(e.pipe.id),
          e.availableExpressionVariables.map(commands.expressions.ExpressionVariable.of).toArray,
          id))
      case e: NestedPipeExistsExpression =>
        Some(slotted.expressions.NestedPipeExistsSlottedExpression(
          e.pipe,
          physicalPlan.nestedPlanArgumentConfigurations(e.pipe.id),
          e.availableExpressionVariables.map(commands.expressions.ExpressionVariable.of).toArray,
          id))
      case _ =>
        None
    }

  private def hasLabelsFromSlot(id: Id, e: physicalplanning.ast.HasLabelsFromSlot, self: ExpressionConverters): Predicate = {
    val preds =
      e.resolvedLabelTokens.map { labelId =>
        HasLabelFromSlot(e.offset, labelId)
      } ++
        e.lateLabels.map { labelName =>
          HasLabelFromSlotLate(e.offset, labelName): Predicate
        }
    commands.predicates.Ands(preds: _*)
  }

  def toCommandProjectedPath(id:Id, e: expressions.PathExpression, self: ExpressionConverters): SlottedProjectedPath = {
    def project(pathStep: PathStep): Projector = pathStep match {

      case NodePathStep(nodeExpression, next) =>
        singleNodeProjector(toCommandExpression(id, nodeExpression, self).get, project(next))

      case expressions.SingleRelationshipPathStep(relExpression, _, Some(targetNodeExpression), next) =>
        singleRelationshipWithKnownTargetProjector(toCommandExpression(id, relExpression, self).get,
          toCommandExpression(id, targetNodeExpression, self).get,
          project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.INCOMING, _, next) =>
        singleIncomingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, _, next) =>
        singleOutgoingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.BOTH, _, next) =>
        singleUndirectedRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.INCOMING, Some(targetNodeExpression), next) =>
        multiIncomingRelationshipWithKnownTargetProjector(
          toCommandExpression(id, relExpression, self).get,
          toCommandExpression(id, targetNodeExpression, self).get,
          project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.INCOMING, _, next) =>
        multiIncomingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, Some(targetNodeExpression), next) =>
        multiOutgoingRelationshipWithKnownTargetProjector(
          toCommandExpression(id, relExpression, self).get,
          toCommandExpression(id, targetNodeExpression, self).get,
          project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, _, next) =>
        multiOutgoingRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.BOTH, Some(targetNodeExpression), next) =>
        multiUndirectedRelationshipWithKnownTargetProjector(
          toCommandExpression(id, relExpression, self).get,
          toCommandExpression(id, targetNodeExpression, self).get,
          project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.BOTH, _, next) =>
        multiUndirectedRelationshipProjector(toCommandExpression(id, relExpression, self).get, project(next))

      case NilPathStep =>
        nilProjector
    }

    val projector = project(e.step)
    // Symbol table dependencies is only used for runtime expressions by PatternMatcher. It would be nice to
    // get rid of this from runtime expressions.
    val dependencies = e.step.dependencies.flatMap(_.dependencies).map(_.name)

    SlottedProjectedPath(dependencies, projector)
  }

}
