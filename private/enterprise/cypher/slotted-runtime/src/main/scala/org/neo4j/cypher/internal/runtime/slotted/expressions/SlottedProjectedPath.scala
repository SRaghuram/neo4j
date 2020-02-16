/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PathValueBuilder
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.cypher.operations.CypherFunctions.endNode
import org.neo4j.cypher.operations.CypherFunctions.startNode
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

object SlottedProjectedPath {

  trait Projector {
    def apply(context: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder

    /**
     * The Expressions used in this Projector
     */
    def arguments: Seq[Expression]
  }

  object nilProjector extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = builder

    override def arguments: Seq[Expression] = Seq.empty
  }

  case class singleNodeProjector(node: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val nodeValue = node.apply(ctx, state)
      tailProjector(ctx, state, builder.addNode(nodeValue))
    }

    override def arguments: Seq[Expression] = Seq(node) ++ tailProjector.arguments
  }

  case class singleRelationshipWithKnownTargetProjector(rel: Expression, target: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relValue = rel.apply(ctx, state)
      val nodeValue = target.apply(ctx, state)
      tailProjector(ctx, state, builder.addRelationship(relValue).addNode(nodeValue))
    }

    override def arguments: Seq[Expression] = Seq(rel, target) ++ tailProjector.arguments
  }

  case class singleIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addIncoming(rel.apply(ctx,state), state, builder))

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class singleOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addOutgoing(rel.apply(ctx,state), state, builder))

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class singleUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addUndirected(rel.apply(ctx,state), state, builder))

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class multiIncomingRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue if list.nonEmpty() =>
        val aggregated = addAllExceptLast(builder, list, (b, v) => b.addIncomingRelationship(v))
        tailProjector(ctx, state, aggregated.addRelationship(list.last()).addNode(node.apply(ctx, state)))

      case _: ListValue => tailProjector(ctx, state, builder)
      case x if x eq NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }

    override def arguments: Seq[Expression] = Seq(rel, node) ++ tailProjector.arguments
  }

  case class multiOutgoingRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue if list.nonEmpty() =>
        val aggregated = addAllExceptLast(builder, list, (b, v) => b.addOutgoingRelationship(v))
        tailProjector(ctx, state, aggregated.addRelationship(list.last()).addNode(node.apply(ctx, state)))

      case _: ListValue => tailProjector(ctx, state, builder)
      case x if x eq NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }

    override def arguments: Seq[Expression] = Seq(rel, node) ++ tailProjector.arguments
  }

  case class multiUndirectedRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue if list.nonEmpty() =>
        if (correctDirection(builder.previousNode, list.head()))  {
          val aggregated = addAllExceptLast(builder, list, (b, v) => b.addUndirectedRelationship(v))
          tailProjector(ctx, state,aggregated.addRelationship(list.last()).addNode(node.apply(ctx, state)))
        } else {
          val reversed = list.reverse()
          val aggregated = addAllExceptLast(builder, reversed, (b, v) => b.addUndirectedRelationship(v))
          tailProjector(ctx, state, aggregated.addRelationship(reversed.last()).addNode(node.apply(ctx, state)))
        }

      case _: ListValue => tailProjector(ctx, state, builder)
      case x if x eq NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }

    override def arguments: Seq[Expression] = Seq(rel, node) ++ tailProjector.arguments
  }

  case class multiIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      //we know these relationships have already loaded start and end relationship
      //so we should not use CypherFunctions::[start,end]Node to look them up
      tailProjector(ctx, state, builder.addIncomingRelationships(relListValue))
    }

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class multiOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      //we know these relationships have already loaded start and end relationship
      //so we should not use CypherFunctions::[start,end]Node to look them up
      tailProjector(ctx, state, builder.addOutgoingRelationships(relListValue))
    }

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  case class multiUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    override def apply(ctx: CypherRow, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      //we know these relationships have already loaded start and end relationship
      //so we should not use CypherFunctions::[start,end]Node to look them up
      tailProjector(ctx, state, builder.addUndirectedRelationships(relListValue))
    }

    override def arguments: Seq[Expression] = Seq(rel) ++ tailProjector.arguments
  }

  private def addIncoming(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      builder.addRelationship(r).addNode(startNode(r, state.query, state.cursors.relationshipScanCursor))

    case x if x eq NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addOutgoing(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      builder.addRelationship(r).addNode(endNode(r, state.query, state.cursors.relationshipScanCursor))

    case x if x eq NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addUndirected(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      val previous = builder.previousNode
      builder.addRelationship(r).addNode(CypherFunctions.otherNode(r, state.query, previous, state.cursors.relationshipScanCursor))

    case x if x eq NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addAllExceptLast(builder: PathValueBuilder, list: ListValue, f: (PathValueBuilder, AnyValue) => PathValueBuilder) = {
    var aggregated = builder
    val size = list.size()
    var i = 0
    while (i < size - 1) {
      //we know these relationships have already loaded start and end relationship
      //so we should not use CypherFunctions::[start,end]Node to look them up
      aggregated = f(aggregated, list.value(i))
      i += 1
    }
    aggregated
  }

  private def correctDirection(previous: NodeValue, first: AnyValue) = {
    val rel = first.asInstanceOf[RelationshipValue]
    val correctDirection = rel.startNode().id() == previous.id() || rel.endNode().id() == previous.id()
    correctDirection
  }
}

/*
 Expressions for materializing new paths (used by ronja)

 These expressions cannot be generated by the user directly
 */
case class SlottedProjectedPath(symbolTableDependencies: Set[String], projector: SlottedProjectedPath.Projector) extends Expression {
  override def apply(ctx: CypherRow, state: QueryState): AnyValue = projector(ctx, state, state.clearPathValueBuilder).result()

  override def arguments: Seq[Expression] = Seq.empty

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def children: Seq[AstNode[_]] = projector.arguments
}

