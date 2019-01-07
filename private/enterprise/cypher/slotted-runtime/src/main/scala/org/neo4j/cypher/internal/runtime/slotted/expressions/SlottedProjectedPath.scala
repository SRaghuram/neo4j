/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, PathValueBuilder}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.{ListValue, RelationshipValue, VirtualRelationshipValue}

object SlottedProjectedPath {

  type Projector = (ExecutionContext, QueryState, PathValueBuilder) => PathValueBuilder

  object nilProjector extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = builder
  }

  case class singleNodeProjector(node: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val nodeValue = node.apply(ctx, state)
      tailProjector(ctx, state, builder.addNode(nodeValue))
    }
  }

  case class singleRelationshipWithKnownTargetProjector(rel: Expression, target: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relValue = rel.apply(ctx, state)
      val nodeValue = target.apply(ctx, state)
      tailProjector(ctx, state, builder.addRelationship(relValue).addNode(nodeValue))
    }
  }

  case class singleIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addIncoming(rel.apply(ctx,state), state.query, builder))
  }

  case class singleOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addOutgoing(rel.apply(ctx,state), state.query, builder))
  }

  case class singleUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addUndirected(rel.apply(ctx,state), state.query, builder))
  }

  case class multiIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue =>
        val iterator = list.iterator
        var aggregated = builder
        while (iterator.hasNext)
          aggregated = addIncoming(iterator.next(), state.query, aggregated)
        tailProjector(ctx, state, aggregated)
      case NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }
  }

  case class multiOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue =>
        val iterator = list.iterator
        var aggregated = builder
        while (iterator.hasNext)
          aggregated = addOutgoing(iterator.next(), state.query, aggregated)
        tailProjector(ctx, state, aggregated)
      case NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }
  }

  case class multiUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue if list.nonEmpty() =>
        val previous = builder.previousNode.id()
        val first = list.head().asInstanceOf[RelationshipValue]
        val correctDirection = first.startNode().id() == previous || first.endNode().id() == previous
        if (correctDirection) {
         val iterator = list.iterator
          var aggregated = builder
          while (iterator.hasNext)
            aggregated = addUndirected(iterator.next(), state.query, aggregated)
          tailProjector(ctx, state, aggregated)
        } else {
          val reversed = list.reverse()
          val iterator = reversed.iterator
          var aggregated = builder
          while (iterator.hasNext)
            aggregated = addUndirected(iterator.next(), state.query, aggregated)
          tailProjector(ctx, state, aggregated)
        }
      case _: ListValue => tailProjector(ctx, state, builder)
      case NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }
  }

  private def addIncoming(relValue: AnyValue, query: QueryContext, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      val cursor = query.singleRelationship(r.id())
      val nextNode = try {
        query.nodeById(cursor.sourceNodeReference())
      } finally {
        cursor.close()
      }
     builder.addRelationship(relValue).addNode(nextNode)

    case NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addOutgoing(relValue: AnyValue, query: QueryContext, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      val cursor = query.singleRelationship(r.id())
      val nextNode = try {
        query.nodeById(cursor.targetNodeReference())
      } finally {
        cursor.close()
      }
      builder.addRelationship(relValue).addNode(nextNode)

    case NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addUndirected(relValue: AnyValue, query: QueryContext, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      val cursor = query.singleRelationship(r.id())
      val previous = builder.previousNode
      val nextNode = try {
        if (cursor.targetNodeReference() == previous.id()) query.nodeById(cursor.sourceNodeReference())
        else if (cursor.sourceNodeReference() == previous.id()) query.nodeById(cursor.targetNodeReference())
        else throw new IllegalArgumentException(s"Invalid usage of PathValueBuilder, $previous must be a node in $relValue")
      } finally {
        cursor.close()
      }
      builder.addRelationship(relValue).addNode(nextNode)

    case NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }
}

/*
 Expressions for materializing new paths (used by ronja)

 These expressions cannot be generated by the user directly
 */
case class SlottedProjectedPath(symbolTableDependencies: Set[String], projector: SlottedProjectedPath.Projector) extends Expression {
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = projector(ctx, state, state.clearPathValueBuilder).result()

  def arguments: Seq[Expression] = Seq.empty

  def rewrite(f: Expression => Expression): Expression = f(this)
}

