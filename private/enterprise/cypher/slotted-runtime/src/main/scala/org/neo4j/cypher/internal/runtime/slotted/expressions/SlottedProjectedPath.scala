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
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.{ListValue, RelationshipValue}

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

  case class multiIncomingRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue =>
        var aggregated = builder
        val size = list.size()
        var i = 0
        while (i < size - 1) {
          aggregated = addIncoming(list.value(i), state.query, aggregated)
          i += 1
        }
        tailProjector(ctx, state, aggregated.addRelationship(list.value(i)).addNode(node.apply(ctx, state)))
      case NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }
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


  case class multiOutgoingRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue =>
        var aggregated = builder
        val size = list.size()
        var i = 0
        while (i < size - 1) {
          aggregated = addOutgoing(list.value(i), state.query, aggregated)
          i += 1
        }
        tailProjector(ctx, state, aggregated.addRelationship(list.value(i)).addNode(node.apply(ctx, state)))
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

  case class multiUndirectedRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue =>
        val previous = builder.previousNode.id()
        val first = list.head().asInstanceOf[RelationshipValue]
        val correctDirection = first.startNode().id() == previous || first.endNode().id() == previous
        var aggregated = builder
        val size = list.size()
        if (correctDirection) {
          var i = 0
          while (i < size - 1) {
            aggregated = addUndirected(list.value(i), state.query, aggregated)
            i += 1
          }
          aggregated.addRelationship(list.value(i))
        } else {
          val reversed = list.reverse()
          var i = 0
          while (i < size - 1) {
            aggregated = addUndirected(reversed.value(i), state.query, aggregated)
            i += 1
          }
          aggregated.addRelationship(reversed.value(i))
        }
        tailProjector(ctx, state, aggregated.addNode(node.apply(ctx, state)))

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
      builder.addRelationship(r).addNode(CypherFunctions.startNode(r, query))

    case NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addOutgoing(relValue: AnyValue, query: QueryContext, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      builder.addRelationship(r).addNode(CypherFunctions.endNode(r, query))

    case NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addUndirected(relValue: AnyValue, query: QueryContext, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      val previous = builder.previousNode
      builder.addRelationship(r).addNode(CypherFunctions.otherNode(r, query, previous))

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

