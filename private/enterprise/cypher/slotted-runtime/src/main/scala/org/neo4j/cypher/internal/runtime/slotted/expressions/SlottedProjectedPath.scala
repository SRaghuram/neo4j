/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, PathValueBuilder}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.cypher.operations.CypherFunctions.{endNode, startNode}
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
      tailProjector(ctx, state, addIncoming(rel.apply(ctx,state), state, builder))
  }

  case class singleOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addOutgoing(rel.apply(ctx,state), state, builder))
  }

  case class singleUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder =
      tailProjector(ctx, state, addUndirected(rel.apply(ctx,state), state, builder))
  }

  case class multiIncomingRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue if list.nonEmpty() =>
        var aggregated = builder
        val size = list.size()
        var i = 0
        while (i < size - 1) {
          //we know these relationships have already loaded start and end relationship
          //so we should not use CypherFunctions::[start,end]Node to look them up
          aggregated = builder.addIncomingRelationship(list.value(i))
          i += 1
        }
        tailProjector(ctx, state, aggregated.addRelationship(list.value(i)).addNode(node.apply(ctx, state)))

      case _: ListValue => tailProjector(ctx, state, builder)
      case NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }
  }

  case class multiOutgoingRelationshipWithKnownTargetProjector(rel: Expression, node: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = rel.apply(ctx, state) match {
      case list: ListValue if list.nonEmpty() =>
        var aggregated = builder
        val size = list.size()
        var i = 0
        while (i < size - 1) {
          //we know these relationships have already loaded start and end relationship
          //so we should not use CypherFunctions::[start,end]Node to look them up
          aggregated = builder.addOutgoingRelationship(list.value(i))
          i += 1
        }
        tailProjector(ctx, state, aggregated.addRelationship(list.value(i)).addNode(node.apply(ctx, state)))

      case _: ListValue => tailProjector(ctx, state, builder)
      case NO_VALUE =>   tailProjector(ctx, state, builder.addNoValue())
      case value => throw new CypherTypeException(s"Expected ListValue but got ${value.getTypeName}")
    }
  }

  case class multiIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      //we know these relationships have already loaded start and end relationship
      //so we should not use CypherFunctions::[start,end]Node to look them up
      tailProjector(ctx, state, builder.addIncomingRelationships(relListValue))
    }
  }

  case class multiOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      //we know these relationships have already loaded start and end relationship
      //so we should not use CypherFunctions::[start,end]Node to look them up
      tailProjector(ctx, state, builder.addOutgoingRelationships(relListValue))
    }
  }

  case class multiUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder): PathValueBuilder = {
      val relListValue = rel.apply(ctx, state)
      //we know these relationships have already loaded start and end relationship
      //so we should not use CypherFunctions::[start,end]Node to look them up
      tailProjector(ctx, state, builder.addUndirectedRelationships(relListValue))
    }
  }

  private def addIncoming(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      builder.addRelationship(r).addNode(startNode(r, state.query, state.cursors.relationshipScanCursor))

    case NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addOutgoing(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      builder.addRelationship(r).addNode(endNode(r, state.query, state.cursors.relationshipScanCursor))

    case NO_VALUE => builder.addNoValue()
    case _ => throw new CypherTypeException(s"Expected RelationshipValue but got ${relValue.getTypeName}")
  }

  private def addUndirected(relValue: AnyValue, state: QueryState, builder: PathValueBuilder) = relValue match {
    case r: RelationshipValue =>
      val previous = builder.previousNode
      builder.addRelationship(r).addNode(CypherFunctions.otherNode(r, state.query, previous, state.cursors.relationshipScanCursor))

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

