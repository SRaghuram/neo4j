/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, PathValueBuilder}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

object SlottedProjectedPath {

  type Projector = (ExecutionContext, QueryState, PathValueBuilder) => PathValueBuilder

  object nilProjector extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = builder
  }

  case class singleNodeProjector(node: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = {
      val nodeValue = node.apply(ctx, state)
      tailProjector(ctx, state, builder.addNode(nodeValue))
    }
  }

  case class singleIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = {
      val relValue = rel.apply(ctx, state)
      tailProjector(ctx, state, builder.addIncomingRelationship(relValue))
    }
  }

  case class singleOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = {
      val relValue = rel.apply(ctx, state)
      tailProjector(ctx, state, builder.addOutgoingRelationship(relValue))
    }
  }

  case class singleUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = {
      val relValue = rel.apply(ctx, state)
      tailProjector(ctx, state, builder.addUndirectedRelationship(relValue))
    }
  }

  case class multiIncomingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = {
      val relListValue = rel.apply(ctx, state)
      tailProjector(ctx, state, builder.addIncomingRelationships(relListValue))
    }
  }

  case class multiOutgoingRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = {
      val relListValue = rel.apply(ctx, state)
      tailProjector(ctx, state, builder.addOutgoingRelationships(relListValue))
    }
  }

  case class multiUndirectedRelationshipProjector(rel: Expression, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, state: QueryState, builder: PathValueBuilder) = {
      val relListValue = rel.apply(ctx, state)
      tailProjector(ctx, state, builder.addUndirectedRelationships(relListValue))
    }
  }
}

/*
 Expressions for materializing new paths (used by ronja)

 These expressions cannot be generated by the user directly
 */
case class SlottedProjectedPath(symbolTableDependencies: Set[String], projector: SlottedProjectedPath.Projector) extends Expression {
  def apply(ctx: ExecutionContext, state: QueryState) = projector(ctx, state, state.clearPathValueBuilder).result()

  def arguments = Seq.empty

  def rewrite(f: (Expression) => Expression): Expression = f(this)
}

