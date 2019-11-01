/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import java.util

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

/**
  * Slotted variant of [[org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NestedPipeExpression]]
  */
case class NestedPipeSlottedExpression(pipe: Pipe,
                                       inner: Expression,
                                       slots: SlotConfiguration) extends Expression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val initialContext: SlottedExecutionContext = createInitialContext(ctx, state)

    val innerState =
      if (owningPipe.isDefined) {
        state.withInitialContext(initialContext).withDecorator(state.decorator.innerDecorator(owningPipe.get))
      } else {
        // We will get inaccurate profiling information of any db hits incurred by this nested expression
        // but at least we will be able to execute the query
        state.withInitialContext(initialContext)
      }


    val results = pipe.createResults(innerState)
    val all = new util.ArrayList[AnyValue]()
    while (results.hasNext) {
      all.add(inner(results.next(), state))
    }
    VirtualValues.fromList(all)
  }

  private def createInitialContext(ctx: ExecutionContext, state: QueryState): SlottedExecutionContext = {
    val initialContext = new SlottedExecutionContext(slots)
    initialContext.copyFrom(ctx, slots.numberOfLongs, slots.numberOfReferences)
    initialContext
  }

  override def rewrite(f: Expression => Expression): Expression = f(NestedPipeSlottedExpression(pipe, inner.rewrite(f), slots))

  override def arguments = List(inner)

  override def toString: String = s"NestedExpression()"

  override def children: Seq[AstNode[_]] = Seq(inner)

  override def symbolTableDependencies: Set[String] = Set()
}
