/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.exceptions.InternalException
import org.neo4j.values.virtual.NodeReference
import org.neo4j.values.virtual.NodeValue

class SetNodePropertyOperator(val workIdentity: WorkIdentity,
                              idName: String,
                              propertyKey: String,
                              propertyValue: commands.expressions.Expression) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {

    val queryState = state.queryStateForExpressionEvaluation(resources)
    val write = state.query.transactionalContext.dataWrite
    val tokenWrite = state.query.transactionalContext.transaction.tokenWrite()

    val cursor: MorselFullCursor = morsel.fullCursor()
    while (cursor.next()) {
      val nodeOption = cursor.getByName(idName) match {
        case n: NodeValue => Some(n.id())
        case l: NodeReference => Some(l.id())
        case IsNoValue() => None
        case x => throw new InternalException(s"Expected to find a node at '$idName' but found instead: $x")
      }

      nodeOption.map(node => SetPropertyOperator.setNodeProperty(
        node,
        propertyKey,
        propertyValue.apply(cursor, queryState),
        tokenWrite,
        write,
        resources.queryStatisticsTracker
      ))
    }
  }

  private def getNode(row: MorselFullCursor, name: String): Long =
    row.getByName(name) match {
      case n: NodeValue => n.id()
      case l: NodeReference => l.id()
      case x => throw new InternalException(s"Expected to find a node at '$name' but found instead: $x")
    }
}
