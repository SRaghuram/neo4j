/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.v4_0.util.CypherTypeException
import org.neo4j.internal.kernel.api._
import org.neo4j.values.storable.{TextValue, Values}

class NodeIndexContainsScanOperator(val workIdentity: WorkIdentity,
                                    nodeOffset: Int,
                                    property: SlottedIndexedProperty,
                                    queryIndexId: Int,
                                    indexOrder: IndexOrder,
                                    valueExpr: Expression,
                                    argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, Array(property)) {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    val indexSession = state.queryIndexes(queryIndexId)
    IndexedSeq(new OTask(inputMorsel.nextCopy, indexSession))
  }

  class OTask(val inputMorsel: MorselExecutionContext, index: IndexReadSession) extends InputLoopTask {

    override def workIdentity: WorkIdentity = NodeIndexContainsScanOperator.this.workIdentity

    private var cursor: NodeValueIndexCursor = _

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources): Boolean = {

      val read = context.transactionalContext.dataRead
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots))
      val value = valueExpr(inputMorsel, queryState)

      value match {
        case value: TextValue =>
          val indexQuery = IndexQuery.stringContains(property.propertyKeyId, value)
          cursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()
          read.nodeIndexSeek(index, cursor, indexOrder, property.maybeCachedNodePropertySlot.isDefined, indexQuery)
          true

        case Values.NO_VALUE =>
          false // CONTAINS null does not produce any rows

        case x => throw new CypherTypeException(s"Expected a string value, but got $x")
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputMorsel, outputRow, cursor, argumentSize)
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeValueIndexCursorPool.free(cursor)
      cursor = null
    }
  }
}
