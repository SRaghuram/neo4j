/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physical_planning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.{ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.CypherTypeException
import org.neo4j.internal.kernel.api._
import org.neo4j.values.storable.{TextValue, Values}

class NodeIndexContainsScanOperator(val workIdentity: WorkIdentity,
                                    nodeOffset: Int,
                                    label: Int,
                                    property: SlottedIndexedProperty,
                                    queryIndexId: Int,
                                    indexOrder: IndexOrder,
                                    valueExpr: Expression,
                                    argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, property.maybeCachedNodePropertySlot) {

  override def init(context: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselExecutionContext,
                    resources: QueryResources): IndexedSeq[ContinuableOperatorTask] = {
    val index = context.transactionalContext.schemaRead.index(label, property.propertyKeyId)
    val indexSession = context.transactionalContext.dataRead.indexReadSession(index)
    IndexedSeq(new OTask(inputMorsel, indexSession))
  }

  class OTask(val inputRow: MorselExecutionContext, index: IndexReadSession) extends StreamingContinuableOperatorTask {

    private var cursor: NodeValueIndexCursor = _

    override protected def initializeInnerLoop(context: QueryContext, state: QueryState,
                                               resources: QueryResources): Boolean = {
      cursor = resources.cursorPools.nodeValueIndexCursorPool.allocate()

      val read = context.transactionalContext.dataRead

      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession])
      val value = valueExpr(inputRow, queryState)

      value match {
        case value: TextValue =>
          val indexQuery = IndexQuery.stringContains(property.propertyKeyId, value)
          read.nodeIndexSeek(index, cursor, indexOrder, property.maybeCachedNodePropertySlot.isDefined, indexQuery)
          true

        case Values.NO_VALUE =>
          false // CONTAINS null does not produce any rows

        case x => throw new CypherTypeException(s"Expected a string value, but got $x")
      }
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputRow, outputRow, cursor, argumentSize)
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeValueIndexCursorPool.free(cursor)
      cursor = null
    }
  }
}
