/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.InCheckContainer
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.SingleThreadedLRUCache
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExecutionContextFactory
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NullPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

class SlottedQueryState(query: QueryContext,
                        resources: ExternalCSVResource,
                        params: Array[AnyValue],
                        cursors: ExpressionCursors,
                        queryIndexes: Array[IndexReadSession],
                        expressionVariables: Array[AnyValue],
                        subscriber: QuerySubscriber,
                        memoryTracker: QueryMemoryTracker,
                        decorator: PipeDecorator = NullPipeDecorator,
                        initialContext: Option[CypherRow] = None,
                        cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] = new SingleThreadedLRUCache(maxSize = 16),
                        lenientCreateRelationship: Boolean = false,
                        prePopulateResults: Boolean = false,
                        input: InputDataStream = NoInput)
  extends QueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
    initialContext, cachedIn, lenientCreateRelationship, prePopulateResults, input) {

  override def withDecorator(decorator: PipeDecorator) =
    new SlottedQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
      initialContext, cachedIn, lenientCreateRelationship, prePopulateResults, input)

  override def withInitialContext(initialContext: CypherRow) =
    new SlottedQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
      Some(initialContext), cachedIn, lenientCreateRelationship, prePopulateResults, input)

  override def withQueryContext(query: QueryContext) =
    new SlottedQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
      initialContext, cachedIn, lenientCreateRelationship, prePopulateResults, input)
}

case class SlottedExecutionContextFactory(slots: SlotConfiguration) extends ExecutionContextFactory {

  override def newExecutionContext(): CypherRow =
    SlottedRow(slots)

  override def copyWith(row: ReadableRow): CypherRow = {
    val newCtx = SlottedRow(slots)
    row.copyTo(newCtx)
    newCtx
  }

  override def copyWith(row: ReadableRow, newEntries: Seq[(String, AnyValue)]): CypherRow = {
    val newCopy = SlottedRow(slots)
    row.copyTo(newCopy)
    for ((key,value) <- newEntries) {
      newCopy.set(key, value)
    }
    newCopy
  }

  override def copyWith(row: ReadableRow, key: String, value: AnyValue): CypherRow = {
    val newCtx = SlottedRow(slots)
    row.copyTo(newCtx)
    newCtx.set(key, value)
    newCtx
  }

  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = {
    val newCopy = SlottedRow(slots)
    row.copyTo(newCopy)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy
  }

  override def copyWith(row: ReadableRow, key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): CypherRow = {
    val newCopy = SlottedRow(slots)
    row.copyTo(newCopy)
    newCopy.set(key1, value1)
    newCopy.set(key2, value2)
    newCopy.set(key3, value3)
    newCopy
  }
}
