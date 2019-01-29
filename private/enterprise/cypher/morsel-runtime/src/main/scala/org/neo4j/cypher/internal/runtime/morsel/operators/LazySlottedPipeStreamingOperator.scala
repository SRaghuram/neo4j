/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{InCheckContainer, SingleThreadedLRUCache}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => _, _}
import org.neo4j.cypher.internal.runtime.interpreted.{CSVResources, pipes}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedExecutionContext, SlottedQueryState}
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.{ExecutionContext, ExpressionCursors, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalException
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.virtual.MapValue

trait LazySlottedPipeStreamingOperator extends StreamingOperator with InitialComposableOperator[Pipe] {
  protected final var finalPipe: Pipe = _ // Write once, read many

  override def setFinalComposition(pipe: Pipe): Unit = {
    if (finalPipe != null)
      throw new InternalException("Can only set final pipe once")
    finalPipe = pipe
  }
}

class LazySlottedPipeComposableOperator(val workIdentity: WorkIdentity,
                                        val constructor: Pipe => Pipe) extends ComposableOperator[Pipe] {

  override def compose(source: Pipe): Pipe =
    constructor(source)
}

object LazySlottedPipeStreamingOperator {
  private val pipeDecorator: PipeDecorator = NullPipeDecorator // TODO: Support monitoring etc.

  def createSlottedQueryState(queryContext: QueryContext, morselQueryState: QueryState, cursors: ExpressionCursors): SlottedQueryState = {
    val externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    new SlottedQueryState(queryContext,
      externalResource,
      morselQueryState.params,
      cursors,
      morselQueryState.queryIndexes,
      pipeDecorator,
      lenientCreateRelationship = false)
  }

  def createFeedPipeQueryState(inputMorsel: MorselExecutionContext,
                               queryContext: QueryContext,
                               morselQueryState: QueryState,
                               cursors: ExpressionCursors): FeedPipeQueryState = {
    val externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    new FeedPipeQueryState(queryContext,
      externalResource,
      morselQueryState.params,
      cursors,
      morselQueryState.queryIndexes,
      pipeDecorator,
      lenientCreateRelationship = false,
      inputMorsel = inputMorsel)
  }
}

class FeedPipeQueryState(query: QueryContext,
                         resources: ExternalCSVResource,
                         params: MapValue,
                         cursors: ExpressionCursors,
                         queryIndexes: Array[IndexReadSession],
                         decorator: PipeDecorator = NullPipeDecorator,
                         initialContext: Option[ExecutionContext] = None,
                         cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] = new SingleThreadedLRUCache(maxSize = 16),
                         lenientCreateRelationship: Boolean = false,
                         var inputMorsel: MorselExecutionContext = null)
  extends SlottedQueryState(query, resources, params, cursors, queryIndexes, decorator, initialContext, cachedIn, lenientCreateRelationship) {

  var isNextRowReady: Boolean = false

  override def withInitialContext(initialContext: ExecutionContext): FeedPipeQueryState = {
    new FeedPipeQueryState(query, resources, params, cursors, queryIndexes, decorator, Some(initialContext), cachedIn, lenientCreateRelationship, inputMorsel)
  }
}

case class FeedPipe(slots: SlotConfiguration)(val id: Id = Id.INVALID_ID) extends Pipe {

  override protected def internalCreateResults(state: pipes.QueryState): Iterator[ExecutionContext] = {
    val feedPipeState = state.asInstanceOf[FeedPipeQueryState]

    new Iterator[ExecutionContext] {

      override def hasNext: Boolean = {
        feedPipeState.isNextRowReady // && sourceMorsel.hasMoreRows
      }

      override def next(): ExecutionContext = {
        feedPipeState.isNextRowReady = false
        // TODO: Replace all explicit SlottedExecutionContext casts in copy methods with something that works also with MorselExecutionContext and get rid of this copy
        // Copy the row to a slotted context for now
        val slottedContext = SlottedExecutionContext(slots)
        feedPipeState.inputMorsel.copyTo(slottedContext)
        slottedContext
      }
    }
  }
}
