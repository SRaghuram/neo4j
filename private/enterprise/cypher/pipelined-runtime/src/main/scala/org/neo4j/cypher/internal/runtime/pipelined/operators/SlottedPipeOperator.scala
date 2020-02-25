/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.interpreted.CSVResources
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.InCheckContainer
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.SingleThreadedLRUCache
import org.neo4j.cypher.internal.runtime.interpreted.pipes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NullPipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeDecorator
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.runtime.interpreted.profiler.Profiler
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeOperator.createFeedPipeQueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeOperator.updateProfileEvent
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityMutableDescription
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

// We have two types of pipes to specialize for
// 1) Head operator pipes, that may be cardinality increasing, only reads from the input row (which is already expected to have different (subset) slot configuration,
//    and always creates its own output row copy (as a SlottedExecutionContext).
//    In this case we do not need to copy on the input side, but we always need to copy on the output side of the operator
// 2) Middle operator pipes writes to the given input row, which has to have the same slot configuration.
//    In this case we have to pass outputRow to the input side to get the correct slot configuration.
//    We may or may not need to copy on the output side of the operator, depending on if the pipe creates a copy of the row.

abstract class SlottedPipeOperator(val workIdentity: WorkIdentityMutableDescription, initialPipe: Pipe) {
  private var _pipe: Pipe = initialPipe
  def pipe: Pipe = _pipe
  def setPipe(newPipe: Pipe): Unit = _pipe = newPipe
}

trait OperatorWithInterpretedDBHitsProfiling extends OperatorTask {
  // Overridden to not set the tracer on the operatorExecutionEvent. Otherwise we would count dbHits twice,
  // once from the morsel Tracer and once from the interpreted Profiler.
  override def operateWithProfile(output: Morsel,
                                  context: QueryContext,
                                  state: QueryState,
                                  resources: QueryResources,
                                  queryProfiler: QueryProfiler): Unit = {
    val operatorExecutionEvent = queryProfiler.executeOperator(workIdentity.workId)
    setExecutionEvent(operatorExecutionEvent)
    try {
      operate(output, context, state, resources)
      if (operatorExecutionEvent != null) {
        operatorExecutionEvent.rows(output.numberOfRows)
      }
    } finally {
      setExecutionEvent(null)
      if (operatorExecutionEvent != null) {
        operatorExecutionEvent.close()
      }
    }
  }
}

class SlottedPipeHeadOperator(workIdentity: WorkIdentityMutableDescription,
                              initialPipe: Pipe) extends SlottedPipeOperator(workIdentity, initialPipe) with Operator {

  override def toString: String = workIdentity.toString

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           queryContext: QueryContext,
                           state: QueryState,
                           resources: QueryResources): OperatorState = {
    new OperatorState {
      override def nextTasks(context: QueryContext,
                             state: QueryState,
                             operatorInput: OperatorInput,
                             parallelism: Int,
                             resources: QueryResources,
                             argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTask] = {
        val input = operatorInput.takeMorsel()
        if (input != null) {
          val inputMorsel = input.nextCopy
          val inputQueryState = createFeedPipeQueryState(inputMorsel, context, state, resources, stateFactory.memoryTracker)
          IndexedSeq(new OTask(inputMorsel, inputQueryState))
        } else {
          null
        }
      }
    }
  }

  class OTask(val inputMorsel: Morsel, val feedPipeQueryState: FeedPipeQueryState) extends ContinuableOperatorTaskWithMorsel with OperatorWithInterpretedDBHitsProfiling {

    private var resultIterator: Iterator[CypherRow] = _
    private var profileEvent: OperatorProfileEvent = _

    override def workIdentity: WorkIdentity = SlottedPipeHeadOperator.this.workIdentity

    override def toString: String = s"SlottedPipeTask($pipe)"

    override def operate(outputMorsel: Morsel,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      if (resultIterator == null) {
        resultIterator = pipe.createResults(feedPipeQueryState)
      }

      val outputCursor = outputMorsel.writeCursor()
      while (outputCursor.next() && resultIterator.hasNext) {
        val resultRow = resultIterator.next()
        outputCursor.copyFrom(resultRow, outputMorsel.longsPerRow, outputMorsel.refsPerRow)
      }
      if (profileEvent != null && feedPipeQueryState.profileInformation != null) {
        updateProfileEvent(profileEvent, feedPipeQueryState.profileInformation)
      }
      outputCursor.truncate()
    }

    override def canContinue: Boolean = resultIterator.hasNext

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      profileEvent = event
    }

    override protected def closeCursors(resources: QueryResources): Unit = {}
  }
}

class SlottedPipeMiddleOperator(workIdentity: WorkIdentityMutableDescription,
                                val initialPipe: Pipe) extends SlottedPipeOperator(workIdentity, initialPipe) with MiddleOperator {

  override def toString: String = workIdentity.toString

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          queryContext: QueryContext,
                          state: QueryState,
                          resources: QueryResources): OperatorTask = {
    val inputQueryState = createFeedPipeQueryState(null, queryContext, state, resources, stateFactory.memoryTracker)
    new OMiddleTask(inputQueryState)
  }

  class OMiddleTask(val feedPipeQueryState: FeedPipeQueryState) extends OperatorTask with OperatorWithInterpretedDBHitsProfiling {

    private var resultIterator: Iterator[CypherRow] = _
    private var profileEvent: OperatorProfileEvent = _
    private var _canContinue: Boolean = false

    override def workIdentity: WorkIdentity = SlottedPipeMiddleOperator.this.workIdentity

    override def toString: String = s"SlottedPipeMiddleTask($pipe)"

    override def operate(outputMorsel: Morsel,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      // Set the output row in the state, so that the feed pipe can access it to iterate over
      // To be safe for all types of non-breaking plans we need to a fresh morsel cursor, since the pipe may be wrapping the iterator
      val feedRow = outputMorsel.fullCursor()
      feedPipeQueryState.morselCursor = feedRow

      if (resultIterator == null) {
        resultIterator = pipe.createResults(feedPipeQueryState)
      }

      // NOTE: resultIterator.hasNext will check the validity of outputRow so we do not need to also do that explicitly in this loop condition
      _canContinue = resultIterator.hasNext
      val outputCursor = outputMorsel.writeCursor(onFirstRow = true)
      while (_canContinue) {
        val resultRow = resultIterator.next() // This may advance feedRow
        if (!(resultRow eq feedRow)) {
          outputCursor.copyFrom(resultRow, outputMorsel.longsPerRow, outputMorsel.refsPerRow)
        }
        outputCursor.next()
        _canContinue = resultIterator.hasNext
      }
      if (profileEvent != null && feedPipeQueryState.profileInformation != null) {
        updateProfileEvent(profileEvent, feedPipeQueryState.profileInformation)
      }
      if (!_canContinue) {
        resultIterator = null
      }
      outputCursor.truncate()
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      profileEvent = event
    }
  }
}

object SlottedPipeOperator {
  def createFeedPipeQueryState(inputMorsel: Morsel,
                               queryContext: QueryContext,
                               morselQueryState: QueryState,
                               resources: QueryResources,
                               memoryTracker: QueryMemoryTracker): FeedPipeQueryState = {

    val (pipeDecorator, profileInformation) =
      if (morselQueryState.doProfile) {
        val profileInformation = new InterpretedProfileInformation
        (new Profiler(queryContext.transactionalContext.databaseInfo, profileInformation), profileInformation)
      } else {
        (NullPipeDecorator, null)
      }

    val externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    new FeedPipeQueryState(queryContext,
      externalResource,
      morselQueryState.params,
      resources.expressionCursors,
      morselQueryState.queryIndexes,
      resources.expressionVariables(morselQueryState.nExpressionSlots),
      morselQueryState.subscriber,
      memoryTracker,
      pipeDecorator,
      profileInformation = profileInformation,
      morselCursor = if (inputMorsel != null) inputMorsel.fullCursor() else null)
  }

  def updateProfileEvent(profileEvent: OperatorProfileEvent, profileInformation: InterpretedProfileInformation): Unit = {
    if (profileEvent != null) {
      val dbHits = profileInformation.dbHitsMap.values.map(_.count).sum
      profileEvent.dbHits(dbHits.toInt)
    }
  }
}

class FeedPipeQueryState(query: QueryContext,
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
                         input: InputDataStream = NoInput,
                         val profileInformation: InterpretedProfileInformation = null,
                         var morselCursor: MorselFullCursor = null)
  extends SlottedQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, initialContext,
    cachedIn, lenientCreateRelationship, prePopulateResults, input) {

  override def withDecorator(decorator: PipeDecorator): FeedPipeQueryState = {
    new FeedPipeQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
      initialContext, cachedIn, lenientCreateRelationship, prePopulateResults, input, profileInformation, morselCursor)
  }

  override def withInitialContext(initialContext: CypherRow): FeedPipeQueryState = {
    new FeedPipeQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, Some(initialContext),
      cachedIn, lenientCreateRelationship, prePopulateResults, input, profileInformation, morselCursor)
  }


  override def withQueryContext(query: QueryContext): FeedPipeQueryState = {
    new FeedPipeQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
      initialContext, cachedIn, lenientCreateRelationship, prePopulateResults, input, profileInformation, morselCursor)
  }
}

case class MorselFeedPipe()(val id: Id = Id.INVALID_ID) extends Pipe {

  override protected def internalCreateResults(state: pipes.QueryState): Iterator[CypherRow] = {
    val feedPipeQueryState = state.asInstanceOf[FeedPipeQueryState]
    val morselCursor = feedPipeQueryState.morselCursor

    morselCursor.setToStart()

    new Iterator[CypherRow] {

      override def hasNext: Boolean = {
        morselCursor.hasNext
      }

      override def next(): CypherRow = {
        morselCursor.next()
        morselCursor
      }
    }
  }
}
