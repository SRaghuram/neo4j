/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.profiling.QueryProfiler
import org.neo4j.cypher.internal.profiling.QueryProfiler.NONE
import org.neo4j.cypher.internal.runtime.ClosingIterator
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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.profiler.InterpretedProfileInformation
import org.neo4j.cypher.internal.runtime.interpreted.profiler.Profiler
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.SlottedPipeOperator.createFeedPipeQueryState
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentityMutableDescription
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

import scala.collection.mutable

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
  def setPipe(newPipe: Pipe): Unit = {
    _pipe = newPipe
    _pipeIDs += newPipe.id
  }

  private val _pipeIDs = mutable.ArrayBuffer(initialPipe.id)
  def pipeIDs: Seq[Id] = _pipeIDs
}

abstract class OperatorWithInterpretedDBHitsProfiling(pipeIDs: Seq[Id], feedPipeQueryState: FeedPipeQueryState) extends OperatorTask {
  // Overridden to not set the tracer on the operatorExecutionEvent. Otherwise we would count dbHits twice,
  // once from the morsel Tracer and once from the interpreted Profiler.
  override final def operateWithProfile(output: Morsel,
                                        state: PipelinedQueryState,
                                        resources: QueryResources,
                                        queryProfiler: QueryProfiler): Unit = {
    // We don't track time for slotted fallback operators, since we cannot attribute time spent to individual pipes in the chain.
    val operatorExecutionEvents = if (queryProfiler != NONE) pipeIDs.map(queryProfiler.executeOperator(_, false)) else null
    try {
      val (currentDbHits, currentRows) = currentProfileInformation
      operate(output, state, resources)
      profileSlottedPipes(operatorExecutionEvents, currentDbHits, currentRows)
    } finally {
      if (operatorExecutionEvents != null) {
        operatorExecutionEvents.foreach(_.close())
      }
    }
  }

  override final def setExecutionEvent(event: OperatorProfileEvent): Unit = {}

  private def currentProfileInformation: (collection.Map[Id, Long], collection.Map[Id, Long]) = {
    if (feedPipeQueryState.profileInformation != null) {
      val currentDbHitsMap: collection.Map[Id, Long] = feedPipeQueryState.profileInformation.dbHitsMap.map { case (k, v) => (k, v.count) }.withDefaultValue(0)
      val currentRowsMap: collection.Map[Id, Long] = feedPipeQueryState.profileInformation.rowMap.map { case (k, v) => (k, v.count) }.withDefaultValue(0)
      (currentDbHitsMap, currentRowsMap)
    } else (null, null)
  }

  private def profileSlottedPipes(operatorExecutionEvents: Seq[OperatorProfileEvent], previousDbHits: collection.Map[Id, Long], previousRows: collection.Map[Id, Long]): Unit = {
    if (feedPipeQueryState.profileInformation != null) {
      var i = 0
      while (i < pipeIDs.length) {
        val dbHits = feedPipeQueryState.profileInformation.dbHitsMap(pipeIDs(i)).count - previousDbHits(pipeIDs(i))
        val rows = feedPipeQueryState.profileInformation.rowMap(pipeIDs(i)).count - previousRows(pipeIDs(i))
        // TODO what about page cache stats?
        operatorExecutionEvents(i).dbHits(dbHits)
        operatorExecutionEvents(i).rows(rows)
        i += 1
      }
    }
  }
}

class SlottedPipeHeadOperator(workIdentity: WorkIdentityMutableDescription,
                              initialPipe: Pipe) extends SlottedPipeOperator(workIdentity, initialPipe) with Operator {

  override def toString: String = workIdentity.toString

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    new MorselInputOperatorState {
      override protected def nextTasks(state: PipelinedQueryState,
                                       input: MorselParallelizer,
                                       parallelism: Int,
                                       resources: QueryResources,
                                       argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
        val inputMorsel = input.nextCopy
        val inputQueryState = createFeedPipeQueryState(inputMorsel, state, resources, stateFactory.memoryTracker)
        singletonIndexedSeq(new OTask(inputMorsel, inputQueryState))
      }
    }
  }

  class OTask(val inputMorsel: Morsel, val feedPipeQueryState: FeedPipeQueryState) extends OperatorWithInterpretedDBHitsProfiling(pipeIDs, feedPipeQueryState) with ContinuableOperatorTaskWithMorsel {

    private var resultIterator: Iterator[CypherRow] = _

    override def workIdentity: WorkIdentity = SlottedPipeHeadOperator.this.workIdentity

    override def toString: String = s"SlottedPipeTask($pipe)"

    override def operate(outputMorsel: Morsel,
                         state: PipelinedQueryState,
                         resources: QueryResources): Unit = {

      if (resultIterator == null) {
        resultIterator = pipe.createResults(feedPipeQueryState)
      }

      val outputCursor = outputMorsel.writeCursor()
      while (outputCursor.next() && resultIterator.hasNext) {
        val resultRow = resultIterator.next()
        outputCursor.copyFromSlottedRowOrCursor(resultRow)
      }
      outputCursor.truncate()
    }

    override def canContinue: Boolean = resultIterator.hasNext

    override protected def closeCursors(resources: QueryResources): Unit = {}
  }
}

class SlottedPipeMiddleOperator(workIdentity: WorkIdentityMutableDescription,
                                val initialPipe: Pipe) extends SlottedPipeOperator(workIdentity, initialPipe) with MiddleOperator {

  override def toString: String = workIdentity.toString

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          state: PipelinedQueryState,
                          resources: QueryResources): OperatorTask = {
    val inputQueryState = createFeedPipeQueryState(null, state, resources, stateFactory.memoryTracker)
    new OMiddleTask(inputQueryState)
  }

  class OMiddleTask(val feedPipeQueryState: FeedPipeQueryState) extends OperatorWithInterpretedDBHitsProfiling(pipeIDs, feedPipeQueryState) with OperatorTask {

    private var resultIterator: Iterator[CypherRow] = _
    private var _canContinue: Boolean = false

    override def workIdentity: WorkIdentity = SlottedPipeMiddleOperator.this.workIdentity

    override def toString: String = s"SlottedPipeMiddleTask($pipe)"

    override def operate(outputMorsel: Morsel,
                         state: PipelinedQueryState,
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
        outputCursor.copyFromSlottedRowOrCursor(resultRow)
        outputCursor.next()
        _canContinue = resultIterator.hasNext
      }
      if (!_canContinue) {
        resultIterator = null
      }
      outputCursor.truncate()
    }
  }
}

object SlottedPipeOperator {
  def createFeedPipeQueryState(inputMorsel: Morsel,
                               morselQueryState: PipelinedQueryState,
                               resources: QueryResources,
                               memoryTracker: QueryMemoryTracker): FeedPipeQueryState = {

    val (pipeDecorator, profileInformation) =
      if (morselQueryState.doProfile) {
        val profileInformation = new InterpretedProfileInformation
        (new Profiler(morselQueryState.queryContext.transactionalContext.dbmsInfo, profileInformation), profileInformation)
      } else {
        (NullPipeDecorator, null)
      }

    val externalResource: ExternalCSVResource = new CSVResources(morselQueryState.queryContext.resources)
    new FeedPipeQueryState(morselQueryState.queryContext,
      externalResource,
      morselQueryState.params,
      resources.expressionCursors,
      morselQueryState.queryIndexes,
      resources.expressionVariables(morselQueryState.nExpressionSlots),
      morselQueryState.subscriber,
      memoryTracker,
      pipeDecorator,
      lenientCreateRelationship = morselQueryState.lenientCreateRelationship,
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
  extends QueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, initialContext,
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

  override protected def internalCreateResults(state: pipes.QueryState): ClosingIterator[CypherRow] = {
    val feedPipeQueryState = state.asInstanceOf[FeedPipeQueryState]
    val morselCursor = feedPipeQueryState.morselCursor

    morselCursor.setToStart()

    new ClosingIterator[CypherRow] {

      override protected[this] def closeMore(): Unit = ()

      override def innerHasNext: Boolean = {
        morselCursor.hasNext
      }

      override def next(): CypherRow = {
        morselCursor.next()
        morselCursor
      }
    }
  }
}
