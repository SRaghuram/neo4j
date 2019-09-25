/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{InCheckContainer, SingleThreadedLRUCache}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, ExpressionCursors, InputDataStream, NoInput, QueryContext, QueryMemoryTracker}
import org.neo4j.cypher.internal.runtime.interpreted.{CSVResources, pipes}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ExternalCSVResource, NullPipeDecorator, Pipe, PipeDecorator}
import org.neo4j.cypher.internal.runtime.interpreted.profiler.{InterpretedProfileInformation, Profiler}
import org.neo4j.cypher.internal.runtime.morsel.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.SlottedPipeOperator.{createFeedPipeQueryState, updateProfileEvent}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
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

abstract class SlottedPipeOperator(initialPipe: Pipe) {
  private var _pipe: Pipe = initialPipe
  def pipe: Pipe = _pipe
  def setPipe(newPipe: Pipe): Unit = _pipe = newPipe
}

class SlottedPipeHeadOperator(val workIdentity: WorkIdentity,
                              initialPipe: Pipe) extends SlottedPipeOperator(initialPipe) with Operator {

  override def toString: String = "SlottedPipe"

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

  class OTask(val inputMorsel: MorselExecutionContext, val feedPipeQueryState: FeedPipeQueryState) extends ContinuableOperatorTaskWithMorsel {

    private var resultIterator: Iterator[ExecutionContext] = _
    private var profileEvent: OperatorProfileEvent = _

    override def workIdentity: WorkIdentity = SlottedPipeHeadOperator.this.workIdentity

    override def toString: String = s"SlottedPipeTask($pipe)"

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      if (resultIterator == null) {
        resultIterator = pipe.createResults(feedPipeQueryState)
      }

      while (outputRow.isValidRow && resultIterator.hasNext) {
        val resultRow = resultIterator.next()
        outputRow.copyFrom(resultRow, outputRow.getLongsPerRow, outputRow.getRefsPerRow)
        outputRow.moveToNextRow()
      }
      if (profileEvent != null && feedPipeQueryState.profileInformation != null) {
        updateProfileEvent(profileEvent, feedPipeQueryState.profileInformation)
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = resultIterator.hasNext

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      profileEvent = event
    }

    override protected def closeCursors(resources: QueryResources): Unit = {}
  }
}

class SlottedPipeMiddleOperator(val workIdentity: WorkIdentity,
                                val initialPipe: Pipe) extends SlottedPipeOperator(initialPipe) with MiddleOperator {

  override def toString: String = "SlottedPipeMiddle"

  override def createTask(argumentStateCreator: ArgumentStateMapCreator,
                          stateFactory: StateFactory,
                          queryContext: QueryContext,
                          state: QueryState,
                          resources: QueryResources): OperatorTask = {
    val inputQueryState = createFeedPipeQueryState(null, queryContext, state, resources, stateFactory.memoryTracker)
    new OMiddleTask(inputQueryState)
  }

  class OMiddleTask(val feedPipeQueryState: FeedPipeQueryState) extends OperatorTask {

    private var resultIterator: Iterator[ExecutionContext] = _
    private var profileEvent: OperatorProfileEvent = _

    override def workIdentity: WorkIdentity = SlottedPipeMiddleOperator.this.workIdentity

    override def toString: String = s"SlottedPipeMiddleTask($pipe)"

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      // Set the output row in the state, so that the feed pipe can access it to iterate over
      feedPipeQueryState.morsel = outputRow

      if (resultIterator == null) {
        resultIterator = pipe.createResults(feedPipeQueryState)
      }

      // NOTE: resultIterator.hasNext will check the validity of outputRow so we do not need to also do that explicitly in this loop condition
      while (resultIterator.hasNext) {
        val resultRow = resultIterator.next()
        if (!(resultRow eq outputRow)) {
          outputRow.copyFrom(resultRow, outputRow.getLongsPerRow, outputRow.getRefsPerRow)
        }
        outputRow.moveToNextRow()
      }
      if (profileEvent != null && feedPipeQueryState.profileInformation != null) {
        updateProfileEvent(profileEvent, feedPipeQueryState.profileInformation)
      }
      outputRow.finishedWriting()
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      profileEvent = event
    }
  }
}

object SlottedPipeOperator {
  def createFeedPipeQueryState(inputMorsel: MorselExecutionContext,
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
                           morsel = inputMorsel)
  }

  def updateProfileEvent(profileEvent: OperatorProfileEvent, profileInformation: InterpretedProfileInformation): Unit = {
    val dbHits = profileInformation.dbHitsMap.values.map(_.count).sum
    profileEvent.dbHits(dbHits.toInt)
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
                         initialContext: Option[ExecutionContext] = None,
                         cachedIn: SingleThreadedLRUCache[Any, InCheckContainer] = new SingleThreadedLRUCache(maxSize = 16),
                         lenientCreateRelationship: Boolean = false,
                         prePopulateResults: Boolean = false,
                         input: InputDataStream = NoInput,
                         val profileInformation: InterpretedProfileInformation = null,
                         var morsel: MorselExecutionContext = null)
  extends SlottedQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, initialContext,
                            cachedIn, lenientCreateRelationship, prePopulateResults, input) {

  override def withDecorator(decorator: PipeDecorator): FeedPipeQueryState = {
    new FeedPipeQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
                           initialContext, cachedIn, lenientCreateRelationship, prePopulateResults, input, profileInformation, morsel)
  }

  override def withInitialContext(initialContext: ExecutionContext): FeedPipeQueryState = {
    new FeedPipeQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, Some(initialContext),
                           cachedIn, lenientCreateRelationship, prePopulateResults, input, profileInformation, morsel)
  }


  override def withQueryContext(query: QueryContext): FeedPipeQueryState = {
    new FeedPipeQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator,
                           initialContext, cachedIn, lenientCreateRelationship, prePopulateResults, input, profileInformation, morsel)
  }
}

case class MorselFeedPipe()(val id: Id = Id.INVALID_ID) extends Pipe {

  override protected def internalCreateResults(state: pipes.QueryState): Iterator[ExecutionContext] = {
    val feedPipeQueryState = state.asInstanceOf[FeedPipeQueryState]
    val morsel = feedPipeQueryState.morsel

    morsel.resetToBeforeFirstRow()

    new Iterator[ExecutionContext] {

      override def hasNext: Boolean = {
        morsel.hasNextRow
      }

      override def next(): ExecutionContext = {
        morsel.moveToNextRow()
        morsel
      }
    }
  }
}
