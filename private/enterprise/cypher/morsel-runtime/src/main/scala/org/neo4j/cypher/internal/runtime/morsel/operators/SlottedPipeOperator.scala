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
import org.neo4j.cypher.internal.runtime.morsel.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.operators.SlottedPipeOperator.createInputMorselPipeQueryState
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

class SlottedPipeOperator(val workIdentity: WorkIdentity,
                          val pipe: Pipe) extends Operator {

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
          val inputQueryState = createInputMorselPipeQueryState(inputMorsel, context, state, resources, stateFactory.memoryTracker)
          IndexedSeq(new OTask(inputMorsel, inputQueryState))
        } else {
          null
        }
      }
    }
  }

  class OTask(val inputMorsel: MorselExecutionContext, val inputMorselPipeQueryState: InputMorselPipeQueryState) extends ContinuableOperatorTaskWithMorsel {

    private var resultIterator: Iterator[ExecutionContext] = _

    override def workIdentity: WorkIdentity = SlottedPipeOperator.this.workIdentity

    override def toString: String = s"SlottedPipeTask($pipe)"

    override def operate(outputRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState,
                         resources: QueryResources): Unit = {

      // Set the output row in the state, so that the InputMorselPipe can access it to copy over data from the inputMorsel and pass it into the pipe
      inputMorselPipeQueryState.outputRow = outputRow

      if (resultIterator == null) {
        resultIterator = pipe.createResults(inputMorselPipeQueryState)
      }

      while (outputRow.isValidRow && resultIterator.hasNext) {
        val resultRow = resultIterator.next()
        if (!(resultRow eq outputRow)) {
          outputRow.copyFrom(resultRow, outputRow.getLongsPerRow, outputRow.getRefsPerRow)
        }
        //println(s"SlottedPipeOperator OUTPUT: $outputRow")
        outputRow.moveToNextRow()
      }
      outputRow.finishedWriting()
    }

    override def canContinue: Boolean = resultIterator.hasNext

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {}
    override protected def closeCursors(resources: QueryResources): Unit = {}
  }
}

object SlottedPipeOperator {
  private val pipeDecorator: PipeDecorator = NullPipeDecorator // TODO: Support monitoring etc.

  def createInputMorselPipeQueryState(inputMorsel: MorselExecutionContext,
                                      queryContext: QueryContext,
                                      morselQueryState: QueryState,
                                      resources: QueryResources,
                                      memoryTracker: QueryMemoryTracker): InputMorselPipeQueryState = {
    val externalResource: ExternalCSVResource = new CSVResources(queryContext.resources)
    new InputMorselPipeQueryState(queryContext,
                                  externalResource,
                                  morselQueryState.params,
                                  resources.expressionCursors,
                                  morselQueryState.queryIndexes,
                                  resources.expressionVariables(morselQueryState.nExpressionSlots),
                                  morselQueryState.subscriber,
                                  memoryTracker,
                                  pipeDecorator,
                                  inputMorsel = inputMorsel)
  }
}

class InputMorselPipeQueryState(query: QueryContext,
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
                                var inputMorsel: MorselExecutionContext = null,
                                var outputRow: MorselExecutionContext = null)
  extends SlottedQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, initialContext,
                            cachedIn, lenientCreateRelationship, prePopulateResults, input) {

  override def withInitialContext(initialContext: ExecutionContext): InputMorselPipeQueryState = {
    new InputMorselPipeQueryState(query, resources, params, cursors, queryIndexes, expressionVariables, subscriber, memoryTracker, decorator, Some(initialContext),
                                  cachedIn, lenientCreateRelationship, prePopulateResults, input, inputMorsel)
  }
}

case class InputMorselPipe()(val id: Id = Id.INVALID_ID) extends Pipe {

  override protected def internalCreateResults(state: pipes.QueryState): Iterator[ExecutionContext] = {
    val inputMorselPipeQueryState = state.asInstanceOf[InputMorselPipeQueryState]
    val inputMorsel = inputMorselPipeQueryState.inputMorsel
    val outputRow = inputMorselPipeQueryState.outputRow

    inputMorsel.resetToBeforeFirstRow()

    new Iterator[ExecutionContext] {

      override def hasNext: Boolean = {
        inputMorsel.hasNextRow
      }

      override def next(): ExecutionContext = {
        inputMorsel.moveToNextRow()
        //println(s"InputMorselPipe INPUT: $inputMorsel")
        //println(s"InputMorselPipe OUTPUT: $outputRow")
        inputMorsel.copyTo(outputRow)
        outputRow
      }
    }
  }
}

