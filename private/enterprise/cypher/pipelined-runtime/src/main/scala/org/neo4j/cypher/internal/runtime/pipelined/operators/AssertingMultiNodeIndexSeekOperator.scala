/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.pipelined.NodeIndexSeekParameters
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.exceptions.MergeConstraintConflictException
import org.neo4j.internal.kernel.api.PropertyIndexQuery

class AssertingMultiNodeIndexSeekOperator(val workIdentity: WorkIdentity,
                                          argumentSize: SlotConfiguration.Size,
                                          nodeIndexSeekParameters: Seq[NodeIndexSeekParameters])
  extends StreamingOperator {


  private val indexPropertyIndices: Array[Int] = nodeIndexSeekParameters.head.slottedIndexProperties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  private val indexPropertySlotOffsets: Array[Int] = nodeIndexSeekParameters.head.slottedIndexProperties.flatMap(_.maybeCachedNodePropertySlot)
  private val propertyIds = nodeIndexSeekParameters.head.slottedIndexProperties.map(_.propertyKeyId)
  private val indexSeekers: Array[IndexSeeker] = nodeIndexSeekParameters.map(new IndexSeeker(_)).toArray
  private val numberOfSeeks: Int = nodeIndexSeekParameters.length

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(new AssertingMultiNodeIndexSeekTask(inputMorsel.nextCopy))
  }

  class AssertingMultiNodeIndexSeekTask(inputMorsel: Morsel) extends
                                                             NodeIndexSeekTask(
                                                               inputMorsel,
                                                               workIdentity,
                                                               nodeIndexSeekParameters.head.nodeSlotOffset,
                                                               indexPropertyIndices,
                                                               indexPropertySlotOffsets,
                                                               nodeIndexSeekParameters.head.queryIndex,
                                                               nodeIndexSeekParameters.head.kernelIndexOrder,
                                                               argumentSize, propertyIds,
                                                               nodeIndexSeekParameters.head.valueExpression,
                                                               nodeIndexSeekParameters.head.indexSeekMode) {
    private var nodeId = -1L
    private var emptyRhs = true
    private var emptyLhs = true

    override protected def initializeInnerLoop(state: PipelinedQueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ReadWriteRow): Boolean = {
      val read = state.queryContext.transactionalContext.transaction.dataRead()
      val queryState = state.queryStateForExpressionEvaluation(resources)

      /*
       * We check so that all but the leftmost index seek returns consistent results, i.e. they all return the same node.
       * If everything checks out we remember the id of the node and in `innerLoop` we also check so that we obtain consistent
       * result from the leftmost index seek.
       */
      var i = 1
      while (i < numberOfSeeks) {
        val currentSeekParameters = nodeIndexSeekParameters(i)
        val seeker = indexSeekers(i)
        val indexQueries: Seq[Seq[PropertyIndexQuery]] = seeker.computeIndexQueries(queryState, initExecutionContext)
        val (nodeCursor, cursorsToClose) =
          computeCursor(indexQueries, read, state, resources, currentSeekParameters.queryIndex, currentSeekParameters.kernelIndexOrder, needsValues = false)
        try {
          var hasNext = nodeCursor.next()
          if (i == 1) {
            emptyRhs = !hasNext
            if (hasNext) {
              nodeId = nodeCursor.nodeReference()
              hasNext = nodeCursor.next()
            }
          } else if (hasNext == emptyRhs) {
            throw new MergeConstraintConflictException(s"Merge did not find a matching node $nodeId and can not create a new node due to conflicts with existing unique nodes")
          }
          //check that we get the same node back every time
          while (hasNext) {
            if (nodeId != nodeCursor.nodeReference()) {
              throw new MergeConstraintConflictException(s"Merge did not find a matching node $nodeId and can not create a new node due to conflicts with existing unique nodes")
            }
            hasNext = nodeCursor.next()
          }
        } finally {
          cursorsToClose.foreach(resources.cursorPools.nodeValueIndexCursorPool.free)
        }
        i += 1
      }

      //delegate to normal initialization
      super.initializeInnerLoop(state, resources, initExecutionContext)
    }

    override protected def onNode(currentNode: Long): Unit = {
      //we have at least seen one node
      emptyLhs = false
      //assert that all nodes are the same
      if (currentNode != nodeId) {
        throw new MergeConstraintConflictException( s"Merge did not find a matching node $nodeId and can not create a new node due to conflicts with existing unique nodes")
      }
    }

    override protected def onExitInnerLoop(): Unit = {
      //either all seeks return empty or none of them return empty
      if (emptyLhs != emptyRhs) {
        throw new MergeConstraintConflictException( s"Merge did not find a matching node $nodeId and can not create a new node due to conflicts with existing unique nodes")
      }
    }
  }
}

