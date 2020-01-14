/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.pipelined.NodeIndexSeekParameters
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.{SlottedQueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.{FloatingPointValue, Value, Values}

import scala.collection.mutable.ArrayBuffer

class MultiNodeIndexSeekOperator(val workIdentity: WorkIdentity,
                                 argumentSize: SlotConfiguration.Size,
                                 nodeIndexSeekParameters: Seq[NodeIndexSeekParameters])
  extends StreamingOperator {

  class IndexSeeker(parameters: NodeIndexSeekParameters) extends NodeIndexSeeker {
    override val indexMode: IndexSeekMode = parameters.indexSeekMode

    override val valueExpr: QueryExpression[Expression] = parameters.valueExpression

    override val propertyIds: Array[Int] = parameters.slottedIndexProperties.map(_.propertyKeyId)

    val indexPropertyIndices: Array[Int] = parameters.slottedIndexProperties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
    val indexPropertySlotOffsets: Array[Int] = parameters.slottedIndexProperties.flatMap(_.maybeCachedNodePropertySlot)
    val needsValues: Boolean = indexPropertyIndices.nonEmpty
  }

  private val indexSeekers: Array[IndexSeeker] = nodeIndexSeekParameters.map(new IndexSeeker(_)).toArray

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    IndexedSeq(new OTask(inputMorsel.nextCopy))
  }

  class OTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def workIdentity: WorkIdentity = MultiNodeIndexSeekOperator.this.workIdentity

    private val indexQueries: Array[Seq[Seq[IndexQuery]]] = new Array(indexSeekers.length)
    private val indexQueryIterators: Array[Iterator[Seq[IndexQuery]]] = new Array(indexSeekers.length)
    private val currentSeekI: Array[Int] = new Array(indexSeekers.length)
    private val seeks: Array[ArrayBuffer[() => Unit]] = new Array(indexSeekers.length)
    private val nodeCursors: Array[NodeValueIndexCursor] = new Array(indexSeekers.length)
    private val exactSeekValues: Array[Array[Value]] = new Array(indexSeekers.length)

    // INPUT LOOP TASK
    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val read = context.transactionalContext.transaction.dataRead()
      val queryState = new OldQueryState(context,
                                         resources = null,
                                         params = state.params,
                                         resources.expressionCursors,
                                         Array.empty[IndexReadSession],
                                         resources.expressionVariables(state.nExpressionSlots),
                                         state.subscriber,
                                         NoMemoryTracker)
      initExecutionContext.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
      var i = 0
      while (i < indexSeekers.length) {
        nodeCursors(i) = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
        val indexQueries = indexSeekers(i).computeIndexQueries(queryState, initExecutionContext)
        val indexQueryIterator = indexQueries.toIterator
        if (!indexQueryIterator.hasNext) {
          // If an index query does not have any predicates we can abort already
          assert(assertion = false, "An index query should always have at least one predicate")
          return false
        }
        seeks(i) = new ArrayBuffer[() => Unit]()
        do {
          val indexQuery = indexQueryIterator.next()
          val indexSeek: () => Unit = seek(state.queryIndexes(nodeIndexSeekParameters(i).queryIndex), nodeCursors(i), read, indexQuery,
                                           indexSeekers(i).needsValues, nodeIndexSeekParameters(i).kernelIndexOrder, i)
          seeks(i) += indexSeek
          currentSeekI(i) = 0
          seeks(i)(0)() // Perform the first seek
          // Advance the cursors one step, except for the innermost seek, which will be advanced on the first call to next() in the inner loop
          if (i < indexSeekers.length - 1 && !nodeCursors(i).next()) {
            // If a cursor does not have any results we can abort already
            return false
          }
        } while (indexQueryIterator.hasNext)
        i += 1
      }
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {

      while (outputRow.isValidRow && next(indexSeekers.length - 1)) {
        var j = 0
        while (j < indexSeekers.length) {
          val offset = nodeIndexSeekParameters(j).nodeSlotOffset
          val nodeCursor = nodeCursors(j)
          val indexPropertyIndices = indexSeekers(j).indexPropertyIndices
          val indexPropertySlotOffsets = indexSeekers(j).indexPropertySlotOffsets

          outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
          outputRow.setLongAt(offset, nodeCursor.nodeReference())
          var i = 0
          while (i < indexPropertyIndices.length) {
            val indexPropertyIndex = indexPropertyIndices(i)
            val value =
              if (exactSeekValues(j) != null) exactSeekValues(j)(indexPropertyIndex)
              else nodeCursor.propertyValue(indexPropertyIndex)

            outputRow.setCachedPropertyAt(indexPropertySlotOffsets(i), value)
            i += 1
          }
          j += 1
        }
        outputRow.moveToNextRow()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      var i = 0
      while (i < indexSeekers.length) {
        if (nodeCursors(i) != null) {
          nodeCursors(i).setTracer(event)
        }
        i += 1
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      var i = 0
      while (i < indexSeekers.length) {
        indexQueries(i) = null
        indexQueryIterators(i) = null
        seeks(i) = null
        resources.cursorPools.nodeValueIndexCursorPool.free(nodeCursors(i))
        nodeCursors(i) = null
        i += 1
      }
    }

    // HELPERS
    private def next(i: Int): Boolean = {
      assert (i >= 0 || i < indexSeekers.length)

      while (true) {
        if (nodeCursors(i) != null && nodeCursors(i).next()) {
          if (i >= indexSeekers.length - 1) {
            // We are positioned at a valid row
            return true
          } else {
            // Restart the next index query
            val nextI = i + 1
            seeks(nextI)(0)()
            currentSeekI(nextI) = 0
            return next(nextI)
          }
        } else if (currentSeekI(i) < seeks(i).length - 1) {
          currentSeekI(i) += 1
          seeks(i)(currentSeekI(i))()
        } else {
          if (i > 0)
            return next(i - 1)
          else
            return false
        }
      }
      throw new IllegalStateException("Unreachable code")
    }

    private def seek[RESULT <: AnyRef](index: IndexReadSession,
                                       nodeCursor: NodeValueIndexCursor,
                                       read: Read,
                                       predicates: Seq[IndexQuery],
                                       needsValues: Boolean,
                                       indexOrder: IndexOrder,
                                       exactSeekValueIndex: Int): () => Unit = {

      val impossiblePredicate =
        predicates.exists {
          case p: IndexQuery.ExactPredicate => (p.value() eq Values.NO_VALUE) || (p.value().isInstanceOf[FloatingPointValue] && p.value().asInstanceOf[FloatingPointValue].isNaN)
          case _: IndexQuery.ExistsPredicate if predicates.length > 1 => false
          case p: IndexQuery =>
            !RANGE_SEEKABLE_VALUE_GROUPS.contains(p.valueGroup())
        }

      if (impossiblePredicate) {
        return () => () // leave cursor un-initialized/empty
      }

      // We don't need property values from the index for an exact seek
      exactSeekValues(exactSeekValueIndex) =
        if (needsValues && predicates.forall(_.isInstanceOf[ExactPredicate]))
          predicates.map(_.asInstanceOf[ExactPredicate].value()).toArray
        else
          null

      val needsValuesFromIndexSeek = exactSeekValues(exactSeekValueIndex) == null && needsValues

      () => read.nodeIndexSeek(index, nodeCursor, indexOrder, needsValuesFromIndexSeek, predicates: _*)
    }
  }
}

