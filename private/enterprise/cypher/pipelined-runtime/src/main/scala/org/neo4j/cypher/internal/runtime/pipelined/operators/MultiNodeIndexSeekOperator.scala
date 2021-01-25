/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.KernelAPISupport.RANGE_SEEKABLE_VALUE_GROUPS
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IndexSeekMode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.NodeIndexSeeker
import org.neo4j.cypher.internal.runtime.pipelined.NodeIndexSeekParameters
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate
import org.neo4j.internal.kernel.api.IndexQueryConstraints
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

import scala.collection.mutable.ArrayBuffer

class MultiNodeIndexSeekOperator(val workIdentity: WorkIdentity,
                                 argumentSize: SlotConfiguration.Size,
                                 nodeIndexSeekParameters: Seq[NodeIndexSeekParameters])
  extends StreamingOperator {

  private val indexSeekers: Array[IndexSeeker] = nodeIndexSeekParameters.map(new IndexSeeker(_)).toArray
  private val numberOfSeeks: Int = indexSeekers.length

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {

    singletonIndexedSeq(new OTask(inputMorsel.nextCopy))
  }

  class OTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def workIdentity: WorkIdentity = MultiNodeIndexSeekOperator.this.workIdentity

    // We have one node cursor for each index seek
    private var nodeCursors: Array[NodeValueIndexCursor] = _

    private var indexQueries: Seq[Seq[IndexQuery]] = _

    // For exact seeks we also cache the values that we extract from the predicates instead of reading the property values
    // from the index using the cursor (which by definition have to be the same)
    private var exactSeekValues: Array[Array[Value]] = _

    // Each IndexSeeker can have multiple index queries, hence the nesting.
    // The outer array has one element per IndexSeeker (i.e for each node variable)
    // The inner ArrayBuffer holds the lambdas used to execute the seek for each individual index query of that IndexSeeker
    private var seeks: Array[ArrayBuffer[() => Unit]] = _
    private var currentIndexQuery: Array[Int] = _ // For each IndexSeeker this holds the index of the current index query we are positioned at

    // INPUT LOOP TASK
    override protected def initializeInnerLoop(state: PipelinedQueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ReadWriteRow): Boolean = {
      // First time initialization: allocate node cursors and state holders for all index seeks
      if (nodeCursors == null) {
        nodeCursors = new Array(numberOfSeeks)
        exactSeekValues = new Array(numberOfSeeks)
        seeks = new Array(numberOfSeeks)
        currentIndexQuery = new Array(numberOfSeeks)
        var i = 0
        while (i < numberOfSeeks) {
          nodeCursors(i) = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
          seeks(i) = new ArrayBuffer[() => Unit]()
          i += 1
        }
      }

      // For every input row, set up index seek lambdas with index queries computed on the current input row,
      // execute the first index query of all index seeks, and point all cursors except the innermost (rhs) at the first result
      val read = state.queryContext.transactionalContext.transaction.dataRead()
      val queryState = state.queryStateForExpressionEvaluation(resources)

      initExecutionContext.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences) // Copy arguments from the input row
      var i = 0
      while (i < numberOfSeeks) {
        // Recompute the index queries for the seek
        // [Here we have an optimization opportunity to skip this step if the variables that the seek depends on did not change compared to the last input row]
        indexQueries = indexSeekers(i).computeIndexQueries(queryState, initExecutionContext)
        val indexQueryIterator = indexQueries.toIterator
        seeks(i).clear()
        do {
          if (indexQueryIterator.hasNext) {
            val indexQuery = indexQueryIterator.next()
            val indexSeek: () => Unit = seek(state.queryIndexes(nodeIndexSeekParameters(i).queryIndex),
              nodeCursors(i),
              read,
              indexQuery,
              indexSeekers(i).needsValues,
              nodeIndexSeekParameters(i).kernelIndexOrder,
              i)
            seeks(i) += indexSeek
          } else {
            seeks(i) += (() => {})
          }
        } while (indexQueryIterator.hasNext)

        // Start the first index query of the seek
        currentIndexQuery(i) = 0
        seeks(i)(0)()

        // Advance the cursors one step, except for the innermost seek, which will be advanced on the first call to next() in the inner loop
        if (i < numberOfSeeks - 1 && !nodeCursors(i).next()) {
          // If a cursor does not have any results we can abort already
          return false
        }
        i += 1
      }
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && next(numberOfSeeks - 1)) {
        var j = 0
        while (j < numberOfSeeks) {
          val offset = nodeIndexSeekParameters(j).nodeSlotOffset
          val nodeCursor = nodeCursors(j)
          val indexPropertyIndices = indexSeekers(j).indexPropertyIndices
          val indexPropertySlotOffsets = indexSeekers(j).indexPropertySlotOffsets

          // Copy arguments
          outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)

          // Set node id
          outputRow.setLongAt(offset, nodeCursor.nodeReference())

          // Set cached properties
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
        outputRow.next()
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (nodeCursors != null) {
        var i = 0
        while (i < numberOfSeeks) {
          if (nodeCursors(i) != null) {
            nodeCursors(i).setTracer(event)
          }
          i += 1
        }
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {}

    override protected def closeCursors(resources: QueryResources): Unit = {
      var i = 0
      while (i < numberOfSeeks) {
        seeks(i) = null
        resources.cursorPools.nodeValueIndexCursorPool.free(nodeCursors(i))
        nodeCursors(i) = null
        i += 1
      }
    }

    // HELPERS

    // Advance node cursors for the next output row.
    // This is always called with the highest seek index from outside (numberOfSeeks - 1), which points at the innermost (rhs) index seek
    // but can call itself recursively to restart the outer/lhs index seeks (i.e. at lower seek indices) when an inner/rhs seek is exhausted.
    // Returns true if a new output row should be produced
    // (at this point it has always returned from any recursion back to point at the innermost seek (i.e. seekIndex == numberOfSeeks - 1))
    private def next(seekIndex: Int): Boolean = {
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(seekIndex >= 0 || seekIndex < numberOfSeeks)

      while (true) {
        if (nodeCursors(seekIndex) != null && nodeCursors(seekIndex).next()) {
          if (seekIndex >= numberOfSeeks - 1) {
            // We are positioned at a valid row
            return true
          } else {
            // Restart the next index seek
            val nextSeekIndex = seekIndex + 1
            currentIndexQuery(nextSeekIndex) = 0
            seeks(nextSeekIndex)(0)() // Execute the first index query of the next index seek
            return next(nextSeekIndex)
          }
        } else if (currentIndexQuery(seekIndex) < seeks(seekIndex).length - 1) {
          currentIndexQuery(seekIndex) += 1
          seeks(seekIndex)(currentIndexQuery(seekIndex))() // Execute the actual index seek
        } else {
          if (seekIndex > 0)
            return next(seekIndex - 1)
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

      // We don't need property values from the index for an exact seek,
      // so instead we extract the values directly from the predicates and cache them for later use in the innerLoop
      exactSeekValues(exactSeekValueIndex) =
        if (needsValues && predicates.forall(_.isInstanceOf[ExactPredicate]))
          predicates.map(_.asInstanceOf[ExactPredicate].value()).toArray
        else
          null

      val needsValuesFromIndexSeek = exactSeekValues(exactSeekValueIndex) == null && needsValues

      () => read.nodeIndexSeek(index, nodeCursor, IndexQueryConstraints.constrained(indexOrder, needsValuesFromIndexSeek), predicates: _*)
    }
  }
}

class IndexSeeker(parameters: NodeIndexSeekParameters) extends NodeIndexSeeker {
  override val indexMode: IndexSeekMode = parameters.indexSeekMode

  override val valueExpr: QueryExpression[Expression] = parameters.valueExpression

  override val propertyIds: Array[Int] = parameters.slottedIndexProperties.map(_.propertyKeyId)

  val indexPropertyIndices: Array[Int] = parameters.slottedIndexProperties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  val indexPropertySlotOffsets: Array[Int] = parameters.slottedIndexProperties.flatMap(_.maybeCachedNodePropertySlot)
  val needsValues: Boolean = indexPropertyIndices.nonEmpty
}

