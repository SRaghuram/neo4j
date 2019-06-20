/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyLabel, RelationshipTypes}
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.NameId
import org.neo4j.values.storable.Values


class RelationshipCountFromCountStoreOperator(val workIdentity: WorkIdentity,
                                              offset: Int,
                                              startLabel: Option[LazyLabel],
                                              relationshipTypes: RelationshipTypes,
                                              endLabel: Option[LazyLabel],
                                              argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    IndexedSeq(new RelationshipFromCountStoreTask(inputMorsel.nextCopy))
  }

  sealed trait RelId {
    def id: Int
  }

  case object Unknown extends RelId {
    override def id: Int = throw new UnsupportedOperationException
  }

  case object Wildcard extends RelId {
    override def id: Int = NameId.WILDCARD
  }

  case class Known(override val id: Int) extends RelId

  class RelationshipFromCountStoreTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "RelationshipFromCountStoreTask"

    private var hasNext = false
    private var executionEvent: OperatorProfileEvent = OperatorProfileEvent.NONE

    override def workIdentity: WorkIdentity = RelationshipCountFromCountStoreOperator.this.workIdentity

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      hasNext = true
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      if (hasNext) {
        val startLabelId = getLabelId(startLabel, context)
        val endLabelId = getLabelId(endLabel, context)

        val count = if (startLabelId == Unknown || endLabelId == Unknown)
          0
        else
          countOneDirection(context, startLabelId.id, endLabelId.id)

        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setRefAt(offset, Values.longValue(count))
        outputRow.moveToNextRow()
        hasNext = false
      }
    }

    private def getLabelId(lazyLabel: Option[LazyLabel], context: QueryContext): RelId = lazyLabel match {
      case Some(label) =>
        val id = label.getId(context)
        if (id == LazyLabel.UNKNOWN)
          Unknown
        else
          Known(id)
      case _ => Wildcard
    }

    private def countOneDirection(context: QueryContext, startLabelId: Int, endLabelId: Int): Long = {
      val relationshipTypeIds: Array[Int] = relationshipTypes.types(context)
      if (relationshipTypeIds == null) {
        executionEvent.dbHit()
        context.relationshipCountByCountStore(startLabelId, NameId.WILDCARD, endLabelId)
      } else {
        var i = 0
        var count = 0L
        while (i < relationshipTypeIds.length) {
          executionEvent.dbHit()
          count += context.relationshipCountByCountStore(startLabelId, relationshipTypeIds(i), endLabelId)
          i += 1
        }
        count
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      this.executionEvent = event
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      // nothing to do here
    }
  }

}
