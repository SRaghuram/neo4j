/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.v4_0.util.NameId
import org.neo4j.values.storable.Values


class NodeCountFromCountStoreOperator(val workIdentity: WorkIdentity,
                                      offset: Int,
                                      labels: Seq[Option[LazyLabel]],
                                      argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  private val lazyLabels: Array[LazyLabel] = labels.flatten.toArray
  private val wildCards: Int = labels.count(_.isEmpty)

  override def nextTasks(queryContext: QueryContext,
                         state: QueryState,
                         inputMorsel: MorselParallelizer,
                         parallelism: Int,
                         resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    IndexedSeq(new NodeFromCountStoreTask(inputMorsel.nextCopy))
  }


  class NodeFromCountStoreTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "NodeFromCountStoreTask"

    private var hasNext = false

    override protected def initializeInnerLoop(context: QueryContext, state: QueryState, resources: QueryResources): Boolean = {
      hasNext = true
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      if (hasNext) {
        var count = 1L

        var i = 0
        while (i < lazyLabels.length) {
          val idOfLabel = lazyLabels(i).getId(context)
          if (idOfLabel == LazyLabel.UNKNOWN) {
            count = 0
          } else {
            count = count * context.nodeCountByCountStore(idOfLabel)
          }
          i += 1
        }
        i = 0
        while (i < wildCards) {
          count *= context.nodeCountByCountStore(NameId.WILDCARD)
          i += 1
        }

        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setRefAt(offset, Values.longValue(count))
        outputRow.moveToNextRow()
        hasNext = false
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      // nothing to do here
    }
  }

}
