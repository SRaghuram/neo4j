/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state.buffers

import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.physicalplanning.ReadOnlyArray
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselReadCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselRow
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateFactory
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.MorselAccumulator
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.PerArgument
import org.neo4j.cypher.internal.runtime.pipelined.state.QueryCompletionTracker
import org.neo4j.cypher.internal.runtime.pipelined.state.buffers.Buffers.AccumulatingBuffer
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker

import scala.collection.mutable.ArrayBuffer

/**
 * Extension of OptionalMorselBuffer.
 * Holds an ASM in order to track argument rows that do not result in any output rows, i.e. gets filtered out.
 * This is used in front of a pipeline with an AntiOperator.
 * This buffer sits between two pipelines.
 */
class AntiMorselBuffer(id: BufferId,
                       tracker: QueryCompletionTracker,
                       downstreamArgumentReducers: ReadOnlyArray[AccumulatingBuffer],
                       override val argumentStateMaps: ArgumentStateMaps,
                       argumentStateMapId: ArgumentStateMapId,
                       morselSize: Int
                      )
  extends BaseArgExistsMorselBuffer[Seq[MorselData], AntiArgumentState](id, tracker, downstreamArgumentReducers, argumentStateMaps, argumentStateMapId) {

  override def canPut: Boolean = true

  override def put(data: IndexedSeq[PerArgument[Morsel]], resources: QueryResources): Unit = {
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[put]   $this <- ${data.mkString(", ")}")
    }

    var i = 0
    while (i < data.length) {
      argumentStateMap.update(data(i).argumentRowId, {acc =>
        // updating the AntiArgumentState simply sets a flag -> no need to increment tracker or reducers.
        acc.update(data(i).value, resources)
      })
      i += 1
    }
  }

  override protected def clearArgumentState(s: AntiArgumentState): Unit = {}

  override def take(): Seq[MorselData] = {
    // To keep input order (i.e., place the null rows at the right position), we give the data out in ascending argument row id order.

    var data = getNextArgumentState()
    val result =
      if (null == data) {
        null.asInstanceOf[Seq[MorselData]]
      } else {
        val datas = new ArrayBuffer[MorselData]()
        var i = morselSize

        def addData(data: MorselData): Unit = {
          datas += data
          if (data.argumentStream == EndOfEmptyStream) {
            i -= 1
          }
        }
        addData(data)

        while (null != data && i > 0) {
          data = getNextArgumentState()
          if (null != data) {
            addData(data)
          }
          i -= 1
        }
        datas
      }
    if (DebugSupport.BUFFERS.enabled) {
      DebugSupport.BUFFERS.log(s"[take]  $this -> $result")
    }
    result
  }

  private def getNextArgumentState(): MorselData = {
    val argumentStateList = argumentStateMap.takeCompleted(1)
    if (argumentStateList == null) null
    else {
      val argumentState = argumentStateList.head
      val endOfStream = if (argumentState.didReceiveData) EndOfNonEmptyStream else EndOfEmptyStream
      MorselData(IndexedSeq.empty, endOfStream, argumentState.argumentRowIdsForReducers, argumentState.argumentRow)
    }
  }

  override def hasData: Boolean = {
    argumentStateMap.someArgumentStateIsCompletedOr(_ => false)
  }

  override def close(datas: Seq[MorselData]): Unit = {
    for (data <- datas) {
      if (DebugSupport.BUFFERS.enabled) {
        DebugSupport.BUFFERS.log(s"[closeOne] $this -X- ${data.argumentStream} , 0 , ${data.argumentRowIdsForReducers}")
      }

      data.argumentStream match {
        case _: EndOfStream =>
          // Decrement that corresponds to the increment in initiate
          tracker.decrement()
          forAllArgumentReducers(downstreamArgumentReducers, data.argumentRowIdsForReducers, _.decrement(_))
        case _ =>
          // Do nothing
      }
    }
  }

  override def toString: String =
    s"${getClass.getSimpleName}($argumentStateMap)"
}

trait AntiArgumentState extends MorselAccumulator[Morsel] {
  /**
   * @return `true` if this accumulater saw data at any point in time, `false` otherwise.
   */
  def didReceiveData: Boolean

  def argumentRow: MorselRow
}

object AntiArgumentState {
  object Factory extends ArgumentStateFactory[AntiArgumentState] {
    override def newStandardArgumentState(argumentRowId: Long,
                                          argumentMorsel: MorselReadCursor,
                                          argumentRowIdsForReducers: Array[Long],
                                          memoryTracker: MemoryTracker): AntiArgumentState =
      new StandardAntiArgumentState(argumentRowId, argumentMorsel.snapshot(), argumentRowIdsForReducers)

    override def newConcurrentArgumentState(argumentRowId: Long, argumentMorsel: MorselReadCursor, argumentRowIdsForReducers: Array[Long]): AntiArgumentState =
      new ConcurrentAntiArgumentState(argumentRowId, argumentMorsel.snapshot(), argumentRowIdsForReducers)
  }

  class StandardAntiArgumentState(override val argumentRowId: Long,
                                  override val argumentRow: MorselRow,
                                  override val argumentRowIdsForReducers: Array[Long]) extends AntiArgumentState {

    private var _didReceiveData: Boolean = false

    def didReceiveData: Boolean = _didReceiveData

    override def update(data: Morsel, resources: QueryResources): Unit = _didReceiveData = true

    override def toString: String = {
      s"StandardAntiArgumentState(argumentRowId=$argumentRowId, argu\\mentRowIdsForReducers=[${argumentRowIdsForReducers.mkString(",")}], argumentMorsel=$argumentRow)"
    }

    override final def shallowSize: Long = StandardAntiArgumentState.SHALLOW_SIZE
  }

  object StandardAntiArgumentState {
    private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[AntiArgumentState])
  }

  class ConcurrentAntiArgumentState(override val argumentRowId: Long,
                                    override val argumentRow: MorselRow,
                                    override val argumentRowIdsForReducers: Array[Long]) extends AntiArgumentState {

    @volatile
    private var _didReceiveData: Boolean = false

    def didReceiveData: Boolean = _didReceiveData

    override def update(data: Morsel, resources: QueryResources): Unit = _didReceiveData = true

    override def toString: String = {
      s"ConcurrentAntiArgumentState(argumentRowId=$argumentRowId, argumentRowIdsForReducers=[${argumentRowIdsForReducers.mkString(",")}], argumentMorsel=$argumentRow)"
    }

    override final def shallowSize: Long = ConcurrentAntiArgumentState.SHALLOW_SIZE
  }

  object ConcurrentAntiArgumentState {
    private final val SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(classOf[ConcurrentAntiArgumentState])
  }
}

