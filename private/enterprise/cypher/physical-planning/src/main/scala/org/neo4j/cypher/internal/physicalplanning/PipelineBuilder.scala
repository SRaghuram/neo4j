/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

// TODO: refactor this file into a proper builder pattern, where the result of
//  building is immutable definitions, and mutability is visible inside the builder only

case class BufferId(x: Int) extends AnyVal
case class PipelineId(x: Int) extends AnyVal
case class ArgumentStateMapId(x: Int) extends AnyVal

object PipelineId {
  val NO_PIPELINE: PipelineId = PipelineId(-1)
}

/**
  * Maps to one ArgumentStateMap.
  */
class ArgumentStateDefinition(val id: ArgumentStateMapId,
                              val planId: Id,
                              val argumentSlotOffset: Int,
                              val counts: Boolean)

/**
  * Common superclass of all buffer definitions.
  */
abstract class BufferDefinition(val id: BufferId) {
  // We need multiple reducers because a buffer might need to
  // reference count for multiple downstream reduce operators,
  // at potentially different argument depths
  val reducers = new ArrayBuffer[ArgumentStateMapId]
  val workCancellers = new ArrayBuffer[ArgumentStateDefinition]
  def workOnPut: Option[HasWorkIdentity] = None
}

/**
  * A buffer between two pipelines. Maps to a MorselBuffer.
  */
class MorselBufferDefinition(id: BufferId,
                             val producingPipelineId: PipelineId) extends BufferDefinition(id)

/**
  * One of the delegates of an ApplyBufferDefinition. Maps to a MorselBuffer.
  */
class DelegateBufferDefinition(id: BufferId,
                               val applyBuffer: ApplyBufferDefinition) extends BufferDefinition(id)

/**
  * Sits between the LHS and RHS of an apply.
  * This acts as a multiplexer. It receives input and copies it into
  * its delegates. Maps to a MorselApplyBuffer.
  */
class ApplyBufferDefinition(id: BufferId,
                            producingPipelineId: PipelineId,
                            val argumentSlotOffset: Int
                           ) extends MorselBufferDefinition(id, producingPipelineId) {
  // These are ArgumentStates of reducers on the RHS
  val reducersOnRHS = new ArrayBuffer[ArgumentStateDefinition]
  val delegates: ArrayBuffer[BufferId] = new ArrayBuffer[BufferId]()
}

/**
  * This buffer groups data by argument row and sits between a pre-reduce and a reduce operator.
  * Maps to a MorselArgumentStateBuffer.
  */
class ArgumentStateBufferDefinition(id: BufferId,
                                    producingPipelineId: PipelineId,
                                    val argumentStateMapId: ArgumentStateMapId) extends MorselBufferDefinition(id, producingPipelineId)

/**
  * This buffer maps to a LHSAccumulatingRHSStreamingBuffer. It sits before a hash join.
  */
class LHSAccumulatingRHSStreamingBufferDefinition(id: BufferId,
                                                  val lhsPipelineId: PipelineId,
                                                  val rhsPipelineId: PipelineId,
                                                  val lhsargumentStateMapId: ArgumentStateMapId,
                                                  val rhsargumentStateMapId: ArgumentStateMapId,
                                                  val argumentSlotOffset: Int) extends BufferDefinition(id)

sealed trait OutputDefinition
case class ProduceResultOutput(plan: ProduceResult) extends OutputDefinition
case class MorselBufferOutput(id: BufferId) extends OutputDefinition
case class MorselArgumentStateBufferOutput(id: BufferId, argumentSlotOffset: Int) extends OutputDefinition
case class ReduceOutput(buffer: BufferDefinition, plan: LogicalPlan) extends OutputDefinition
case object NoOutput extends OutputDefinition

class Pipeline(val id: PipelineId,
               val headPlan: LogicalPlan) {
  var inputBuffer: BufferDefinition = _
  var outputDefinition: OutputDefinition = NoOutput
  val middlePlans = new ArrayBuffer[LogicalPlan]
  var serial: Boolean = false
  var checkHasDemand: Boolean = false
}

class StateDefinition(val physicalPlan: PhysicalPlan) {
  val buffers = new ArrayBuffer[BufferDefinition]
  val argumentStateMaps = new ArrayBuffer[ArgumentStateDefinition]

  def newArgumentStateMap(planId: Id, argumentSlotOffset: Int, counts: Boolean): ArgumentStateDefinition = {
    val x = argumentStateMaps.size
    val asm = new ArgumentStateDefinition(ArgumentStateMapId(x), planId, argumentSlotOffset, counts)
    argumentStateMaps += asm
    asm
  }

  def newBuffer(producingPipelineId: PipelineId): MorselBufferDefinition = {
    val x = buffers.size
    val buffer = new MorselBufferDefinition(BufferId(x), producingPipelineId)
    buffers += buffer
    buffer
  }

  def newDelegateBuffer(applyBufferDefinition: ApplyBufferDefinition): DelegateBufferDefinition = {
    val x = buffers.size
    val buffer = new DelegateBufferDefinition(BufferId(x), applyBufferDefinition)
    buffers += buffer
    buffer
  }

  def newApplyBuffer(producingPipelineId: PipelineId,
                     argumentSlotOffset: Int): ApplyBufferDefinition = {
    val x = buffers.size
    val buffer = new ApplyBufferDefinition(BufferId(x), producingPipelineId, argumentSlotOffset)
    buffers += buffer
    buffer
  }

  def newArgumentStateBuffer(producingPipelineId: PipelineId,
                             argumentStateMapId: ArgumentStateMapId): ArgumentStateBufferDefinition = {
    val x = buffers.size
    val buffer = new ArgumentStateBufferDefinition(BufferId(x), producingPipelineId, argumentStateMapId)
    buffers += buffer
    buffer
  }

  def newLhsAccumulatingRhsStreamingBuffer(lhsProducingPipelineId: PipelineId,
                                           rhsProducingPipelineId: PipelineId,
                                           lhsargumentStateMapId: ArgumentStateMapId,
                                           rhsargumentStateMapId: ArgumentStateMapId,
                                           argumentSlotOffset: Int): LHSAccumulatingRHSStreamingBufferDefinition = {
    val x = buffers.size
    val buffer = new LHSAccumulatingRHSStreamingBufferDefinition(BufferId(x), lhsProducingPipelineId, rhsProducingPipelineId, lhsargumentStateMapId, rhsargumentStateMapId, argumentSlotOffset)
    buffers += buffer
    buffer
  }

  def findArgumentStateMapForPlan(planId: Id): ArgumentStateMapId = {
    argumentStateMaps.find(_.planId == planId).map(_.id).getOrElse {
      throw new IllegalStateException("Requested an ArgumentStateMap for an operator which does not have any.")
    }
  }

  var initBuffer: ApplyBufferDefinition = _
}

object PipelineBuilder {
  val NO_PRODUCING_PIPELINE: Int = -1
}

class PipelineBuilder(breakingPolicy: PipelineBreakingPolicy,
                      stateDefinition: StateDefinition,
                      slotConfigurations: SlotConfigurations)
  extends TreeBuilderWithArgument[Pipeline, ApplyBufferDefinition] {

  val pipelines = new ArrayBuffer[Pipeline]

  private def newPipeline(plan: LogicalPlan) = {
    val pipeline = new Pipeline(PipelineId(pipelines.size), plan)
    pipelines += pipeline
    pipeline
  }

  private def outputToBuffer(pipeline: Pipeline): MorselBufferDefinition = {
    val output = stateDefinition.newBuffer(pipeline.id)
    pipeline.outputDefinition = MorselBufferOutput(output.id)
    output
  }

  private def outputToApplyBuffer(pipeline: Pipeline, argumentSlotOffset: Int): ApplyBufferDefinition = {
    val output = stateDefinition.newApplyBuffer(pipeline.id, argumentSlotOffset)
    pipeline.outputDefinition = MorselBufferOutput(output.id)
    output
  }

  private def outputToArgumentStateBuffer(pipeline: Pipeline, plan: LogicalPlan, applyBuffer: ApplyBufferDefinition, argumentSlotOffset: Int): ArgumentStateBufferDefinition = {
    val asm = stateDefinition.newArgumentStateMap(plan.id, argumentSlotOffset, true)
    val output = stateDefinition.newArgumentStateBuffer(pipeline.id, asm.id)
    pipeline.outputDefinition = ReduceOutput(output, plan)
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToLhsAccumulatingRhsStreamingBuffer(lhs: Pipeline,
                                                        rhs: Pipeline,
                                                        planId: Id,
                                                        applyBuffer: ApplyBufferDefinition,
                                                        argumentSlotOffset: Int): LHSAccumulatingRHSStreamingBufferDefinition = {
    val lhsAsm = stateDefinition.newArgumentStateMap(planId, argumentSlotOffset, true)
    val rhsAsm = stateDefinition.newArgumentStateMap(planId, argumentSlotOffset, true)
    val output = stateDefinition.newLhsAccumulatingRhsStreamingBuffer(lhs.id, rhs.id, lhsAsm.id, rhsAsm.id, argumentSlotOffset)
    lhs.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset)
    rhs.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset)
    markReducerInUpstreamBuffers(lhs.inputBuffer, applyBuffer, lhsAsm)
    markReducerInUpstreamBuffers(rhs.inputBuffer, applyBuffer, rhsAsm)
    output
  }

  override protected def initialArgument(leftLeaf: LogicalPlan): ApplyBufferDefinition = {
    val initialArgumentSlotOffset = slotConfigurations(leftLeaf.id).getArgumentLongOffsetFor(Id.INVALID_ID)
    stateDefinition.initBuffer = stateDefinition.newApplyBuffer(NO_PIPELINE, initialArgumentSlotOffset)
    stateDefinition.initBuffer
  }

  override protected def onLeaf(plan: LogicalPlan,
                                argument: ApplyBufferDefinition): Pipeline = {
    if (breakingPolicy.breakOn(plan)) {
      val pipeline = newPipeline(plan)
      val delegate = stateDefinition.newDelegateBuffer(argument)
      argument.delegates += delegate.id
      pipeline.inputBuffer = delegate
      pipeline
    } else {
      throw new UnsupportedOperationException("not implemented")
    }
  }

  override protected def onOneChildPlan(plan: LogicalPlan,
                                        source: Pipeline,
                                        argument: ApplyBufferDefinition): Pipeline = {
    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source)
          pipeline.serial = true
          pipeline.checkHasDemand = true
          pipeline
        } else {
          source.outputDefinition = ProduceResultOutput(produceResult)
          source.serial = true
          source.checkHasDemand = true
          source
        }

      case _: Sort |
           _: Aggregation =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val argumentStateBuffer = outputToArgumentStateBuffer(source, plan, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = argumentStateBuffer
          pipeline
        } else {
          throw new UnsupportedOperationException("not implemented")
        }

      case _: Expand |
           _: PruningVarExpand |
           _: VarExpand |
           _: OptionalExpand |
           _: FindShortestPaths |
           _: UnwindCollection |
           _: VarExpand =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source)
          pipeline
        } else {
          source.middlePlans += plan
          source
        }

      case _: Limit =>
        val asm = stateDefinition.newArgumentStateMap(plan.id, argument.argumentSlotOffset, false)
        markCancellerInUpstreamBuffers(source.inputBuffer, argument, asm)
        source.middlePlans += plan
        source

      case _ =>
        source.middlePlans += plan
        source
    }
  }

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan,
                                                      lhs: Pipeline,
                                                      argument: ApplyBufferDefinition): ApplyBufferDefinition =
  {
    plan match {
      case _: plans.Apply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToApplyBuffer(lhs, argumentSlotOffset)

      case _ =>
        argument
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline, argument: ApplyBufferDefinition): Pipeline = {

    plan match {
      case _: plans.Apply =>
        rhs

      case _: plans.NodeHashJoin =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(lhs, rhs, plan.id, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = buffer
          pipeline
        } else {
          throw new UnsupportedOperationException("not implemented")
        }

      case _ =>
        throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
    }
  }

  // HELPERS

  /*
    * Plan:
    *             ProduceResults
    *               |
    *             Apply
    *             /  \
    *           LHS  Sort
    *                |
    *                Expand
    *                |
    *                ...
    *                |
    *                Scan
    *
    * Pipelines:
    *  -LHS->  ApplyBuffer  -Scan->  Buffer -...->  Buffer  -Presort->  ArgumentStateMapBuffer  -Sort,ProduceResults->
    *             ^                  |--------------------|
    *             |                                  ^
    *   reducersOnRHS += argumentStateDefinition     reducers += argumentStateDefinition.id
    *
    * Mark `argumentStateMapId` as a reducer in all buffers between `buffer` and `applyBuffer`. This has
    * to be done so that reference counting of inflight work will work correctly, so that `buffer` knows
    * when each argument is complete.
    */
  private def markReducerInUpstreamBuffers(buffer: BufferDefinition,
                                           applyBuffer: ApplyBufferDefinition,
                                           argumentStateDefinition: ArgumentStateDefinition): Unit = {
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.reducers += argumentStateDefinition.id,
      lHSAccumulatingRHSStreamingBufferDefinition => lHSAccumulatingRHSStreamingBufferDefinition.reducers += argumentStateDefinition.id,
      delegateBuffer => {
        val b = delegateBuffer.applyBuffer
        b.reducers += argumentStateDefinition.id
        delegateBuffer.reducers += argumentStateDefinition.id
      },
      lastDelegateBuffer => {
        val b = lastDelegateBuffer.applyBuffer
        b.reducersOnRHS += argumentStateDefinition
        lastDelegateBuffer.reducers += argumentStateDefinition.id
      }
    )
  }

  private def markCancellerInUpstreamBuffers(buffer: BufferDefinition,
                                             applyBuffer: ApplyBufferDefinition,
                                             argumentStateDefinition: ArgumentStateDefinition): Unit = {
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.workCancellers += argumentStateDefinition,
      lHSAccumulatingRHSStreamingBufferDefinition => lHSAccumulatingRHSStreamingBufferDefinition.workCancellers += argumentStateDefinition,
      delegateBuffer => {
        val b = delegateBuffer.applyBuffer
        b.workCancellers += argumentStateDefinition
        delegateBuffer.workCancellers += argumentStateDefinition
      },
      lastDelegateBuffer => {
        val b = lastDelegateBuffer.applyBuffer
        b.workCancellers += argumentStateDefinition
        lastDelegateBuffer.workCancellers += argumentStateDefinition
      }
    )
  }

  /**
    * This traverses buffers in a breadth first manner, from the given buffer towards the input, stopping at applyBuffer.
    *
    * @param buffer                              start the traversal here
    * @param applyBuffer                         end the traversal here
    * @param onInputBuffer                       called for every input buffer
    * @param onDelegateBuffer                    called for every delegate buffer except the last one belonging to applyBuffer
    * @param onLHSAccumulatingRHSStreamingBuffer called for every LHSAccumulatingRHSStreamingBufferDefinition
    * @param onLastDelegate                      called for the last delegate belonging to applyBuffer
    */
  private def traverseBuffers(buffer: BufferDefinition,
                              applyBuffer: ApplyBufferDefinition,
                              onInputBuffer: MorselBufferDefinition => Unit,
                              onLHSAccumulatingRHSStreamingBuffer: LHSAccumulatingRHSStreamingBufferDefinition => Unit,
                              onDelegateBuffer: DelegateBufferDefinition => Unit,
                              onLastDelegate: DelegateBufferDefinition => Unit): Unit = {
    @tailrec
    def bfs(buffers: Seq[BufferDefinition]): Unit = {
      val upstreams = new ArrayBuffer[BufferDefinition]()

      buffers.foreach {
        case d: DelegateBufferDefinition if d.applyBuffer == applyBuffer =>
          onLastDelegate(d)
        case _: ApplyBufferDefinition =>
          throw new IllegalStateException("Nothing should have an apply buffer as immediate input, it should have a delegate buffer instead.")
        case b: LHSAccumulatingRHSStreamingBufferDefinition =>
          onLHSAccumulatingRHSStreamingBuffer(b)
          // We only add the RHS since the LHS is not streaming through the join
          // Therefore it needs to finish before the join can even start
          upstreams += pipelines(b.rhsPipelineId.x).inputBuffer
        case d: DelegateBufferDefinition =>
          onDelegateBuffer(d)
          upstreams += pipelines(d.applyBuffer.producingPipelineId.x).inputBuffer
        case b: MorselBufferDefinition =>
          onInputBuffer(b)
          upstreams += pipelines(b.producingPipelineId.x).inputBuffer
      }

      if (upstreams.nonEmpty) {
        bfs(upstreams)
      }
    }

    val buffers = Seq(buffer)
    bfs(buffers)
  }
}
