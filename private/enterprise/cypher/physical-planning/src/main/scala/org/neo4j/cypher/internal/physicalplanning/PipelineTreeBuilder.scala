/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Collection of mutable builder classes that are modified by [[PipelineTreeBuilder]] and finally
  * converted to an [[ExecutionGraphDefinition]] by the [[PipelineBuilder]].
  */
object PipelineTreeBuilder {
  /**
    * Builder for [[PipelineDefinition]]
    */
  class PipelineDefinitionBuild(val id: PipelineId,
                                val headPlan: LogicalPlan) {
    var inputBuffer: BufferDefinitionBuild = _
    var outputDefinition: OutputDefinition = NoOutput
    /**
      * The list of fused plans contains all fusable plans starting from the headPlan and
      * continuing with as many fusable consecutive middlePlans as possible.
      * If a plan is in `fusedPlans`. it will not be in `middlePlans` and vice versa.
      */
    val fusedPlans = new ArrayBuffer[LogicalPlan]
    val middlePlans = new ArrayBuffer[LogicalPlan]
    var serial: Boolean = false
  }

  /**
    * Builder for [[ArgumentStateDefinition]]
    */
  case class ArgumentStateDefinitionBuild(id: ArgumentStateMapId,
                                          planId: Id,
                                          argumentSlotOffset: Int,
                                          counts: Boolean)

  sealed trait DownstreamStateOperator
  case class DownstreamReduce(id: ArgumentStateMapId) extends DownstreamStateOperator
  case class DownstreamWorkCanceller(id: ArgumentStateMapId) extends DownstreamStateOperator
  case class DownstreamState(id: ArgumentStateMapId) extends DownstreamStateOperator

  /**
    * Builder for [[BufferDefinition]]
    */
  abstract class BufferDefinitionBuild(val id: BufferId, val bufferConfiguration: SlotConfiguration) {
    val downstreamStates = new ArrayBuffer[DownstreamStateOperator]
  }

  /**
    * Builder for [[RegularBufferVariant]]
    */
  class MorselBufferDefinitionBuild(id: BufferId,
                                    val producingPipelineId: PipelineId,
                                    bufferConfiguration: SlotConfiguration) extends BufferDefinitionBuild(id, bufferConfiguration)

  /**
    * Builder for [[OptionalBufferVariant]]
    */
  class OptionalMorselBufferDefinitionBuild(id: BufferId,
                                            val producingPipelineId: PipelineId,
                                            val argumentStateMapId: ArgumentStateMapId,
                                            val argumentSlotOffset: Int,
                                            bufferConfiguration: SlotConfiguration) extends BufferDefinitionBuild(id, bufferConfiguration)
  /**
    * Builder for [[RegularBufferVariant]], that is a delegate.
    */
  class DelegateBufferDefinitionBuild(id: BufferId,
                                      val applyBuffer: ApplyBufferDefinitionBuild,
                                      bufferConfiguration: SlotConfiguration) extends BufferDefinitionBuild(id, bufferConfiguration)

  /**
    * Builder for [[ApplyBufferVariant]]
    */
  class ApplyBufferDefinitionBuild(id: BufferId,
                                   producingPipelineId: PipelineId,
                                   val argumentSlotOffset: Int,
                                   bufferSlotConfiguration: SlotConfiguration
                             ) extends MorselBufferDefinitionBuild(id, producingPipelineId, bufferSlotConfiguration) {
    // These are ArgumentStates of reducers on the RHS
    val reducersOnRHS = new ArrayBuffer[ArgumentStateDefinitionBuild]
    val delegates: ArrayBuffer[BufferId] = new ArrayBuffer[BufferId]()
  }

  /**
    * Builder for [[ArgumentStateBufferVariant]]
    */
  class ArgumentStateBufferDefinitionBuild(id: BufferId,
                                           producingPipelineId: PipelineId,
                                           val argumentStateMapId: ArgumentStateMapId,
                                           bufferSlotConfiguration: SlotConfiguration) extends MorselBufferDefinitionBuild(id, producingPipelineId, bufferSlotConfiguration)

  /**
    * Builder for [[LHSAccumulatingRHSStreamingBufferVariant]]
    */
  class LHSAccumulatingRHSStreamingBufferDefinitionBuild(id: BufferId,
                                                         val lhsPipelineId: PipelineId,
                                                         val rhsPipelineId: PipelineId,
                                                         val lhsArgumentStateMapId: ArgumentStateMapId,
                                                         val rhsArgumentStateMapId: ArgumentStateMapId,
                                                         bufferSlotConfiguration: SlotConfiguration) extends BufferDefinitionBuild(id, bufferSlotConfiguration)

  /**
    * Builder for [[ExecutionGraphDefinition]]
    */
  class ExecutionStateDefinitionBuild(val physicalPlan: PhysicalPlan) {
    val buffers = new ArrayBuffer[BufferDefinitionBuild]
    val argumentStateMaps = new ArrayBuffer[ArgumentStateDefinitionBuild]
    var initBuffer: ApplyBufferDefinitionBuild = _

    def newArgumentStateMap(planId: Id, argumentSlotOffset: Int, counts: Boolean): ArgumentStateDefinitionBuild = {
      val x = argumentStateMaps.size
      val asm = ArgumentStateDefinitionBuild(ArgumentStateMapId(x), planId, argumentSlotOffset, counts)
      argumentStateMaps += asm
      asm
    }

    def newBuffer(producingPipelineId: PipelineId, bufferSlotConfiguration: SlotConfiguration): MorselBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new MorselBufferDefinitionBuild(BufferId(x), producingPipelineId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newOptionalBuffer(producingPipelineId: PipelineId, argumentStateMapId: ArgumentStateMapId, argumentSlotOffset: Int,
                          bufferSlotConfiguration: SlotConfiguration): OptionalMorselBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new OptionalMorselBufferDefinitionBuild(BufferId(x), producingPipelineId, argumentStateMapId, argumentSlotOffset, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newDelegateBuffer(applyBufferDefinition: ApplyBufferDefinitionBuild, bufferSlotConfiguration: SlotConfiguration): DelegateBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new DelegateBufferDefinitionBuild(BufferId(x), applyBufferDefinition, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newApplyBuffer(producingPipelineId: PipelineId,
                       argumentSlotOffset: Int, bufferSlotConfiguration: SlotConfiguration): ApplyBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new ApplyBufferDefinitionBuild(BufferId(x), producingPipelineId, argumentSlotOffset, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newArgumentStateBuffer(producingPipelineId: PipelineId,
                               argumentStateMapId: ArgumentStateMapId,
                               bufferSlotConfiguration: SlotConfiguration): ArgumentStateBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new ArgumentStateBufferDefinitionBuild(BufferId(x), producingPipelineId, argumentStateMapId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newLhsAccumulatingRhsStreamingBuffer(lhsProducingPipelineId: PipelineId,
                                             rhsProducingPipelineId: PipelineId,
                                             lhsargumentStateMapId: ArgumentStateMapId,
                                             rhsargumentStateMapId: ArgumentStateMapId,
                                             bufferSlotConfiguration: SlotConfiguration): LHSAccumulatingRHSStreamingBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new LHSAccumulatingRHSStreamingBufferDefinitionBuild(BufferId(x), lhsProducingPipelineId, rhsProducingPipelineId, lhsargumentStateMapId, rhsargumentStateMapId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }
  }
}

/**
  * Fills an [[ExecutionStateDefinitionBuild]] and and array of [[PipelineDefinitionBuild]]s.
  * Final conversion to [[ExecutionGraphDefinition]] is done by [[PipelineBuilder]].
  */
class PipelineTreeBuilder(breakingPolicy: PipelineBreakingPolicy,
                          operatorFusionPolicy: OperatorFusionPolicy,
                          stateDefinition: ExecutionStateDefinitionBuild,
                          slotConfigurations: SlotConfigurations)
  extends TreeBuilder[PipelineDefinitionBuild, ApplyBufferDefinitionBuild] {

  private[physicalplanning] val pipelines = new ArrayBuffer[PipelineDefinitionBuild]
  private[physicalplanning] val applyRhsPlans = new mutable.HashMap[Int, Int]()

  private def newPipeline(plan: LogicalPlan) = {
    val pipeline = new PipelineDefinitionBuild(PipelineId(pipelines.size), plan)
    pipelines += pipeline
    if (operatorFusionPolicy.canFuse(plan))
      pipeline.fusedPlans += plan
    pipeline
  }

  private def outputToBuffer(pipeline: PipelineDefinitionBuild, nextPipelineHeadPlan: LogicalPlan): MorselBufferDefinitionBuild = {
    val output = stateDefinition.newBuffer(pipeline.id, slotConfigurations(pipeline.headPlan.id))
    pipeline.outputDefinition = MorselBufferOutput(output.id, nextPipelineHeadPlan.id)
    output
  }

  private def outputToApplyBuffer(pipeline: PipelineDefinitionBuild, argumentSlotOffset: Int, nextPipelineHeadPlan: LogicalPlan): ApplyBufferDefinitionBuild = {
    val output = stateDefinition.newApplyBuffer(pipeline.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id))
    pipeline.outputDefinition = MorselBufferOutput(output.id, nextPipelineHeadPlan.id)
    output
  }

  private def outputToArgumentStateBuffer(pipeline: PipelineDefinitionBuild, plan: LogicalPlan, applyBuffer: ApplyBufferDefinitionBuild, argumentSlotOffset: Int): ArgumentStateBufferDefinitionBuild = {
    val asm = stateDefinition.newArgumentStateMap(plan.id, argumentSlotOffset, counts = true)
    val output = stateDefinition.newArgumentStateBuffer(pipeline.id, asm.id, slotConfigurations(pipeline.headPlan.id))
    pipeline.outputDefinition = ReduceOutput(output.id, plan)
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToOptionalMorselBuffer(pipeline: PipelineDefinitionBuild, plan: LogicalPlan, applyBuffer: ApplyBufferDefinitionBuild, argumentSlotOffset: Int): OptionalMorselBufferDefinitionBuild = {
    val asm = stateDefinition.newArgumentStateMap(plan.id, argumentSlotOffset, counts = true)
    val output = stateDefinition.newOptionalBuffer(pipeline.id, asm.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id))
    pipeline.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, plan.id)
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToLhsAccumulatingRhsStreamingBuffer(lhs: PipelineDefinitionBuild,
                                                        rhs: PipelineDefinitionBuild,
                                                        planId: Id,
                                                        applyBuffer: ApplyBufferDefinitionBuild,
                                                        argumentSlotOffset: Int): LHSAccumulatingRHSStreamingBufferDefinitionBuild = {
    val lhsAsm = stateDefinition.newArgumentStateMap(planId, argumentSlotOffset, counts = true)
    val rhsAsm = stateDefinition.newArgumentStateMap(planId, argumentSlotOffset, counts = true)
    val output = stateDefinition.newLhsAccumulatingRhsStreamingBuffer(lhs.id, rhs.id, lhsAsm.id, rhsAsm.id, slotConfigurations(rhs.headPlan.id))
    lhs.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, planId)
    rhs.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, planId)
    markReducerInUpstreamBuffers(lhs.inputBuffer, applyBuffer, lhsAsm)
    markReducerInUpstreamBuffers(rhs.inputBuffer, applyBuffer, rhsAsm)
    output
  }

  override protected def validatePlan(plan: LogicalPlan): Unit = breakingPolicy.breakOn(plan)

  override protected def initialArgument(leftLeaf: LogicalPlan): ApplyBufferDefinitionBuild = {
    val initialArgumentSlotOffset = slotConfigurations(leftLeaf.id).getArgumentLongOffsetFor(Id.INVALID_ID)
    stateDefinition.initBuffer = stateDefinition.newApplyBuffer(NO_PIPELINE, initialArgumentSlotOffset, SlotAllocation.INITIAL_SLOT_CONFIGURATION)
    stateDefinition.initBuffer
  }

  override protected def onLeaf(plan: LogicalPlan,
                                argument: ApplyBufferDefinitionBuild): PipelineDefinitionBuild = {
    if (breakingPolicy.breakOn(plan)) {
      val pipeline = newPipeline(plan)
      val delegate = stateDefinition.newDelegateBuffer(argument, argument.bufferConfiguration)
      argument.delegates += delegate.id
      pipeline.inputBuffer = delegate
      pipeline
    } else {
      throw new UnsupportedOperationException("not implemented")
    }
  }

  override protected def onOneChildPlan(plan: LogicalPlan,
                                        source: PipelineDefinitionBuild,
                                        argument: ApplyBufferDefinitionBuild): PipelineDefinitionBuild = {

    def canFuseOverPipeline: Boolean = source.fusedPlans.nonEmpty && source.middlePlans.isEmpty && operatorFusionPolicy.canFuseOverPipeline(plan)
    def canFuse: Boolean = source.fusedPlans.nonEmpty && source.middlePlans.isEmpty && operatorFusionPolicy.canFuse(plan)

    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source, plan)
          pipeline.serial = true
          pipeline
        } else {
          source.outputDefinition = ProduceResultOutput(produceResult)
          source.serial = true
          source
        }

      case _: Sort |
           _: Aggregation |
           _: Top =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val argumentStateBuffer = outputToArgumentStateBuffer(source, plan, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = argumentStateBuffer
          pipeline
        } else {
          throw new UnsupportedOperationException("not implemented")
        }

      case _: Optional =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val optionalMorselBuffer = outputToOptionalMorselBuffer(source, plan, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = optionalMorselBuffer
          pipeline
        } else {
          throw new UnsupportedOperationException("not implemented")
        }

      case _: Limit =>
        val asm = stateDefinition.newArgumentStateMap(plan.id, argument.argumentSlotOffset, counts = false)
        markInUpstreamBuffers(source.inputBuffer, argument, DownstreamWorkCanceller(asm.id))
        if (canFuseOverPipeline) {
          source.fusedPlans += plan
          source
        } else {
          source.middlePlans += plan
          source
        }

      case _: Distinct =>
        val asm = stateDefinition.newArgumentStateMap(plan.id, argument.argumentSlotOffset, false)
        argument.downstreamStates += DownstreamState(asm.id)
        source.middlePlans += plan
        source

      case _ =>
        if (canFuseOverPipeline) {
          source.fusedPlans += plan
          source
        } else if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source, plan)
          pipeline
        } else if (canFuse) {
          source.fusedPlans += plan
          source
        } else {
          source.middlePlans += plan
          source
        }
    }
  }

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan,
                                                      lhs: PipelineDefinitionBuild,
                                                      argument: ApplyBufferDefinitionBuild): ApplyBufferDefinitionBuild =
  {
    plan match {
      case _: plans.Apply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToApplyBuffer(lhs, argumentSlotOffset, plan)

      case _ =>
        argument
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: PipelineDefinitionBuild, rhs: PipelineDefinitionBuild, argument: ApplyBufferDefinitionBuild): PipelineDefinitionBuild = {

    plan match {
      case apply: plans.Apply =>
        //This is a little complicated: rhs.middlePlans can be empty because we have fused plans
        val applyRhsPlan = if (rhs.middlePlans.isEmpty && rhs.fusedPlans.size <= 1) rhs.headPlan
                            else if (rhs.middlePlans.isEmpty) rhs.fusedPlans.last
                            else rhs.middlePlans.last
        applyRhsPlans(apply.id.x) = applyRhsPlan.id.x
        rhs

      case _: plans.NodeHashJoin |
           _: plans.CartesianProduct=>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(lhs, rhs, plan.id, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = buffer
          pipeline
        } else {
          throw new UnsupportedOperationException("not implemented")
        }
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
  private def markReducerInUpstreamBuffers(buffer: BufferDefinitionBuild,
                                           applyBuffer: ApplyBufferDefinitionBuild,
                                           argumentStateDefinition: ArgumentStateDefinitionBuild): Unit = {
    val downstreamReduce = DownstreamReduce(argumentStateDefinition.id)
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.downstreamStates += downstreamReduce,
      lHSAccumulatingRHSStreamingBufferDefinition => lHSAccumulatingRHSStreamingBufferDefinition.downstreamStates += downstreamReduce,
      delegateBuffer => {
        val b = delegateBuffer.applyBuffer
        b.downstreamStates += downstreamReduce
        delegateBuffer.downstreamStates += downstreamReduce
      },
      lastDelegateBuffer => {
        val b = lastDelegateBuffer.applyBuffer
        b.reducersOnRHS += argumentStateDefinition
        lastDelegateBuffer.downstreamStates += downstreamReduce
      }
    )
  }

  private def markInUpstreamBuffers(buffer: BufferDefinitionBuild,
                                    applyBuffer: ApplyBufferDefinitionBuild,
                                    downstreamState: DownstreamStateOperator): Unit = {
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.downstreamStates += downstreamState,
      lHSAccumulatingRHSStreamingBufferDefinition => lHSAccumulatingRHSStreamingBufferDefinition.downstreamStates += downstreamState,
      delegateBuffer => delegateBuffer.downstreamStates += downstreamState,
      lastDelegateBuffer => {
        val b = lastDelegateBuffer.applyBuffer
        b.downstreamStates += downstreamState
        lastDelegateBuffer.downstreamStates += downstreamState
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
  private def traverseBuffers(buffer: BufferDefinitionBuild,
                              applyBuffer: ApplyBufferDefinitionBuild,
                              onInputBuffer: BufferDefinitionBuild => Unit,
                              onLHSAccumulatingRHSStreamingBuffer: LHSAccumulatingRHSStreamingBufferDefinitionBuild => Unit,
                              onDelegateBuffer: DelegateBufferDefinitionBuild => Unit,
                              onLastDelegate: DelegateBufferDefinitionBuild => Unit): Unit = {
    @tailrec
    def bfs(buffers: Seq[BufferDefinitionBuild]): Unit = {
      val upstreams = new ArrayBuffer[BufferDefinitionBuild]()

      buffers.foreach {
        case d: DelegateBufferDefinitionBuild if d.applyBuffer == applyBuffer =>
          onLastDelegate(d)
        case _: ApplyBufferDefinitionBuild =>
          throw new IllegalStateException("Nothing should have an apply buffer as immediate input, it should have a delegate buffer instead.")
        case b: LHSAccumulatingRHSStreamingBufferDefinitionBuild =>
          onLHSAccumulatingRHSStreamingBuffer(b)
          // We only add the RHS since the LHS is not streaming through the join
          // Therefore it needs to finish before the join can even start
          upstreams += pipelines(b.rhsPipelineId.x).inputBuffer
        case d: DelegateBufferDefinitionBuild =>
          onDelegateBuffer(d)
          upstreams += pipelines(d.applyBuffer.producingPipelineId.x).inputBuffer
        case b: MorselBufferDefinitionBuild =>
          onInputBuffer(b)
          upstreams += pipelines(b.producingPipelineId.x).inputBuffer
        case b: OptionalMorselBufferDefinitionBuild =>
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
