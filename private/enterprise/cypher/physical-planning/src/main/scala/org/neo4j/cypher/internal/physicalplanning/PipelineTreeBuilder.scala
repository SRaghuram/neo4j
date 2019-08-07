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
  abstract class BufferDefinitionBuild(val id: BufferId) {
    val downstreamStates = new ArrayBuffer[DownstreamStateOperator]
  }

  /**
    * Builder for [[RegularBufferVariant]]
    */
  class MorselBufferDefinitionBuild(id: BufferId,
                                    val producingPipelineId: PipelineId) extends BufferDefinitionBuild(id)

  /**
    * Builder for [[OptionalBufferVariant]]
    */
  class OptionalMorselBufferDefinitionBuild(id: BufferId,
                                            val producingPipelineId: PipelineId,
                                            val argumentStateMapId: ArgumentStateMapId,
                                            val argumentSlotOffset: Int) extends BufferDefinitionBuild(id)

  /**
    * Builder for [[RegularBufferVariant]], that is a delegate.
    */
  class DelegateBufferDefinitionBuild(id: BufferId,
                                      val applyBuffer: ApplyBufferDefinitionBuild) extends BufferDefinitionBuild(id)

  /**
    * Builder for [[ApplyBufferVariant]]
    */
  class ApplyBufferDefinitionBuild(id: BufferId,
                                   producingPipelineId: PipelineId,
                                   val argumentSlotOffset: Int
                             ) extends MorselBufferDefinitionBuild(id, producingPipelineId) {
    // These are ArgumentStates of reducers on the RHS
    val reducersOnRHS = new ArrayBuffer[ArgumentStateDefinitionBuild]
    val delegates: ArrayBuffer[BufferId] = new ArrayBuffer[BufferId]()
  }

  /**
    * Builder for [[ArgumentStateBufferVariant]]
    */
  class ArgumentStateBufferDefinitionBuild(id: BufferId,
                                           producingPipelineId: PipelineId,
                                           val argumentStateMapId: ArgumentStateMapId) extends MorselBufferDefinitionBuild(id, producingPipelineId)

  /**
    * Builder for [[LHSAccumulatingRHSStreamingBufferVariant]]
    */
  class LHSAccumulatingRHSStreamingBufferDefinitionBuild(id: BufferId,
                                                         val lhsPipelineId: PipelineId,
                                                         val rhsPipelineId: PipelineId,
                                                         val lhsArgumentStateMapId: ArgumentStateMapId,
                                                         val rhsArgumentStateMapId: ArgumentStateMapId) extends BufferDefinitionBuild(id)

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

    def newBuffer(producingPipelineId: PipelineId): MorselBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new MorselBufferDefinitionBuild(BufferId(x), producingPipelineId)
      buffers += buffer
      buffer
    }

    def newOptionalBuffer(producingPipelineId: PipelineId, argumentStateMapId: ArgumentStateMapId, argumentSlotOffset: Int): OptionalMorselBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new OptionalMorselBufferDefinitionBuild(BufferId(x), producingPipelineId, argumentStateMapId, argumentSlotOffset)
      buffers += buffer
      buffer
    }

    def newDelegateBuffer(applyBufferDefinition: ApplyBufferDefinitionBuild): DelegateBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new DelegateBufferDefinitionBuild(BufferId(x), applyBufferDefinition)
      buffers += buffer
      buffer
    }

    def newApplyBuffer(producingPipelineId: PipelineId,
                       argumentSlotOffset: Int): ApplyBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new ApplyBufferDefinitionBuild(BufferId(x), producingPipelineId, argumentSlotOffset)
      buffers += buffer
      buffer
    }

    def newArgumentStateBuffer(producingPipelineId: PipelineId,
                               argumentStateMapId: ArgumentStateMapId): ArgumentStateBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new ArgumentStateBufferDefinitionBuild(BufferId(x), producingPipelineId, argumentStateMapId)
      buffers += buffer
      buffer
    }

    def newLhsAccumulatingRhsStreamingBuffer(lhsProducingPipelineId: PipelineId,
                                             rhsProducingPipelineId: PipelineId,
                                             lhsargumentStateMapId: ArgumentStateMapId,
                                             rhsargumentStateMapId: ArgumentStateMapId): LHSAccumulatingRHSStreamingBufferDefinitionBuild = {
      val x = buffers.size
      val buffer = new LHSAccumulatingRHSStreamingBufferDefinitionBuild(BufferId(x), lhsProducingPipelineId, rhsProducingPipelineId, lhsargumentStateMapId, rhsargumentStateMapId)
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
                          stateDefinition: ExecutionStateDefinitionBuild,
                          slotConfigurations: SlotConfigurations)
  extends TreeBuilder[PipelineDefinitionBuild, ApplyBufferDefinitionBuild] {

  private[physicalplanning] val pipelines = new ArrayBuffer[PipelineDefinitionBuild]
  private[physicalplanning] val applyRhsPlans = new mutable.HashMap[Int, Int]()

  private def newPipeline(plan: LogicalPlan) = {
    val pipeline = new PipelineDefinitionBuild(PipelineId(pipelines.size), plan)
    pipelines += pipeline
    pipeline
  }

  private def outputToBuffer(pipeline: PipelineDefinitionBuild): MorselBufferDefinitionBuild = {
    val output = stateDefinition.newBuffer(pipeline.id)
    pipeline.outputDefinition = MorselBufferOutput(output.id)
    output
  }

  private def outputToApplyBuffer(pipeline: PipelineDefinitionBuild, argumentSlotOffset: Int): ApplyBufferDefinitionBuild = {
    val output = stateDefinition.newApplyBuffer(pipeline.id, argumentSlotOffset)
    pipeline.outputDefinition = MorselBufferOutput(output.id)
    output
  }

  private def outputToArgumentStateBuffer(pipeline: PipelineDefinitionBuild, plan: LogicalPlan, applyBuffer: ApplyBufferDefinitionBuild, argumentSlotOffset: Int): ArgumentStateBufferDefinitionBuild = {
    val asm = stateDefinition.newArgumentStateMap(plan.id, argumentSlotOffset, counts = true)
    val output = stateDefinition.newArgumentStateBuffer(pipeline.id, asm.id)
    pipeline.outputDefinition = ReduceOutput(output.id, plan)
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToOptionalMorselBuffer(pipeline: PipelineDefinitionBuild, plan: LogicalPlan, applyBuffer: ApplyBufferDefinitionBuild, argumentSlotOffset: Int): OptionalMorselBufferDefinitionBuild = {
    val asm = stateDefinition.newArgumentStateMap(plan.id, argumentSlotOffset, counts = true)
    val output = stateDefinition.newOptionalBuffer(pipeline.id, asm.id, argumentSlotOffset)
    pipeline.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset)
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
    val output = stateDefinition.newLhsAccumulatingRhsStreamingBuffer(lhs.id, rhs.id, lhsAsm.id, rhsAsm.id)
    lhs.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset)
    rhs.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset)
    markReducerInUpstreamBuffers(lhs.inputBuffer, applyBuffer, lhsAsm)
    markReducerInUpstreamBuffers(rhs.inputBuffer, applyBuffer, rhsAsm)
    output
  }

  override protected def initialArgument(leftLeaf: LogicalPlan): ApplyBufferDefinitionBuild = {
    val initialArgumentSlotOffset = slotConfigurations(leftLeaf.id).getArgumentLongOffsetFor(Id.INVALID_ID)
    stateDefinition.initBuffer = stateDefinition.newApplyBuffer(NO_PIPELINE, initialArgumentSlotOffset)
    stateDefinition.initBuffer
  }

  override protected def onLeaf(plan: LogicalPlan,
                                argument: ApplyBufferDefinitionBuild): PipelineDefinitionBuild = {
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
                                        source: PipelineDefinitionBuild,
                                        argument: ApplyBufferDefinitionBuild): PipelineDefinitionBuild = {
    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source)
          pipeline.serial = true
          pipeline
        } else {
          source.outputDefinition = ProduceResultOutput(produceResult)
          source.serial = true
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

      case _: Optional =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val optionalMorselBuffer = outputToOptionalMorselBuffer(source, plan, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = optionalMorselBuffer
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
        val asm = stateDefinition.newArgumentStateMap(plan.id, argument.argumentSlotOffset, counts = false)
        markInUpstreamBuffers(source.inputBuffer, argument, DownstreamWorkCanceller(asm.id))
        source.middlePlans += plan
        source

      case _: Distinct =>
        val asm = stateDefinition.newArgumentStateMap(plan.id, argument.argumentSlotOffset, false)
        argument.downstreamStates += DownstreamState(asm.id)
        source.middlePlans += plan
        source

      case _ =>
        source.middlePlans += plan
        source
    }
  }

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan,
                                                      lhs: PipelineDefinitionBuild,
                                                      argument: ApplyBufferDefinitionBuild): ApplyBufferDefinitionBuild =
  {
    plan match {
      case _: plans.Apply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToApplyBuffer(lhs, argumentSlotOffset)

      case _ =>
        argument
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: PipelineDefinitionBuild, rhs: PipelineDefinitionBuild, argument: ApplyBufferDefinitionBuild): PipelineDefinitionBuild = {

    plan match {
      case apply: plans.Apply =>
        val applyRhsPlan = if (rhs.middlePlans.isEmpty) rhs.headPlan else rhs.middlePlans.last
        applyRhsPlans(apply.id.x) = applyRhsPlan.id.x
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
      delegateBuffer => {
        val b = delegateBuffer.applyBuffer
        b.downstreamStates += downstreamState
        delegateBuffer.downstreamStates += downstreamState
      },
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
