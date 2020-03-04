/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.eclipse.collections.api.map.primitive.MutableObjectIntMap
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ApplyBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.AttachBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.BufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DelegateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamReduce
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamState
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamStateOperator
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DownstreamWorkCanceller
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ExecutionStateDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.LHSAccumulatingRHSStreamingBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.MorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.NO_PIPELINE_BUILD
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.OptionalMorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.PipelineDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.UnionBufferDefiner
import org.neo4j.cypher.internal.util.attribution.Id

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Collection of mutable builder classes that are modified by [[PipelineTreeBuilder]] and finally
 * converted to an [[ExecutionGraphDefinition]] by the [[ExecutionGraphDefiner]].
 */
object PipelineTreeBuilder {
  /**
   * Builder for [[PipelineDefinition]]
   */
  class PipelineDefiner(val id: PipelineId,
                        val headPlan: LogicalPlan) {
    var lhs: PipelineId = NO_PIPELINE
    var rhs: PipelineId = NO_PIPELINE
    var inputBuffer: BufferDefiner = _
    var outputDefinition: OutputDefinition = NoOutput
    /**
     * The list of fused plans contains all fusable plans starting from the headPlan and
     * continuing with as many fusable consecutive middlePlans as possible.
     * If a plan is in `fusedPlans`. it will not be in `middlePlans` and vice versa.
     */
    val fusedPlans = new ArrayBuffer[LogicalPlan]
    val middlePlans = new ArrayBuffer[LogicalPlan]
    var serial: Boolean = false

    def result: PipelineDefinition = PipelineDefinition(id, lhs, rhs, headPlan, fusedPlans, inputBuffer.result, outputDefinition, middlePlans, serial)
  }

  // In the execution graph for cartesian product we directly connect an applyBuffer with the LHS of an
  // LHSAccumulatingRHSStreamingBuffer. We use NO_PIPELINE_BUILD to signal that.
  val NO_PIPELINE_BUILD = new PipelineDefiner(PipelineId.NO_PIPELINE, null)

  sealed trait DownstreamStateOperator
  case class DownstreamReduce(id: ArgumentStateMapId) extends DownstreamStateOperator
  case class DownstreamWorkCanceller(id: ArgumentStateMapId) extends DownstreamStateOperator
  case class DownstreamState(id: ArgumentStateMapId) extends DownstreamStateOperator

  abstract class BufferDefiner(val id: BufferId, val memoryTrackingOperatorId: Id, val bufferConfiguration: SlotConfiguration) {
    val downstreamStates = new ArrayBuffer[DownstreamStateOperator]

    def result: BufferDefinition = {
      val downstreamReducers = downstreamStates.collect { case d: DownstreamReduce => d.id }
      val workCancellerIDs = downstreamStates.collect { case d: DownstreamWorkCanceller => d.id }
      val downstreamStateIDs = downstreamStates.collect { case d: DownstreamState => d.id }

      BufferDefinition(id,
                       memoryTrackingOperatorId,
                       downstreamReducers.toArray[ArgumentStateMapId],
                       workCancellerIDs.toArray[ArgumentStateMapId],
                       downstreamStateIDs.toArray[ArgumentStateMapId],
                       variant
                     )(bufferConfiguration)
    }

    protected def variant: BufferVariant
  }

  class MorselBufferDefiner(id: BufferId,
                            memoryTrackingOperatorId: Id,
                            val producingPipelineId: PipelineId,
                            bufferConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = RegularBufferVariant
  }

  class UnionBufferDefiner(id: BufferId,
                           memoryTrackingOperatorId: Id,
                           val lhsProducingPipelineId: PipelineId,
                           val rhsProducingPipelineId: PipelineId,
                           bufferConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = RegularBufferVariant
  }

  class OptionalMorselBufferDefiner(id: BufferId,
                                    memoryTrackingOperatorId: Id,
                                    val producingPipelineId: PipelineId,
                                    val argumentStateMapId: ArgumentStateMapId,
                                    val argumentSlotOffset: Int,
                                    bufferConfiguration: SlotConfiguration,
                                    val optionalBufferVariantType: OptionalBufferVariantType) extends BufferDefiner(id, memoryTrackingOperatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = OptionalBufferVariant(argumentStateMapId, optionalBufferVariantType)
  }

  class DelegateBufferDefiner(id: BufferId,
                              operatorId: Id,
                              val applyBuffer: ApplyBufferDefiner,
                              bufferConfiguration: SlotConfiguration) extends BufferDefiner(id, operatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = RegularBufferVariant
  }

  class ApplyBufferDefiner(id: BufferId,
                           memoryTrackingOperatorId: Id,
                           producingPipelineId: PipelineId,
                           val argumentSlotOffset: Int,
                           bufferSlotConfiguration: SlotConfiguration
                          ) extends MorselBufferDefiner(id, memoryTrackingOperatorId, producingPipelineId, bufferSlotConfiguration) {
    // Map from the ArgumentStates of reducers on the RHS to the initial count they should be initialized with
    private val reducersOnRHSMap: MutableObjectIntMap[ArgumentStateDefinition] = new ObjectIntHashMap[ArgumentStateDefinition]()
    val delegates: mutable.ArrayBuilder[BufferId] = mutable.ArrayBuilder.make[BufferId]()

    def registerReducerOnRHS(reducer: ArgumentStateDefinition): Unit = {
      reducersOnRHSMap.addToValue(reducer, 1)
    }

    override protected def variant: BufferVariant = {
      val reducersOnRHS = mutable.ArrayBuilder.make[Initialization[ArgumentStateMapId]]()
      reducersOnRHS.sizeHint(reducersOnRHSMap.size())
      reducersOnRHSMap.forEachKeyValue( (argStateBuild, initialCount) => reducersOnRHS += Initialization(argStateBuild.id, initialCount) )

      // Argument state maps IDs are given in upstream -> downstream order.
      // By ordering the reducers by ID descending, we get them in downstream -> upstream order.
      val reducersOnRHSReversed = reducersOnRHS.result().sortBy(_.receiver.x * -1)

      ApplyBufferVariant(argumentSlotOffset,
                         reducersOnRHSReversed,
                         delegates.result())
    }
  }

  class AttachBufferDefiner(id: BufferId,
                            memoryTrackingOperatorId: Id,
                            inputSlotConfiguration: SlotConfiguration,
                            val outputSlotConfiguration: SlotConfiguration,
                            val argumentSlotOffset: Int,
                            val argumentSize: SlotConfiguration.Size
                           ) extends BufferDefiner(id, memoryTrackingOperatorId, inputSlotConfiguration) {

    var applyBuffer: ApplyBufferDefiner = _

    override protected def variant: BufferVariant = AttachBufferVariant(applyBuffer.result, outputSlotConfiguration, argumentSlotOffset, argumentSize)
  }

  class ArgumentStateBufferDefiner(id: BufferId,
                                   memoryTrackingOperatorId: Id,
                                   producingPipelineId: PipelineId,
                                   val argumentStateMapId: ArgumentStateMapId,
                                   bufferSlotConfiguration: SlotConfiguration) extends MorselBufferDefiner(id, memoryTrackingOperatorId, producingPipelineId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = ArgumentStateBufferVariant(argumentStateMapId)
  }


  class LHSAccumulatingBufferDefiner(id: BufferId,
                                     memoryTrackingOperatorId: Id,
                                     val argumentStateMapId: ArgumentStateMapId,
                                     val producingPipelineId: PipelineId,
                                     bufferSlotConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = LHSAccumulatingBufferVariant(argumentStateMapId)
  }

  class RHSStreamingBufferDefiner(id: BufferId,
                                  memoryTrackingOperatorId: Id,
                                  val argumentStateMapId: ArgumentStateMapId,
                                  val producingPipelineId: PipelineId,
                                  bufferSlotConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = RHSStreamingBufferVariant(argumentStateMapId)
  }

  class LHSAccumulatingRHSStreamingBufferDefiner(id: BufferId,
                                                 memoryTrackingOperatorId: Id,
                                                 val lhsSink: LHSAccumulatingBufferDefiner,
                                                 val rhsSink: RHSStreamingBufferDefiner,
                                                 val lhsArgumentStateMapId: ArgumentStateMapId,
                                                 val rhsArgumentStateMapId: ArgumentStateMapId,
                                                 bufferSlotConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant =
      LHSAccumulatingRHSStreamingBufferVariant(lhsSink.result, rhsSink.result, lhsArgumentStateMapId, rhsArgumentStateMapId)
  }

  /**
   * Builder for [[ExecutionGraphDefinition]]
   */
  class ExecutionStateDefiner(val physicalPlan: PhysicalPlan) {
    val buffers = new ArrayBuffer[BufferDefiner]
    val argumentStateMaps = new ArrayBuffer[ArgumentStateDefinition]
    var initBuffer: ApplyBufferDefiner = _

    def newArgumentStateMap(planId: Id, argumentSlotOffset: Int): ArgumentStateDefinition = {
      val x = argumentStateMaps.size
      val asm = ArgumentStateDefinition(ArgumentStateMapId(x), planId, argumentSlotOffset)
      argumentStateMaps += asm
      asm
    }

    def newBuffer(producingPipelineId: PipelineId,
                  memoryTrackingOperatorId: Id,
                  bufferSlotConfiguration: SlotConfiguration): MorselBufferDefiner = {
      val x = buffers.size
      val buffer = new MorselBufferDefiner(BufferId(x), memoryTrackingOperatorId, producingPipelineId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newUnionBuffer(lhsProducingPipelineId: PipelineId,
                       rhsProducingPipelineId: PipelineId,
                       memoryTrackingOperatorId: Id,
                       bufferSlotConfiguration: SlotConfiguration): UnionBufferDefiner = {
      val x = buffers.size
      val buffer = new UnionBufferDefiner(BufferId(x), memoryTrackingOperatorId, lhsProducingPipelineId, rhsProducingPipelineId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newOptionalBuffer(producingPipelineId: PipelineId,
                          memoryTrackingOperatorId: Id,
                          argumentStateMapId: ArgumentStateMapId,
                          argumentSlotOffset: Int,
                          bufferSlotConfiguration: SlotConfiguration,
                          optionalBufferVariantType: OptionalBufferVariantType): OptionalMorselBufferDefiner = {
      val x = buffers.size
      val buffer = new OptionalMorselBufferDefiner(BufferId(x),
                                                   memoryTrackingOperatorId,
                                                   producingPipelineId,
                                                   argumentStateMapId,
                                                   argumentSlotOffset,
                                                   bufferSlotConfiguration,
                                                   optionalBufferVariantType)
      buffers += buffer
      buffer
    }

    def newDelegateBuffer(applyBufferDefinition: ApplyBufferDefiner,
                          memoryTrackingOperatorId: Id,
                          bufferSlotConfiguration: SlotConfiguration): DelegateBufferDefiner = {
      val x = buffers.size
      val buffer = new DelegateBufferDefiner(BufferId(x), memoryTrackingOperatorId, applyBufferDefinition, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newApplyBuffer(producingPipelineId: PipelineId,
                       memoryTrackingOperatorId: Id,
                       argumentSlotOffset: Int,
                       bufferSlotConfiguration: SlotConfiguration): ApplyBufferDefiner = {
      val x = buffers.size
      val buffer = new ApplyBufferDefiner(BufferId(x), memoryTrackingOperatorId, producingPipelineId, argumentSlotOffset, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newAttachBuffer(producingPipelineId: PipelineId,
                        memoryTrackingOperatorId: Id,
                        inputSlotConfiguration: SlotConfiguration,
                        postAttachSlotConfiguration: SlotConfiguration,
                        outerArgumentSlotOffset: Int,
                        outerArgumentSize: SlotConfiguration.Size): AttachBufferDefiner = {

      val x = buffers.size
      val buffer = new AttachBufferDefiner(BufferId(x),
                                           memoryTrackingOperatorId,
                                           inputSlotConfiguration,
                                           postAttachSlotConfiguration,
                                           outerArgumentSlotOffset,
                                           outerArgumentSize)
      buffers += buffer
      buffer
    }

    def newArgumentStateBuffer(producingPipelineId: PipelineId,
                               memoryTrackingOperatorId: Id,
                               argumentStateMapId: ArgumentStateMapId,
                               bufferSlotConfiguration: SlotConfiguration): ArgumentStateBufferDefiner = {
      val x = buffers.size
      val buffer = new ArgumentStateBufferDefiner(BufferId(x), memoryTrackingOperatorId, producingPipelineId, argumentStateMapId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newLhsAccumulatingRhsStreamingBuffer(lhsProducingPipelineId: PipelineId,
                                             rhsProducingPipelineId: PipelineId,
                                             memoryTrackingOperatorId: Id,
                                             lhsArgumentStateMapId: ArgumentStateMapId,
                                             rhsArgumentStateMapId: ArgumentStateMapId,
                                             bufferSlotConfiguration: SlotConfiguration): LHSAccumulatingRHSStreamingBufferDefiner = {
      val lhsAccId = BufferId(buffers.size)
      val lhsAcc = new LHSAccumulatingBufferDefiner(lhsAccId,
                                                    memoryTrackingOperatorId,
                                                    lhsArgumentStateMapId,
                                                    lhsProducingPipelineId,
                                                    bufferSlotConfiguration = null) // left null because buffer is never used as source
      buffers += lhsAcc

      val rhsAccId = BufferId(buffers.size)
      val rhsAcc = new RHSStreamingBufferDefiner(rhsAccId,
                                                 memoryTrackingOperatorId,
                                                 rhsArgumentStateMapId,
                                                 rhsProducingPipelineId,
                                                 bufferSlotConfiguration = null) // left null because buffer is never used as source
      buffers += rhsAcc

      val x = buffers.size
      val buffer = new LHSAccumulatingRHSStreamingBufferDefiner(BufferId(x),
                                                                memoryTrackingOperatorId,
                                                                lhsAcc,
                                                                rhsAcc,
                                                                lhsArgumentStateMapId,
                                                                rhsArgumentStateMapId,
                                                                bufferSlotConfiguration)
      buffers += buffer
      buffer
    }
  }
}

/**
 * Fills an [[ExecutionStateDefiner]] and and array of [[PipelineDefiner]]s.
 * Final conversion to [[ExecutionGraphDefinition]] is done by [[ExecutionGraphDefiner]].
 */
class PipelineTreeBuilder(breakingPolicy: PipelineBreakingPolicy,
                          operatorFusionPolicy: OperatorFusionPolicy,
                          stateDefiner: ExecutionStateDefiner,
                          slotConfigurations: SlotConfigurations,
                          argumentSizes: ArgumentSizes)
  extends TreeBuilder[PipelineDefiner, ApplyBufferDefiner] {

  private[physicalplanning] val pipelines = new ArrayBuffer[PipelineDefiner]
  private[physicalplanning] val applyRhsPlans = new mutable.HashMap[Int, Int]()

  private def newPipeline(plan: LogicalPlan): PipelineDefiner = {
    val pipeline = new PipelineDefiner(PipelineId(pipelines.size), plan)
    pipelines += pipeline
    if (operatorFusionPolicy.canFuse(plan)) {
      pipeline.fusedPlans += plan
    }
    pipeline
  }

  private def outputToBuffer(pipeline: PipelineDefiner, nextPipelineHeadPlan: LogicalPlan): MorselBufferDefiner = {
    val output = stateDefiner.newBuffer(pipeline.id, nextPipelineHeadPlan.id, slotConfigurations(pipeline.headPlan.id))
    pipeline.outputDefinition = MorselBufferOutput(output.id, nextPipelineHeadPlan.id)
    output
  }

  private def outputToUnionBuffer(lhs: PipelineDefiner, rhs: PipelineDefiner, nextPipelineHeadPlan: Id): UnionBufferDefiner = {
    // The Buffer doesn't really have a slot configuration, since it can store Morsels from both LHS and RHS with different slot configs.
    // Does it work to use the configuration of the Union plan?
    val output = stateDefiner.newUnionBuffer(lhs.id, rhs.id, nextPipelineHeadPlan, slotConfigurations(nextPipelineHeadPlan))
    lhs.outputDefinition = MorselBufferOutput(output.id, nextPipelineHeadPlan)
    rhs.outputDefinition = MorselBufferOutput(output.id, nextPipelineHeadPlan)
    output
  }

  private def outputToApplyBuffer(pipeline: PipelineDefiner, argumentSlotOffset: Int, nextPipelineHeadPlan: LogicalPlan): ApplyBufferDefiner = {
    val output = stateDefiner.newApplyBuffer(pipeline.id, nextPipelineHeadPlan.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id))
    pipeline.outputDefinition = MorselBufferOutput(output.id, nextPipelineHeadPlan.id)
    output
  }

  private def outputToAttachApplyBuffer(pipeline: PipelineDefiner,
                                        attachingPlanId: Id,
                                        argumentSlotOffset: Int,
                                        postAttachSlotConfiguration: SlotConfiguration,
                                        outerArgumentSlotOffset: Int,
                                        outerArgumentSize: SlotConfiguration.Size): AttachBufferDefiner = {

    val output = stateDefiner.newAttachBuffer(pipeline.id,
      attachingPlanId,
      slotConfigurations(pipeline.headPlan.id),
      postAttachSlotConfiguration,
      outerArgumentSlotOffset,
      outerArgumentSize)

    output.applyBuffer = stateDefiner.newApplyBuffer(pipeline.id, attachingPlanId, argumentSlotOffset, postAttachSlotConfiguration)
    pipeline.outputDefinition = MorselBufferOutput(output.id, attachingPlanId)
    output
  }

  private def outputToArgumentStateBuffer(pipeline: PipelineDefiner, plan: LogicalPlan, applyBuffer: ApplyBufferDefiner, argumentSlotOffset: Int): ArgumentStateBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newArgumentStateBuffer(pipeline.id, plan.id, asm.id, slotConfigurations(pipeline.headPlan.id))
    pipeline.outputDefinition = ReduceOutput(output.id, plan)
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToOptionalPipelinedBuffer(pipeline: PipelineDefiner,
                                              plan: LogicalPlan,
                                              applyBuffer: ApplyBufferDefiner,
                                              argumentSlotOffset: Int): OptionalMorselBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newOptionalBuffer(pipeline.id, plan.id, asm.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id), OptionalType)
    pipeline.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, plan.id)
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToAntiPipelinedBuffer(pipeline: PipelineDefiner,
                                          plan: LogicalPlan,
                                          applyBuffer: ApplyBufferDefiner,
                                          argumentSlotOffset: Int): OptionalMorselBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newOptionalBuffer(pipeline.id, plan.id, asm.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id), AntiType)
    pipeline.outputDefinition = MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, plan.id)
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToLhsAccumulatingRhsStreamingBuffer(lhs: PipelineDefiner,
                                                        rhs: PipelineDefiner,
                                                        planId: Id,
                                                        applyBuffer: ApplyBufferDefiner,
                                                        argumentSlotOffset: Int): LHSAccumulatingRHSStreamingBufferDefiner = {
    val lhsAsm = stateDefiner.newArgumentStateMap(planId, argumentSlotOffset)
    val rhsAsm = stateDefiner.newArgumentStateMap(planId, argumentSlotOffset)
    val output = stateDefiner.newLhsAccumulatingRhsStreamingBuffer(lhs.id, rhs.id, planId, lhsAsm.id, rhsAsm.id, slotConfigurations(rhs.headPlan.id))
    if (lhs != NO_PIPELINE_BUILD) {
      lhs.outputDefinition = MorselArgumentStateBufferOutput(output.lhsSink.id, argumentSlotOffset, planId)
      markReducerInUpstreamBuffers(lhs.inputBuffer, applyBuffer, lhsAsm)
    } else {
      // The LHS argument state map has to be initiated by the apply even if there is no pipeline
      // connecting them. Otherwise the LhsAccumulatingRhsStreamingBuffer can never output anything.
      applyBuffer.registerReducerOnRHS(lhsAsm)
    }
    rhs.outputDefinition = MorselArgumentStateBufferOutput(output.rhsSink.id, argumentSlotOffset, planId)
    markReducerInUpstreamBuffers(rhs.inputBuffer, applyBuffer, rhsAsm)
    output
  }

  override protected def validatePlan(plan: LogicalPlan): Unit = breakingPolicy.breakOn(plan)

  override protected def initialArgument(leftLeaf: LogicalPlan): ApplyBufferDefiner = {
    val initialArgumentSlotOffset = slotConfigurations(leftLeaf.id).getArgumentLongOffsetFor(Id.INVALID_ID)
    stateDefiner.initBuffer = stateDefiner.newApplyBuffer(NO_PIPELINE, leftLeaf.id, initialArgumentSlotOffset, SlotAllocation.INITIAL_SLOT_CONFIGURATION)
    stateDefiner.initBuffer
  }

  override protected def onLeaf(plan: LogicalPlan,
                                argument: ApplyBufferDefiner): PipelineDefiner = {
    if (breakingPolicy.breakOn(plan)) {
      val pipeline = newPipeline(plan)
      val delegate = stateDefiner.newDelegateBuffer(argument, plan.id, argument.bufferConfiguration)
      argument.delegates += delegate.id
      pipeline.inputBuffer = delegate
      pipeline.lhs = argument.producingPipelineId
      pipeline
    } else {
      throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
    }
  }

  override protected def onOneChildPlan(plan: LogicalPlan,
                                        source: PipelineDefiner,
                                        argument: ApplyBufferDefiner): PipelineDefiner = {

    def canFuseOverPipeline: Boolean = source.fusedPlans.nonEmpty && source.middlePlans.isEmpty && operatorFusionPolicy.canFuseOverPipeline(plan)
    def canFuse: Boolean = source.fusedPlans.nonEmpty && source.middlePlans.isEmpty && operatorFusionPolicy.canFuse(plan)

    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source, plan)
          pipeline.lhs = source.id
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
          pipeline.lhs = source.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: Optional =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val optionalPipelinedBuffer = outputToOptionalPipelinedBuffer(source, plan, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = optionalPipelinedBuffer
          pipeline.lhs = source.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: Anti =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val antiPipelinedBuffer = outputToAntiPipelinedBuffer(source, plan, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = antiPipelinedBuffer
          pipeline.lhs = source.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: Limit | _: Skip =>
        val asm = stateDefiner.newArgumentStateMap(plan.id, argument.argumentSlotOffset)
        markInUpstreamBuffers(source.inputBuffer, argument, DownstreamWorkCanceller(asm.id))
        if (canFuseOverPipeline) {
          source.fusedPlans += plan
          source
        } else {
          source.middlePlans += plan
          source
        }

      case _: Distinct =>
        val asm = stateDefiner.newArgumentStateMap(plan.id, argument.argumentSlotOffset)
        argument.downstreamStates += DownstreamState(asm.id)
        if (canFuseOverPipeline) {
          source.fusedPlans += plan
          source
        } else {
          source.middlePlans += plan
          source
        }

      case _ =>
        if (canFuseOverPipeline) {
          source.fusedPlans += plan
          source
        } else if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source, plan)
          pipeline.lhs = source.id
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
                                                      lhs: PipelineDefiner,
                                                      argument: ApplyBufferDefiner): ApplyBufferDefiner =
  {
    plan match {
      case _: plans.Apply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToApplyBuffer(lhs, argumentSlotOffset, plan)

      case _: plans.CartesianProduct =>
        val rhsSlots = slotConfigurations(plan.rhs.get.id)
        val argumentSlotOffset = rhsSlots.getArgumentLongOffsetFor(plan.id)
        val outerArgumentSize = argumentSizes.get(LogicalPlans.leftLeaf(plan).id)
        val outerArgumentSlotOffset = argument.argumentSlotOffset

        outputToAttachApplyBuffer(lhs, plan.id, argumentSlotOffset, rhsSlots, outerArgumentSlotOffset, outerArgumentSize).applyBuffer

      case _ =>
        argument
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan,
                                                       lhs: PipelineDefiner,
                                                       rhs: PipelineDefiner,
                                                       argument: ApplyBufferDefiner): PipelineDefiner = {

    plan match {
      case apply: plans.Apply =>
        //This is a little complicated: rhs.middlePlans can be empty because we have fused plans
        val applyRhsPlan = if (rhs.middlePlans.isEmpty && rhs.fusedPlans.size <= 1) {
          rhs.headPlan
        } else if (rhs.middlePlans.isEmpty) {
          rhs.fusedPlans.last
        } else {
          rhs.middlePlans.last
        }
        applyRhsPlans(apply.id.x) = applyRhsPlan.id.x
        rhs

      case _: CartesianProduct =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(NO_PIPELINE_BUILD, rhs, plan.id, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = buffer
          pipeline.lhs = lhs.id
          pipeline.rhs = rhs.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: plans.NodeHashJoin =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(lhs, rhs, plan.id, argument, argument.argumentSlotOffset)
          pipeline.inputBuffer = buffer
          pipeline.lhs = lhs.id
          pipeline.rhs = rhs.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: plans.Union =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          val buffer = outputToUnionBuffer(lhs, rhs, plan.id)
          pipeline.inputBuffer = buffer
          pipeline.lhs = lhs.id
          pipeline.rhs = rhs.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
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
  private def markReducerInUpstreamBuffers(buffer: BufferDefiner,
                                           applyBuffer: ApplyBufferDefiner,
                                           argumentStateDefinition: ArgumentStateDefinition): Unit = {
    val downstreamReduce = DownstreamReduce(argumentStateDefinition.id)
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.downstreamStates += downstreamReduce,
      lHSAccumulatingRHSStreamingBufferDefinition => {
        lHSAccumulatingRHSStreamingBufferDefinition.lhsSink.downstreamStates += downstreamReduce
        lHSAccumulatingRHSStreamingBufferDefinition.rhsSink.downstreamStates += downstreamReduce
        lHSAccumulatingRHSStreamingBufferDefinition.downstreamStates += downstreamReduce
      },
      delegateBuffer => {
        val b = delegateBuffer.applyBuffer
        b.downstreamStates += downstreamReduce
        delegateBuffer.downstreamStates += downstreamReduce
      },
      lastDelegateBuffer => {
        val b = lastDelegateBuffer.applyBuffer
        b.registerReducerOnRHS(argumentStateDefinition)
        lastDelegateBuffer.downstreamStates += downstreamReduce
      }
    )
  }

  private def markInUpstreamBuffers(buffer: BufferDefiner,
                                    applyBuffer: ApplyBufferDefiner,
                                    downstreamState: DownstreamStateOperator): Unit = {
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.downstreamStates += downstreamState,
      lHSAccumulatingRHSStreamingBufferDefinition => {
        lHSAccumulatingRHSStreamingBufferDefinition.lhsSink.downstreamStates += downstreamState
        lHSAccumulatingRHSStreamingBufferDefinition.rhsSink.downstreamStates += downstreamState
        lHSAccumulatingRHSStreamingBufferDefinition.downstreamStates += downstreamState
      },
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
  private def traverseBuffers(buffer: BufferDefiner,
                              applyBuffer: ApplyBufferDefiner,
                              onInputBuffer: BufferDefiner => Unit,
                              onLHSAccumulatingRHSStreamingBuffer: LHSAccumulatingRHSStreamingBufferDefiner => Unit,
                              onDelegateBuffer: DelegateBufferDefiner => Unit,
                              onLastDelegate: DelegateBufferDefiner => Unit): Unit = {
    @tailrec
    def bfs(buffers: Seq[BufferDefiner]): Unit = {
      val upstreams = new ArrayBuffer[BufferDefiner]()

      buffers.foreach {
        case d: DelegateBufferDefiner if d.applyBuffer == applyBuffer =>
          onLastDelegate(d)
        case _: AttachBufferDefiner =>
          throw new IllegalStateException("Nothing should have an attach buffer as immediate input, it should have a delegate buffer instead.")
        case _: ApplyBufferDefiner =>
          throw new IllegalStateException("Nothing should have an apply buffer as immediate input, it should have a delegate buffer instead.")
        case b: LHSAccumulatingRHSStreamingBufferDefiner =>
          onLHSAccumulatingRHSStreamingBuffer(b)
          // We only add the RHS since the LHS is not streaming through the join
          // Therefore it needs to finish before the join can even start
          upstreams += pipelines(b.rhsSink.producingPipelineId.x).inputBuffer
        case d: DelegateBufferDefiner =>
          onDelegateBuffer(d)
          upstreams += pipelines(d.applyBuffer.producingPipelineId.x).inputBuffer
        case b: MorselBufferDefiner =>
          onInputBuffer(b)
          upstreams += pipelines(b.producingPipelineId.x).inputBuffer
        case b: UnionBufferDefiner =>
          onInputBuffer(b)
          upstreams += pipelines(b.lhsProducingPipelineId.x).inputBuffer
          upstreams += pipelines(b.rhsProducingPipelineId.x).inputBuffer
        case b: OptionalMorselBufferDefiner =>
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
